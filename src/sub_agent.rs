use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use futures_util::future::join_all;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::context::{ContextConfig, ContextManager};
use crate::loop_engine::{CancellationToken, LoopEngine};
use crate::plan::PlanStore;
use crate::provider::Provider;
use crate::tools::{Tool, ToolCancellation, ToolOutput, ToolRegistry};

const DEFAULT_MAX_ROUNDS: usize = 15;
const DEFAULT_TOOLS: [&str; 3] = ["read_file", "exec", "recall_memory"];
const PARALLEL_TOOLS: [&str; 2] = ["read_file", "recall_memory"];
const MAX_PARALLEL_TASKS: usize = 4;
const SUB_AGENT_SYSTEM_PROMPT: &str = "你是主 Agent 派出的只负责一个明确子任务的子 Agent。你拥有全新且独立的消息历史，不知道主对话内容；只依据用户给出的子任务和可用工具开展工作。优先调查、核验和提炼结论，不扩展任务范围。工具失败时可调整方案。完成后只返回给主 Agent 一份自洽、简洁、包含关键证据的结论。你不能再派生子 Agent。";

pub struct SubAgentTool {
    provider: Arc<dyn Provider>,
    available_tools: ToolRegistry,
    workspace: PathBuf,
    context_config: ContextConfig,
    max_rounds: usize,
}

impl SubAgentTool {
    pub fn new(
        provider: Arc<dyn Provider>,
        available_tools: ToolRegistry,
        workspace: PathBuf,
        context_config: ContextConfig,
    ) -> Self {
        Self {
            provider,
            available_tools,
            workspace,
            context_config,
            max_rounds: DEFAULT_MAX_ROUNDS,
        }
    }

    #[cfg(test)]
    fn with_max_rounds(mut self, max_rounds: usize) -> Self {
        self.max_rounds = max_rounds;
        self
    }
}

#[derive(Deserialize)]
struct SubAgentArgs {
    task: Option<String>,
    tasks: Option<Vec<String>>,
    tools: Option<Vec<String>>,
}

struct CancelChildrenOnDrop(CancellationToken);

impl Drop for CancelChildrenOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

impl SubAgentTool {
    async fn run(&self, args: Value, parent: Option<&dyn ToolCancellation>) -> Result<String> {
        let args: SubAgentArgs = serde_json::from_value(args).context("sub_agent 参数无效")?;
        let parallel = args.tasks.is_some();
        let tasks = match (args.task, args.tasks) {
            (Some(task), None) => vec![task],
            (None, Some(tasks)) if !tasks.is_empty() && tasks.len() <= MAX_PARALLEL_TASKS => tasks,
            (None, Some(_)) => bail!("sub_agent.tasks 必须包含 1 到 {MAX_PARALLEL_TASKS} 个子任务"),
            _ => bail!("sub_agent 必须且只能提供 task 或 tasks"),
        };
        if tasks.iter().any(|task| task.trim().is_empty()) {
            bail!("sub_agent 的子任务不能为空");
        }
        let defaults = if parallel {
            PARALLEL_TOOLS.as_slice()
        } else {
            DEFAULT_TOOLS.as_slice()
        };
        let requested = args
            .tools
            .unwrap_or_else(|| defaults.iter().map(|name| (*name).to_owned()).collect());
        if requested.is_empty() {
            bail!("sub_agent.tools 不能为空数组；若不需要工具请省略该字段");
        }
        if requested.iter().any(|name| name == "sub_agent") {
            bail!("子 Agent 不能递归派生子 Agent");
        }
        let tools = self
            .available_tools
            .subset(requested.iter().map(String::as_str))?;
        if parallel && requested.iter().any(|name| !tools.is_read_only(name)) {
            bail!("并发子任务只能使用只读工具；需要 exec 或写入工具时请单独调用 sub_agent");
        }
        if parent.is_some_and(ToolCancellation::is_cancelled) {
            bail!("请求已取消");
        }

        let cancellation = CancellationToken::new();
        let _cancel_children = CancelChildrenOnDrop(cancellation.clone());
        let mut handles = Vec::with_capacity(tasks.len());
        for task in tasks.iter().cloned() {
            let provider = self.provider.clone();
            let tools = tools.clone();
            let workspace = self.workspace.clone();
            let context_config = self.context_config.clone();
            let cancellation = cancellation.clone();
            let max_rounds = self.max_rounds;
            handles.push(tokio::spawn(async move {
                let context = ContextManager::with_system_prompt(
                    provider.clone(),
                    &workspace,
                    context_config,
                    Arc::new(PlanStore::memory_only()),
                    SUB_AGENT_SYSTEM_PROMPT,
                )?;
                let runner = LoopEngine::ephemeral(provider, tools, context, max_rounds);
                runner
                    .run_turn_with_events(&mut Vec::new(), task, None, cancellation)
                    .await
            }));
        }
        let joined = join_all(handles);
        tokio::pin!(joined);
        let outcomes = if let Some(parent) = parent {
            tokio::select! {
                results = &mut joined => results,
                _ = parent.cancelled() => {
                    cancellation.cancel();
                    joined.await
                }
            }
        } else {
            joined.await
        };
        if parent.is_some_and(ToolCancellation::is_cancelled) {
            bail!("请求已取消");
        }
        if !parallel {
            return outcomes
                .into_iter()
                .next()
                .expect("单任务必须产生一个结果")
                .context("子 Agent 运行任务失败")?;
        }
        let results = tasks
            .into_iter()
            .zip(outcomes)
            .map(|(task, outcome)| match outcome {
                Ok(Ok(result)) => json!({"task": task, "success": true, "result": result}),
                Ok(Err(error)) => {
                    json!({"task": task, "success": false, "error": format!("{error:#}")})
                }
                Err(error) => json!({"task": task, "success": false, "error": error.to_string()}),
            })
            .collect::<Vec<Value>>();
        Ok(json!({"results": results}).to_string())
    }
}

#[async_trait]
impl Tool for SubAgentTool {
    fn name(&self) -> &str {
        "sub_agent"
    }

    fn description(&self) -> &str {
        "将一个子任务交给独立上下文的子 Agent；也可传 tasks 并发执行最多 4 个独立只读子任务，按输入顺序返回结果。单任务默认开放 read_file、exec、recall_memory；并发任务默认只开放 read_file、recall_memory"
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "task": {
                    "type": "string",
                    "description": "单个完整、自洽的子任务描述；与 tasks 二选一，子 Agent 不会看到主对话"
                },
                "tasks": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "1 到 4 个可独立并发执行的只读子任务；与 task 二选一"
                },
                "tools": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "可选工具名列表；单任务默认 read_file、exec、recall_memory，并发任务默认 read_file、recall_memory；并发时只允许只读工具"
                }
            },
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: Value) -> Result<String> {
        self.run(args, None).await
    }

    async fn execute_rich_with_cancellation(
        &self,
        args: Value,
        cancellation: &dyn ToolCancellation,
    ) -> Result<ToolOutput> {
        self.run(args, Some(cancellation))
            .await
            .map(ToolOutput::text)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::time::Duration;

    use super::*;
    use crate::provider::{Message, Response, Role, ToolSpec};
    use tokio::sync::Barrier;

    struct MockProvider {
        responses: Mutex<VecDeque<Response>>,
        snapshots: Mutex<Vec<(Vec<Message>, Vec<ToolSpec>)>>,
    }

    #[async_trait]
    impl Provider for MockProvider {
        async fn chat(&self, messages: &[Message], tools: &[ToolSpec]) -> Result<Response> {
            self.snapshots
                .lock()
                .unwrap()
                .push((messages.to_vec(), tools.to_vec()));
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| anyhow::anyhow!("mock 响应不足"))
        }
    }

    struct NamedTool(&'static str);

    #[async_trait]
    impl Tool for NamedTool {
        fn name(&self) -> &str {
            self.0
        }

        fn description(&self) -> &str {
            "测试工具"
        }

        fn parameters(&self) -> Value {
            json!({"type": "object"})
        }

        fn is_read_only(&self) -> bool {
            matches!(self.0, "read_file" | "recall_memory")
        }

        async fn execute(&self, _args: Value) -> Result<String> {
            Ok("ok".to_owned())
        }
    }

    fn config() -> ContextConfig {
        ContextConfig {
            token_budget: 1_000_000,
            recent_messages: 100,
            mild_compression_percent: 60,
            strong_compression_percent: 85,
            summary_chunk_tokens: 100_000,
        }
    }

    fn available_tools() -> ToolRegistry {
        let mut tools = ToolRegistry::new();
        for name in [
            "read_file",
            "exec",
            "recall_memory",
            "write_file",
            "edit_file",
        ] {
            tools.register(NamedTool(name));
        }
        tools
    }

    #[tokio::test]
    async fn uses_fresh_history_and_default_limited_tools() {
        let provider = Arc::new(MockProvider {
            responses: Mutex::new(VecDeque::from([Response::Text("调研结论".to_owned())])),
            snapshots: Mutex::new(Vec::new()),
        });
        let tool = SubAgentTool::new(
            provider.clone(),
            available_tools(),
            std::env::current_dir().unwrap(),
            config(),
        );

        let result = tool
            .execute(json!({"task": "只调查模块关系"}))
            .await
            .unwrap();

        assert_eq!(result, "调研结论");
        let snapshots = provider.snapshots.lock().unwrap();
        assert_eq!(snapshots.len(), 1);
        let (messages, specs) = &snapshots[0];
        assert_eq!(messages[0].role, Role::System);
        assert!(
            messages[0]
                .content
                .as_deref()
                .unwrap()
                .contains("全新且独立")
        );
        assert!(messages.iter().any(|message: &Message| {
            message.role == Role::User && message.content.as_deref() == Some("只调查模块关系")
        }));
        let names = specs
            .iter()
            .map(|spec: &ToolSpec| spec.name.as_str())
            .collect::<Vec<&str>>();
        assert_eq!(names, ["exec", "read_file", "recall_memory"]);
        assert!(!names.contains(&"write_file"));
        assert!(!names.contains(&"sub_agent"));
    }

    #[tokio::test]
    async fn enforces_round_limit_without_persisting_a_session() {
        let provider = Arc::new(MockProvider {
            responses: Mutex::new(VecDeque::from([Response::ToolCalls(Vec::new())])),
            snapshots: Mutex::new(Vec::new()),
        });
        let tool = SubAgentTool::new(
            provider,
            available_tools(),
            std::env::current_dir().unwrap(),
            config(),
        )
        .with_max_rounds(1);

        let error = tool.execute(json!({"task": "不要结束"})).await.unwrap_err();

        assert!(error.to_string().contains("最大轮次 1"));
    }

    struct ParallelProvider {
        barrier: Arc<Barrier>,
        snapshots: Mutex<Vec<(Vec<Message>, Vec<ToolSpec>)>>,
    }

    #[async_trait]
    impl Provider for ParallelProvider {
        async fn chat(&self, messages: &[Message], tools: &[ToolSpec]) -> Result<Response> {
            self.snapshots
                .lock()
                .unwrap()
                .push((messages.to_vec(), tools.to_vec()));
            self.barrier.wait().await;
            let task = messages
                .iter()
                .find(|message| message.role == Role::User)
                .and_then(|message| message.content.as_deref())
                .unwrap();
            Ok(Response::Text(format!("{task}完成")))
        }
    }

    #[tokio::test]
    async fn runs_independent_read_only_tasks_concurrently_in_input_order() {
        let provider = Arc::new(ParallelProvider {
            barrier: Arc::new(Barrier::new(2)),
            snapshots: Mutex::new(Vec::new()),
        });
        let tool = SubAgentTool::new(
            provider.clone(),
            available_tools(),
            std::env::current_dir().unwrap(),
            config(),
        );

        let output = tokio::time::timeout(
            Duration::from_secs(2),
            tool.execute(json!({"tasks": ["调查甲", "调查乙"]})),
        )
        .await
        .expect("两个子任务必须并发运行")
        .unwrap();
        let value: Value = serde_json::from_str(&output).unwrap();
        assert_eq!(value["results"][0]["result"], "调查甲完成");
        assert_eq!(value["results"][1]["result"], "调查乙完成");
        let snapshots = provider.snapshots.lock().unwrap();
        assert_eq!(snapshots.len(), 2);
        for (messages, specs) in snapshots.iter() {
            assert_eq!(
                messages
                    .iter()
                    .filter(|message| message.role == Role::User)
                    .count(),
                1
            );
            assert_eq!(
                specs
                    .iter()
                    .map(|spec| spec.name.as_str())
                    .collect::<Vec<_>>(),
                ["read_file", "recall_memory"]
            );
        }
    }

    #[tokio::test]
    async fn rejects_parallel_side_effects_and_recursive_delegation() {
        let provider = Arc::new(MockProvider {
            responses: Mutex::new(VecDeque::new()),
            snapshots: Mutex::new(Vec::new()),
        });
        let tool = SubAgentTool::new(
            provider,
            available_tools(),
            std::env::current_dir().unwrap(),
            config(),
        );

        let error = tool
            .execute(json!({"tasks": ["甲", "乙"], "tools": ["read_file", "exec"]}))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("只能使用只读工具"));
        let error = tool
            .execute(json!({"task": "甲", "tools": ["sub_agent"]}))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("不能递归"));
        assert!(
            tool.execute(json!({"task": "甲", "tasks": ["乙"]}))
                .await
                .is_err()
        );
    }

    struct WaitingProvider(Arc<Barrier>);

    #[async_trait]
    impl Provider for WaitingProvider {
        async fn chat(&self, _messages: &[Message], _tools: &[ToolSpec]) -> Result<Response> {
            self.0.wait().await;
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn parent_cancellation_stops_child_model_request() {
        let barrier = Arc::new(Barrier::new(2));
        let tool = SubAgentTool::new(
            Arc::new(WaitingProvider(barrier.clone())),
            available_tools(),
            std::env::current_dir().unwrap(),
            config(),
        );
        let cancellation = CancellationToken::new();
        let child_cancellation = cancellation.clone();
        let running = tokio::spawn(async move {
            tool.execute_rich_with_cancellation(json!({"task": "一直等待"}), &child_cancellation)
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), barrier.wait())
            .await
            .unwrap();
        cancellation.cancel();
        let error = tokio::time::timeout(Duration::from_secs(2), running)
            .await
            .expect("取消后子 Agent 应及时退出")
            .unwrap()
            .err()
            .expect("取消应返回错误");
        assert!(error.to_string().contains("请求已取消"));
    }
}
