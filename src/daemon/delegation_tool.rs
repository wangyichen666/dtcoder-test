//! Model-facing projection of the daemon's durable delegation control plane.
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde_json::{Value, json};

use super::DaemonState;
use crate::loop_engine::current_tool_owner;
use crate::storage::{EventSeq, RunId};
use crate::tools::{Tool, ToolCancellation, ToolOutput};

#[derive(Clone)]
pub struct DelegationTool {
    name: &'static str,
    daemon: Arc<OnceLock<Weak<DaemonState>>>,
}

impl DelegationTool {
    pub fn new(name: &'static str, daemon: Arc<OnceLock<Weak<DaemonState>>>) -> Self {
        Self { name, daemon }
    }

    async fn run(
        &self,
        args: Value,
        cancellation: Option<&dyn ToolCancellation>,
    ) -> Result<String> {
        let daemon = self
            .daemon
            .get()
            .and_then(Weak::upgrade)
            .context("委派 daemon 尚未就绪")?;
        let (parent_run_id, call_id) =
            current_tool_owner().context("委派工具只能在持久 run 中调用")?;
        if self.name == "list_subagents" {
            let root = daemon
                .run_store
                .delegation(&parent_run_id)?
                .map(|child| child.root_run_id)
                .unwrap_or(parent_run_id);
            return Ok(json!({"children": daemon.run_store.list_delegations(&root)?}).to_string());
        }
        if self.name == "wait_subagents" {
            let ids = args
                .get("child_run_ids")
                .and_then(Value::as_array)
                .context("需要 child_run_ids 数组")?
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .map(|id| RunId(id.to_owned()))
                        .context("child_run_ids 中只能包含字符串")
                })
                .collect::<Result<Vec<_>>>()?;
            let timeout_ms = args
                .get("timeout_ms")
                .and_then(Value::as_u64)
                .unwrap_or(30_000);
            let after_seq = args.get("after_seq").and_then(Value::as_u64).map(EventSeq);
            let result = daemon
                .wait_subagents_for_tool(parent_run_id, ids, timeout_ms, after_seq)
                .await
                .map_err(|(_, message)| anyhow::anyhow!(message))?;
            return Ok(result.to_string());
        }
        if self.name == "cancel_subagent" {
            let id = args
                .get("child_run_id")
                .and_then(Value::as_str)
                .context("需要 child_run_id")?;
            let result = daemon
                .cancel_subagent_for_tool(parent_run_id, RunId(id.to_owned()))
                .await
                .map_err(|(_, message)| anyhow::anyhow!(message))?;
            return Ok(result.to_string());
        }
        let tasks = match (
            args.get("task").and_then(Value::as_str),
            args.get("tasks").and_then(Value::as_array),
        ) {
            (Some(task), None) if !task.trim().is_empty() => vec![task.to_owned()],
            (None, Some(tasks))
                if self.name == "sub_agent" && !tasks.is_empty() && tasks.len() <= 4 =>
            {
                tasks
                    .iter()
                    .map(|task| {
                        task.as_str()
                            .map(str::to_owned)
                            .context("tasks 中只能包含字符串")
                    })
                    .collect::<Result<Vec<_>>>()?
            }
            _ => bail!("需要非空 task；兼容 sub_agent 可传 1 到 4 个 tasks"),
        };
        if tasks.iter().any(|task| task.trim().is_empty()) {
            bail!("子任务不能为空");
        }
        let tools = args
            .get("tools")
            .map(|value| serde_json::from_value::<Vec<String>>(value.clone()))
            .transpose()
            .context("tools 必须是字符串数组")?;
        let mut children = Vec::with_capacity(tasks.len());
        for (index, task) in tasks.into_iter().enumerate() {
            let child = daemon
                .spawn_subagent_for_tool(
                    parent_run_id.clone(),
                    format!("{call_id}:{index}"),
                    task.clone(),
                    tools.clone(),
                )
                .await
                .map_err(|(_, message)| anyhow::anyhow!(message))?;
            children.push((task, child));
        }
        if self.name == "spawn_subagent" {
            return Ok(
                json!({"children": children.iter().map(|(_, child)| child).collect::<Vec<_>>()})
                    .to_string(),
            );
        }
        // The old tool name remains a synchronous compatibility wrapper. Its
        // children use the same SQLite runs and can be read after disconnect.
        let mut results = Vec::with_capacity(children.len());
        for (task, child) in children {
            let child_run_id: RunId = child["child_run_id"]
                .as_str()
                .map(|id| RunId(id.to_owned()))
                .context("缺少 child_run_id")?;
            loop {
                if cancellation.is_some_and(ToolCancellation::is_cancelled) {
                    bail!("父 run 已取消；委派子树由 daemon 收敛");
                }
                let record = daemon
                    .run_store
                    .delegation(&child_run_id)?
                    .context("子 Agent 记录消失")?;
                if record.status.terminal() {
                    results.push(json!({"task": task, "child_run_id": child_run_id,
                        "success": record.status == crate::storage::RunStatus::Completed,
                        "result": record.content, "status": record.status,
                        "error": record.error_message}));
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        Ok(json!({"results": results}).to_string())
    }
}

#[async_trait]
impl Tool for DelegationTool {
    fn name(&self) -> &str {
        self.name
    }

    fn description(&self) -> &str {
        match self.name {
            "spawn_subagent" => "异步派出持久子 Agent，立即返回稳定 child run/session ID",
            "wait_subagents" => "按稳定 child run ID 等待子 Agent；等待超时不改变运行状态",
            "cancel_subagent" => "取消当前委派作用域中的指定 child run",
            "list_subagents" => "列出当前 root run 下的子 Agent",
            _ => "兼容工具：派出持久子 Agent 并等待结果；新任务优先使用 spawn_subagent",
        }
    }

    fn parameters(&self) -> Value {
        if self.name == "wait_subagents" {
            return json!({"type":"object","properties":{
                "child_run_ids":{"type":"array","items":{"type":"string"}},
                "timeout_ms":{"type":"integer"},"after_seq":{"type":"integer"}},"required":["child_run_ids"],
                "additionalProperties":false});
        }
        if self.name == "cancel_subagent" {
            return json!({"type":"object","properties":{
                "child_run_id":{"type":"string"}},"required":["child_run_id"],
                "additionalProperties":false});
        }
        if self.name == "list_subagents" {
            return json!({"type":"object","properties":{},"additionalProperties":false});
        }
        json!({"type":"object","properties":{
            "task":{"type":"string"},
            "tasks":{"type":"array","items":{"type":"string"}},
            "tools":{"type":"array","items":{"type":"string"}}
        },"additionalProperties":false})
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
