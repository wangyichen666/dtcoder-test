use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use super::{
    ExecRequest, NativeSandbox, Sandbox, SandboxBackend, Tool, ToolCancellation, ToolOutput,
};
use crate::safety::SafetyPolicy;

const DEFAULT_EXEC_TIMEOUT_SECS: u64 = 300;

pub struct ExecTool {
    safety: Arc<SafetyPolicy>,
    sandbox: Arc<dyn Sandbox>,
}

impl ExecTool {
    pub fn new(safety: Arc<SafetyPolicy>) -> Self {
        Self {
            safety,
            sandbox: Arc::new(NativeSandbox::new()),
        }
    }
}

#[derive(Deserialize)]
struct ExecArgs {
    command: String,
    sandbox: Option<SandboxBackend>,
}

#[async_trait]
impl Tool for ExecTool {
    fn name(&self) -> &str {
        "exec"
    }
    fn description(&self) -> &str {
        "在工作区执行 shell 命令，返回退出码、stdout、stderr 和实际 sandbox；Native 不提供强隔离"
    }
    fn parameters(&self) -> Value {
        json!({"type":"object","properties":{"command":{"type":"string","description":"要执行的 shell 命令"},"sandbox":{"type":"string","enum":["native","docker"],"description":"请求的执行后端；docker 尚未实现"}},"required":["command"]})
    }
    fn stop_resources(&self) {
        self.sandbox.stop_all();
    }
    async fn execute(&self, args: Value) -> Result<String> {
        self.execute_command(args, &NeverCancelled).await
    }
    async fn execute_rich_with_cancellation(
        &self,
        args: Value,
        cancellation: &dyn ToolCancellation,
    ) -> Result<ToolOutput> {
        self.execute_command(args, cancellation)
            .await
            .map(ToolOutput::text)
    }
}

impl ExecTool {
    async fn execute_command(
        &self,
        args: Value,
        cancellation: &dyn ToolCancellation,
    ) -> Result<String> {
        let args: ExecArgs = serde_json::from_value(args).context("exec 参数无效")?;
        self.safety.authorize_command(&args.command).await?;
        if cancellation.is_cancelled() {
            bail!("命令执行已取消");
        }
        let request = ExecRequest {
            command: args.command,
            shell: PathBuf::from("/bin/sh"),
            cwd: self.safety.workspace().to_path_buf(),
            timeout: execution_timeout(),
            requested: args.sandbox.unwrap_or(SandboxBackend::Native),
        };
        let result = self.sandbox.execute(request, cancellation).await?;
        Ok(format!(
            "exit_code: {}\nsandbox_requested: {:?}\nsandbox_effective: {:?}\nstdout:\n{}\nstderr:\n{}",
            result.exit_code, result.requested, result.effective, result.stdout, result.stderr
        ))
    }
}

struct NeverCancelled;

#[async_trait]
impl ToolCancellation for NeverCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
    async fn cancelled(&self) {
        std::future::pending::<()>().await;
    }
}

fn execution_timeout() -> Duration {
    std::env::var("MY_AGENT_EXEC_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|seconds| *seconds > 0)
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(DEFAULT_EXEC_TIMEOUT_SECS))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::loop_engine::CancellationToken;
    use crate::safety::Approval;

    struct AllowApproval;

    #[async_trait]
    impl Approval for AllowApproval {
        async fn request(&self, _prompt: &str) -> Result<bool> {
            Ok(true)
        }
    }

    #[tokio::test]
    async fn executes_shell_command() {
        let safety = Arc::new(
            SafetyPolicy::new(std::env::current_dir().unwrap(), Arc::new(AllowApproval)).unwrap(),
        );
        let output = ExecTool::new(safety)
            .execute(json!({"command": "printf hello"}))
            .await
            .unwrap();
        assert!(output.contains("exit_code: 0"));
        assert!(output.contains("hello"));
    }

    #[tokio::test]
    async fn refuses_catastrophic_command() {
        let safety = Arc::new(
            SafetyPolicy::new(std::env::current_dir().unwrap(), Arc::new(AllowApproval)).unwrap(),
        );
        let error = ExecTool::new(safety)
            .execute(json!({"command": "rm -rf /"}))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("已拦截"));
    }

    #[tokio::test]
    async fn docker_request_fails_closed() {
        let safety = Arc::new(
            SafetyPolicy::new(std::env::current_dir().unwrap(), Arc::new(AllowApproval)).unwrap(),
        );
        let error = ExecTool::new(safety)
            .execute(json!({"command":"printf hello", "sandbox":"docker"}))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("尚未实现"));
    }

    #[tokio::test]
    async fn cancellation_terminates_a_long_running_command() {
        let safety = Arc::new(
            SafetyPolicy::new(std::env::current_dir().unwrap(), Arc::new(AllowApproval)).unwrap(),
        );
        let tool = ExecTool::new(safety);
        let cancellation = CancellationToken::new();
        let ready = std::env::temp_dir().join(format!("agent-exec-ready-{}", std::process::id()));
        let marker =
            std::env::temp_dir().join(format!("agent-exec-grandchild-{}", std::process::id()));
        let _ = std::fs::remove_file(&ready);
        let _ = std::fs::remove_file(&marker);
        let command = format!(
            "echo ready > {}; sh -c 'sleep 1; touch {}' & wait",
            ready.display(),
            marker.display()
        );
        let task_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            tool.execute_rich_with_cancellation(json!({"command": command}), &task_cancellation)
                .await
        });
        for _ in 0..100 {
            if ready.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(ready.exists(), "测试命令必须已经启动");
        tokio::time::sleep(Duration::from_millis(100)).await;
        cancellation.cancel();
        let result = match tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("取消长命令不应超过 2 秒")
            .unwrap()
        {
            Ok(_) => panic!("命令取消后不应成功"),
            Err(error) => error,
        };
        assert!(result.to_string().contains("已取消"));
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert!(!marker.exists(), "取消后孙进程仍在运行");
        let _ = std::fs::remove_file(ready);
    }

    #[tokio::test]
    async fn output_flood_is_bounded_while_pipes_are_drained() {
        let safety = Arc::new(
            SafetyPolicy::new(std::env::current_dir().unwrap(), Arc::new(AllowApproval)).unwrap(),
        );
        let output = tokio::time::timeout(
            Duration::from_secs(5),
            ExecTool::new(safety).execute(json!({
                "command": "yes x | head -c 200000; yes y | head -c 200000 >&2"
            })),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(output.contains("exit_code: 0"));
        assert_eq!(output.matches("输出已截断").count(), 2);
        assert!(output.len() < 140_000);
    }
}
