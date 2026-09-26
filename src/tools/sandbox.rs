use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;

use super::ToolCancellation;

const MAX_OUTPUT_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct ResourceId(pub u64);

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxBackend {
    Native,
    Docker,
}

pub struct ExecRequest {
    pub command: String,
    pub shell: PathBuf,
    pub cwd: PathBuf,
    pub timeout: Duration,
    pub requested: SandboxBackend,
}

pub struct ExecResult {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
    pub requested: SandboxBackend,
    pub effective: SandboxBackend,
}

#[derive(Default)]
pub struct ResourceManager {
    next_id: AtomicU64,
    processes: Mutex<HashMap<ResourceId, u32>>,
}

impl ResourceManager {
    pub(crate) fn register_process(self: &Arc<Self>, pid: u32) -> ResourceRegistration {
        let id = ResourceId(self.next_id.fetch_add(1, Ordering::Relaxed) + 1);
        self.processes.lock().unwrap().insert(id, pid);
        ResourceRegistration {
            manager: self.clone(),
            id,
            pid: Some(pid),
        }
    }

    #[allow(dead_code)]
    pub fn list(&self) -> Vec<ResourceId> {
        self.processes.lock().unwrap().keys().copied().collect()
    }

    #[allow(dead_code)]
    pub fn stop(&self, id: ResourceId) -> bool {
        let pid = self.processes.lock().unwrap().get(&id).copied();
        if let Some(pid) = pid {
            terminate_process_group(pid);
            true
        } else {
            false
        }
    }

    pub fn stop_all(&self) {
        let pids = self
            .processes
            .lock()
            .unwrap()
            .values()
            .copied()
            .collect::<Vec<_>>();
        for pid in pids {
            terminate_process_group(pid);
        }
    }
}

pub(crate) struct ResourceRegistration {
    manager: Arc<ResourceManager>,
    id: ResourceId,
    pid: Option<u32>,
}

impl ResourceRegistration {
    #[allow(dead_code)]
    pub fn id(&self) -> ResourceId {
        self.id
    }

    fn disarm(&mut self) {
        self.pid = None;
    }
}

impl Drop for ResourceRegistration {
    fn drop(&mut self) {
        if let Some(pid) = self.pid {
            terminate_process_group(pid);
        }
        self.manager.processes.lock().unwrap().remove(&self.id);
    }
}

#[async_trait]
pub trait Sandbox: Send + Sync {
    async fn execute(
        &self,
        request: ExecRequest,
        cancellation: &dyn ToolCancellation,
    ) -> Result<ExecResult>;
    fn stop_all(&self);
}

pub struct NativeSandbox {
    resources: Arc<ResourceManager>,
}

impl NativeSandbox {
    pub fn new() -> Self {
        Self {
            resources: Arc::new(ResourceManager::default()),
        }
    }
}

#[async_trait]
impl Sandbox for NativeSandbox {
    async fn execute(
        &self,
        request: ExecRequest,
        cancellation: &dyn ToolCancellation,
    ) -> Result<ExecResult> {
        if !matches!(request.requested, SandboxBackend::Native) {
            bail!("Docker sandbox 尚未实现");
        }
        if cancellation.is_cancelled() {
            bail!("命令执行已取消");
        }
        let mut command = Command::new(&request.shell);
        command
            .arg("-c")
            .arg(&request.command)
            .current_dir(&request.cwd)
            .env_clear()
            .env("PWD", &request.cwd)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for key in [
            "PATH",
            "HOME",
            "TMPDIR",
            "LANG",
            "LC_ALL",
            "TERM",
            "CARGO_HOME",
            "RUSTUP_HOME",
        ] {
            if let Some(value) = std::env::var_os(key) {
                command.env(key, value);
            }
        }
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command
            .spawn()
            .with_context(|| format!("执行命令失败: {}", request.command))?;
        let mut guard = child.id().map(|pid| self.resources.register_process(pid));
        let stdout = child.stdout.take().context("stdout 管道不存在")?;
        let stderr = child.stderr.take().context("stderr 管道不存在")?;
        let out_task = tokio::spawn(capture_bounded(stdout));
        let err_task = tokio::spawn(capture_bounded(stderr));
        let status = tokio::select! {
            result = child.wait() => result.context("等待命令结束失败")?,
            _ = cancellation.cancelled() => {
                if let Some(pid) = child.id() { terminate_process_group(pid); }
                let _ = child.wait().await;
                if let Some(guard) = guard.as_mut() { guard.disarm(); }
                let _ = out_task.await;
                let _ = err_task.await;
                bail!("命令执行已取消: {}", request.command);
            },
            _ = tokio::time::sleep(request.timeout) => {
                if let Some(pid) = child.id() { terminate_process_group(pid); }
                let _ = child.wait().await;
                if let Some(guard) = guard.as_mut() { guard.disarm(); }
                let _ = out_task.await;
                let _ = err_task.await;
                bail!("命令执行超时（{} 秒）: {}", request.timeout.as_secs(), request.command);
            }
        };
        if let Some(guard) = guard.as_mut() {
            guard.disarm();
        }
        let stdout = out_task.await.context("读取 stdout 任务失败")??;
        let stderr = err_task.await.context("读取 stderr 任务失败")??;
        Ok(ExecResult {
            exit_code: status.code().unwrap_or(-1),
            stdout,
            stderr,
            requested: request.requested,
            effective: SandboxBackend::Native,
        })
    }

    fn stop_all(&self) {
        self.resources.stop_all();
    }
}

async fn capture_bounded(mut reader: impl AsyncRead + Unpin) -> Result<String> {
    let mut kept = Vec::with_capacity(MAX_OUTPUT_BYTES);
    let mut total = 0u64;
    let mut chunk = [0u8; 8192];
    loop {
        let count = reader.read(&mut chunk).await?;
        if count == 0 {
            break;
        }
        total += count as u64;
        let remaining = MAX_OUTPUT_BYTES.saturating_sub(kept.len());
        kept.extend_from_slice(&chunk[..count.min(remaining)]);
    }
    let mut output = String::from_utf8_lossy(&kept).into_owned();
    if total > MAX_OUTPUT_BYTES as u64 {
        output.push_str(&format!("\n...[输出已截断，原始大小 {total} 字节]"));
    }
    Ok(output)
}

#[cfg(unix)]
fn terminate_process_group(pid: u32) {
    if let Ok(group) = i32::try_from(pid) {
        unsafe {
            libc::kill(-group, libc::SIGKILL);
        }
    }
}

#[cfg(not(unix))]
fn terminate_process_group(_pid: u32) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_registration_can_be_queried_and_removed() {
        let manager = Arc::new(ResourceManager::default());
        let mut registration = manager.register_process(u32::MAX);
        assert_eq!(manager.list(), vec![registration.id]);
        assert!(manager.stop(registration.id));
        registration.disarm();
        drop(registration);
        assert!(manager.list().is_empty());
    }
}
