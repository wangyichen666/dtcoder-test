//! Black-box daemon contract: a process restart must preserve committed facts.
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, UnixStream};
use tokio::process::{Child, Command};
use tokio_tungstenite::tungstenite::Message as WsMessage;

static NEXT: AtomicU64 = AtomicU64::new(0);

fn temp_workspace() -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "my-agent-restart-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&path).unwrap();
    std::fs::canonicalize(path).unwrap()
}

async fn mock_ollama() -> (String, tokio::task::JoinHandle<()>, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let count = Arc::new(AtomicUsize::new(0));
    let requests = count.clone();
    let task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let served = requests.fetch_add(1, Ordering::SeqCst) + 1;
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buffer = [0u8; 8192];
                loop {
                    let read = stream.read(&mut buffer).await.unwrap();
                    if read == 0 {
                        return;
                    }
                    request.extend_from_slice(&buffer[..read]);
                    if request.windows(4).any(|part| part == b"\r\n\r\n") {
                        break;
                    }
                }
                if served == 1 {
                    let body = "{\"message\":{\"role\":\"assistant\",\"content\":\"已完成\"},\"done\":false}\n{\"done\":true,\"prompt_eval_count\":2,\"eval_count\":2}\n";
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/x-ndjson\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    stream.write_all(response.as_bytes()).await.unwrap();
                } else {
                    std::future::pending::<()>().await;
                }
            });
        }
    });
    (url, task, count)
}

async fn daemon(workspace: &Path, runtime_dir: &Path, url: &str) -> (Child, PathBuf) {
    let child = Command::new(env!("CARGO_BIN_EXE_my-agent"))
        .arg("--workspace")
        .arg(workspace)
        .arg("daemon")
        .env("MY_AGENT_RUNTIME_DIR", runtime_dir)
        .env("MY_AGENT_CONFIG", workspace.join("empty-config.json"))
        .env("API_TYPE", "ollama")
        .env("MODEL_NAME", "mock")
        .env("OPENAI_BASE_URL", url)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    for _ in 0..100 {
        if let Ok(entries) = std::fs::read_dir(runtime_dir) {
            for entry in entries.flatten() {
                let socket = entry.path().join("daemon.sock");
                if socket.exists() && UnixStream::connect(&socket).await.is_ok() {
                    return (child, socket);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("daemon socket did not start");
}

async fn rpc(socket: &Path, id: &str, method: &str, params: Value) -> Value {
    let stream = UnixStream::connect(socket).await.unwrap();
    let (reader, mut writer) = stream.into_split();
    writer
        .write_all(
            format!(
                "{}\n",
                json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let mut lines = BufReader::new(reader).lines();
    loop {
        let line = lines.next_line().await.unwrap().unwrap();
        let frame: Value = serde_json::from_str(&line).unwrap();
        if frame.get("id").is_some() {
            return frame;
        }
    }
}

async fn chat(
    socket: &Path,
    id: &str,
    session: &str,
    message: &str,
    wait_terminal: bool,
) -> (String, Option<Value>) {
    let stream = UnixStream::connect(socket).await.unwrap();
    let (reader, mut writer) = stream.into_split();
    writer.write_all(format!("{}\n", json!({"jsonrpc":"2.0","id":id,"method":"chat.send","params":{"session_id":session,"message":message}})).as_bytes()).await.unwrap();
    let mut lines = BufReader::new(reader).lines();
    let mut run_id = None;
    loop {
        let line = lines.next_line().await.unwrap().unwrap();
        let frame: Value = serde_json::from_str(&line).unwrap();
        if let Some(id) = frame.get("run_id").and_then(Value::as_str) {
            run_id = Some(id.to_owned());
        }
        if frame.get("id").is_some() {
            return (
                run_id
                    .or_else(|| frame["result"]["run_id"].as_str().map(str::to_owned))
                    .unwrap(),
                Some(frame),
            );
        }
        if !wait_terminal && frame["event"] == "turn_started" {
            return (run_id.unwrap(), None);
        }
    }
}

async fn entry_views(workspace: &Path, runtime_dir: &Path, url: &str, run_id: &str) {
    let mut cli = Command::new(env!("CARGO_BIN_EXE_my-agent"))
        .arg("--workspace")
        .arg(workspace)
        .arg("chat")
        .env("MY_AGENT_RUNTIME_DIR", runtime_dir)
        .env("MY_AGENT_CONFIG", workspace.join("empty-config.json"))
        .env("API_TYPE", "ollama")
        .env("MODEL_NAME", "mock")
        .env("OPENAI_BASE_URL", url)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    cli.stdin
        .take()
        .unwrap()
        .write_all(format!("/run {run_id}\n/exit\n").as_bytes())
        .await
        .unwrap();
    let output = cli.wait_with_output().await.unwrap();
    assert!(
        output.status.success(),
        "CLI: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let cli_text = String::from_utf8_lossy(&output.stdout);
    assert!(
        cli_text.contains(&format!("run_id={run_id}"))
            && cli_text.contains("status=completed")
            && cli_text.contains("已完成"),
        "{cli_text}"
    );

    let mut acp = Command::new(env!("CARGO_BIN_EXE_my-agent"))
        .arg("--workspace")
        .arg(workspace)
        .arg("editor")
        .env("MY_AGENT_RUNTIME_DIR", runtime_dir)
        .env("MY_AGENT_CONFIG", workspace.join("empty-config.json"))
        .env("API_TYPE", "ollama")
        .env("MODEL_NAME", "mock")
        .env("OPENAI_BASE_URL", url)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut acp_in = acp.stdin.take().unwrap();
    let mut acp_out = BufReader::new(acp.stdout.take().unwrap()).lines();
    acp_in
        .write_all(
            format!(
                "{}\n",
                json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}})
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let init: Value = serde_json::from_str(&acp_out.next_line().await.unwrap().unwrap()).unwrap();
    assert!(init.get("result").is_some(), "ACP initialize: {init}");
    acp_in.write_all(format!("{}\n", json!({"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":workspace,"mcpServers":[]}})).as_bytes()).await.unwrap();
    let new_session: Value =
        serde_json::from_str(&acp_out.next_line().await.unwrap().unwrap()).unwrap();
    let acp_session = new_session["result"]["sessionId"]
        .as_str()
        .unwrap_or_else(|| panic!("ACP session id: {new_session}"));
    acp_in.write_all(format!("{}\n", json!({"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":acp_session,"prompt":[{"type":"text","text":format!("/run {run_id}")}]}})).as_bytes()).await.unwrap();
    let mut acp_text = String::new();
    loop {
        let line = acp_out
            .next_line()
            .await
            .unwrap()
            .expect("ACP prompt response");
        acp_text.push_str(&line);
        let value: Value = serde_json::from_str(&line).unwrap();
        if value["id"] == 3 {
            assert!(value.get("result").is_some(), "ACP prompt: {value}");
            break;
        }
    }
    assert!(
        acp_text.contains(&format!("run_id={run_id}"))
            && acp_text.contains("status=completed")
            && acp_text.contains("已完成"),
        "{acp_text}"
    );
    acp.kill().await.unwrap();
    acp.wait().await.unwrap();

    let port_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = port_listener.local_addr().unwrap().port();
    drop(port_listener);
    let mut web = Command::new(env!("CARGO_BIN_EXE_my-agent"))
        .arg("--workspace")
        .arg(workspace)
        .arg("serve")
        .arg("--bind")
        .arg(format!("127.0.0.1:{port}"))
        .env("MY_AGENT_RUNTIME_DIR", runtime_dir)
        .env("MY_AGENT_CONFIG", workspace.join("empty-config.json"))
        .env("API_TYPE", "ollama")
        .env("MODEL_NAME", "mock")
        .env("OPENAI_BASE_URL", url)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let mut socket = None;
    for _ in 0..100 {
        if let Ok((connected, _)) =
            tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/ws")).await
        {
            socket = Some(connected);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let mut socket = socket.expect("WebSocket start");
    socket
        .send(WsMessage::Text(
            json!({"type":"connect","workspace":workspace})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let connected = socket.next().await.unwrap().unwrap().into_text().unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&connected).unwrap()["type"],
        "connected"
    );
    socket
        .send(WsMessage::Text(
            json!({"jsonrpc":"2.0","id":"web-read","method":"run.read","params":{"run_id":run_id}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let web_result = loop {
        let text = socket.next().await.unwrap().unwrap().into_text().unwrap();
        let value: Value = serde_json::from_str(&text).unwrap();
        if value["id"] == "web-read" {
            break value;
        }
    };
    assert_eq!(web_result["result"]["run_id"], run_id);
    assert_eq!(web_result["result"]["status"], "completed");
    assert_eq!(web_result["result"]["content"], "已完成");
    web.kill().await.unwrap();
    web.wait().await.unwrap();
}

#[tokio::test]
async fn committed_terminal_survives_real_daemon_restart_and_uncertain_run_is_not_replayed() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let workspace = temp_workspace();
        let runtime_dir = PathBuf::from(format!("/tmp/ma-runtime-{}", std::process::id()));
        let (url, server, provider_requests) = mock_ollama().await;
        let (mut child, socket) = daemon(&workspace, &runtime_dir, &url).await;
        let session = rpc(&socket, "new", "session.new", json!({})).await["result"]["session_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let (complete_id, response) = chat(&socket, "complete", &session, "say done", true).await;
        assert_eq!(response.unwrap()["result"]["content"], "已完成");
        let (uncertain_id, _) = chat(&socket, "uncertain", &session, "hold request", false).await;
        for _ in 0..100 {
            if provider_requests.load(Ordering::SeqCst) >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(provider_requests.load(Ordering::SeqCst), 2);
        child.kill().await.unwrap();
        child.wait().await.unwrap();
        let (mut restarted, socket) = daemon(&workspace, &runtime_dir, &url).await;
        let complete = rpc(
            &socket,
            "read-complete",
            "run.read",
            json!({"run_id":complete_id}),
        )
        .await;
        assert_eq!(complete["result"]["status"], "completed");
        assert_eq!(complete["result"]["content"], "已完成");
        let unknown = rpc(
            &socket,
            "read-unknown",
            "run.read",
            json!({"run_id":uncertain_id}),
        )
        .await;
        assert_eq!(unknown["result"]["status"], "unknown_after_restart");
        assert_eq!(
            provider_requests.load(Ordering::SeqCst),
            2,
            "未知 run 不能自动重放 Provider 请求"
        );
        let events = rpc(
            &socket,
            "events",
            "run.events",
            json!({"run_id":complete_id,"after_seq":0}),
        )
        .await;
        assert!(
            events["result"]["events"]
                .as_array()
                .unwrap()
                .iter()
                .any(|event| event["event"] == "terminal")
        );
        let duplicate = rpc(
            &socket,
            "complete",
            "chat.send",
            json!({"session_id":session,"message":"say done"}),
        )
        .await;
        assert_eq!(duplicate["result"]["run_id"], complete_id);
        entry_views(&workspace, &runtime_dir, &url, &complete_id).await;
        restarted.kill().await.unwrap();
        restarted.wait().await.unwrap();
        server.abort();
        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(runtime_dir);
    })
    .await
    .expect("restart contract timed out");
}
