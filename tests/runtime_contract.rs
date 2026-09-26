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

async fn mock_delegation_ollama(
    completed: Vec<usize>,
) -> (String, tokio::task::JoinHandle<()>, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let count = Arc::new(AtomicUsize::new(0));
    let requests = count.clone();
    let completed = Arc::new(completed);
    let task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let served = requests.fetch_add(1, Ordering::SeqCst) + 1;
            let completed = completed.clone();
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
                if !completed.contains(&served) {
                    std::future::pending::<()>().await;
                }
                let body = "{\"message\":{\"role\":\"assistant\",\"content\":\"子任务完成\"},\"done\":false}\n{\"done\":true,\"prompt_eval_count\":2,\"eval_count\":2}\n";
                stream.write_all(format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/x-ndjson\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()).as_bytes()).await.unwrap();
            });
        }
    });
    (url, task, count)
}

async fn mock_tool_spawn_ollama() -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let count = Arc::new(AtomicUsize::new(0));
    let task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let served = count.fetch_add(1, Ordering::SeqCst) + 1;
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
                let body = if served == 1 {
                    "{\"message\":{\"role\":\"assistant\",\"content\":\"\",\"tool_calls\":[{\"function\":{\"name\":\"spawn_subagent\",\"arguments\":{\"task\":\"核对一个问题\"}}}]},\"done\":false}\n{\"done\":true}\n"
                } else {
                    "{\"message\":{\"role\":\"assistant\",\"content\":\"完成\"},\"done\":false}\n{\"done\":true}\n"
                };
                stream.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/x-ndjson\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()).as_bytes()).await.unwrap();
            });
        }
    });
    (url, task)
}

async fn mock_child_outside_read_ollama(
    release: Arc<tokio::sync::Notify>,
) -> (String, tokio::task::JoinHandle<()>, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let count = Arc::new(AtomicUsize::new(0));
    let requests = count.clone();
    let task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let served = requests.fetch_add(1, Ordering::SeqCst) + 1;
            let release = release.clone();
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
                    std::future::pending::<()>().await;
                }
                let body = if served == 2 {
                    release.notified().await;
                    "{\"message\":{\"role\":\"assistant\",\"content\":\"\",\"tool_calls\":[{\"function\":{\"name\":\"read_file\",\"arguments\":{\"path\":\"/etc/hosts\"}}}]},\"done\":false}\n{\"done\":true}\n"
                } else {
                    "{\"message\":{\"role\":\"assistant\",\"content\":\"已核对\"},\"done\":false}\n{\"done\":true}\n"
                };
                stream.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/x-ndjson\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()).as_bytes()).await.unwrap();
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

async fn send_and_disconnect(socket: &Path, id: &str, session: &str, message: &str) {
    let mut stream = UnixStream::connect(socket).await.unwrap();
    stream.write_all(format!("{}\n", json!({"jsonrpc":"2.0","id":id,"method":"chat.send","params":{"session_id":session,"message":message}})).as_bytes()).await.unwrap();
}

async fn entry_views(
    workspace: &Path,
    runtime_dir: &Path,
    url: &str,
    run_id: &str,
    expected_content: &str,
    subagent_parent: Option<&str>,
) {
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
        .write_all(
            format!(
                "/run {run_id}\n{}{}/exit\n",
                subagent_parent.map_or(String::new(), |parent| format!(
                    "/subagent {parent} {run_id}\n"
                )),
                ""
            )
            .as_bytes(),
        )
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
            && cli_text.contains(expected_content)
            && subagent_parent.is_none_or(|_| cli_text.contains(&format!("child_run_id={run_id}"))),
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
            && acp_text.contains(expected_content),
        "{acp_text}"
    );
    if let Some(parent) = subagent_parent {
        acp_in
            .write_all(
                format!(
                    "{}\n",
                    json!({"jsonrpc":"2.0","id":4,
            "method":"session/prompt","params":{"sessionId":acp_session,
            "prompt":[{"type":"text","text":format!("/subagent {parent} {run_id}")}]}})
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let mut child_text = String::new();
        loop {
            let line = acp_out.next_line().await.unwrap().unwrap();
            child_text.push_str(&line);
            if serde_json::from_str::<Value>(&line).unwrap()["id"] == 4 {
                break;
            }
        }
        assert!(
            child_text.contains(&format!("child_run_id={run_id}")),
            "{child_text}"
        );
    }
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
    assert_eq!(web_result["result"]["content"], expected_content);
    if let Some(parent) = subagent_parent {
        socket
            .send(WsMessage::Text(
                json!({"jsonrpc":"2.0","id":"web-child",
            "method":"read_subagent","params":{"parent_run_id":parent,"child_run_id":run_id}})
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
        let child_view = loop {
            let text = socket.next().await.unwrap().unwrap().into_text().unwrap();
            let value: Value = serde_json::from_str(&text).unwrap();
            if value["id"] == "web-child" {
                break value;
            }
        };
        assert_eq!(
            child_view["result"]["child"]["child_run_id"], run_id,
            "{child_view}"
        );
    }
    web.kill().await.unwrap();
    web.wait().await.unwrap();
}

#[tokio::test]
async fn committed_terminal_survives_real_daemon_restart_and_uncertain_run_is_not_replayed() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let workspace = temp_workspace();
        let runtime_dir = PathBuf::from(format!(
            "/tmp/ma-runtime-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
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
        let provider_ledger = rpc(
            &socket,
            "provider-ledger",
            "run.provider_attempts",
            json!({"run_id":complete_id}),
        )
        .await;
        assert_eq!(
            provider_ledger["result"]["route"]["candidates"][0]["model"],
            "mock"
        );
        assert_eq!(
            provider_ledger["result"]["attempts"][0]["status"],
            "succeeded"
        );
        assert_eq!(provider_ledger["result"]["usage"]["input_tokens"], 2);
        assert_eq!(provider_ledger["result"]["usage"]["output_tokens"], 2);
        assert!(!provider_ledger.to_string().contains("api_key"));
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
        entry_views(&workspace, &runtime_dir, &url, &complete_id, "已完成", None).await;
        restarted.kill().await.unwrap();
        restarted.wait().await.unwrap();
        server.abort();
        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(runtime_dir);
    })
    .await
    .expect("restart contract timed out");
}

#[tokio::test]
async fn daemon_queue_is_durable_and_exact_cancel_does_not_hit_running_run() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let workspace = temp_workspace();
        let runtime_dir = PathBuf::from(format!("/tmp/ma-queue-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        let (url, server, provider_requests) = mock_ollama().await;
        let (mut child, socket) = daemon(&workspace, &runtime_dir, &url).await;
        let session = rpc(&socket, "new", "session.new", json!({})).await["result"]["session_id"].as_str().unwrap().to_owned();
        let _ = chat(&socket, "first", &session, "finish", true).await;
        let (running, _) = chat(&socket, "running", &session, "wait", false).await;
        for _ in 0..100 {
            if provider_requests.load(Ordering::SeqCst) >= 2 { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        send_and_disconnect(&socket, "queued", &session, "queued input").await;
        let queued = loop {
            let listed = rpc(&socket, "list", "queue.list", json!({"session_id":session})).await;
            if let Some(item) = listed["result"]["items"].as_array().and_then(|items| items.first()) { break item.clone(); }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let queued_run = queued["run_id"].as_str().unwrap().to_owned();
        assert_eq!(queued["position"], 1);
        assert_eq!(provider_requests.load(Ordering::SeqCst), 2);
        let duplicate = rpc(&socket, "queued", "chat.send", json!({"session_id":session,"message":"queued input"})).await;
        assert_eq!(duplicate["result"]["run_id"], queued_run);
        let rejected = rpc(&socket, "reject", "chat.send", json!({"session_id":session,"message":"no","admission_mode":"reject_if_busy"})).await;
        assert!(rejected.get("error").is_some());
        let cancelled = rpc(&socket, "cancel", "agent.cancel", json!({"session_id":session,"run_id":queued_run})).await;
        assert_eq!(cancelled["result"]["cancelled"], true);
        let read = rpc(&socket, "read", "run.read", json!({"run_id":queued_run})).await;
        assert_eq!(read["result"]["status"], "cancelled");
        let active = rpc(&socket, "active", "run.read", json!({"run_id":running})).await;
        assert_eq!(active["result"]["status"], "running");
        assert_eq!(provider_requests.load(Ordering::SeqCst), 2);
        let port_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = port_listener.local_addr().unwrap().port();
        drop(port_listener);
        let mut web = Command::new(env!("CARGO_BIN_EXE_my-agent"))
            .arg("--workspace").arg(&workspace).arg("serve").arg("--bind").arg(format!("127.0.0.1:{port}"))
            .env("MY_AGENT_RUNTIME_DIR", &runtime_dir)
            .env("MY_AGENT_CONFIG", workspace.join("empty-config.json"))
            .env("API_TYPE", "ollama").env("MODEL_NAME", "mock").env("OPENAI_BASE_URL", &url)
            .stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null()).spawn().unwrap();
        let mut web_socket = None;
        for _ in 0..100 {
            if let Ok((connected, _)) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/ws")).await {
                web_socket = Some(connected); break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let mut web_socket = web_socket.unwrap();
        web_socket.send(WsMessage::Text(json!({"type":"connect","workspace":workspace}).to_string().into())).await.unwrap();
        let _ = web_socket.next().await.unwrap().unwrap();
        web_socket.send(WsMessage::Text(json!({"jsonrpc":"2.0","id":"web-survivor","method":"chat.send",
            "params":{"session_id":session,"message":"resume after crash"}}).to_string().into())).await.unwrap();
        let survivor = loop {
            let listed = rpc(&socket, "list-survivor", "queue.list", json!({"session_id":session})).await;
            if let Some(item) = listed["result"]["items"].as_array().and_then(|items| items.first()) { break item["run_id"].as_str().unwrap().to_owned(); }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        drop(web_socket);
        web.kill().await.unwrap(); web.wait().await.unwrap();
        child.kill().await.unwrap(); child.wait().await.unwrap();
        let (mut restarted, socket) = daemon(&workspace, &runtime_dir, &url).await;
        for _ in 0..100 {
            let run = rpc(&socket, "survivor-read", "run.read", json!({"run_id":survivor})).await;
            if run["result"]["status"] == "running" { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(rpc(&socket, "survivor-read-final", "run.read", json!({"run_id":survivor})).await["result"]["status"], "running");
        assert_eq!(rpc(&socket, "old-read", "run.read", json!({"run_id":running})).await["result"]["status"], "unknown_after_restart");
        assert_eq!(provider_requests.load(Ordering::SeqCst), 3);
        restarted.kill().await.unwrap(); restarted.wait().await.unwrap(); server.abort();
        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(runtime_dir);
    }).await.expect("queue contract timed out");
}

#[tokio::test]
async fn durable_subagent_spawn_wait_restart_and_scoped_cancel() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let workspace = temp_workspace();
        let runtime_dir = PathBuf::from(format!("/tmp/ma-child-{}-{}", std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)));
        let (url, server, provider_requests) = mock_delegation_ollama(vec![2]).await;
        let (mut daemon_process, socket) = daemon(&workspace, &runtime_dir, &url).await;
        let session = rpc(&socket, "new", "session.new", json!({})).await["result"]["session_id"]
            .as_str().unwrap().to_owned();
        let (parent, _) = chat(&socket, "parent", &session, "等待", false).await;
        for _ in 0..100 {
            if provider_requests.load(Ordering::SeqCst) >= 1 { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let spawned = rpc(&socket, "spawn", "spawn_subagent", json!({
            "parent_session_id":session,"parent_run_id":parent,"spawn_key":"first",
            "task":"调查一个问题"})).await;
        assert!(spawned.get("result").is_some(), "{spawned}");
        let child = spawned["result"]["child"]["child_run_id"].as_str().unwrap().to_owned();
        let child_session = spawned["result"]["child"]["child_session_id"].as_str().unwrap().to_owned();
        assert_ne!(child_session, session);
        let timed_out = rpc(&socket, "wait-short", "wait_subagents", json!({
            "parent_run_id":parent,"child_run_ids":[child],"timeout_ms":0})).await;
        assert_eq!(timed_out["result"]["timed_out"], true, "{timed_out}");
        let checkpoint = rpc(&socket, "checkpoint", "wait_subagents", json!({
            "parent_run_id":parent,"child_run_ids":[child],"after_seq":0,"timeout_ms":1000})).await;
        assert_eq!(checkpoint["result"]["checkpoints"][0]["child_run_id"], child);
        let completed = rpc(&socket, "wait", "wait_subagents", json!({
            "parent_run_id":parent,"child_run_ids":[child],"timeout_ms":5000})).await;
        assert_eq!(completed["result"]["children"][0]["status"], "completed", "{completed}");
        assert_eq!(completed["result"]["children"][0]["content"], "子任务完成");
        assert_eq!(rpc(&socket, "parent-read", "run.read", json!({"run_id":parent})).await["result"]["status"], "running");
        let duplicate = rpc(&socket, "spawn-retry", "spawn_subagent", json!({
            "parent_session_id":session,"parent_run_id":parent,"spawn_key":"first",
            "task":"调查一个问题"})).await;
        assert_eq!(duplicate["result"]["child"]["child_run_id"], child, "{duplicate}");
        let stolen = rpc(&socket, "wrong-scope", "cancel_subagent", json!({
            "parent_run_id":"run-does-not-own","child_run_id":child})).await;
        assert!(stolen.get("error").is_some());
        let elevated_tool = rpc(&socket, "elevate-tool", "spawn_subagent", json!({
            "parent_session_id":session,"parent_run_id":parent,"spawn_key":"bad-tool",
            "task":"越权","tools":["exec"]})).await;
        assert!(elevated_tool.get("error").is_some());
        let elevated_tokens = rpc(&socket, "elevate-budget", "spawn_subagent", json!({
            "parent_session_id":session,"parent_run_id":parent,"spawn_key":"bad-budget",
            "task":"越权","max_tokens":500000})).await;
        assert!(elevated_tokens.get("error").is_some());
        let reserved = rpc(&socket, "reserve", "subagent.result.reserve", json!({
            "parent_run_id":parent,"child_run_id":child,"owner":"reader-a","revision":0})).await;
        assert_eq!(reserved["result"]["child"]["result_state"], "reserved", "{reserved}");
        let reserved_again = rpc(&socket, "reserve-again", "subagent.result.reserve", json!({
            "parent_run_id":parent,"child_run_id":child,"owner":"reader-a","revision":0})).await;
        assert_eq!(reserved_again["result"]["child"]["revision"], 1);
        let conflicted = rpc(&socket, "reserve-conflict", "subagent.result.reserve", json!({
            "parent_run_id":parent,"child_run_id":child,"owner":"reader-b","revision":1})).await;
        assert!(conflicted.get("error").is_some());
        let released = rpc(&socket, "release", "subagent.result.release", json!({
            "parent_run_id":parent,"child_run_id":child,"owner":"reader-a","revision":1})).await;
        assert_eq!(released["result"]["child"]["result_state"], "unconsumed");
        let reserved_b = rpc(&socket, "reserve-b", "subagent.result.reserve", json!({
            "parent_run_id":parent,"child_run_id":child,"owner":"reader-b","revision":2})).await;
        assert_eq!(reserved_b["result"]["child"]["revision"], 3);
        let delivered = rpc(&socket, "commit", "subagent.result.commit", json!({
            "parent_run_id":parent,"child_run_id":child,"owner":"reader-b","revision":3})).await;
        assert_eq!(delivered["result"]["child"]["result_state"], "delivered");
        let second = rpc(&socket, "spawn-second", "spawn_subagent", json!({
            "parent_session_id":session,"parent_run_id":parent,"spawn_key":"second",
            "task":"持续调查"})).await;
        assert!(second.get("result").is_some(), "{second}");
        let second_id = second["result"]["child"]["child_run_id"].as_str().unwrap().to_owned();
        for _ in 0..100 {
            if provider_requests.load(Ordering::SeqCst) >= 3 { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let other_session = rpc(&socket, "new-other", "session.new", json!({})).await
            ["result"]["session_id"].as_str().unwrap().to_owned();
        let (unrelated, _) = chat(&socket, "unrelated", &other_session, "另一个根任务", false).await;
        for _ in 0..100 {
            if provider_requests.load(Ordering::SeqCst) >= 4 { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let cancelled = rpc(&socket, "cancel-root", "agent.cancel", json!({
            "session_id":session,"run_id":parent})).await;
        assert_eq!(cancelled["result"]["cancelled"], true, "{cancelled}");
        for _ in 0..100 {
            let status = rpc(&socket, "second-read", "run.read", json!({"run_id":second_id})).await;
            if status["result"]["status"] == "cancelled" { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(rpc(&socket, "second-final", "run.read", json!({"run_id":second_id})).await["result"]["status"], "cancelled");
        assert_eq!(rpc(&socket, "unrelated-read", "run.read", json!({"run_id":unrelated})).await["result"]["status"], "running");
        let late_spawn = rpc(&socket, "late-spawn", "spawn_subagent", json!({
            "parent_session_id":session,"parent_run_id":parent,"spawn_key":"after-cancel",
            "task":"不能再创建"})).await;
        assert!(late_spawn.get("error").is_some(), "{late_spawn}");
        daemon_process.kill().await.unwrap(); daemon_process.wait().await.unwrap();
        let (mut restarted, socket) = daemon(&workspace, &runtime_dir, &url).await;
        let listed = rpc(&socket, "list", "list_subagents", json!({"root_run_id":parent})).await;
        assert_eq!(listed["result"]["children"][0]["child_run_id"], child);
        assert_eq!(listed["result"]["children"][0]["status"], "completed");
        assert_eq!(listed["result"]["children"][0]["result_state"], "delivered");
        assert_eq!(rpc(&socket, "second-restarted", "run.read", json!({"run_id":second_id})).await["result"]["status"], "cancelled");
        assert_eq!(rpc(&socket, "unrelated-unknown", "run.read", json!({"run_id":unrelated})).await["result"]["status"], "unknown_after_restart");
        assert_eq!(provider_requests.load(Ordering::SeqCst), 4);
        entry_views(&workspace, &runtime_dir, &url, &child, "子任务完成", Some(&parent)).await;
        restarted.kill().await.unwrap(); restarted.wait().await.unwrap(); server.abort();
        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(runtime_dir);
    }).await.expect("subagent contract timed out");
}

#[tokio::test]
async fn running_subagent_becomes_unknown_after_daemon_crash_without_replay() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let workspace = temp_workspace();
        let runtime_dir = PathBuf::from(format!(
            "/tmp/ma-child-crash-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let (url, server, requests) = mock_delegation_ollama(vec![2]).await;
        let (mut daemon_process, socket) = daemon(&workspace, &runtime_dir, &url).await;
        let session = rpc(&socket, "new", "session.new", json!({})).await["result"]["session_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let (parent, _) = chat(&socket, "parent", &session, "等待", false).await;
        for _ in 0..100 {
            if requests.load(Ordering::SeqCst) >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let first = rpc(
            &socket,
            "first",
            "spawn_subagent",
            json!({
            "parent_session_id":session,"parent_run_id":parent,"spawn_key":"first",
            "task":"完成"}),
        )
        .await;
        let first_id = first["result"]["child"]["child_run_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let _ = rpc(
            &socket,
            "wait-first",
            "wait_subagents",
            json!({
            "parent_run_id":parent,"child_run_ids":[first_id],"timeout_ms":5000}),
        )
        .await;
        let second = rpc(
            &socket,
            "second",
            "spawn_subagent",
            json!({
            "parent_session_id":session,"parent_run_id":parent,"spawn_key":"second",
            "task":"保持运行"}),
        )
        .await;
        let second_id = second["result"]["child"]["child_run_id"]
            .as_str()
            .unwrap()
            .to_owned();
        for _ in 0..100 {
            if requests.load(Ordering::SeqCst) >= 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(requests.load(Ordering::SeqCst), 3);
        daemon_process.kill().await.unwrap();
        daemon_process.wait().await.unwrap();
        let (mut restarted, socket) = daemon(&workspace, &runtime_dir, &url).await;
        let unknown = rpc(
            &socket,
            "read",
            "read_subagent",
            json!({
            "parent_run_id":parent,"child_run_id":second_id}),
        )
        .await;
        assert_eq!(
            unknown["result"]["child"]["status"], "unknown_after_restart",
            "{unknown}"
        );
        assert_eq!(unknown["result"]["child"]["result_state"], "unconsumed");
        assert_eq!(requests.load(Ordering::SeqCst), 3);
        restarted.kill().await.unwrap();
        restarted.wait().await.unwrap();
        server.abort();
        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(runtime_dir);
    })
    .await
    .expect("subagent crash contract timed out");
}

#[tokio::test]
async fn model_spawn_tool_uses_durable_delegation_and_receipt() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let workspace = temp_workspace();
        let runtime_dir = PathBuf::from(format!(
            "/tmp/ma-child-tool-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let (url, server) = mock_tool_spawn_ollama().await;
        let (mut daemon_process, socket) = daemon(&workspace, &runtime_dir, &url).await;
        let session = rpc(&socket, "new", "session.new", json!({})).await["result"]["session_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let (parent, response) = chat(&socket, "parent", &session, "派出子任务", true).await;
        assert!(response.unwrap().get("result").is_some());
        let tools = rpc(&socket, "tools", "run.tools", json!({"run_id":parent})).await;
        assert_eq!(
            tools["result"]["receipts"][0]["name"], "spawn_subagent",
            "{tools}"
        );
        let listed = rpc(
            &socket,
            "list",
            "list_subagents",
            json!({"root_run_id":parent}),
        )
        .await;
        assert_eq!(
            listed["result"]["children"].as_array().unwrap().len(),
            1,
            "{listed}"
        );
        let child = listed["result"]["children"][0]["child_run_id"]
            .as_str()
            .unwrap();
        let waited = rpc(
            &socket,
            "wait",
            "wait_subagents",
            json!({
            "parent_run_id":parent,"child_run_ids":[child],"timeout_ms":5000}),
        )
        .await;
        assert_eq!(
            waited["result"]["children"][0]["status"], "completed",
            "{waited}"
        );
        daemon_process.kill().await.unwrap();
        daemon_process.wait().await.unwrap();
        server.abort();
        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(runtime_dir);
    })
    .await
    .expect("model delegation tool contract timed out");
}

#[tokio::test]
async fn concurrent_children_finish_and_remain_addressable_by_stable_ids() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let workspace = temp_workspace();
        let runtime_dir = PathBuf::from(format!(
            "/tmp/ma-child-parallel-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let (url, server, requests) = mock_delegation_ollama(vec![2, 3]).await;
        let (mut daemon_process, socket) = daemon(&workspace, &runtime_dir, &url).await;
        let session = rpc(&socket, "new", "session.new", json!({})).await["result"]["session_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let (parent, _) = chat(&socket, "parent", &session, "等待", false).await;
        for _ in 0..100 {
            if requests.load(Ordering::SeqCst) >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let first = rpc(
            &socket,
            "first",
            "spawn_subagent",
            json!({
            "parent_session_id":session,"parent_run_id":parent,"spawn_key":"a",
            "task":"任务 A"}),
        )
        .await;
        let second = rpc(
            &socket,
            "second",
            "spawn_subagent",
            json!({
            "parent_session_id":session,"parent_run_id":parent,"spawn_key":"b",
            "task":"任务 B"}),
        )
        .await;
        let a = first["result"]["child"]["child_run_id"].as_str().unwrap();
        let b = second["result"]["child"]["child_run_id"].as_str().unwrap();
        assert_ne!(a, b);
        let waited = rpc(
            &socket,
            "wait",
            "wait_subagents",
            json!({
            "parent_run_id":parent,"child_run_ids":[a,b],"timeout_ms":5000}),
        )
        .await;
        assert_eq!(waited["result"]["children"][0]["child_run_id"], a);
        assert_eq!(waited["result"]["children"][1]["child_run_id"], b);
        for id in [a, b] {
            for _ in 0..100 {
                let child = rpc(
                    &socket,
                    "read",
                    "read_subagent",
                    json!({
                    "parent_run_id":parent,"child_run_id":id}),
                )
                .await;
                if child["result"]["child"]["status"] == "completed" {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let child = rpc(
                &socket,
                "read-final",
                "read_subagent",
                json!({
                "parent_run_id":parent,"child_run_id":id}),
            )
            .await;
            assert_eq!(child["result"]["child"]["content"], "子任务完成");
        }
        assert_eq!(requests.load(Ordering::SeqCst), 3);
        daemon_process.kill().await.unwrap();
        daemon_process.wait().await.unwrap();
        server.abort();
        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(runtime_dir);
    })
    .await
    .expect("parallel delegation contract timed out");
}

#[tokio::test]
async fn child_read_permission_stays_frozen_after_parent_mode_changes() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let workspace = temp_workspace();
        let runtime_dir = PathBuf::from(format!(
            "/tmp/ma-child-mode-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let release = Arc::new(tokio::sync::Notify::new());
        let (url, server, requests) = mock_child_outside_read_ollama(release.clone()).await;
        let (mut daemon_process, socket) = daemon(&workspace, &runtime_dir, &url).await;
        let session = rpc(&socket, "new", "session.new", json!({})).await["result"]["session_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let (parent, _) = chat(&socket, "parent", &session, "等待", false).await;
        for _ in 0..100 {
            if requests.load(Ordering::SeqCst) >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let spawned = rpc(
            &socket,
            "spawn",
            "spawn_subagent",
            json!({
            "parent_session_id":session,"parent_run_id":parent,"spawn_key":"frozen",
            "task":"核对文件"}),
        )
        .await;
        assert!(spawned.get("result").is_some(), "{spawned}");
        let child = spawned["result"]["child_run_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let changed = rpc(&socket, "mode", "permissions.set", json!({"mode":"full"})).await;
        assert!(changed.get("result").is_some(), "{changed}");
        release.notify_one();
        let completed = rpc(
            &socket,
            "wait",
            "wait_subagents",
            json!({
            "parent_run_id":parent,"child_run_ids":[child],"timeout_ms":5000}),
        )
        .await;
        assert_eq!(
            completed["result"]["children"][0]["status"], "completed",
            "{completed}"
        );
        let receipts = rpc(&socket, "tools", "run.tools", json!({"run_id":child})).await;
        assert_eq!(
            receipts["result"]["receipts"][0]["name"], "read_file",
            "{receipts}"
        );
        assert_eq!(
            receipts["result"]["receipts"][0]["outcome"], "tool_error",
            "{receipts}"
        );
        daemon_process.kill().await.unwrap();
        daemon_process.wait().await.unwrap();
        server.abort();
        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(runtime_dir);
    })
    .await
    .expect("child permission contract timed out");
}
