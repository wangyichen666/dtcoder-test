use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::time::Instant;
use tokio::io::AsyncReadExt;
use tokio::sync::{Mutex, mpsc};
use tracing::{Instrument, info, info_span};

use super::approval::PendingApprovalInfo;
use super::protocol::{
    EventKind, JsonRpcRequest, JsonRpcResponse, MAX_FRAME_BYTES, RequestId, ServerFrame,
};
use super::{ActiveKey, ActiveRequest, ActiveRequestUpdate, DaemonState, SessionRuntime};
use crate::config::ProfileSummary;
use crate::cron::ScheduleSpec;
use crate::loop_engine::{AgentEvent, CancellationToken};
use crate::provider::ProviderProfile;
use crate::safety::SafetyMode;
use crate::session::{SessionStatus, SessionTraceRecord};
use crate::slash::{SlashAction, SlashParse, SlashRegistry, SlashResponse};
use crate::storage::{
    Admission, AdmissionMode, EventSeq, InteractionId, RunId, RunStatus, RuntimeError, SessionId,
    StoredEvent,
};

const INVALID_PARAMS: i64 = -32602;
const METHOD_NOT_FOUND: i64 = -32601;
const INTERNAL_ERROR: i64 = -32603;
const REQUEST_CANCELLED: i64 = -32800;
const REQUEST_CONFLICT: i64 = -32001;
const WEB_PAGE_MAX_BYTES: usize = MAX_FRAME_BYTES - 256 * 1024;
const WEB_PAGE_MAX_ITEM_BYTES: usize = 256 * 1024;
const WEB_PAGE_DEFAULT_LIMIT: usize = 80;
const WEB_PAGE_MAX_LIMIT: usize = 200;

#[derive(Deserialize)]
struct ChatSendParams {
    message: String,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    admission_mode: Option<String>,
}

#[derive(Deserialize)]
struct ApprovalRespondParams {
    #[serde(alias = "interaction_id")]
    approval_id: String,
    #[serde(default)]
    approved: Option<bool>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    owner_run_id: Option<RunId>,
    #[serde(default)]
    revision: Option<i64>,
}

#[derive(Deserialize)]
struct InteractionReadParams {
    interaction_id: InteractionId,
}

#[derive(Deserialize)]
struct CancelParams {
    #[serde(default)]
    request_id: Option<RequestId>,
    #[serde(default)]
    run_id: Option<RunId>,
    #[serde(default)]
    session_id: Option<String>,
}

#[derive(Deserialize)]
struct SubscribeParams {
    request_id: RequestId,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    after_seq: Option<EventSeq>,
}

#[derive(Deserialize)]
struct RunReadParams {
    run_id: RunId,
}

#[derive(Deserialize)]
struct RunReconcileParams {
    session_id: SessionId,
    run_id: RunId,
    expected_last_seq: EventSeq,
    status: RunStatus,
    #[serde(default)]
    content: Option<String>,
    evidence: String,
}

#[derive(Deserialize)]
struct QueueItemParams {
    session_id: String,
    run_id: RunId,
}

#[derive(Deserialize)]
struct RunEventsParams {
    run_id: RunId,
    #[serde(default)]
    after_seq: Option<EventSeq>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Deserialize)]
struct SessionResumeParams {
    session_id: String,
}

#[derive(Deserialize)]
struct SessionPageParams {
    session_id: String,
    #[serde(default)]
    offset: usize,
    #[serde(default = "default_web_page_limit")]
    limit: usize,
}

#[derive(Deserialize)]
struct PermissionModeParams {
    mode: String,
}

#[derive(Deserialize)]
struct ModelUseParams {
    profile_id: String,
}

#[derive(Deserialize)]
struct ModelSaveParams {
    profile: ProviderProfile,
    #[serde(default)]
    activate: bool,
}

fn default_web_page_limit() -> usize {
    WEB_PAGE_DEFAULT_LIMIT
}

#[derive(Deserialize, Default)]
struct SessionSelectorParams {
    #[serde(default)]
    session_id: Option<String>,
}

#[derive(Deserialize)]
struct SlashExecuteParams {
    line: String,
    #[serde(default)]
    session_id: Option<String>,
}

impl DaemonState {
    pub async fn handle_request(
        self: Arc<Self>,
        request: JsonRpcRequest,
        frames: mpsc::UnboundedSender<ServerFrame>,
    ) {
        match request.method.as_str() {
            "chat.send" => self.handle_chat_send(request, frames).await,
            "session.load" => {
                let result = match parse_params::<SessionSelectorParams>(&request.params) {
                    Ok(params) => self.session_load(params.session_id.as_deref()).await,
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "session.load_page" => {
                let result = match parse_params::<SessionPageParams>(&request.params) {
                    Ok(params) => self.session_load_page(params).await,
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "session.list" => {
                let result = self.session_list().await;
                send_result(&frames, request.id, result);
            }
            "session.new" => {
                let result = self.session_new().await;
                send_result(&frames, request.id, result);
            }
            "session.resume" => {
                let result = match parse_params::<SessionResumeParams>(&request.params) {
                    Ok(params) => self.session_resume(&params.session_id).await,
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "session.trace" => {
                let result = match parse_params::<SessionResumeParams>(&request.params) {
                    Ok(params) => self.session_trace(&params.session_id).await,
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "session.trace_page" => {
                let result = match parse_params::<SessionPageParams>(&request.params) {
                    Ok(params) => self.session_trace_page(params).await,
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "permissions.get" => {
                send_result(&frames, request.id, self.permissions_get());
            }
            "permissions.set" => {
                let result = match parse_params::<PermissionModeParams>(&request.params) {
                    Ok(params) => self.permissions_set(&params.mode),
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "models.list" => send_result(&frames, request.id, self.models_list()),
            "models.use" => {
                let result = match parse_params::<ModelUseParams>(&request.params) {
                    Ok(params) => self.models_use(&params.profile_id).await,
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "models.save" => {
                let result = match parse_params::<ModelSaveParams>(&request.params) {
                    Ok(params) => self.models_save(params).await,
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "approval.respond" | "interaction.respond" | "interaction.reject" => {
                let rejected = request.method == "interaction.reject";
                let result = match parse_params::<ApprovalRespondParams>(&request.params) {
                    Ok(params) => self.resolve_interaction(params, rejected).await,
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "interaction.read" => {
                let result = parse_params::<InteractionReadParams>(&request.params)
                    .map_err(|error| (INVALID_PARAMS, error))
                    .and_then(|params| {
                        self.run_store
                            .read_interaction(&params.interaction_id)
                            .map(|interaction| json!({"interaction": interaction}))
                            .map_err(|error| (INTERNAL_ERROR, error.to_string()))
                    });
                send_result(&frames, request.id, result);
            }
            "interaction.list" => {
                let result = parse_params::<SessionResumeParams>(&request.params)
                    .map_err(|error| (INVALID_PARAMS, error))
                    .and_then(|params| {
                        self.run_store
                            .list_interactions(&params.session_id)
                            .map(|interactions| json!({"interactions": interactions}))
                            .map_err(|error| (INTERNAL_ERROR, error.to_string()))
                    });
                send_result(&frames, request.id, result);
            }
            "agent.cancel" => {
                let result = match parse_params::<CancelParams>(&request.params) {
                    Ok(params) => self.cancel(params).await,
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "queue.list" => {
                let result = parse_params::<SessionResumeParams>(&request.params)
                    .map_err(|error| (INVALID_PARAMS, error))
                    .and_then(|params| {
                        self.run_store
                            .queued_messages(&params.session_id)
                            .map(|items| json!({"items": items}))
                            .map_err(|error| (INTERNAL_ERROR, error.to_string()))
                    });
                send_result(&frames, request.id, result);
            }
            "queue.read" => {
                let result = parse_params::<QueueItemParams>(&request.params)
                    .map_err(|error| (INVALID_PARAMS, error))
                    .and_then(|params| {
                        self.run_store
                            .queued_message(&params.session_id, &params.run_id)
                            .map(|item| json!({"item": item}))
                            .map_err(|error| (INTERNAL_ERROR, error.to_string()))
                    });
                send_result(&frames, request.id, result);
            }
            "queue.remove" => {
                let result = match parse_params::<QueueItemParams>(&request.params) {
                    Ok(params) => {
                        self.cancel(CancelParams {
                            request_id: None,
                            run_id: Some(params.run_id),
                            session_id: Some(params.session_id),
                        })
                        .await
                    }
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "agent.subscribe" => self.handle_subscribe(request, frames).await,
            "run.read" => {
                let result = parse_params::<RunReadParams>(&request.params)
                    .map_err(|error| (INVALID_PARAMS, error))
                    .and_then(|params| {
                        self.run_store
                            .read_run(&params.run_id)
                            .map_err(|error| (INTERNAL_ERROR, error.to_string()))
                            .and_then(|run| {
                                run.map(|run| json!(run))
                                    .ok_or((INVALID_PARAMS, "run 不存在".into()))
                            })
                    });
                send_result(&frames, request.id, result);
            }
            "run.tools" => {
                let result = parse_params::<RunReadParams>(&request.params)
                    .map_err(|error| (INVALID_PARAMS, error))
                    .and_then(|params| {
                        self.run_store
                            .tool_receipts(&params.run_id)
                            .map(|receipts| json!({"receipts": receipts}))
                            .map_err(|error| (INTERNAL_ERROR, error.to_string()))
                    });
                send_result(&frames, request.id, result);
            }
            "run.audit" => {
                let result = match parse_params::<RunReadParams>(&request.params) {
                    Ok(params) => self.audit_run(&params.run_id).await,
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "run.reconcile" => {
                let result = match parse_params::<RunReconcileParams>(&request.params) {
                    Ok(params) => {
                        let active = self
                            .active
                            .lock()
                            .await
                            .values()
                            .any(|item| item.run_id == params.run_id);
                        if active {
                            Err((REQUEST_CONFLICT, "run 仍有活动执行体".into()))
                        } else {
                            self.run_store
                                .reconcile_unknown(
                                    &params.run_id,
                                    &params.session_id,
                                    params.expected_last_seq,
                                    params.status,
                                    params.content.as_deref(),
                                    &params.evidence,
                                )
                                .map(|run| json!({"run": run}))
                                .map_err(|error| (REQUEST_CONFLICT, error.to_string()))
                        }
                    }
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "run.events" => {
                let result = parse_params::<RunEventsParams>(&request.params)
                    .map_err(|error| (INVALID_PARAMS, error))
                    .and_then(|params| {
                        self.run_store
                            .events_after(
                                &params.run_id,
                                params.after_seq.unwrap_or(EventSeq(0)),
                                params.limit.unwrap_or(200),
                            )
                            .map(|events| json!({"events": events}))
                            .map_err(|error| (INTERNAL_ERROR, error.to_string()))
                    });
                send_result(&frames, request.id, result);
            }
            "slash.execute" => {
                let result = match parse_params::<SlashExecuteParams>(&request.params) {
                    Ok(params) => {
                        self.execute_slash(&params.line, params.session_id.as_deref())
                            .await
                    }
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "daemon.stop" => {
                send_result(
                    &frames,
                    request.id,
                    Ok(
                        json!({"stopping": true, "active_turns_finish_gracefully": false,
                        "active_turns_cancelled": true}),
                    ),
                );
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                self.shutdown.cancel();
            }
            _ => {
                let _ = frames.send(ServerFrame::Response(JsonRpcResponse::failure(
                    request.id,
                    METHOD_NOT_FOUND,
                    format!("未知 RPC 方法: {}", request.method),
                )));
            }
        }
    }

    async fn handle_chat_send(
        self: Arc<Self>,
        request: JsonRpcRequest,
        frames: mpsc::UnboundedSender<ServerFrame>,
    ) {
        if self.shutdown.is_cancelled() {
            send_result(
                &frames,
                request.id,
                Err((REQUEST_CONFLICT, "daemon 正在关闭，停止新准入".into())),
            );
            return;
        }
        let params = match parse_params::<ChatSendParams>(&request.params) {
            Ok(params) if !params.message.trim().is_empty() => params,
            Ok(_) => {
                send_result(
                    &frames,
                    request.id,
                    Err((INVALID_PARAMS, "message 不能为空".to_owned())),
                );
                return;
            }
            Err(error) => {
                send_result(&frames, request.id, Err((INVALID_PARAMS, error)));
                return;
            }
        };

        let session = match self.session_runtime(params.session_id.as_deref()).await {
            Ok(session) => session,
            Err(error) => {
                send_result(&frames, request.id, Err(error));
                return;
            }
        };
        let session_id = session.id.clone();
        let mode = match params.admission_mode.as_deref().unwrap_or("queue") {
            "queue" => AdmissionMode::Queue,
            "reject_if_busy" => AdmissionMode::RejectIfBusy,
            _ => {
                send_result(
                    &frames,
                    request.id,
                    Err((INVALID_PARAMS, "不支持的 admission_mode".into())),
                );
                return;
            }
        };
        let admitted = match self.run_store.admit_with_mode(
            SessionId(session_id.clone()),
            request.id.clone(),
            &params.message,
            mode,
        ) {
            Ok(admitted) => admitted,
            Err(error) => {
                let code = if matches!(error, RuntimeError::Protocol(_)) {
                    REQUEST_CONFLICT
                } else {
                    INTERNAL_ERROR
                };
                send_result(&frames, request.id, Err((code, error.to_string())));
                return;
            }
        };
        let run_record = match admitted {
            Admission::New(run) => run,
            Admission::Existing(run) => {
                if run.status == RunStatus::Completed {
                    send_result(
                        &frames,
                        request.id,
                        Ok(
                            json!({"content": run.content, "run_id": run.run_id, "turn_id": run.turn_id}),
                        ),
                    );
                    return;
                } else if run.status != RunStatus::Queued {
                    send_result(
                        &frames,
                        request.id,
                        Ok(json!({"run_id": run.run_id, "turn_id": run.turn_id,
                            "status": run.status, "duplicate": true,
                            "error_code": run.error_code, "error_message": run.error_message})),
                    );
                    return;
                } else {
                    run
                }
            }
        };
        let started_at = Instant::now();
        info!(
            session_id = %session_id,
            request_id = ?request.id,
            "chat 请求开始"
        );
        let active_key = ActiveKey {
            session_id: session_id.clone(),
            request_id: request.id.clone(),
        };
        let cancellation = CancellationToken::new();
        let mut active = self.active.lock().await;
        if active.contains_key(&active_key) {
            drop(active);
            send_result(
                &frames,
                request.id,
                Ok(
                    json!({"run_id": run_record.run_id, "turn_id": run_record.turn_id, "status": "queued_or_running", "duplicate": true}),
                ),
            );
            return;
        }
        active.insert(
            active_key.clone(),
            ActiveRequest::new(cancellation.clone(), run_record.run_id.clone()),
        );
        drop(active);
        self.queue_notify.notify_waiters();
        loop {
            let notified = self.queue_notify.notified();
            if cancellation.is_cancelled() {
                self.active.lock().await.remove(&active_key);
                send_result(
                    &frames,
                    request.id,
                    Err((REQUEST_CANCELLED, "请求已取消".into())),
                );
                return;
            }
            match self.run_store.try_start_queued(&run_record.run_id) {
                Ok(true) => break,
                Ok(false) => {}
                Err(error) => {
                    self.active.lock().await.remove(&active_key);
                    send_result(
                        &frames,
                        request.id,
                        Err((INTERNAL_ERROR, error.to_string())),
                    );
                    return;
                }
            }
            tokio::select! {
                _ = notified => {},
                _ = cancellation.cancelled() => {},
                _ = self.shutdown.cancelled() => {
                    self.active.lock().await.remove(&active_key);
                    return;
                }
            }
        }

        let (agent_events, mut event_receiver) = mpsc::unbounded_channel();
        let (approval_events, mut approval_receiver) = mpsc::unbounded_channel();
        let trace_request_id = request_id_label(&request.id);
        let turn_span = info_span!(
            "agent_turn",
            session_id = %session_id,
            request_id = ?request.id,
        );
        let run = crate::loop_engine::with_tool_audit(
            self.run_store.clone(),
            run_record.run_id.clone(),
            self.approvals.with_session_context(
                session_id.clone(),
                request.id.clone(),
                approval_events,
                async {
                    let mut history = session.history.lock().await;
                    session
                        .engine
                        .run_turn_with_events_for_request(
                            &mut history,
                            params.message,
                            Some(agent_events),
                            cancellation.clone(),
                            Some(trace_request_id),
                        )
                        .await
                },
            ),
        )
        .instrument(turn_span);
        tokio::pin!(run);

        let result = loop {
            tokio::select! {
                result = &mut run => break result,
                event = event_receiver.recv() => {
                    if let Some(event) = event {
                        self.publish_update(
                            &frames,
                            &active_key,
                            agent_event_update(event),
                        ).await;
                    }
                }
                approval = approval_receiver.recv() => {
                    if let Some(ServerFrame::Event(event)) = approval {
                        self.publish_update(
                            &frames,
                            &active_key,
                            ActiveRequestUpdate::Event {
                                kind: event.event,
                                data: event.data,
                                run_id: None,
                                seq: None,
                            },
                        ).await;
                    }
                }
            }
        };
        while let Ok(event) = event_receiver.try_recv() {
            self.publish_update(&frames, &active_key, agent_event_update(event))
                .await;
        }
        while let Ok(ServerFrame::Event(event)) = approval_receiver.try_recv() {
            self.publish_update(
                &frames,
                &active_key,
                ActiveRequestUpdate::Event {
                    kind: event.event,
                    data: event.data,
                    run_id: None,
                    seq: None,
                },
            )
            .await;
        }

        let storage_error = self
            .active
            .lock()
            .await
            .get(&active_key)
            .and_then(|active| active.storage_error.clone());
        let response = match (storage_error, result) {
            (Some(error), _) => Err((INTERNAL_ERROR, format!("持久化事件失败: {error}"))),
            (None, Ok(_)) if cancellation.is_cancelled() => {
                Err((REQUEST_CANCELLED, "请求已取消".to_owned()))
            }
            (None, Ok(content)) => Ok(
                json!({"content": content, "run_id": run_record.run_id, "turn_id": run_record.turn_id}),
            ),
            (None, Err(ref error))
                if cancellation.is_cancelled()
                    || matches!(
                        error.downcast_ref::<RuntimeError>(),
                        Some(RuntimeError::Cancelled)
                    ) =>
            {
                Err((REQUEST_CANCELLED, "请求已取消".to_owned()))
            }
            (None, Err(error)) => Err((INTERNAL_ERROR, format!("{error:#}"))),
        };
        let terminal_status = match &response {
            Ok(_) => RunStatus::Completed,
            Err((REQUEST_CANCELLED, _)) => RunStatus::Cancelled,
            Err(_) => RunStatus::Failed,
        };
        let content = response
            .as_ref()
            .ok()
            .and_then(|value| value["content"].as_str());
        let error = response
            .as_ref()
            .err()
            .map(|(code, message)| (*code, message.as_str()));
        let committed = match self.run_store.finish(
            &run_record.run_id,
            terminal_status,
            content,
            error,
        ) {
            Ok(committed) => committed,
            Err(storage_error) => {
                tracing::error!(run_id = %run_record.run_id.0, error = %storage_error, "提交 run 终态失败");
                send_result(
                    &frames,
                    request.id,
                    Err((INTERNAL_ERROR, format!("持久化终态失败: {storage_error}"))),
                );
                self.active.lock().await.remove(&active_key);
                return;
            }
        };
        let response = if committed.status == RunStatus::UnknownAfterRestart {
            Err((-32002, "工具执行结果未知，禁止自动重放".to_owned()))
        } else {
            response
        };
        if let Some(content) = committed.content.as_deref() {
            self.publish_committed_completion(&frames, &active_key, content)
                .await;
        }
        match &response {
            Ok(_) => info!(
                session_id = %session_id,
                request_id = ?active_key.request_id,
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                outcome = "completed",
                "chat 请求结束"
            ),
            Err((code, error)) => info!(
                session_id = %session_id,
                request_id = ?active_key.request_id,
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                outcome = "failed",
                error_code = *code,
                error,
                "chat 请求结束"
            ),
        }
        self.publish_update(
            &frames,
            &active_key,
            ActiveRequestUpdate::Terminal(response),
        )
        .await;
        self.active.lock().await.remove(&active_key);
        self.queue_notify.notify_waiters();
    }

    async fn session_runtime(
        &self,
        requested_id: Option<&str>,
    ) -> Result<Arc<SessionRuntime>, (i64, String)> {
        let session_id = match requested_id {
            Some(session_id) => session_id.to_owned(),
            None => self.legacy_session_id.lock().await.clone(),
        };
        if session_id == self.default_session.id {
            return Ok(self.default_session.clone());
        }
        if let Some(runtime) = self.sessions.lock().await.get(&session_id).cloned() {
            return Ok(runtime);
        }
        let store = match self.session.open_session(&session_id) {
            Ok(store) => store,
            Err(open_error) => {
                if !self
                    .run_store
                    .session_exists(&session_id)
                    .map_err(|error| (INTERNAL_ERROR, error.to_string()))?
                {
                    return Err((INVALID_PARAMS, format!("{open_error:#}")));
                }
                self.session
                    .open_known_session(&session_id)
                    .map_err(|error| (INVALID_PARAMS, format!("{error:#}")))?
            }
        };
        let store = Arc::new(store);
        let history = store
            .load()
            .await
            .map_err(|error| (INTERNAL_ERROR, format!("{error:#}")))?;
        let runtime = Arc::new(SessionRuntime {
            id: session_id.clone(),
            engine: Arc::new(self.default_session.engine.for_session(store.clone())),
            history: Mutex::new(history),
            store,
        });
        let mut sessions = self.sessions.lock().await;
        Ok(sessions
            .entry(session_id)
            .or_insert_with(|| runtime.clone())
            .clone())
    }

    async fn session_new(&self) -> Result<Value, (i64, String)> {
        let (session_id, store) = self
            .session
            .create_isolated_session()
            .map_err(|error| (INTERNAL_ERROR, format!("{error:#}")))?;
        let store = Arc::new(store);
        let runtime = Arc::new(SessionRuntime {
            id: session_id.clone(),
            engine: Arc::new(self.default_session.engine.for_session(store.clone())),
            history: Mutex::new(Vec::new()),
            store,
        });
        self.sessions
            .lock()
            .await
            .insert(session_id.clone(), runtime);
        *self.legacy_session_id.lock().await = session_id.clone();
        Ok(json!({
            "created": true,
            "session_id": session_id,
            "messages": [],
            "pending_approvals": [],
            "active_requests": [],
        }))
    }

    async fn session_load(&self, session_id: Option<&str>) -> Result<Value, (i64, String)> {
        let runtime = self.session_runtime(session_id).await?;
        self.session_snapshot(&runtime.id).await
    }

    async fn session_load_page(&self, params: SessionPageParams) -> Result<Value, (i64, String)> {
        let runtime = self.session_runtime(Some(&params.session_id)).await?;
        let history = runtime
            .store
            .load()
            .await
            .map_err(|error| (INTERNAL_ERROR, format!("{error:#}")))?;
        let total_messages = history.len();
        let limit = params.limit.clamp(1, WEB_PAGE_MAX_LIMIT);
        let (messages, has_more) = bounded_json_page(
            history.into_iter().skip(params.offset),
            limit,
            "消息过大，已截断；可缩小加载范围查看其余 Session 内容",
        );
        let (active_requests, approvals, status) = self.session_activity(&runtime.id).await?;
        Ok(json!({
            "session_id": runtime.id,
            "messages": messages,
            "offset": params.offset,
            "limit": limit,
            "total_messages": total_messages,
            "has_more": has_more,
            "pending_approvals": approvals,
            "active_requests": active_requests,
            "status": status,
        }))
    }

    async fn session_snapshot(&self, session_id: &str) -> Result<Value, (i64, String)> {
        // 活动 turn（尤其是等待人工审批时）会长期持有内存历史锁。恢复端必须仍能
        // 立即读取快照，因此以每条消息均已 flush 的 append-only 会话文件为来源。
        let runtime = self.session_runtime(Some(session_id)).await?;
        let history = runtime
            .store
            .load()
            .await
            .map_err(|error| (INTERNAL_ERROR, format!("{error:#}")))?;
        let active = self.active.lock().await;
        let active_requests = active
            .keys()
            .filter(|key| key.session_id == session_id)
            .map(|key| key.request_id.clone())
            .collect::<Vec<RequestId>>();
        let request_ids = active
            .keys()
            .filter(|key| key.session_id == session_id)
            .map(|key| key.request_id.clone())
            .collect::<HashSet<RequestId>>();
        drop(active);
        let approvals = self
            .approvals
            .pending_for_session(Some(session_id))
            .await
            .into_iter()
            .filter(|approval| request_ids.contains(&approval.request_id))
            .collect::<Vec<_>>();
        let status = if !approvals.is_empty() {
            "waiting"
        } else if !active_requests.is_empty() {
            "running"
        } else {
            "idle"
        };
        Ok(json!({
            "session_id": session_id,
            "messages": history,
            "pending_approvals": approvals,
            "active_requests": active_requests,
            "status": status,
        }))
    }

    async fn session_list(&self) -> Result<Value, (i64, String)> {
        Ok(json!({"sessions": self.session_infos().await?}))
    }

    async fn session_trace(&self, session_id: &str) -> Result<Value, (i64, String)> {
        let runtime = self.session_runtime(Some(session_id)).await?;
        let records = runtime
            .store
            .load_trace()
            .await
            .map_err(|error| (INTERNAL_ERROR, format!("{error:#}")))?;
        Ok(json!({
            "session_id": session_id,
            "records": records,
        }))
    }

    async fn session_trace_page(&self, params: SessionPageParams) -> Result<Value, (i64, String)> {
        let runtime = self.session_runtime(Some(&params.session_id)).await?;
        let records = runtime
            .store
            .load_trace()
            .await
            .map_err(|error| (INTERNAL_ERROR, format!("{error:#}")))?;
        let total_records = records.len();
        let limit = params.limit.clamp(1, WEB_PAGE_MAX_LIMIT);
        let (records, has_more) = bounded_json_page(
            records.into_iter().skip(params.offset),
            limit,
            "链路记录过大，已截断；可继续加载其余记录",
        );
        Ok(json!({
            "session_id": runtime.id,
            "records": records,
            "offset": params.offset,
            "limit": limit,
            "total_records": total_records,
            "has_more": has_more,
        }))
    }

    async fn session_activity(
        &self,
        session_id: &str,
    ) -> Result<(Vec<RequestId>, Vec<PendingApprovalInfo>, &'static str), (i64, String)> {
        let active = self.active.lock().await;
        let active_requests = active
            .keys()
            .filter(|key| key.session_id == session_id)
            .map(|key| key.request_id.clone())
            .collect::<Vec<RequestId>>();
        let request_ids = active
            .keys()
            .filter(|key| key.session_id == session_id)
            .map(|key| key.request_id.clone())
            .collect::<HashSet<RequestId>>();
        drop(active);
        let approvals = self
            .approvals
            .pending_for_session(Some(session_id))
            .await
            .into_iter()
            .filter(|approval| request_ids.contains(&approval.request_id))
            .collect::<Vec<_>>();
        let status = if !approvals.is_empty() {
            "waiting"
        } else if !active_requests.is_empty() {
            "running"
        } else {
            "idle"
        };
        Ok((active_requests, approvals, status))
    }

    fn permissions_get(&self) -> Result<Value, (i64, String)> {
        let Some(safety) = self.safety.as_ref() else {
            return Err((
                INTERNAL_ERROR,
                "当前 daemon 未暴露权限模式控制器".to_owned(),
            ));
        };
        let mode = safety.mode();
        Ok(json!({
            "mode": mode.key(),
            "label": mode.label(),
            "description": mode.description(),
            "options": SafetyMode::all().into_iter().map(|option| json!({
                "mode": option.key(),
                "label": option.label(),
                "description": option.description(),
            })).collect::<Vec<_>>(),
        }))
    }

    fn models_list(&self) -> Result<Value, (i64, String)> {
        let config = self
            .config_store
            .load()
            .map_err(|error| (INTERNAL_ERROR, format!("读取模型配置失败：{error:#}")))?;
        let active_id = config.active_profile.clone().or_else(|| {
            self.provider_manager
                .as_ref()
                .map(|manager| manager.profile().id)
        });
        let mut profiles = config
            .profiles
            .iter()
            .map(ProfileSummary::from_profile)
            .collect::<Vec<_>>();
        if profiles.is_empty()
            && let Some(manager) = &self.provider_manager
        {
            profiles.push(ProfileSummary::from_profile(&manager.profile()));
        }
        Ok(json!({
            "active_id": active_id,
            "profiles": profiles,
            "config_path": self.config_store.path(),
            "providers": [
                {"api_type": "openai-chat", "label": "OpenAI 兼容"},
                {"api_type": "anthropic-messages", "label": "Anthropic Messages"},
                {"api_type": "ollama", "label": "Ollama 本地模型"},
            ],
        }))
    }

    async fn models_use(&self, profile_id: &str) -> Result<Value, (i64, String)> {
        if self.has_active_turns().await {
            return Err((
                REQUEST_CONFLICT,
                "当前仍有 Agent 任务运行，请等待完成或取消后再切换模型".to_owned(),
            ));
        }
        let Some(manager) = &self.provider_manager else {
            return Err((INTERNAL_ERROR, "当前 daemon 未启用模型配置管理".to_owned()));
        };
        let profile = self
            .config_store
            .activate(profile_id)
            .map_err(|error| (INVALID_PARAMS, format!("切换模型失败：{error:#}")))?;
        manager
            .switch(profile.clone())
            .map_err(|error| (INVALID_PARAMS, format!("加载模型失败：{error:#}")))?;
        Ok(json!({
            "changed": true,
            "active_id": profile.id,
            "profile": ProfileSummary::from_profile(&profile),
        }))
    }

    async fn models_save(&self, params: ModelSaveParams) -> Result<Value, (i64, String)> {
        if self.has_active_turns().await {
            return Err((
                REQUEST_CONFLICT,
                "当前仍有 Agent 任务运行，请等待完成或取消后再保存模型配置".to_owned(),
            ));
        }
        let Some(manager) = &self.provider_manager else {
            return Err((INTERNAL_ERROR, "当前 daemon 未启用模型配置管理".to_owned()));
        };
        let config = self
            .config_store
            .upsert(params.profile, params.activate)
            .map_err(|error| (INVALID_PARAMS, format!("保存模型配置失败：{error:#}")))?;
        let active_id = config.active_profile.clone();
        let Some(active_id) = active_id else {
            return Err((INTERNAL_ERROR, "保存后没有活动模型配置".to_owned()));
        };
        let profile = config
            .profiles
            .iter()
            .find(|profile| profile.id == active_id)
            .cloned()
            .ok_or_else(|| (INTERNAL_ERROR, "活动模型配置不存在".to_owned()))?;
        manager
            .switch(profile.clone())
            .map_err(|error| (INVALID_PARAMS, format!("加载模型失败：{error:#}")))?;
        Ok(json!({
            "changed": true,
            "active_id": profile.id,
            "profile": ProfileSummary::from_profile(&profile),
        }))
    }

    fn permissions_set(&self, value: &str) -> Result<Value, (i64, String)> {
        let Some(safety) = self.safety.as_ref() else {
            return Err((
                INTERNAL_ERROR,
                "当前 daemon 未暴露权限模式控制器".to_owned(),
            ));
        };
        let Some(mode) = SafetyMode::parse(value) else {
            return Err((
                INVALID_PARAMS,
                "权限模式无效，可选值：request、risk、full".to_owned(),
            ));
        };
        safety.set_mode(mode);
        Ok(json!({
            "changed": true,
            "mode": mode.key(),
            "label": mode.label(),
            "description": mode.description(),
        }))
    }

    async fn session_infos(&self) -> Result<Vec<crate::session::SessionInfo>, (i64, String)> {
        let mut sessions = self
            .session
            .list_sessions()
            .await
            .map_err(|error| (INTERNAL_ERROR, format!("{error:#}")))?;
        let current_id = self.legacy_session_id.lock().await.clone();
        for session in &mut sessions {
            session.active = session.id == current_id;
        }
        let active_counts = self.active.lock().await.keys().fold(
            HashMap::<String, usize>::new(),
            |mut counts, key| {
                *counts.entry(key.session_id.clone()).or_default() += 1;
                counts
            },
        );
        let pending_sessions = self.approvals.pending_sessions().await;
        for session in &mut sessions {
            let active_requests = active_counts.get(&session.id).copied().unwrap_or(0);
            session.active_requests = active_requests;
            session.status = if pending_sessions.contains(&session.id) {
                SessionStatus::Waiting
            } else if active_requests > 0 {
                SessionStatus::Running
            } else {
                SessionStatus::Idle
            };
            session.updated_at = session.modified_at;
        }
        let runtimes = self
            .sessions
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for runtime in runtimes {
            if sessions.iter().any(|session| session.id == runtime.id) {
                continue;
            }
            sessions.push(crate::session::SessionInfo {
                id: runtime.id.clone(),
                path: runtime.store.path_for_session(&runtime.id),
                active: runtime.id == current_id,
                message_count: 0,
                modified_at: None,
                preview: None,
                status: if pending_sessions.contains(&runtime.id) {
                    SessionStatus::Waiting
                } else if active_counts.get(&runtime.id).copied().unwrap_or(0) > 0 {
                    SessionStatus::Running
                } else {
                    SessionStatus::Idle
                },
                active_requests: active_counts.get(&runtime.id).copied().unwrap_or(0),
                updated_at: None,
            });
        }
        if !sessions.iter().any(|session| session.id == current_id) {
            let active_requests = active_counts.get(&current_id).copied().unwrap_or(0);
            sessions.push(crate::session::SessionInfo {
                id: current_id.clone(),
                path: self.session.path_for_session(&current_id),
                active: true,
                message_count: 0,
                modified_at: None,
                preview: None,
                status: if pending_sessions.contains(&current_id) {
                    SessionStatus::Waiting
                } else if active_requests > 0 {
                    SessionStatus::Running
                } else {
                    SessionStatus::Idle
                },
                active_requests,
                updated_at: None,
            });
        }
        sessions.sort_by(|left, right| {
            right
                .active
                .cmp(&left.active)
                .then_with(|| right.modified_at.cmp(&left.modified_at))
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(sessions)
    }

    async fn session_resume(&self, session_id: &str) -> Result<Value, (i64, String)> {
        self.session_runtime(Some(session_id)).await?;
        let mut snapshot = self.session_snapshot(session_id).await?;
        snapshot["resumed"] = json!(true);
        Ok(snapshot)
    }

    async fn execute_slash(
        &self,
        line: &str,
        session_id: Option<&str>,
    ) -> Result<Value, (i64, String)> {
        let registry = SlashRegistry::builtin();
        let response = match registry.parse(line) {
            SlashParse::NotCommand => SlashResponse::Text {
                content: "输入不是 slash 命令".to_owned(),
            },
            SlashParse::Error(content) => SlashResponse::Text { content },
            SlashParse::Command(invocation) => match invocation.action {
                SlashAction::Help => SlashResponse::Text {
                    content: registry.help(),
                },
                SlashAction::Status => {
                    let current_session = self.session_runtime(session_id).await?;
                    let snapshot = self.session_snapshot(&current_session.id).await?;
                    let model = self
                        .provider_manager
                        .as_ref()
                        .map(|manager| manager.profile().model)
                        .unwrap_or_else(|| "未配置".to_owned());
                    SlashResponse::Text {
                        content: format!(
                            "会话 {} · 模型 {} · 历史 {} 条 · 活动请求 {} 个 · 待审批 {} 个",
                            snapshot["session_id"].as_str().unwrap_or("unknown"),
                            model,
                            snapshot["messages"].as_array().map_or(0, Vec::len),
                            snapshot["active_requests"].as_array().map_or(0, Vec::len),
                            snapshot["pending_approvals"].as_array().map_or(0, Vec::len),
                        ),
                    }
                }
                SlashAction::Run => {
                    let id = RunId(invocation.args.join(" "));
                    let run = self
                        .run_store
                        .read_run(&id)
                        .map_err(|error| (INTERNAL_ERROR, error.to_string()))?
                        .ok_or_else(|| (INVALID_PARAMS, format!("找不到 run: {}", id.0)))?;
                    SlashResponse::Text {
                        content: format!(
                            "run_id={} · session_id={} · status={} · seq={} · terminal={}",
                            run.run_id.0,
                            run.session_id.0,
                            serde_json::to_value(run.status)
                                .unwrap_or_default()
                                .as_str()
                                .unwrap_or("unknown"),
                            run.last_seq.0,
                            run.content
                                .as_deref()
                                .or(run.error_message.as_deref())
                                .unwrap_or("")
                        ),
                    }
                }
                SlashAction::Sessions => SlashResponse::Sessions {
                    sessions: self.session_infos().await?,
                    select: false,
                },
                SlashAction::Resume if invocation.args.is_empty() => SlashResponse::Sessions {
                    sessions: self
                        .session_infos()
                        .await?
                        .into_iter()
                        .filter(|session| session.message_count > 0)
                        .collect(),
                    select: true,
                },
                SlashAction::Resume => {
                    let sessions = self
                        .session_infos()
                        .await?
                        .into_iter()
                        .filter(|session| session.message_count > 0)
                        .collect::<Vec<_>>();
                    let selection = &invocation.args[0];
                    let session_id = match selection.parse::<usize>() {
                        Ok(index) if index > 0 => sessions
                            .get(index - 1)
                            .map(|session| session.id.clone())
                            .ok_or_else(|| {
                                (INVALID_PARAMS, format!("会话编号超出范围：{index}"))
                            })?,
                        Ok(_) => return Err((INVALID_PARAMS, "会话编号从 1 开始".to_owned())),
                        Err(_) => selection.clone(),
                    };
                    let snapshot = self.session_resume(&session_id).await?;
                    let count = snapshot["messages"].as_array().map_or(0, Vec::len);
                    SlashResponse::SessionChanged {
                        message: format!("已恢复会话 {session_id}，共 {count} 条消息。"),
                        snapshot,
                    }
                }
                SlashAction::New => {
                    let snapshot = self.session_new().await?;
                    let session_id = snapshot["session_id"].as_str().unwrap_or("unknown");
                    SlashResponse::SessionChanged {
                        message: format!("已新建会话：{session_id}"),
                        snapshot,
                    }
                }
                SlashAction::Cancel => SlashResponse::Text {
                    content: "当前没有前台请求；运行中按 Ctrl-C 可取消本轮。".to_owned(),
                },
                SlashAction::Skill => self.execute_skill_command(&invocation.args).await,
                SlashAction::Cron => self.execute_cron_command(&invocation.args).await,
                SlashAction::Mcp => self.execute_mcp_command(&invocation.args).await,
                SlashAction::Permissions => self.execute_permissions_command(&invocation.args),
                SlashAction::Models => self.execute_models_command(&invocation.args).await,
                SlashAction::Ping => SlashResponse::Text {
                    content: "pong".to_owned(),
                },
                SlashAction::Dogfood => self.execute_dogfood(session_id).await?,
                SlashAction::Web => SlashResponse::Text {
                    content: "请在 TUI 中使用 /web，或运行 `my-agent serve`。".to_owned(),
                },
                SlashAction::Exit => SlashResponse::Exit,
            },
        };
        serde_json::to_value(response)
            .map_err(|error| (INTERNAL_ERROR, format!("序列化 slash 响应失败：{error}")))
    }

    async fn execute_dogfood(
        &self,
        session_id: Option<&str>,
    ) -> Result<SlashResponse, (i64, String)> {
        let runtime = self.session_runtime(session_id).await?;
        let session_path = runtime.store.path_for_session(&runtime.id);
        let dogfood_path = export_dogfood_file(&runtime.id, &session_path, &self.daemon_log_path)
            .await
            .map_err(|error| (INTERNAL_ERROR, format!("生成 dogfood 日志失败：{error:#}")))?;
        info!(
            session_id = %runtime.id,
            dogfood_path = %dogfood_path.display(),
            "已导出 dogfood 日志"
        );
        Ok(SlashResponse::Text {
            content: format!("dogfood 已生成：{}", dogfood_path.display()),
        })
    }

    fn execute_permissions_command(&self, args: &[String]) -> SlashResponse {
        let Some(safety) = self.safety.as_ref() else {
            return SlashResponse::Text {
                content: "当前 daemon 未暴露权限模式控制器。".to_owned(),
            };
        };
        if args.is_empty() {
            let mode = safety.mode();
            return SlashResponse::Text {
                content: format!(
                    "当前权限模式：{}（{}）。切换用法：/permissions request|risk|full",
                    mode.label(),
                    mode.description()
                ),
            };
        }
        let Some(mode) = SafetyMode::parse(&args[0]) else {
            return SlashResponse::Text {
                content: "权限模式无效，可选值：request（请求批准）、risk（帮我批准）、full（完全访问权限）。"
                    .to_owned(),
            };
        };
        safety.set_mode(mode);
        SlashResponse::Text {
            content: format!("已切换权限模式：{}（{}）", mode.label(), mode.description()),
        }
    }

    async fn execute_models_command(&self, args: &[String]) -> SlashResponse {
        let listed = match self.models_list() {
            Ok(value) => value,
            Err((_, message)) => return SlashResponse::Text { content: message },
        };
        let active_id = listed
            .get("active_id")
            .and_then(Value::as_str)
            .unwrap_or("未设置");
        let profiles = listed
            .get("profiles")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if args.is_empty() {
            if profiles.is_empty() {
                return SlashResponse::Text {
                    content: format!(
                        "暂无模型配置。请在 Web 设置中添加模型，或设置环境变量后重启 daemon。配置文件：{}",
                        self.config_store.path().display()
                    ),
                };
            }
            let lines = profiles
                .iter()
                .enumerate()
                .map(|(index, profile)| {
                    let id = profile["id"].as_str().unwrap_or("unknown");
                    let name = profile["name"].as_str().unwrap_or(id);
                    let provider = profile["api_type"].as_str().unwrap_or("unknown");
                    let model = profile["model"].as_str().unwrap_or("unknown");
                    let marker = if id == active_id { "*" } else { " " };
                    format!(
                        "{marker} {}. {name} · {provider} · {model} · id={id}",
                        index + 1
                    )
                })
                .collect::<Vec<_>>();
            return SlashResponse::Text {
                content: format!(
                    "当前模型：{active_id}\n{}\n切换用法：/models <编号或 ID>",
                    lines.join("\n")
                ),
            };
        }
        let selection = &args[0];
        let profile_id = match selection.parse::<usize>() {
            Ok(index) if index > 0 => profiles
                .get(index - 1)
                .and_then(|profile| profile["id"].as_str())
                .map(str::to_owned),
            _ => Some(selection.clone()),
        };
        let Some(profile_id) = profile_id else {
            return SlashResponse::Text {
                content: format!("模型编号超出范围：{selection}"),
            };
        };
        match self.models_use(&profile_id).await {
            Ok(value) => {
                let profile = value.get("profile").cloned().unwrap_or(Value::Null);
                SlashResponse::Text {
                    content: format!(
                        "已切换模型：{} · {}",
                        profile["name"].as_str().unwrap_or(&profile_id),
                        profile["model"].as_str().unwrap_or("unknown")
                    ),
                }
            }
            Err((_, message)) => SlashResponse::Text { content: message },
        }
    }

    async fn execute_skill_command(&self, args: &[String]) -> SlashResponse {
        let Some(skills) = self.skills.as_ref() else {
            return SlashResponse::Text {
                content: "当前 daemon 未启用 skill 管理器。".to_owned(),
            };
        };
        let result = match args.first().map(String::as_str) {
            Some("list") if args.len() == 1 => skills.list().await.map(|items| {
                if items.is_empty() {
                    "暂无已安装 skill。".to_owned()
                } else {
                    items
                        .iter()
                        .map(|item| format!("{}@{}：{}", item.name, item.version, item.description))
                        .collect::<Vec<_>>()
                        .join("\n")
                }
            }),
            Some("install") if args.len() >= 2 => {
                let force = args.last().is_some_and(|arg| arg == "--force");
                let end = args.len() - usize::from(force);
                let path = args[1..end].join(" ");
                if path.is_empty() {
                    Err(anyhow::anyhow!("用法：/skill install <路径> [--force]"))
                } else {
                    skills
                        .install(std::path::Path::new(&path), force)
                        .await
                        .map(|outcomes| {
                            outcomes
                                .iter()
                                .map(ToString::to_string)
                                .collect::<Vec<_>>()
                                .join("\n")
                        })
                }
            }
            Some("update") if args.len() >= 3 => {
                let path = args[2..].join(" ");
                skills
                    .update(&args[1], std::path::Path::new(&path))
                    .await
                    .map(|outcome| outcome.to_string())
            }
            Some("remove") if args.len() >= 2 => {
                let confirmed = args.get(2).is_some_and(|arg| arg == "--confirm");
                if args.len() > 3 {
                    Err(anyhow::anyhow!("用法：/skill remove <name> --confirm"))
                } else {
                    skills.remove(&args[1], confirmed).await
                }
            }
            _ => Err(anyhow::anyhow!(
                "用法：/skill list | install <路径> [--force] | update <name> <路径> | remove <name> --confirm"
            )),
        };
        SlashResponse::Text {
            content: match result {
                Ok(content) => content,
                Err(error) => format!("Skill 命令失败：{error:#}"),
            },
        }
    }

    async fn execute_cron_command(&self, args: &[String]) -> SlashResponse {
        let Some(cron) = self.cron.as_ref() else {
            return SlashResponse::Text {
                content: "当前 daemon 未启用 cron 管理器。".to_owned(),
            };
        };
        let result = match args.first().map(String::as_str) {
            Some("list") if args.len() == 1 => {
                let jobs = cron.store().list().await;
                Ok(if jobs.is_empty() {
                    "暂无 cron 任务。".to_owned()
                } else {
                    jobs.iter()
                        .map(|job| {
                            let last = job.history.last().map_or_else(
                                || "尚未运行".to_owned(),
                                |run| {
                                    format!(
                                        "上次={}，尝试={}，结果={}",
                                        run.finished_at,
                                        run.attempts,
                                        if run.success { "成功" } else { "失败" }
                                    )
                                },
                            );
                            format!(
                                "{} [{}] {} · {} · next={} · {}",
                                job.name,
                                job.id,
                                if job.enabled { "启用" } else { "停用" },
                                job.schedule.display(),
                                job.next_run_at,
                                last
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("\n")
                })
            }
            Some("add") if args.len() >= 4 => match parse_cron_add(args) {
                Ok((name, schedule, prompt, retries, backoff)) => cron
                    .store()
                    .add(name, schedule, prompt, retries, backoff)
                    .await
                    .map(|job| {
                        format!(
                            "已添加 {} [{}]，下次运行 {}",
                            job.name, job.id, job.next_run_at
                        )
                    }),
                Err(error) => Err(error),
            },
            Some("enable") if args.len() == 2 => cron
                .store()
                .set_enabled(&args[1], true)
                .await
                .map(|job| format!("已启用 {}", job.name)),
            Some("disable") if args.len() == 2 => cron
                .store()
                .set_enabled(&args[1], false)
                .await
                .map(|job| format!("已停用 {}", job.name)),
            Some("run-now") if args.len() == 2 => cron.run_now(&args[1]).await.map(|run| {
                format!(
                    "立即运行{}（尝试 {} 次）：{}",
                    if run.success { "成功" } else { "失败" },
                    run.attempts,
                    run.result
                )
            }),
            Some("remove") if args.len() == 3 && args[2] == "--confirm" => cron
                .store()
                .remove(&args[1])
                .await
                .map(|job| format!("已删除 {}", job.name)),
            Some("remove") if args.len() == 2 => Err(anyhow::anyhow!(
                "删除需要显式确认：/cron remove {} --confirm",
                args[1]
            )),
            _ => Err(anyhow::anyhow!(
                "用法：/cron list | add <name> interval=<秒>|cron=<分,时,日,月,周> [--retries=N] [--backoff=N] <prompt> | enable|disable|run-now <ID或名称> | remove <ID或名称> --confirm"
            )),
        };
        SlashResponse::Text {
            content: match result {
                Ok(content) => content,
                Err(error) => format!("Cron 命令失败：{error:#}"),
            },
        }
    }

    async fn execute_mcp_command(&self, args: &[String]) -> SlashResponse {
        let Some(mcp) = self.mcp.as_ref() else {
            return SlashResponse::Text {
                content: "当前 daemon 未启用 MCP 管理器。".to_owned(),
            };
        };
        let content = match args {
            [command] if command == "reload" => {
                mcp.reload().await;
                format_mcp_status(&mcp.status(), true)
            }
            [command] if command == "status" => format_mcp_status(&mcp.status(), true),
            [command] if command == "list" => format_mcp_status(&mcp.status(), false),
            _ => "MCP 命令失败：用法：/mcp list | status | reload".to_owned(),
        };
        SlashResponse::Text { content }
    }

    async fn handle_subscribe(
        self: Arc<Self>,
        request: JsonRpcRequest,
        frames: mpsc::UnboundedSender<ServerFrame>,
    ) {
        let params = match parse_params::<SubscribeParams>(&request.params) {
            Ok(params) => params,
            Err(error) => {
                send_result(&frames, request.id, Err((INVALID_PARAMS, error)));
                return;
            }
        };
        let matches = self
            .active
            .lock()
            .await
            .iter()
            .filter(|(key, _)| {
                key.request_id == params.request_id
                    && params
                        .session_id
                        .as_deref()
                        .is_none_or(|id| key.session_id == id)
            })
            .map(|(key, active)| (key.clone(), active.run_id.clone(), active.subscribe().1))
            .collect::<Vec<_>>();
        if matches.len() > 1 {
            send_result(
                &frames,
                request.id,
                Err((
                    REQUEST_CONFLICT,
                    "request_id 跨 session 不唯一；请指定 session_id".into(),
                )),
            );
            return;
        }
        let active_match = matches.into_iter().next();
        let run = if let Some((_, run_id, _)) = &active_match {
            self.run_store.read_run(run_id)
        } else if let Some(session_id) = params.session_id.as_deref() {
            self.run_store.find_request(session_id, &params.request_id)
        } else {
            Ok(None)
        };
        let run = match run {
            Ok(Some(run)) => run,
            Ok(None) => {
                send_result(
                    &frames,
                    request.id,
                    Ok(
                        json!({"subscribed": false, "request_id": params.request_id, "reason": "请求未在执行"}),
                    ),
                );
                return;
            }
            Err(error) => {
                send_result(
                    &frames,
                    request.id,
                    Err((INTERNAL_ERROR, error.to_string())),
                );
                return;
            }
        };
        let mut cursor = params.after_seq.unwrap_or(EventSeq(0));
        let pending_ids = self
            .approvals
            .pending()
            .await
            .into_iter()
            .map(|item| item.id)
            .collect::<HashSet<_>>();
        let events = match self.run_store.events_after(&run.run_id, cursor, 1000) {
            Ok(events) => events,
            Err(error) => {
                send_result(
                    &frames,
                    request.id,
                    Err((INTERNAL_ERROR, error.to_string())),
                );
                return;
            }
        };
        if events.len() == 1000 && events.last().is_some_and(|event| event.seq < run.last_seq) {
            let next = events.last().map_or(cursor, |event| event.seq);
            let _ = frames.send(ServerFrame::Response(JsonRpcResponse::failure_data(
                request.id,
                REQUEST_CONFLICT,
                "resync_required",
                json!({"kind": "resync_required", "snapshot": run,
                    "last_seq": run.last_seq, "cursor": next, "next_method": "run.events"}),
            )));
            return;
        }
        for event in events {
            cursor = event.seq;
            if let Some(update) = stored_event_update(event)
                && !is_resolved_approval(&update, &pending_ids)
                && frames.send(update.to_frame(request.id.clone())).is_err()
            {
                return;
            }
        }
        if run.status == RunStatus::Completed {
            send_result(
                &frames,
                request.id,
                Ok(json!({"content": run.content, "run_id": run.run_id, "turn_id": run.turn_id})),
            );
            return;
        }
        if matches!(
            run.status,
            RunStatus::Failed | RunStatus::Cancelled | RunStatus::UnknownAfterRestart
        ) {
            send_result(
                &frames,
                request.id,
                Err((
                    run.error_code.unwrap_or(INTERNAL_ERROR),
                    run.error_message
                        .unwrap_or_else(|| format!("run 状态: {:?}", run.status)),
                )),
            );
            return;
        }
        let Some((_, _, mut receiver)) = active_match else {
            send_result(
                &frames,
                request.id,
                Ok(
                    json!({"subscribed": false, "request_id": params.request_id, "run_id": run.run_id, "reason": "请求没有活动执行体"}),
                ),
            );
            return;
        };
        loop {
            match receiver.recv().await {
                Ok(update) => {
                    if let ActiveRequestUpdate::Event { seq: Some(seq), .. } = &update {
                        if *seq <= cursor {
                            continue;
                        }
                        cursor = *seq;
                    }
                    let terminal = matches!(update, ActiveRequestUpdate::Terminal(_));
                    if frames.send(update.to_frame(request.id.clone())).is_err() || terminal {
                        return;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    send_result(
                        &frames,
                        request.id,
                        Ok(
                            json!({"subscribed": false, "request_id": params.request_id, "reason": "请求已结束"}),
                        ),
                    );
                    return;
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    let snapshot = self.run_store.read_run(&run.run_id).ok().flatten();
                    let last_seq = snapshot.as_ref().map(|item| item.last_seq);
                    let _ = frames.send(ServerFrame::Response(JsonRpcResponse::failure_data(
                        request.id,
                        REQUEST_CONFLICT,
                        "resync_required",
                        json!({"kind": "resync_required", "snapshot": snapshot,
                            "last_seq": last_seq, "cursor": cursor, "next_method": "run.events"}),
                    )));
                    return;
                }
            }
        }
    }

    async fn publish_update(
        &self,
        frames: &mpsc::UnboundedSender<ServerFrame>,
        active_key: &ActiveKey,
        mut update: ActiveRequestUpdate,
    ) {
        if matches!(
            update,
            ActiveRequestUpdate::Event {
                kind: EventKind::TurnCompleted,
                ..
            }
        ) {
            return; // The final content and terminal become visible after one SQLite commit.
        }
        let run_id = self
            .active
            .lock()
            .await
            .get(active_key)
            .map(|active| active.run_id.clone());
        if let (
            Some(run_id),
            ActiveRequestUpdate::Event {
                kind,
                data,
                run_id: field_id,
                seq,
            },
        ) = (&run_id, &mut update)
        {
            let name = serde_json::to_value(&*kind)
                .ok()
                .and_then(|value| value.as_str().map(str::to_owned))
                .unwrap_or_default();
            if *kind == EventKind::ApprovalRequired
                && let Some(id) = data["approval"]["id"].as_str()
            {
                data["interaction"] = json!({"interaction_id": id, "session_id": active_key.session_id,
                    "owner_run_id": run_id, "kind": "approval", "status": "pending", "revision": 0,
                    "payload": data["approval"]});
            }
            match self.run_store.append_event(run_id, &name, data) {
                Ok(next_seq) => {
                    *field_id = Some(run_id.clone());
                    *seq = Some(next_seq);
                }
                Err(error) => {
                    tracing::error!(run_id = %run_id.0, %error, "持久化事件失败");
                    if let Some(active) = self.active.lock().await.get_mut(active_key) {
                        active.storage_error = Some(error.to_string());
                        active.cancellation.cancel();
                    }
                    return;
                }
            }
        }
        if let Some(active) = self.active.lock().await.get_mut(active_key) {
            active.publish(update.clone());
        }
        let _ = frames.send(update.to_frame(active_key.request_id.clone()));
    }

    async fn publish_committed_completion(
        &self,
        frames: &mpsc::UnboundedSender<ServerFrame>,
        active_key: &ActiveKey,
        content: &str,
    ) {
        let Some(active) = self
            .active
            .lock()
            .await
            .get(active_key)
            .map(|active| active.run_id.clone())
        else {
            return;
        };
        let seq = self
            .run_store
            .read_run(&active)
            .ok()
            .flatten()
            .map(|run| EventSeq(run.last_seq.0.saturating_sub(1)));
        let update = ActiveRequestUpdate::Event {
            kind: EventKind::TurnCompleted,
            data: json!({"content": content}),
            run_id: Some(active),
            seq,
        };
        if let Some(active) = self.active.lock().await.get_mut(active_key) {
            active.publish(update.clone());
        }
        let _ = frames.send(update.to_frame(active_key.request_id.clone()));
    }

    async fn resolve_interaction(
        &self,
        params: ApprovalRespondParams,
        rejected: bool,
    ) -> Result<Value, (i64, String)> {
        let approved = if rejected {
            false
        } else {
            params
                .approved
                .ok_or((INVALID_PARAMS, "缺少 approved".into()))?
        };
        let _guard = self.control_lock.lock().await;
        let id = InteractionId(params.approval_id);
        let current = self
            .run_store
            .read_interaction(&id)
            .map_err(|error| (INTERNAL_ERROR, error.to_string()))?
            .ok_or((INVALID_PARAMS, "interaction 不存在".into()))?;
        if params
            .session_id
            .as_deref()
            .is_some_and(|session| session != current.session_id.0)
            || params
                .owner_run_id
                .as_ref()
                .is_some_and(|run| run != &current.owner_run_id)
        {
            return Err((REQUEST_CONFLICT, "interaction owner 不匹配".into()));
        }
        let revision = params.revision.unwrap_or(current.revision);
        let claim = || {
            self.run_store.claim_interaction(
                &id,
                &current.session_id,
                &current.owner_run_id,
                revision,
                approved,
            )
        };
        let record = if current.status == "pending" {
            match self.approvals.respond_claimed(&id.0, approved, claim).await {
                Ok(record) => record,
                Err(error) => {
                    if let Ok(Some(after)) = self.run_store.read_interaction(&id)
                        && matches!(after.status.as_str(), "answered" | "rejected")
                    {
                        let _ = self.run_store.finish(
                            &after.owner_run_id,
                            RunStatus::UnknownAfterRestart,
                            None,
                            Some((-32002, "审批执行体已消失")),
                        );
                    }
                    return Err((REQUEST_CONFLICT, format!("{error:#}")));
                }
            }
        } else {
            claim().map_err(|error| (REQUEST_CONFLICT, error.to_string()))?
        };
        Ok(json!({"accepted": true, "interaction": record}))
    }

    async fn audit_run(&self, run_id: &RunId) -> Result<Value, (i64, String)> {
        let run = self
            .run_store
            .read_run(run_id)
            .map_err(|error| (INTERNAL_ERROR, error.to_string()))?
            .ok_or((INVALID_PARAMS, "run 不存在".into()))?;
        let store = self
            .session
            .open_known_session(&run.session_id.0)
            .map_err(|error| (INTERNAL_ERROR, format!("JSONL 读取失败: {error:#}")))?;
        let (messages, jsonl_error) = match store.load().await {
            Ok(messages) => (messages, None),
            Err(error) => (Vec::new(), Some(format!("{error:#}"))),
        };
        let matching_assistant = run.content.as_ref().is_some_and(|content| {
            messages.iter().any(|message| {
                message.role == crate::provider::Role::Assistant
                    && message.content.as_ref() == Some(content)
            })
        });
        let trace_label = request_id_label(&run.request_id);
        let (trace_started, trace_completed, trace_error) = match store.load_trace().await {
            Ok(records) => {
                let started = records.iter().filter(|record| matches!(record,
                    SessionTraceRecord::TurnStarted { request_id, .. } if request_id == &trace_label)).count();
                let completed = records.iter().filter(|record| matches!(record,
                    SessionTraceRecord::TurnCompleted { request_id, .. } if request_id == &trace_label)).count();
                (started, completed, None)
            }
            Err(error) => (0, 0, Some(format!("{error:#}"))),
        };
        let receipts = self
            .run_store
            .tool_receipts(run_id)
            .map_err(|error| (INTERNAL_ERROR, error.to_string()))?;
        let incomplete = receipts
            .iter()
            .filter(|receipt| receipt.status != "terminal")
            .count();
        let mut missing_artifacts = Vec::new();
        let mut corrupt_artifacts = Vec::new();
        for receipt in &receipts {
            let Some(path) = receipt.artifact_ref.as_deref() else {
                continue;
            };
            let expected = receipt
                .receipt
                .as_ref()
                .and_then(|value| value["output_sha256"].as_str());
            match artifact_sha256(path).await {
                Ok(actual) if expected.is_some_and(|expected| expected != actual) => {
                    corrupt_artifacts
                        .push(json!({"round": receipt.round, "call_id": receipt.call_id}))
                }
                Ok(_) => {}
                Err(_) => missing_artifacts
                    .push(json!({"round": receipt.round, "call_id": receipt.call_id})),
            }
        }
        let diagnostic = if !missing_artifacts.is_empty() || !corrupt_artifacts.is_empty() {
            "artifact_missing_or_corrupt"
        } else if run.status == RunStatus::Completed
            && (!matching_assistant || jsonl_error.is_some())
        {
            "jsonl_missing_or_diverged"
        } else if run.status == RunStatus::UnknownAfterRestart {
            "control_state_unknown"
        } else {
            "no_detected_divergence"
        };
        Ok(
            json!({"run_id": run_id, "session_id": run.session_id, "control_status": run.status,
            "last_seq": run.last_seq, "diagnostic": diagnostic,
            "jsonl_matching_assistant": matching_assistant,
            "incomplete_tool_receipts": incomplete,
            "jsonl_is_authoritative": false,
            "jsonl": {"message_count": messages.len(), "matching_assistant_anywhere": matching_assistant,
                "error": jsonl_error, "authoritative": false},
            "trace": {"turn_started": trace_started, "turn_completed": trace_completed, "error": trace_error},
            "tools": {"receipt_count": receipts.len(), "incomplete": incomplete,
                "missing_artifacts": missing_artifacts, "corrupt_artifacts": corrupt_artifacts},
            "manual_resolution": if run.status == RunStatus::UnknownAfterRestart { "run.reconcile 需要人工证据和 expected_last_seq" } else { "不适用" }}),
        )
    }

    async fn cancel(&self, params: CancelParams) -> Result<Value, (i64, String)> {
        let _guard = self.control_lock.lock().await;
        let mut run = if let Some(run_id) = params.run_id {
            let session_id = params
                .session_id
                .ok_or((INVALID_PARAMS, "exact cancel 需要 session_id".into()))?;
            let run = self
                .run_store
                .read_run(&run_id)
                .map_err(|error| (INTERNAL_ERROR, error.to_string()))?
                .ok_or((INVALID_PARAMS, "run 不存在".into()))?;
            if run.session_id.0 != session_id {
                return Err((REQUEST_CONFLICT, "run 不属于指定 session".into()));
            }
            run
        } else if let Some(request_id) = params.request_id {
            let result = if let Some(session_id) = params.session_id {
                self.run_store.find_request(&session_id, &request_id)
            } else {
                self.run_store.find_request_unique(&request_id)
            };
            match result.map_err(|error| (REQUEST_CONFLICT, error.to_string()))? {
                Some(run) => run,
                None => return Ok(json!({"cancelled": false, "reason": "请求不存在"})),
            }
        } else {
            return Err((INVALID_PARAMS, "需要 run_id 或 request_id".into()));
        };
        if run.status == RunStatus::Queued {
            let removed = self
                .run_store
                .remove_queued(&run.run_id)
                .map_err(|error| (INTERNAL_ERROR, error.to_string()))?;
            if removed {
                let key = ActiveKey {
                    session_id: run.session_id.0.clone(),
                    request_id: run.request_id.clone(),
                };
                if let Some(active) = self.active.lock().await.get_mut(&key) {
                    active.publish(ActiveRequestUpdate::Terminal(Err((
                        REQUEST_CANCELLED,
                        "请求已取消".into(),
                    ))));
                    active.cancellation.cancel();
                }
                self.queue_notify.notify_waiters();
            }
            if removed {
                return Ok(json!({"cancelled": true, "run_id": run.run_id, "status": "queued"}));
            }
            run = self
                .run_store
                .read_run(&run.run_id)
                .map_err(|error| (INTERNAL_ERROR, error.to_string()))?
                .ok_or((INVALID_PARAMS, "run 不存在".into()))?;
        }
        if !matches!(
            run.status,
            RunStatus::Running | RunStatus::WaitingInteraction
        ) {
            return Ok(json!({"cancelled": false, "run_id": run.run_id, "status": run.status}));
        }
        let key = ActiveKey {
            session_id: run.session_id.0.clone(),
            request_id: run.request_id.clone(),
        };
        let token = self
            .active
            .lock()
            .await
            .get(&key)
            .filter(|active| active.run_id == run.run_id)
            .map(|active| active.cancellation.clone());
        let Some(token) = token else {
            return Ok(json!({"cancelled": false, "reason": "执行体已消失", "run_id": run.run_id}));
        };
        token.cancel();
        self.approvals
            .cancel_request_in_session(&run.session_id.0, &run.request_id)
            .await;
        Ok(json!({"cancelled": true, "run_id": run.run_id, "status": "running"}))
    }
}

fn bounded_json_page<T: Serialize>(
    mut items: impl Iterator<Item = T>,
    limit: usize,
    oversized_message: &str,
) -> (Vec<Value>, bool) {
    let mut values = Vec::new();
    let mut used_bytes = 1024;
    let mut has_more = false;
    for _ in 0..limit {
        let Some(item) = items.next() else {
            return (values, false);
        };
        let value = serde_json::to_value(item).unwrap_or_else(|_| {
            json!({
                "truncated": true,
                "content": "该记录无法序列化，已隐藏"
            })
        });
        let value = compact_json_for_web(value, WEB_PAGE_MAX_ITEM_BYTES, oversized_message);
        let item_bytes = serde_json::to_vec(&value).map_or(0, |bytes| bytes.len());
        if !values.is_empty() && used_bytes + item_bytes + 1 > WEB_PAGE_MAX_BYTES {
            has_more = true;
            break;
        }
        values.push(value);
        used_bytes += item_bytes + 1;
    }
    if !has_more && items.next().is_some() {
        has_more = true;
    }
    (values, has_more)
}

fn compact_json_for_web(value: Value, max_bytes: usize, message: &str) -> Value {
    let original_bytes = serde_json::to_vec(&value).map_or(max_bytes + 1, |bytes| bytes.len());
    if original_bytes <= max_bytes {
        return value;
    }
    for max_chars in [65_536, 16_384, 4_096, 1_024, 256] {
        let mut candidate = value.clone();
        truncate_json_strings(&mut candidate, max_chars);
        let candidate_bytes =
            serde_json::to_vec(&candidate).map_or(max_bytes + 1, |bytes| bytes.len());
        if candidate_bytes <= max_bytes {
            return candidate;
        }
    }
    json!({
        "truncated": true,
        "content": format!("{message}（原始大小约 {original_bytes} 字节）")
    })
}

fn truncate_json_strings(value: &mut Value, max_chars: usize) {
    match value {
        Value::String(text) if text.chars().count() > max_chars => {
            let marker = "… [已截断]";
            let keep = max_chars.saturating_sub(marker.chars().count());
            let prefix = text.chars().take(keep).collect::<String>();
            *text = format!("{prefix}{marker}");
        }
        Value::Array(items) => {
            for item in items {
                truncate_json_strings(item, max_chars);
            }
        }
        Value::Object(fields) => {
            for field in fields.values_mut() {
                truncate_json_strings(field, max_chars);
            }
        }
        _ => {}
    }
}

fn parse_cron_add(args: &[String]) -> anyhow::Result<(String, ScheduleSpec, String, u32, u64)> {
    let name = args[1].clone();
    let schedule = if let Some(seconds) = args[2].strip_prefix("interval=") {
        ScheduleSpec::Interval {
            seconds: seconds.parse().context("interval 必须是正整数秒")?,
        }
    } else if let Some(expression) = args[2].strip_prefix("cron=") {
        ScheduleSpec::Cron {
            expression: expression.replace(',', " "),
        }
    } else {
        anyhow::bail!("schedule 必须是 interval=<秒> 或 cron=<分,时,日,月,周>");
    };
    let mut retries = 0_u32;
    let mut backoff = 5_u64;
    let mut prompt = Vec::new();
    for argument in &args[3..] {
        if let Some(value) = argument.strip_prefix("--retries=") {
            retries = value.parse().context("--retries 必须是非负整数")?;
        } else if let Some(value) = argument.strip_prefix("--backoff=") {
            backoff = value.parse().context("--backoff 必须是非负整数秒")?;
        } else {
            prompt.push(argument.as_str());
        }
    }
    if prompt.is_empty() {
        anyhow::bail!("cron prompt 不能为空");
    }
    Ok((name, schedule, prompt.join(" "), retries, backoff))
}

fn format_mcp_status(status: &crate::mcp::McpStatus, include_errors: bool) -> String {
    let mut lines = Vec::new();
    if let Some(error) = &status.file_error {
        lines.push(format!("配置错误：{error}"));
    }
    for server in &status.servers {
        let state = if server.connected {
            "已连接"
        } else {
            "不可用"
        };
        let tools = if server.tools.is_empty() {
            "无工具".to_owned()
        } else {
            server.tools.join("、")
        };
        let mut line = format!("{}：{} · {}", server.name, state, tools);
        if include_errors && let Some(error) = &server.error {
            line.push_str(&format!(" · {error}"));
        }
        lines.push(line);
    }
    if lines.is_empty() {
        "未配置 MCP server（期望 .my-agent/mcp.json）。".to_owned()
    } else {
        lines.join("\n")
    }
}

async fn export_dogfood_file(
    session_id: &str,
    session_path: &Path,
    daemon_log_path: &Path,
) -> anyhow::Result<PathBuf> {
    let session_bytes = match tokio::fs::read(session_path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("读取 session 文件失败: {}", session_path.display()));
        }
    };
    let daemon_bytes = match tokio::fs::read(daemon_log_path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("读取 daemon 日志失败: {}", daemon_log_path.display()));
        }
    };
    let session_content = String::from_utf8_lossy(&session_bytes);
    let daemon_content = String::from_utf8_lossy(&daemon_bytes);
    let session_marker = format!("session_id={session_id}");
    let quoted_session_marker = format!("session_id=\"{session_id}\"");
    let chain_logs = daemon_content
        .lines()
        // tracing-subscriber may decorate field names and `=` separately, for example
        // `\x1b[3msession_id\x1b[0m\x1b[2m=\x1b[0msession-...`. Match against a
        // plain-text copy so dogfood exports work whether ANSI colors are enabled or not.
        .map(strip_ansi_sequences)
        .filter(|line| line.contains(&session_marker) || line.contains(&quoted_session_marker))
        .collect::<Vec<_>>()
        .join("\n");
    let message_count = session_content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .count();
    let chain_log_count = chain_logs.lines().count();
    let generated_at_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("系统时间早于 UNIX_EPOCH")?
        .as_millis();
    let session_directory = session_path.parent().unwrap_or_else(|| Path::new("."));
    tokio::fs::create_dir_all(session_directory)
        .await
        .with_context(|| format!("创建 session 目录失败: {}", session_directory.display()))?;
    let session_stem = session_path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("session");
    let dogfood_path = session_directory.join(format!("dogfood-{session_stem}.log"));
    let mut output = format!(
        "# my-agent dogfood export\n\
schema_version=1\n\
generated_at_unix_ms={generated_at_ms}\n\
session_id={session_id}\n\
session_file={}\n\
daemon_log_file={}\n\
conversation_messages={message_count}\n\
chain_log_lines={chain_log_count}\n\n\
===== LLM / REACT CONVERSATION (RAW SESSION JSONL) =====\n",
        session_path.display(),
        daemon_log_path.display(),
    );
    if session_content.is_empty() {
        output.push_str("(当前 session 尚无持久化消息)\n");
    } else {
        output.push_str(&session_content);
        if !session_content.ends_with('\n') {
            output.push('\n');
        }
    }
    output.push_str("\n===== DAEMON CHAIN LOG (CURRENT SESSION ONLY) =====\n");
    if chain_logs.is_empty() {
        output.push_str("(未找到带当前 session_id 的 daemon 链路日志)\n");
    } else {
        output.push_str(&chain_logs);
        output.push('\n');
    }
    tokio::fs::write(&dogfood_path, output)
        .await
        .with_context(|| format!("写入 dogfood 文件失败: {}", dogfood_path.display()))?;
    tokio::fs::canonicalize(&dogfood_path)
        .await
        .with_context(|| format!("解析 dogfood 文件路径失败: {}", dogfood_path.display()))
}

fn strip_ansi_sequences(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut output = String::with_capacity(input.len());
    let mut cursor = 0;
    let mut plain_start = 0;

    while cursor < bytes.len() {
        if bytes[cursor] == 0x1b && bytes.get(cursor + 1) == Some(&b'[') {
            output.push_str(&input[plain_start..cursor]);
            cursor += 2;
            while cursor < bytes.len() {
                let byte = bytes[cursor];
                cursor += 1;
                if (0x40..=0x7e).contains(&byte) {
                    break;
                }
            }
            plain_start = cursor;
        } else {
            cursor += 1;
        }
    }
    output.push_str(&input[plain_start..]);
    output
}

fn parse_params<T: for<'de> Deserialize<'de>>(params: &Value) -> Result<T, String> {
    serde_json::from_value(params.clone()).map_err(|error| format!("参数无效: {error}"))
}

async fn artifact_sha256(path: &str) -> std::io::Result<String> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn request_id_label(request_id: &RequestId) -> String {
    match request_id {
        RequestId::Number(value) => value.to_string(),
        RequestId::String(value) => value.clone(),
    }
}

fn send_result(
    frames: &mpsc::UnboundedSender<ServerFrame>,
    id: RequestId,
    result: Result<Value, (i64, String)>,
) {
    let response = match result {
        Ok(value) => JsonRpcResponse::success(id, value),
        Err((code, message)) => JsonRpcResponse::failure(id, code, message),
    };
    let _ = frames.send(ServerFrame::Response(response));
}

fn agent_event_update(event: AgentEvent) -> ActiveRequestUpdate {
    let (kind, data) = match event {
        AgentEvent::TurnStarted => (EventKind::TurnStarted, json!({})),
        AgentEvent::ThinkingDelta(delta) => (EventKind::ThinkingDelta, json!({"delta": delta})),
        AgentEvent::ThinkingFinished => (EventKind::ThinkingFinished, json!({})),
        AgentEvent::TextDelta(delta) => (EventKind::TextDelta, json!({"delta": delta})),
        AgentEvent::ToolStarted {
            call_id,
            name,
            round,
        } => (
            EventKind::ToolStarted,
            json!({"tool_call_id": call_id, "name": name, "round": round}),
        ),
        AgentEvent::ToolFinished {
            call_id,
            name,
            output,
            round,
            duration_ms,
            success,
            error,
        } => (
            EventKind::ToolFinished,
            json!({
                "tool_call_id": call_id,
                "name": name,
                "output": output,
                "round": round,
                "duration_ms": duration_ms,
                "success": success,
                "error": error,
            }),
        ),
        AgentEvent::TurnCompleted { content } => {
            (EventKind::TurnCompleted, json!({"content": content}))
        }
    };
    ActiveRequestUpdate::Event {
        kind,
        data,
        run_id: None,
        seq: None,
    }
}

fn stored_event_update(event: StoredEvent) -> Option<ActiveRequestUpdate> {
    let kind = if event.event == "assistant_content" {
        EventKind::TurnCompleted
    } else {
        serde_json::from_value::<EventKind>(Value::String(event.event)).ok()?
    };
    Some(ActiveRequestUpdate::Event {
        kind,
        data: event.data,
        run_id: Some(event.run_id),
        seq: Some(event.seq),
    })
}

fn is_resolved_approval(update: &ActiveRequestUpdate, pending_ids: &HashSet<String>) -> bool {
    let ActiveRequestUpdate::Event {
        kind: EventKind::ApprovalRequired,
        data,
        ..
    } = update
    else {
        return false;
    };
    data["approval"]["id"]
        .as_str()
        .is_some_and(|id| !pending_ids.contains(id))
}

#[cfg(test)]
mod dogfood_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{WEB_PAGE_MAX_BYTES, bounded_json_page, export_dogfood_file};
    use crate::daemon::protocol::{JsonRpcResponse, RequestId, encode_frame};
    use crate::provider::{Message, Role};

    static NEXT_TEST: AtomicUsize = AtomicUsize::new(0);

    #[tokio::test]
    async fn exports_full_conversation_and_only_current_session_chain_logs() {
        let id = NEXT_TEST.fetch_add(1, Ordering::SeqCst);
        let directory =
            std::env::temp_dir().join(format!("my-agent-dogfood-{}-{id}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let session_id = "session-42.jsonl";
        let session_path = directory.join(session_id);
        let daemon_log_path = directory.join("daemon.log");
        let conversation = concat!(
            "{\"role\":\"user\",\"content\":\"检查项目\"}\n",
            "{\"role\":\"assistant\",\"tool_calls\":[{\"name\":\"exec\",\"arguments\":{\"command\":\"pwd\"}}]}\n",
            "{\"role\":\"tool\",\"content\":\"/workspace\",\"name\":\"exec\"}\n",
            "{\"role\":\"assistant\",\"content\":\"完成\"}\n",
        );
        std::fs::write(&session_path, conversation).unwrap();
        std::fs::write(
            &daemon_log_path,
            concat!(
                "\x1b[32m INFO\x1b[0m agent_turn{\x1b[3msession_id\x1b[0m\x1b[2m=\x1b[0msession-42.jsonl request_id=Number(7)}: 开始 ReAct 轮次\n",
                "INFO agent_turn{session_id=other.jsonl request_id=Number(8)}: 其他会话\n",
                "INFO session_id=session-42.jsonl request_id=Number(7): chat 请求结束\n",
            ),
        )
        .unwrap();

        let exported = export_dogfood_file(session_id, &session_path, &daemon_log_path)
            .await
            .unwrap();
        let content = std::fs::read_to_string(&exported).unwrap();

        assert_eq!(
            exported.file_name().and_then(|value| value.to_str()),
            Some("dogfood-session-42.log")
        );
        assert!(content.contains("conversation_messages=4"));
        assert!(content.contains("chain_log_lines=2"));
        assert!(content.contains(conversation));
        assert!(content.contains("request_id=Number(7)"));
        assert!(!content.contains("其他会话"));
        assert!(!content.contains('\x1b'));

        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn bounds_large_web_pages_and_marks_oversized_content() {
        let messages = (0..80)
            .map(|_| Message::text(Role::Tool, "x".repeat(300 * 1024)))
            .collect::<Vec<_>>();
        let (items, has_more) = bounded_json_page(messages.into_iter(), 80, "消息过大，已截断");
        let encoded = serde_json::to_vec(&items).unwrap();
        assert!(encoded.len() <= WEB_PAGE_MAX_BYTES);
        let frame = encode_frame(&JsonRpcResponse::success(
            RequestId::Number(1),
            serde_json::json!({"messages": items.clone()}),
        ))
        .unwrap();
        assert!(frame.len() <= crate::daemon::protocol::MAX_FRAME_BYTES + 1);
        assert!(has_more);
        assert!(items.len() < 80);
        assert_eq!(items[0]["role"], "tool");
        assert!(items[0]["content"].as_str().unwrap().contains("已截断"));
    }
}
