use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Result, bail};
use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use sha2::{Digest, Sha256};
use tokio::sync::{Notify, mpsc};
use tracing::{debug, info, warn};

use crate::context::ContextManager;
use crate::provider::{
    AttemptStatus, FrozenRoute, Message, Provider, ProviderAttempt, ProviderError,
    ProviderErrorKind, ProviderEvent, ProviderUsage, Response, RetryDecision, Role, TimeoutPhase,
    TimeoutPolicy, ToolCall,
};
use crate::session::{SessionStore, SessionTraceRecord};
use crate::storage::{RunId, RunStore, RuntimeError};
use crate::tool_calls::ToolCallAssembler;
use crate::tools::{Tool, ToolCancellation, ToolOutput, ToolRegistry};

pub const DEFAULT_PROGRESS_CHECKPOINT_ROUNDS: usize = 50;
tokio::task_local! { static TOOL_AUDIT: ToolAuditContext; }
tokio::task_local! { static TOOL_CALL_ID: String; }

#[derive(Clone)]
struct ToolAuditContext {
    store: Arc<RunStore>,
    run_id: RunId,
}

pub async fn with_tool_audit<F: std::future::Future>(
    store: Arc<RunStore>,
    run_id: RunId,
    future: F,
) -> F::Output {
    TOOL_AUDIT
        .scope(ToolAuditContext { store, run_id }, future)
        .await
}

pub(crate) fn current_tool_owner() -> Option<(RunId, String)> {
    let run_id = TOOL_AUDIT.try_with(|audit| audit.run_id.clone()).ok()?;
    let call_id = TOOL_CALL_ID.try_with(Clone::clone).ok()?;
    Some((run_id, call_id))
}
pub const MAX_CONSECUTIVE_TOOL_FAILURES: usize = 3;
const REPETITION_THRESHOLD: usize = 3;
const REPETITION_ABORT_THRESHOLD: usize = 10;
const REPETITION_REMINDER: &str = "检测到连续重复的工具调用、参数与结果。可以继续使用任何工具，但请先判断该重复是否必要；若没有新信息，考虑换个思路或直接给出结论。";
const PROGRESS_CHECKPOINT_REMINDER: &str = "这是一次长任务的进度检查点，不是终止信号。请核对当前计划和已完成工作：若任务已经完成，立即给出最终结论；若仍有必要工作，继续执行剩余步骤，避免重做已经完成的内容。";
static NEXT_ATTEMPT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgentEvent {
    TurnStarted,
    ThinkingDelta(String),
    ThinkingFinished,
    TextDelta(String),
    ToolStarted {
        call_id: String,
        name: String,
        round: usize,
    },
    ToolFinished {
        call_id: String,
        name: String,
        output: String,
        round: usize,
        duration_ms: u64,
        success: bool,
        error: Option<String>,
    },
    TurnCompleted {
        content: String,
    },
}

#[derive(Clone, Default)]
pub struct CancellationToken {
    inner: Arc<CancellationState>,
}

#[derive(Default)]
struct CancellationState {
    cancelled: AtomicBool,
    notify: Notify,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        if !self.inner.cancelled.swap(true, Ordering::SeqCst) {
            self.inner.notify.notify_waiters();
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::SeqCst)
    }

    pub async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        let notified = self.inner.notify.notified();
        if self.is_cancelled() {
            return;
        }
        notified.await;
    }
}

#[async_trait]
impl ToolCancellation for CancellationToken {
    fn is_cancelled(&self) -> bool {
        self.is_cancelled()
    }

    async fn cancelled(&self) {
        self.cancelled().await;
    }
}

#[derive(Clone)]
pub struct LoopEngine {
    provider: Arc<dyn Provider>,
    tools: ToolRegistry,
    context: ContextManager,
    session: Option<Arc<SessionStore>>,
    round_limit: Option<usize>,
    total_token_budget: Option<u64>,
    route: Option<FrozenRoute>,
}

impl LoopEngine {
    pub fn stop_resources(&self) {
        self.tools.stop_resources();
    }

    pub fn new(
        provider: Arc<dyn Provider>,
        tools: ToolRegistry,
        context: ContextManager,
        session: Arc<SessionStore>,
    ) -> Self {
        Self {
            provider,
            tools,
            context,
            session: Some(session),
            // 主任务不使用固定轮次硬上限。复杂任务依靠进度检查点继续运行，
            // 真正的失控则由重复调用与连续失败熔断器识别。
            round_limit: None,
            total_token_budget: None,
            route: None,
        }
    }

    #[cfg(test)]
    pub fn ephemeral(
        provider: Arc<dyn Provider>,
        tools: ToolRegistry,
        context: ContextManager,
        max_rounds: usize,
    ) -> Self {
        Self {
            provider,
            tools,
            context,
            session: None,
            round_limit: Some(max_rounds),
            total_token_budget: None,
            route: None,
        }
    }

    pub fn for_session(&self, session: Arc<SessionStore>) -> Self {
        Self {
            provider: self.provider.clone(),
            tools: self.tools.clone(),
            context: self.context.clone(),
            session: Some(session),
            round_limit: self.round_limit,
            total_token_budget: self.total_token_budget,
            route: self.route.clone(),
        }
    }

    pub fn for_delegation(
        &self,
        names: &[String],
        max_rounds: usize,
        max_tokens: usize,
    ) -> Result<Self> {
        let mut copy = self.clone();
        copy.tools = self.tools.subset(names.iter().map(String::as_str))?;
        copy.round_limit = Some(max_rounds);
        copy.total_token_budget = Some(max_tokens as u64);
        copy.context = copy
            .context
            .with_token_budget(max_tokens.min(self.context.token_budget()));
        Ok(copy)
    }

    pub fn token_budget(&self) -> usize {
        self.context.token_budget()
    }

    pub fn with_tool<T: Tool + 'static>(&self, tool: T) -> Self {
        let mut copy = self.clone();
        copy.tools.register(tool);
        copy
    }

    pub fn with_route(&self, route: FrozenRoute) -> Self {
        let mut copy = self.clone();
        let primary = route.primary();
        copy.context = self
            .context
            .with_provider(primary.clone())
            .with_token_budget(route.snapshot.context_policy.token_budget);
        copy.provider = primary;
        copy.route = Some(route);
        copy
    }

    pub async fn run_turn(&self, history: &mut Vec<Message>, input: String) -> Result<String> {
        self.run_turn_with_events(history, input, None, CancellationToken::new())
            .await
    }

    pub async fn run_turn_with_events(
        &self,
        history: &mut Vec<Message>,
        input: String,
        events: Option<mpsc::UnboundedSender<AgentEvent>>,
        cancellation: CancellationToken,
    ) -> Result<String> {
        self.run_turn_with_events_for_request(history, input, events, cancellation, None)
            .await
    }

    pub async fn run_turn_with_events_for_request(
        &self,
        history: &mut Vec<Message>,
        input: String,
        events: Option<mpsc::UnboundedSender<AgentEvent>>,
        cancellation: CancellationToken,
        request_id: Option<String>,
    ) -> Result<String> {
        let started_at = Instant::now();
        if let Some(request_id) = request_id.as_deref() {
            self.record_trace(SessionTraceRecord::TurnStarted {
                timestamp_ms: unix_time_ms(),
                request_id: request_id.to_owned(),
                input: input.clone(),
            })
            .await;
        }
        let result = self
            .run_turn_inner(history, input, events, cancellation, request_id.as_deref())
            .await;
        if let Some(request_id) = request_id {
            self.record_trace(SessionTraceRecord::TurnCompleted {
                timestamp_ms: unix_time_ms(),
                request_id,
                duration_ms: started_at.elapsed().as_millis() as u64,
                success: result.is_ok(),
                error: result.as_ref().err().map(|error| format!("{error:#}")),
            })
            .await;
        }
        result
    }

    async fn run_turn_inner(
        &self,
        history: &mut Vec<Message>,
        input: String,
        events: Option<mpsc::UnboundedSender<AgentEvent>>,
        cancellation: CancellationToken,
        trace_request_id: Option<&str>,
    ) -> Result<String> {
        if cancellation.is_cancelled() {
            return Err(RuntimeError::Cancelled.into());
        }
        let _turn_guard = match &self.session {
            Some(session) => Some(tokio::select! {
                guard = session.lock_turn() => guard,
                _ = cancellation.cancelled() => return Err(RuntimeError::Cancelled.into()),
            }),
            None => None,
        };
        emit(&events, AgentEvent::TurnStarted);
        self.record(history, Message::text(Role::User, input))
            .await?;
        let capabilities = self.provider.capabilities();
        let specs = if capabilities.tools {
            self.tools.specs()
        } else {
            Vec::new()
        };
        let mut transient_messages = Vec::new();
        let mut repeat_detector = RepeatDetector::default();
        let mut repetition_reminder = false;
        let mut consecutive_tool_failures = 0usize;
        let mut estimated_total_tokens = 0u64;

        let mut round = 0usize;
        loop {
            round = round.saturating_add(1);
            if self
                .round_limit
                .is_some_and(|round_limit| round > round_limit)
            {
                bail!(
                    "ReAct 循环达到最大轮次 {}，已停止",
                    self.round_limit.expect("已检查 round_limit 存在")
                );
            }
            let progress_checkpoint = self.round_limit.is_none()
                && round > 1
                && (round - 1).is_multiple_of(DEFAULT_PROGRESS_CHECKPOINT_ROUNDS);
            info!(
                round,
                round_limit = ?self.round_limit,
                progress_checkpoint,
                history_messages = history.len(),
                "开始 ReAct 轮次"
            );
            let mut request_messages = self.context.prepare(history, &specs).await?;
            request_messages.extend(transient_messages.clone());
            if progress_checkpoint {
                info!(round, "长任务越过进度检查点，将继续执行");
                request_messages.push(Message::text(Role::System, PROGRESS_CHECKPOINT_REMINDER));
            }
            if repetition_reminder {
                request_messages.push(Message::text(Role::System, REPETITION_REMINDER));
                repetition_reminder = false;
            }
            let output = self
                .request_model(
                    &request_messages,
                    &specs,
                    &events,
                    &cancellation,
                    round,
                    trace_request_id,
                )
                .await?;
            if let Some(budget) = self.total_token_budget {
                let estimate_input = request_messages
                    .iter()
                    .map(|message| {
                        message
                            .content
                            .as_deref()
                            .map_or(0, crate::context::estimate_text_tokens)
                            as u64
                    })
                    .sum::<u64>();
                let estimate_output = match &output.response {
                    Response::Text(text) => crate::context::estimate_text_tokens(text) as u64,
                    Response::ToolCalls(calls) => calls
                        .iter()
                        .map(|call| {
                            crate::context::estimate_text_tokens(&call.arguments.to_string()) as u64
                        })
                        .sum(),
                    Response::ToolAssemblyFailed(_) => 0,
                };
                estimated_total_tokens = estimated_total_tokens
                    .saturating_add(estimate_input.saturating_add(estimate_output));
                let usage = TOOL_AUDIT
                    .try_with(|audit| audit.store.provider_usage(&audit.run_id))
                    .ok()
                    .transpose()?;
                let measured = usage.map_or(0, |usage| {
                    usage
                        .input_tokens
                        .unwrap_or(0)
                        .saturating_add(usage.output_tokens.unwrap_or(0))
                });
                if measured.max(estimated_total_tokens) > budget
                    && !matches!(output.response, Response::Text(_))
                {
                    bail!("子 Agent token 预算已耗尽：上限 {budget}");
                }
            }
            match output.response {
                Response::Text(text) => {
                    self.record(
                        history,
                        Message::assistant_with_thinking(text.clone(), output.thinking),
                    )
                    .await?;
                    emit(
                        &events,
                        AgentEvent::TurnCompleted {
                            content: text.clone(),
                        },
                    );
                    return Ok(text);
                }
                Response::ToolCalls(calls) => {
                    if calls.is_empty() {
                        consecutive_tool_failures = consecutive_tool_failures.saturating_add(1);
                        let error = "模型返回了空工具调用列表";
                        self.record(
                            history,
                            Message::text(
                                Role::System,
                                format!(
                                    "[empty_tool_calls] {error}；请直接回答或生成有效工具调用。"
                                ),
                            ),
                        )
                        .await?;
                        warn!(
                            round,
                            consecutive_tool_failures,
                            threshold = MAX_CONSECUTIVE_TOOL_FAILURES,
                            "模型返回空工具调用列表"
                        );
                        if consecutive_tool_failures >= MAX_CONSECUTIVE_TOOL_FAILURES {
                            bail!(
                                "连续 {consecutive_tool_failures} 次收到空工具调用，已停止无效重试"
                            );
                        }
                        continue;
                    }
                    if let Err(error) = self.tools.admit_all(&calls) {
                        consecutive_tool_failures = consecutive_tool_failures.saturating_add(1);
                        self.record(
                            history,
                            Message::text(
                                Role::System,
                                format!(
                                    "[tool_call_admission_error:{}] {}。本轮没有执行任何工具；请修正全部调用后重试。",
                                    error.code(),
                                    error
                                ),
                            ),
                        )
                        .await?;
                        warn!(
                            round,
                            consecutive_tool_failures,
                            threshold = MAX_CONSECUTIVE_TOOL_FAILURES,
                            "工具调用准入失败"
                        );
                        if consecutive_tool_failures >= MAX_CONSECUTIVE_TOOL_FAILURES {
                            bail!(
                                "连续 {consecutive_tool_failures} 次工具调用无效，已停止自动重试。最近错误：{error}"
                            );
                        }
                        continue;
                    }
                    let audit = TOOL_AUDIT.try_with(Clone::clone).ok();
                    if let Some(audit) = &audit {
                        let prepared = calls
                            .iter()
                            .map(|call| {
                                let effect = if self.tools.is_read_only(&call.name) {
                                    "read"
                                } else {
                                    "external_side_effect"
                                };
                                let digest = format!(
                                    "{:x}",
                                    Sha256::digest(call.arguments.to_string().as_bytes())
                                );
                                (call.clone(), effect.to_owned(), digest, false)
                            })
                            .collect::<Vec<_>>();
                        audit
                            .store
                            .prepare_tool_batch(&audit.run_id, round, &prepared)?;
                    }
                    self.record(
                        history,
                        Message::assistant_tool_calls_with_thinking(calls.clone(), output.thinking),
                    )
                    .await?;
                    let execute = self.execute_in_waves(
                        &calls,
                        events.clone(),
                        &cancellation,
                        round,
                        trace_request_id,
                    );
                    tokio::pin!(execute);
                    let results = tokio::select! {
                        results = &mut execute => results?,
                        _ = cancellation.cancelled() => return Err(RuntimeError::Cancelled.into()),
                    };
                    let round_failures = results.iter().filter(|result| result.failed).count();
                    let last_error = results
                        .iter()
                        .rev()
                        .find_map(|result| result.error.as_deref())
                        .map(str::to_owned);
                    let mut fingerprints = Vec::with_capacity(results.len());
                    for result in results {
                        self.record(history, result.message).await?;
                        transient_messages.extend(result.transient_messages);
                        fingerprints.push(result.fingerprint);
                    }
                    if let Some(audit) = &audit {
                        audit.store.finish_tool_batch(&audit.run_id, round)?;
                    }
                    let repeat_count = repeat_detector.observe(fingerprints);
                    if repeat_count == REPETITION_THRESHOLD {
                        repetition_reminder = true;
                    }
                    if repeat_count >= REPETITION_ABORT_THRESHOLD {
                        bail!(
                            "检测到同一组工具调用及结果连续重复 {repeat_count} 轮，判定为无进展循环，已停止；这不是任务轮次上限"
                        );
                    }
                    if round_failures == 0 {
                        consecutive_tool_failures = 0;
                    } else {
                        consecutive_tool_failures =
                            consecutive_tool_failures.saturating_add(round_failures);
                        warn!(
                            round,
                            round_failures,
                            consecutive_tool_failures,
                            threshold = MAX_CONSECUTIVE_TOOL_FAILURES,
                            "工具执行失败"
                        );
                        if consecutive_tool_failures >= MAX_CONSECUTIVE_TOOL_FAILURES {
                            let detail = last_error.unwrap_or_else(|| "未提供错误详情".to_owned());
                            bail!(
                                "连续 {consecutive_tool_failures} 次工具执行失败，已停止自动重试。最近错误：{detail}"
                            );
                        }
                    }
                }
                Response::ToolAssemblyFailed(error) => {
                    consecutive_tool_failures = consecutive_tool_failures.saturating_add(1);
                    warn!(code = %error.code, message = %error.message, "工具调用装配失败，整轮拒绝");
                    self.record(
                        history,
                        Message::text(
                            Role::System,
                            format!(
                                "[tool_call_assembly_error:{}] {}。本轮没有执行任何工具；请重新生成完整且合法的工具调用。",
                                error.code, error.message
                            ),
                        ),
                    )
                    .await?;
                    warn!(
                        round,
                        consecutive_tool_failures,
                        threshold = MAX_CONSECUTIVE_TOOL_FAILURES,
                        "工具调用装配失败"
                    );
                    if consecutive_tool_failures >= MAX_CONSECUTIVE_TOOL_FAILURES {
                        bail!(
                            "连续 {consecutive_tool_failures} 次工具调用装配失败，已停止自动重试。最近错误：{}",
                            error.message
                        );
                    }
                }
            }
        }
    }

    async fn request_model(
        &self,
        messages: &[Message],
        specs: &[crate::provider::ToolSpec],
        events: &Option<mpsc::UnboundedSender<AgentEvent>>,
        cancellation: &CancellationToken,
        round: usize,
        trace_request_id: Option<&str>,
    ) -> Result<ModelOutput> {
        if cancellation.is_cancelled() {
            return Err(RuntimeError::Cancelled.into());
        }
        let Some(route) = &self.route else {
            return self
                .request_model_once(
                    &self.provider,
                    messages,
                    specs,
                    events,
                    cancellation,
                    round,
                    trace_request_id,
                    None,
                    &mut AttemptObservation::default(),
                )
                .await;
        };
        let mut request_messages = messages.to_vec();
        let mut compacted = false;
        let mut last_error =
            ProviderError::new(ProviderErrorKind::Server, "没有可用的 Provider 路由");
        for (candidate_index, (candidate, provider)) in route
            .snapshot
            .candidates
            .iter()
            .zip(&route.providers)
            .enumerate()
        {
            let circuit_key = candidate.circuit_key();
            if route.circuit.acquire(&circuit_key) == crate::provider::CircuitState::Open {
                if let Ok(audit) = TOOL_AUDIT.try_with(Clone::clone) {
                    audit.store.append_event(
                        &audit.run_id,
                        "provider_circuit",
                        &serde_json::json!({
                            "candidate_index": candidate_index, "state": "open", "action": "skip",
                        }),
                    )?;
                }
                continue;
            }
            let mut prior_retries = 0;
            loop {
                if cancellation.is_cancelled() {
                    return Err(RuntimeError::Cancelled.into());
                }
                let audit = TOOL_AUDIT.try_with(Clone::clone).ok();
                let mut attempt = audit.as_ref().map(|audit| ProviderAttempt {
                    attempt_id: format!(
                        "{}-{round}-{candidate_index}-{}",
                        audit.run_id.0,
                        NEXT_ATTEMPT_ID.fetch_add(1, Ordering::Relaxed)
                    ),
                    run_id: audit.run_id.clone(),
                    round: round as u32,
                    candidate_index: candidate_index as u32,
                    provider_profile_id: candidate.profile_id.clone(),
                    api_type: candidate.api_type,
                    model: candidate.model.clone(),
                    status: AttemptStatus::Started,
                    error_kind: None,
                    diagnostic: None,
                    retry_after_ms: None,
                    stream_committed: false,
                    started_at_ms: unix_time_ms() as i64,
                    first_event_at_ms: None,
                    finished_at_ms: None,
                    usage: None,
                });
                if let (Some(audit), Some(attempt)) = (&audit, &attempt) {
                    audit.store.start_provider_attempt(attempt)?;
                }
                let mut observation = AttemptObservation::default();
                let outcome = self
                    .request_model_once(
                        provider,
                        &request_messages,
                        specs,
                        events,
                        cancellation,
                        round,
                        trace_request_id,
                        Some(&route.snapshot.timeout_policy),
                        &mut observation,
                    )
                    .await;
                let result: std::result::Result<(), ProviderError> = match outcome {
                    Ok(_output) if observation.output_truncated => Err(ProviderError::new(
                        ProviderErrorKind::OutputTruncated,
                        "Provider 输出被截断",
                    )),
                    Ok(output) if matches!(&output.response, Response::Text(text) if text.is_empty()) =>
                    {
                        let kind = if output.thinking.is_some() {
                            ProviderErrorKind::ReasoningOnly
                        } else {
                            ProviderErrorKind::EmptyCompletion
                        };
                        Err(ProviderError::new(kind, "Provider 未返回可用内容"))
                    }
                    Ok(output) => {
                        if matches!(output.response, Response::ToolAssemblyFailed(_)) {
                            last_error = ProviderError::new(
                                ProviderErrorKind::Protocol,
                                "工具调用流装配失败",
                            );
                        }
                        if let (Some(audit), Some(attempt)) = (&audit, &mut attempt) {
                            attempt.status =
                                if matches!(output.response, Response::ToolAssemblyFailed(_)) {
                                    AttemptStatus::Failed
                                } else {
                                    AttemptStatus::Succeeded
                                };
                            if attempt.status == AttemptStatus::Failed {
                                attempt.error_kind = Some(last_error.kind);
                                attempt.diagnostic = Some(last_error.diagnostic.clone());
                            }
                            finish_attempt(audit, attempt, &observation)?;
                        }
                        let circuit_state = route.circuit.record(
                            &circuit_key,
                            !matches!(output.response, Response::ToolAssemblyFailed(_)),
                            false,
                        );
                        if let Some(audit) = &audit {
                            audit.store.append_event(&audit.run_id, "provider_circuit", &serde_json::json!({
                                "candidate_index": candidate_index, "state": format!("{circuit_state:?}").to_ascii_lowercase(),
                            }))?;
                        }
                        return Ok(output);
                    }
                    Err(error) => Err(error.downcast::<ProviderError>().unwrap_or_else(|error| {
                        if error
                            .downcast_ref::<RuntimeError>()
                            .is_some_and(|error| matches!(error, RuntimeError::Cancelled))
                        {
                            ProviderError::new(ProviderErrorKind::Cancelled, "请求已取消")
                        } else {
                            ProviderError::new(ProviderErrorKind::Protocol, "Provider 请求失败")
                        }
                    })),
                };
                let error = result.expect_err("失败分支");
                if let (Some(audit), Some(attempt)) = (&audit, &mut attempt) {
                    attempt.status = if error.kind == ProviderErrorKind::Cancelled {
                        AttemptStatus::Cancelled
                    } else {
                        AttemptStatus::Failed
                    };
                    attempt.error_kind = Some(error.kind);
                    attempt.diagnostic = Some(error.diagnostic.clone());
                    attempt.retry_after_ms = error.diagnostic.retry_after_ms;
                    finish_attempt(audit, attempt, &observation)?;
                }
                let transient = matches!(
                    error.kind,
                    ProviderErrorKind::RateLimit
                        | ProviderErrorKind::Transport
                        | ProviderErrorKind::Server
                        | ProviderErrorKind::Timeout(_)
                );
                let circuit_state = route.circuit.record(&circuit_key, false, transient);
                if let Some(audit) = &audit {
                    audit.store.append_event(&audit.run_id, "provider_circuit", &serde_json::json!({
                        "candidate_index": candidate_index, "state": format!("{circuit_state:?}").to_ascii_lowercase(),
                    }))?;
                }
                let safe = !observation.stream_committed && !observation.tool_call_seen;
                if error.kind == ProviderErrorKind::ContextOverflow && !compacted && safe {
                    compacted = true;
                    if let Ok(Some(smaller)) =
                        self.context.compact_for_overflow(&request_messages).await
                    {
                        request_messages = smaller;
                        continue;
                    }
                }
                let mut decision = route.snapshot.retry_policy.decide(
                    error.kind,
                    prior_retries,
                    error.diagnostic.retry_after_ms,
                    safe,
                    candidate_index + 1 < route.providers.len(),
                );
                if circuit_state == crate::provider::CircuitState::Open
                    && matches!(decision, RetryDecision::Retry(_))
                {
                    decision = if safe && candidate_index + 1 < route.providers.len() {
                        RetryDecision::Fallback
                    } else {
                        RetryDecision::Fail
                    };
                }
                last_error = error;
                match decision {
                    RetryDecision::Retry(delay) => {
                        if let Some(audit) = &audit {
                            audit.store.append_event(&audit.run_id, "provider_retry", &serde_json::json!({
                                "candidate_index": candidate_index, "round": round,
                                "error_kind": last_error.kind, "delay_ms": delay.as_millis() as u64,
                            }))?;
                        }
                        prior_retries += 1;
                        tokio::select! {
                            _ = tokio::time::sleep(delay) => {},
                            _ = cancellation.cancelled() => return Err(RuntimeError::Cancelled.into()),
                        }
                    }
                    RetryDecision::Fallback => {
                        if let Some(audit) = &audit {
                            audit.store.append_event(&audit.run_id, "provider_fallback", &serde_json::json!({
                                "from_candidate_index": candidate_index, "to_candidate_index": candidate_index + 1,
                                "reason": last_error.kind,
                            }))?;
                        }
                        break;
                    }
                    RetryDecision::Fail => return Err(last_error.into()),
                }
            }
        }
        Err(last_error.into())
    }

    #[allow(clippy::too_many_arguments)]
    async fn request_model_once(
        &self,
        provider: &Arc<dyn Provider>,
        messages: &[Message],
        specs: &[crate::provider::ToolSpec],
        events: &Option<mpsc::UnboundedSender<AgentEvent>>,
        cancellation: &CancellationToken,
        round: usize,
        trace_request_id: Option<&str>,
        timeout: Option<&TimeoutPolicy>,
        observation: &mut AttemptObservation,
    ) -> Result<ModelOutput> {
        let request_messages;
        let messages = if provider.capabilities().images {
            messages
        } else {
            request_messages = without_images(messages);
            &request_messages
        };
        let (provider_events, mut provider_rx) = mpsc::unbounded_channel();
        info!(
            round,
            provider = %provider.api_type(),
            message_count = messages.len(),
            tool_spec_count = specs.len(),
            "开始请求模型"
        );
        if let Some(request_id) = trace_request_id {
            self.record_trace(SessionTraceRecord::ModelRequest {
                timestamp_ms: unix_time_ms(),
                request_id: request_id.to_owned(),
                round,
                provider: provider.api_type().to_string(),
                messages: messages.to_vec(),
                tools: specs.to_vec(),
            })
            .await;
        }
        let request = provider.chat_stream(messages, specs, provider_events);
        tokio::pin!(request);
        let mut assembler = ToolCallAssembler::default();
        let started_at = Instant::now();
        let mut timing = StreamTiming {
            started_at,
            first_delta_ms: None,
            round,
        };
        let mut thinking = String::new();
        let mut thinking_active = false;
        let mut phase_deadline =
            timeout.map(|policy| started_at + std::time::Duration::from_millis(policy.connect_ms));
        let overall_deadline =
            timeout.map(|policy| started_at + std::time::Duration::from_millis(policy.overall_ms));
        let provider_result = loop {
            tokio::select! {
                response = &mut request => {
                    let success = response.is_ok();
                    info!(
                        round,
                        elapsed_ms = started_at.elapsed().as_millis() as u64,
                        first_delta_ms = timing.first_delta_ms,
                        success,
                        "模型响应完成"
                    );
                    break response;
                },
                event = provider_rx.recv() => {
                    if let Some(event) = event {
                        let response_started = observation.response_started;
                        let first_event = observation.first_event;
                        observation.observe(&event);
                        if let Some(policy) = timeout
                            && (observation.response_started != response_started ||
                                observation.first_event != first_event || event.is_semantic()) {
                            let duration = if !observation.first_event {
                                policy.first_event_ms
                            } else { policy.stream_idle_ms };
                            phase_deadline = Some(Instant::now() + std::time::Duration::from_millis(duration));
                        }
                        consume_provider_event(
                            event,
                            &mut assembler,
                            events,
                            &mut thinking,
                            &mut thinking_active,
                            &mut timing,
                        );
                    }
                }
                _ = cancellation.cancelled() => break Err(RuntimeError::Cancelled.into()),
                _ = async {
                    if let Some(deadline) = phase_deadline {
                        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
                    } else { std::future::pending::<()>().await; }
                } => {
                    let phase = if !observation.response_started { TimeoutPhase::Connect }
                        else if !observation.first_event { TimeoutPhase::FirstEvent }
                        else { TimeoutPhase::StreamIdle };
                    break Err(ProviderError::timeout(phase).into());
                }
                _ = async {
                    if let Some(deadline) = overall_deadline {
                        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
                    } else { std::future::pending::<()>().await; }
                } => break Err(ProviderError::timeout(TimeoutPhase::Overall).into()),
            }
        };
        while let Ok(event) = provider_rx.try_recv() {
            observation.observe(&event);
            consume_provider_event(
                event,
                &mut assembler,
                events,
                &mut thinking,
                &mut thinking_active,
                &mut timing,
            );
        }
        if let Err(error) = provider_result {
            finish_thinking(events, &mut thinking_active);
            if let Some(request_id) = trace_request_id {
                self.record_trace(SessionTraceRecord::ModelResponse {
                    timestamp_ms: unix_time_ms(),
                    request_id: request_id.to_owned(),
                    round,
                    duration_ms: started_at.elapsed().as_millis() as u64,
                    first_delta_ms: timing.first_delta_ms,
                    success: false,
                    response: None,
                    error: Some(format!("{error:#}")),
                })
                .await;
            }
            return Err(error);
        }
        finish_thinking(events, &mut thinking_active);
        if observation.response_started && !observation.protocol_done {
            return Err(
                ProviderError::new(ProviderErrorKind::Protocol, "Provider 流未正常结束").into(),
            );
        }
        let response = assembler.finish();
        if let Some(request_id) = trace_request_id {
            self.record_trace(SessionTraceRecord::ModelResponse {
                timestamp_ms: unix_time_ms(),
                request_id: request_id.to_owned(),
                round,
                duration_ms: started_at.elapsed().as_millis() as u64,
                first_delta_ms: timing.first_delta_ms,
                success: !matches!(response, Response::ToolAssemblyFailed(_)),
                response: Some(trace_response_value(
                    &response,
                    (!thinking.is_empty()).then_some(thinking.as_str()),
                )),
                error: match &response {
                    Response::ToolAssemblyFailed(error) => Some(error.message.clone()),
                    Response::Text(_) | Response::ToolCalls(_) => None,
                },
            })
            .await;
        }
        match &response {
            Response::Text(text) => info!(
                round,
                response_kind = "text",
                text_chars = text.chars().count(),
                "模型响应已装配"
            ),
            Response::ToolCalls(calls) => {
                let tool_names = calls
                    .iter()
                    .map(|call| call.name.as_str())
                    .collect::<Vec<_>>()
                    .join(",");
                info!(
                    round,
                    response_kind = "tool_calls",
                    tool_call_count = calls.len(),
                    tool_names,
                    "模型响应已装配"
                );
                for call in calls {
                    debug!(
                        round,
                        tool_call_id = %call.id,
                        tool = %call.name,
                        "模型生成工具调用"
                    );
                }
            }
            Response::ToolAssemblyFailed(error) => warn!(
                round,
                response_kind = "tool_assembly_failed",
                code = %error.code,
                message = %error.message,
                "模型响应装配失败"
            ),
        }
        Ok(ModelOutput {
            response,
            thinking: (!thinking.is_empty()).then_some(thinking),
        })
    }

    async fn record(&self, history: &mut Vec<Message>, message: Message) -> Result<()> {
        if let Some(session) = &self.session {
            session.append(&message).await?;
        }
        history.push(message);
        Ok(())
    }

    async fn record_trace(&self, record: SessionTraceRecord) {
        if let Some(session) = &self.session
            && let Err(error) = session.append_trace(&record).await
        {
            warn!(%error, "写入 session trace 失败");
        }
    }

    async fn execute_in_waves(
        &self,
        calls: &[ToolCall],
        events: Option<mpsc::UnboundedSender<AgentEvent>>,
        cancellation: &CancellationToken,
        round: usize,
        trace_request_id: Option<&str>,
    ) -> Result<Vec<ToolExecution>> {
        let mut results = Vec::with_capacity(calls.len());
        let mut cursor = 0;
        while cursor < calls.len() {
            if self.tools.is_read_only(&calls[cursor].name) {
                let mut end = cursor + 1;
                while end < calls.len() && self.tools.is_read_only(&calls[end].name) {
                    end += 1;
                }
                let batch = stream::iter(calls[cursor..end].to_vec())
                    .map(|call: ToolCall| {
                        let events = events.clone();
                        async move {
                            self.execute_one(&call, events, cancellation, round, trace_request_id)
                                .await
                        }
                    })
                    .buffered(8)
                    .collect::<Vec<Result<ToolExecution>>>()
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>>>()?;
                results.extend(batch);
                cursor = end;
            } else {
                results.push(
                    self.execute_one(
                        &calls[cursor],
                        events.clone(),
                        cancellation,
                        round,
                        trace_request_id,
                    )
                    .await?,
                );
                cursor += 1;
            }
        }
        Ok(results)
    }

    async fn execute_one(
        &self,
        call: &ToolCall,
        events: Option<mpsc::UnboundedSender<AgentEvent>>,
        cancellation: &CancellationToken,
        round: usize,
        trace_request_id: Option<&str>,
    ) -> Result<ToolExecution> {
        let started_at = Instant::now();
        let audit = TOOL_AUDIT.try_with(Clone::clone).ok();
        if let Some(audit) = &audit {
            audit.store.start_tool(&audit.run_id, round, &call.id)?;
        }
        debug!(
            tool_call_id = %call.id,
            tool = %call.name,
            round,
            "准备执行工具"
        );
        emit(
            &events,
            AgentEvent::ToolStarted {
                call_id: call.id.clone(),
                name: call.name.clone(),
                round,
            },
        );
        if let Some(request_id) = trace_request_id {
            self.record_trace(SessionTraceRecord::ToolStarted {
                timestamp_ms: unix_time_ms(),
                request_id: request_id.to_owned(),
                round,
                tool_call_id: call.id.clone(),
                name: call.name.clone(),
                arguments: call.arguments.clone(),
            })
            .await;
        }
        let (result, failed, error_message) = match TOOL_CALL_ID
            .scope(
                call.id.clone(),
                self.tools.execute_with_cancellation(
                    &call.name,
                    call.arguments.clone(),
                    cancellation,
                ),
            )
            .await
        {
            Ok(output) => (output, false, None),
            Err(error) => {
                warn!(tool = %call.name, %error, "工具执行失败，将错误回填给模型");
                let error_message = format!("工具执行错误: {error:#}");
                (
                    ToolOutput::text(error_message.clone()),
                    true,
                    Some(error_message),
                )
            }
        };
        if let Some(audit) = &audit {
            let artifact_ref = audit.store.store_tool_output(&result.content)?;
            let output_preview = result.content.chars().take(4096).collect::<String>();
            audit.store.finish_tool(&audit.run_id, round, &call.id,
                if failed { "tool_error" } else { "success" }, Some(&artifact_ref),
                &serde_json::json!({"success": !failed, "output": output_preview, "output_truncated": result.content.chars().count() > 4096,
                    "output_sha256": format!("{:x}", Sha256::digest(result.content.as_bytes())),
                    "error": error_message, "output_bytes": result.content.len(),
                    "duration_ms": started_at.elapsed().as_millis() as u64}))?;
        }
        emit(
            &events,
            AgentEvent::ToolFinished {
                call_id: call.id.clone(),
                name: call.name.clone(),
                output: result.content.clone(),
                round,
                duration_ms: started_at.elapsed().as_millis() as u64,
                success: !failed,
                error: error_message.clone(),
            },
        );
        if let Some(request_id) = trace_request_id {
            self.record_trace(SessionTraceRecord::ToolFinished {
                timestamp_ms: unix_time_ms(),
                request_id: request_id.to_owned(),
                round,
                tool_call_id: call.id.clone(),
                name: call.name.clone(),
                duration_ms: started_at.elapsed().as_millis() as u64,
                success: !failed,
                output: result.content.clone(),
                error: error_message.clone(),
            })
            .await;
        }
        info!(
            tool_call_id = %call.id,
            tool = %call.name,
            round,
            duration_ms = started_at.elapsed().as_millis() as u64,
            success = !failed,
            output_bytes = result.content.len(),
            "工具执行完成"
        );
        let fingerprint = ToolFingerprint {
            tool_name: call.name.clone(),
            arguments: fingerprint_json(&call.arguments),
            result: fingerprint_text(&result.content),
        };
        Ok(ToolExecution {
            message: Message::tool_result(call, result.content),
            transient_messages: result.transient_messages,
            fingerprint,
            failed,
            error: error_message,
        })
    }
}

fn without_images(messages: &[Message]) -> Vec<Message> {
    messages
        .iter()
        .cloned()
        .map(|mut message| {
            if !message.image_urls.is_empty() {
                let note = format!(
                    "[当前 provider 不支持图片，已省略 {} 个图片内容块]",
                    message.image_urls.len()
                );
                message.content = Some(match message.content.take() {
                    Some(content) if !content.is_empty() => format!("{content}\n{note}"),
                    _ => note,
                });
                message.image_urls.clear();
            }
            message
        })
        .collect()
}

fn emit(events: &Option<mpsc::UnboundedSender<AgentEvent>>, event: AgentEvent) {
    if let Some(events) = events {
        let _ = events.send(event);
    }
}

#[derive(Debug)]
struct ModelOutput {
    response: Response,
    thinking: Option<String>,
}

#[derive(Default)]
struct AttemptObservation {
    response_started: bool,
    first_event: bool,
    first_event_at_ms: Option<i64>,
    protocol_done: bool,
    stream_committed: bool,
    tool_call_seen: bool,
    output_truncated: bool,
    usage: Option<ProviderUsage>,
}

impl AttemptObservation {
    fn observe(&mut self, event: &ProviderEvent) {
        match event {
            ProviderEvent::ResponseStarted => self.response_started = true,
            ProviderEvent::ProtocolDone => self.protocol_done = true,
            ProviderEvent::Usage(usage) => self.usage.get_or_insert_default().merge_partial(usage),
            ProviderEvent::TextDelta(text) | ProviderEvent::ThinkingDelta(text) => {
                if !text.is_empty() {
                    self.stream_committed = true;
                    self.mark_first_event();
                }
            }
            ProviderEvent::ToolCallStarted { .. }
            | ProviderEvent::ToolCallDelta { .. }
            | ProviderEvent::ToolCallCompleted { .. }
            | ProviderEvent::ToolCallFailed(_) => {
                self.tool_call_seen = true;
                self.stream_committed = true;
                self.mark_first_event();
            }
            ProviderEvent::OutputTruncated => {
                self.output_truncated = true;
                self.mark_first_event();
            }
        }
    }

    fn mark_first_event(&mut self) {
        if !self.first_event {
            self.first_event = true;
            self.first_event_at_ms = Some(unix_time_ms() as i64);
        }
    }
}

fn finish_attempt(
    audit: &ToolAuditContext,
    attempt: &mut ProviderAttempt,
    observation: &AttemptObservation,
) -> Result<()> {
    attempt.stream_committed = observation.stream_committed;
    attempt.first_event_at_ms = observation.first_event_at_ms;
    attempt.finished_at_ms = Some(unix_time_ms() as i64);
    attempt.usage = observation.usage.clone();
    audit.store.finish_provider_attempt(attempt)?;
    Ok(())
}

struct StreamTiming {
    started_at: Instant,
    first_delta_ms: Option<u64>,
    round: usize,
}

fn consume_provider_event(
    event: ProviderEvent,
    assembler: &mut ToolCallAssembler,
    events: &Option<mpsc::UnboundedSender<AgentEvent>>,
    thinking: &mut String,
    thinking_active: &mut bool,
    timing: &mut StreamTiming,
) {
    if let ProviderEvent::ThinkingDelta(delta) = event {
        if !delta.is_empty() {
            thinking.push_str(&delta);
            *thinking_active = true;
            emit(events, AgentEvent::ThinkingDelta(delta));
        }
        return;
    }
    if let Some(delta) = assembler.accept(event) {
        finish_thinking(events, thinking_active);
        if timing.first_delta_ms.is_none() {
            timing.first_delta_ms = Some(timing.started_at.elapsed().as_millis() as u64);
            info!(
                round = timing.round,
                first_delta_ms = ?timing.first_delta_ms,
                "收到模型首个流式增量"
            );
        }
        emit(events, AgentEvent::TextDelta(delta));
    }
}

fn finish_thinking(events: &Option<mpsc::UnboundedSender<AgentEvent>>, thinking_active: &mut bool) {
    if *thinking_active {
        emit(events, AgentEvent::ThinkingFinished);
        *thinking_active = false;
    }
}

fn trace_response_value(response: &Response, thinking: Option<&str>) -> serde_json::Value {
    let mut value = match response {
        Response::Text(text) => serde_json::json!({
            "type": "text",
            "content": text,
        }),
        Response::ToolCalls(calls) => serde_json::json!({
            "type": "tool_calls",
            "tool_calls": calls,
        }),
        Response::ToolAssemblyFailed(error) => serde_json::json!({
            "type": "tool_assembly_failed",
            "code": error.code,
            "message": error.message,
        }),
    };
    if let Some(thinking) = thinking.filter(|thinking| !thinking.is_empty()) {
        value["thinking"] = serde_json::Value::String(thinking.to_owned());
    }
    value
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

struct ToolExecution {
    message: Message,
    transient_messages: Vec<Message>,
    fingerprint: ToolFingerprint,
    failed: bool,
    error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ToolFingerprint {
    tool_name: String,
    arguments: u64,
    result: u64,
}

#[derive(Default)]
struct RepeatDetector {
    last: Option<Vec<ToolFingerprint>>,
    count: usize,
}

impl RepeatDetector {
    fn observe(&mut self, fingerprints: Vec<ToolFingerprint>) -> usize {
        if self.last.as_ref() == Some(&fingerprints) {
            self.count = self.count.saturating_add(1);
        } else {
            self.last = Some(fingerprints);
            self.count = 1;
        }
        self.count
    }
}

fn fingerprint_json(value: &serde_json::Value) -> u64 {
    serde_json::to_string(value).map_or_else(
        |_| fingerprint_text("<invalid-json>"),
        |text| fingerprint_text(&text),
    )
}

fn fingerprint_text(text: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    text.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use anyhow::Result;
    use async_trait::async_trait;
    use serde_json::json;

    use super::*;
    use crate::context::ContextConfig;
    use crate::plan::PlanStore;
    use crate::provider::{ToolCall, ToolSpec};
    use crate::safety::{Approval, SafetyPolicy};
    use crate::session::SessionStore;
    use crate::tools::{ExecTool, ReadFileTool, ToolOutput};

    struct AllowApproval;

    #[async_trait]
    impl Approval for AllowApproval {
        async fn request(&self, _prompt: &str) -> Result<bool> {
            Ok(true)
        }
    }

    struct MockProvider {
        responses: Mutex<VecDeque<Response>>,
        snapshots: Mutex<Vec<Vec<Message>>>,
    }

    struct ThinkingProvider;

    struct PendingProvider;

    struct FailingTool;

    #[async_trait]
    impl crate::tools::Tool for FailingTool {
        fn name(&self) -> &str {
            "failing_tool"
        }

        fn description(&self) -> &str {
            "总是失败的测试工具"
        }

        fn parameters(&self) -> serde_json::Value {
            json!({"type": "object"})
        }

        async fn execute(&self, _args: serde_json::Value) -> Result<String> {
            anyhow::bail!("测试工具故意失败")
        }
    }

    fn test_context(provider: Arc<dyn Provider>) -> ContextManager {
        ContextManager::new(
            provider,
            std::env::current_dir().unwrap(),
            ContextConfig {
                token_budget: 1_000_000,
                recent_messages: 100,
                mild_compression_percent: 60,
                strong_compression_percent: 85,
                summary_chunk_tokens: 100_000,
            },
            Arc::new(PlanStore::memory_only()),
        )
        .unwrap()
    }

    fn test_session(name: &str) -> Arc<SessionStore> {
        let path =
            std::env::temp_dir().join(format!("my-agent-loop-{}-{name}.jsonl", std::process::id()));
        let _ = std::fs::remove_file(&path);
        Arc::new(SessionStore::new(path))
    }

    #[async_trait]
    impl Provider for MockProvider {
        async fn chat(&self, messages: &[Message], _tools: &[ToolSpec]) -> Result<Response> {
            self.snapshots.lock().unwrap().push(messages.to_vec());
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| anyhow::anyhow!("mock 响应不足"))
        }
    }

    #[async_trait]
    impl Provider for ThinkingProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[ToolSpec],
            events: tokio::sync::mpsc::UnboundedSender<crate::provider::ProviderEvent>,
        ) -> Result<()> {
            use crate::provider::ProviderEvent;

            events.send(ProviderEvent::ThinkingDelta("先分析".to_owned()))?;
            events.send(ProviderEvent::ThinkingDelta("，再回答".to_owned()))?;
            events.send(ProviderEvent::TextDelta("最终回答".to_owned()))?;
            Ok(())
        }
    }

    #[async_trait]
    impl Provider for PendingProvider {
        async fn chat(&self, _messages: &[Message], _tools: &[ToolSpec]) -> Result<Response> {
            std::future::pending::<Result<Response>>().await
        }
    }

    #[tokio::test]
    async fn queued_turn_can_be_cancelled_before_acquiring_session_lock() {
        let provider: Arc<dyn Provider> = Arc::new(PendingProvider);
        let engine = Arc::new(LoopEngine::new(
            provider.clone(),
            ToolRegistry::new(),
            test_context(provider),
            test_session("queued-cancel"),
        ));
        let first_token = CancellationToken::new();
        let first_token_for_task = first_token.clone();
        let (events, mut event_receiver) = mpsc::unbounded_channel();
        let first_engine = engine.clone();
        let first_handle = tokio::spawn(async move {
            let mut history = Vec::new();
            first_engine
                .run_turn_with_events(
                    &mut history,
                    "第一个请求".to_owned(),
                    Some(events),
                    first_token_for_task,
                )
                .await
        });
        assert_eq!(event_receiver.recv().await, Some(AgentEvent::TurnStarted));

        let second_token = CancellationToken::new();
        let second_engine = engine;
        let second_token_for_task = second_token.clone();
        let second_handle = tokio::spawn(async move {
            let mut history = Vec::new();
            second_engine
                .run_turn_with_events(
                    &mut history,
                    "第二个请求".to_owned(),
                    None,
                    second_token_for_task,
                )
                .await
        });
        second_token.cancel();
        let second_result = tokio::time::timeout(std::time::Duration::from_secs(1), second_handle)
            .await
            .expect("排队请求取消不应等待第一个请求结束")
            .expect("排队请求任务不应 panic");
        assert_eq!(second_result.unwrap_err().to_string(), "请求已取消");

        first_token.cancel();
        let first_result = tokio::time::timeout(std::time::Duration::from_secs(1), first_handle)
            .await
            .expect("第一个请求应响应取消")
            .expect("第一个请求任务不应 panic");
        assert_eq!(first_result.unwrap_err().to_string(), "请求已取消");
    }

    #[tokio::test]
    async fn streams_thinking_before_text_and_persists_it_in_session_history() {
        let provider: Arc<dyn Provider> = Arc::new(ThinkingProvider);
        let session = test_session("thinking-stream");
        let engine = LoopEngine::new(
            provider.clone(),
            ToolRegistry::new(),
            test_context(provider),
            session.clone(),
        );
        let (events, mut receiver) = mpsc::unbounded_channel();
        let mut history = Vec::new();

        let answer = engine
            .run_turn_with_events_for_request(
                &mut history,
                "请先思考再回答".to_owned(),
                Some(events),
                CancellationToken::new(),
                Some("thinking-request".to_owned()),
            )
            .await
            .unwrap();

        assert_eq!(answer, "最终回答");
        let captured = std::iter::from_fn(|| receiver.try_recv().ok()).collect::<Vec<_>>();
        assert!(matches!(captured[0], AgentEvent::TurnStarted));
        assert_eq!(
            captured
                .iter()
                .filter_map(|event| match event {
                    AgentEvent::ThinkingDelta(delta) => Some(delta.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec!["先分析", "，再回答"]
        );
        let thinking_finished = captured
            .iter()
            .position(|event| matches!(event, AgentEvent::ThinkingFinished))
            .expect("思考完成事件应在正文前发出");
        let text_delta = captured
            .iter()
            .position(|event| matches!(event, AgentEvent::TextDelta(delta) if delta == "最终回答"))
            .expect("正文增量应发出");
        assert!(thinking_finished < text_delta);
        assert!(
            matches!(captured.last(), Some(AgentEvent::TurnCompleted { content }) if content == "最终回答")
        );
        assert_eq!(
            history
                .last()
                .and_then(|message| message.thinking.as_deref()),
            Some("先分析，再回答")
        );
        assert!(
            session
                .load_trace()
                .await
                .unwrap()
                .iter()
                .any(|record| matches!(
                    record,
                    SessionTraceRecord::ModelResponse { response: Some(response), .. }
                        if response["thinking"] == "先分析，再回答"
                ))
        );
    }

    #[tokio::test]
    async fn executes_tool_and_pairs_result_by_call_id() {
        let workspace = std::env::current_dir().unwrap();
        let temp_path = workspace.join(format!(
            "my-agent-phase1-{}-{}.txt",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        std::fs::write(&temp_path, "这是 README 内容").unwrap();
        let call = ToolCall {
            id: "call-read-1".to_owned(),
            name: "read_file".to_owned(),
            arguments: json!({"path": temp_path}),
        };
        let provider = Arc::new(MockProvider {
            responses: Mutex::new(VecDeque::from([
                Response::ToolCalls(vec![call]),
                Response::Text("总结完成".to_owned()),
            ])),
            snapshots: Mutex::new(Vec::new()),
        });
        let mut registry = ToolRegistry::new();
        let safety = Arc::new(SafetyPolicy::new(&workspace, Arc::new(AllowApproval)).unwrap());
        registry.register(ReadFileTool::new(safety));
        let engine = LoopEngine::new(
            provider.clone(),
            registry,
            test_context(provider.clone()),
            test_session("pairing"),
        );
        let mut history = Vec::new();

        let answer = engine
            .run_turn(&mut history, "读取并总结".to_owned())
            .await
            .unwrap();

        assert_eq!(answer, "总结完成");
        let snapshots = provider.snapshots.lock().unwrap();
        assert_eq!(snapshots.len(), 2);
        let tool_result = snapshots[1]
            .iter()
            .find(|message: &&Message| {
                message
                    .tool_call_id
                    .as_deref()
                    .and_then(crate::provider::outbound_wire_id)
                    .as_deref()
                    == Some("call-read-1")
            })
            .unwrap();
        assert_eq!(tool_result.content.as_deref(), Some("这是 README 内容"));
        let _ = std::fs::remove_file(temp_path);
    }

    #[tokio::test]
    async fn stops_after_consecutive_tool_failures() {
        let call = || ToolCall {
            id: "call-fail".to_owned(),
            name: "failing_tool".to_owned(),
            arguments: json!({}),
        };
        let provider = Arc::new(MockProvider {
            responses: Mutex::new(VecDeque::from([
                Response::ToolCalls(vec![call()]),
                Response::ToolCalls(vec![call()]),
                Response::ToolCalls(vec![call()]),
            ])),
            snapshots: Mutex::new(Vec::new()),
        });
        let mut registry = ToolRegistry::new();
        registry.register(FailingTool);
        let engine = LoopEngine::ephemeral(provider.clone(), registry, test_context(provider), 10);

        let error = engine
            .run_turn(&mut Vec::new(), "执行失败工具".to_owned())
            .await
            .expect_err("连续工具失败应触发熔断");

        assert!(error.to_string().contains("连续 3 次工具执行失败"));
    }

    #[tokio::test]
    async fn stops_at_round_limit() {
        let provider = Arc::new(MockProvider {
            responses: Mutex::new(VecDeque::from([Response::ToolCalls(Vec::new())])),
            snapshots: Mutex::new(Vec::new()),
        });
        let engine = LoopEngine::ephemeral(
            provider.clone(),
            ToolRegistry::new(),
            test_context(provider),
            1,
        );
        let error = engine
            .run_turn(&mut Vec::new(), "继续".to_owned())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("最大轮次"));
    }

    #[tokio::test]
    async fn executes_exec_and_returns_output_to_provider() {
        let call = ToolCall {
            id: "call-exec-1".to_owned(),
            name: "exec".to_owned(),
            arguments: json!({"command": "find . -maxdepth 1 -type f | wc -l"}),
        };
        let provider = Arc::new(MockProvider {
            responses: Mutex::new(VecDeque::from([
                Response::ToolCalls(vec![call]),
                Response::Text("已完成文件计数".to_owned()),
            ])),
            snapshots: Mutex::new(Vec::new()),
        });
        let safety = Arc::new(
            SafetyPolicy::new(std::env::current_dir().unwrap(), Arc::new(AllowApproval)).unwrap(),
        );
        let mut registry = ToolRegistry::new();
        registry.register(ExecTool::new(safety));
        let engine = LoopEngine::new(
            provider.clone(),
            registry,
            test_context(provider.clone()),
            test_session("exec"),
        );

        let answer = engine
            .run_turn(&mut Vec::new(), "统计当前目录文件数".to_owned())
            .await
            .unwrap();

        assert_eq!(answer, "已完成文件计数");
        let snapshots = provider.snapshots.lock().unwrap();
        let result = snapshots[1]
            .iter()
            .find(|message: &&Message| {
                message
                    .tool_call_id
                    .as_deref()
                    .and_then(crate::provider::outbound_wire_id)
                    .as_deref()
                    == Some("call-exec-1")
            })
            .unwrap();
        assert!(result.content.as_deref().unwrap().contains("exit_code: 0"));
    }

    struct ParallelProbe {
        active: std::sync::atomic::AtomicUsize,
        peak: std::sync::atomic::AtomicUsize,
    }

    struct DelayedReadTool {
        name: &'static str,
        probe: Arc<ParallelProbe>,
    }

    #[async_trait]
    impl crate::tools::Tool for DelayedReadTool {
        fn name(&self) -> &str {
            self.name
        }

        fn description(&self) -> &str {
            "并行测试工具"
        }

        fn parameters(&self) -> serde_json::Value {
            json!({"type": "object"})
        }

        fn is_read_only(&self) -> bool {
            true
        }

        async fn execute(&self, _args: serde_json::Value) -> Result<String> {
            use std::sync::atomic::Ordering;

            let active = self.probe.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.probe.peak.fetch_max(active, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            self.probe.active.fetch_sub(1, Ordering::SeqCst);
            Ok(self.name.to_owned())
        }
    }

    #[tokio::test]
    async fn runs_consecutive_read_only_calls_in_parallel() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let calls = ["read_a", "read_b", "read_c"]
            .into_iter()
            .enumerate()
            .map(|(index, name): (usize, &str)| ToolCall {
                id: format!("call-{index}"),
                name: name.to_owned(),
                arguments: json!({}),
            })
            .collect::<Vec<ToolCall>>();
        let provider = Arc::new(MockProvider {
            responses: Mutex::new(VecDeque::from([
                Response::ToolCalls(calls),
                Response::Text("完成".to_owned()),
            ])),
            snapshots: Mutex::new(Vec::new()),
        });
        let probe = Arc::new(ParallelProbe {
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        });
        let mut registry = ToolRegistry::new();
        for name in ["read_a", "read_b", "read_c"] {
            registry.register(DelayedReadTool {
                name,
                probe: probe.clone(),
            });
        }
        let engine = LoopEngine::new(
            provider.clone(),
            registry,
            test_context(provider),
            test_session("parallel"),
        );

        engine
            .run_turn(&mut Vec::new(), "并行读取".to_owned())
            .await
            .unwrap();

        assert!(probe.peak.load(Ordering::SeqCst) >= 2);
    }

    struct ImageProbeTool;

    #[async_trait]
    impl crate::tools::Tool for ImageProbeTool {
        fn name(&self) -> &str {
            "image_probe"
        }

        fn description(&self) -> &str {
            "返回测试图片"
        }

        fn parameters(&self) -> serde_json::Value {
            json!({"type": "object"})
        }

        async fn execute(&self, _args: serde_json::Value) -> Result<String> {
            Ok("已读取图片".to_owned())
        }

        async fn execute_rich(&self, _args: serde_json::Value) -> Result<ToolOutput> {
            Ok(ToolOutput {
                content: "已读取图片".to_owned(),
                transient_messages: vec![Message::user_with_images(
                    "测试图片",
                    vec!["data:image/png;base64,AAAA".to_owned()],
                )],
            })
        }
    }

    #[tokio::test]
    async fn sends_image_transiently_without_persisting_base64() {
        let call = ToolCall {
            id: "call-image-1".to_owned(),
            name: "image_probe".to_owned(),
            arguments: json!({}),
        };
        let provider = Arc::new(MockProvider {
            responses: Mutex::new(VecDeque::from([
                Response::ToolCalls(vec![call]),
                Response::Text("看到了图片".to_owned()),
            ])),
            snapshots: Mutex::new(Vec::new()),
        });
        let mut registry = ToolRegistry::new();
        registry.register(ImageProbeTool);
        let engine = LoopEngine::new(
            provider.clone(),
            registry,
            test_context(provider.clone()),
            test_session("image"),
        );
        let mut history = Vec::new();

        engine
            .run_turn(&mut history, "分析图片".to_owned())
            .await
            .unwrap();

        let snapshots = provider.snapshots.lock().unwrap();
        assert!(
            snapshots[1]
                .iter()
                .any(|message: &Message| !message.image_urls.is_empty())
        );
        assert!(
            history
                .iter()
                .all(|message: &Message| message.image_urls.is_empty())
        );
    }

    struct RepeatingTool {
        executions: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait]
    impl crate::tools::Tool for RepeatingTool {
        fn name(&self) -> &str {
            "repeat_probe"
        }

        fn description(&self) -> &str {
            "重复检测测试工具"
        }

        fn parameters(&self) -> serde_json::Value {
            json!({"type": "object"})
        }

        fn is_read_only(&self) -> bool {
            true
        }

        async fn execute(&self, _args: serde_json::Value) -> Result<String> {
            self.executions
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok("相同结果".to_owned())
        }
    }

    #[tokio::test]
    async fn gently_reminds_after_three_identical_tool_results_without_disabling_tool() {
        let calls = (1..=3)
            .map(|index: usize| {
                Response::ToolCalls(vec![ToolCall {
                    id: format!("repeat-{index}"),
                    name: "repeat_probe".to_owned(),
                    arguments: json!({"same": true}),
                }])
            })
            .chain(std::iter::once(Response::Text("换个思路后完成".to_owned())))
            .collect::<VecDeque<Response>>();
        let provider = Arc::new(MockProvider {
            responses: Mutex::new(calls),
            snapshots: Mutex::new(Vec::new()),
        });
        let executions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut registry = ToolRegistry::new();
        registry.register(RepeatingTool {
            executions: executions.clone(),
        });
        let engine = LoopEngine::new(
            provider.clone(),
            registry,
            test_context(provider.clone()),
            test_session("repeat"),
        );

        let answer = engine
            .run_turn(&mut Vec::new(), "触发重复".to_owned())
            .await
            .unwrap();

        assert_eq!(answer, "换个思路后完成");
        assert_eq!(executions.load(std::sync::atomic::Ordering::SeqCst), 3);
        let snapshots = provider.snapshots.lock().unwrap();
        assert!(snapshots[3].iter().any(|message: &Message| {
            message.role == Role::System
                && message
                    .content
                    .as_deref()
                    .is_some_and(|content: &str| content.contains("检测到连续重复"))
        }));
        assert!(
            engine
                .tools
                .specs()
                .iter()
                .any(|spec: &ToolSpec| spec.name == "repeat_probe")
        );
    }

    #[tokio::test]
    async fn stops_a_genuine_identical_no_progress_loop() {
        let call = || ToolCall {
            id: "call-repeat".to_owned(),
            name: "repeat_probe".to_owned(),
            arguments: json!({"value": "same"}),
        };
        let provider = Arc::new(MockProvider {
            responses: Mutex::new(VecDeque::from(
                (0..REPETITION_ABORT_THRESHOLD)
                    .map(|_| Response::ToolCalls(vec![call()]))
                    .collect::<Vec<_>>(),
            )),
            snapshots: Mutex::new(Vec::new()),
        });
        let executions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut registry = ToolRegistry::new();
        registry.register(RepeatingTool {
            executions: executions.clone(),
        });
        let engine = LoopEngine::new(
            provider.clone(),
            registry,
            test_context(provider),
            test_session("repeat-abort"),
        );

        let error = engine
            .run_turn(&mut Vec::new(), "触发无进展循环".to_owned())
            .await
            .expect_err("完全相同的调用和结果不应无限循环");

        assert!(error.to_string().contains("无进展循环"));
        assert!(!error.to_string().contains("最大轮次"));
        assert_eq!(
            executions.load(std::sync::atomic::Ordering::SeqCst),
            REPETITION_ABORT_THRESHOLD
        );
    }

    struct InvalidArgumentsStream {
        requests: std::sync::atomic::AtomicUsize,
    }

    struct TruncatedToolStream {
        requests: std::sync::atomic::AtomicUsize,
    }

    #[async_trait]
    impl Provider for InvalidArgumentsStream {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[ToolSpec],
            events: tokio::sync::mpsc::UnboundedSender<crate::provider::ProviderEvent>,
        ) -> Result<()> {
            use crate::provider::{
                ApiType, ExecutionIdentity, ProviderEvent, ToolArgumentsFragment,
            };
            if self
                .requests
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                == 0
            {
                for (wire_id, arguments) in [("first", "{}"), ("broken", "{")] {
                    let exec_id = ExecutionIdentity::wire(ApiType::OpenaiChat, wire_id);
                    events.send(ProviderEvent::ToolCallStarted {
                        exec_id: exec_id.clone(),
                        name: "side_effect".to_owned(),
                    })?;
                    events.send(ProviderEvent::ToolCallDelta {
                        exec_id: exec_id.clone(),
                        fragment: ToolArgumentsFragment::Append(arguments.to_owned()),
                    })?;
                    events.send(ProviderEvent::ToolCallCompleted { exec_id })?;
                }
            } else {
                events.send(ProviderEvent::TextDelta("已修正".to_owned()))?;
            }
            Ok(())
        }
    }

    #[async_trait]
    impl Provider for TruncatedToolStream {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[ToolSpec],
            events: tokio::sync::mpsc::UnboundedSender<crate::provider::ProviderEvent>,
        ) -> Result<()> {
            use crate::provider::{
                ApiType, ExecutionIdentity, ProviderEvent, ToolArgumentsFragment,
            };
            if self
                .requests
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                == 0
            {
                let exec_id = ExecutionIdentity::wire(ApiType::OpenaiChat, "truncated-call");
                events.send(ProviderEvent::ToolCallStarted {
                    exec_id: exec_id.clone(),
                    name: "side_effect".to_owned(),
                })?;
                events.send(ProviderEvent::ToolCallDelta {
                    exec_id: exec_id.clone(),
                    fragment: ToolArgumentsFragment::Append(r#"{"value":"unsafe"}"#.to_owned()),
                })?;
                events.send(ProviderEvent::ToolCallCompleted { exec_id })?;
                events.send(ProviderEvent::OutputTruncated)?;
            } else {
                events.send(ProviderEvent::TextDelta("已改为安全回答".to_owned()))?;
            }
            Ok(())
        }
    }

    struct SideEffectProbe {
        name: &'static str,
        executions: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait]
    impl crate::tools::Tool for SideEffectProbe {
        fn name(&self) -> &str {
            self.name
        }

        fn description(&self) -> &str {
            "原子拒绝测试工具"
        }

        fn parameters(&self) -> serde_json::Value {
            json!({
                "type": "object",
                "properties": {"value": {"type": "string"}},
                "required": ["value"],
                "additionalProperties": false
            })
        }

        async fn execute(&self, _args: serde_json::Value) -> Result<String> {
            self.executions
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok("executed".to_owned())
        }
    }

    #[tokio::test]
    async fn main_turn_continues_past_fifty_rounds_and_completes() {
        let mut responses = (0..=DEFAULT_PROGRESS_CHECKPOINT_ROUNDS)
            .map(|index| {
                Response::ToolCalls(vec![ToolCall {
                    id: format!("long-call-{index}"),
                    name: "side_effect".to_owned(),
                    arguments: json!({"value": index.to_string()}),
                }])
            })
            .collect::<Vec<_>>();
        responses.push(Response::Text("长任务完成".to_owned()));
        let provider = Arc::new(MockProvider {
            responses: Mutex::new(VecDeque::from(responses)),
            snapshots: Mutex::new(Vec::new()),
        });
        let executions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut registry = ToolRegistry::new();
        registry.register(SideEffectProbe {
            name: "side_effect",
            executions: executions.clone(),
        });
        let engine = LoopEngine::new(
            provider.clone(),
            registry,
            test_context(provider.clone()),
            test_session("past-fifty"),
        );

        let answer = engine
            .run_turn(&mut Vec::new(), "执行超过五十轮的长任务".to_owned())
            .await
            .unwrap();

        assert_eq!(answer, "长任务完成");
        assert_eq!(
            executions.load(std::sync::atomic::Ordering::SeqCst),
            DEFAULT_PROGRESS_CHECKPOINT_ROUNDS + 1
        );
        let snapshots = provider.snapshots.lock().unwrap();
        assert!(
            snapshots[DEFAULT_PROGRESS_CHECKPOINT_ROUNDS]
                .iter()
                .any(|message| {
                    message.role == Role::System
                        && message.content.as_deref() == Some(PROGRESS_CHECKPOINT_REMINDER)
                })
        );
    }

    #[tokio::test]
    async fn audited_turn_records_exact_model_input_response_and_timing() {
        let provider = Arc::new(MockProvider {
            responses: Mutex::new(VecDeque::from([Response::Text("审计回答".to_owned())])),
            snapshots: Mutex::new(Vec::new()),
        });
        let session = test_session("structured-trace");
        let engine = LoopEngine::new(
            provider.clone(),
            ToolRegistry::new(),
            test_context(provider),
            session.clone(),
        );

        let answer = engine
            .run_turn_with_events_for_request(
                &mut Vec::new(),
                "审计问题".to_owned(),
                None,
                CancellationToken::new(),
                Some("web-request-9".to_owned()),
            )
            .await
            .unwrap();
        let records = session.load_trace().await.unwrap();

        assert_eq!(answer, "审计回答");
        assert!(matches!(
            records.first(),
            Some(SessionTraceRecord::TurnStarted { request_id, input, .. })
                if request_id == "web-request-9" && input == "审计问题"
        ));
        assert!(records.iter().any(|record| matches!(
            record,
            SessionTraceRecord::ModelRequest { request_id, messages, .. }
                if request_id == "web-request-9"
                    && messages.iter().any(|message| message.content.as_deref() == Some("审计问题"))
        )));
        assert!(records.iter().any(|record| matches!(
            record,
            SessionTraceRecord::ModelResponse { response: Some(response), success: true, .. }
                if response["content"] == "审计回答"
        )));
        assert!(matches!(
            records.last(),
            Some(SessionTraceRecord::TurnCompleted { success: true, .. })
        ));
    }

    #[tokio::test]
    async fn assembly_failure_rejects_the_whole_wave_before_side_effects() {
        let provider = Arc::new(InvalidArgumentsStream {
            requests: std::sync::atomic::AtomicUsize::new(0),
        });
        let executions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut registry = ToolRegistry::new();
        registry.register(SideEffectProbe {
            name: "side_effect",
            executions: executions.clone(),
        });
        let engine = LoopEngine::new(
            provider.clone(),
            registry,
            test_context(provider),
            test_session("assembly-fail-closed"),
        );
        let mut history = Vec::new();

        let answer = engine
            .run_turn(&mut history, "测试装配失败".to_owned())
            .await
            .unwrap();

        assert_eq!(answer, "已修正");
        assert_eq!(executions.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(history.iter().any(|message| {
            message
                .content
                .as_deref()
                .is_some_and(|content| content.contains("tool_call_assembly_error:invalid_json"))
        }));
    }

    #[tokio::test]
    async fn truncated_tool_batch_is_rejected_before_side_effects_and_retried() {
        let provider = Arc::new(TruncatedToolStream {
            requests: std::sync::atomic::AtomicUsize::new(0),
        });
        let executions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut registry = ToolRegistry::new();
        registry.register(SideEffectProbe {
            name: "side_effect",
            executions: executions.clone(),
        });
        let engine = LoopEngine::new(
            provider.clone(),
            registry,
            test_context(provider),
            test_session("truncated-tool-batch"),
        );
        let mut history = Vec::new();

        let answer = engine
            .run_turn(&mut history, "不要执行截断调用".to_owned())
            .await
            .unwrap();

        assert_eq!(answer, "已改为安全回答");
        assert_eq!(executions.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(history.iter().any(|message| {
            message.content.as_deref().is_some_and(|content| {
                content.contains("tool_call_assembly_error:output_truncated")
            })
        }));
    }

    #[tokio::test]
    async fn admission_failure_rejects_all_calls_before_side_effects() {
        let provider = Arc::new(MockProvider {
            responses: Mutex::new(VecDeque::from([
                Response::ToolCalls(vec![
                    ToolCall {
                        id: "valid".to_owned(),
                        name: "first_effect".to_owned(),
                        arguments: json!({"value": "ok"}),
                    },
                    ToolCall {
                        id: "invalid".to_owned(),
                        name: "second_effect".to_owned(),
                        arguments: json!({}),
                    },
                ]),
                Response::Text("已修正".to_owned()),
            ])),
            snapshots: Mutex::new(Vec::new()),
        });
        let executions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut registry = ToolRegistry::new();
        for name in ["first_effect", "second_effect"] {
            registry.register(SideEffectProbe {
                name,
                executions: executions.clone(),
            });
        }
        let engine = LoopEngine::new(
            provider.clone(),
            registry,
            test_context(provider),
            test_session("admission-fail-closed"),
        );

        let answer = engine
            .run_turn(&mut Vec::new(), "测试准入失败".to_owned())
            .await
            .unwrap();

        assert_eq!(answer, "已修正");
        assert_eq!(executions.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
}

#[cfg(test)]
mod resilience_tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;
    use crate::context::ContextConfig;
    use crate::daemon::protocol::RequestId;
    use crate::plan::PlanStore;
    use crate::provider::wire_tests::{MockHttpResponse, start_mock_http};
    use crate::provider::{
        ApiType, CircuitBreaker, ContextPolicySnapshot, RetryPolicy, RouteCandidate, RouteSnapshot,
    };
    use crate::storage::{Admission, AdmissionMode, SessionId};

    struct Script {
        events: Vec<(u64, ProviderEvent)>,
        error: Option<ProviderError>,
        hang: bool,
    }

    struct ScriptedProvider {
        scripts: Mutex<VecDeque<Script>>,
        calls: AtomicUsize,
    }

    impl ScriptedProvider {
        fn new(scripts: Vec<Script>) -> Arc<Self> {
            Arc::new(Self {
                scripts: Mutex::new(scripts.into()),
                calls: AtomicUsize::new(0),
            })
        }
        fn count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl Provider for ScriptedProvider {
        async fn chat_stream(
            &self,
            _: &[Message],
            _: &[crate::provider::ToolSpec],
            sender: mpsc::UnboundedSender<ProviderEvent>,
        ) -> Result<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let script = self
                .scripts
                .lock()
                .unwrap()
                .pop_front()
                .expect("mock 请求超过脚本");
            for (delay, event) in script.events {
                tokio::time::sleep(Duration::from_millis(delay)).await;
                sender.send(event)?;
            }
            if script.hang {
                std::future::pending::<()>().await;
            }
            if let Some(error) = script.error {
                Err(error.into())
            } else {
                Ok(())
            }
        }
    }

    fn success(text: &str, usage: Option<ProviderUsage>) -> Script {
        let mut events = vec![
            (0, ProviderEvent::ResponseStarted),
            (0, ProviderEvent::TextDelta(text.into())),
        ];
        if let Some(usage) = usage {
            events.push((0, ProviderEvent::Usage(usage)));
        }
        events.push((0, ProviderEvent::ProtocolDone));
        Script {
            events,
            error: None,
            hang: false,
        }
    }

    fn failure(kind: ProviderErrorKind) -> Script {
        Script {
            events: vec![(0, ProviderEvent::ResponseStarted)],
            error: Some(ProviderError::new(kind, "mock Provider 错误")),
            hang: false,
        }
    }

    fn engine(
        providers: Vec<Arc<dyn Provider>>,
        retry: RetryPolicy,
        timeout: TimeoutPolicy,
    ) -> LoopEngine {
        let primary = providers[0].clone();
        let context = ContextManager::new(
            primary.clone(),
            std::env::current_dir().unwrap(),
            ContextConfig {
                token_budget: 100_000,
                recent_messages: 1,
                mild_compression_percent: 60,
                strong_compression_percent: 85,
                summary_chunk_tokens: 1000,
            },
            Arc::new(PlanStore::memory_only()),
        )
        .unwrap();
        let engine = LoopEngine::ephemeral(primary, ToolRegistry::new(), context, 1);
        let snapshot = RouteSnapshot {
            candidates: providers
                .iter()
                .enumerate()
                .map(|(index, _)| RouteCandidate {
                    profile_id: format!("candidate-{index}"),
                    api_type: ApiType::OpenaiChat,
                    model: "mock".into(),
                    base_url_sha256: "mock".into(),
                })
                .collect(),
            retry_policy: retry,
            timeout_policy: timeout,
            context_policy: ContextPolicySnapshot {
                token_budget: 100_000,
            },
            config_generation: 1,
        };
        engine.with_route(FrozenRoute {
            snapshot,
            providers,
            circuit: Arc::new(CircuitBreaker::default()),
        })
    }

    async fn request(
        engine: &LoopEngine,
        events: Option<mpsc::UnboundedSender<AgentEvent>>,
        cancellation: &CancellationToken,
    ) -> Result<ModelOutput> {
        engine
            .request_model(
                &[Message::text(Role::User, "private input")],
                &[],
                &events,
                cancellation,
                1,
                None,
            )
            .await
    }

    fn no_retry() -> RetryPolicy {
        RetryPolicy {
            rate_limit_retries: 0,
            transport_retries: 0,
            server_retries: 0,
            timeout_retries: 0,
            protocol_retries: 0,
            max_backoff_ms: 10,
        }
    }

    fn short_timeout() -> TimeoutPolicy {
        TimeoutPolicy {
            connect_ms: 25,
            first_event_ms: 25,
            stream_idle_ms: 25,
            overall_ms: 300,
        }
    }

    #[tokio::test]
    async fn retries_and_persists_attempt_usage_across_restart() {
        let provider = ScriptedProvider::new(vec![
            Script {
                events: vec![
                    (0, ProviderEvent::ResponseStarted),
                    (
                        0,
                        ProviderEvent::Usage(ProviderUsage {
                            input_tokens: Some(3),
                            ..Default::default()
                        }),
                    ),
                ],
                error: Some(ProviderError::new(ProviderErrorKind::Server, "mock 503")),
                hang: false,
            },
            success(
                "完成",
                Some(ProviderUsage {
                    input_tokens: Some(7),
                    output_tokens: Some(2),
                    ..Default::default()
                }),
            ),
        ]);
        let mut retry = no_retry();
        retry.server_retries = 1;
        let engine = engine(vec![provider.clone()], retry, short_timeout());
        let path = std::env::temp_dir().join(format!(
            "p4-ledger-{}-{}.sqlite",
            std::process::id(),
            NEXT_ATTEMPT_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let store = Arc::new(RunStore::open(&path).unwrap());
        let Admission::New(run) = store
            .admit_with_route(
                SessionId("s".into()),
                RequestId::Number(1),
                "input",
                AdmissionMode::Queue,
                engine.route.as_ref().map(|route| &route.snapshot),
            )
            .unwrap()
        else {
            panic!("应该创建 run")
        };
        let output = with_tool_audit(
            store.clone(),
            run.run_id.clone(),
            request(&engine, None, &CancellationToken::new()),
        )
        .await
        .unwrap();
        assert_eq!(output.response, Response::Text("完成".into()));
        assert_eq!(provider.count(), 2);
        let attempts = store.provider_attempts(&run.run_id).unwrap();
        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[0].status, AttemptStatus::Failed);
        assert_eq!(attempts[1].status, AttemptStatus::Succeeded);
        store.finish_provider_attempt(&attempts[1]).unwrap();
        let mut conflict = attempts[1].clone();
        conflict.status = AttemptStatus::Failed;
        assert!(store.finish_provider_attempt(&conflict).is_err());
        assert_eq!(
            store.provider_usage(&run.run_id).unwrap().input_tokens,
            Some(10)
        );
        drop(store);
        let reopened = RunStore::open(&path).unwrap();
        assert_eq!(reopened.provider_attempts(&run.run_id).unwrap().len(), 2);
        assert_eq!(
            reopened.provider_usage(&run.run_id).unwrap().output_tokens,
            Some(2)
        );
        assert_eq!(
            reopened
                .route_snapshot(&run.run_id)
                .unwrap()
                .unwrap()
                .config_generation,
            1
        );
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn fallback_requires_no_visible_text_or_partial_tool_call() {
        let first = ScriptedProvider::new(vec![failure(ProviderErrorKind::Server)]);
        let second = ScriptedProvider::new(vec![success("备用", None)]);
        let retry = no_retry();
        let routed = engine(
            vec![first.clone(), second.clone()],
            retry.clone(),
            short_timeout(),
        );
        assert_eq!(
            request(&routed, None, &CancellationToken::new())
                .await
                .unwrap()
                .response,
            Response::Text("备用".into())
        );
        assert_eq!((first.count(), second.count()), (1, 1));

        for event in [
            ProviderEvent::TextDelta("部分".into()),
            ProviderEvent::ToolCallStarted {
                exec_id: crate::provider::ExecutionIdentity::position(ApiType::OpenaiChat, 0),
                name: "write".into(),
            },
        ] {
            let first = ScriptedProvider::new(vec![Script {
                events: vec![(0, ProviderEvent::ResponseStarted), (0, event)],
                error: Some(ProviderError::new(ProviderErrorKind::Server, "mock 503")),
                hang: false,
            }]);
            let second = ScriptedProvider::new(vec![success("不应执行", None)]);
            let engine = engine(vec![first, second.clone()], retry.clone(), short_timeout());
            let error = request(&engine, None, &CancellationToken::new())
                .await
                .unwrap_err();
            assert_eq!(
                error.downcast_ref::<ProviderError>().unwrap().kind,
                ProviderErrorKind::Server
            );
            assert_eq!(second.count(), 0);
        }
    }

    #[tokio::test]
    async fn timeout_phases_and_semantic_idle_are_distinct() {
        let cases = [
            (vec![], TimeoutPhase::Connect),
            (
                vec![
                    (0, ProviderEvent::ResponseStarted),
                    (10, ProviderEvent::Usage(ProviderUsage::default())),
                ],
                TimeoutPhase::FirstEvent,
            ),
            (
                vec![
                    (0, ProviderEvent::ResponseStarted),
                    (0, ProviderEvent::TextDelta("a".into())),
                ],
                TimeoutPhase::StreamIdle,
            ),
        ];
        for (events, phase) in cases {
            let provider = ScriptedProvider::new(vec![Script {
                events,
                error: None,
                hang: true,
            }]);
            let engine = engine(vec![provider], no_retry(), short_timeout());
            let error = request(&engine, None, &CancellationToken::new())
                .await
                .unwrap_err();
            assert_eq!(
                error.downcast_ref::<ProviderError>().unwrap().kind,
                ProviderErrorKind::Timeout(phase)
            );
        }
        let provider = ScriptedProvider::new(vec![Script {
            events: vec![],
            error: None,
            hang: true,
        }]);
        let overall_engine = engine(
            vec![provider],
            no_retry(),
            TimeoutPolicy {
                connect_ms: 100,
                first_event_ms: 100,
                stream_idle_ms: 100,
                overall_ms: 15,
            },
        );
        let error = request(&overall_engine, None, &CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(
            error.downcast_ref::<ProviderError>().unwrap().kind,
            ProviderErrorKind::Timeout(TimeoutPhase::Overall)
        );
        let provider = ScriptedProvider::new(vec![Script {
            events: vec![
                (0, ProviderEvent::ResponseStarted),
                (0, ProviderEvent::TextDelta("a".into())),
                (15, ProviderEvent::TextDelta("b".into())),
                (15, ProviderEvent::TextDelta("c".into())),
                (0, ProviderEvent::ProtocolDone),
            ],
            error: None,
            hang: false,
        }]);
        let engine = engine(vec![provider], no_retry(), short_timeout());
        assert_eq!(
            request(&engine, None, &CancellationToken::new())
                .await
                .unwrap()
                .response,
            Response::Text("abc".into())
        );
    }

    #[tokio::test]
    async fn cancellation_interrupts_connection_stream_and_backoff() {
        for events in [
            vec![],
            vec![(0, ProviderEvent::ResponseStarted)],
            vec![
                (0, ProviderEvent::ResponseStarted),
                (0, ProviderEvent::TextDelta("a".into())),
            ],
        ] {
            let provider = ScriptedProvider::new(vec![Script {
                events,
                error: None,
                hang: true,
            }]);
            let engine = engine(
                vec![provider],
                no_retry(),
                TimeoutPolicy {
                    connect_ms: 1000,
                    first_event_ms: 1000,
                    stream_idle_ms: 1000,
                    overall_ms: 2000,
                },
            );
            let token = CancellationToken::new();
            let cancel = token.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                cancel.cancel();
            });
            let error = request(&engine, None, &token).await.unwrap_err();
            assert_eq!(
                error.downcast_ref::<ProviderError>().unwrap().kind,
                ProviderErrorKind::Cancelled
            );
        }
        let provider = ScriptedProvider::new(vec![failure(ProviderErrorKind::Server)]);
        let mut retry = no_retry();
        retry.server_retries = 1;
        retry.max_backoff_ms = 1000;
        let engine = engine(vec![provider.clone()], retry, short_timeout());
        let token = CancellationToken::new();
        let cancel = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            cancel.cancel();
        });
        let _ = request(&engine, None, &token).await.unwrap_err();
        assert_eq!(provider.count(), 1);
    }

    #[tokio::test]
    async fn context_overflow_compacts_once_and_never_loops() {
        let provider = ScriptedProvider::new(vec![
            failure(ProviderErrorKind::ContextOverflow),
            success("摘要", None),
            failure(ProviderErrorKind::ContextOverflow),
        ]);
        let engine = engine(vec![provider.clone()], no_retry(), short_timeout());
        let messages = [
            Message::text(Role::System, "很长的旧系统说明".repeat(100)),
            Message::text(Role::User, "很长的旧对话".repeat(100)),
            Message::text(Role::User, "现在的请求"),
        ];
        let error = engine
            .request_model(&messages, &[], &None, &CancellationToken::new(), 1, None)
            .await
            .unwrap_err();
        assert_eq!(
            error.downcast_ref::<ProviderError>().unwrap().kind,
            ProviderErrorKind::ContextOverflow
        );
        assert_eq!(provider.count(), 3);
    }

    #[tokio::test]
    async fn empty_reasoning_truncated_and_protocol_eof_are_distinct() {
        let cases = [
            (
                vec![ProviderEvent::ResponseStarted, ProviderEvent::ProtocolDone],
                ProviderErrorKind::EmptyCompletion,
            ),
            (
                vec![
                    ProviderEvent::ResponseStarted,
                    ProviderEvent::ThinkingDelta("内部思考".into()),
                    ProviderEvent::ProtocolDone,
                ],
                ProviderErrorKind::ReasoningOnly,
            ),
            (
                vec![
                    ProviderEvent::ResponseStarted,
                    ProviderEvent::TextDelta("部分".into()),
                    ProviderEvent::OutputTruncated,
                    ProviderEvent::ProtocolDone,
                ],
                ProviderErrorKind::OutputTruncated,
            ),
            (
                vec![
                    ProviderEvent::ResponseStarted,
                    ProviderEvent::TextDelta("部分".into()),
                ],
                ProviderErrorKind::Protocol,
            ),
        ];
        for (events, kind) in cases {
            let provider = ScriptedProvider::new(vec![Script {
                events: events.into_iter().map(|event| (0, event)).collect(),
                error: None,
                hang: false,
            }]);
            let engine = engine(vec![provider], no_retry(), short_timeout());
            let error = request(&engine, None, &CancellationToken::new())
                .await
                .unwrap_err();
            assert_eq!(error.downcast_ref::<ProviderError>().unwrap().kind, kind);
        }
    }

    #[tokio::test]
    async fn real_http_5xx_and_rate_limit_follow_retry_budget() {
        let sse = "data: {\"choices\":[{\"delta\":{\"content\":\"成功\"}}]}\n\ndata: [DONE]\n\n";
        let (url, mut requests) = start_mock_http(vec![
            MockHttpResponse::error(500, "", r#"{"error":{"code":"server_error"}}"#),
            MockHttpResponse::error(503, "", r#"{"error":{"code":"server_error"}}"#),
            MockHttpResponse::sse(sse),
        ])
        .await;
        let provider: Arc<dyn Provider> = Arc::new(crate::provider::OpenAiProvider::new(
            "key".into(),
            url,
            "model".into(),
        ));
        let mut retry = no_retry();
        retry.server_retries = 2;
        retry.max_backoff_ms = 1;
        let first_engine = engine(
            vec![provider],
            retry,
            TimeoutPolicy {
                connect_ms: 1000,
                first_event_ms: 1000,
                stream_idle_ms: 1000,
                overall_ms: 2000,
            },
        );
        assert_eq!(
            request(&first_engine, None, &CancellationToken::new())
                .await
                .unwrap()
                .response,
            Response::Text("成功".into())
        );
        for _ in 0..3 {
            requests.recv().await.expect("应按预算请求三次");
        }

        let (url, mut requests) = start_mock_http(vec![
            MockHttpResponse::error(
                429,
                "Retry-After: 0\r\n",
                r#"{"error":{"code":"rate_limit"}}"#,
            ),
            MockHttpResponse::sse(sse),
        ])
        .await;
        let provider: Arc<dyn Provider> = Arc::new(crate::provider::OpenAiProvider::new(
            "key".into(),
            url,
            "model".into(),
        ));
        let mut retry = no_retry();
        retry.rate_limit_retries = 1;
        let engine = engine(
            vec![provider],
            retry,
            TimeoutPolicy {
                connect_ms: 1000,
                first_event_ms: 1000,
                stream_idle_ms: 1000,
                overall_ms: 2000,
            },
        );
        assert_eq!(
            request(&engine, None, &CancellationToken::new())
                .await
                .unwrap()
                .response,
            Response::Text("成功".into())
        );
        for _ in 0..2 {
            requests.recv().await.expect("应按 Retry-After 重试一次");
        }
    }

    async fn stalled_http_stream(first_delta: bool) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let mut buffer = [0_u8; 4096];
            while !bytes.windows(4).any(|part| part == b"\r\n\r\n") {
                let size = socket.read(&mut buffer).await.unwrap();
                if size == 0 {
                    return;
                }
                bytes.extend_from_slice(&buffer[..size]);
            }
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").await.unwrap();
            if first_delta {
                let payload = "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n";
                socket
                    .write_all(format!("{:X}\r\n{payload}\r\n", payload.len()).as_bytes())
                    .await
                    .unwrap();
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        });
        (url, task)
    }

    #[tokio::test]
    async fn real_http_headers_without_events_and_idle_after_text_have_different_timeouts() {
        for (first_delta, phase) in [
            (false, TimeoutPhase::FirstEvent),
            (true, TimeoutPhase::StreamIdle),
        ] {
            let (url, task) = stalled_http_stream(first_delta).await;
            let provider: Arc<dyn Provider> = Arc::new(crate::provider::OpenAiProvider::new(
                "key".into(),
                url,
                "model".into(),
            ));
            let engine = engine(
                vec![provider],
                no_retry(),
                TimeoutPolicy {
                    connect_ms: 100,
                    first_event_ms: 30,
                    stream_idle_ms: 30,
                    overall_ms: 500,
                },
            );
            let error = request(&engine, None, &CancellationToken::new())
                .await
                .unwrap_err();
            assert_eq!(
                error.downcast_ref::<ProviderError>().unwrap().kind,
                ProviderErrorKind::Timeout(phase)
            );
            task.abort();
        }
    }
}
