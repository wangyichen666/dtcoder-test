use std::collections::{HashMap, VecDeque};
use std::io::{self, Stdout};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use crossterm::event::{
    self, DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture,
    Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseEventKind,
};
use crossterm::execute;
use crossterm::terminal::{self, EnterAlternateScreen, LeaveAlternateScreen};
use futures_util::FutureExt;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::text::Line;
use serde_json::json;

use crate::client::{DaemonClient, RpcStream};
use crate::daemon::approval::PendingApprovalInfo;
use crate::daemon::protocol::{EventKind, RequestId, ServerFrame};
use crate::entry::recovery;
use crate::provider::{Message, Role};
use crate::session::SessionInfo;
use crate::slash::{SlashAction, SlashCommand, SlashParse, SlashRegistry, SlashResponse};

type TuiTerminal = Terminal<CrosstermBackend<Stdout>>;

mod input_editor;
mod keybindings;
mod view;
use input_editor::InputEditor;
use keybindings::TuiAction;
use view::draw_ui;

#[derive(Clone)]
struct UiMessage {
    id: u64,
    role: Role,
    kind: UiMessageKind,
    created_at: Instant,
    token_usage: Option<TokenUsage>,
    content_version: u64,
    turn_id: Option<RequestId>,
}

#[derive(Clone)]
enum UiMessageKind {
    Text(String),
    Tool(UiToolCall),
}

#[derive(Clone)]
struct TokenUsage {
    input: u32,
    output: u32,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ToolStatus {
    Running,
    Ok,
    Failed,
}

#[derive(Clone)]
struct UiToolCall {
    turn_id: RequestId,
    call_id: Option<String>,
    name: String,
    heading: String,
    round: usize,
    status: ToolStatus,
    output: Vec<String>,
    expanded: Option<bool>,
    started_at: Instant,
    finished_at: Option<Instant>,
    duration_ms: Option<u64>,
}

struct ActiveTurn {
    request_id: RequestId,
    stream: RpcStream,
    recovery_attempts: u8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ActivityPhase {
    Idle,
    Recovering,
    WaitingModel,
    Streaming,
    RunningTool,
    WaitingApproval,
}

impl ActivityPhase {
    fn is_animated(self) -> bool {
        matches!(
            self,
            Self::Recovering | Self::WaitingModel | Self::Streaming | Self::RunningTool
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum TuiThemeMode {
    Terminal,
    Dark,
    Light,
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
struct RenderCacheKey {
    message_id: u64,
    content_version: u64,
    width: u16,
    show_tools: bool,
    theme_mode: TuiThemeMode,
}

impl TuiThemeMode {
    fn from_env() -> Self {
        match std::env::var("MY_AGENT_TUI_THEME") {
            Ok(value)
                if matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "dark" | "truecolor"
                ) =>
            {
                Self::Dark
            }
            Ok(value) if value.trim().eq_ignore_ascii_case("light") => Self::Light,
            _ => Self::Terminal,
        }
    }
}

struct TuiState {
    session_id: String,
    messages: Vec<UiMessage>,
    next_message_id: u64,
    input: InputEditor,
    input_history: VecDeque<String>,
    history_cursor: Option<usize>,
    active_turns: Vec<ActiveTurn>,
    recovery_active_requests: Vec<RequestId>,
    queued_count: usize,
    pending_approvals: VecDeque<PendingApprovalInfo>,
    approval_scroll: usize,
    status: String,
    should_quit: bool,
    scroll: usize,
    follow_bottom: bool,
    unread_messages: usize,
    transcript_start: usize,
    preserve_transcript_start: Option<usize>,
    preserve_message_id: Option<u64>,
    show_tools: bool,
    show_help: bool,
    turn_phases: HashMap<RequestId, ActivityPhase>,
    animation_tick: u64,
    workspace: String,
    web_url: String,
    theme_mode: TuiThemeMode,
    resume_choices: Vec<SessionInfo>,
    slash_selection: usize,
    render_cache: HashMap<RenderCacheKey, Vec<Line<'static>>>,
}

impl TuiState {
    fn from_snapshot(snapshot: recovery::RecoverySnapshot) -> Self {
        let has_active = !snapshot.active_requests.is_empty();
        let has_pending = !snapshot.pending_approvals.is_empty();
        let next_message_id = snapshot.messages.len() as u64 + 1;
        let turn_phases = snapshot
            .active_requests
            .iter()
            .cloned()
            .map(|request_id| (request_id, ActivityPhase::Recovering))
            .collect();
        Self {
            session_id: snapshot.session_id,
            messages: snapshot
                .messages
                .into_iter()
                .enumerate()
                .filter_map(|(index, message)| message_to_ui(message, index as u64 + 1))
                .collect(),
            next_message_id,
            input: InputEditor::default(),
            input_history: VecDeque::new(),
            history_cursor: None,
            active_turns: Vec::new(),
            recovery_active_requests: snapshot.active_requests,
            queued_count: 0,
            pending_approvals: snapshot.pending_approvals.into(),
            approval_scroll: 0,
            status: if has_active {
                "正在恢复活动请求".to_owned()
            } else if has_pending {
                "等待审批".to_owned()
            } else {
                "就绪".to_owned()
            },
            should_quit: false,
            scroll: 0,
            follow_bottom: true,
            unread_messages: 0,
            transcript_start: 0,
            preserve_transcript_start: None,
            preserve_message_id: None,
            show_tools: false,
            show_help: false,
            turn_phases,
            animation_tick: 0,
            workspace: std::env::current_dir()
                .map(|path| path.display().to_string())
                .unwrap_or_default(),
            web_url: crate::entry::web::configured_url(),
            theme_mode: TuiThemeMode::from_env(),
            resume_choices: Vec::new(),
            slash_selection: 0,
            render_cache: HashMap::new(),
        }
    }

    fn replace_snapshot(&mut self, snapshot: recovery::RecoverySnapshot) {
        self.session_id = snapshot.session_id;
        self.messages = snapshot
            .messages
            .into_iter()
            .enumerate()
            .filter_map(|(index, message)| message_to_ui(message, index as u64 + 1))
            .collect();
        self.next_message_id = self.messages.len() as u64 + 1;
        self.active_turns.clear();
        self.recovery_active_requests = snapshot.active_requests;
        self.turn_phases = self
            .recovery_active_requests
            .iter()
            .cloned()
            .map(|request_id| (request_id, ActivityPhase::Recovering))
            .collect();
        self.pending_approvals = snapshot.pending_approvals.into();
        self.approval_scroll = 0;
        self.scroll = 0;
        self.follow_bottom = true;
        self.unread_messages = 0;
        self.transcript_start = 0;
        self.preserve_transcript_start = None;
        self.preserve_message_id = None;
        self.show_help = false;
        self.animation_tick = 0;
        self.resume_choices.clear();
        self.slash_selection = 0;
        self.render_cache.clear();
    }

    fn push_user(&mut self, content: String) {
        self.push_text(Role::User, content);
        self.scroll = 0;
        self.follow_bottom = true;
        self.unread_messages = 0;
    }

    fn record_history(&mut self, input: &str) {
        if input.trim().is_empty() || self.input_history.back().is_some_and(|last| last == input) {
            return;
        }
        self.input_history.push_back(input.to_owned());
        if self.input_history.len() > 100 {
            self.input_history.pop_front();
        }
        self.history_cursor = None;
    }

    fn history_previous(&mut self) {
        if self.input_history.is_empty() {
            return;
        }
        let index = self
            .history_cursor
            .unwrap_or(self.input_history.len())
            .saturating_sub(1);
        if let Some(item) = self.input_history.get(index) {
            self.input.replace(item);
            self.history_cursor = Some(index);
        }
    }

    fn history_next(&mut self) {
        let Some(index) = self.history_cursor else {
            return;
        };
        let next = index + 1;
        if let Some(item) = self.input_history.get(next) {
            self.input.replace(item);
            self.history_cursor = Some(next);
        } else {
            self.input.clear();
            self.history_cursor = None;
        }
    }

    fn append_assistant(&mut self, turn_id: &RequestId, delta: &str) {
        if let Some(UiMessage {
            role: Role::Assistant,
            kind: UiMessageKind::Text(content),
            content_version,
            ..
        }) = self.messages.iter_mut().rev().find(|message| {
            message.role == Role::Assistant && message.turn_id.as_ref() == Some(turn_id)
        }) {
            content.push_str(delta);
            *content_version = content_version.saturating_add(1);
        } else {
            self.push_text_for_turn(Role::Assistant, delta.to_owned(), Some(turn_id.clone()));
        }
    }

    fn push_text(&mut self, role: Role, content: String) {
        self.push_text_for_turn(role, content, None);
    }

    fn push_text_for_turn(&mut self, role: Role, content: String, turn_id: Option<RequestId>) {
        if !self.follow_bottom {
            self.unread_messages = self.unread_messages.saturating_add(1);
        }
        self.messages.push(UiMessage {
            id: self.next_message_id,
            role,
            kind: UiMessageKind::Text(content),
            created_at: Instant::now(),
            token_usage: None,
            content_version: 0,
            turn_id,
        });
        self.next_message_id = self.next_message_id.saturating_add(1);
    }

    fn start_tool(
        &mut self,
        turn_id: RequestId,
        call_id: Option<String>,
        name: String,
        round: usize,
    ) {
        if !self.follow_bottom {
            self.unread_messages = self.unread_messages.saturating_add(1);
        }
        self.messages.push(UiMessage {
            id: self.next_message_id,
            role: Role::Tool,
            kind: UiMessageKind::Tool(UiToolCall {
                turn_id,
                heading: tool_heading(&name),
                name,
                call_id,
                round,
                status: ToolStatus::Running,
                output: Vec::new(),
                expanded: None,
                started_at: Instant::now(),
                finished_at: None,
                duration_ms: None,
            }),
            created_at: Instant::now(),
            token_usage: None,
            content_version: 0,
            turn_id: None,
        });
        self.next_message_id = self.next_message_id.saturating_add(1);
    }

    fn finish_tool(
        &mut self,
        turn_id: &RequestId,
        call_id: Option<&str>,
        name: &str,
        output: &str,
        success: bool,
        duration_ms: u64,
    ) {
        let Some(message) = self.messages.iter_mut().rev().find(|message| {
            matches!(
                &message.kind,
                UiMessageKind::Tool(tool)
                    if &tool.turn_id == turn_id
                        && tool.status == ToolStatus::Running
                        && (call_id.is_some_and(|id| tool.call_id.as_deref() == Some(id))
                            || (call_id.is_none() && tool.name == name))
            )
        }) else {
            self.start_tool(
                turn_id.clone(),
                call_id.map(str::to_owned),
                name.to_owned(),
                0,
            );
            self.finish_tool(turn_id, call_id, name, output, success, duration_ms);
            return;
        };
        if let UiMessageKind::Tool(tool) = &mut message.kind {
            tool.output = output.lines().map(str::to_owned).collect();
            tool.status = if success {
                ToolStatus::Ok
            } else {
                ToolStatus::Failed
            };
            tool.finished_at = Some(Instant::now());
            tool.duration_ms = Some(duration_ms);
            message.content_version = message.content_version.saturating_add(1);
        }
    }

    fn request_quit(&mut self) {
        self.should_quit = true;
        self.status = "再见".to_owned();
    }

    fn toggle_tool_details(&mut self) {
        self.preserve_transcript_start = Some(self.transcript_start);
        self.preserve_message_id = self
            .messages
            .iter()
            .rev()
            .find(|message| message.role == Role::User)
            .map(|message| message.id);
        self.follow_bottom = false;
        self.show_tools = !self.show_tools;
        self.status = if self.show_tools {
            "工具详情已在原对话中展开".to_owned()
        } else {
            "工具调用已折叠".to_owned()
        };
    }

    fn advance_animation(&mut self) {
        self.animation_tick = self.animation_tick.wrapping_add(1);
    }

    fn activity_phase(&self) -> ActivityPhase {
        if !self.pending_approvals.is_empty() {
            return ActivityPhase::WaitingApproval;
        }
        for phase in [
            ActivityPhase::RunningTool,
            ActivityPhase::Streaming,
            ActivityPhase::WaitingModel,
            ActivityPhase::Recovering,
        ] {
            if self.turn_phases.values().any(|current| *current == phase) {
                return phase;
            }
        }
        ActivityPhase::Idle
    }

    fn set_turn_phase(&mut self, request_id: &RequestId, phase: ActivityPhase) {
        self.turn_phases.insert(request_id.clone(), phase);
    }

    fn scroll_by(&mut self, lines: isize) {
        self.preserve_transcript_start = None;
        self.preserve_message_id = None;
        if lines.is_positive() {
            self.scroll = self.scroll.saturating_add(lines.unsigned_abs());
            self.follow_bottom = false;
        } else {
            self.scroll = self.scroll.saturating_sub(lines.unsigned_abs());
            self.follow_bottom = self.scroll == 0;
            if self.follow_bottom {
                self.unread_messages = 0;
            }
        }
    }

    fn scroll_to_top(&mut self) {
        self.preserve_transcript_start = None;
        self.preserve_message_id = None;
        self.scroll = usize::MAX;
        self.follow_bottom = false;
    }

    fn scroll_to_bottom(&mut self) {
        self.preserve_transcript_start = None;
        self.preserve_message_id = None;
        self.scroll = 0;
        self.follow_bottom = true;
        self.unread_messages = 0;
    }

    fn slash_suggestions(&self) -> Vec<&'static SlashCommand> {
        SlashRegistry::builtin().suggestions(&self.input.text())
    }

    fn select_previous_slash(&mut self) {
        let count = self.slash_suggestions().len();
        if count > 0 {
            self.slash_selection = self.slash_selection.checked_sub(1).unwrap_or(count - 1);
        }
    }

    fn select_next_slash(&mut self) {
        let count = self.slash_suggestions().len();
        if count > 0 {
            self.slash_selection = (self.slash_selection + 1) % count;
        }
    }

    fn selected_slash(&self) -> Option<&'static SlashCommand> {
        let suggestions = self.slash_suggestions();
        suggestions
            .get(
                self.slash_selection
                    .min(suggestions.len().saturating_sub(1)),
            )
            .copied()
    }

    fn complete_selected_slash(&mut self) {
        if let Some(command) = self.selected_slash() {
            self.input.replace(command.completion());
            self.slash_selection = 0;
        }
    }

    fn reset_slash_selection(&mut self) {
        self.slash_selection = 0;
    }
}

pub async fn run_tui(client: DaemonClient, workspace: &std::path::Path) -> Result<()> {
    let snapshot = recovery::start_new_session(&client).await?;
    let session_id = snapshot.session_id.clone();
    let mut state = TuiState::from_snapshot(snapshot);
    if let Ok(queued) = crate::entry::cli::request_result(
        &client,
        "queue.list",
        json!({"session_id": state.session_id}),
    )
    .await
    {
        state.queued_count = queued["items"].as_array().map_or(0, Vec::len);
    }
    let mut recovery_failures = 0_usize;
    for request_id in std::mem::take(&mut state.recovery_active_requests) {
        match recovery::subscribe_for_session(&client, &request_id, &session_id).await {
            Ok(stream) => state.active_turns.push(ActiveTurn {
                request_id,
                stream,
                recovery_attempts: 0,
            }),
            Err(_) => {
                state.turn_phases.remove(&request_id);
                recovery_failures += 1;
            }
        }
    }
    state.workspace = workspace.display().to_string();
    state.web_url = crate::entry::web::configured_url();
    state.status = if recovery_failures == 0 {
        format!("新会话 {session_id} · /web 打开 {}", state.web_url)
    } else {
        format!(
            "新会话 {session_id} · {recovery_failures} 个活动请求恢复失败 · /web 打开 {}",
            state.web_url
        )
    };

    let _guard = TerminalGuard;
    let mut terminal = setup_terminal()?;
    let result = run_event_loop(&client, &mut terminal, &mut state).await;
    restore_terminal(&mut terminal)?;
    result
}

fn setup_terminal() -> Result<TuiTerminal> {
    terminal::enable_raw_mode().context("启用终端 raw mode 失败")?;
    let mut stdout = io::stdout();
    let mouse_enabled = std::env::var("MY_AGENT_TUI_MOUSE").is_ok_and(|value| value == "1");
    let terminal_result = if mouse_enabled {
        execute!(
            stdout,
            EnterAlternateScreen,
            EnableBracketedPaste,
            EnableMouseCapture
        )
    } else {
        execute!(stdout, EnterAlternateScreen, EnableBracketedPaste)
    };
    if let Err(error) = terminal_result {
        let _ = terminal::disable_raw_mode();
        return Err(error).context("进入终端 alternate screen 失败");
    }
    let mut terminal = Terminal::new(CrosstermBackend::new(stdout)).context("创建 TUI 终端失败")?;
    terminal.clear().context("清空 TUI 屏幕失败")?;
    Ok(terminal)
}

struct TerminalGuard;

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = terminal::disable_raw_mode();
        let _ = execute!(
            io::stdout(),
            DisableBracketedPaste,
            DisableMouseCapture,
            LeaveAlternateScreen,
            crossterm::cursor::Show
        );
    }
}

fn restore_terminal(terminal: &mut TuiTerminal) -> Result<()> {
    terminal::disable_raw_mode().context("恢复终端 raw mode 失败")?;
    execute!(
        terminal.backend_mut(),
        DisableBracketedPaste,
        DisableMouseCapture,
        LeaveAlternateScreen
    )
    .context("退出终端 alternate screen 失败")?;
    terminal.show_cursor().context("恢复终端光标失败")
}

async fn run_event_loop(
    client: &DaemonClient,
    terminal: &mut TuiTerminal,
    state: &mut TuiState,
) -> Result<()> {
    let mut dirty = true;
    let mut last_animation = Instant::now();
    while !state.should_quit {
        if dirty {
            terminal
                .draw(|frame| draw_ui(frame, state))
                .context("绘制 TUI 失败")?;
            dirty = false;
        }

        if event::poll(Duration::from_millis(16)).context("读取终端事件失败")? {
            dirty = true;
            match event::read().context("读取键盘事件失败")? {
                Event::Key(key) if key.kind != KeyEventKind::Release => {
                    if let Err(error) = handle_key(client, state, key).await {
                        state.status = format!("操作失败：{error:#}");
                    }
                }
                Event::Paste(text) if state.pending_approvals.is_empty() && !state.show_help => {
                    state.input.insert_text(&text);
                    state.reset_slash_selection();
                }
                Event::Mouse(_) if state.show_help => {}
                Event::Mouse(mouse) => match mouse.kind {
                    MouseEventKind::ScrollUp if !state.pending_approvals.is_empty() => {
                        state.approval_scroll = state.approval_scroll.saturating_sub(3);
                    }
                    MouseEventKind::ScrollDown if !state.pending_approvals.is_empty() => {
                        state.approval_scroll = state.approval_scroll.saturating_add(3);
                    }
                    MouseEventKind::ScrollUp => state.scroll_by(3),
                    MouseEventKind::ScrollDown => state.scroll_by(-3),
                    _ => {}
                },
                _ => {}
            }
        }
        let mut index = 0;
        let mut idle_checks = 0;
        for _ in 0..128 {
            if state.active_turns.is_empty() || idle_checks >= state.active_turns.len() {
                break;
            }
            index %= state.active_turns.len();
            let request_id = state.active_turns[index].request_id.clone();
            let next = {
                let active = &mut state.active_turns[index];
                active.stream.next().now_or_never()
            };
            match next {
                Some(Some(frame)) => {
                    let prior_active = state.active_turns.len();
                    handle_frame(state, &request_id, frame).await?;
                    if state.active_turns.len() < prior_active
                        && let Ok(queued) = crate::entry::cli::request_result(
                            client,
                            "queue.list",
                            json!({"session_id": state.session_id}),
                        )
                        .await
                    {
                        state.queued_count = queued["items"].as_array().map_or(0, Vec::len);
                    }
                    dirty = true;
                    idle_checks = 0;
                    if state
                        .active_turns
                        .get(index)
                        .is_some_and(|active| active.request_id == request_id)
                    {
                        index = (index + 1) % state.active_turns.len();
                    }
                }
                Some(None) => {
                    let interrupted = state.active_turns.remove(index);
                    idle_checks = 0;
                    let attempts = interrupted.recovery_attempts + 1;
                    if attempts <= 3 {
                        state.set_turn_phase(&request_id, ActivityPhase::Recovering);
                        match recovery::subscribe_for_session(
                            client,
                            &request_id,
                            &state.session_id,
                        )
                        .await
                        {
                            Ok(stream) => {
                                state.active_turns.push(ActiveTurn {
                                    request_id,
                                    stream,
                                    recovery_attempts: attempts,
                                });
                                state.status =
                                    format!("连接中断，已重新订阅任务（第 {attempts}/3 次）");
                            }
                            Err(error) => {
                                state.turn_phases.remove(&request_id);
                                state.status = format!(
                                    "恢复任务失败：{error:#} · 可通过 /resume 恢复 Session"
                                );
                            }
                        }
                    } else {
                        state.turn_phases.remove(&request_id);
                        state.status = "任务流多次中断 · 可通过 /resume 恢复 Session".to_owned();
                    }
                    dirty = true;
                }
                None => {
                    idle_checks += 1;
                    index = (index + 1) % state.active_turns.len();
                }
            }
        }
        if state.activity_phase().is_animated()
            && last_animation.elapsed() >= Duration::from_millis(90)
        {
            state.advance_animation();
            last_animation = Instant::now();
            dirty = true;
        } else if !state.activity_phase().is_animated() {
            last_animation = Instant::now();
        }
    }
    Ok(())
}

async fn handle_key(client: &DaemonClient, state: &mut TuiState, key: KeyEvent) -> Result<()> {
    let action = keybindings::resolve(key);
    if action == Some(TuiAction::Escape) {
        if state.show_help {
            state.show_help = false;
        } else if state.input.text().trim_start().starts_with('/') {
            state.input.clear();
            state.reset_slash_selection();
        } else {
            state.request_quit();
        }
        return Ok(());
    }
    if action == Some(TuiAction::ToggleHelp) {
        state.show_help = !state.show_help;
        return Ok(());
    }
    if state.show_help {
        return Ok(());
    }
    match action {
        Some(TuiAction::PageUp) if !state.pending_approvals.is_empty() => {
            state.approval_scroll = state.approval_scroll.saturating_sub(4);
            return Ok(());
        }
        Some(TuiAction::PageDown) if !state.pending_approvals.is_empty() => {
            state.approval_scroll = state.approval_scroll.saturating_add(4);
            return Ok(());
        }
        Some(TuiAction::PageUp) => {
            state.scroll_by(8);
            return Ok(());
        }
        Some(TuiAction::PageDown) => {
            state.scroll_by(-8);
            return Ok(());
        }
        _ => {}
    }
    if action == Some(TuiAction::ToggleToolDetails) {
        state.toggle_tool_details();
        return Ok(());
    }
    if action == Some(TuiAction::ClearInput) {
        state.input.clear();
        state.reset_slash_selection();
        state.history_cursor = None;
        return Ok(());
    }
    if action == Some(TuiAction::ClearQueue) {
        let listed = crate::entry::cli::request_result(
            client,
            "queue.list",
            json!({"session_id": state.session_id}),
        )
        .await?;
        let mut cleared = 0;
        if let Some(items) = listed["items"].as_array() {
            for item in items {
                if let Some(run_id) = item["run_id"].as_str() {
                    let result = crate::entry::cli::request_result(
                        client,
                        "queue.remove",
                        json!({"session_id": state.session_id, "run_id": run_id}),
                    )
                    .await?;
                    if result["cancelled"] == true {
                        cleared += 1;
                    }
                }
            }
        }
        state.queued_count = state.queued_count.saturating_sub(cleared);
        state.status = if cleared == 0 {
            "发送队列为空".to_owned()
        } else {
            format!("已清空 {cleared} 条排队消息")
        };
        return Ok(());
    }
    if action == Some(TuiAction::CancelTurn) {
        if let Some(request_id) = state
            .active_turns
            .last()
            .map(|turn| turn.request_id.clone())
        {
            let _ = crate::entry::cli::request_result(
                client,
                "agent.cancel",
                json!({"request_id": request_id, "session_id": state.session_id}),
            )
            .await?;
            state.status = "已发送取消请求".to_owned();
        }
        return Ok(());
    }

    if let Some(approval) = state.pending_approvals.front().cloned() {
        match keybindings::approval_decision(key) {
            Some(true) => {
                recovery::respond_to_approval(client, &approval.id, true).await?;
                state.pending_approvals.pop_front();
                state.status = "审批已允许，继续执行".to_owned();
            }
            Some(false) => {
                recovery::respond_to_approval(client, &approval.id, false).await?;
                state.pending_approvals.pop_front();
                state.status = "审批已拒绝，继续执行".to_owned();
            }
            _ => {}
        }
        return Ok(());
    }

    if let Some(selected) = state.selected_slash() {
        match action {
            Some(TuiAction::Previous) => {
                state.select_previous_slash();
                return Ok(());
            }
            Some(TuiAction::Next) => {
                state.select_next_slash();
                return Ok(());
            }
            Some(TuiAction::Complete) => {
                state.complete_selected_slash();
                return Ok(());
            }
            Some(TuiAction::Submit) if !selected.matches_exact(&state.input.text()) => {
                state.complete_selected_slash();
                return Ok(());
            }
            _ => {}
        }
    }

    match action {
        Some(TuiAction::CursorWordLeft) => state.input.move_word_left(),
        Some(TuiAction::CursorWordRight) => state.input.move_word_right(),
        Some(TuiAction::CursorLeft) => state.input.move_left(),
        Some(TuiAction::CursorRight) => state.input.move_right(),
        Some(TuiAction::ScrollTop) => state.scroll_to_top(),
        Some(TuiAction::ScrollBottom) => state.scroll_to_bottom(),
        Some(TuiAction::ScrollLineUp) => state.scroll_by(1),
        Some(TuiAction::ScrollLineDown) => state.scroll_by(-1),
        Some(TuiAction::CursorLineStart) => state.input.move_line_start(),
        Some(TuiAction::CursorLineEnd) => state.input.move_line_end(),
        Some(TuiAction::Previous) if state.input.is_single_line() => state.history_previous(),
        Some(TuiAction::Next) if state.input.is_single_line() => state.history_next(),
        Some(TuiAction::DeleteWordLeft) => {
            state.input.delete_word_left();
            state.reset_slash_selection();
        }
        Some(TuiAction::Backspace) => {
            state.input.backspace();
            state.reset_slash_selection();
        }
        Some(TuiAction::Delete) => {
            state.input.delete();
            state.reset_slash_selection();
        }
        Some(TuiAction::InsertNewline) => {
            state.input.insert('\n');
            state.reset_slash_selection();
        }
        Some(TuiAction::Submit) if !state.input.is_blank() => {
            let message = state.input.take();
            if let Err(error) = submit_input(client, state, message.clone()).await {
                state.input.replace(&message);
                return Err(error);
            }
            state.record_history(&message);
        }
        None => {
            if let KeyCode::Char(character) = key.code
                && !key.modifiers.contains(KeyModifiers::CONTROL)
            {
                state.input.insert(character);
                state.reset_slash_selection();
            }
        }
        _ => {}
    }
    Ok(())
}

async fn handle_frame(state: &mut TuiState, turn_id: &RequestId, frame: ServerFrame) -> Result<()> {
    match frame {
        ServerFrame::Event(event) => match event.event {
            EventKind::ThinkingDelta => {
                state.set_turn_phase(turn_id, ActivityPhase::Streaming);
                state.status = format!("request={turn_id:?} · 模型思考中");
            }
            EventKind::ThinkingFinished => {
                state.set_turn_phase(turn_id, ActivityPhase::Streaming);
                state.status = format!("request={turn_id:?} · 思考完成，开始输出回答");
            }
            EventKind::TextDelta => {
                if let Some(delta) = event.data["delta"].as_str() {
                    state.append_assistant(turn_id, delta);
                    state.set_turn_phase(turn_id, ActivityPhase::Streaming);
                    state.status = format!("模型流式输出 · request={turn_id:?}");
                }
            }
            EventKind::ToolStarted => {
                state.set_turn_phase(turn_id, ActivityPhase::RunningTool);
                state.start_tool(
                    turn_id.clone(),
                    event.data["tool_call_id"].as_str().map(str::to_owned),
                    event.data["name"].as_str().unwrap_or("unknown").to_owned(),
                    event.data["round"].as_u64().unwrap_or_default() as usize,
                );
                state.status = format!(
                    "request={turn_id:?} · 第 {} 轮 · 工具执行中：{}",
                    event.data["round"].as_u64().unwrap_or_default(),
                    event.data["name"].as_str().unwrap_or("unknown")
                );
            }
            EventKind::ToolFinished => {
                state.set_turn_phase(turn_id, ActivityPhase::WaitingModel);
                let success = event.data["success"].as_bool().unwrap_or_else(|| {
                    !event.data["output"]
                        .as_str()
                        .unwrap_or_default()
                        .starts_with("工具执行错误:")
                });
                let duration_ms = event.data["duration_ms"].as_u64().unwrap_or_default();
                state.status = format!(
                    "request={turn_id:?} · 第 {} 轮 · 工具{}：{} · {}ms",
                    event.data["round"].as_u64().unwrap_or_default(),
                    if success { "完成" } else { "失败" },
                    event.data["name"].as_str().unwrap_or("unknown"),
                    duration_ms
                );
                state.finish_tool(
                    turn_id,
                    event.data["tool_call_id"].as_str(),
                    event.data["name"].as_str().unwrap_or("工具"),
                    event.data["output"].as_str().unwrap_or_default(),
                    success,
                    duration_ms,
                );
            }
            EventKind::ApprovalRequired => {
                state.approval_scroll = 0;
                state.pending_approvals.push_back(
                    serde_json::from_value(event.data["approval"].clone())
                        .context("审批事件格式无效")?,
                );
                state.status = format!("等待审批：还有 {} 项", state.pending_approvals.len());
            }
            EventKind::TurnStarted => {
                state.set_turn_phase(turn_id, ActivityPhase::WaitingModel);
            }
            EventKind::TurnCompleted => {}
            EventKind::DelegationSpawned | EventKind::DelegationTerminal => {
                state.push_text(
                    Role::System,
                    format!(
                        "子 Agent {} · {}",
                        event.data["child_run_id"].as_str().unwrap_or("?"),
                        event.data["status"].as_str().unwrap_or("?")
                    ),
                );
            }
        },
        ServerFrame::Response(response) => {
            state
                .active_turns
                .retain(|turn| &turn.request_id != turn_id);
            state
                .pending_approvals
                .retain(|approval| &approval.request_id != turn_id);
            state.turn_phases.remove(turn_id);
            if let Some(error) = response.error {
                state.push_text_for_turn(
                    Role::System,
                    format!("✗ 任务未完成 · request={turn_id:?} · {}", error.message),
                    Some(turn_id.clone()),
                );
                state.status = format!("请求失败（{}）：{}", error.code, error.message);
            } else if response
                .result
                .as_ref()
                .and_then(|result| result["subscribed"].as_bool())
                == Some(false)
            {
                state.push_text_for_turn(
                    Role::System,
                    format!("✗ 执行体不可用 · request={turn_id:?} · 请查看 Session 记录"),
                    Some(turn_id.clone()),
                );
                state.status = format!("执行体不可用 · request={turn_id:?}");
            } else {
                state.push_text_for_turn(
                    Role::System,
                    format!("✓ 任务完成 · request={turn_id:?}"),
                    Some(turn_id.clone()),
                );
                state.status = format!("任务完成 · request={turn_id:?}");
            }
        }
    }
    Ok(())
}

async fn submit_input(client: &DaemonClient, state: &mut TuiState, message: String) -> Result<()> {
    let input = message.trim();
    if input.starts_with('/') {
        execute_slash(client, state, input).await?;
        return Ok(());
    }
    if !state.resume_choices.is_empty() && input.chars().all(|character| character.is_ascii_digit())
    {
        execute_slash(client, state, &format!("/resume {input}")).await?;
        return Ok(());
    }

    state.resume_choices.clear();
    begin_turn(client, state, message).await
}

async fn begin_turn(client: &DaemonClient, state: &mut TuiState, message: String) -> Result<()> {
    let stream = client
        .request(
            "chat.send",
            json!({"message": message, "session_id": state.session_id}),
        )
        .await?;
    let request_id = stream.request_id().clone();
    state.push_user(message);
    let request_label = format!("{request_id:?}");
    state.set_turn_phase(&request_id, ActivityPhase::WaitingModel);
    state.active_turns.push(ActiveTurn {
        request_id,
        stream,
        recovery_attempts: 0,
    });
    let queued = crate::entry::cli::request_result(
        client,
        "queue.list",
        json!({"session_id": state.session_id}),
    )
    .await;
    match queued {
        Ok(queued) => {
            let count = queued["items"].as_array().map_or(0, Vec::len);
            state.queued_count = count;
            state.status = format!("等待模型响应 · request={request_label} · 队列 {count}");
        }
        Err(error) => {
            state.status = format!("等待模型响应 · request={request_label} · 队列状态暂不可用");
            tracing::warn!(%error, "任务已提交，但查询队列失败");
        }
    }
    Ok(())
}

async fn execute_slash(client: &DaemonClient, state: &mut TuiState, line: &str) -> Result<()> {
    if matches!(
        SlashRegistry::builtin().parse(line),
        SlashParse::Command(crate::slash::SlashInvocation {
            action: SlashAction::Web,
            ..
        })
    ) {
        let launch =
            crate::entry::web::ensure_and_open(std::path::Path::new(&state.workspace)).await?;
        let action = if launch.started {
            "已启动并打开"
        } else {
            "已在运行，已打开"
        };
        state.web_url = launch.url.clone();
        state.push_text(Role::System, format!("{action} Web 控制台：{}", launch.url));
        state.status = format!("Web 控制台 · {}", launch.url);
        return Ok(());
    }
    let value = crate::entry::cli::request_result(
        client,
        "slash.execute",
        json!({"line": line, "session_id": state.session_id}),
    )
    .await?;
    let response: SlashResponse =
        serde_json::from_value(value).context("daemon slash.execute 格式无效")?;
    match response {
        SlashResponse::Text { content } => {
            state.push_text(Role::System, content);
            state.status = "命令已完成".to_owned();
        }
        SlashResponse::Exit => state.request_quit(),
        SlashResponse::Sessions { sessions, select } => {
            state.resume_choices = if select { sessions.clone() } else { Vec::new() };
            state.push_text(Role::System, render_session_choices(&sessions, select));
            state.scroll = 0;
            state.status = if sessions.is_empty() {
                "暂无会话记录".to_owned()
            } else if select {
                "请选择要恢复的 session".to_owned()
            } else {
                "已列出 session".to_owned()
            };
        }
        SlashResponse::SessionChanged { message, snapshot } => {
            let snapshot = recovery::parse_snapshot(snapshot, "slash.execute")?;
            state.replace_snapshot(snapshot);
            state.status = message;
        }
    }
    Ok(())
}

fn render_session_choices(sessions: &[SessionInfo], select: bool) -> String {
    if sessions.is_empty() {
        return if select {
            "暂无可恢复的历史会话。".to_owned()
        } else {
            "暂无会话记录。".to_owned()
        };
    }
    let mut lines = vec!["可恢复的历史会话：".to_owned()];
    for (index, session) in sessions.iter().enumerate() {
        let marker = if session.active { " · 当前" } else { "" };
        let preview = session.preview.as_deref().unwrap_or("无摘要");
        lines.push(format!(
            "{}. {} · {} · {} 个活动请求 · {} 条消息{marker}\n   {preview}",
            index + 1,
            session.id,
            session.status,
            session.active_requests,
            session.message_count
        ));
    }
    if select {
        lines.push("输入编号，或使用 /resume <编号或 session ID>。".to_owned());
    }
    lines.join("\n")
}

fn message_to_ui(message: Message, id: u64) -> Option<UiMessage> {
    let content = message.content?.to_owned();
    Some(UiMessage {
        id,
        role: message.role,
        kind: UiMessageKind::Text(content),
        created_at: Instant::now(),
        token_usage: None,
        content_version: 0,
        turn_id: None,
    })
}

fn tool_heading(name: &str) -> String {
    match name {
        "read_file" => "读取文件".to_owned(),
        "write_file" => "写入文件".to_owned(),
        "edit_file" => "编辑文件".to_owned(),
        "exec" => "运行命令".to_owned(),
        _ => format!("执行 {name}"),
    }
}

#[cfg(test)]
fn message_text(message: &UiMessage) -> &str {
    match &message.kind {
        UiMessageKind::Text(content) => content,
        UiMessageKind::Tool(_) => "",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use super::*;
    use crate::context::{ContextConfig, ContextManager};
    use crate::daemon::DaemonState;
    use crate::daemon::approval::ApprovalBroker;
    use crate::daemon::protocol::{EventFrame, JsonRpcResponse};
    use crate::daemon::server::InMemoryServer;
    use crate::loop_engine::LoopEngine;
    use crate::plan::PlanStore;
    use crate::provider::{Provider, Response, ToolSpec};
    use crate::session::SessionStore;
    use crate::tools::ToolRegistry;

    static NEXT_TEST: AtomicUsize = AtomicUsize::new(0);

    struct UnusedProvider;

    #[async_trait]
    impl Provider for UnusedProvider {
        async fn chat(&self, _messages: &[Message], _tools: &[ToolSpec]) -> Result<Response> {
            anyhow::bail!("恢复会话测试不应调用 Provider")
        }
    }

    #[test]
    fn converts_persisted_message_to_ui_message() {
        let message = Message::text(Role::User, "检查项目");
        let converted = message_to_ui(message, 7).expect("文本消息应可显示");
        assert_eq!(converted.role, Role::User);
        assert_eq!(converted.id, 7);
        assert!(matches!(converted.kind, UiMessageKind::Text(content) if content == "检查项目"));
    }

    #[test]
    fn quit_signal_is_independent_from_status_text() {
        let mut state = TuiState::from_snapshot(recovery::RecoverySnapshot {
            session_id: "test".to_owned(),
            messages: Vec::new(),
            pending_approvals: Vec::new(),
            active_requests: Vec::new(),
        });
        state.status = "任意状态文案".to_owned();
        assert!(!state.should_quit);
        state.request_quit();
        assert!(state.should_quit);
        assert_eq!(state.status, "再见");
    }

    #[test]
    fn slash_menu_filters_selects_and_completes_commands() {
        let mut state = TuiState::from_snapshot(recovery::RecoverySnapshot {
            session_id: "test".to_owned(),
            messages: Vec::new(),
            pending_approvals: Vec::new(),
            active_requests: Vec::new(),
        });
        state.input.replace("/");
        assert_eq!(
            state.slash_suggestions().len(),
            SlashRegistry::builtin().commands().len()
        );
        state.select_next_slash();
        state.complete_selected_slash();
        assert_eq!(state.input.text(), "/status");

        state.input.replace("/do");
        state.reset_slash_selection();
        assert_eq!(state.slash_suggestions().len(), 1);
        state.complete_selected_slash();
        assert_eq!(state.input.text(), "/dogfood");
    }

    #[test]
    fn tracks_unread_messages_when_scrolled_away_from_bottom() {
        let mut state = TuiState::from_snapshot(recovery::RecoverySnapshot {
            session_id: "test".to_owned(),
            messages: Vec::new(),
            pending_approvals: Vec::new(),
            active_requests: Vec::new(),
        });
        state.scroll_by(8);
        state.push_text(Role::Assistant, "新消息".to_owned());
        assert_eq!(state.unread_messages, 1);
        assert!(!state.follow_bottom);
        state.scroll_to_bottom();
        assert_eq!(state.unread_messages, 0);
        assert!(state.follow_bottom);
    }

    #[test]
    fn derives_activity_from_all_turns_and_pending_approvals() {
        let mut state = TuiState::from_snapshot(recovery::RecoverySnapshot {
            session_id: "test".to_owned(),
            messages: Vec::new(),
            pending_approvals: Vec::new(),
            active_requests: Vec::new(),
        });
        let waiting = RequestId::String("waiting".to_owned());
        let running = RequestId::String("running".to_owned());
        state.set_turn_phase(&waiting, ActivityPhase::WaitingModel);
        state.set_turn_phase(&running, ActivityPhase::RunningTool);
        assert_eq!(state.activity_phase(), ActivityPhase::RunningTool);

        state.pending_approvals.push_back(PendingApprovalInfo {
            id: "approval".to_owned(),
            request_id: waiting.clone(),
            prompt: "允许测试？".to_owned(),
        });
        assert_eq!(state.activity_phase(), ActivityPhase::WaitingApproval);

        state.pending_approvals.clear();
        state.turn_phases.remove(&running);
        assert_eq!(state.activity_phase(), ActivityPhase::WaitingModel);
    }

    #[tokio::test]
    async fn preserves_concurrent_turns_and_approval_queue() {
        let first = RequestId::String("turn-a".to_owned());
        let second = RequestId::String("turn-b".to_owned());
        let mut state = TuiState::from_snapshot(recovery::RecoverySnapshot {
            session_id: "test".to_owned(),
            messages: Vec::new(),
            pending_approvals: vec![
                PendingApprovalInfo {
                    id: "approval-a".to_owned(),
                    request_id: first.clone(),
                    prompt: "允许 A".to_owned(),
                },
                PendingApprovalInfo {
                    id: "approval-b".to_owned(),
                    request_id: second.clone(),
                    prompt: "允许 B".to_owned(),
                },
            ],
            active_requests: vec![first.clone(), second.clone()],
        });
        assert_eq!(state.pending_approvals.len(), 2);
        assert_eq!(
            state.recovery_active_requests,
            vec![first.clone(), second.clone()]
        );

        handle_frame(
            &mut state,
            &first,
            ServerFrame::Event(EventFrame::new(
                first.clone(),
                EventKind::TextDelta,
                json!({"delta": "来自 A"}),
            )),
        )
        .await
        .unwrap();
        handle_frame(
            &mut state,
            &second,
            ServerFrame::Event(EventFrame::new(
                second.clone(),
                EventKind::TextDelta,
                json!({"delta": "来自 B"}),
            )),
        )
        .await
        .unwrap();
        assert_eq!(state.messages.len(), 2);
        assert!(state.messages.iter().any(|message| {
            message.turn_id.as_ref() == Some(&first) && message_text(message) == "来自 A"
        }));
        assert!(state.messages.iter().any(|message| {
            message.turn_id.as_ref() == Some(&second) && message_text(message) == "来自 B"
        }));
    }

    #[tokio::test]
    async fn shows_explicit_completion_marker_after_response() {
        let request_id = RequestId::String("turn-complete".to_owned());
        let mut state = TuiState::from_snapshot(recovery::RecoverySnapshot {
            session_id: "test".to_owned(),
            messages: Vec::new(),
            pending_approvals: Vec::new(),
            active_requests: Vec::new(),
        });

        handle_frame(
            &mut state,
            &request_id,
            ServerFrame::Response(JsonRpcResponse::success(
                request_id.clone(),
                json!({"content": "完成"}),
            )),
        )
        .await
        .unwrap();

        assert!(state.status.contains("任务完成"));
        assert_eq!(state.activity_phase(), ActivityPhase::Idle);
        assert!(
            state
                .messages
                .iter()
                .any(|message| message_text(message).contains("任务完成"))
        );
    }

    #[tokio::test]
    async fn unavailable_subscription_does_not_claim_turn_completed() {
        let request_id = RequestId::String("turn-missing".to_owned());
        let mut state = TuiState::from_snapshot(recovery::RecoverySnapshot {
            session_id: "test".to_owned(),
            messages: Vec::new(),
            pending_approvals: Vec::new(),
            active_requests: Vec::new(),
        });
        handle_frame(
            &mut state,
            &request_id,
            ServerFrame::Response(JsonRpcResponse::success(
                request_id.clone(),
                json!({"subscribed": false}),
            )),
        )
        .await
        .unwrap();
        assert!(state.status.contains("执行体不可用"));
        assert!(
            state
                .messages
                .iter()
                .all(|message| !message_text(message).contains("任务完成"))
        );
    }

    #[tokio::test]
    async fn resume_command_lists_and_restores_selected_session() {
        let id = NEXT_TEST.fetch_add(1, Ordering::SeqCst);
        let session_path = std::env::temp_dir().join(format!(
            "my-agent-tui-resume-{}-{id}.jsonl",
            std::process::id()
        ));
        let session = Arc::new(SessionStore::new(&session_path));
        session
            .append(&Message::text(Role::User, "需要恢复的旧问题"))
            .await
            .unwrap();
        session
            .append(&Message::text(Role::Assistant, "旧回答"))
            .await
            .unwrap();
        let history = session.load().await.unwrap();
        let provider: Arc<dyn Provider> = Arc::new(UnusedProvider);
        let context = ContextManager::new(
            provider.clone(),
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
        .unwrap();
        let engine = Arc::new(LoopEngine::new(
            provider,
            ToolRegistry::new(),
            context,
            session.clone(),
        ));
        let client = InMemoryServer::start(Arc::new(DaemonState::new(
            engine,
            history,
            session,
            ApprovalBroker::new(),
        )));

        let fresh = recovery::start_new_session(&client).await.unwrap();
        let mut state = TuiState::from_snapshot(fresh);
        assert!(state.messages.is_empty());
        submit_input(&client, &mut state, "/resume".to_owned())
            .await
            .unwrap();
        assert_eq!(state.resume_choices.len(), 1);
        assert!(message_text(&state.messages[0]).contains("需要恢复的旧问题"));

        submit_input(&client, &mut state, "1".to_owned())
            .await
            .unwrap();
        assert_eq!(state.messages.len(), 2);
        assert_eq!(message_text(&state.messages[0]), "需要恢复的旧问题");
        assert_eq!(message_text(&state.messages[1]), "旧回答");

        submit_input(&client, &mut state, "/ping".to_owned())
            .await
            .unwrap();
        assert_eq!(state.messages.last().map(message_text), Some("pong"));

        let _ = std::fs::remove_file(SessionStore::pointer_path(&session_path));
        let _ = std::fs::remove_file(session_path);
    }
}
