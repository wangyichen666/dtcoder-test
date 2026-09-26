(() => {
  "use strict";

  const $ = (selector) => document.querySelector(selector);
  const $$ = (selector) => [...document.querySelectorAll(selector)];
  const graphemeSegmenter = typeof Intl.Segmenter === "function"
    ? new Intl.Segmenter("zh-CN", { granularity: "grapheme" })
    : null;
  const state = {
    rpc: null,
    connected: false,
    workspace: "",
    defaultWorkspace: "",
    browsePath: "",
    browseParent: null,
    sessions: [],
    agentSessionId: null,
    inspectedSessionId: null,
    agentSnapshot: null,
    inspectedSnapshot: null,
    inspectGeneration: 0,
    traces: [],
    openTraceKeys: new Set(),
    inspectedMessageOffset: 0,
    inspectedMessageTotal: 0,
    inspectedMessageHasMore: false,
    inspectedTraceOffset: 0,
    inspectedTraceTotal: 0,
    inspectedTraceHasMore: false,
    traceFilter: "all",
    activeRequest: null,
    activeRunId: null,
    activeRunSeq: 0,
    recoveryRpc: null,
    submitting: false,
    creatingSession: false,
    cancelling: false,
    reconnectTimer: null,
    reconnectAttempts: 0,
    connectionAttempt: 0,
    browseRequest: 0,
    draftAssistant: null,
    pendingApproval: null,
    pendingApprovals: [],
    agentFollowBottom: true,
    activities: [],
    activeView: "agent",
    collapsedDays: new Set(),
    permissionMode: null,
    modelProfiles: [],
    modelRequest: 0,
    modelActivationQueue: Promise.resolve(),
    ephemeralToken: null,
    activeModelId: null,
    modelConfigPath: "",
    transcriptRenderPending: false,
    thinkingFinished: false,
  };

  class RpcSocket {
    constructor(token, workspace) {
      this.token = token;
      this.workspace = workspace;
      this.nextId = 1;
      this.pending = new Map();
      this.socket = null;
    }

    connect() {
      return new Promise((resolve, reject) => {
        const protocol = location.protocol === "https:" ? "wss:" : "ws:";
        const socket = new WebSocket(`${protocol}//${location.host}/ws`);
        this.socket = socket;
        let settled = false;
        const settle = (error, workspace) => {
          if (settled) return;
          settled = true;
          clearTimeout(timeout);
          if (error) reject(error);
          else resolve(workspace);
        };
        const timeout = setTimeout(() => {
          settle(new Error("连接 daemon 超时"));
          socket.close();
        }, 8000);
        socket.addEventListener("open", () => {
          if (settled) return;
          socket.send(JSON.stringify({
            type: "connect",
            token: this.token || undefined,
            workspace: this.workspace || undefined,
          }));
        });
        socket.addEventListener("message", (event) => {
          let frame;
          try { frame = JSON.parse(event.data); } catch { return; }
          if (frame.type === "connected") {
            settle(null, frame.workspace || this.workspace);
            return;
          }
          if (frame.type === "error") {
            if (settled) return;
            settle(new Error(frame.error || "连接失败"));
            socket.close();
            return;
          }
          if (frame.type === "recovery" || frame.type === "recovery_error") return;
          const id = frame.frame === "event" ? frame.request_id : frame.id;
          const pending = this.pending.get(String(id));
          if (!pending) return;
          if (frame.frame === "event") {
            pending.onEvent?.(frame);
          } else if (frame.frame === "response") {
            this.pending.delete(String(id));
            if (frame.error) pending.reject(new Error(frame.error.message));
            else pending.resolve(frame.result);
          }
        });
        socket.addEventListener("close", () => {
          settle(new Error("WebSocket 已断开"));
          for (const pending of this.pending.values()) pending.reject(new Error("WebSocket 已断开"));
          this.pending.clear();
          if (state.rpc === this) {
            state.connected = false;
            setConnection("connecting", "连接已断开，正在重连");
            setAgentControls();
            scheduleReconnect();
          }
        });
        socket.addEventListener("error", () => settle(new Error("无法连接 Web 服务")));
      });
    }

    request(method, params = {}, onEvent) {
      if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
        return { id: null, promise: Promise.reject(new Error("尚未连接 daemon")) };
      }
      const id = this.nextId++;
      const promise = new Promise((resolve, reject) => {
        this.pending.set(String(id), { resolve, reject, onEvent });
      });
      try {
        this.socket.send(JSON.stringify({ jsonrpc: "2.0", id, method, params }));
      } catch (error) {
        this.pending.delete(String(id));
        return { id: null, promise: Promise.reject(error) };
      }
      return { id, promise };
    }

    close() { this.socket?.close(); }
  }

  async function bootstrap() {
    const requested = new URL(location.href).searchParams.get("workspace")
      || safeStorageGet("my-agent-workspace")
      || "";
    try {
      const health = await fetchJson("/health");
      state.defaultWorkspace = health.workspace || "";
    } catch (error) {
      setConnection("error", error.message);
    }
    await connect(requested || state.defaultWorkspace);
  }

  async function connect(workspace = state.workspace, automatic = false) {
    if (state.reconnectTimer) clearTimeout(state.reconnectTimer);
    state.reconnectTimer = null;
    const attempt = ++state.connectionAttempt;
    const previous = state.rpc;
    const previousConnected = previous?.socket?.readyState === WebSocket.OPEN;
    state.connected = false;
    setConnection("connecting", "正在连接工作区");
    setAgentControls();
    const rpc = new RpcSocket(apiToken(), workspace);
    try {
      const connectedWorkspace = await rpc.connect();
      if (attempt !== state.connectionAttempt) { rpc.close(); return false; }
      if (rpc.socket && rpc.socket.readyState !== WebSocket.OPEN) throw new Error("连接已断开");
      const nextWorkspace = connectedWorkspace || workspace;
      const changedWorkspace = Boolean(state.workspace && state.workspace !== nextWorkspace);
      state.rpc = rpc;
      previous?.close();
      if (changedWorkspace) resetWorkspaceState();
      state.workspace = nextWorkspace;
      state.connected = true;
      state.reconnectAttempts = 0;
      if (state.reconnectTimer) clearTimeout(state.reconnectTimer);
      state.reconnectTimer = null;
      rememberWorkspace(state.workspace);
      updateWorkspaceUrl(state.workspace);
      updateWorkspaceUi();
      updateAgentSessionUi();
      setConnection("online", "Agent 已连接");
      setAgentControls();
      if (state.activeRequest === "reconnecting") {
        recoverAgentTurn().catch((error) => toast(`恢复任务失败：${error.message}`));
      }
      const [, , sessions] = await Promise.allSettled([loadPermissionMode(), loadModels(), refreshSessions()]);
      if (sessions.status === "rejected" && attempt === state.connectionAttempt && state.rpc === rpc) {
        toast(`读取 Session 列表失败：${sessions.reason.message}`);
      }
      return attempt === state.connectionAttempt && state.rpc === rpc && state.connected;
    } catch (error) {
      rpc.close();
      if (attempt !== state.connectionAttempt) return false;
      state.rpc = previous;
      const restored = previousConnected && previous?.socket?.readyState === WebSocket.OPEN;
      state.connected = restored;
      setConnection(restored ? "online" : "error", restored ? "Agent 已连接" : "工作区连接失败");
      setAgentControls();
      if (!automatic) toast(`${error.message}。请检查目录或连接设置。`);
      if (automatic && !restored) scheduleReconnect();
      return false;
    }
  }

  function scheduleReconnect() {
    if (state.reconnectTimer || state.connected || state.reconnectAttempts >= 5) {
      if (state.reconnectAttempts >= 5) setConnection("error", "自动重连失败，请手动重试");
      return;
    }
    const delay = Math.min(1000 * 2 ** state.reconnectAttempts, 16000);
    state.reconnectAttempts += 1;
    setConnection("connecting", `正在重连（第 ${state.reconnectAttempts}/5 次）`);
    state.reconnectTimer = setTimeout(() => {
      state.reconnectTimer = null;
      connect(state.workspace, true);
    }, delay);
  }

  function setConnection(mode, label) {
    const node = $("#connection");
    node.className = `connection is-${mode}`;
    node.querySelector("span").textContent = label;
    $("#retry-connection").classList.toggle("hidden", mode !== "error" && state.reconnectAttempts === 0);
  }

  function setAgentControls() {
    const ready = state.connected && !state.activeRequest && !state.submitting;
    $("#new-agent-session").disabled = !state.connected || Boolean(state.activeRequest) || state.submitting || state.creatingSession;
    $("#prompt").disabled = !state.connected || (state.submitting && !state.activeRequest);
    $("#send").disabled = !ready;
    $("#inspect-current").disabled = !state.agentSessionId;
    $("#workspace-trigger").disabled = Boolean(state.activeRequest) || state.submitting;
    $("#change-workspace").disabled = Boolean(state.activeRequest) || state.submitting;
    $("#permission-trigger").disabled = !state.connected || Boolean(state.activeRequest) || state.submitting;
    $("#send").setAttribute("aria-busy", String(Boolean(state.activeRequest)));
    $("#agent-transcript").setAttribute("aria-busy", String(Boolean(state.activeRequest)));
    $("#cancel-turn").disabled = state.cancelling || !state.connected
      || (state.activeRequest === "reconnecting" && !state.activeRunId);
    $$('[data-approval-choice]').forEach((button) => { button.disabled = !state.connected; });
    updateLatestButton();
  }

  function updateWorkspaceUi() {
    const label = directoryName(state.workspace) || "选择目录";
    $("#workspace-label").textContent = label;
    $("#workspace-label").title = state.workspace;
    $("#workspace-path").textContent = state.workspace || "—";
    $("#session-workspace").textContent = state.workspace || "尚未连接工作目录";
  }

  async function loadPermissionMode() {
    if (!state.connected) return;
    const rpc = state.rpc;
    try {
      const result = await rpc.request("permissions.get").promise;
      if (rpc !== state.rpc) return;
      state.permissionMode = result.mode;
      updatePermissionUi(result);
    } catch (error) {
      if (rpc !== state.rpc) return;
      if (state.permissionMode == null) $("#permission-label").textContent = "不可用";
      toast(`读取权限模式失败：${error.message}`);
    }
  }

  async function loadModels() {
    const request = ++state.modelRequest;
    try {
      const result = await fetchJson("/api/models");
      if (request !== state.modelRequest) return;
      state.modelProfiles = result.profiles || [];
      state.activeModelId = result.active_id || null;
      state.modelConfigPath = result.config_path || "";
      renderModelProfiles();
      updateModelUi();
    } catch (error) {
      if (request !== state.modelRequest) return;
      toast(`读取模型配置失败：${error.message}`);
    }
  }

  function updateModelUi() {
    const active = state.modelProfiles.find((profile) => profile.id === state.activeModelId);
    const label = active ? `${active.name || active.id} · ${active.model}` : "尚未配置模型";
    $("#active-model").textContent = label;
    $("#active-model").title = active ? `${active.api_type} · ${active.base_url}` : "";
    $("#model-config-path").textContent = state.modelConfigPath ? `配置文件：${state.modelConfigPath}` : "";
  }

  function renderModelProfiles() {
    const node = $("#model-profile-list");
    if (!state.modelProfiles.length) {
      node.innerHTML = `<div class="directory-empty">暂无已保存模型，请在下方添加。</div>`;
      updateModelUi();
      return;
    }
    node.innerHTML = state.modelProfiles.map((profile) => {
      const active = profile.id === state.activeModelId ? " active" : "";
      const key = profile.has_api_key ? "已配置 key" : "未配置 key";
      return `<div class="model-profile${active}" data-model-profile="${escapeAttr(profile.id)}">
        <button type="button" class="model-profile-main"><span><strong>${escapeHtml(profile.name || profile.id)}</strong><small>${escapeHtml(profile.api_type)} · ${escapeHtml(profile.model)} · ${key}</small></span></button>
        <span class="model-profile-actions"><button type="button" data-model-edit="${escapeAttr(profile.id)}">编辑</button><button type="button" data-model-use="${escapeAttr(profile.id)}"${active ? " disabled" : ""}>使用</button></span>
      </div>`;
    }).join("");
    $$('[data-model-profile]').forEach((item) => item.querySelector(".model-profile-main")?.addEventListener("click", () => editModelProfile(item.dataset.modelProfile)));
    $$('[data-model-edit]').forEach((button) => button.addEventListener("click", () => editModelProfile(button.dataset.modelEdit)));
    $$('[data-model-use]').forEach((button) => button.addEventListener("click", () => useModelProfile(button.dataset.modelUse)));
    updateModelUi();
  }

  function useModelProfile(id) {
    if (!id) return;
    const operation = state.modelActivationQueue.then(() => activateModelProfile(id));
    state.modelActivationQueue = operation.catch(() => {});
    return operation;
  }

  async function activateModelProfile(id) {
    try {
      const result = await fetchJson("/api/models/activate", {
        method: "POST",
        headers: { ...authorizationHeaders(), "Content-Type": "application/json" },
        body: JSON.stringify({ profile_id: id, workspace: state.workspace || null }),
      });
      state.activeModelId = result.active_id || id;
      await loadModels();
      const timing = state.activeRequest ? "；当前任务沿用原模型，后续任务使用新模型" : "";
      toast(result.warning ? `已保存切换；${result.warning}${timing}` : `已切换模型：${result.profile?.name || id}${timing}`);
    } catch (error) {
      toast(`切换模型失败：${error.message}`);
    }
  }

  function editModelProfile(id) {
    const profile = state.modelProfiles.find((item) => item.id === id);
    if (!profile) return;
    $("#model-profile-id").value = profile.id || "";
    $("#model-profile-name").value = profile.name || "";
    $("#model-api-type").value = profile.api_type || "openai-chat";
    $("#model-name").value = profile.model || "";
    $("#model-base-url").value = profile.base_url || "";
    $("#model-api-key").value = "";
    $("#model-activate").checked = profile.id === state.activeModelId;
  }

  async function saveModelProfile() {
    const apiType = $("#model-api-type").value;
    const model = $("#model-name").value.trim();
    const baseUrl = $("#model-base-url").value.trim();
    const apiKey = $("#model-api-key").value.trim();
    if (!model || !baseUrl) {
      toast("请填写模型名称和服务地址。");
      return;
    }
    if (apiType !== "ollama" && !apiKey) {
      const id = $("#model-profile-id").value.trim();
      const existing = state.modelProfiles.find((profile) => profile.id === id);
      if (!existing?.has_api_key) {
        toast("此厂商需要 API key；已有配置可留空以保留原 key。");
        return;
      }
    }
    const button = $("#save-model");
    button.disabled = true;
    try {
      const result = await fetchJson("/api/models", {
        method: "POST",
        headers: { ...authorizationHeaders(), "Content-Type": "application/json" },
        body: JSON.stringify({
          activate: $("#model-activate").checked,
          workspace: state.workspace || null,
          profile: {
            id: $("#model-profile-id").value.trim(),
            name: $("#model-profile-name").value.trim(),
            api_type: apiType,
            api_key: apiKey || null,
            base_url: baseUrl,
            model,
          },
        }),
      });
      state.activeModelId = result.active_id || state.activeModelId;
      $("#model-api-key").value = "";
      await loadModels();
      const timing = state.activeRequest && $("#model-activate").checked ? "；当前任务沿用原模型，后续任务使用新模型" : "";
      toast(result.warning
        ? `模型配置已保存；${result.warning}${timing}`
        : `模型配置已保存：${result.profile?.name || model}${timing}`);
      if (!state.connected) await connect(state.workspace || state.defaultWorkspace);
    } catch (error) {
      toast(`保存模型配置失败：${error.message}`);
    } finally {
      button.disabled = false;
    }
  }

  function updatePermissionUi(result) {
    $("#permission-label").textContent = result.label || "未设置";
    $("#permission-label").title = result.description || "";
    $$("[data-permission-mode]").forEach((button) => {
      button.classList.toggle("active", button.dataset.permissionMode === result.mode);
    });
  }

  async function openPermissionsDialog() {
    if (!state.connected) {
      toast("连接工作区后才能切换权限模式。");
      return;
    }
    $("#permissions-dialog").showModal();
    await loadPermissionMode();
  }

  async function selectPermissionMode(mode) {
    if (!state.connected) { toast("连接工作区后才能切换权限模式。"); return; }
    if (state.activeRequest) { toast("当前任务结束后才能切换权限模式。"); return; }
    if (mode === state.permissionMode) { $("#permissions-dialog").close(); toast("当前已是该权限模式。"); return; }
    const rpc = state.rpc;
    $$("[data-permission-mode]").forEach((button) => { button.disabled = true; });
    try {
      const result = await rpc.request("permissions.set", { mode }).promise;
      if (rpc !== state.rpc) return;
      state.permissionMode = result.mode;
      updatePermissionUi(result);
      $("#permissions-dialog").close();
      toast(`已切换：${result.label}`);
    } catch (error) {
      if (rpc === state.rpc) toast(`切换权限模式失败：${error.message}`);
    } finally {
      $$("[data-permission-mode]").forEach((button) => { button.disabled = false; });
    }
  }

  async function refreshSessions() {
    if (!state.connected) return;
    const rpc = state.rpc;
    const result = await rpc.request("session.list").promise;
    if (rpc !== state.rpc) return;
    state.sessions = result.sessions || [];
    renderSessions();

    if (!state.activeRequest && !state.submitting && !state.sessions.some((item) => item.id === state.agentSessionId)) {
      state.agentSessionId = state.sessions[0]?.id || null;
    }
    if (state.agentSessionId) {
      if (!state.activeRequest && !state.submitting) await loadAgentSession(state.agentSessionId);
    } else {
      resetAgentSession();
    }
    if (rpc !== state.rpc) return;

    if (!state.sessions.some((item) => item.id === state.inspectedSessionId)) {
      state.inspectedSessionId = state.sessions[0]?.id || null;
    }
    if (state.activeView === "sessions" && state.inspectedSessionId) {
      await inspectSession(state.inspectedSessionId, false);
    }
  }

  function renderSessions() {
    const query = $("#session-search").value.trim().toLocaleLowerCase();
    const filtered = state.sessions.filter((session) =>
      `${session.id} ${session.preview || ""}`.toLocaleLowerCase().includes(query)
    );
    $("#session-count").textContent = query ? `${filtered.length}/${state.sessions.length}` : state.sessions.length;
    if (!filtered.length) {
      $("#session-list").innerHTML = `<div class="trace-empty">${state.connected ? "没有匹配的 Session。" : "连接工作区后查看 Session。"}</div>`;
      return;
    }
    const groups = new Map();
    filtered.forEach((session) => {
      const day = sessionDayKey(session);
      if (!groups.has(day)) groups.set(day, []);
      groups.get(day).push(session);
    });
    $("#session-list").innerHTML = [...groups.entries()].sort(([left], [right]) => right.localeCompare(left)).map(([day, sessions]) => `
      <section class="session-day ${state.collapsedDays.has(day) ? "is-collapsed" : ""}" data-day="${escapeAttr(day)}">
        <button class="session-day-toggle" data-day-toggle="${escapeAttr(day)}" type="button" aria-expanded="${!state.collapsedDays.has(day)}">
          <span class="day-title"><i>▾</i><strong>${escapeHtml(sessionDayLabel(day))}</strong><small>${sessions.length} 个 Session</small></span>
          <span class="day-chevron">⌄</span>
        </button>
        <div class="session-day-list">${sessions.map(renderSessionItem).join("")}</div>
      </section>`).join("");
    $$('[data-session]').forEach((button) => button.addEventListener("click", () => inspectSession(button.dataset.session)));
    $$('[data-day-toggle]').forEach((button) => button.addEventListener("click", () => toggleSessionDay(button.dataset.dayToggle)));
  }

  function renderSessionItem(session) {
    const selected = session.id === state.inspectedSessionId;
    const status = statusLabel(session.status || "idle");
    return `<button class="session-item ${selected ? "active" : ""}" data-session="${escapeAttr(session.id)}" aria-current="${selected ? "true" : "false"}">
      <span class="session-row">
        <i class="dot ${escapeAttr(session.status || "idle")}"></i>
        <strong>${escapeHtml(shortId(session.id))}</strong>
        <span class="session-status">${escapeHtml(status)}</span>
      </span>
      <p>${escapeHtml(session.preview || "空白 Session")}</p>
      <small>${session.message_count || 0} 条消息 · <time data-updated-at="${escapeAttr(session.updated_at || "")}">${formatRelative(session.updated_at)}</time></small>
    </button>`;
  }

  function updateRelativeTimes() {
    $$('[data-updated-at]').forEach((node) => { node.textContent = formatRelative(Number(node.dataset.updatedAt)); });
  }

  function toggleSessionDay(day) {
    if (state.collapsedDays.has(day)) state.collapsedDays.delete(day);
    else state.collapsedDays.add(day);
    renderSessions();
  }

  function sessionDayKey(session) {
    const timestamp = session.updated_at || session.modified_at;
    if (!timestamp) return "unknown";
    const date = new Date(timestamp * 1000);
    if (Number.isNaN(date.getTime())) return "unknown";
    return [date.getFullYear(), String(date.getMonth() + 1).padStart(2, "0"), String(date.getDate()).padStart(2, "0")].join("-");
  }

  function todaySessionDayKey() {
    const today = new Date();
    return [today.getFullYear(), String(today.getMonth() + 1).padStart(2, "0"), String(today.getDate()).padStart(2, "0")].join("-");
  }

  function sessionDayLabel(day) {
    if (day === "unknown") return "日期未知";
    const [year, month, date] = day.split("-").map(Number);
    const value = new Date(year, month - 1, date);
    const today = new Date();
    const todayKey = todaySessionDayKey();
    if (day === todayKey) return "今天";
    const yesterday = new Date(today.getFullYear(), today.getMonth(), today.getDate() - 1);
    const yesterdayKey = [yesterday.getFullYear(), String(yesterday.getMonth() + 1).padStart(2, "0"), String(yesterday.getDate()).padStart(2, "0")].join("-");
    if (day === yesterdayKey) return "昨天";
    return new Intl.DateTimeFormat("zh-CN", { year: "numeric", month: "long", day: "numeric", weekday: "short" }).format(value);
  }

  async function loadLatestAgentSnapshot(rpc, sessionId) {
    const first = await rpc.request("session.load_page", { session_id: sessionId, offset: 0, limit: 60 }).promise;
    const total = first.total_messages ?? first.messages?.length ?? 0;
    if (total <= 60) return first;
    return rpc.request("session.load_page", { session_id: sessionId, offset: Math.max(0, total - 60), limit: 60 }).promise;
  }

  async function loadAgentSession(sessionId) {
    const rpc = state.rpc;
    try {
      const snapshot = await loadLatestAgentSnapshot(rpc, sessionId);
      if (rpc !== state.rpc || state.agentSessionId !== sessionId) return;
      state.agentSnapshot = snapshot;
      renderAgentTranscript();
      updateAgentSessionUi();
    } catch (error) {
      if (rpc !== state.rpc || state.agentSessionId !== sessionId) return;
      toast(`读取 Agent Session 失败：${error.message}`);
    }
  }

  async function inspectSession(sessionId, rerender = true) {
    const generation = ++state.inspectGeneration;
    state.inspectedSessionId = sessionId;
    $("#load-more-messages").disabled = false;
    $("#load-more-traces").disabled = false;
    if (rerender) renderSessions();
    $("#session-title").textContent = shortId(sessionId, 34);
    $("#session-meta").textContent = "正在读取 Session 快照与执行链路…";
    $("#continue-session").disabled = true;
    state.inspectedMessageOffset = 0;
    state.inspectedMessageTotal = 0;
    state.inspectedMessageHasMore = false;
    state.inspectedTraceOffset = 0;
    state.inspectedTraceTotal = 0;
    state.inspectedTraceHasMore = false;
    state.inspectedSnapshot = null;
    state.traces = [];
    state.openTraceKeys.clear();
    $("#session-transcript").innerHTML = '<div class="trace-empty">正在读取会话消息…</div>';
    $("#session-transcript").setAttribute("aria-busy", "true");
    $("#trace-list").innerHTML = '<div class="trace-empty">正在读取链路记录…</div>';
    $("#trace-summary").innerHTML = '<div><strong>—</strong><span>模型调用</span></div><div><strong>—</strong><span>工具调用</span></div><div><strong>—</strong><span>总耗时</span></div>';
    $("#trace-count").textContent = "0";
    updateSessionPagers();
    try {
      const snapshotCall = state.rpc.request("session.load_page", { session_id: sessionId, offset: 0, limit: 60 }).promise;
      const traceCall = state.rpc.request("session.trace_page", { session_id: sessionId, offset: 0, limit: 60 }).promise;
      const [snapshotResult, traceResult] = await Promise.allSettled([snapshotCall, traceCall]);
      if (state.inspectedSessionId !== sessionId || generation !== state.inspectGeneration) return;
      if (snapshotResult.status === "rejected") throw snapshotResult.reason;
      const snapshot = snapshotResult.value;
      const trace = traceResult.status === "fulfilled" ? traceResult.value : { records: [], total_records: 0 };
      if (traceResult.status === "rejected") toast(`链路读取失败：${traceResult.reason?.message || "未知错误"}`);
      state.inspectedSnapshot = snapshot;
      state.traces = trace.records || [];
      state.inspectedMessageOffset = (snapshot.offset || 0) + state.inspectedSnapshot.messages.length;
      state.inspectedMessageTotal = snapshot.total_messages ?? state.inspectedSnapshot.messages.length;
      state.inspectedMessageHasMore = Boolean(snapshot.has_more);
      state.inspectedTraceOffset = (trace.offset || 0) + state.traces.length;
      state.inspectedTraceTotal = trace.total_records ?? state.traces.length;
      state.inspectedTraceHasMore = Boolean(trace.has_more);
      renderTranscript($("#session-transcript"), snapshot.messages || [], false);
      $("#session-transcript").setAttribute("aria-busy", "false");
      renderTrace();
      const status = snapshot.status || "idle";
      $("#session-meta").textContent = `${statusLabel(status)} · ${formatLoadedCount(state.inspectedMessageOffset, state.inspectedMessageTotal, "消息")} · ${formatLoadedCount(state.inspectedTraceOffset, state.inspectedTraceTotal, "链路记录")}`;
      $("#continue-session").disabled = false;
      updateSessionPagers();
    } catch (error) {
      if (state.inspectedSessionId !== sessionId || generation !== state.inspectGeneration) return;
      $("#message-pager").classList.add("hidden");
      $("#trace-pager").classList.add("hidden");
      $("#session-transcript").innerHTML = `<div class="trace-empty">读取失败：${escapeHtml(error.message)}</div>`;
      $("#session-transcript").setAttribute("aria-busy", "false");
      toast(`读取 Session 失败：${error.message}`);
    }
  }

  async function loadMoreMessages() {
    const sessionId = state.inspectedSessionId;
    if (!sessionId || !state.inspectedMessageHasMore) return;
    const generation = state.inspectGeneration;
    const rpc = state.rpc;
    const button = $("#load-more-messages");
    button.disabled = true;
    try {
      const page = await rpc.request("session.load_page", {
        session_id: sessionId,
        offset: state.inspectedMessageOffset,
        limit: 60,
      }).promise;
      if (rpc !== state.rpc || state.inspectedSessionId !== sessionId || generation !== state.inspectGeneration) return;
      state.inspectedSnapshot ||= { messages: [] };
      state.inspectedSnapshot.messages ||= [];
      state.inspectedSnapshot.messages.push(...(page.messages || []));
      state.inspectedMessageOffset = (page.offset || state.inspectedMessageOffset) + (page.messages || []).length;
      state.inspectedMessageTotal = page.total_messages ?? state.inspectedMessageTotal;
      state.inspectedMessageHasMore = Boolean(page.has_more);
      renderTranscript($("#session-transcript"), state.inspectedSnapshot.messages, false);
      updateSessionPagers();
      updateSessionMeta();
    } catch (error) {
      if (rpc === state.rpc && state.inspectedSessionId === sessionId && generation === state.inspectGeneration) toast(`加载更多消息失败：${error.message}`);
    } finally {
      if (rpc === state.rpc && state.inspectedSessionId === sessionId && generation === state.inspectGeneration) button.disabled = false;
    }
  }

  async function loadMoreTraces() {
    const sessionId = state.inspectedSessionId;
    if (!sessionId || !state.inspectedTraceHasMore) return;
    const generation = state.inspectGeneration;
    const rpc = state.rpc;
    const button = $("#load-more-traces");
    button.disabled = true;
    try {
      const page = await rpc.request("session.trace_page", {
        session_id: sessionId,
        offset: state.inspectedTraceOffset,
        limit: 60,
      }).promise;
      if (rpc !== state.rpc || state.inspectedSessionId !== sessionId || generation !== state.inspectGeneration) return;
      state.traces.push(...(page.records || []));
      state.inspectedTraceOffset = (page.offset || state.inspectedTraceOffset) + (page.records || []).length;
      state.inspectedTraceTotal = page.total_records ?? state.inspectedTraceTotal;
      state.inspectedTraceHasMore = Boolean(page.has_more);
      renderTrace();
      updateSessionPagers();
      updateSessionMeta();
    } catch (error) {
      if (rpc === state.rpc && state.inspectedSessionId === sessionId && generation === state.inspectGeneration) toast(`加载更多链路失败：${error.message}`);
    } finally {
      if (rpc === state.rpc && state.inspectedSessionId === sessionId && generation === state.inspectGeneration) button.disabled = false;
    }
  }

  function formatLoadedCount(loaded, total, label) {
    return total > loaded ? `已加载 ${loaded}/${total} 条${label}` : `${total} 条${label}`;
  }

  function updateSessionMeta() {
    if (!state.inspectedSnapshot) return;
    const status = state.inspectedSnapshot.status || "idle";
    $("#session-meta").textContent = `${statusLabel(status)} · ${formatLoadedCount(state.inspectedMessageOffset, state.inspectedMessageTotal, "消息")} · ${formatLoadedCount(state.inspectedTraceOffset, state.inspectedTraceTotal, "链路记录")}`;
  }

  function updateSessionPagers() {
    const messagePager = $("#message-pager");
    const tracePager = $("#trace-pager");
    messagePager.classList.toggle("hidden", !state.inspectedMessageHasMore && !state.inspectedMessageTotal);
    tracePager.classList.toggle("hidden", !state.inspectedTraceHasMore && !state.inspectedTraceTotal);
    $("#message-progress").textContent = state.inspectedMessageTotal
      ? formatLoadedCount(state.inspectedMessageOffset, state.inspectedMessageTotal, "消息")
      : "暂无消息";
    $("#trace-progress").textContent = state.inspectedTraceTotal
      ? formatLoadedCount(state.inspectedTraceOffset, state.inspectedTraceTotal, "链路")
      : "暂无链路";
    $("#load-more-messages").classList.toggle("hidden", !state.inspectedMessageHasMore);
    $("#load-more-traces").classList.toggle("hidden", !state.inspectedTraceHasMore);
  }

  function renderAgentTranscript() {
    renderTranscript($("#agent-transcript"), state.agentSnapshot?.messages || [], true);
  }

  function scheduleAgentTranscriptRender() {
    if (state.transcriptRenderPending) return;
    state.transcriptRenderPending = true;
    requestAnimationFrame(() => {
      state.transcriptRenderPending = false;
      renderAgentTranscript();
    });
  }

  function renderTranscript(node, messages, agentMode) {
    const followLatest = agentMode && (state.agentFollowBottom || isNearBottom(node));
    const previousTop = node.scrollTop;
    const openDetails = [...node.querySelectorAll("details")].map((item, index) => item.open ? index : -1).filter((index) => index >= 0);
    const html = messages.map((message) => renderMessage(message, agentMode)).join("");
    const approval = agentMode ? state.pendingApprovals.map(renderApprovalCard).join("") : "";
    const empty = agentMode ? agentEmptyTemplate() : `
      <div class="empty-state">
        <span class="empty-orbit">⌁</span>
        <h2>空白 Session</h2>
        <p>这个 Session 暂时没有消息。</p>
      </div>`;
    node.innerHTML = `${html || empty}${approval}`;
    const nextDetails = node.querySelectorAll("details");
    openDetails.forEach((index) => { if (nextDetails[index]) nextDetails[index].open = true; });
    if (agentMode) bindStarterButtons();
    if (agentMode) bindApprovalCard();
    requestAnimationFrame(() => {
      if (followLatest) node.scrollTop = node.scrollHeight;
      else node.scrollTop = previousTop;
      if (agentMode) updateLatestButton(node);
    });
  }

  function isNearBottom(node) {
    return node.scrollHeight - node.scrollTop - node.clientHeight < 72;
  }

  function updateLatestButton(node = $("#agent-transcript")) {
    if (!node) return;
    state.agentFollowBottom = isNearBottom(node);
    $("#jump-latest").classList.toggle("hidden", state.agentFollowBottom);
  }

  function scrollAgentToLatest() {
    const node = $("#agent-transcript");
    node.scrollTop = node.scrollHeight;
    state.agentFollowBottom = true;
    updateLatestButton(node);
  }

  function agentEmptyTemplate() {
    return `<div class="empty-state agent-empty">
      <span class="empty-orbit">✦</span>
      <h2>让 Agent 真正在项目里工作</h2>
      <p>模型响应会在这里实时展示。当前任务将绑定到 <strong>${escapeHtml(state.workspace || "所选工作目录")}</strong>，你可以直接描述目标，Agent 会读取代码、执行工具并完成验证。</p>
      <div class="starter-grid">
        <button type="button" data-starter="分析这个项目的结构，并告诉我最值得优先改进的三个地方。">分析项目结构</button>
        <button type="button" data-starter="运行项目测试，定位失败原因并修复。">运行并修复测试</button>
        <button type="button" data-starter="阅读当前项目，帮我实现一个合理的小功能并完成验证。">开始开发功能</button>
      </div>
    </div>`;
  }

  function renderApprovalCard(approval) {
    return `<section class="approval-card" data-approval-card="${escapeAttr(approval.id)}">
      <strong>需要审批</strong>
      <p>${escapeHtml(approval.prompt || "Agent 请求执行受保护操作")}</p>
      <div class="approval-actions">
        <button class="approve" type="button" data-approval-choice="approve">允许</button>
        <button class="deny" type="button" data-approval-choice="deny">拒绝</button>
      </div>
    </section>`;
  }

  function bindApprovalCard() {
    $$('[data-approval-card]').forEach((card) => {
      const id = card.dataset.approvalCard;
      card.querySelector('[data-approval-choice="approve"]')?.addEventListener("click", () => respondApproval(id, true, card));
      card.querySelector('[data-approval-choice="deny"]')?.addEventListener("click", () => respondApproval(id, false, card));
    });
    $$('[data-approval-choice]').forEach((button) => { button.disabled = !state.connected; });
  }

  function renderMessage(message) {
    const role = message.role || "system";
    const label = { user: "You", assistant: "Agent", tool: `Tool · ${message.name || "result"}`, system: "System" }[role] || role;
    const toolCalls = (message.tool_calls || []).map((call) => `
      <div class="tool-call-pill"><strong>${escapeHtml(call.name)}</strong><code>${escapeHtml(JSON.stringify(call.arguments))}</code></div>`).join("");
    const toolDetails = toolCalls ? `<details class="tool-details">
      <summary>工具调用 · ${message.tool_calls.length} 项（默认折叠）</summary>
      <div class="tool-call-list">${toolCalls}</div>
    </details>` : "";
    if (role === "tool") {
      return `<details class="message tool tool-details">
        <summary><span>${escapeHtml(label)}</span><small>输出已折叠，点击查看</small></summary>
        <div class="message-content tool-output">${escapeHtml(message.content || "无输出")}</div>
      </details>`;
    }
    const content = message.content == null ? "" : String(message.content);
    const thinking = message.thinking == null ? "" : String(message.thinking);
    const thinkingOpen = message === state.draftAssistant && !state.thinkingFinished && state.activeRequest;
    const thinkingDetails = thinking ? `<details class="thinking-details"${thinkingOpen ? " open" : ""}>
      <summary><span>思考过程</span><small>${thinkingOpen ? "生成中" : "已自动折叠"}</small></summary>
      <div class="thinking-content">${renderMarkdown(thinking)}${thinkingOpen ? `<span class="streaming-cursor" aria-label="正在生成思考"></span>` : ""}</div>
    </details>` : "";
    const renderedContent = content
      ? role === "assistant" ? renderMarkdown(content) : escapeHtml(content)
      : "";
    const streamingCursor = role === "assistant" && message === state.draftAssistant && state.activeRequest
      ? `<span class="streaming-cursor" aria-label="正在生成"></span>`
      : "";
    const contentHtml = renderedContent || streamingCursor
      ? `<div class="message-content">${renderedContent}${streamingCursor}</div>`
      : "";
    return `<article class="message ${escapeAttr(role)}">
      <div class="message-label"><span>${escapeHtml(label)}</span></div>
      ${thinkingDetails}
      ${contentHtml}
      ${toolDetails}
    </article>`;
  }

  function renderMarkdown(value) {
    const lines = String(value ?? "").replace(/\r\n?/g, "\n").split("\n");
    const blocks = [];
    let paragraph = [];
    let listType = null;
    let quoteLines = [];
    const flushParagraph = () => {
      if (!paragraph.length) return;
      blocks.push(`<p>${renderInlineMarkdown(paragraph.join("\n")).replace(/\n/g, "<br />")}</p>`);
      paragraph = [];
    };
    const flushList = () => {
      if (!listType) return;
      blocks.push(`</${listType}>`);
      listType = null;
    };
    const flushQuote = () => {
      if (!quoteLines.length) return;
      blocks.push(`<blockquote>${quoteLines.map(renderInlineMarkdown).join("<br />")}</blockquote>`);
      quoteLines = [];
    };
    for (let index = 0; index < lines.length; index += 1) {
      const line = lines[index];
      if (line.includes("|") && index + 1 < lines.length && isTableSeparator(lines[index + 1])) {
        flushParagraph(); flushList(); flushQuote();
        const headers = parseTableCells(line);
        const alignments = parseTableCells(lines[index + 1]).map(tableAlignment);
        const rows = [];
        index += 2;
        while (index < lines.length && lines[index].includes("|") && lines[index].trim()) {
          rows.push(parseTableCells(lines[index]));
          index += 1;
        }
        index -= 1;
        const headerHtml = headers.map((cell, cellIndex) => `<th class="${alignments[cellIndex] || ""}">${renderInlineMarkdown(cell)}</th>`).join("");
        const rowHtml = rows.map((row) => `<tr>${headers.map((_, cellIndex) => `<td class="${alignments[cellIndex] || ""}">${renderInlineMarkdown(row[cellIndex] || "")}</td>`).join("")}</tr>`).join("");
        blocks.push(`<table><thead><tr>${headerHtml}</tr></thead>${rowHtml ? `<tbody>${rowHtml}</tbody>` : ""}</table>`);
        continue;
      }
      const fence = line.match(/^ {0,3}(```+|~~~+)\s*([^ ]*)\s*$/);
      if (fence) {
        flushParagraph(); flushList(); flushQuote();
        const marker = fence[1][0];
        const code = [];
        index += 1;
        while (index < lines.length && !new RegExp(`^ {0,3}${marker}{3,}\\s*$`).test(lines[index])) {
          code.push(lines[index]);
          index += 1;
        }
        const language = fence[2] ? ` class="language-${escapeAttr(fence[2])}"` : "";
        blocks.push(`<pre><code${language}>${escapeHtml(code.join("\n"))}</code></pre>`);
        continue;
      }
      const heading = line.match(/^ {0,3}(#{1,6})\s+(.+?)\s*#*\s*$/);
      if (heading) {
        flushParagraph(); flushList(); flushQuote();
        const level = heading[1].length;
        blocks.push(`<h${level}>${renderInlineMarkdown(heading[2])}</h${level}>`);
        continue;
      }
      if (/^\s{0,3}((\*\s*){3,}|(-\s*){3,}|(_\s*){3,})$/.test(line)) {
        flushParagraph(); flushList(); flushQuote();
        blocks.push("<hr />");
        continue;
      }
      const unordered = line.match(/^ {0,3}[-*+]\s+(.+)$/);
      const ordered = line.match(/^ {0,3}\d+[.)]\s+(.+)$/);
      if (unordered || ordered) {
        flushParagraph(); flushQuote();
        const nextType = unordered ? "ul" : "ol";
        if (listType && listType !== nextType) flushList();
        if (!listType) {
          listType = nextType;
          blocks.push(`<${listType}>`);
        }
        blocks.push(`<li>${renderInlineMarkdown((unordered || ordered)[1])}</li>`);
        continue;
      }
      if (/^\s*$/.test(line)) {
        flushParagraph();
        flushList();
        flushQuote();
        continue;
      }
      const quote = line.match(/^ {0,3}>\s?(.*)$/);
      if (quote) {
        flushParagraph(); flushList();
        quoteLines.push(quote[1]);
        continue;
      }
      flushQuote();
      flushList();
      paragraph.push(line);
    }
    flushParagraph();
    flushList();
    flushQuote();
    return blocks.join("");
  }

  function parseTableCells(line) {
    const value = String(line ?? "").trim().replace(/^\|/, "").replace(/\|$/, "");
    return value.split("|").map((cell) => cell.trim());
  }

  function isTableSeparator(line) {
    const cells = parseTableCells(line);
    return cells.length > 0 && cells.every((cell) => /^:?-{3,}:?$/.test(cell));
  }

  function tableAlignment(cell) {
    const value = String(cell ?? "");
    if (value.startsWith(":") && value.endsWith(":")) return "align-center";
    if (value.endsWith(":")) return "align-right";
    if (value.startsWith(":")) return "align-left";
    return "";
  }

  function renderInlineMarkdown(value) {
    const source = String(value ?? "");
    const tokenPattern = /(`[^`]+`|\[[^\]]+\]\([^\s)]+(?:\s+["'][^"']*["'])?\)|\*\*[^*]+\*\*|__[^_]+__|~~[^~]+~~|\*[^*]+\*|_[^_]+_)/g;
    let output = "";
    let cursor = 0;
    let match;
    while ((match = tokenPattern.exec(source))) {
      output += escapeHtml(source.slice(cursor, match.index));
      const token = match[0];
      if (token.startsWith("`") && token.endsWith("`")) {
        output += `<code>${escapeHtml(token.slice(1, -1))}</code>`;
      } else if (token.startsWith("[") && token.includes("](")) {
        const link = token.match(/^\[([^\]]+)\]\(([^\s)]+)(?:\s+["']([^"']*)["'])?\)$/);
        const href = link && safeMarkdownUrl(link[2]);
        if (!link || !href) output += escapeHtml(token);
        else {
          const title = link[3] ? ` title="${escapeAttr(link[3])}"` : "";
          const external = /^(?:https?:|mailto:)/i.test(href) ? ` target="_blank" rel="noreferrer"` : "";
          output += `<a href="${escapeAttr(href)}"${title}${external}>${renderInlineMarkdown(link[1])}</a>`;
        }
      } else if (token.startsWith("**") || token.startsWith("__")) {
        output += `<strong>${renderInlineMarkdown(token.slice(2, -2))}</strong>`;
      } else if (token.startsWith("~~")) {
        output += `<del>${renderInlineMarkdown(token.slice(2, -2))}</del>`;
      } else {
        output += `<em>${renderInlineMarkdown(token.slice(1, -1))}</em>`;
      }
      cursor = tokenPattern.lastIndex;
    }
    return output + escapeHtml(source.slice(cursor));
  }

  function safeMarkdownUrl(value) {
    const href = String(value ?? "").trim();
    if (!href || /^(?:javascript|data|vbscript):/i.test(href)) return null;
    if (/^(?:#|\/|\.\.?\/)/.test(href)) return href;
    try {
      const protocol = new URL(href, window.location.href).protocol;
      return ["http:", "https:", "mailto:"].includes(protocol) ? href : null;
    } catch {
      return null;
    }
  }

  async function newAgentSession() {
    if (!state.connected || state.activeRequest || state.submitting || state.creatingSession) return;
    state.creatingSession = true;
    setAgentControls();
    const rpc = state.rpc;
    try {
      const snapshot = await rpc.request("session.new").promise;
      if (rpc !== state.rpc) return;
      state.agentSessionId = snapshot.session_id;
      state.agentSnapshot = snapshot;
      state.activities = [];
      state.pendingApproval = null;
      state.pendingApprovals = [];
      state.agentFollowBottom = true;
      state.thinkingFinished = false;
      renderAgentTranscript();
      renderActivities();
      updateAgentSessionUi();
      await refreshSessionListOnly();
      switchView("agent");
      $("#prompt").focus();
    } catch (error) {
      if (rpc === state.rpc) toast(`新建任务失败：${error.message}`);
    } finally {
      state.creatingSession = false;
      setAgentControls();
    }
  }

  async function ensureAgentSession() {
    if (state.agentSessionId) return state.agentSessionId;
    const snapshot = await state.rpc.request("session.new").promise;
    state.agentSessionId = snapshot.session_id;
    state.agentSnapshot = snapshot;
    updateAgentSessionUi();
    return snapshot.session_id;
  }

  async function refreshSessionListOnly() {
    const rpc = state.rpc;
    const result = await rpc.request("session.list").promise;
    if (rpc !== state.rpc) return;
    state.sessions = result.sessions || [];
    renderSessions();
  }

  function resetAgentSession() {
    state.agentSessionId = null;
    state.agentSnapshot = null;
    state.activities = [];
    state.pendingApproval = null;
    state.pendingApprovals = [];
    state.agentFollowBottom = true;
    state.thinkingFinished = false;
    renderAgentTranscript();
    renderActivities();
    updateAgentSessionUi();
  }

  function updateAgentSessionUi() {
    const id = state.agentSessionId;
    $("#agent-session-id").textContent = id ? shortId(id, 30) : "发送任务时自动创建";
    $("#agent-meta").textContent = id
      ? `当前任务 ${shortId(id, 26)} · 工作区 ${directoryName(state.workspace)}`
      : `准备在 ${directoryName(state.workspace) || "所选目录"} 开始新的开发任务`;
    $("#inspect-current").disabled = !id;
    setAgentControls();
  }

  async function sendPrompt(event) {
    event.preventDefault();
    const prompt = $("#prompt").value.trim();
    if (!prompt || !state.connected || state.activeRequest || state.submitting) return;
    state.submitting = true;
    setAgentControls();
    let sent = false;
    let disconnected = false;
    let optimisticUser = null;
    try {
      const sessionId = await ensureAgentSession();
      if (!state.connected) throw new Error("连接已断开，任务尚未提交");
      state.agentSnapshot ||= { messages: [] };
      state.agentSnapshot.messages ||= [];
      optimisticUser = { role: "user", content: prompt };
      state.agentSnapshot.messages.push(optimisticUser);
      state.draftAssistant = { role: "assistant", content: "" };
      state.pendingApproval = null;
      state.pendingApprovals = [];
      state.agentFollowBottom = true;
      state.thinkingFinished = false;
      state.agentSnapshot.messages.push(state.draftAssistant);
      state.activities = [];
      addActivity("turn", "任务已提交", "Agent 正在理解目标并规划下一步");
      renderAgentTranscript();
      setAgentStatus("running", "工作中");
      $("#cancel-turn").classList.remove("hidden");
      const call = state.rpc.request(
        "chat.send",
        { message: prompt, session_id: sessionId },
        handleAgentEvent,
      );
      if (call.id == null) {
        await call.promise;
        throw new Error("任务尚未提交，连接已断开");
      }
      sent = true;
      $("#prompt").value = "";
      resizePrompt();
      state.activeRequest = call.id;
      state.activeRunId = null;
      state.activeRunSeq = 0;
      setAgentControls();
      await call.promise;
      await finishAgentTurn();
    } catch (error) {
      disconnected = sent && (!state.connected || error.message === "WebSocket 已断开");
      if (disconnected) {
        state.activeRequest = "reconnecting";
        addActivity("turn", "连接中断", "正在重连并确认任务状态；Agent 可能仍在运行");
        setAgentStatus("waiting", "确认任务状态");
        if (state.connected) recoverAgentTurn().catch((failure) => toast(`恢复任务失败：${failure.message}`));
      } else {
        if (!sent && optimisticUser) {
          state.agentSnapshot.messages = state.agentSnapshot.messages.filter((message) => message !== optimisticUser);
        }
        failAgentTurn(error, sent);
        if (!sent && !$("#prompt").value.trim()) {
          $("#prompt").value = prompt;
          resizePrompt();
        }
      }
    } finally {
      state.submitting = false;
      if (!disconnected) clearAgentTurn();
      setAgentControls();
      await refreshSessionListOnly().catch(() => {});
    }
  }

  async function finishAgentTurn() {
    addActivity("turn", "任务完成", "结果已写入当前 Session");
    setAgentStatus("idle", "已完成");
    toast("Agent 任务完成");
    if (state.agentSessionId) await loadAgentSession(state.agentSessionId);
  }

  function failAgentTurn(error, submitted = true) {
    const label = submitted ? "任务失败" : "提交失败";
    state.agentSnapshot ||= { messages: [] };
    state.agentSnapshot.messages ||= [];
    if (state.draftAssistant && !state.draftAssistant.content && !state.draftAssistant.thinking) {
      const index = state.agentSnapshot.messages.indexOf(state.draftAssistant);
      if (index >= 0) state.agentSnapshot.messages.splice(index, 1);
    }
    state.agentSnapshot.messages.push({ role: "system", content: `${label}：${error.message}` });
    addActivity("error", label, error.message);
    setAgentStatus("error", "失败");
    renderAgentTranscript();
    toast(`${label}：${error.message}`);
  }

  function clearAgentTurn() {
    state.activeRequest = null;
    state.activeRunId = null;
    state.activeRunSeq = 0;
    state.draftAssistant = null;
    state.pendingApproval = null;
    state.pendingApprovals = [];
    state.cancelling = false;
    $("#cancel-turn").disabled = false;
    $("#cancel-turn").classList.add("hidden");
  }

  async function recoverAgentTurn() {
    if (state.activeRequest !== "reconnecting" || !state.agentSessionId) return;
    const rpc = state.rpc;
    if (state.recoveryRpc === rpc) return;
    state.recoveryRpc = rpc;
    const sessionId = state.agentSessionId;
    try {
      const snapshot = await loadLatestAgentSnapshot(rpc, sessionId);
      if (rpc !== state.rpc || state.activeRequest !== "reconnecting") return;
      const active = snapshot.active_requests || [];
      const run = state.activeRunId
        ? await rpc.request("run.read", { run_id: state.activeRunId }).promise
        : null;
      if (rpc !== state.rpc || state.activeRequest !== "reconnecting") return;
      const matches = run && active.some((id) => JSON.stringify(id) === JSON.stringify(run.request_id));
      if (!active.length || (run && !matches)) {
        state.agentSnapshot = snapshot;
        renderAgentTranscript();
        showRecoveredTerminal(run);
        clearAgentTurn();
        setAgentControls();
        return;
      }
      const requestId = run ? run.request_id : active.length === 1 ? active[0] : null;
      if (requestId == null) {
        setAgentStatus("error", "需选择任务");
        addActivity("error", "无法自动恢复", "Session 中有多个活动任务，请在链路详情中确认目标");
        clearAgentTurn();
        setAgentControls();
        return;
      }
      state.pendingApprovals = snapshot.pending_approvals || [];
      state.pendingApproval = state.pendingApprovals[0] || null;
      renderAgentTranscript();
      const call = rpc.request("agent.subscribe", {
        request_id: requestId,
        session_id: sessionId,
        after_seq: state.activeRunSeq,
      }, handleAgentEvent);
      if (call.id == null) await call.promise;
      state.activeRequest = call.id;
      setAgentStatus("running", "已恢复任务流");
      setAgentControls();
      try {
        const result = await call.promise;
        if (result?.subscribed === false) {
          state.agentSnapshot = await loadLatestAgentSnapshot(rpc, sessionId);
          renderAgentTranscript();
          setAgentStatus("error", "执行体不可用");
          addActivity("error", "订阅已结束", "执行体不可用；请查看 Session 与链路记录");
          return;
        }
        await finishAgentTurn();
      } catch (error) {
        if (rpc !== state.rpc) return;
        if (!state.connected || error.message === "WebSocket 已断开") {
          state.activeRequest = "reconnecting";
          setAgentStatus("waiting", "重新连接中");
          if (!state.reconnectTimer) scheduleReconnect();
          return;
        }
        failAgentTurn(error);
      } finally {
        if (rpc === state.rpc) {
          if (state.activeRequest !== "reconnecting") clearAgentTurn();
          setAgentControls();
          await refreshSessionListOnly().catch(() => {});
        }
      }
    } catch (error) {
      if (rpc === state.rpc && state.connected && state.activeRequest === "reconnecting") {
        setAgentStatus("error", "恢复失败");
        addActivity("error", "恢复失败", error.message);
        clearAgentTurn();
        setAgentControls();
        toast(`恢复任务状态失败：${error.message}；可在 Session 查看中确认结果`);
      }
    } finally {
      if (state.recoveryRpc === rpc) state.recoveryRpc = null;
    }
  }

  function showRecoveredTerminal(run) {
    const status = run?.status;
    if (status === "completed") {
      setAgentStatus("idle", "已完成");
      addActivity("turn", "任务已完成", "已从 daemon 读取最终状态");
    } else if (status === "failed" || status === "cancelled" || status === "unknown_after_restart") {
      setAgentStatus("error", status === "cancelled" ? "已取消" : status === "failed" ? "失败" : "状态待核对");
      addActivity("error", "任务未完成", run.error_message || `运行状态：${status}`);
    } else {
      setAgentStatus("waiting", "状态待核对");
      addActivity("turn", "任务不再运行", "请查看 Session 中的最终消息和链路记录");
    }
  }

  function handleAgentEvent(frame) {
    if (frame.run_id) state.activeRunId = frame.run_id;
    if (Number.isInteger(frame.seq)) state.activeRunSeq = Math.max(state.activeRunSeq, frame.seq);
    const event = frame.event;
    const data = frame.data || {};
    if (event === "text_delta") {
      state.draftAssistant ||= { role: "assistant", content: "" };
      state.draftAssistant.content += data.delta || "";
      scheduleAgentTranscriptRender();
      setAgentStatus("running", "生成响应");
    } else if (event === "thinking_delta") {
      state.draftAssistant ||= { role: "assistant", content: "" };
      state.draftAssistant.thinking = (state.draftAssistant.thinking || "") + (data.delta || "");
      state.thinkingFinished = false;
      scheduleAgentTranscriptRender();
      setAgentStatus("running", "思考中");
    } else if (event === "thinking_finished") {
      state.thinkingFinished = true;
      scheduleAgentTranscriptRender();
      setAgentStatus("running", "生成响应");
    } else if (event === "tool_started") {
      addActivity("tool", `执行 ${data.name || "工具"}`, `第 ${data.round || "?"} 轮 · 正在运行`);
      setAgentStatus("running", "执行工具");
    } else if (event === "tool_finished") {
      addActivity(data.success === false ? "error" : "tool", `${data.name || "工具"}${data.success === false ? "失败" : "完成"}`, `${data.duration_ms || 0}ms`);
    } else if (event === "approval_required") {
      renderApproval(data.approval);
      addActivity("approval", "等待审批", data.approval?.prompt || "Agent 请求执行受保护操作");
      setAgentStatus("waiting", "待审批");
    }
  }

  function addActivity(kind, title, detail) {
    state.activities.unshift({ kind, title, detail });
    state.activities = state.activities.slice(0, 30);
    renderActivities();
  }

  function renderActivities() {
    const visible = state.activities.filter((item) => item.kind !== "tool");
    const tools = state.activities.filter((item) => item.kind === "tool");
    const toolDetails = tools.length ? `<details class="activity-tools">
      <summary>工具动态 · ${tools.length} 项（默认折叠）</summary>
      <div class="activity-tools-list">${tools.map(renderActivityItem).join("")}</div>
    </details>` : "";
    $("#activity-list").innerHTML = visible.length || toolDetails
      ? `${visible.map(renderActivityItem).join("")}${toolDetails}`
      : `<div class="activity-empty">发出任务后，这里会显示模型响应、审批状态和执行结果。</div>`;
  }

  function renderActivityItem(item) {
    return `<div class="activity-item ${escapeAttr(item.kind)}"><strong>${escapeHtml(item.title)}</strong><span>${escapeHtml(item.detail)}</span></div>`;
  }

  function setAgentStatus(mode, label) {
    const node = $("#agent-status");
    node.className = `status-pill ${mode}`;
    node.textContent = label;
  }

  function renderApproval(approval) {
    if (!approval) return;
    state.pendingApproval = approval;
    if (!state.pendingApprovals.some((item) => item.id === approval.id)) state.pendingApprovals.push(approval);
    renderAgentTranscript();
  }

  async function respondApproval(id, approved, card) {
    if (!state.connected) { toast("连接恢复后才能处理审批。"); return; }
    const buttons = [...card.querySelectorAll("button")];
    buttons.forEach((button) => { button.disabled = true; });
    try {
      await state.rpc.request("approval.respond", { approval_id: id, approved }).promise;
      state.pendingApprovals = state.pendingApprovals.filter((item) => item.id !== id);
      state.pendingApproval = state.pendingApprovals[0] || null;
      renderAgentTranscript();
      addActivity("approval", approved ? "已允许操作" : "已拒绝操作", "Agent 将继续处理当前任务");
      setAgentStatus("running", "继续工作");
    } catch (error) {
      buttons.forEach((button) => { button.disabled = !state.connected; });
      toast(`审批失败：${error.message}`);
    }
  }

  async function cancelTurn() {
    if (!state.activeRequest || state.cancelling || !state.connected) return;
    state.cancelling = true;
    $("#cancel-turn").disabled = true;
    try {
      const target = state.activeRunId
        ? { run_id: state.activeRunId, session_id: state.agentSessionId }
        : { request_id: state.activeRequest, session_id: state.agentSessionId };
      const result = await state.rpc.request("agent.cancel", target).promise;
      if (result?.cancelled === false) throw new Error(result.reason || "请求未在运行");
      addActivity("error", "已请求停止", "等待 Agent 结束当前步骤");
      toast("已发送停止请求");
    } catch (error) {
      state.cancelling = false;
      $("#cancel-turn").disabled = false;
      toast(`停止失败：${error.message}`);
    }
  }

  async function continueInspectedSession() {
    if (!state.inspectedSessionId || !state.inspectedSnapshot) return;
    if (state.activeRequest || state.submitting) {
      toast("当前任务结束后再切换 Agent Session。");
      return;
    }
    const sessionId = state.inspectedSessionId;
    const rpc = state.rpc;
    $("#continue-session").disabled = true;
    try {
      const snapshot = await loadLatestAgentSnapshot(rpc, sessionId);
      if (rpc !== state.rpc || sessionId !== state.inspectedSessionId) return;
      state.agentSessionId = sessionId;
      state.agentSnapshot = snapshot;
      state.activities = [];
      state.pendingApproval = null;
      state.pendingApprovals = [];
      state.agentFollowBottom = true;
      state.thinkingFinished = false;
      renderAgentTranscript();
      renderActivities();
      updateAgentSessionUi();
      switchView("agent");
      $("#prompt").focus();
    } catch (error) {
      if (rpc === state.rpc && sessionId === state.inspectedSessionId) toast(`继续 Session 失败：${error.message}`);
    } finally {
      if (rpc === state.rpc && sessionId === state.inspectedSessionId) $("#continue-session").disabled = false;
    }
  }

  function inspectCurrentAgentSession() {
    if (!state.agentSessionId) return;
    state.inspectedSessionId = state.agentSessionId;
    switchView("sessions");
  }

  function switchView(view) {
    state.activeView = view;
    $$("[data-view]").forEach((button) => {
      const active = button.dataset.view === view;
      button.classList.toggle("active", active);
      button.setAttribute("aria-current", active ? "page" : "false");
    });
    $$(".view").forEach((panel) => panel.classList.toggle("is-active", panel.id === `${view}-view`));
    if (view === "sessions") {
      renderSessions();
      const next = state.inspectedSessionId || state.sessions[0]?.id;
      if (next && state.inspectedSnapshot?.session_id !== next) inspectSession(next).catch((error) => toast(error.message));
    }
  }

  function renderTrace() {
    const records = state.traces || [];
    const visible = records.map((record, index) => ({ record, index }))
      .filter(({ record }) => state.traceFilter === "all" || traceGroup(record.kind) === state.traceFilter);
    $("#trace-count").textContent = state.inspectedTraceTotal || records.length;
    $("#trace-list").innerHTML = visible.length
      ? visible.map(({ record, index }) => renderTraceItem(record, index)).join("")
      : `<div class="trace-empty">${records.length ? "当前筛选条件下没有记录。" : "这个 Session 尚无结构化链路。旧 Session 的消息仍会正常显示。"}</div>`;
    $("#trace-list").querySelectorAll("details[data-trace-key]").forEach((detail) => detail.addEventListener("toggle", () => {
      if (!detail.isConnected) return;
      const key = Number(detail.dataset.traceKey);
      if (detail.open) state.openTraceKeys.add(key);
      else state.openTraceKeys.delete(key);
    }));
    const modelCalls = records.filter((record) => record.kind === "model_request").length;
    const toolCalls = records.filter((record) => record.kind === "tool_started").length;
    const turns = records.filter((record) => record.kind === "turn_completed");
    const totalMs = turns.reduce((sum, record) => sum + (record.duration_ms || 0), 0);
    const scope = state.inspectedTraceTotal > records.length ? "（已加载）" : "";
    $("#trace-summary").innerHTML = `
      <div><strong>${modelCalls}</strong><span>模型调用${scope}</span></div>
      <div><strong>${toolCalls}</strong><span>工具调用${scope}</span></div>
      <div><strong>${formatDuration(totalMs)}</strong><span>总耗时${scope}</span></div>`;
  }

  function renderTraceItem(record, index) {
    const group = traceGroup(record.kind);
    const failed = record.success === false ? " failed" : "";
    const info = traceInfo(record);
    return `<article class="trace-item ${group}${failed}">
      <div class="trace-title"><strong>${escapeHtml(info.title)}</strong><time>${formatTime(record.timestamp_ms)}</time></div>
      <div class="trace-meta">${escapeHtml(info.meta)}</div>
      ${info.payload == null ? "" : `<details data-trace-key="${index}"${state.openTraceKeys.has(index) ? " open" : ""}><summary>${escapeHtml(info.detailLabel)}</summary><pre>${escapeHtml(stringify(info.payload))}</pre></details>`}
    </article>`;
  }

  function traceInfo(record) {
    switch (record.kind) {
      case "turn_started": return { title: "Turn started", meta: `request ${record.request_id}`, detailLabel: "用户输入", payload: record.input };
      case "turn_completed": return { title: record.success ? "Turn completed" : "Turn failed", meta: `request ${record.request_id} · ${formatDuration(record.duration_ms)}`, detailLabel: "错误详情", payload: record.error };
      case "model_request": return { title: `LM request · R${record.round}`, meta: `${record.provider} · ${record.messages?.length || 0} messages · ${record.tools?.length || 0} tools`, detailLabel: "查看完整提示词与工具定义", payload: { messages: record.messages, tools: record.tools } };
      case "model_response": return { title: `LM response · R${record.round}`, meta: `${record.success ? "成功" : "失败"} · ${formatDuration(record.duration_ms)}${record.first_delta_ms == null ? "" : ` · 首字 ${record.first_delta_ms}ms`}`, detailLabel: "查看完整模型响应", payload: record.response || record.error };
      case "tool_started": return { title: `${record.name} · start`, meta: `R${record.round} · ${shortId(record.tool_call_id, 18)}`, detailLabel: "查看调用参数", payload: record.arguments };
      case "tool_finished": return { title: `${record.name} · ${record.success ? "done" : "failed"}`, meta: `R${record.round} · ${formatDuration(record.duration_ms)}`, detailLabel: "查看工具输出", payload: record.output || record.error };
      default: return { title: record.kind || "Event", meta: "", detailLabel: "原始记录", payload: record };
    }
  }

  function traceGroup(kind = "") {
    if (kind.startsWith("model_")) return "model";
    if (kind.startsWith("tool_")) return "tool";
    return "turn";
  }

  async function openWorkspacePicker() {
    if (state.activeRequest) {
      toast("Agent 正在工作，结束或停止当前任务后再切换目录。");
      return;
    }
    $("#workspace-dialog").showModal();
    state.browsePath = null;
    $("#select-workspace").disabled = true;
    await browseDirectory(state.workspace || state.defaultWorkspace).catch((error) => {
      $("#directory-list").innerHTML = `<div class="directory-empty">${escapeHtml(error.message)}</div>`;
    });
  }

  async function browseDirectory(path) {
    const request = ++state.browseRequest;
    $("#directory-list").innerHTML = `<div class="directory-empty">正在读取目录…</div>`;
    const query = path ? `?path=${encodeURIComponent(path)}` : "";
    let listing;
    try {
      listing = await fetchJson(`/api/directories${query}`);
    } catch (error) {
      if (request !== state.browseRequest) return;
      state.browsePath = null;
      state.browseParent = null;
      $("#select-workspace").disabled = true;
      $("#directory-list").innerHTML = `<div class="directory-empty">${escapeHtml(error.message)}</div>`;
      throw error;
    }
    if (request !== state.browseRequest) return;
    state.browsePath = listing.path;
    $("#select-workspace").disabled = false;
    state.browseParent = listing.parent || null;
    $("#browse-path").value = listing.path;
    $("#browse-current").textContent = listing.path;
    $("#browse-parent").disabled = !listing.parent;
    renderWorkspaceFavorites(listing.favorites || []);
    $("#directory-list").innerHTML = listing.directories?.length
      ? listing.directories.map((directory) => `<button class="directory-item" type="button" data-directory="${escapeAttr(directory.path)}"><span>▱</span><strong>${escapeHtml(directory.name)}</strong></button>`).join("")
      : `<div class="directory-empty">这个目录没有子文件夹。</div>`;
    $$('[data-directory]').forEach((button) => button.addEventListener("click", () => browseDirectory(button.dataset.directory).catch((error) => toast(error.message))));
  }

  function renderWorkspaceFavorites(serverFavorites) {
    const recents = recentWorkspaces();
    const favorites = [...new Set([...serverFavorites, ...recents])].filter(Boolean).slice(0, 10);
    $("#workspace-favorites").innerHTML = favorites.map((path) => `<button type="button" data-favorite="${escapeAttr(path)}" title="${escapeAttr(path)}">${escapeHtml(directoryName(path) || path)}</button>`).join("");
    $$('[data-favorite]').forEach((button) => button.addEventListener("click", () => browseDirectory(button.dataset.favorite).catch((error) => toast(error.message))));
  }

  async function selectBrowsedWorkspace() {
    if (state.activeRequest || state.submitting) {
      toast("当前任务结束后再切换工作目录。");
      return;
    }
    const typedPath = $("#browse-path").value.trim();
    if (!typedPath) { toast("请输入或选择工作目录。"); return; }
    if (typedPath && typedPath !== state.browsePath) {
      try { await browseDirectory(typedPath); } catch (error) { toast(`读取目录失败：${error.message}`); return; }
    }
    if (!state.browsePath) { toast("请先选择有效目录。"); return; }
    if (state.browsePath === state.workspace) {
      $("#workspace-dialog").close();
      return;
    }
    $("#select-workspace").disabled = true;
    state.browseRequest += 1;
    try {
      if (await connect(state.browsePath)) {
        $("#workspace-dialog").close();
        switchView("agent");
      }
    } finally {
      $("#select-workspace").disabled = false;
    }
  }

  function resetWorkspaceState() {
    state.inspectGeneration += 1;
    state.sessions = [];
    state.agentSessionId = null;
    state.inspectedSessionId = null;
    state.agentSnapshot = null;
    state.inspectedSnapshot = null;
    state.traces = [];
    state.pendingApproval = null;
    state.pendingApprovals = [];
    state.recoveryRpc = null;
    state.collapsedDays.clear();
    state.openTraceKeys.clear();
    state.traceFilter = "all";
    $("#session-search").value = "";
    $$(".trace-filter button").forEach((button) => {
      const active = button.dataset.filter === "all";
      button.classList.toggle("active", active);
      button.setAttribute("aria-pressed", String(active));
    });
    $("#session-title").textContent = "选择一个 Session";
    $("#session-meta").textContent = "查看 Web、TUI、CLI 与 ACP 产生的历史会话。";
    $("#session-transcript").innerHTML = `<div class="empty-state"><h2>选择左侧 Session</h2></div>`;
    $("#continue-session").disabled = true;
    $("#message-pager").classList.add("hidden");
    $("#trace-pager").classList.add("hidden");
    state.permissionMode = null;
    state.modelProfiles = [];
    state.activeModelId = null;
    state.modelConfigPath = "";
    state.thinkingFinished = false;
    state.inspectedMessageOffset = 0;
    state.inspectedMessageTotal = 0;
    state.inspectedMessageHasMore = false;
    state.inspectedTraceOffset = 0;
    state.inspectedTraceTotal = 0;
    state.inspectedTraceHasMore = false;
    state.activities = [];
    renderSessions();
    renderAgentTranscript();
    renderActivities();
    renderTrace();
    updateAgentSessionUi();
  }

  async function fetchJson(path, options = {}) {
    const response = await fetch(path, { ...options, headers: { ...authorizationHeaders(), ...(options.headers || {}) } });
    const value = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(value.error?.message || `请求失败：${response.status}`);
    return value;
  }

  function authorizationHeaders() {
    const token = apiToken();
    return token ? { Authorization: `Bearer ${token}` } : {};
  }

  function safeStorageGet(key) {
    try { return localStorage.getItem(key); } catch { return null; }
  }
  function apiToken() { return state.ephemeralToken ?? safeStorageGet("my-agent-token") ?? ""; }
  function recentWorkspaces() {
    try {
      const paths = JSON.parse(safeStorageGet("my-agent-workspaces") || "[]");
      return Array.isArray(paths) ? paths.filter((path) => typeof path === "string") : [];
    }
    catch { return []; }
  }
  function rememberWorkspace(path) {
    if (!path) return;
    try { localStorage.setItem("my-agent-workspace", path); } catch { return; }
    const recents = [path, ...recentWorkspaces().filter((item) => item !== path)].slice(0, 8);
    try { localStorage.setItem("my-agent-workspaces", JSON.stringify(recents)); } catch { /* private browsing */ }
  }
  function updateWorkspaceUrl(path) {
    const url = new URL(location.href);
    if (path) url.searchParams.set("workspace", path);
    history.replaceState(null, "", url);
  }
  function directoryName(path = "") {
    return path.replace(/[\\/]+$/, "").split(/[\\/]/).pop() || path;
  }
  function bindStarterButtons() {
    $$('[data-starter]').forEach((button) => button.addEventListener("click", () => {
      $("#prompt").value = button.dataset.starter;
      resizePrompt();
      $("#prompt").focus();
    }));
  }
  function resizePrompt() {
    const prompt = $("#prompt");
    prompt.style.height = "auto";
    prompt.style.height = `${Math.min(Math.max(prompt.scrollHeight, 88), 220)}px`;
    const count = graphemeSegmenter
      ? [...graphemeSegmenter.segment(prompt.value)].length
      : Array.from(prompt.value).length;
    $("#prompt-count").textContent = `${count} 字`;
  }
  function shortId(value = "", max = 22) {
    if (value.length <= max) return value;
    const keep = Math.max(5, Math.floor((max - 1) / 2));
    return `${value.slice(0, keep)}…${value.slice(-keep)}`;
  }
  function formatTime(value) {
    if (!Number.isFinite(Number(value)) || !value) return "时间未知";
    const date = new Date(Number(value));
    if (Number.isNaN(date.getTime())) return "时间未知";
    return new Intl.DateTimeFormat("zh-CN", { hour: "2-digit", minute: "2-digit", second: "2-digit", fractionalSecondDigits: 3 }).format(date);
  }
  function formatRelative(seconds) {
    if (!seconds) return "时间未知";
    const delta = Math.max(0, Math.floor(Date.now() / 1000 - seconds));
    if (delta < 60) return "刚刚更新";
    if (delta < 3600) return `${Math.floor(delta / 60)} 分钟前`;
    if (delta < 86400) return `${Math.floor(delta / 3600)} 小时前`;
    return `${Math.floor(delta / 86400)} 天前`;
  }
  function formatDuration(ms = 0) {
    if (!Number.isFinite(ms) || ms < 0) return "—";
    if (ms < 1000) return `${ms}ms`;
    if (ms < 60000) return `${(ms / 1000).toFixed(ms < 10000 ? 1 : 0)}s`;
    return `${(ms / 60000).toFixed(1)}m`;
  }
  function stringify(value) { return typeof value === "string" ? value : JSON.stringify(value, null, 2); }
  function escapeHtml(value = "") { return String(value).replace(/[&<>"']/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[char]); }
  function escapeAttr(value = "") { return escapeHtml(value); }
  function statusLabel(status) { return ({ idle: "空闲", running: "运行中", waiting: "等待审批" })[status] || status; }
  let toastTimer;
  function toast(message) {
    const node = $("#toast");
    node.textContent = message;
    node.classList.add("show");
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => node.classList.remove("show"), 3400);
  }

  $("#composer").addEventListener("submit", sendPrompt);
  $("#prompt").addEventListener("input", resizePrompt);
  $("#prompt").addEventListener("keydown", (event) => {
    if (!event.isComposing && event.keyCode !== 229 && (event.metaKey || event.ctrlKey) && event.key === "Enter") sendPrompt(event);
  });
  $("#new-agent-session").addEventListener("click", newAgentSession);
  $("#cancel-turn").addEventListener("click", cancelTurn);
  $("#inspect-current").addEventListener("click", inspectCurrentAgentSession);
  $("#continue-session").addEventListener("click", continueInspectedSession);
  $("#load-more-messages").addEventListener("click", loadMoreMessages);
  $("#load-more-traces").addEventListener("click", loadMoreTraces);
  $("#refresh").addEventListener("click", () => refreshSessions().catch((error) => toast(error.message)));
  $("#session-search").addEventListener("input", renderSessions);
  $$("[data-view]").forEach((button) => button.addEventListener("click", () => switchView(button.dataset.view)));
  $("#workspace-trigger").addEventListener("click", openWorkspacePicker);
  $("#change-workspace").addEventListener("click", openWorkspacePicker);
  $("#permission-trigger").addEventListener("click", openPermissionsDialog);
  $$('[data-permission-mode]').forEach((button) => button.addEventListener("click", () => selectPermissionMode(button.dataset.permissionMode)));
  $("#browse-go").addEventListener("click", () => browseDirectory($("#browse-path").value.trim()).catch((error) => toast(error.message)));
  $("#browse-path").addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      browseDirectory(event.currentTarget.value.trim()).catch((error) => toast(error.message));
    }
  });
  $("#browse-parent").addEventListener("click", () => state.browseParent && browseDirectory(state.browseParent).catch((error) => toast(error.message)));
  $("#select-workspace").addEventListener("click", selectBrowsedWorkspace);
  $("#settings").addEventListener("click", () => {
    $("#token").value = apiToken();
    $("#settings-dialog").showModal();
    loadModels().catch((error) => toast(`读取模型配置失败：${error.message}`));
  });
  $("#open-model-settings").addEventListener("click", () => {
    $("#token").value = apiToken();
    $("#settings-dialog").showModal();
    loadModels().catch((error) => toast(`读取模型配置失败：${error.message}`));
  });
  $("#refresh-models").addEventListener("click", () => loadModels().catch((error) => toast(`读取模型配置失败：${error.message}`)));
  $("#new-model-profile").addEventListener("click", () => {
    for (const id of ["model-profile-name", "model-profile-id", "model-name", "model-base-url", "model-api-key"]) $("#" + id).value = "";
    $("#model-api-type").value = "openai-chat";
    $("#model-activate").checked = true;
    $("#model-profile-name").focus();
  });
  $("#save-model").addEventListener("click", saveModelProfile);
  $("#reconnect").addEventListener("click", () => {
    state.ephemeralToken = $("#token").value.trim();
    try { localStorage.setItem("my-agent-token", state.ephemeralToken); } catch { toast("浏览器无法持久保存 Token；本页关闭前仍可使用。"); }
    $("#settings-dialog").close();
    connect();
  });
  $("#retry-connection").addEventListener("click", () => connect().catch((error) => toast(error.message)));
  $("#agent-transcript").addEventListener("scroll", () => updateLatestButton());
  $("#jump-latest").addEventListener("click", scrollAgentToLatest);
  $$(".trace-filter button").forEach((button) => button.addEventListener("click", () => {
    state.traceFilter = button.dataset.filter;
    $$(".trace-filter button").forEach((item) => {
      item.classList.toggle("active", item === button);
      item.setAttribute("aria-pressed", String(item === button));
    });
    renderTrace();
  }));

  $("#send-modifier").textContent = /Mac|iPhone|iPad/.test(navigator.platform || "") ? "⌘" : "Ctrl";
  setInterval(updateRelativeTimes, 60000);
  bindStarterButtons();
  resizePrompt();
  switchView("agent");
  bootstrap();
})();
