const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

const source = fs.readFileSync(path.join(__dirname, "app.js"), "utf8");
const entry = source.indexOf('  $("#composer").addEventListener("submit", sendPrompt);');
assert.ok(entry > 0, "前端测试入口存在");
const harness = `${source.slice(0, entry)}
  globalThis.__appTest = { state, RpcSocket, connect, sendPrompt, recoverAgentTurn,
    renderTranscript, updateLatestButton, resizePrompt, inspectSession, refreshSessions,
    cancelTurn, useModelProfile, saveModelProfile, browseDirectory, selectBrowsedWorkspace,
    loadLatestAgentSnapshot, loadModels, resetWorkspaceState, formatDuration,
    renderAgentTranscript, newAgentSession, refreshSubagents };
})();`;

function createHarness() {
  const nodes = new Map();
  const timers = [];
  const stored = new Map();
  class FakeElement {
    constructor() {
      this.value = "";
      this.textContent = "";
      this.innerHTML = "";
      this.scrollTop = 0;
      this.scrollHeight = 500;
      this.clientHeight = 200;
      this.style = {};
      this.attributes = new Map();
      this.classes = new Set();
      this.classList = {
        add: (name) => this.classes.add(name),
        remove: (name) => this.classes.delete(name),
        toggle: (name, forced) => {
          if (forced === undefined ? !this.classes.has(name) : forced) this.classes.add(name);
          else this.classes.delete(name);
        },
        contains: (name) => this.classes.has(name),
      };
    }
    querySelector() { return new FakeElement(); }
    querySelectorAll() { return []; }
    setAttribute(key, value) { this.attributes.set(key, value); }
    addEventListener() {}
    focus() {}
  }
  class FakeWebSocket {
    static OPEN = 1;
    static instances = [];
    constructor() {
      this.readyState = 0;
      this.listeners = new Map();
      this.sent = [];
      FakeWebSocket.instances.push(this);
    }
    addEventListener(name, listener) {
      const listeners = this.listeners.get(name) || [];
      listeners.push(listener);
      this.listeners.set(name, listeners);
    }
    emit(name, data = {}) {
      for (const listener of this.listeners.get(name) || []) listener(data);
    }
    send(value) { this.sent.push(value); }
    close() {
      this.readyState = 3;
      this.emit("close");
    }
  }
  const context = {
    document: {
      querySelector(selector) {
        if (!nodes.has(selector)) nodes.set(selector, new FakeElement());
        return nodes.get(selector);
      },
      querySelectorAll() { return []; },
    },
    location: { href: "http://localhost/", protocol: "http:", host: "localhost" },
    history: { replaceState() {} },
    localStorage: {
      getItem: (key) => stored.get(key) || null,
      setItem: (key, value) => stored.set(key, value),
    },
    WebSocket: FakeWebSocket,
    URL,
    Intl,
    Date,
    console,
    fetch: async () => ({ ok: true, json: async () => ({ profiles: [] }) }),
    requestAnimationFrame: (callback) => callback(),
    setTimeout: (callback, delay) => {
      const timer = { callback, delay, cancelled: false };
      timers.push(timer);
      return timer;
    },
    clearTimeout: (timer) => { if (timer) timer.cancelled = true; },
  };
  vm.runInNewContext(harness, context, { filename: "app.js" });
  return { ...context.__appTest, context, nodes, timers, FakeWebSocket, element: context.document.querySelector };
}

test("WebSocket 握手前关闭立即失败", async () => {
  const app = createHarness();
  const socket = new app.RpcSocket("", "/work");
  const pending = socket.connect();
  app.FakeWebSocket.instances[0].close();
  await assert.rejects(pending, /WebSocket 已断开/);
});

test("WebSocket 握手超时关闭连接", async () => {
  const app = createHarness();
  const socket = new app.RpcSocket("", "/work");
  const pending = socket.connect();
  app.timers.find((timer) => timer.delay === 8000).callback();
  await assert.rejects(pending, /连接 daemon 超时/);
  assert.equal(app.FakeWebSocket.instances[0].readyState, 3);
});

test("活动连接断开后安排有限自动重连", async () => {
  const app = createHarness();
  const rpc = new app.RpcSocket("", "/work");
  app.state.rpc = rpc;
  app.state.connected = true;
  app.state.workspace = "/work";
  const pending = rpc.connect();
  const socket = app.FakeWebSocket.instances[0];
  socket.readyState = 1;
  socket.emit("open");
  socket.emit("message", { data: JSON.stringify({ type: "connected", workspace: "/work" }) });
  await pending;
  socket.close();
  assert.equal(app.state.connected, false);
  assert.equal(app.state.reconnectAttempts, 1);
  assert.ok(app.timers.some((timer) => timer.delay === 1000 && !timer.cancelled));
});

test("Session 列表错误不把已连接 socket 标成断线", async () => {
  const app = createHarness();
  app.RpcSocket.prototype.connect = async function () { return "/work"; };
  app.RpcSocket.prototype.request = function (method) {
    return { id: 1, promise: method === "session.list"
      ? Promise.reject(new Error("列表暂不可用"))
      : Promise.resolve({ mode: "default" }) };
  };
  assert.equal(await app.connect("/work"), true);
  assert.equal(app.state.connected, true);
  assert.equal(app.state.workspace, "/work");
  assert.match(app.nodes.get("#toast").textContent, /读取 Session 列表失败/);
});

test("目标工作区连接失败保留当前 Session", async () => {
  const app = createHarness();
  app.state.workspace = "/old";
  app.state.agentSessionId = "session-old";
  app.state.connected = true;
  app.state.rpc = { socket: { readyState: 1 }, close() {} };
  app.RpcSocket.prototype.connect = async function () { throw new Error("目录不可用"); };
  assert.equal(await app.connect("/new"), false);
  assert.equal(app.state.workspace, "/old");
  assert.equal(app.state.agentSessionId, "session-old");
  assert.equal(app.state.connected, true);
});

test("首次提交等待新建 Session 时只发一次请求", async () => {
  const app = createHarness();
  app.state.connected = true;
  app.state.workspace = "/work";
  app.element("#prompt").value = "修复测试";
  let creates = 0;
  let resolveCreate;
  app.state.rpc = {
    request(method) {
      if (method === "session.new") {
        creates += 1;
        return { id: 1, promise: new Promise((resolve) => { resolveCreate = resolve; }) };
      }
      if (method === "chat.send") return { id: 2, promise: Promise.resolve({ content: "完成" }) };
      if (method === "session.load_page") return { id: 3, promise: Promise.resolve({ messages: [], status: "idle" }) };
      if (method === "session.list") return { id: 4, promise: Promise.resolve({ sessions: [] }) };
      throw new Error(method);
    },
  };
  const event = { preventDefault() {} };
  const first = app.sendPrompt(event);
  await app.sendPrompt(event);
  assert.equal(creates, 1);
  resolveCreate({ session_id: "session-one", messages: [] });
  await first;
  assert.equal(app.state.submitting, false);
});

test("发送未被 socket 接受时保留草稿并移除乐观消息", async () => {
  const app = createHarness();
  app.state.connected = true;
  app.state.agentSessionId = "session-one";
  app.state.agentSnapshot = { messages: [] };
  app.element("#prompt").value = "未发送的任务";
  app.state.rpc = { request(method) {
    if (method === "chat.send") return { id: null, promise: Promise.reject(new Error("连接已断开")) };
    if (method === "session.list") return { id: 2, promise: Promise.resolve({ sessions: [] }) };
    throw new Error(method);
  } };
  await app.sendPrompt({ preventDefault() {} });
  assert.equal(app.element("#prompt").value, "未发送的任务");
  assert.equal(app.state.agentSnapshot.messages.some((message) => message.role === "user"), false);
});

test("只读历史重绘保留滚动位置，回到最新按钮可在完成后显示", () => {
  const app = createHarness();
  const transcript = app.element("#session-transcript");
  transcript.scrollTop = 100;
  app.renderTranscript(transcript, [{ role: "user", content: "旧消息" }], false);
  assert.equal(transcript.scrollTop, 100);
  const agent = app.element("#agent-transcript");
  agent.scrollTop = 100;
  app.state.activeRequest = null;
  app.updateLatestButton(agent);
  assert.equal(app.element("#jump-latest").classList.contains("hidden"), false);
});

test("流式重绘保留用户展开的消息详情", () => {
  const app = createHarness();
  const transcript = app.element("#agent-transcript");
  let details = [{ open: true }];
  transcript.querySelectorAll = () => details;
  Object.defineProperty(transcript, "innerHTML", {
    set() { details = [{ open: false }]; },
    get() { return ""; },
  });
  app.renderTranscript(transcript, [{ role: "tool", content: "输出" }], true);
  assert.equal(details[0].open, true);
});

test("字数按字素计算组合 emoji", () => {
  const app = createHarness();
  app.element("#prompt").value = "👩‍💻";
  app.resizePrompt();
  assert.equal(app.element("#prompt-count").textContent, "1 字");
});

test("断线恢复订阅活动请求，完成后清理运行状态", async () => {
  const app = createHarness();
  app.state.connected = true;
  app.state.agentSessionId = "session-one";
  app.state.activeRequest = "reconnecting";
  app.state.rpc = {
    request(method) {
      if (method === "session.load_page") {
        return { id: 1, promise: Promise.resolve({ messages: [], active_requests: [17], status: "running" }) };
      }
      if (method === "agent.subscribe") return { id: 2, promise: Promise.resolve({ content: "完成" }) };
      if (method === "session.list") return { id: 3, promise: Promise.resolve({ sessions: [] }) };
      throw new Error(method);
    },
  };
  await app.recoverAgentTurn();
  assert.equal(app.state.activeRequest, null);
  assert.equal(app.element("#agent-status").textContent, "已完成");
  assert.doesNotMatch(app.element("#toast").textContent, /任务失败/);
});

test("重连刷新列表时保留正在运行的 Agent Session", async () => {
  const app = createHarness();
  app.state.connected = true;
  app.state.activeRequest = "reconnecting";
  app.state.agentSessionId = "session-one";
  app.state.agentSnapshot = { messages: [{ role: "user", content: "正在运行" }] };
  app.state.rpc = { request(method) {
    if (method === "session.list") return { id: 1, promise: Promise.resolve({ sessions: [] }) };
    throw new Error(`不应读取 ${method}`);
  } };
  await app.refreshSessions();
  assert.equal(app.state.agentSessionId, "session-one");
  assert.equal(app.state.agentSnapshot.messages[0].content, "正在运行");
});

test("停止任务按 run ID 精确取消且避免重复请求", async () => {
  const app = createHarness();
  app.state.connected = true;
  app.state.activeRequest = 9;
  app.state.activeRunId = "run-42";
  app.state.agentSessionId = "session-one";
  const calls = [];
  let resolveCancel;
  app.state.rpc = { request(method, params) {
    calls.push({ method, params });
    return { id: 10, promise: new Promise((resolve) => { resolveCancel = resolve; }) };
  } };
  const first = app.cancelTurn();
  await app.cancelTurn();
  assert.equal(calls.length, 1);
  assert.equal(calls[0].params.run_id, "run-42");
  assert.equal(calls[0].params.session_id, "session-one");
  resolveCancel({ cancelled: true });
  await first;
});

test("运行中仍可激活和保存模型，并提示下一轮生效", async () => {
  const app = createHarness();
  app.state.connected = true;
  app.state.activeRequest = 9;
  app.state.workspace = "/work";
  const paths = [];
  app.context.fetch = async (path) => {
    paths.push(path);
    return { ok: true, json: async () => path === "/api/models"
      ? { profiles: [], active_id: "model-one" }
      : { profile: { name: "模型一" }, active_id: "model-one" } };
  };
  await app.useModelProfile("model-one");
  assert.ok(paths.includes("/api/models/activate"));
  assert.match(app.element("#toast").textContent, /后续任务使用新模型/);
  app.element("#model-api-type").value = "ollama";
  app.element("#model-name").value = "local";
  app.element("#model-base-url").value = "http://localhost:11434";
  app.element("#model-activate").checked = true;
  await app.saveModelProfile();
  assert.ok(paths.includes("/api/models"));
  assert.match(app.element("#toast").textContent, /后续任务使用新模型/);
});

test("过期目录响应不能覆盖后选目录", async () => {
  const app = createHarness();
  const pending = new Map();
  app.context.fetch = (path) => new Promise((resolve) => pending.set(path, resolve));
  const older = app.browseDirectory("/old");
  const newer = app.browseDirectory("/new");
  pending.get("/api/directories?path=%2Fnew")({ ok: true, json: async () => ({ path: "/new", directories: [] }) });
  await newer;
  pending.get("/api/directories?path=%2Fold")({ ok: true, json: async () => ({ path: "/old", directories: [] }) });
  await older;
  assert.equal(app.state.browsePath, "/new");
});

test("旧 Session 的读取错误不覆盖新 Session", async () => {
  const app = createHarness();
  const pending = new Map();
  app.state.rpc = { request(method, params) {
    return { id: 1, promise: new Promise((resolve, reject) => pending.set(`${params.session_id}:${method}`, { resolve, reject })) };
  } };
  const older = app.inspectSession("old", false);
  const newer = app.inspectSession("new", false);
  pending.get("new:session.load_page").resolve({ messages: [], status: "idle", total_messages: 0 });
  pending.get("new:session.trace_page").resolve({ records: [], total_records: 0 });
  await newer;
  pending.get("old:session.load_page").reject(new Error("旧请求失败"));
  pending.get("old:session.trace_page").resolve({ records: [], total_records: 0 });
  await older;
  assert.equal(app.state.inspectedSessionId, "new");
  assert.doesNotMatch(app.element("#toast").textContent, /旧请求失败/);
});

test("长会话打开时读取末尾页", async () => {
  const app = createHarness();
  const offsets = [];
  const rpc = { request(method, params) {
    assert.equal(method, "session.load_page");
    offsets.push(params.offset);
    return { promise: Promise.resolve({ total_messages: 130, messages: [{ role: "assistant", content: String(params.offset) }] }) };
  } };
  const page = await app.loadLatestAgentSnapshot(rpc, "long");
  assert.deepEqual(offsets, [0, 70]);
  assert.equal(page.messages[0].content, "70");
});

test("恢复快照失败后释放输入锁", async () => {
  const app = createHarness();
  app.state.connected = true;
  app.state.agentSessionId = "one";
  app.state.activeRequest = "reconnecting";
  app.state.rpc = { request() { return { promise: Promise.reject(new Error("读取失败")) }; } };
  await app.recoverAgentTurn();
  assert.equal(app.state.activeRequest, null);
  assert.equal(app.element("#agent-status").textContent, "恢复失败");
});

test("已知 run 不匹配时不订阅其他任务", async () => {
  const app = createHarness();
  app.state.connected = true;
  app.state.agentSessionId = "one";
  app.state.activeRequest = "reconnecting";
  app.state.activeRunId = "run-one";
  const methods = [];
  app.state.rpc = { request(method) {
    methods.push(method);
    if (method === "session.load_page") return { promise: Promise.resolve({ messages: [], active_requests: [2] }) };
    if (method === "run.read") return { promise: Promise.resolve({ status: "failed", request_id: 1, error_message: "执行失败" }) };
    throw new Error(`不应订阅 ${method}`);
  } };
  await app.recoverAgentTurn();
  assert.equal(methods.includes("agent.subscribe"), false);
  assert.equal(app.element("#agent-status").textContent, "失败");
  assert.equal(app.state.activeRequest, null);
});

test("同一 Session 的较早检查响应不能覆盖新检查", async () => {
  const app = createHarness();
  const pending = [];
  app.state.rpc = { request(method) {
    const entry = { method };
    entry.promise = new Promise((resolve) => { entry.resolve = resolve; });
    pending.push(entry);
    return { promise: entry.promise };
  } };
  const old = app.inspectSession("same", false);
  const fresh = app.inspectSession("same", false);
  pending[2].resolve({ messages: [{ role: "assistant", content: "新响应" }], total_messages: 1 });
  pending[3].resolve({ records: [], total_records: 0 });
  await fresh;
  pending[0].resolve({ messages: [{ role: "assistant", content: "旧响应" }], total_messages: 1 });
  pending[1].resolve({ records: [], total_records: 0 });
  await old;
  assert.equal(app.state.inspectedSnapshot.messages[0].content, "新响应");
});

test("模型切换按用户点击顺序提交", async () => {
  const app = createHarness();
  const activated = [];
  app.context.fetch = async (path, options) => {
    if (path === "/api/models/activate") {
      const id = JSON.parse(options.body).profile_id;
      activated.push(id);
      return { ok: true, json: async () => ({ active_id: id, profile: { name: id } }) };
    }
    return { ok: true, json: async () => ({ profiles: [], active_id: activated.at(-1) }) };
  };
  await Promise.all([app.useModelProfile("first"), app.useModelProfile("second")]);
  assert.deepEqual(activated, ["first", "second"]);
  assert.equal(app.state.activeModelId, "second");
});

test("目录读取失败清空旧路径，手动输入路径先验证", async () => {
  const app = createHarness();
  app.state.connected = true;
  app.state.workspace = "/current";
  app.state.browsePath = "/old";
  app.context.fetch = async () => ({ ok: false, status: 404, json: async () => ({ error: { message: "无此目录" } }) });
  await assert.rejects(app.browseDirectory("/missing"), /无此目录/);
  assert.equal(app.state.browsePath, null);
  app.element("#browse-path").value = "/missing";
  await app.selectBrowsedWorkspace();
  assert.equal(app.state.workspace, "/current");
  assert.equal(app.state.browsePath, null);
});

test("切换工作区清除旧搜索和详情，耗时异常值显示占位", () => {
  const app = createHarness();
  app.state.collapsedDays.add("2026-09-25");
  app.state.traceFilter = "tool";
  app.state.inspectedSessionId = "old";
  app.element("#session-search").value = "旧任务";
  app.element("#session-title").textContent = "旧会话";
  app.resetWorkspaceState();
  assert.equal(app.element("#session-search").value, "");
  assert.equal(app.element("#session-title").textContent, "选择一个 Session");
  assert.equal(app.state.traceFilter, "all");
  assert.equal(app.state.collapsedDays.size, 0);
  assert.equal(app.formatDuration(NaN), "—");
  assert.equal(app.formatDuration(-1), "—");
});

test("恢复时显示所有待审批项", () => {
  const app = createHarness();
  app.state.agentSnapshot = { messages: [] };
  app.state.pendingApprovals = [
    { id: "a", prompt: "允许 A" },
    { id: "b", prompt: "允许 B" },
  ];
  app.renderAgentTranscript();
  const html = app.element("#agent-transcript").innerHTML;
  assert.match(html, /允许 A/);
  assert.match(html, /允许 B/);
  assert.equal((html.match(/class="approval-card"/g) || []).length, 2);
});

test("并发恢复只订阅一次同一请求", async () => {
  const app = createHarness();
  app.state.connected = true;
  app.state.agentSessionId = "one";
  app.state.activeRequest = "reconnecting";
  let resolvePage;
  let pageCalls = 0;
  let subscriptions = 0;
  app.state.rpc = { request(method) {
    if (method === "session.load_page") {
      pageCalls += 1;
      return { promise: pageCalls === 1
        ? new Promise((resolve) => { resolvePage = resolve; })
        : Promise.resolve({ messages: [], active_requests: [], session_id: "one" }) };
    }
    if (method === "agent.subscribe") { subscriptions += 1; return { id: 5, promise: Promise.resolve({ content: "完成" }) }; }
    if (method === "session.list") return { promise: Promise.resolve({ sessions: [] }) };
    throw new Error(method);
  } };
  const first = app.recoverAgentTurn();
  await app.recoverAgentTurn();
  resolvePage({ messages: [], active_requests: [7] });
  await first;
  assert.equal(subscriptions, 1);
});

test("快速新建任务只创建一个 Session", async () => {
  const app = createHarness();
  app.state.connected = true;
  let resolveNew;
  let creations = 0;
  app.state.rpc = { request(method) {
    if (method === "session.new") {
      creations += 1;
      return { promise: new Promise((resolve) => { resolveNew = resolve; }) };
    }
    if (method === "session.list") return { promise: Promise.resolve({ sessions: [] }) };
    throw new Error(method);
  } };
  const first = app.newAgentSession();
  await app.newAgentSession();
  assert.equal(creations, 1);
  resolveNew({ session_id: "new", messages: [] });
  await first;
});

test("子 Agent 列表读取共享 RPC 并转义结果内容", async () => {
  const app = createHarness();
  app.state.connected = true;
  app.element("#subagent-root").value = "run-root";
  app.state.rpc = { request(method, params) {
    assert.equal(method, "list_subagents");
    assert.equal(params.root_run_id, "run-root");
    return { promise: Promise.resolve({ children: [{
      child_run_id: "run-child", status: "completed", content: "<script>alert(1)</script>",
    }] }) };
  } };
  await app.refreshSubagents();
  const html = app.element("#subagent-list").innerHTML;
  assert.match(html, /run-child/);
  assert.match(html, /&lt;script&gt;/);
  assert.doesNotMatch(html, /<script>/);
});
