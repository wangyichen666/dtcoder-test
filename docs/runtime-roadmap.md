# 本地 Agent Runtime 路线图

更新日期：2026-09-26。本文件只记录已实现的行为与待办，不把下一阶段设计当作现有保证。

## 阶段与状态

| 阶段 | 状态 | 本轮范围 | 后续 |
|---|---|---|---|
| P0 基线与护栏 | 已完成 | 类型化 ID、Runtime 错误分类、模块依赖说明、CLI/ACP/Web 同一 run 黑盒契约测试 | 后续按需扩大故障矩阵 |
| P0 MSRV 与 CI | 本地门禁全绿 | 修复 TUI match guard；固定 Rust 1.88.0；Linux stable、Rust 1.88、macOS、Node 与供应链 CI；升级有公告的依赖 | 新工作流尚未在 GitHub Actions 实际运行 |
| P1 持久 Run | 已完成本阶段 | 工具回执、`run.audit`、显式 `run.reconcile`、崩溃窗口与旧库迁移测试 | JSONL 只是诊断；未知副作用不重放 |
| P2 控制面 | 已完成本阶段 | 持久队列、exact 取消、approval interaction、resync、受管请求任务与关闭收敛 | 不恢复已消失的 LLM future；question/plan 仅预留 payload；不实现 steer |
| P3 | Native 纵切完成 | 文件能力、原子写入、流式有界 exec、资源登记与关闭清理 | Docker backend、后台进程另行实施 |
| P4 Provider 韧性 | 已完成本阶段纵切 | 类型化错误、冻结路由、attempt/usage 持久化、阶段超时、安全重试 | 后续按风险项扩展真实服务兼容测试 |
| P5 异步子 Agent | 本地纵切通过 | v5 委派迁移、daemon 持有 child、工具/RPC/入口投影、取消树、结果租约、重启读回 | 真实远端 Provider 兼容性待验证；只开放只读 `read_file` |
| P7 及后续阶段 | 未开始 | 不提前改变上下文与工具治理行为 | 按 P7→P6→P8→P9→P10→P11 顺序实施 |

## P0 MSRV 与 CI（2026-09-26）

当前 crate 名称为 `my-agent`，与需求文本中的 `dtcoder-test` 不同；以本仓库的 Cargo 与测试为准。基线 HEAD 为 `c5fe686`，开始时工作区干净。核对发现 `src/entry/tui.rs` 使用 `if let` match guard；现已改为 Rust 1.88 可编译的普通 `None` 分支。Rust 1.88 严格 Clippy 另发现 CLI 和 HTTP 入口各一处旧代码告警，已作等价改写。仓库此前没有 `rust-toolchain.toml`、CI 工作流或 `cargo-deny` 策略。已加入固定工具链和 CI：Linux stable 执行 fmt、全目标测试、严格 Clippy、release 构建与提交差异空白检查；1.88 执行全目标 check；macOS 执行全目标 check/test；Node 执行 Web 测试与语法检查；供应链检查覆盖漏洞公告、许可证与来源。CI 不传入真实 Provider 密钥。

P0 只改工具链、代码语法与检查配置，没有新增业务状态、数据库 migration、RPC、事件或入口投影；现有 CLI、ACP、WebSocket 契约由全目标测试中的真实 daemon 进程测试继续覆盖。

本机实际通过：Rust 1.88.0 `cargo check --locked --all-targets`、全目标测试（199 个单元测试、2 个真实 daemon 进程契约测试）、严格 Clippy、release 构建和格式检查；Node 26 个测试与语法检查；`git diff --check`；完整 `cargo deny check`。首次拉取 RustSec 公告库时网络不通；网络恢复后发现 `lopdf 0.38` 的栈溢出公告、`rustls 0.23.44` 的 TLS 漏洞及 `ttf-parser` 不再维护。已将 `lopdf` 升至 0.45.0、`rustls` 升至 0.23.45；`ttf-parser` 从依赖图移除。更新后的 PDF 读取回归测试通过。新工作流尚未在 GitHub Actions 实际运行。

P0 验证后进入 P5。P0 的上述测试数量为进入 P5 前的快照，P5 当前验证见下节。

## P5 异步子 Agent（2026-09-26）

代码核对确认旧 `sub_agent` 是一次工具调用内的 ephemeral `LoopEngine`，结果随工具返回，daemon 无法按 child ID 恢复。当前生产注册已替换为 `DelegationTool`；旧 `sub_agent` 名称继续可用，但通过同一持久委派链路准入并等待，不再运行独立的临时状态机。新增 `spawn_subagent`、`wait_subagents`、`list_subagents`、`cancel_subagent` 模型工具。child 只开放 `read_file`，权限独立冻结为 `request_approval`；父会话之后切换至 FullAccess 不会扩张已准入 child 的读取范围。

v5 前进迁移在现有 WAL 数据库新增 `delegations`，保存 root/parent/child session 与 run ID、深度、冻结工具与权限、Provider route、cwd、round/token/tool-call/deadline 预算、终态、结果状态、owner/revision 和 30 秒租约信息。child 准入、队列、事件与委派行在同一事务提交；`spawn_key` 在父 run 内唯一，用于重试幂等。深度上限 2，每 root 总数上限 8、活动 child 上限 4。子 session 只允许已准入的 request ID 驱动；每 session 仍由现有队列保证单 writer。Provider route 从父 run 的持久快照复制；热切换不改变已准入 child。round、工具调用和 wall-clock 限制在执行链路检查；token 限制结合持久 Provider usage 与无 usage 时的估算，超额时不进入下一批工具。

共享 RPC 为 `spawn_subagent`、`list_subagents`、`read_subagent`、`wait_subagents`、`cancel_subagent` 和 `subagent.result.reserve/release/commit`。`wait_subagents` 默认等待任一指定 child 终态；传 `after_seq` 时也可等待子 run 事件游标前进，最多等待 60 秒。等待只读，不占用结果 reservation，因此等待断线不会吞掉结果。结果领取采用 owner/revision CAS；显式 release 或租约过期后可由其他 owner 重领。child 终态与结果消费分开。父/root 取消按持久关系遍历子树，不按 ID 前缀猜测。运行中 child 消失后标记 `unknown_after_restart` 并保留回执，不自动重放；queued child 复用原有恢复队列；terminal child 可在重启后读回。人工 reconcile unknown child 会更新委派终态，已领取结果不能改写。

child 原始流由 child session 的现有订阅读取；父 run 仍活动时，其流只发送委派准入和最终摘要事件，事件先写 SQLite 再广播。CLI、ACP、TUI 通过 `/subagents`、`/subagent`、`/subagent-wait`、`/subagent-cancel` 映射共享事实；WebSocket 暴露相同 RPC，Web 工作台有子 Agent 列表和读取、等待、取消入口。契约测试覆盖模型工具派生、并行 child、稳定 ID、等待超时、结果领取、越权拒绝、父取消隔离、并发上限、旧库迁移、重启 terminal 与 running unknown、CLI/ACP/WebSocket 同事实。

P5 保持本地优先的边界：尚无向活动 child 发送新消息或 terminal revival；只读能力限制是当前明确支持的最小集合。单次 Provider 响应可能超出预估 token，预算在响应落地后阻止后续工具/轮次；部分 Provider 不返回 usage 时使用估算。P7 的持久 compact/checkpoint/rewind 尚未开始。新 CI 工作流仍待 GitHub Actions 首次运行。

本机 P5 验证：`cargo fmt --all -- --check`、`cargo test --all-targets`（197 个单元测试、7 个真实 daemon 进程契约测试）、严格 Clippy、`cargo +1.88.0 check --locked --all-targets`、release 构建、Node 27 个测试、Node 语法检查和 `git diff --check` 均退出 0。`cargo deny check` 本轮在线刷新 RustSec 公告库时无法连接 GitHub（端口 443，75 秒超时）；使用已缓存公告库的 `cargo deny --offline check --hide-inclusion-graph` 退出 0，四项检查通过。此前 P0 依赖升级后曾成功完成在线全量检查；本轮无法确认公告库是否有更新。

## 已完成阶段的历史记录

本轮在原 `RunStore` 上添加 v2/v3 前进迁移。v2 让工具回执按 `(run_id, round, call_id)` 唯一，新增 effect、参数 SHA-256 摘要、起止时间、typed outcome、artifact 引用和 replay 标志；队列加唯一索引、session 状态索引，interaction 加 revision。完整工具输出经原子写入 `.my-agent/runtime.artifacts/`，SQLite 回执只保留有界预览和摘要。v3 为 interaction 增加类型化 payload。旧 v1 数据原地保留并回填 queued run；未来版本继续 fail closed。

## 事实所有权

- daemon 的 `RunStore`（`src/storage/`）拥有 run/turn 状态、队列位置、工具回执、事件顺序、interaction revision、取消和终态。SQLite 位于工作区 `.my-agent/runtime.sqlite3`，WAL 启用。schema migration 只前进，不删除旧数据。
- `SessionStore` 继续写 JSONL 对话与 trace，供旧客户端、审计和导出使用。旧 JSONL 不迁移、不删除。SQLite 不会把旧 JSONL 自动重解释为已确认 run。
- `ActiveRequest` 的 broadcast 只降低活动订阅延迟；`agent.subscribe` 先读 SQLite 事件，再接内存通知，并按 seq 去重。内存 `ApprovalBroker` 只唤醒活的执行体，SQLite interaction 才是控制事实。`run.read`、`run.events`、`run.tools` 可在 daemon 重启后读取。
- Provider、工具、上下文继续属于 `LoopEngine`；它在工具副作用前写回执，完成后再发送工具事件。安全决策属于 `SafetyPolicy` 与工具准入。TUI 的队列数来自 `queue.list`，入口只映射协议与展示，不存在第二个权威队列。

依赖方向：`entry → daemon/control → loop_engine → provider/tools/context`；`daemon → storage`；`tools → safety`。`storage` 只依赖协议中的兼容 `RequestId`，不依赖入口。未来稳定后再考虑拆 crate。

## 本轮状态与协议

- ID：`SessionId`、`RunId`、`TurnId`、`InteractionId` 为透明字符串新类型；`EventSeq` 为无符号序号。旧 `request_id` 仍为 JSON-RPC 数字或字符串，并在同一 session 内作为幂等键。
- `chat.send` 成功结果保留 `content`，新增 `run_id`、`turn_id`，并接受可选 `admission_mode=queue|reject_if_busy`。同 session、同 request id、同输入幂等；不同输入冲突。首次 queued 请求可保持流连接等待，断开不取消；重复 queued 请求立即返回同一 run ID。
- `EventFrame` 可选新增 `run_id`、`seq`，老客户端忽略即可。序号由 daemon 在 SQLite 事务中分配，首条 `user_message` 序号为 1。
- `run.read {run_id}`、`run.events {run_id, after_seq?, limit?}`、`run.tools {run_id}`、`run.audit {run_id}` 读取控制事实与诊断。`agent.subscribe` 可选 `after_seq`；带 `session_id` 时可读回已结束 run。单次回放上限为 1000 条；超限/广播缺口返回 `error.data={kind:"resync_required", snapshot, last_seq, cursor, next_method:"run.events"}`。
- `queue.list/read/remove` 按 session 与 run ID 读取或取消队列项。`agent.cancel` 优先 exact `session_id+run_id`；旧 request ID 在指定 session 内解析，未指定 session 时若跨 session 歧义则拒绝。取消 queued run 在 SQLite 事务中提交 terminal，不会命中后继 run。
- `interaction.list/read/respond/reject` 提供 owner、kind、status、revision、payload。respond/reject 验证 owner/revision 后先持久 claim，再唤醒执行体；相同答案幂等，冲突答案拒绝。旧 `approval.respond` 兼容映射；没有活执行体的 pending interaction 不会接受幽灵回答。
- 状态：`queued`、`running`、`waiting_interaction`、`completed`、`failed`、`cancelled`、`unknown_after_restart`。`terminal` 事件与最终状态同事务提交。成功时 `assistant_content` 与 `terminal` 同事务提交；客户端只在该事务成功后收到完成事件和响应。
- `RuntimeError` 已定义取消、超时、Provider、工具准入、工具执行、持久化、协议、交互等待、内部错误分类。daemon 不再用错误字符串识别取消；其它模块的错误边界将按阶段逐步迁移。

## 崩溃窗口

| 窗口 | 恢复行为 |
|---|---|
| 准入前 | 没有 run，调用者可重新提交 |
| 用户消息已入 SQLite，但尚未取得 writer permit | queued run 与消息保留；daemon 重启后按队列顺序启动 |
| 已取得 writer permit，Provider 前 | running run 重启标为 `unknown_after_restart`，不自动执行 |
| 工具 prepared/running 后，receipt 前 | 重启标为 `unknown_after_restart`，保留未完成回执，不自动重放工具 |
| 最终助手内容已写 JSONL，SQLite 终态前 | SQLite run 仍非终态；重启标为 unknown，JSONL 仅供人工排障 |
| SQLite 终态已提交，客户端未收到 | `run.read` 和重订阅返回已提交结果；相同幂等键不会再次执行 |

审批在活动 daemon 内持久记录为 interaction。若 daemon 在等待审批时重启，执行体已经消失；run 标为 unknown、interaction 标为 orphaned，不接受过期审批，不伪造继续执行。取消在工具副作用结果不明时也收敛为 unknown。JSONL/SQLite 分歧由 `run.audit` 报告，不从 JSONL 猜测成功；旧 JSONL 原样保留。

## 下一步顺序

P1/P2 的人工 repair、受管任务关闭和审批取消副作用测试已完成。P3 Native 纵切和 P4 Provider 韧性已落地。此段以下是 P4 的历史交付记录；当前阶段状态以文首 P5 记录为准。本轮不自动重放 unknown run，也不把 JSONL 当作终态权威。

## P4 本轮范围与验收

基线为 `a3243ed`，开始时工作区干净；Rust/Cargo 为 1.98.1。`cargo fmt --all -- --check`、`cargo test --all-targets`、严格 clippy、release build 和 `git diff --check` 可运行。代码核对确认热切换 `ProviderManager` 在每次模型请求时重新取当前实现，HTTP 适配器直接返回含响应正文的 anyhow 错误，usage 只进入 tracing，流等待缺少连接、首语义事件和 idle 的独立时限。

本轮仅实现 P4：在现有 Provider/LoopEngine/RunStore 上增加类型化错误、不可变 route snapshot、持久 attempt/usage、阶段超时和有限重试。v4 SQLite 前进迁移新增 `run_routes` 和 `provider_attempts`，`run.provider_attempts` 读回快照、尝试与聚合用量。每个 run 冻结候选实现和模型；运行时热切换只影响新 run。脚本化 HTTP mock 验证分类、脱敏和 usage；脚本化 Provider 验证 retry/fallback、阶段时钟、取消、上下文溢出、终态分类、重启 readback。P5 异步委派、P7 checkpoint、P6 调度、P9 隔离/存储和 P10 远程 MCP 本轮不实施。

## P3 本轮范围与边界

本轮建立 `Sandbox`/Native backend、显式 `ExecRequest`/`ExecResult`、文件 intent 与不可由调用者构造的 `AuthorizedPath`，并将内置 read/write/edit/exec 工具接入。Native 文件操作通过目录句柄相对打开、最终组件 no-follow、身份与内容版本复核、同目录临时文件和原子替换完成。命令输出以固定大小缓冲区流式采集，取消和超时清理进程组；登记前台资源供关闭时停止。安全策略明确保护运行时数据库、Git 元数据、系统目录与用户密钥目录，并拒绝硬链接写入。

Native 不是强隔离：同一用户的恶意进程仍可能修改目录树或绕开 Agent 工具，Shell 命令本身可直接访问宿主文件。Docker backend、跨平台完整实现和进程后台化留待后续纵切；接口会明确报告 requested/effective backend。任何不满足当前能力验证的文件操作都拒绝，不降级为普通路径重开。

## 验证

```bash
cargo fmt --all -- --check
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
cargo build --release
git diff --check
```

`tests/runtime_contract.rs` 使用本地 mock Ollama 和真实 daemon 进程，不需要 API key。测试检查终态重启读回、未知 run 不重放、排队请求断线 readback、exact queued cancel、旧幂等键、重启后 queued run 恢复；CLI `/run`、ACP `/run`、WebSocket `run.read` 核对同一 run。单元/契约测试覆盖 v1 原地迁移、未来版本拒绝、工具崩溃窗口、单写者、interaction owner/revision/幂等与订阅 resync 游标。

P3 基线为 181 个单元测试和 2 个进程契约测试。P4 交付时 198 个单元测试和 2 个进程契约测试通过，格式检查、严格 Clippy、release 构建及 `git diff --check` 均通过。P5 当前状态见文首。
