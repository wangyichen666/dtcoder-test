# 本地 Agent Runtime 路线图

更新日期：2026-09-24。本文件只记录已实现的行为与待办，不把下一阶段设计当作现有保证。

## 阶段与状态

| 阶段 | 状态 | 本轮范围 | 后续 |
|---|---|---|---|
| P0 基线与护栏 | 已完成 | 类型化 ID、Runtime 错误分类、模块依赖说明、CLI/ACP/Web 同一 run 黑盒契约测试 | 后续按需扩大故障矩阵 |
| P1 持久 Run | 本轮纵切完成 | 工具批次 prepared/running/terminal 回执、`run.tools`、`run.audit`、崩溃窗口与旧库迁移测试 | JSONL 检查只是诊断，未提供自动 repair；未知副作用不重放 |
| P2 控制面 | 本轮纵切完成 | daemon 持久队列与单写者、exact run 取消、approval interaction 读写、结构化 resync、TUI 队列投影 | 不恢复已消失的 LLM future；question/plan 仅预留 payload；不实现 steer |
| P3–P8 | 未开始 | 保持现有安全、Provider、子 Agent 和工具行为 | 按需求文档顺序实施 |

本轮在原 `RunStore` 上添加 v2/v3 前进迁移。v2 让工具回执按 `(run_id, round, call_id)` 唯一，新增 effect、参数 SHA-256 摘要、起止时间、typed outcome、artifact 引用和 replay 标志；队列加唯一索引、session 状态索引，interaction 加 revision。完整工具输出经原子写入 `.my-agent/tool-artifacts/`，SQLite 回执只保留有界预览和摘要。v3 为 interaction 增加类型化 payload。旧 v1 数据原地保留并回填 queued run；未来版本继续 fail closed。

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

1. 进入 P3 前需把 `run.audit` 的诊断扩为可操作的人工 repair 流程，并补更细的 JSONL/tool-result 配对故障注入；当前不会自动改写历史。
2. 进入 P3 前还需为后台资源建立可 join/abort 的 owner 管理，并补审批响应与取消在真实工具副作用边界的并发故障测试。现有 daemon shutdown 停止准入、取消活动执行体并等待收敛；queued run 保留供重启调度。
3. P3 以后再实现强文件身份校验、可插拔 sandbox、Provider 韧性和异步子 Agent。当前不得宣传这些保证。

## 验证

```bash
cargo fmt --all -- --check
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
cargo build --release
git diff --check
```

`tests/runtime_contract.rs` 使用本地 mock Ollama 和真实 daemon 进程，不需要 API key。测试检查终态重启读回、未知 run 不重放、排队请求断线 readback、exact queued cancel、旧幂等键、重启后 queued run 恢复；CLI `/run`、ACP `/run`、WebSocket `run.read` 核对同一 run。单元/契约测试覆盖 v1 原地迁移、未来版本拒绝、工具崩溃窗口、单写者、interaction owner/revision/幂等与订阅 resync 游标。

本轮验证：165 个单元测试、2 个真实进程黑盒测试通过；`cargo fmt --all -- --check`、`cargo clippy --all-targets --all-features -- -D warnings`、`cargo build --release`、`git diff --check` 均通过。P1/P2 的本轮最小纵切完成；上文列出的 repair、后台资源 owner 与更细故障注入仍是进入 P3 前的门槛。
