# 本地 Agent Runtime 路线图

更新日期：2026-09-24。本文件只记录已实现的行为与待办，不把下一阶段设计当作现有保证。

## 阶段与状态

| 阶段 | 状态 | 本轮范围 | 后续 |
|---|---|---|---|
| P0 基线与护栏 | 已完成 | 类型化 ID、Runtime 错误分类、模块依赖说明、CLI/ACP/Web 同一 run 黑盒契约测试 | 后续按需扩大故障矩阵 |
| P1 持久 Run | 最小纵切已实现 | WAL/migration、session/run/turn/event/interaction 表、准入前落盘、单调事件序号、幂等终态、重启读回、事件游标 | 工具 receipt 与 JSONL/SQLite 完整事务桥接、交互执行体恢复、更多故障注入 |
| P2 控制面 | 设计中 | 本轮不迁移 TUI 队列 | daemon 单写者队列、exact run 取消、interaction 统一读写、snapshot/resync、受管 shutdown |
| P3–P8 | 未开始 | 保持现有安全、Provider、子 Agent 和工具行为 | 按需求文档顺序实施 |

## 事实所有权

- daemon 的 `RunStore`（`src/storage/`）拥有 run/turn 状态、事件顺序、终态和交互记录。SQLite 位于工作区 `.my-agent/runtime.sqlite3`，WAL 启用。schema migration 只前进，不删除旧数据。
- `SessionStore` 继续写 JSONL 对话与 trace，供旧客户端、审计和导出使用。旧 JSONL 不迁移、不删除。SQLite 不会把旧 JSONL 自动重解释为已确认 run。
- `ActiveRequest` 的 broadcast 只降低活动订阅延迟；`agent.subscribe` 先读 SQLite 事件，再接内存通知，并按 seq 去重。`run.read` 与 `run.events` 可在 daemon 重启后读取。
- Provider、工具、上下文继续属于 `LoopEngine`；安全决策属于 `SafetyPolicy` 与工具准入。入口只映射协议与展示。

依赖方向：`entry → daemon/control → loop_engine → provider/tools/context`；`daemon → storage`；`tools → safety`。`storage` 只依赖协议中的兼容 `RequestId`，不依赖入口。未来稳定后再考虑拆 crate。

## 本轮状态与协议

- ID：`SessionId`、`RunId`、`TurnId`、`InteractionId` 为透明字符串新类型；`EventSeq` 为无符号序号。旧 `request_id` 仍为 JSON-RPC 数字或字符串，并在同一 session 内作为幂等键。
- `chat.send` 成功结果保留 `content`，新增 `run_id`、`turn_id`。同 session、同 request id、同输入的已完成请求再次提交会返回同一结果；不同输入冲突。
- `EventFrame` 可选新增 `run_id`、`seq`，老客户端忽略即可。序号由 daemon 在 SQLite 事务中分配，首条 `user_message` 序号为 1。
- 新增 `run.read {run_id}`、`run.events {run_id, after_seq?, limit?}`。`agent.subscribe` 可选 `after_seq`；带 `session_id` 时还可读回已结束 run。单次回放上限为 1000 条；超限返回明确错误，客户端应分页调用 `run.events`。
- 状态：`queued`、`running`、`waiting_interaction`、`completed`、`failed`、`cancelled`、`unknown_after_restart`。`terminal` 事件与最终状态同事务提交。成功时 `assistant_content` 与 `terminal` 同事务提交；客户端只在该事务成功后收到完成事件和响应。
- `RuntimeError` 已定义取消、超时、Provider、工具准入、工具执行、持久化、协议、交互等待、内部错误分类。daemon 不再用错误字符串识别取消；其它模块的错误边界将按阶段逐步迁移。

## 崩溃窗口

| 窗口 | 恢复行为 |
|---|---|
| 准入前 | 没有 run，调用者可重新提交 |
| 用户消息已入 SQLite，Provider 前 | run 存在；重启标为 `unknown_after_restart`，不自动执行 |
| 工具开始前或副作用后、receipt 前 | 重启标为 `unknown_after_restart`，不自动重放工具 |
| 最终助手内容已写 JSONL，SQLite 终态前 | SQLite run 仍非终态；重启标为 unknown，JSONL 仅供人工排障 |
| SQLite 终态已提交，客户端未收到 | `run.read` 和重订阅返回已提交结果；相同幂等键不会再次执行 |

审批在活动 daemon 内会持久记录为 interaction。若 daemon 在等待审批时重启，执行体已经消失；当前最小版本保留问题记录但将 run 标为 unknown、interaction 标为 orphaned，不接受过期审批，不伪造继续执行。P2 才实现可恢复的统一交互控制面。

## 下一步顺序

1. P1 补工具执行 receipt、崩溃注入点、JSONL 与 SQLite 一致性校验/修复；明确何时可判定已完成。
2. P2 将客户端本地队列移入 daemon；admission `queue`/`reject_if_busy`；取消绑定 `session_id + run_id`；interaction list/readback/respond/reject；断线缺口 snapshot/resync；shutdown 收敛。
3. 完成上述验证后才进入 P3。

## 验证

```bash
cargo fmt --all -- --check
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
cargo build --release
git diff --check
```

`tests/runtime_contract.rs` 使用本地 mock Ollama 和真实 daemon 进程，不需要 API key。测试检查已提交终态重启读回、未知 run 不重放（同时核对 mock Provider 请求次数）、事件游标与重复请求，并通过 CLI `/run`、ACP `/run`、WebSocket `run.read` 核对同一 run 的 ID、状态和内容。`src/storage` 的测试覆盖迁移、序号、终态幂等和冲突。

本轮结果：158 个单元测试与 1 个黑盒集成测试通过；格式检查、全目标 Clippy（`-D warnings`）、release 构建及 `git diff --check` 通过。
