//! SQLite control facts. JSONL remains a readable transcript, not the run authority.
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::daemon::protocol::RequestId;
use crate::provider::ToolCall;

macro_rules! string_id {
    ($name:ident) => {
        #[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub String);
    };
}
string_id!(SessionId);
string_id!(RunId);
string_id!(TurnId);
string_id!(InteractionId);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EventSeq(pub u64);

#[derive(Debug, Error)]
#[allow(dead_code)] // P0 defines the shared taxonomy; later stages will produce every variant.
pub enum RuntimeError {
    #[error("请求已取消")]
    Cancelled,
    #[error("请求超时")]
    Timeout,
    #[error("Provider 错误: {0}")]
    Provider(String),
    #[error("工具准入错误: {0}")]
    ToolAdmission(String),
    #[error("工具执行错误: {0}")]
    ToolExecution(String),
    #[error("持久化错误: {0}")]
    Persistence(#[from] rusqlite::Error),
    #[error("协议错误: {0}")]
    Protocol(String),
    #[error("等待交互: {0}")]
    InteractionWaiting(String),
    #[error("内部错误: {0}")]
    Internal(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Queued,
    Running,
    WaitingInteraction,
    Completed,
    Failed,
    Cancelled,
    UnknownAfterRestart,
}
impl RunStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::WaitingInteraction => "waiting_interaction",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::UnknownAfterRestart => "unknown_after_restart",
        }
    }
    fn parse(value: &str) -> Result<Self, RuntimeError> {
        match value {
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "waiting_interaction" => Ok(Self::WaitingInteraction),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            "unknown_after_restart" => Ok(Self::UnknownAfterRestart),
            _ => Err(RuntimeError::Protocol(format!("未知 run 状态: {value}"))),
        }
    }
    fn terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::UnknownAfterRestart
        )
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct RunRecord {
    pub run_id: RunId,
    pub turn_id: TurnId,
    pub session_id: SessionId,
    pub request_id: RequestId,
    pub status: RunStatus,
    pub last_seq: EventSeq,
    pub content: Option<String>,
    pub error_code: Option<i64>,
    pub error_message: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct StoredEvent {
    pub run_id: RunId,
    pub seq: EventSeq,
    pub event: String,
    pub data: Value,
}

#[derive(Clone, Debug, Serialize)]
pub struct ToolReceipt {
    pub run_id: RunId,
    pub round: i64,
    pub call_id: String,
    pub name: String,
    pub status: String,
    pub effect: String,
    pub argument_digest: String,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
    pub outcome: Option<String>,
    pub artifact_ref: Option<String>,
    pub safe_to_replay: bool,
    pub receipt: Option<Value>,
}

#[derive(Clone, Debug, Serialize)]
pub struct QueuedMessage {
    pub id: i64,
    pub position: i64,
    pub session_id: SessionId,
    pub run_id: RunId,
    pub message: String,
    pub status: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct InteractionRecord {
    pub interaction_id: InteractionId,
    pub session_id: SessionId,
    pub owner_run_id: RunId,
    pub kind: String,
    pub status: String,
    pub revision: i64,
    pub prompt: String,
    pub payload: Value,
    pub response: Option<Value>,
}

pub enum Admission {
    New(RunRecord),
    Existing(RunRecord),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionMode {
    Queue,
    RejectIfBusy,
}

pub struct RunStore {
    connection: Mutex<Connection>,
    artifact_dir: PathBuf,
}
static NEXT_ARTIFACT: AtomicU64 = AtomicU64::new(0);
impl RunStore {
    pub fn open(path: &Path) -> Result<Self, RuntimeError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| RuntimeError::Internal(error.to_string()))?;
        }
        let connection = Connection::open(path)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        connection.execute_batch("BEGIN IMMEDIATE;
            CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL);
            COMMIT;")?;
        let version: i64 = connection.query_row(
            "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
            [],
            |row| row.get(0),
        )?;
        if version > 3 {
            return Err(RuntimeError::Protocol(format!(
                "SQLite schema 版本 {version} 比当前程序支持的版本新"
            )));
        }
        if version < 1 {
            connection.execute_batch("BEGIN IMMEDIATE;
                CREATE TABLE sessions(id TEXT PRIMARY KEY, created_at_ms INTEGER NOT NULL);
                CREATE TABLE runs(
                    id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES sessions(id),
                    request_id_json TEXT NOT NULL, status TEXT NOT NULL,
                    input TEXT NOT NULL, last_seq INTEGER NOT NULL DEFAULT 0,
                    content TEXT, error_code INTEGER, error_message TEXT,
                    created_at_ms INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL,
                    UNIQUE(session_id, request_id_json));
                CREATE TABLE turns(id TEXT PRIMARY KEY, run_id TEXT NOT NULL UNIQUE REFERENCES runs(id), status TEXT NOT NULL);
                CREATE TABLE events(run_id TEXT NOT NULL REFERENCES runs(id), seq INTEGER NOT NULL,
                    event TEXT NOT NULL, data_json TEXT NOT NULL, created_at_ms INTEGER NOT NULL,
                    PRIMARY KEY(run_id, seq));
                CREATE TABLE interactions(id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id),
                    kind TEXT NOT NULL, status TEXT NOT NULL, prompt TEXT NOT NULL, response_json TEXT);
                CREATE TABLE queued_messages(id INTEGER PRIMARY KEY, session_id TEXT NOT NULL REFERENCES sessions(id),
                    run_id TEXT REFERENCES runs(id), message TEXT NOT NULL, status TEXT NOT NULL);
                CREATE TABLE tool_executions(id INTEGER PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id),
                    call_id TEXT NOT NULL, status TEXT NOT NULL, receipt_json TEXT,
                    UNIQUE(run_id, call_id));
                CREATE INDEX events_run_seq ON events(run_id, seq);
                INSERT INTO schema_migrations(version, applied_at_ms) VALUES (1, CAST(strftime('%s','now') AS INTEGER) * 1000);
                COMMIT;")?;
        }
        if version < 2 {
            connection.execute_batch("BEGIN IMMEDIATE;
                ALTER TABLE tool_executions RENAME TO tool_executions_v1;
                CREATE TABLE tool_executions(
                    id INTEGER PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id),
                    round INTEGER NOT NULL, call_id TEXT NOT NULL, name TEXT NOT NULL,
                    status TEXT NOT NULL, effect TEXT NOT NULL, argument_digest TEXT NOT NULL,
                    prepared_at_ms INTEGER NOT NULL, started_at_ms INTEGER, finished_at_ms INTEGER,
                    outcome TEXT, artifact_ref TEXT, safe_to_replay INTEGER NOT NULL DEFAULT 0,
                    receipt_json TEXT, UNIQUE(run_id, round, call_id));
                INSERT INTO tool_executions(run_id, round, call_id, name, status, effect,
                    argument_digest, prepared_at_ms, receipt_json)
                    SELECT run_id, 0, call_id, 'unknown', status, 'unknown', '',
                    CAST(strftime('%s','now') AS INTEGER) * 1000, receipt_json
                    FROM tool_executions_v1;
                DROP TABLE tool_executions_v1;
                CREATE INDEX tool_executions_run ON tool_executions(run_id, round, id);
                DELETE FROM queued_messages WHERE run_id IS NOT NULL AND id NOT IN
                    (SELECT MIN(id) FROM queued_messages WHERE run_id IS NOT NULL GROUP BY run_id);
                CREATE UNIQUE INDEX queued_messages_run ON queued_messages(run_id);
                CREATE INDEX queued_messages_session_status ON queued_messages(session_id, status, id);
                INSERT OR IGNORE INTO queued_messages(session_id, run_id, message, status)
                    SELECT session_id, id, input, 'queued' FROM runs WHERE status='queued';
                ALTER TABLE interactions ADD COLUMN revision INTEGER NOT NULL DEFAULT 0;
                INSERT INTO schema_migrations(version, applied_at_ms)
                    VALUES (2, CAST(strftime('%s','now') AS INTEGER) * 1000);
                COMMIT;")?;
        }
        if version < 3 {
            connection.execute_batch(
                "BEGIN IMMEDIATE;
                ALTER TABLE interactions ADD COLUMN payload_json TEXT NOT NULL DEFAULT '{}';
                INSERT INTO schema_migrations(version, applied_at_ms)
                    VALUES (3, CAST(strftime('%s','now') AS INTEGER) * 1000);
                COMMIT;",
            )?;
        }
        Ok(Self {
            connection: Mutex::new(connection),
            artifact_dir: path.with_extension("artifacts"),
        })
    }

    #[cfg(test)]
    pub fn admit(
        &self,
        session_id: SessionId,
        request_id: RequestId,
        input: &str,
    ) -> Result<Admission, RuntimeError> {
        self.admit_with_mode(session_id, request_id, input, AdmissionMode::Queue)
    }

    pub fn admit_with_mode(
        &self,
        session_id: SessionId,
        request_id: RequestId,
        input: &str,
        mode: AdmissionMode,
    ) -> Result<Admission, RuntimeError> {
        let mut connection = self.connection.lock().expect("SQLite mutex poisoned");
        let transaction = connection.transaction()?;
        let request_json = serde_json::to_string(&request_id)
            .map_err(|error| RuntimeError::Protocol(error.to_string()))?;
        if let Some(id) = transaction
            .query_row(
                "SELECT id FROM runs WHERE session_id=?1 AND request_id_json=?2",
                params![session_id.0, request_json],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        {
            let stored_input: String = transaction.query_row(
                "SELECT input FROM runs WHERE id=?1",
                params![id],
                |row| row.get(0),
            )?;
            if stored_input != input {
                return Err(RuntimeError::Protocol(
                    "相同 request id 的输入内容不同".into(),
                ));
            }
            let run = read_run_in(&transaction, &id)?.expect("existing run");
            transaction.commit()?;
            return Ok(Admission::Existing(run));
        }
        if mode == AdmissionMode::RejectIfBusy {
            let busy: i64 = transaction.query_row("SELECT count(*) FROM runs WHERE session_id=?1 AND status IN ('queued','running','waiting_interaction')", params![session_id.0], |row| row.get(0))?;
            if busy != 0 {
                return Err(RuntimeError::Protocol("session 忙碌".into()));
            }
        }
        let now = now_ms();
        transaction.execute(
            "INSERT OR IGNORE INTO sessions(id, created_at_ms) VALUES (?1, ?2)",
            params![session_id.0, now],
        )?;
        // The rowid is allocated by SQLite; the ID is stable across daemon restarts.
        transaction.execute("INSERT INTO runs(id, session_id, request_id_json, status, input, created_at_ms, updated_at_ms)
            VALUES ('pending', ?1, ?2, 'queued', ?3, ?4, ?4)", params![session_id.0, request_json, input, now])?;
        let rowid = transaction.last_insert_rowid();
        let run_id = RunId(format!("run-{rowid}"));
        let turn_id = TurnId(format!("turn-{rowid}"));
        transaction.execute(
            "UPDATE runs SET id=?1 WHERE rowid=?2",
            params![run_id.0, rowid],
        )?;
        transaction.execute(
            "INSERT INTO turns(id, run_id, status) VALUES (?1, ?2, 'queued')",
            params![turn_id.0, run_id.0],
        )?;
        insert_event(
            &transaction,
            &run_id,
            "user_message",
            &serde_json::json!({"content": input}),
        )?;
        transaction.execute("INSERT INTO queued_messages(session_id, run_id, message, status) VALUES (?1, ?2, ?3, 'queued')", params![session_id.0, run_id.0, input])?;
        let run = read_run_in(&transaction, &run_id.0)?.expect("inserted run");
        transaction.commit()?;
        Ok(Admission::New(run))
    }

    pub fn try_start_queued(&self, run_id: &RunId) -> Result<bool, RuntimeError> {
        let mut connection = self.connection.lock().expect("SQLite mutex poisoned");
        let transaction = connection.transaction()?;
        let candidate: Option<String> = transaction
            .query_row(
                "SELECT q.run_id FROM queued_messages q
            WHERE q.status='queued' AND q.session_id=(SELECT session_id FROM runs WHERE id=?1)
            ORDER BY q.id LIMIT 1",
                params![run_id.0],
                |row| row.get(0),
            )
            .optional()?;
        if candidate.as_deref() != Some(&run_id.0) {
            transaction.commit()?;
            return Ok(false);
        }
        let busy: i64 = transaction.query_row("SELECT count(*) FROM runs WHERE session_id=(SELECT session_id FROM runs WHERE id=?1) AND status IN ('running','waiting_interaction')", params![run_id.0], |row| row.get(0))?;
        if busy != 0 {
            transaction.commit()?;
            return Ok(false);
        }
        let changed = transaction.execute(
            "UPDATE runs SET status='running', updated_at_ms=?2 WHERE id=?1 AND status='queued'",
            params![run_id.0, now_ms()],
        )?;
        if changed == 1 {
            transaction.execute(
                "UPDATE turns SET status='running' WHERE run_id=?1",
                params![run_id.0],
            )?;
            transaction.execute(
                "UPDATE queued_messages SET status='running' WHERE run_id=?1",
                params![run_id.0],
            )?;
        }
        transaction.commit()?;
        Ok(changed == 1)
    }

    pub fn queued_messages(&self, session_id: &str) -> Result<Vec<QueuedMessage>, RuntimeError> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        let mut statement = connection.prepare("SELECT id, run_id, message, status FROM queued_messages WHERE session_id=?1 AND status='queued' ORDER BY id")?;
        let rows = statement.query_map(params![session_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        rows.enumerate()
            .map(|(index, row)| {
                let (id, run_id, message, status) = row?;
                Ok(QueuedMessage {
                    id,
                    position: index as i64 + 1,
                    session_id: SessionId(session_id.to_owned()),
                    run_id: RunId(run_id),
                    message,
                    status,
                })
            })
            .collect()
    }

    pub fn queued_message(
        &self,
        session_id: &str,
        run_id: &RunId,
    ) -> Result<Option<QueuedMessage>, RuntimeError> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        connection.query_row("SELECT id, message, status FROM queued_messages WHERE session_id=?1 AND run_id=?2",
            params![session_id, run_id.0], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?)))
            .optional()?.map(|(id, message, status)| {
                let position = if status == "queued" {
                    connection.query_row("SELECT count(*) FROM queued_messages WHERE session_id=?1 AND status='queued' AND id<=?2", params![session_id, id], |row| row.get(0))?
                } else { 0 };
                Ok(QueuedMessage { id, position, session_id: SessionId(session_id.into()), run_id: run_id.clone(), message, status })
            }).transpose()
    }

    pub fn recoverable_queued(&self) -> Result<Vec<RunRecord>, RuntimeError> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        let mut statement = connection
            .prepare("SELECT id FROM runs WHERE status='queued' ORDER BY created_at_ms, rowid")?;
        let ids = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        ids.iter()
            .map(|id| {
                read_run_in(&connection, id)?
                    .ok_or_else(|| RuntimeError::Protocol("queued run 消失".into()))
            })
            .collect()
    }

    pub fn session_exists(&self, session_id: &str) -> Result<bool, RuntimeError> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        Ok(connection
            .query_row(
                "SELECT 1 FROM sessions WHERE id=?1",
                params![session_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .is_some())
    }

    pub fn remove_queued(&self, run_id: &RunId) -> Result<bool, RuntimeError> {
        let mut connection = self.connection.lock().expect("SQLite mutex poisoned");
        let transaction = connection.transaction()?;
        let changed = transaction.execute(
            "UPDATE runs SET status='cancelled', updated_at_ms=?2 WHERE id=?1 AND status='queued'",
            params![run_id.0, now_ms()],
        )?;
        if changed == 1 {
            transaction.execute(
                "UPDATE turns SET status='cancelled' WHERE run_id=?1",
                params![run_id.0],
            )?;
            transaction.execute(
                "UPDATE queued_messages SET status='cancelled' WHERE run_id=?1 AND status='queued'",
                params![run_id.0],
            )?;
            insert_event(
                &transaction,
                run_id,
                "terminal",
                &serde_json::json!({"status": RunStatus::Cancelled}),
            )?;
        }
        transaction.commit()?;
        Ok(changed == 1)
    }

    pub fn prepare_tool_batch(
        &self,
        run_id: &RunId,
        round: usize,
        calls: &[(ToolCall, String, String, bool)],
    ) -> Result<(), RuntimeError> {
        let mut connection = self.connection.lock().expect("SQLite mutex poisoned");
        let transaction = connection.transaction()?;
        let status: String = transaction.query_row(
            "SELECT status FROM runs WHERE id=?1",
            params![run_id.0],
            |row| row.get(0),
        )?;
        if status != "running" {
            return Err(RuntimeError::Protocol("run 不在执行状态".into()));
        }
        for (call, effect, digest, replay) in calls {
            transaction.execute("INSERT INTO tool_executions(run_id, round, call_id, name, status, effect, argument_digest, prepared_at_ms, safe_to_replay)
                VALUES (?1, ?2, ?3, ?4, 'prepared', ?5, ?6, ?7, ?8)",
                params![run_id.0, round as i64, call.id, call.name, effect, digest, now_ms(), replay])?;
        }
        insert_event(
            &transaction,
            run_id,
            "tool_batch_prepared",
            &serde_json::json!({"round": round, "calls": calls.iter().map(|(call, _, _, _)| &call.id).collect::<Vec<_>>()}),
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn start_tool(
        &self,
        run_id: &RunId,
        round: usize,
        call_id: &str,
    ) -> Result<(), RuntimeError> {
        let changed = self.connection.lock().expect("SQLite mutex poisoned").execute(
            "UPDATE tool_executions SET status='running', started_at_ms=?4 WHERE run_id=?1 AND round=?2 AND call_id=?3 AND status='prepared'",
            params![run_id.0, round as i64, call_id, now_ms()])?;
        if changed != 1 {
            return Err(RuntimeError::Protocol("工具不是 prepared 状态".into()));
        }
        Ok(())
    }

    pub fn finish_tool(
        &self,
        run_id: &RunId,
        round: usize,
        call_id: &str,
        outcome: &str,
        artifact_ref: Option<&str>,
        receipt: &Value,
    ) -> Result<(), RuntimeError> {
        let changed = self.connection.lock().expect("SQLite mutex poisoned").execute(
            "UPDATE tool_executions SET status='terminal', finished_at_ms=?4, outcome=?5, artifact_ref=?6, receipt_json=?7
                WHERE run_id=?1 AND round=?2 AND call_id=?3 AND status='running'",
            params![run_id.0, round as i64, call_id, now_ms(), outcome, artifact_ref, receipt.to_string()])?;
        if changed != 1 {
            return Err(RuntimeError::Protocol("工具不是 running 状态".into()));
        }
        Ok(())
    }

    pub fn store_tool_output(&self, content: &str) -> Result<String, RuntimeError> {
        std::fs::create_dir_all(&self.artifact_dir)
            .map_err(|error| RuntimeError::Internal(error.to_string()))?;
        let digest = format!("{:x}", Sha256::digest(content.as_bytes()));
        let target = self.artifact_dir.join(format!("sha256-{digest}.txt"));
        if target.exists() {
            return Ok(target.to_string_lossy().into_owned());
        }
        let temporary = self.artifact_dir.join(format!(
            ".{digest}-{}-{}.tmp",
            std::process::id(),
            NEXT_ARTIFACT.fetch_add(1, Ordering::Relaxed)
        ));
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let result = (|| -> std::io::Result<()> {
            let mut file = options.open(&temporary)?;
            file.write_all(content.as_bytes())?;
            file.sync_all()?;
            std::fs::rename(&temporary, &target)?;
            std::fs::File::open(&self.artifact_dir)?.sync_all()?;
            Ok(())
        })();
        if let Err(error) = result {
            let _ = std::fs::remove_file(&temporary);
            return Err(RuntimeError::Internal(error.to_string()));
        }
        Ok(target.to_string_lossy().into_owned())
    }

    pub fn finish_tool_batch(&self, run_id: &RunId, round: usize) -> Result<(), RuntimeError> {
        let mut connection = self.connection.lock().expect("SQLite mutex poisoned");
        let transaction = connection.transaction()?;
        let incomplete: i64 = transaction.query_row("SELECT count(*) FROM tool_executions WHERE run_id=?1 AND round=?2 AND status!='terminal'", params![run_id.0, round as i64], |row| row.get(0))?;
        if incomplete != 0 {
            return Err(RuntimeError::Protocol("工具批次尚未全部完成".into()));
        }
        insert_event(
            &transaction,
            run_id,
            "tool_batch_completed",
            &serde_json::json!({"round": round}),
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn tool_receipts(&self, run_id: &RunId) -> Result<Vec<ToolReceipt>, RuntimeError> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        let mut statement = connection.prepare("SELECT round, call_id, name, status, effect, argument_digest, started_at_ms, finished_at_ms, outcome, artifact_ref, safe_to_replay, receipt_json FROM tool_executions WHERE run_id=?1 ORDER BY round, id")?;
        let rows = statement.query_map(params![run_id.0], |row| {
            Ok((
                ToolReceipt {
                    run_id: run_id.clone(),
                    round: row.get(0)?,
                    call_id: row.get(1)?,
                    name: row.get(2)?,
                    status: row.get(3)?,
                    effect: row.get(4)?,
                    argument_digest: row.get(5)?,
                    started_at_ms: row.get(6)?,
                    finished_at_ms: row.get(7)?,
                    outcome: row.get(8)?,
                    artifact_ref: row.get(9)?,
                    safe_to_replay: row.get(10)?,
                    receipt: None,
                },
                row.get::<_, Option<String>>(11)?,
            ))
        })?;
        rows.map(|row| {
            let (mut receipt, json) = row?;
            receipt.receipt = json
                .map(|json| {
                    serde_json::from_str(&json)
                        .map_err(|error| RuntimeError::Protocol(error.to_string()))
                })
                .transpose()?;
            Ok(receipt)
        })
        .collect()
    }

    #[cfg(test)]
    pub fn mark_running(&self, run_id: &RunId) -> Result<(), RuntimeError> {
        let mut connection = self.connection.lock().expect("SQLite mutex poisoned");
        let transaction = connection.transaction()?;
        transaction.execute(
            "UPDATE runs SET status='running', updated_at_ms=?2 WHERE id=?1 AND status='queued'",
            params![run_id.0, now_ms()],
        )?;
        transaction.execute(
            "UPDATE turns SET status='running' WHERE run_id=?1 AND status='queued'",
            params![run_id.0],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn append_event(
        &self,
        run_id: &RunId,
        event: &str,
        data: &Value,
    ) -> Result<EventSeq, RuntimeError> {
        let mut connection = self.connection.lock().expect("SQLite mutex poisoned");
        let transaction = connection.transaction()?;
        let seq = insert_event(&transaction, run_id, event, data)?;
        if event == "approval_required"
            && let Some(approval) = data.get("approval")
        {
            let id = approval["id"]
                .as_str()
                .ok_or_else(|| RuntimeError::Protocol("approval id 缺失".into()))?;
            let prompt = approval["prompt"].as_str().unwrap_or_default();
            transaction.execute("INSERT INTO interactions(id, run_id, kind, status, prompt, payload_json) VALUES (?1, ?2, 'approval', 'pending', ?3, ?4)", params![id, run_id.0, prompt, approval.to_string()])?;
            transaction.execute(
                "UPDATE runs SET status='waiting_interaction' WHERE id=?1",
                params![run_id.0],
            )?;
        }
        transaction.commit()?;
        Ok(seq)
    }

    pub fn read_interaction(
        &self,
        id: &InteractionId,
    ) -> Result<Option<InteractionRecord>, RuntimeError> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        read_interaction_in(&connection, &id.0)
    }

    pub fn list_interactions(
        &self,
        session_id: &str,
    ) -> Result<Vec<InteractionRecord>, RuntimeError> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        let mut statement = connection.prepare("SELECT i.id FROM interactions i JOIN runs r ON r.id=i.run_id WHERE r.session_id=?1 ORDER BY i.rowid")?;
        let ids = statement
            .query_map(params![session_id], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        ids.iter()
            .map(|id| {
                read_interaction_in(&connection, id)?
                    .ok_or_else(|| RuntimeError::Protocol("interaction 消失".into()))
            })
            .collect()
    }

    pub fn claim_interaction(
        &self,
        id: &InteractionId,
        session_id: &SessionId,
        run_id: &RunId,
        revision: i64,
        approved: bool,
    ) -> Result<InteractionRecord, RuntimeError> {
        let mut connection = self.connection.lock().expect("SQLite mutex poisoned");
        let transaction = connection.transaction()?;
        let current = read_interaction_in(&transaction, &id.0)?
            .ok_or_else(|| RuntimeError::Protocol("interaction 不存在".into()))?;
        if current.session_id != *session_id || current.owner_run_id != *run_id {
            return Err(RuntimeError::Protocol("interaction owner 不匹配".into()));
        }
        let response = serde_json::json!({"approved": approved});
        if current.status != "pending" {
            if matches!(current.status.as_str(), "answered" | "rejected")
                && current.response == Some(response)
            {
                return Ok(current);
            }
            return Err(RuntimeError::Protocol(
                "interaction 已解决或孤立，答案冲突".into(),
            ));
        }
        if current.revision != revision {
            return Err(RuntimeError::Protocol("interaction revision 冲突".into()));
        }
        let status: String = transaction.query_row(
            "SELECT status FROM runs WHERE id=?1",
            params![run_id.0],
            |row| row.get(0),
        )?;
        if status != "waiting_interaction" {
            return Err(RuntimeError::Protocol(
                "interaction 的 run 已不在等待".into(),
            ));
        }
        transaction.execute("UPDATE interactions SET status=?2, response_json=?3, revision=revision+1 WHERE id=?1 AND status='pending'",
            params![id.0, if approved {"answered"} else {"rejected"}, response.to_string()])?;
        let pending: i64 = transaction.query_row(
            "SELECT count(*) FROM interactions WHERE run_id=?1 AND status='pending'",
            params![run_id.0],
            |row| row.get(0),
        )?;
        if pending == 0 {
            transaction.execute(
                "UPDATE runs SET status='running' WHERE id=?1 AND status='waiting_interaction'",
                params![run_id.0],
            )?;
        }
        insert_event(
            &transaction,
            run_id,
            "interaction_resolved",
            &serde_json::json!({"interaction_id": id, "approved": approved, "revision": revision + 1}),
        )?;
        let result = read_interaction_in(&transaction, &id.0)?.expect("claimed interaction");
        transaction.commit()?;
        Ok(result)
    }

    pub fn finish(
        &self,
        run_id: &RunId,
        status: RunStatus,
        content: Option<&str>,
        error: Option<(i64, &str)>,
    ) -> Result<RunRecord, RuntimeError> {
        if !matches!(
            status,
            RunStatus::Completed
                | RunStatus::Failed
                | RunStatus::Cancelled
                | RunStatus::UnknownAfterRestart
        ) {
            return Err(RuntimeError::Protocol("finish 必须写入终态".into()));
        }
        let mut connection = self.connection.lock().expect("SQLite mutex poisoned");
        let transaction = connection.transaction()?;
        let current = read_run_in(&transaction, &run_id.0)?
            .ok_or_else(|| RuntimeError::Protocol("run 不存在".into()))?;
        if current.status.terminal() {
            if current.status == status
                && current.content.as_deref() == content
                && current.error_code == error.map(|item| item.0)
                && current.error_message.as_deref() == error.map(|item| item.1)
            {
                return Ok(current);
            }
            tracing::warn!(run_id = %run_id.0, existing = ?current.status, attempted = ?status, "拒绝冲突的 run 终态");
            return Err(RuntimeError::Protocol(format!("run {} 终态冲突", run_id.0)));
        }
        let incomplete_tools: i64 = transaction.query_row(
            "SELECT count(*) FROM tool_executions WHERE run_id=?1 AND status!='terminal'",
            params![run_id.0],
            |row| row.get(0),
        )?;
        let started_side_effects: i64 = transaction.query_row(
            "SELECT count(*) FROM tool_executions WHERE run_id=?1 AND effect!='read' AND status IN ('running','terminal')",
            params![run_id.0],
            |row| row.get(0),
        )?;
        let status = if incomplete_tools > 0
            || (status == RunStatus::Cancelled && started_side_effects > 0)
        {
            RunStatus::UnknownAfterRestart
        } else {
            status
        };
        let content = if status == RunStatus::Completed {
            content
        } else {
            None
        };
        let error = if status == RunStatus::UnknownAfterRestart {
            Some((-32002, "工具执行结果未知，禁止自动重放"))
        } else {
            error
        };
        if let Some(content) = content {
            insert_event(
                &transaction,
                run_id,
                "assistant_content",
                &serde_json::json!({"content": content}),
            )?;
        }
        insert_event(
            &transaction,
            run_id,
            "terminal",
            &serde_json::json!({"status": status, "error_code": error.map(|item| item.0), "error_message": error.map(|item| item.1)}),
        )?;
        transaction.execute("UPDATE runs SET status=?2, content=?3, error_code=?4, error_message=?5, updated_at_ms=?6 WHERE id=?1",
            params![run_id.0, status.as_str(), content, error.map(|item| item.0), error.map(|item| item.1), now_ms()])?;
        transaction.execute(
            "UPDATE turns SET status=?2 WHERE run_id=?1",
            params![run_id.0, status.as_str()],
        )?;
        transaction.execute(
            "UPDATE queued_messages SET status=?2 WHERE run_id=?1",
            params![run_id.0, status.as_str()],
        )?;
        transaction.execute("UPDATE interactions SET status='orphaned', revision=revision+1 WHERE run_id=?1 AND status='pending'", params![run_id.0])?;
        let result = read_run_in(&transaction, &run_id.0)?.expect("run still exists");
        transaction.commit()?;
        Ok(result)
    }

    pub fn reconcile_unknown(
        &self,
        run_id: &RunId,
        session_id: &SessionId,
        expected_last_seq: EventSeq,
        status: RunStatus,
        content: Option<&str>,
        evidence: &str,
    ) -> Result<RunRecord, RuntimeError> {
        if !matches!(
            status,
            RunStatus::Completed | RunStatus::Failed | RunStatus::Cancelled
        ) || evidence.trim().chars().count() < 16
            || evidence.len() > 4096
            || (status == RunStatus::Completed && content.is_none_or(str::is_empty))
        {
            return Err(RuntimeError::Protocol(
                "人工修复需要明确终态、内容和 16-4096 字节证据说明".into(),
            ));
        }
        let mut connection = self.connection.lock().expect("SQLite mutex poisoned");
        let transaction = connection.transaction()?;
        let run = read_run_in(&transaction, &run_id.0)?
            .ok_or_else(|| RuntimeError::Protocol("run 不存在".into()))?;
        if run.session_id != *session_id
            || run.status != RunStatus::UnknownAfterRestart
            || run.last_seq != expected_last_seq
        {
            return Err(RuntimeError::Protocol(
                "run owner、状态或事件序号冲突".into(),
            ));
        }
        let digest = format!("{:x}", Sha256::digest(evidence.as_bytes()));
        if let Some(content) = content.filter(|_| status == RunStatus::Completed) {
            insert_event(
                &transaction,
                run_id,
                "assistant_content",
                &serde_json::json!({"content": content, "manual": true}),
            )?;
        }
        insert_event(
            &transaction,
            run_id,
            "manual_resolution",
            &serde_json::json!({
            "from": "unknown_after_restart", "status": status, "evidence_sha256": digest,
            "evidence_note": evidence, "previous_last_seq": expected_last_seq}),
        )?;
        let (error_code, error_message): (Option<i64>, Option<&str>) = match status {
            RunStatus::Completed => (None, None),
            RunStatus::Failed => (Some(-32003), Some("人工核验为失败")),
            RunStatus::Cancelled => (Some(-32800), Some("人工核验为取消")),
            _ => unreachable!(),
        };
        transaction.execute("UPDATE runs SET status=?2, content=?3, error_code=?4, error_message=?5, updated_at_ms=?6 WHERE id=?1",
            params![run_id.0, status.as_str(), content.filter(|_| status == RunStatus::Completed), error_code, error_message, now_ms()])?;
        transaction.execute(
            "UPDATE turns SET status=?2 WHERE run_id=?1",
            params![run_id.0, status.as_str()],
        )?;
        transaction.execute(
            "UPDATE queued_messages SET status=?2 WHERE run_id=?1",
            params![run_id.0, status.as_str()],
        )?;
        let result = read_run_in(&transaction, &run_id.0)?.expect("reconciled run");
        transaction.commit()?;
        Ok(result)
    }

    pub fn read_run(&self, run_id: &RunId) -> Result<Option<RunRecord>, RuntimeError> {
        read_run_in(
            &self.connection.lock().expect("SQLite mutex poisoned"),
            &run_id.0,
        )
    }

    pub fn find_request(
        &self,
        session_id: &str,
        request_id: &RequestId,
    ) -> Result<Option<RunRecord>, RuntimeError> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        let request_json = serde_json::to_string(request_id)
            .map_err(|error| RuntimeError::Protocol(error.to_string()))?;
        let id = connection
            .query_row(
                "SELECT id FROM runs WHERE session_id=?1 AND request_id_json=?2",
                params![session_id, request_json],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        id.map(|id| read_run_in(&connection, &id))
            .transpose()
            .map(Option::flatten)
    }

    pub fn find_request_unique(
        &self,
        request_id: &RequestId,
    ) -> Result<Option<RunRecord>, RuntimeError> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        let request_json = serde_json::to_string(request_id)
            .map_err(|error| RuntimeError::Protocol(error.to_string()))?;
        let mut statement = connection
            .prepare("SELECT id FROM runs WHERE request_id_json=?1 ORDER BY rowid LIMIT 2")?;
        let ids = statement
            .query_map(params![request_json], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        if ids.len() > 1 {
            return Err(RuntimeError::Protocol(
                "request_id 跨 session 不唯一；必须提供 session_id + run_id".into(),
            ));
        }
        ids.first()
            .map(|id| read_run_in(&connection, id))
            .transpose()
            .map(Option::flatten)
    }

    pub fn events_after(
        &self,
        run_id: &RunId,
        after: EventSeq,
        limit: usize,
    ) -> Result<Vec<StoredEvent>, RuntimeError> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        let mut statement = connection.prepare("SELECT seq, event, data_json FROM events WHERE run_id=?1 AND seq>?2 ORDER BY seq LIMIT ?3")?;
        let rows =
            statement.query_map(params![run_id.0, after.0, limit.min(1000) as i64], |row| {
                Ok((
                    row.get::<_, u64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?;
        rows.map(|row| {
            let (seq, event, data) = row?;
            Ok(StoredEvent {
                run_id: run_id.clone(),
                seq: EventSeq(seq),
                event,
                data: serde_json::from_str(&data)
                    .map_err(|error| RuntimeError::Protocol(error.to_string()))?,
            })
        })
        .collect()
    }

    pub fn recover(&self) -> Result<usize, RuntimeError> {
        let mut connection = self.connection.lock().expect("SQLite mutex poisoned");
        let transaction = connection.transaction()?;
        let ids = {
            let mut statement = transaction
                .prepare("SELECT id FROM runs WHERE status IN ('running','waiting_interaction')")?;
            statement
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?
        };
        for id in &ids {
            let run_id = RunId(id.clone());
            insert_event(
                &transaction,
                &run_id,
                "terminal",
                &serde_json::json!({"status": RunStatus::UnknownAfterRestart}),
            )?;
            transaction.execute(
                "UPDATE runs SET status='unknown_after_restart', updated_at_ms=?2 WHERE id=?1",
                params![id, now_ms()],
            )?;
            transaction.execute(
                "UPDATE turns SET status='unknown_after_restart' WHERE run_id=?1",
                params![id],
            )?;
            transaction.execute(
                "UPDATE queued_messages SET status='unknown_after_restart' WHERE run_id=?1",
                params![id],
            )?;
        }
        // The question stays readable, but no vanished task may answer or execute it.
        transaction.execute(
            "UPDATE interactions SET status='orphaned' WHERE status='pending' AND run_id IN
            (SELECT id FROM runs WHERE status='unknown_after_restart')",
            [],
        )?;
        transaction.commit()?;
        Ok(ids.len())
    }
}

fn insert_event(
    connection: &Connection,
    run_id: &RunId,
    event: &str,
    data: &Value,
) -> Result<EventSeq, RuntimeError> {
    let seq: u64 = connection.query_row(
        "SELECT last_seq + 1 FROM runs WHERE id=?1",
        params![run_id.0],
        |row| row.get(0),
    )?;
    connection.execute("INSERT INTO events(run_id, seq, event, data_json, created_at_ms) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![run_id.0, seq, event, data.to_string(), now_ms()])?;
    connection.execute(
        "UPDATE runs SET last_seq=?2, updated_at_ms=?3 WHERE id=?1",
        params![run_id.0, seq, now_ms()],
    )?;
    Ok(EventSeq(seq))
}
fn read_run_in(connection: &Connection, id: &str) -> Result<Option<RunRecord>, RuntimeError> {
    let raw = connection.query_row("SELECT session_id, request_id_json, status, last_seq, content, error_code, error_message FROM runs WHERE id=?1",
        params![id], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?, row.get::<_, u64>(3)?, row.get::<_, Option<String>>(4)?, row.get::<_, Option<i64>>(5)?, row.get::<_, Option<String>>(6)?))).optional()?;
    let Some((session_id, request_id_json, status, last_seq, content, error_code, error_message)) =
        raw
    else {
        return Ok(None);
    };
    let request_id = serde_json::from_str(&request_id_json)
        .map_err(|error| RuntimeError::Protocol(error.to_string()))?;
    let rowid: i64 = id
        .strip_prefix("run-")
        .ok_or_else(|| RuntimeError::Protocol("无效 run id".into()))?
        .parse()
        .map_err(|_| RuntimeError::Protocol("无效 run id".into()))?;
    Ok(Some(RunRecord {
        run_id: RunId(id.into()),
        turn_id: TurnId(format!("turn-{rowid}")),
        session_id: SessionId(session_id),
        request_id,
        status: RunStatus::parse(&status)?,
        last_seq: EventSeq(last_seq),
        content,
        error_code,
        error_message,
    }))
}

fn read_interaction_in(
    connection: &Connection,
    id: &str,
) -> Result<Option<InteractionRecord>, RuntimeError> {
    let raw = connection
        .query_row(
            "SELECT r.session_id, i.run_id, i.kind, i.status, i.revision, i.prompt, i.response_json, i.payload_json
        FROM interactions i JOIN runs r ON r.id=i.run_id WHERE i.id=?1",
            params![id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, String>(7)?,
                ))
            },
        )
        .optional()?;
    raw.map(
        |(session_id, run_id, kind, status, revision, prompt, response_json, payload_json)| {
            Ok(InteractionRecord {
                interaction_id: InteractionId(id.to_owned()),
                session_id: SessionId(session_id),
                owner_run_id: RunId(run_id),
                kind,
                status,
                revision,
                prompt,
                payload: serde_json::from_str(&payload_json)
                    .map_err(|error| RuntimeError::Protocol(error.to_string()))?,
                response: response_json
                    .map(|json| {
                        serde_json::from_str(&json)
                            .map_err(|error| RuntimeError::Protocol(error.to_string()))
                    })
                    .transpose()?,
            })
        },
    )
    .transpose()
}
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);

    fn path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "my-agent-run-store-{}-{}.sqlite3",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[test]
    fn migration_sequence_and_terminal_are_durable_and_idempotent() {
        let path = path();
        let store = RunStore::open(&path).unwrap();
        let Admission::New(run) = store
            .admit(SessionId("session-a".into()), RequestId::Number(7), "hello")
            .unwrap()
        else {
            panic!("new run")
        };
        assert_eq!(run.last_seq, EventSeq(1));
        store.mark_running(&run.run_id).unwrap();
        assert_eq!(
            store
                .append_event(&run.run_id, "turn_started", &serde_json::json!({}))
                .unwrap(),
            EventSeq(2)
        );
        let committed = store
            .finish(&run.run_id, RunStatus::Completed, Some("done"), None)
            .unwrap();
        assert_eq!(committed.last_seq, EventSeq(4));
        assert_eq!(
            store
                .finish(&run.run_id, RunStatus::Completed, Some("done"), None)
                .unwrap()
                .last_seq,
            EventSeq(4)
        );
        assert!(
            store
                .finish(&run.run_id, RunStatus::Failed, None, Some((-1, "conflict")))
                .is_err()
        );
        drop(store);
        let reopened = RunStore::open(&path).unwrap();
        assert_eq!(reopened.recover().unwrap(), 0);
        let loaded = reopened.read_run(&run.run_id).unwrap().unwrap();
        assert_eq!(loaded.status, RunStatus::Completed);
        assert_eq!(loaded.content.as_deref(), Some("done"));
        assert_eq!(
            reopened
                .events_after(&run.run_id, EventSeq(2), 10)
                .unwrap()
                .len(),
            2
        );
        let Admission::Existing(duplicate) = reopened
            .admit(SessionId("session-a".into()), RequestId::Number(7), "hello")
            .unwrap()
        else {
            panic!("duplicate")
        };
        assert_eq!(duplicate.run_id, run.run_id);
    }

    #[test]
    fn restart_marks_uncertain_run_unknown_without_replaying() {
        let path = path();
        let store = RunStore::open(&path).unwrap();
        let Admission::New(run) = store
            .admit(
                SessionId("session-b".into()),
                RequestId::String("one".into()),
                "side effect?",
            )
            .unwrap()
        else {
            panic!("new run")
        };
        store.mark_running(&run.run_id).unwrap();
        store
            .append_event(
                &run.run_id,
                "tool_started",
                &serde_json::json!({"name":"exec"}),
            )
            .unwrap();
        drop(store);
        let reopened = RunStore::open(&path).unwrap();
        assert_eq!(reopened.recover().unwrap(), 1);
        assert_eq!(reopened.recover().unwrap(), 0);
        assert_eq!(
            reopened.read_run(&run.run_id).unwrap().unwrap().status,
            RunStatus::UnknownAfterRestart
        );
        assert!(
            reopened
                .finish(&run.run_id, RunStatus::Completed, Some("late"), None)
                .is_err()
        );
    }

    #[test]
    fn queue_is_ordered_single_writer_and_survives_restart() {
        let path = path();
        let store = RunStore::open(&path).unwrap();
        let session = SessionId("shared".into());
        let Admission::New(first) = store
            .admit(session.clone(), RequestId::Number(1), "first")
            .unwrap()
        else {
            panic!()
        };
        let Admission::New(second) = store
            .admit(session.clone(), RequestId::Number(2), "second")
            .unwrap()
        else {
            panic!()
        };
        assert!(!store.try_start_queued(&second.run_id).unwrap());
        assert!(store.try_start_queued(&first.run_id).unwrap());
        assert!(!store.try_start_queued(&second.run_id).unwrap());
        assert!(
            store
                .admit_with_mode(
                    session.clone(),
                    RequestId::Number(3),
                    "third",
                    AdmissionMode::RejectIfBusy
                )
                .is_err()
        );
        assert_eq!(
            store.queued_messages(&session.0).unwrap()[0].run_id,
            second.run_id
        );
        drop(store);
        let reopened = RunStore::open(&path).unwrap();
        assert_eq!(reopened.recover().unwrap(), 1);
        assert_eq!(
            reopened.read_run(&first.run_id).unwrap().unwrap().status,
            RunStatus::UnknownAfterRestart
        );
        assert_eq!(
            reopened.recoverable_queued().unwrap()[0].run_id,
            second.run_id
        );
        assert!(reopened.try_start_queued(&second.run_id).unwrap());
    }

    #[test]
    fn tool_receipt_crash_window_is_unknown_and_never_replayed() {
        let path = path();
        let store = RunStore::open(&path).unwrap();
        let Admission::New(run) = store
            .admit(SessionId("effects".into()), RequestId::Number(1), "write")
            .unwrap()
        else {
            panic!()
        };
        assert!(store.try_start_queued(&run.run_id).unwrap());
        let call = ToolCall {
            id: "call-1".into(),
            name: "write_file".into(),
            arguments: serde_json::json!({"path":"secret"}),
        };
        store
            .prepare_tool_batch(
                &run.run_id,
                1,
                &[(call, "external_side_effect".into(), "digest".into(), false)],
            )
            .unwrap();
        store.start_tool(&run.run_id, 1, "call-1").unwrap();
        drop(store);
        let reopened = RunStore::open(&path).unwrap();
        assert_eq!(reopened.recover().unwrap(), 1);
        let receipts = reopened.tool_receipts(&run.run_id).unwrap();
        assert_eq!(receipts[0].status, "running");
        assert!(!receipts[0].safe_to_replay);
        assert_eq!(
            reopened.read_run(&run.run_id).unwrap().unwrap().status,
            RunStatus::UnknownAfterRestart
        );
        assert!(reopened.finish_tool_batch(&run.run_id, 1).is_err());
    }

    #[test]
    fn prepared_tool_batch_is_atomic_and_cancel_with_incomplete_receipt_is_unknown() {
        let path = path();
        let store = RunStore::open(&path).unwrap();
        let Admission::New(run) = store
            .admit(SessionId("batch".into()), RequestId::Number(1), "batch")
            .unwrap()
        else {
            panic!()
        };
        assert!(store.try_start_queued(&run.run_id).unwrap());
        let call = ToolCall {
            id: "same".into(),
            name: "exec".into(),
            arguments: serde_json::json!({}),
        };
        let duplicate = vec![
            (call.clone(), "process".into(), "digest".into(), false),
            (call.clone(), "process".into(), "digest".into(), false),
        ];
        assert!(
            store
                .prepare_tool_batch(&run.run_id, 1, &duplicate)
                .is_err()
        );
        assert!(store.tool_receipts(&run.run_id).unwrap().is_empty());
        store
            .prepare_tool_batch(&run.run_id, 1, &duplicate[..1])
            .unwrap();
        assert_eq!(
            store.tool_receipts(&run.run_id).unwrap()[0].status,
            "prepared"
        );
        assert!(store.finish_tool_batch(&run.run_id, 1).is_err());
        let ended = store
            .finish(
                &run.run_id,
                RunStatus::Cancelled,
                None,
                Some((-32800, "cancelled")),
            )
            .unwrap();
        assert_eq!(ended.status, RunStatus::UnknownAfterRestart);
        assert_eq!(ended.error_code, Some(-32002));
    }

    #[test]
    fn jsonl_assistant_without_sqlite_terminal_does_not_imply_completion() {
        let path = path();
        let store = RunStore::open(&path).unwrap();
        let Admission::New(run) = store
            .admit(SessionId("jsonl".into()), RequestId::Number(1), "hello")
            .unwrap()
        else {
            panic!()
        };
        assert!(store.try_start_queued(&run.run_id).unwrap());
        let transcript = path.with_extension("jsonl");
        std::fs::write(&transcript, "{\"role\":\"user\",\"content\":\"hello\"}\n{\"role\":\"assistant\",\"content\":\"done\"}\n").unwrap();
        drop(store);
        let reopened = RunStore::open(&path).unwrap();
        assert_eq!(reopened.recover().unwrap(), 1);
        assert_eq!(
            reopened.read_run(&run.run_id).unwrap().unwrap().status,
            RunStatus::UnknownAfterRestart
        );
        assert!(
            std::fs::read_to_string(transcript)
                .unwrap()
                .contains("done")
        );
    }

    #[test]
    fn manual_resolution_requires_exact_owner_and_event_cursor() {
        let path = path();
        let store = RunStore::open(&path).unwrap();
        let Admission::New(run) = store
            .admit(SessionId("repair".into()), RequestId::Number(1), "do work")
            .unwrap()
        else {
            panic!()
        };
        assert!(store.try_start_queued(&run.run_id).unwrap());
        drop(store);
        let reopened = RunStore::open(&path).unwrap();
        assert_eq!(reopened.recover().unwrap(), 1);
        let unknown = reopened.read_run(&run.run_id).unwrap().unwrap();
        assert!(
            reopened
                .reconcile_unknown(
                    &run.run_id,
                    &SessionId("wrong".into()),
                    unknown.last_seq,
                    RunStatus::Completed,
                    Some("done"),
                    "confirmed from external system"
                )
                .is_err()
        );
        assert!(
            reopened
                .reconcile_unknown(
                    &run.run_id,
                    &run.session_id,
                    EventSeq(0),
                    RunStatus::Completed,
                    Some("done"),
                    "confirmed from external system"
                )
                .is_err()
        );
        assert!(
            reopened
                .reconcile_unknown(
                    &run.run_id,
                    &run.session_id,
                    unknown.last_seq,
                    RunStatus::Completed,
                    Some("done"),
                    "short"
                )
                .is_err()
        );
        let resolved = reopened
            .reconcile_unknown(
                &run.run_id,
                &run.session_id,
                unknown.last_seq,
                RunStatus::Completed,
                Some("done"),
                "confirmed from external system",
            )
            .unwrap();
        assert_eq!(resolved.status, RunStatus::Completed);
        assert_eq!(resolved.content.as_deref(), Some("done"));
        assert!(
            reopened
                .events_after(&run.run_id, unknown.last_seq, 10)
                .unwrap()
                .iter()
                .any(|event| event.event == "manual_resolution")
        );
        assert!(
            reopened
                .reconcile_unknown(
                    &run.run_id,
                    &run.session_id,
                    unknown.last_seq,
                    RunStatus::Failed,
                    None,
                    "different external evidence"
                )
                .is_err()
        );
    }

    #[test]
    fn cancellation_after_confirmed_side_effect_is_not_confirmed_cancel() {
        let store = RunStore::open(&path()).unwrap();
        let Admission::New(run) = store
            .admit(
                SessionId("effect-cancel".into()),
                RequestId::Number(1),
                "write",
            )
            .unwrap()
        else {
            panic!()
        };
        assert!(store.try_start_queued(&run.run_id).unwrap());
        let call = ToolCall {
            id: "write-1".into(),
            name: "write_file".into(),
            arguments: serde_json::json!({"path":"x","content":"y"}),
        };
        store
            .prepare_tool_batch(
                &run.run_id,
                1,
                &[(call, "external_side_effect".into(), "digest".into(), false)],
            )
            .unwrap();
        store.start_tool(&run.run_id, 1, "write-1").unwrap();
        store
            .finish_tool(
                &run.run_id,
                1,
                "write-1",
                "success",
                None,
                &serde_json::json!({"success":true}),
            )
            .unwrap();
        store.finish_tool_batch(&run.run_id, 1).unwrap();
        let finished = store
            .finish(
                &run.run_id,
                RunStatus::Cancelled,
                None,
                Some((-32800, "cancelled")),
            )
            .unwrap();
        assert_eq!(finished.status, RunStatus::UnknownAfterRestart);
        assert_eq!(finished.error_code, Some(-32002));
    }

    #[test]
    fn interaction_claim_checks_owner_revision_and_idempotency() {
        let path = path();
        let store = RunStore::open(&path).unwrap();
        let Admission::New(run) = store
            .admit(SessionId("owner".into()), RequestId::Number(1), "approve")
            .unwrap()
        else {
            panic!()
        };
        assert!(store.try_start_queued(&run.run_id).unwrap());
        let id = InteractionId("approval-1".into());
        store
            .append_event(
                &run.run_id,
                "approval_required",
                &serde_json::json!({"approval":{"id":id.0,"prompt":"allow?"}}),
            )
            .unwrap();
        let second = InteractionId("approval-2".into());
        store
            .append_event(
                &run.run_id,
                "approval_required",
                &serde_json::json!({"approval":{"id":second.0,"prompt":"also allow?"}}),
            )
            .unwrap();
        assert!(
            store
                .claim_interaction(&id, &SessionId("other".into()), &run.run_id, 0, true)
                .is_err()
        );
        assert!(
            store
                .claim_interaction(&id, &run.session_id, &run.run_id, 1, true)
                .is_err()
        );
        let answered = store
            .claim_interaction(&id, &run.session_id, &run.run_id, 0, true)
            .unwrap();
        assert_eq!(answered.revision, 1);
        assert_eq!(
            store.read_run(&run.run_id).unwrap().unwrap().status,
            RunStatus::WaitingInteraction
        );
        store
            .claim_interaction(&second, &run.session_id, &run.run_id, 0, false)
            .unwrap();
        assert_eq!(
            store.read_run(&run.run_id).unwrap().unwrap().status,
            RunStatus::Running
        );
        assert_eq!(
            store
                .claim_interaction(&id, &run.session_id, &run.run_id, 0, true)
                .unwrap()
                .revision,
            1
        );
        assert!(
            store
                .claim_interaction(&id, &run.session_id, &run.run_id, 1, false)
                .is_err()
        );
    }

    #[test]
    fn v1_database_upgrades_in_place_and_future_version_fails_closed() {
        let path = path();
        let connection = Connection::open(&path).unwrap();
        connection.execute_batch("CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL);
            INSERT INTO schema_migrations VALUES (1, 1);
            CREATE TABLE sessions(id TEXT PRIMARY KEY, created_at_ms INTEGER NOT NULL);
            INSERT INTO sessions VALUES ('old-session', 1);
            CREATE TABLE runs(id TEXT PRIMARY KEY, session_id TEXT NOT NULL, request_id_json TEXT NOT NULL,
                status TEXT NOT NULL, input TEXT NOT NULL, last_seq INTEGER NOT NULL DEFAULT 0,
                content TEXT, error_code INTEGER, error_message TEXT, created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL, UNIQUE(session_id, request_id_json));
            INSERT INTO runs(id, session_id, request_id_json, status, input, created_at_ms, updated_at_ms)
                VALUES ('old-run','old-session','1','queued','hello',1,1);
            CREATE TABLE interactions(id TEXT PRIMARY KEY, run_id TEXT NOT NULL, kind TEXT NOT NULL,
                status TEXT NOT NULL, prompt TEXT NOT NULL, response_json TEXT);
            CREATE TABLE queued_messages(id INTEGER PRIMARY KEY, session_id TEXT NOT NULL,
                run_id TEXT, message TEXT NOT NULL, status TEXT NOT NULL);
            CREATE TABLE tool_executions(id INTEGER PRIMARY KEY, run_id TEXT NOT NULL, call_id TEXT NOT NULL,
                status TEXT NOT NULL, receipt_json TEXT, UNIQUE(run_id, call_id));
            INSERT INTO tool_executions(run_id,call_id,status) VALUES ('old-run','old-call','prepared');
            CREATE TABLE turns(id TEXT PRIMARY KEY, run_id TEXT NOT NULL UNIQUE, status TEXT NOT NULL);
            INSERT INTO turns VALUES ('old-turn','old-run','queued');
            CREATE TABLE events(run_id TEXT NOT NULL, seq INTEGER NOT NULL, event TEXT NOT NULL,
                data_json TEXT NOT NULL, created_at_ms INTEGER NOT NULL, PRIMARY KEY(run_id,seq));").unwrap();
        drop(connection);
        let upgraded = RunStore::open(&path).unwrap();
        assert_eq!(
            upgraded.queued_messages("old-session").unwrap()[0].run_id.0,
            "old-run"
        );
        assert_eq!(
            upgraded.tool_receipts(&RunId("old-run".into())).unwrap()[0].call_id,
            "old-call"
        );
        drop(upgraded);
        let connection = Connection::open(&path).unwrap();
        connection
            .execute("INSERT INTO schema_migrations VALUES (99, 99)", [])
            .unwrap();
        drop(connection);
        assert!(RunStore::open(&path).is_err());
    }
}
