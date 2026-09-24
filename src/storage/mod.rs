//! SQLite control facts. JSONL remains a readable transcript, not the run authority.
use std::path::Path;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::daemon::protocol::RequestId;

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

pub enum Admission {
    New(RunRecord),
    Existing(RunRecord),
}

pub struct RunStore {
    connection: Mutex<Connection>,
}
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
        if version > 1 {
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
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    pub fn admit(
        &self,
        session_id: SessionId,
        request_id: RequestId,
        input: &str,
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
        let run = read_run_in(&transaction, &run_id.0)?.expect("inserted run");
        transaction.commit()?;
        Ok(Admission::New(run))
    }

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
            transaction.execute("INSERT INTO interactions(id, run_id, kind, status, prompt) VALUES (?1, ?2, 'approval', 'pending', ?3)", params![id, run_id.0, prompt])?;
            transaction.execute(
                "UPDATE runs SET status='waiting_interaction' WHERE id=?1",
                params![run_id.0],
            )?;
        }
        transaction.commit()?;
        Ok(seq)
    }

    pub fn answer_interaction(
        &self,
        id: &InteractionId,
        approved: bool,
    ) -> Result<(), RuntimeError> {
        let mut connection = self.connection.lock().expect("SQLite mutex poisoned");
        let transaction = connection.transaction()?;
        transaction.execute("UPDATE interactions SET status='answered', response_json=?2 WHERE id=?1 AND status='pending'",
            params![id.0, serde_json::json!({"approved": approved}).to_string()])?;
        transaction.execute("UPDATE runs SET status='running' WHERE id IN (SELECT run_id FROM interactions WHERE id=?1) AND status='waiting_interaction'", params![id.0])?;
        transaction.commit()?;
        Ok(())
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
            RunStatus::Completed | RunStatus::Failed | RunStatus::Cancelled
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
        let result = read_run_in(&transaction, &run_id.0)?.expect("run still exists");
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
            let mut statement = transaction.prepare(
                "SELECT id FROM runs WHERE status IN ('queued','running','waiting_interaction')",
            )?;
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
}
