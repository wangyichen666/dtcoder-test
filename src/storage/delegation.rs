//! Durable child-run ownership and result delivery on the existing RunStore connection.
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use serde_json::{Value, json};

use super::{
    RequestId, RouteSnapshot, RunId, RunRecord, RunStatus, RunStore, RuntimeError, SessionId,
    TurnId, insert_event, now_ms, read_run_in,
};

const MAX_DEPTH: i64 = 2;
const MAX_ROOT_SPAWNS: i64 = 8;
const MAX_ROOT_ACTIVE: i64 = 4;

#[derive(Clone, Debug)]
pub struct DelegationRequest {
    pub parent_session_id: SessionId,
    pub parent_run_id: RunId,
    pub child_session_id: SessionId,
    pub spawn_key: String,
    pub task: String,
    pub tools: Vec<String>,
    pub permission_mode: String,
    pub cwd: String,
    pub max_rounds: i64,
    pub max_tokens: i64,
    pub max_tool_calls: i64,
    pub deadline_ms: i64,
}

#[derive(Clone, Debug, Serialize)]
pub struct DelegationRecord {
    pub root_session_id: SessionId,
    pub root_run_id: RunId,
    pub parent_session_id: SessionId,
    pub parent_run_id: RunId,
    pub child_session_id: SessionId,
    pub child_run_id: RunId,
    pub spawn_key: String,
    pub depth: i64,
    pub status: RunStatus,
    pub created_at_ms: i64,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
    pub tools: Vec<String>,
    pub permission_mode: String,
    pub route: Option<RouteSnapshot>,
    pub cwd: String,
    pub max_rounds: i64,
    pub max_tokens: i64,
    pub max_tool_calls: i64,
    pub deadline_ms: i64,
    pub terminal: Option<Value>,
    pub result_state: String,
    pub reservation_owner: Option<String>,
    pub revision: i64,
    pub reservation_released_at_ms: Option<i64>,
    pub reservation_expires_at_ms: Option<i64>,
    pub content: Option<String>,
    pub error_code: Option<i64>,
    pub error_message: Option<String>,
}

impl RunStore {
    pub fn admit_delegation(
        &self,
        request: DelegationRequest,
    ) -> Result<DelegationRecord, RuntimeError> {
        if request.task.trim().is_empty()
            || request.spawn_key.trim().is_empty()
            || request.tools.is_empty()
            || request.max_rounds <= 0
            || request.max_tokens <= 0
            || request.max_tool_calls <= 0
        {
            return Err(RuntimeError::Protocol("子 Agent 参数或预算无效".into()));
        }
        let mut connection = self.connection.lock().expect("SQLite mutex poisoned");
        let transaction = connection.transaction()?;
        let parent = read_run_in(&transaction, &request.parent_run_id.0)?
            .ok_or_else(|| RuntimeError::Protocol("父 run 不存在".into()))?;
        if parent.session_id != request.parent_session_id {
            return Err(RuntimeError::Protocol("父 run 不属于指定 session".into()));
        }
        let duplicate: Option<String> = transaction
            .query_row(
                "SELECT child_run_id FROM delegations WHERE parent_run_id=?1 AND spawn_key=?2",
                params![request.parent_run_id.0, request.spawn_key],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(child) = duplicate {
            let record = read_delegation_in(&transaction, &child)?.expect("existing delegation");
            let existing_task: String = transaction.query_row(
                "SELECT input FROM runs WHERE id=?1",
                params![child],
                |row| row.get(0),
            )?;
            if existing_task != request.task
                || record.tools != request.tools
                || record.max_rounds != request.max_rounds
                || record.max_tokens != request.max_tokens
                || record.max_tool_calls != request.max_tool_calls
            {
                return Err(RuntimeError::Protocol(
                    "相同 spawn_key 的委派参数冲突".into(),
                ));
            }
            transaction.commit()?;
            return Ok(record);
        }
        if !matches!(
            parent.status,
            RunStatus::Running | RunStatus::WaitingInteraction
        ) {
            return Err(RuntimeError::Protocol("父 run 状态不允许委派".into()));
        }
        if request.deadline_ms <= now_ms() {
            return Err(RuntimeError::Protocol("子 Agent deadline 已过".into()));
        }
        let ancestor: Option<(String, String, i64)> = transaction
            .query_row(
                "SELECT root_session_id, root_run_id, depth FROM delegations WHERE child_run_id=?1",
                params![request.parent_run_id.0],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let (root_session_id, root_run_id, depth) = match ancestor {
            Some((session, run, parent_depth)) => (session, run, parent_depth + 1),
            None => (parent.session_id.0.clone(), parent.run_id.0.clone(), 1),
        };
        if depth > MAX_DEPTH {
            return Err(RuntimeError::Protocol("子 Agent 深度上限为 2".into()));
        }
        let (total, active): (i64, i64) = transaction.query_row(
            "SELECT count(*), COALESCE(sum(CASE WHEN status IN ('queued','running','waiting_interaction') THEN 1 ELSE 0 END),0)
             FROM delegations WHERE root_run_id=?1",
            params![root_run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if total >= MAX_ROOT_SPAWNS || active >= MAX_ROOT_ACTIVE {
            return Err(RuntimeError::Protocol("子 Agent 总量或并发上限已满".into()));
        }
        let route_json: Option<String> = transaction
            .query_row(
                "SELECT snapshot_json FROM run_routes WHERE run_id=?1",
                params![request.parent_run_id.0],
                |row| row.get(0),
            )
            .optional()?;
        let now = now_ms();
        transaction.execute(
            "INSERT INTO sessions(id, created_at_ms) VALUES (?1, ?2)",
            params![request.child_session_id.0, now],
        )?;
        let child_request_id =
            RequestId::String(format!("delegation:{}", request.child_session_id.0));
        let request_json = serde_json::to_string(&child_request_id)
            .map_err(|error| RuntimeError::Protocol(error.to_string()))?;
        transaction.execute(
            "INSERT INTO runs(id, session_id, request_id_json, status, input, created_at_ms, updated_at_ms)
             VALUES ('pending', ?1, ?2, 'queued', ?3, ?4, ?4)",
            params![request.child_session_id.0, request_json, request.task, now],
        )?;
        let run_rowid = transaction.last_insert_rowid();
        let child_run_id = RunId(format!("run-{run_rowid}"));
        let child_turn_id = TurnId(format!("turn-{run_rowid}"));
        transaction.execute(
            "UPDATE runs SET id=?1 WHERE rowid=?2",
            params![child_run_id.0, run_rowid],
        )?;
        transaction.execute(
            "INSERT INTO turns(id, run_id, status) VALUES (?1, ?2, 'queued')",
            params![child_turn_id.0, child_run_id.0],
        )?;
        if let Some(route) = &route_json {
            transaction.execute(
                "INSERT INTO run_routes(run_id, snapshot_json) VALUES (?1, ?2)",
                params![child_run_id.0, route],
            )?;
        }
        insert_event(
            &transaction,
            &child_run_id,
            "user_message",
            &json!({"content": request.task}),
        )?;
        transaction.execute(
            "INSERT INTO queued_messages(session_id, run_id, message, status) VALUES (?1, ?2, ?3, 'queued')",
            params![request.child_session_id.0, child_run_id.0, request.task],
        )?;
        let tools_json = serde_json::to_string(&request.tools)
            .map_err(|error| RuntimeError::Protocol(error.to_string()))?;
        transaction.execute(
            "INSERT INTO delegations(root_session_id, root_run_id, parent_session_id, parent_run_id,
                spawn_key, child_session_id, child_run_id, depth, status, created_at_ms, tools_json,
                permission_mode, route_json, cwd, max_rounds, max_tokens, max_tool_calls, deadline_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'queued', ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
            params![root_session_id, root_run_id, request.parent_session_id.0,
                request.parent_run_id.0, request.spawn_key, request.child_session_id.0, child_run_id.0, depth,
                now, tools_json, request.permission_mode, route_json, request.cwd,
                request.max_rounds, request.max_tokens, request.max_tool_calls, request.deadline_ms],
        )?;
        insert_event(
            &transaction,
            &request.parent_run_id,
            "delegation_spawned",
            &json!({"child_session_id": request.child_session_id, "child_run_id": child_run_id,
                "depth": depth, "status": "queued"}),
        )?;
        let record = read_delegation_in(&transaction, &child_run_id.0)?.expect("new delegation");
        transaction.commit()?;
        Ok(record)
    }

    pub fn delegation(
        &self,
        child_run_id: &RunId,
    ) -> Result<Option<DelegationRecord>, RuntimeError> {
        read_delegation_in(
            &self.connection.lock().expect("SQLite mutex poisoned"),
            &child_run_id.0,
        )
    }

    pub fn delegation_by_spawn_key(
        &self,
        parent_run_id: &RunId,
        spawn_key: &str,
    ) -> Result<Option<DelegationRecord>, RuntimeError> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        let child: Option<String> = connection
            .query_row(
                "SELECT child_run_id FROM delegations WHERE parent_run_id=?1 AND spawn_key=?2",
                params![parent_run_id.0, spawn_key],
                |row| row.get(0),
            )
            .optional()?;
        child
            .map(|id| read_delegation_in(&connection, &id))
            .transpose()
            .map(Option::flatten)
    }

    pub fn delegation_for_session(
        &self,
        session_id: &str,
    ) -> Result<Option<DelegationRecord>, RuntimeError> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        let child: Option<String> = connection
            .query_row(
                "SELECT child_run_id FROM delegations WHERE child_session_id=?1",
                params![session_id],
                |row| row.get(0),
            )
            .optional()?;
        child
            .map(|id| read_delegation_in(&connection, &id))
            .transpose()
            .map(Option::flatten)
    }

    pub fn list_delegations(
        &self,
        root_run_id: &RunId,
    ) -> Result<Vec<DelegationRecord>, RuntimeError> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        let mut statement = connection
            .prepare("SELECT child_run_id FROM delegations WHERE root_run_id=?1 ORDER BY id")?;
        let ids = statement
            .query_map(params![root_run_id.0], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        ids.iter()
            .map(|id| {
                read_delegation_in(&connection, id)?
                    .ok_or_else(|| RuntimeError::Protocol("委派记录消失".into()))
            })
            .collect()
    }

    pub fn reserve_delegation_result(
        &self,
        child_run_id: &RunId,
        owner: &str,
        revision: i64,
    ) -> Result<DelegationRecord, RuntimeError> {
        let mut connection = self.connection.lock().expect("SQLite mutex poisoned");
        let transaction = connection.transaction()?;
        let record = read_delegation_in(&transaction, &child_run_id.0)?
            .ok_or_else(|| RuntimeError::Protocol("子 Agent 不存在".into()))?;
        if owner.is_empty() {
            return Err(RuntimeError::Protocol("reservation owner 不能为空".into()));
        }
        if record.reservation_owner.as_deref() == Some(owner)
            && (record.result_state == "delivered"
                || record.result_state == "reserved"
                    && record
                        .reservation_expires_at_ms
                        .is_some_and(|expires| expires > now_ms()))
        {
            transaction.commit()?;
            return Ok(record);
        }
        if !record.status.terminal() || record.revision != revision {
            return Err(RuntimeError::Protocol(
                "结果尚未终态或 revision 冲突".into(),
            ));
        }
        if record.result_state == "reserved"
            && record
                .reservation_expires_at_ms
                .is_some_and(|expires| expires <= now_ms())
        {
            transaction.execute(
                "UPDATE delegations SET result_state='unconsumed',
                reservation_owner=NULL, reservation_expires_at_ms=NULL,
                reservation_released_at_ms=?2, revision=revision+1 WHERE child_run_id=?1",
                params![child_run_id.0, now_ms()],
            )?;
        } else if record.result_state != "unconsumed" {
            return Err(RuntimeError::Protocol("结果已由其他 owner 领取".into()));
        }
        transaction.execute(
            "UPDATE delegations SET result_state='reserved', reservation_owner=?2,
            reservation_expires_at_ms=?3, revision=revision+1 WHERE child_run_id=?1",
            params![child_run_id.0, owner, now_ms() + 30_000],
        )?;
        let result = read_delegation_in(&transaction, &child_run_id.0)?.expect("delegation");
        transaction.commit()?;
        Ok(result)
    }

    pub fn release_delegation_result(
        &self,
        child_run_id: &RunId,
        owner: &str,
        revision: i64,
    ) -> Result<DelegationRecord, RuntimeError> {
        self.transition_result(child_run_id, owner, revision, "unconsumed")
    }

    pub fn deliver_delegation_result(
        &self,
        child_run_id: &RunId,
        owner: &str,
        revision: i64,
    ) -> Result<DelegationRecord, RuntimeError> {
        self.transition_result(child_run_id, owner, revision, "delivered")
    }

    fn transition_result(
        &self,
        child_run_id: &RunId,
        owner: &str,
        revision: i64,
        target: &str,
    ) -> Result<DelegationRecord, RuntimeError> {
        let mut connection = self.connection.lock().expect("SQLite mutex poisoned");
        let transaction = connection.transaction()?;
        let record = read_delegation_in(&transaction, &child_run_id.0)?
            .ok_or_else(|| RuntimeError::Protocol("子 Agent 不存在".into()))?;
        if target == "delivered"
            && record.result_state == "delivered"
            && record.reservation_owner.as_deref() == Some(owner)
        {
            transaction.commit()?;
            return Ok(record);
        }
        if record.result_state != "reserved"
            || record.reservation_owner.as_deref() != Some(owner)
            || record.revision != revision
            || target == "delivered"
                && record
                    .reservation_expires_at_ms
                    .is_some_and(|expires| expires <= now_ms())
        {
            return Err(RuntimeError::Protocol(
                "结果 reservation owner/revision 冲突".into(),
            ));
        }
        transaction.execute("UPDATE delegations SET result_state=?2,
            reservation_owner=CASE WHEN ?2='delivered' THEN reservation_owner ELSE NULL END,
            reservation_expires_at_ms=NULL,
            reservation_released_at_ms=CASE WHEN ?2='unconsumed' THEN ?3 ELSE reservation_released_at_ms END,
            revision=revision+1 WHERE child_run_id=?1", params![child_run_id.0, target, now_ms()])?;
        let result = read_delegation_in(&transaction, &child_run_id.0)?.expect("delegation");
        transaction.commit()?;
        Ok(result)
    }
}

pub(super) fn mark_terminal_in(
    connection: &Connection,
    child_run_id: &RunId,
    run: &RunRecord,
) -> Result<(), RuntimeError> {
    let parent: Option<String> = connection
        .query_row(
            "SELECT parent_run_id FROM delegations WHERE child_run_id=?1",
            params![child_run_id.0],
            |row| row.get(0),
        )
        .optional()?;
    let Some(parent) = parent else {
        return Ok(());
    };
    connection.execute(
        "UPDATE delegations SET status=?2, finished_at_ms=?3, terminal_json=?4
        WHERE child_run_id=?1",
        params![
            child_run_id.0,
            run.status.as_str(),
            now_ms(),
            json!({"status": run.status, "content": run.content, "error_code": run.error_code,
                "error_message": run.error_message})
            .to_string()
        ],
    )?;
    if let Some(parent_run) = read_run_in(connection, &parent)?
        && !parent_run.status.terminal()
    {
        insert_event(
            connection,
            &parent_run.run_id,
            "delegation_terminal",
            &json!({"child_run_id": child_run_id, "status": run.status,
                    "summary": run.content.as_deref().unwrap_or("").chars().take(512).collect::<String>()}),
        )?;
    }
    Ok(())
}

fn read_delegation_in(
    connection: &Connection,
    child_run_id: &str,
) -> Result<Option<DelegationRecord>, RuntimeError> {
    let raw = connection
        .query_row(
            "SELECT d.root_session_id, d.root_run_id, d.parent_session_id, d.parent_run_id,
            d.child_session_id, d.spawn_key, d.depth, d.status, d.created_at_ms, d.started_at_ms,
            d.finished_at_ms, d.tools_json, d.permission_mode, d.route_json, d.cwd,
            d.max_rounds, d.max_tokens, d.max_tool_calls, d.deadline_ms, d.terminal_json,
            d.result_state, d.reservation_owner, d.revision, d.reservation_released_at_ms,
            d.reservation_expires_at_ms,
            r.content, r.error_code, r.error_message
         FROM delegations d JOIN runs r ON r.id=d.child_run_id WHERE d.child_run_id=?1",
            params![child_run_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, i64>(8)?,
                    row.get::<_, Option<i64>>(9)?,
                    row.get::<_, Option<i64>>(10)?,
                    row.get::<_, String>(11)?,
                    row.get::<_, String>(12)?,
                    row.get::<_, Option<String>>(13)?,
                    row.get::<_, String>(14)?,
                    row.get::<_, i64>(15)?,
                    row.get::<_, i64>(16)?,
                    row.get::<_, i64>(17)?,
                    row.get::<_, i64>(18)?,
                    row.get::<_, Option<String>>(19)?,
                    row.get::<_, String>(20)?,
                    row.get::<_, Option<String>>(21)?,
                    row.get::<_, i64>(22)?,
                    row.get::<_, Option<i64>>(23)?,
                    row.get::<_, Option<i64>>(24)?,
                    row.get::<_, Option<String>>(25)?,
                    row.get::<_, Option<i64>>(26)?,
                    row.get::<_, Option<String>>(27)?,
                ))
            },
        )
        .optional()?;
    let Some((
        root_session,
        root_run,
        parent_session,
        parent_run,
        child_session,
        spawn_key,
        depth,
        status,
        created,
        started,
        finished,
        tools,
        permission,
        route,
        cwd,
        rounds,
        tokens,
        tool_calls,
        deadline,
        terminal,
        result_state,
        owner,
        revision,
        released,
        expires,
        content,
        error_code,
        error_message,
    )) = raw
    else {
        return Ok(None);
    };
    Ok(Some(DelegationRecord {
        root_session_id: SessionId(root_session),
        root_run_id: RunId(root_run),
        parent_session_id: SessionId(parent_session),
        parent_run_id: RunId(parent_run),
        child_session_id: SessionId(child_session),
        child_run_id: RunId(child_run_id.to_owned()),
        spawn_key,
        depth,
        status: RunStatus::parse(&status)?,
        created_at_ms: created,
        started_at_ms: started,
        finished_at_ms: finished,
        tools: serde_json::from_str(&tools)
            .map_err(|error| RuntimeError::Protocol(error.to_string()))?,
        permission_mode: permission,
        route: route
            .map(|value| {
                serde_json::from_str(&value)
                    .map_err(|error| RuntimeError::Protocol(error.to_string()))
            })
            .transpose()?,
        cwd,
        max_rounds: rounds,
        max_tokens: tokens,
        max_tool_calls: tool_calls,
        deadline_ms: deadline,
        terminal: terminal
            .map(|value| {
                serde_json::from_str(&value)
                    .map_err(|error| RuntimeError::Protocol(error.to_string()))
            })
            .transpose()?,
        result_state,
        reservation_owner: owner,
        revision,
        reservation_released_at_ms: released,
        reservation_expires_at_ms: expires,
        content,
        error_code,
        error_message,
    }))
}
