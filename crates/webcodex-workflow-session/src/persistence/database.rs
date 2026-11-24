//! SQLite storage for Workflow Session state and independently indexed history.
//! Transactions contain changed Sessions only; events are append/upsert deltas.
//! The database is authoritative backing for clean cache entries, never a replay
//! engine. Loading history cannot dispatch a tool or change Session authority.
use super::*;
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use std::path::Path;
use std::sync::Mutex;

#[derive(Debug)]
pub(crate) struct SessionDatabase {
    connection: Mutex<Connection>,
}

pub(crate) struct SessionWrite {
    pub revision: u64,
    pub expected_revision: Option<u64>,
    pub previous_message_revision: u64,
    pub record: PersistedSessionRecord,
    pub events: Vec<Arc<SessionEvent>>,
}

/// Only a confirmed rollback may be retried. A revision/identity violation
/// or a commit whose result cannot be verified terminates the Writer.
#[derive(Debug, Clone)]
pub(crate) enum SessionWriteFailure {
    RolledBack(String),
    Fatal(String),
}

impl SessionWriteFailure {
    pub(crate) fn detail(&self) -> &str {
        match self {
            Self::RolledBack(detail) | Self::Fatal(detail) => detail,
        }
    }

    pub(crate) fn is_fatal(&self) -> bool {
        matches!(self, Self::Fatal(_))
    }
}

fn error(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}

mod migration;
pub use migration::migrate_fork_session_schema;

/// Both formats used version 1; the column layout, not the version alone,
/// determines the read-only format. Unknown, partial and mixed layouts fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowSessionSchema {
    Absent,
    ForkV1,
    CurrentV1,
}

const FORK_TABLES: &[(&str, &[&str])] = &[
    (
        "workflow_session_storage",
        &["id", "schema_version", "legacy_imported"],
    ),
    (
        "workflow_session_state",
        &[
            "session_id",
            "project",
            "owner_authority_fingerprint",
            "created_at",
            "updated_at",
            "revision",
            "previous_message_revision",
            "message_observation_revision",
            "events_observed",
            "state_json",
        ],
    ),
    (
        "workflow_session_events",
        &["session_id", "ordinal", "event_id", "event_json"],
    ),
    (
        "workflow_session_messages",
        &[
            "session_id",
            "message_id",
            "observation_revision",
            "message_json",
        ],
    ),
    (
        "workflow_session_message_delivery_replays",
        &[
            "session_id",
            "scope_key",
            "payload_fingerprint",
            "message_id",
        ],
    ),
    (
        "workflow_session_retention_tombstones",
        &[
            "session_id",
            "owner_authority_fingerprint",
            "incarnation_fingerprint",
            "expiry_ordinal",
        ],
    ),
];

const CURRENT_TABLES: &[(&str, &[&str])] = &[
    (
        "workflow_session_storage",
        &["singleton", "version", "imported"],
    ),
    (
        "workflow_sessions",
        &[
            "session_id",
            "project",
            "owner",
            "lifecycle",
            "created_at",
            "updated_at",
            "revision",
            "state_json",
        ],
    ),
    (
        "workflow_session_events",
        &[
            "sequence",
            "event_id",
            "session_id",
            "kind",
            "call_id",
            "ordinal",
            "payload",
        ],
    ),
    (
        "workflow_session_messages",
        &[
            "message_id",
            "session_id",
            "revision",
            "payload",
            "assignment_floor",
            "completion_fence",
        ],
    ),
    (
        "workflow_session_deliveries",
        &["session_id", "scope_key", "message_id", "fingerprint"],
    ),
    ("workflow_session_tombstones", &["session_id", "payload"]),
];

pub fn detect_workflow_session_schema(
    connection: &Connection,
) -> io::Result<WorkflowSessionSchema> {
    let mut statement = connection.prepare(
        "SELECT name,type FROM sqlite_master WHERE (lower(name)='workflow_sessions' OR lower(name) GLOB 'workflow_session_*' OR lower(name) GLOB 'wc_session_migration_*') AND type IN ('table','view')",
    ).map_err(io::Error::other)?;
    let mut names = HashSet::new();
    let mut rows = statement.query([]).map_err(io::Error::other)?;
    while let Some(row) = rows.next().map_err(io::Error::other)? {
        let name: String = row.get(0).map_err(io::Error::other)?;
        let kind: String = row.get(1).map_err(io::Error::other)?;
        if kind != "table"
            || name
                .to_ascii_lowercase()
                .starts_with("wc_session_migration_")
        {
            return Err(error("unexpected Workflow Session schema object"));
        }
        names.insert(name);
    }
    if names.is_empty() {
        return Ok(WorkflowSessionSchema::Absent);
    }
    let marker = table_columns(connection, "workflow_session_storage")?;
    let (schema, required, marker_sql) = if marker.contains("schema_version") {
        (
            WorkflowSessionSchema::ForkV1,
            FORK_TABLES,
            "SELECT id,schema_version,legacy_imported FROM workflow_session_storage",
        )
    } else {
        (
            WorkflowSessionSchema::CurrentV1,
            CURRENT_TABLES,
            "SELECT singleton,version,imported FROM workflow_session_storage",
        )
    };
    if names.len() != required.len() {
        return Err(error("incomplete or mixed Workflow Session schema"));
    }
    for &(table, columns) in required {
        if !names.contains(table) {
            return Err(error("incomplete Workflow Session schema"));
        }
        let actual = table_columns(connection, table)?;
        if actual.len() != columns.len() || columns.iter().any(|column| !actual.contains(*column)) {
            return Err(error(format!(
                "unsupported Workflow Session schema for {table}"
            )));
        }
    }
    validate_schema_keys(connection, schema)?;
    let mut stmt = connection.prepare(marker_sql).map_err(io::Error::other)?;
    let mut rows = stmt.query([]).map_err(io::Error::other)?;
    let row = rows
        .next()
        .map_err(io::Error::other)?
        .ok_or_else(|| error("missing Workflow Session marker"))?;
    let id: i64 = row.get(0).map_err(io::Error::other)?;
    let version: i64 = row.get(1).map_err(io::Error::other)?;
    let imported: i64 = row.get(2).map_err(io::Error::other)?;
    if id != 1
        || version != 1
        || !matches!(imported, 0 | 1)
        || rows.next().map_err(io::Error::other)?.is_some()
    {
        return Err(error("unsupported Workflow Session database marker"));
    }
    Ok(schema)
}

pub fn require_current_workflow_session_schema(connection: &Connection) -> io::Result<()> {
    match detect_workflow_session_schema(connection)? {
        WorkflowSessionSchema::Absent | WorkflowSessionSchema::CurrentV1 => Ok(()),
        WorkflowSessionSchema::ForkV1 => Err(error(
            "legacy Fork Workflow Session schema: stop Server and explicitly run the offline migration with a verified backup",
        )),
    }
}

fn table_columns(
    connection: &Connection,
    table: &str,
) -> io::Result<std::collections::HashSet<String>> {
    // Only the fixed domain table names above are supplied here.
    let mut statement = connection
        .prepare(&format!("PRAGMA table_xinfo({table})"))
        .map_err(io::Error::other)?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(io::Error::other)?;
    rows.collect::<Result<_, _>>().map_err(io::Error::other)
}

/// Verify the identity constraints relied on by migration and the current
/// Writer, not only column names. Secondary performance indexes are not format
/// discriminators; primary and unique identity keys are.
fn validate_schema_keys(connection: &Connection, schema: WorkflowSessionSchema) -> io::Result<()> {
    let keys: &[(&str, &[&str])] = match schema {
        WorkflowSessionSchema::ForkV1 => &[
            ("workflow_session_storage", &["id"]),
            ("workflow_session_state", &["session_id"]),
            ("workflow_session_events", &["session_id", "ordinal"]),
            ("workflow_session_messages", &["session_id", "message_id"]),
            (
                "workflow_session_message_delivery_replays",
                &["session_id", "scope_key"],
            ),
            ("workflow_session_retention_tombstones", &["session_id"]),
        ],
        WorkflowSessionSchema::CurrentV1 => &[
            ("workflow_session_storage", &["singleton"]),
            ("workflow_sessions", &["session_id"]),
            ("workflow_session_events", &["sequence"]),
            ("workflow_session_messages", &["message_id"]),
            ("workflow_session_deliveries", &["session_id", "scope_key"]),
            ("workflow_session_tombstones", &["session_id"]),
        ],
        WorkflowSessionSchema::Absent => return Ok(()),
    };
    let mut stmt = connection
        .prepare("SELECT name FROM pragma_table_info(?1) WHERE pk>0 ORDER BY pk")
        .map_err(io::Error::other)?;
    for &(table, expected) in keys {
        let actual = stmt
            .query_map([table], |r| r.get::<_, String>(0))
            .map_err(io::Error::other)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(io::Error::other)?;
        if !actual
            .iter()
            .map(String::as_str)
            .eq(expected.iter().copied())
        {
            return Err(error(format!(
                "unsupported Workflow Session primary key for {table}"
            )));
        }
    }
    let mut indexes = connection.prepare(
        "SELECT name FROM pragma_index_list('workflow_session_events') WHERE [unique]=1 AND partial=0"
    ).map_err(io::Error::other)?;
    let mut columns = connection
        .prepare("SELECT name FROM pragma_index_info(?1) ORDER BY seqno")
        .map_err(io::Error::other)?;
    let mut names = indexes.query([]).map_err(io::Error::other)?;
    let mut unique_event_id = false;
    let mut unique_ordinal = schema == WorkflowSessionSchema::ForkV1;
    while let Some(row) = names.next().map_err(io::Error::other)? {
        let name: String = row.get(0).map_err(io::Error::other)?;
        let key = columns
            .query_map([name], |r| r.get::<_, Option<String>>(0))
            .map_err(io::Error::other)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(io::Error::other)?;
        unique_event_id |= key.iter().map(Option::as_deref).eq([Some("event_id")]);
        unique_ordinal |= key
            .iter()
            .map(Option::as_deref)
            .eq([Some("session_id"), Some("ordinal")]);
    }
    if !unique_event_id || !unique_ordinal {
        return Err(error(
            "missing Workflow Session event identity uniqueness constraint",
        ));
    }
    Ok(())
}

fn initialize_current_schema(connection: &Connection) -> io::Result<()> {
    connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS workflow_session_storage (
                 singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL,
                 imported INTEGER NOT NULL DEFAULT 0
             );
             INSERT OR IGNORE INTO workflow_session_storage(singleton,version) VALUES(1,1);
             CREATE TABLE IF NOT EXISTS workflow_sessions (
                 session_id TEXT PRIMARY KEY, project TEXT, owner TEXT NOT NULL,
                 lifecycle TEXT NOT NULL CHECK(lifecycle IN ('active','closed')),
                 created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
                 revision INTEGER NOT NULL, state_json TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS workflow_sessions_recent ON workflow_sessions(project,updated_at DESC,session_id DESC);
             CREATE INDEX IF NOT EXISTS workflow_sessions_owner ON workflow_sessions(owner,project,updated_at DESC,session_id DESC);
             CREATE TABLE IF NOT EXISTS workflow_session_events (
                 sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                 event_id TEXT NOT NULL UNIQUE, session_id TEXT NOT NULL REFERENCES workflow_sessions(session_id),
                 kind TEXT NOT NULL, call_id TEXT, ordinal INTEGER NOT NULL, payload TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS workflow_session_events_recent ON workflow_session_events(session_id,sequence DESC);
             CREATE INDEX IF NOT EXISTS workflow_session_events_kind ON workflow_session_events(session_id,kind,ordinal DESC);
             CREATE UNIQUE INDEX IF NOT EXISTS workflow_session_events_order ON workflow_session_events(session_id,ordinal);
             CREATE INDEX IF NOT EXISTS workflow_session_events_call ON workflow_session_events(session_id,call_id,ordinal DESC) WHERE kind IN ('tool_call_started','tool_call_finished');
             CREATE TABLE IF NOT EXISTS workflow_session_messages (
                 message_id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES workflow_sessions(session_id),
                 revision INTEGER NOT NULL, payload TEXT NOT NULL, assignment_floor INTEGER, completion_fence TEXT
             );
             CREATE INDEX IF NOT EXISTS workflow_session_messages_revision ON workflow_session_messages(session_id,revision,message_id);
             CREATE TABLE IF NOT EXISTS workflow_session_deliveries (
                 session_id TEXT NOT NULL REFERENCES workflow_sessions(session_id), scope_key TEXT NOT NULL,
                 message_id TEXT NOT NULL REFERENCES workflow_session_messages(message_id), fingerprint TEXT NOT NULL,
                 PRIMARY KEY(session_id,scope_key)
             );
             CREATE TABLE IF NOT EXISTS workflow_session_tombstones (
                 session_id TEXT PRIMARY KEY, payload TEXT NOT NULL
             );",
        ).map_err(error)?;
    Ok(())
}

impl SessionDatabase {
    pub(crate) fn cold(self: &Arc<Self>, write: &SessionWrite) -> io::Result<ColdSessionRecord> {
        let state = state_only(&write.record);
        let mut cold = cold_session_from_persisted(&state, None).map_err(error)?;
        cold.database = Some(Arc::clone(self));
        cold.database_revision = write.revision;
        Ok(cold)
    }
    pub(crate) fn open(path: &Path) -> io::Result<Arc<Self>> {
        if let Some(parent) = path.parent().filter(|path| !path.as_os_str().is_empty()) {
            fs::create_dir_all(parent)?;
        }
        let mut connection = Connection::open(path).map_err(error)?;
        require_current_workflow_session_schema(&connection)?;
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=FULL;
             PRAGMA foreign_keys=ON;
             PRAGMA busy_timeout=5000;
             PRAGMA cache_size=-4096;",
            )
            .map_err(error)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(error)?;
        // Schema and version marker publish atomically, so a failed first open
        // cannot leave a marker pointing at a partial Session domain.
        require_current_workflow_session_schema(&transaction)?;
        initialize_current_schema(&transaction)?;
        if detect_workflow_session_schema(&transaction)? != WorkflowSessionSchema::CurrentV1 {
            return Err(error(
                "Session schema initialization did not produce the current layout",
            ));
        }
        transaction.commit().map_err(error)?;
        Ok(Arc::new(Self {
            connection: Mutex::new(connection),
        }))
    }

    pub(crate) fn write(&self, writes: &[SessionWrite]) -> Result<u64, SessionWriteFailure> {
        use SessionWriteFailure::{Fatal, RolledBack};
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| Fatal("Session database connection poisoned".into()))?;
        if !connection.is_autocommit() {
            return Err(Fatal(
                "Session database transaction state uncertain before write".into(),
            ));
        }
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|err| RolledBack(err.to_string()))?;
        let mut bytes = 0_u64;
        for write in writes {
            match write_row(&transaction, write) {
                Ok(written) => bytes = bytes.saturating_add(written),
                Err(step) => {
                    let rollback = transaction.rollback();
                    if rollback.is_err() || !connection.is_autocommit() {
                        return Err(Fatal(
                            "Session database rollback could not be confirmed".into(),
                        ));
                    }
                    // SQL errors with confirmed rollback may be recovered by a
                    // later barrier; logical identity/revision violations may not.
                    let sqlite_step = step
                        .get_ref()
                        .is_some_and(|source| source.is::<rusqlite::Error>());
                    return Err(if sqlite_step {
                        RolledBack(step.to_string())
                    } else {
                        Fatal(step.to_string())
                    });
                }
            }
        }
        match transaction.commit() {
            Ok(()) => Ok(bytes),
            Err(commit) => {
                // SQLite may roll back on commit failure. Do not infer that
                // from the error alone: every immutable revision must still be
                // exactly its pre-batch value, and no transaction may remain.
                let unchanged = connection.is_autocommit()
                    && writes.iter().all(|write| {
                        let actual = connection
                            .query_row(
                                "SELECT revision FROM workflow_sessions WHERE session_id=?1",
                                [&write.record.session_id],
                                |row| row.get::<_, i64>(0),
                            )
                            .optional()
                            .ok();
                        let expected = write.expected_revision.map(|revision| revision as i64);
                        actual == Some(expected)
                    });
                if unchanged {
                    Err(RolledBack(commit.to_string()))
                } else {
                    Err(Fatal(
                        "Session database commit outcome could not be confirmed".into(),
                    ))
                }
            }
        }
    }

    pub(crate) fn inventory(self: &Arc<Self>) -> io::Result<Vec<ColdSessionRecord>> {
        let connection = self.connection.lock().map_err(error)?;
        let overlap: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM workflow_sessions s JOIN workflow_session_tombstones t ON s.session_id=t.session_id)",
            [], |row| row.get(0),
        ).map_err(error)?;
        if overlap {
            return Err(error("Session state overlaps a retention tombstone"));
        }
        let mut statement = connection.prepare("SELECT revision,state_json,session_id,project,owner,lifecycle,created_at,updated_at FROM workflow_sessions ORDER BY updated_at,session_id").map_err(error)?;
        let mut rows = statement.query([]).map_err(error)?;
        let mut result = Vec::new();
        while let Some(row) = rows.next().map_err(error)? {
            let (revision, record) = state_from_row(row).map_err(error)?;
            let mut cold = cold_session_from_persisted(&record, None).map_err(error)?;
            cold.database = Some(Arc::clone(self));
            cold.database_revision = revision;
            result.push(cold);
        }
        Ok(result)
    }

    #[cfg(any(test, feature = "root-test-support"))]
    pub(crate) fn fail_writes(&self, fail: bool) {
        self.connection
            .lock()
            .unwrap()
            .pragma_update(None, "query_only", fail)
            .unwrap();
    }

    /// Scan exact retained call evidence only when the recent window cannot
    /// prove recovery safety. SQLite orders the committed rows by identity,
    /// so the scan holds one pairing plus the uncommitted overlay, not the
    /// complete Session history. A changed locator is never treated as safe.
    pub(crate) fn has_unclosed_mutating_calls(
        &self,
        cold: &ColdSessionRecord,
        record: &SessionRecord,
    ) -> io::Result<bool> {
        use crate::continuation::{
            mutating_start_like, retained_call_identity, RetainedCallIdentity, RetainedCallPairing,
        };
        let mut connection = self.connection.lock().map_err(error)?;
        let transaction = connection.transaction().map_err(error)?;
        let RetainedHistoryOverlay {
            mut updates,
            appended,
            ..
        } = retained_history_overlay(&transaction, cold, record)?;
        updates.retain(|_, event| {
            matches!(
                event.kind.as_str(),
                "tool_call_started" | "tool_call_finished"
            )
        });
        let mut pending_pairings =
            std::collections::HashMap::<RetainedCallIdentity, RetainedCallPairing>::new();
        for event in appended.iter().rev() {
            let Some(identity) = retained_call_identity(event) else {
                if mutating_start_like(event) {
                    return Ok(true);
                }
                continue;
            };
            if pending_pairings
                .entry(identity)
                .or_default()
                .observe_reverse(event)
            {
                return Ok(true);
            }
        }
        let mut statement = transaction.prepare(
            "SELECT payload,event_id,kind,call_id FROM workflow_session_events
             WHERE session_id=?1 AND kind IN ('tool_call_started','tool_call_finished')
             ORDER BY call_id COLLATE BINARY,
               COALESCE(json_extract(payload,'$.resolved_project'),json_extract(payload,'$.project')) COLLATE BINARY,
               json_extract(payload,'$.tool_name') COLLATE BINARY,
               json_extract(payload,'$.logical_invocation_id') COLLATE BINARY,
               json_extract(payload,'$.logical_invocation_role') COLLATE BINARY, ordinal DESC",
        ).map_err(error)?;
        let mut rows = statement.query([&record.session_id]).map_err(error)?;
        let mut current: Option<RetainedCallIdentity> = None;
        let mut pairing = RetainedCallPairing::default();
        while let Some(row) = rows.next().map_err(error)? {
            let payload: String = row.get(0).map_err(error)?;
            let event_id: String = row.get(1).map_err(error)?;
            let kind: String = row.get(2).map_err(error)?;
            let call_id: Option<String> = row.get(3).map_err(error)?;
            let event: SessionEvent = serde_json::from_str(&payload).map_err(error)?;
            if event.session_id != record.session_id
                || event.event_id != event_id
                || event.kind != kind
                || event.call_id != call_id
            {
                return Err(error("Session recovery event identity mismatch"));
            }
            let event = updates.remove(&event_id).unwrap_or_else(|| Arc::new(event));
            let Some(identity) = retained_call_identity(&event) else {
                if mutating_start_like(&event) {
                    return Ok(true);
                }
                continue;
            };
            if current.as_ref() != Some(&identity) {
                if current
                    .as_ref()
                    .is_some_and(|previous| previous >= &identity)
                {
                    return Err(error("Session recovery call order mismatch"));
                }
                pairing = pending_pairings.remove(&identity).unwrap_or_default();
                current = Some(identity);
            }
            if pairing.observe_reverse(&event) {
                return Ok(true);
            }
        }
        if !updates.is_empty() {
            return Err(error("Session recovery event overlay mismatch"));
        }
        Ok(false)
    }

    /// Fetch one retained event for a late evidence update. The existing
    /// Session revision and authority must match; no history window expansion.
    pub(crate) fn retained_event_for_update(
        &self,
        cold: &ColdSessionRecord,
        event_id: Option<&str>,
    ) -> io::Result<Option<Arc<SessionEvent>>> {
        let mut connection = self.connection.lock().map_err(error)?;
        let transaction = connection.transaction().map_err(error)?;
        let (revision, record) = transaction.query_row(
            "SELECT revision,state_json,session_id,project,owner,lifecycle,created_at,updated_at FROM workflow_sessions WHERE session_id=?1",
            [&cold.session_id], state_from_row,
        ).map_err(error)?;
        if revision != cold.database_revision
            || record.session_id != cold.session_id
            || record.project != cold.project
            || record.project_identity != cold.project_identity
            || record.owner_authority_fingerprint != cold.owner_authority_fingerprint
            || record.created_at != cold.created_at
        {
            return Err(error("Session retained event locator changed"));
        }
        let decode = |row: &rusqlite::Row<'_>| -> rusqlite::Result<_> {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, i64>(4)?,
            ))
        };
        let found = match event_id {
            Some(event_id) => transaction.query_row(
                "SELECT payload,event_id,kind,call_id,ordinal FROM workflow_session_events WHERE session_id=?1 AND event_id=?2",
                params![cold.session_id, event_id], decode,
            ).optional().map_err(error)?,
            None => transaction.query_row(
                "SELECT payload,event_id,kind,call_id,ordinal FROM workflow_session_events WHERE session_id=?1 AND kind='session_closed' ORDER BY ordinal DESC LIMIT 1",
                [&cold.session_id], decode,
            ).optional().map_err(error)?,
        };
        let Some((payload, id, kind, call_id, ordinal)) = found else {
            return Ok(None);
        };
        let event: SessionEvent = serde_json::from_str(&payload).map_err(error)?;
        let ordinal = u64::try_from(ordinal).map_err(error)?;
        if event.session_id != cold.session_id
            || event.event_id != id
            || event.kind != kind
            || event.call_id != call_id
            || ordinal < record.event_history_start
            || ordinal >= record.events_observed
            || event_id.is_some_and(|expected| expected != event.event_id)
        {
            return Err(error("Session retained event identity mismatch"));
        }
        let event = sanitize_persisted_event(event, &cold.session_id)
            .ok_or_else(|| error("Session retained event failed validation"))?;
        Ok(Some(Arc::new(event)))
    }

    /// Exact instruction identity at the caller's event-count snapshot,
    /// including uncommitted instructions that have left the recent tail.
    pub(crate) fn instruction_at(
        &self,
        cold: &ColdSessionRecord,
        record: &SessionRecord,
        through: usize,
    ) -> io::Result<Option<String>> {
        let mut connection = self.connection.lock().map_err(error)?;
        let transaction = connection.transaction().map_err(error)?;
        let overlay = retained_history_overlay(&transaction, cold, record)?;
        let through = u64::try_from(through).map_err(error)?;
        if through > record.events_observed {
            return Err(error("Session instruction snapshot is in the future"));
        }
        let pending_count = through.saturating_sub(overlay.committed_observed);
        if let Some(event) = overlay
            .appended
            .iter()
            .take(usize::try_from(pending_count).map_err(error)?)
            .rev()
            .find(|event| event.kind == "task_instruction")
        {
            return Ok(Some(event.event_id.clone()));
        }
        let limit = through.min(overlay.committed_observed);
        let found: Option<(String, String)> = transaction.query_row(
            "SELECT event_id,payload FROM workflow_session_events WHERE session_id=?1 AND kind='task_instruction' AND ordinal<?2 ORDER BY ordinal DESC LIMIT 1",
            params![record.session_id, i64::try_from(limit).map_err(error)?],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).optional().map_err(error)?;
        if let Some((id, payload)) = found {
            let event: SessionEvent = serde_json::from_str(&payload).map_err(error)?;
            if event.session_id != record.session_id
                || event.event_id != id
                || event.kind != "task_instruction"
            {
                return Err(error("Session instruction history identity mismatch"));
            }
            Ok(Some(id))
        } else {
            Ok(None)
        }
    }

    /// Stream canonical finished events with at most two committed events
    /// buffered per logical invocation. An ambiguous group (>2) contributes
    /// every member, exactly like the canonical in-memory projection.
    pub(crate) fn visit_retained_finished_events(
        &self,
        cold: &ColdSessionRecord,
        record: &SessionRecord,
        mut visit: impl FnMut(&SessionEvent),
    ) -> io::Result<()> {
        use crate::events::{
            canonical_tool_call_finished_event_refs, is_valid_logical_invocation_id,
            is_valid_logical_invocation_role,
        };
        let mut connection = self.connection.lock().map_err(error)?;
        let transaction = connection.transaction().map_err(error)?;
        let RetainedHistoryOverlay {
            mut updates,
            appended,
            ..
        } = retained_history_overlay(&transaction, cold, record)?;
        updates.retain(|_, event| event.kind == "tool_call_finished");
        let correlation = |event: &SessionEvent| {
            event
                .logical_invocation_id
                .as_ref()
                .filter(|id| {
                    is_valid_logical_invocation_id(id)
                        && event
                            .logical_invocation_role
                            .as_deref()
                            .is_some_and(is_valid_logical_invocation_role)
                })
                .cloned()
        };
        let mut pending = BTreeMap::<String, Vec<Arc<SessionEvent>>>::new();
        for event in appended
            .into_iter()
            .filter(|event| event.kind == "tool_call_finished")
        {
            if let Some(id) = correlation(&event) {
                pending.entry(id).or_default().push(event);
            } else {
                visit(&event);
            }
        }
        // Groups are ordered by their exact correlation id. Invalid correlation
        // metadata is not eligible for suppression and is visited individually.
        let mut statement = transaction
            .prepare(
                "SELECT payload,event_id,kind,call_id FROM workflow_session_events
             WHERE session_id=?1 AND kind='tool_call_finished'
             ORDER BY json_extract(payload,'$.logical_invocation_id') COLLATE BINARY, ordinal",
            )
            .map_err(error)?;
        let mut rows = statement.query([&record.session_id]).map_err(error)?;
        let mut current: Option<String> = None;
        let mut group = Vec::<Arc<SessionEvent>>::new();
        let mut ambiguous = false;
        while let Some(row) = rows.next().map_err(error)? {
            let payload: String = row.get(0).map_err(error)?;
            let id: String = row.get(1).map_err(error)?;
            let kind: String = row.get(2).map_err(error)?;
            let call_id: Option<String> = row.get(3).map_err(error)?;
            let event: SessionEvent = serde_json::from_str(&payload).map_err(error)?;
            if event.session_id != record.session_id
                || event.event_id != id
                || event.kind != kind
                || event.call_id != call_id
            {
                return Err(error("Session finished event identity mismatch"));
            }
            let event = updates.remove(&id).unwrap_or_else(|| Arc::new(event));
            let Some(correlation_id) = correlation(&event) else {
                visit(&event);
                continue;
            };
            if current.as_ref() != Some(&correlation_id) {
                for event in canonical_tool_call_finished_event_refs(group.iter().map(Arc::as_ref))
                {
                    visit(event);
                }
                group.clear();
                ambiguous = false;
                if current
                    .as_ref()
                    .is_some_and(|previous| previous >= &correlation_id)
                {
                    return Err(error("Session finished event correlation order mismatch"));
                }
                current = Some(correlation_id.clone());
                group = pending.remove(&correlation_id).unwrap_or_default();
            }
            if ambiguous {
                visit(&event);
            } else {
                group.push(event);
                if group.len() > 2 {
                    for event in group.drain(..) {
                        visit(&event);
                    }
                    ambiguous = true;
                }
            }
        }
        for event in canonical_tool_call_finished_event_refs(group.iter().map(Arc::as_ref)) {
            visit(event);
        }
        for group in pending.into_values() {
            for event in canonical_tool_call_finished_event_refs(group.iter().map(Arc::as_ref)) {
                visit(event);
            }
        }
        if !updates.is_empty() {
            return Err(error("Session finished event overlay mismatch"));
        }
        Ok(())
    }

    pub(crate) fn tombstones(&self) -> io::Result<Vec<SessionRetentionTombstone>> {
        let connection = self.connection.lock().map_err(error)?;
        let mut statement = connection.prepare("SELECT session_id,payload FROM workflow_session_tombstones WHERE session_id NOT IN (SELECT session_id FROM workflow_sessions) ORDER BY session_id").map_err(error)?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(error)?;
        rows.map(|row| {
            let (id, payload) = row.map_err(error)?;
            let value: SessionRetentionTombstone = serde_json::from_str(&payload).map_err(error)?;
            if value.session_id != id
                || !is_valid_session_id(&value.session_id)
                || !is_lower_hex_sha256(&value.owner_authority_fingerprint)
                || !is_lower_hex_sha256(&value.incarnation_fingerprint)
            {
                return Err(error("invalid Session tombstone"));
            }
            Ok(value)
        })
        .collect()
    }

    pub(crate) fn load(
        &self,
        cold: &ColdSessionRecord,
        recent_limit: usize,
    ) -> io::Result<PersistedSessionRecord> {
        let mut connection = self.connection.lock().map_err(error)?;
        let transaction = connection.transaction().map_err(error)?;
        let tombstoned: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM workflow_session_tombstones WHERE session_id=?1)",
                [&cold.session_id],
                |row| row.get(0),
            )
            .map_err(error)?;
        if tombstoned {
            return Err(error("retention-tombstoned Session cannot be loaded"));
        }
        let found = transaction.query_row(
            "SELECT revision,state_json,session_id,project,owner,lifecycle,created_at,updated_at FROM workflow_sessions WHERE session_id=?1",
            [&cold.session_id], state_from_row,
        ).optional().map_err(error)?;
        let (revision, mut record) =
            found.ok_or_else(|| error("Session database row unavailable"))?;
        if revision != cold.database_revision {
            return Err(error("Session database revision changed"));
        }
        if record.session_id != cold.session_id
            || record.project != cold.project
            || record.owner_authority_fingerprint != cold.owner_authority_fingerprint
            || record.project_identity != cold.project_identity
            || record.created_at != cold.created_at
        {
            return Err(error("Session database identity changed"));
        }
        record.events = read_events(
            &transaction,
            &cold.session_id,
            recent_limit,
            record.event_history_start,
            record.events_observed,
        )?;
        let mut statement = transaction.prepare(
            "SELECT payload,revision,assignment_floor,completion_fence,message_id FROM workflow_session_messages WHERE session_id=?1 ORDER BY rowid",
        ).map_err(error)?;
        let mut rows = statement.query([&cold.session_id]).map_err(error)?;
        while let Some(row) = rows.next().map_err(error)? {
            let payload: String = row.get(0).map_err(error)?;
            let message: SessionMessage = serde_json::from_str(&payload).map_err(error)?;
            if message.session_id != record.session_id
                || message.message_id != row.get::<_, String>(4).map_err(error)?
            {
                return Err(error("Session message identity mismatch"));
            }
            let revision: i64 = row.get(1).map_err(error)?;
            if u64::try_from(revision).map_err(error)? > record.message_observation_revision {
                return Err(error("Session message revision mismatch"));
            }
            record.message_observation_revisions.insert(
                message.message_id.clone(),
                u64::try_from(revision).map_err(error)?,
            );
            if let Some(floor) = row.get::<_, Option<i64>>(2).map_err(error)? {
                record.assignment_history_floors.insert(
                    message.message_id.clone(),
                    u64::try_from(floor).map_err(error)?,
                );
            }
            if let Some(fence) = row.get::<_, Option<String>>(3).map_err(error)? {
                record
                    .completion_assignment_fence_fingerprints
                    .insert(message.message_id.clone(), fence);
            }
            record.messages.push(Arc::new(message));
        }
        drop(rows);
        drop(statement);
        let mut statement = transaction.prepare("SELECT scope_key,message_id,fingerprint FROM workflow_session_deliveries WHERE session_id=?1").map_err(error)?;
        let mut rows = statement.query([&cold.session_id]).map_err(error)?;
        while let Some(row) = rows.next().map_err(error)? {
            record.message_delivery_replays.insert(
                row.get(0).map_err(error)?,
                super::super::model::SessionMessageDeliveryReplay {
                    message_id: row.get(1).map_err(error)?,
                    payload_fingerprint: row.get(2).map_err(error)?,
                },
            );
        }
        drop(rows);
        drop(statement);
        transaction.commit().map_err(error)?;
        // SQLite rows are persisted input, so use the same privacy/replay validation boundary.
        let history_start = record.event_history_start;
        let evidence_counts = (
            record.events.len(),
            record.messages.len(),
            record.message_delivery_replays.len(),
            record.completion_assignment_fence_fingerprints.len(),
        );
        let mut validated = record
            .into_record(recent_limit)
            .ok_or_else(|| error("invalid Workflow Session persisted state"))?;
        if evidence_counts
            != (
                validated.events.len(),
                validated.messages.len(),
                validated.message_delivery_replays.len(),
                validated.completion_assignment_fence_fingerprints.len(),
            )
        {
            return Err(error("invalid Session evidence or replay metadata"));
        }
        validated.history_is_durable = true;
        validated.event_history_start = history_start;
        Ok(PersistedSessionRecord::from_record(
            &validated,
            recent_limit,
        ))
    }

    /// Import once, with an all-or-nothing marker in the same transaction. The
    /// source is never overwritten or treated as a second live persistence path.
    pub(crate) fn import_legacy(&self, path: &Path) -> io::Result<()> {
        let mut connection = self.connection.lock().map_err(error)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(error)?;
        let imported: bool = transaction
            .query_row(
                "SELECT imported FROM workflow_session_storage WHERE singleton=1",
                [],
                |r| r.get(0),
            )
            .map_err(error)?;
        if imported {
            return Ok(());
        }
        let occupied: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM workflow_sessions) OR EXISTS(SELECT 1 FROM workflow_session_tombstones)",
            [], |r| r.get(0),
        ).map_err(error)?;
        if occupied {
            return Err(error("unimported Session database already contains history; refusing ambiguous legacy import"));
        }
        let mut tombstone_ordinals = HashSet::new();
        match fs::File::open(path) {
            Ok(file) => {
                super::stream::visit(io::BufReader::new(file), |row| {
                    match row {
                        super::stream::ImportRow::Session(record) => {
                            let events = record.events.clone();
                            write_row(&transaction, &SessionWrite { revision: 1, expected_revision: None, previous_message_revision: 0, record, events }).map(|_| ())
                        }
                        super::stream::ImportRow::Tombstone(tombstone) => {
                            if tombstone.expiry_ordinal == 0 || !tombstone_ordinals.insert(tombstone.expiry_ordinal)
                                || transaction.query_row("SELECT EXISTS(SELECT 1 FROM workflow_sessions WHERE session_id=?1)", [&tombstone.session_id], |r| r.get::<_, bool>(0)).map_err(error)?
                            {
                                return Err(error("overlapping or invalid legacy Session tombstone"));
                            }
                            transaction.execute("INSERT INTO workflow_session_tombstones(session_id,payload) VALUES(?1,?2)", params![tombstone.session_id, serde_json::to_string(&tombstone).map_err(error)?]).map_err(error)?;
                            Ok(())
                        }
                    }
                }).map_err(error)?;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        transaction
            .execute(
                "UPDATE workflow_session_storage SET imported=1 WHERE singleton=1",
                [],
            )
            .map_err(error)?;
        transaction.commit().map_err(error)
    }
}

struct RetainedHistoryOverlay {
    committed_observed: u64,
    updates: std::collections::HashMap<String, Arc<SessionEvent>>,
    appended: Vec<Arc<SessionEvent>>,
}

fn retained_history_overlay(
    transaction: &Connection,
    cold: &ColdSessionRecord,
    record: &SessionRecord,
) -> io::Result<RetainedHistoryOverlay> {
    use crate::continuation::retained_call_identity;
    let (revision, committed) = transaction.query_row(
        "SELECT revision,state_json,session_id,project,owner,lifecycle,created_at,updated_at FROM workflow_sessions WHERE session_id=?1",
        [&record.session_id], state_from_row,
    ).map_err(error)?;
    if revision != cold.database_revision
        || committed.session_id != record.session_id
        || committed.project != record.project
        || committed.project_identity != record.project_identity
        || committed.owner_authority_fingerprint != record.owner_authority_fingerprint
        || committed.created_at != record.created_at
        || committed.events_observed != cold.retained_events_observed
        || committed.event_history_start != record.event_history_start
        || committed.events_observed > record.events_observed
    {
        return Err(error("Session recovery history locator changed"));
    }
    let (count, first, last): (i64, Option<i64>, Option<i64>) = transaction.query_row(
        "SELECT count(*),min(ordinal),max(ordinal) FROM workflow_session_events WHERE session_id=?1",
        [&record.session_id], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    ).map_err(error)?;
    let retained = committed
        .events_observed
        .saturating_sub(committed.event_history_start);
    if u64::try_from(count).map_err(error)? != retained
        || (retained == 0 && (first.is_some() || last.is_some()))
        || (retained > 0
            && (first != Some(i64::try_from(committed.event_history_start).map_err(error)?)
                || last != Some(i64::try_from(committed.events_observed - 1).map_err(error)?)))
    {
        return Err(error("Session recovery history ordinal gap"));
    }

    let mut updates = std::collections::HashMap::<String, Arc<SessionEvent>>::new();
    let mut appended = Vec::<Arc<SessionEvent>>::new();
    let mut lookup = transaction
        .prepare(
            "SELECT session_id,kind,call_id,payload FROM workflow_session_events WHERE event_id=?1",
        )
        .map_err(error)?;
    for event in super::deduplicated_pending_events(record) {
        if event.session_id != record.session_id {
            return Err(error("pending Session event identity mismatch"));
        }
        let existing = lookup
            .query_row([&event.event_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .optional()
            .map_err(error)?;
        if let Some((session, kind, call_id, payload)) = existing {
            let previous: SessionEvent = serde_json::from_str(&payload).map_err(error)?;
            if session != event.session_id
                || kind != event.kind
                || call_id != event.call_id
                || previous.event_id != event.event_id
                || previous.session_id != session
                || previous.kind != kind
                || previous.call_id != call_id
                || retained_call_identity(&previous) != retained_call_identity(&event)
                || previous.write_like != event.write_like
                || previous.shell_like != event.shell_like
            {
                return Err(error("pending Session event changed recovery identity"));
            }
            updates.insert(event.event_id.clone(), event);
        } else {
            appended.push(event);
        }
    }
    drop(lookup);
    if committed.events_observed.checked_add(appended.len() as u64) != Some(record.events_observed)
    {
        return Err(error("Session recovery pending event count mismatch"));
    }

    Ok(RetainedHistoryOverlay {
        committed_observed: committed.events_observed,
        updates,
        appended,
    })
}

fn read_events(
    connection: &Connection,
    session_id: &str,
    limit: usize,
    history_start: u64,
    observed: u64,
) -> io::Result<Vec<Arc<SessionEvent>>> {
    let count = observed
        .checked_sub(history_start)
        .ok_or_else(|| error("Session event history waterline invalid"))?
        .min(limit.max(1).min(i64::MAX as usize) as u64);
    let first = observed - count;
    let mut statement = connection.prepare(
        "SELECT payload,event_id,kind,call_id,ordinal FROM (SELECT ordinal,payload,event_id,kind,call_id FROM workflow_session_events WHERE session_id=?1 ORDER BY ordinal DESC LIMIT ?2) ORDER BY ordinal",
    ).map_err(error)?;
    let mut rows = statement
        .query(params![session_id, i64::try_from(count).map_err(error)?])
        .map_err(error)?;
    let mut events = Vec::new();
    while let Some(row) = rows.next().map_err(error)? {
        let payload: String = row.get(0).map_err(error)?;
        let id: String = row.get(1).map_err(error)?;
        let kind: String = row.get(2).map_err(error)?;
        let call_id: Option<String> = row.get(3).map_err(error)?;
        let ordinal = u64::try_from(row.get::<_, i64>(4).map_err(error)?).map_err(error)?;
        let event: SessionEvent = serde_json::from_str(&payload).map_err(error)?;
        if event.session_id != session_id
            || event.event_id != id
            || event.kind != kind
            || event.call_id != call_id
            || ordinal != first + events.len() as u64
        {
            return Err(error("Session event identity or ordinal mismatch"));
        }
        events.push(Arc::new(event));
    }
    if events.len() as u64 != count {
        return Err(error("Session recent history row count mismatch"));
    }
    Ok(events)
}

fn write_row(connection: &Connection, write: &SessionWrite) -> io::Result<u64> {
    // Serialize the typed state with history detached. Never rewrite another
    // Session or encode all historical events to append one activity.
    let state = state_only(&write.record);
    let state = serde_json::to_string(&state).map_err(io::Error::other)?;
    let record = &write.record;
    if !valid_persisted_project_identity(
        record.project.as_deref(),
        record.project_identity.as_ref(),
    ) || !is_valid_session_id(&record.session_id)
        || !is_lower_hex_sha256(&record.owner_authority_fingerprint)
        || write.revision == 0
        || record.event_history_start > record.events_observed
    {
        return Err(error("invalid Session write state"));
    }
    let tombstoned: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM workflow_session_tombstones WHERE session_id=?1)",
            [&record.session_id],
            |row| row.get(0),
        )
        .map_err(io::Error::other)?;
    if tombstoned {
        return Err(error("retention-tombstoned Session cannot be written"));
    }
    let actual = connection.query_row(
        "SELECT revision,state_json,session_id,project,owner,lifecycle,created_at,updated_at FROM workflow_sessions WHERE session_id=?1",
        [&record.session_id], state_from_row,
    ).optional().map_err(io::Error::other)?;
    if actual.as_ref().map(|(revision, _)| *revision) != write.expected_revision {
        return Err(error("Session database revision conflict"));
    }
    if let Some((revision, old)) = actual {
        if write.revision <= revision
            || old.project != record.project
            || old.project_identity != record.project_identity
            || old.owner_authority_fingerprint != record.owner_authority_fingerprint
            || old.created_at != record.created_at
            || (!old.lifecycle.allows_mutation() && record.lifecycle.allows_mutation())
            || old.message_observation_revision != write.previous_message_revision
            || old.message_observation_revision > record.message_observation_revision
            || old.events_observed > record.events_observed
            || old.event_history_start != record.event_history_start
        {
            return Err(error(
                "Session durable identity, lifecycle or revision changed",
            ));
        }
    } else if write.previous_message_revision != 0 {
        return Err(error(
            "new Session has an unexpected prior message revision",
        ));
    }
    connection.execute(
        "INSERT INTO workflow_sessions(session_id,project,owner,lifecycle,created_at,updated_at,revision,state_json)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8)
         ON CONFLICT(session_id) DO UPDATE SET lifecycle=excluded.lifecycle,updated_at=excluded.updated_at,
             revision=excluded.revision,state_json=excluded.state_json
         WHERE workflow_sessions.owner=excluded.owner AND workflow_sessions.project IS excluded.project
             AND workflow_sessions.created_at=excluded.created_at",
        params![record.session_id,record.project,record.owner_authority_fingerprint,record.lifecycle.as_str(),record.created_at,record.updated_at,i64::try_from(write.revision).map_err(io::Error::other)?,state],
    ).map_err(io::Error::other)?;
    if connection.changes() != 1 {
        return Err(error("Session identity mismatch"));
    }
    let mut bytes = state.len() as u64;
    let last: Option<i64> = connection
        .query_row(
            "SELECT max(ordinal) FROM workflow_session_events WHERE session_id=?1",
            [&record.session_id],
            |row| row.get(0),
        )
        .map_err(io::Error::other)?;
    let mut ordinal = match last {
        Some(last) => last
            .checked_add(1)
            .ok_or_else(|| error("Session event sequence exhausted"))?,
        None => i64::try_from(record.event_history_start).map_err(io::Error::other)?,
    };
    for event in &write.events {
        if event.session_id != record.session_id {
            return Err(error("Session event identity mismatch"));
        }
        let existing: Option<(String, i64, String, Option<String>, String)> = connection
            .query_row(
                "SELECT session_id,ordinal,kind,call_id,payload FROM workflow_session_events WHERE event_id=?1",
                [&event.event_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )
            .optional()
            .map_err(io::Error::other)?;
        if let Some((id, _, kind, call_id, payload)) = &existing {
            let previous: SessionEvent = serde_json::from_str(payload).map_err(io::Error::other)?;
            if id != &record.session_id
                || kind != &event.kind
                || call_id != &event.call_id
                || previous.event_id != event.event_id
                || previous.session_id != event.session_id
                || previous.kind != event.kind
                || previous.call_id != event.call_id
                || crate::continuation::retained_call_identity(&previous)
                    != crate::continuation::retained_call_identity(event)
                || previous.write_like != event.write_like
                || previous.shell_like != event.shell_like
            {
                return Err(error("event identity collision"));
            }
        }
        let event_ordinal = existing
            .as_ref()
            .map_or(ordinal, |(_, ordinal, _, _, _)| *ordinal);
        if existing.is_none() {
            ordinal = ordinal
                .checked_add(1)
                .ok_or_else(|| error("Session event sequence exhausted"))?;
        }
        let payload = serde_json::to_string(event).map_err(io::Error::other)?;
        connection.execute(
            "INSERT INTO workflow_session_events(event_id,session_id,kind,call_id,payload,ordinal) VALUES(?1,?2,?3,?4,?5,?6)
             ON CONFLICT(event_id) DO UPDATE SET payload=excluded.payload WHERE workflow_session_events.session_id=excluded.session_id AND workflow_session_events.payload<>excluded.payload",
            params![event.event_id,event.session_id,event.kind,event.call_id,payload,event_ordinal],
        ).map_err(io::Error::other)?;
        if connection.changes() > 0 {
            bytes = bytes.saturating_add(payload.len() as u64);
        }
    }
    if u64::try_from(ordinal).map_err(io::Error::other)? != record.events_observed {
        return Err(error(
            "Session event ordinal does not match durable observation waterline",
        ));
    }
    for message in &record.messages {
        let revision = record
            .message_observation_revisions
            .get(&message.message_id)
            .copied()
            .unwrap_or(0);
        if write.expected_revision.is_some() && revision <= write.previous_message_revision {
            continue;
        }
        if message.session_id != record.session_id {
            return Err(error("Session message identity mismatch"));
        }
        let existing: Option<String> = connection
            .query_row(
                "SELECT session_id FROM workflow_session_messages WHERE message_id=?1",
                [&message.message_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(io::Error::other)?;
        if existing.as_ref().is_some_and(|id| id != &record.session_id) {
            return Err(error("Session message id collision"));
        }
        let payload = serde_json::to_string(message).map_err(io::Error::other)?;
        connection.execute(
            "INSERT INTO workflow_session_messages(message_id,session_id,revision,payload,assignment_floor,completion_fence) VALUES(?1,?2,?3,?4,?5,?6)
             ON CONFLICT(message_id) DO UPDATE SET revision=excluded.revision,payload=excluded.payload,assignment_floor=excluded.assignment_floor,completion_fence=excluded.completion_fence
             WHERE workflow_session_messages.session_id=excluded.session_id AND (workflow_session_messages.revision<>excluded.revision OR workflow_session_messages.payload<>excluded.payload OR workflow_session_messages.assignment_floor IS NOT excluded.assignment_floor OR workflow_session_messages.completion_fence IS NOT excluded.completion_fence)",
            params![message.message_id,record.session_id,i64::try_from(revision).map_err(io::Error::other)?,payload, record.assignment_history_floors.get(&message.message_id).copied().map(i64::try_from).transpose().map_err(io::Error::other)?, record.completion_assignment_fence_fingerprints.get(&message.message_id)],
        ).map_err(io::Error::other)?;
        if connection.changes() > 0 {
            bytes = bytes.saturating_add(payload.len() as u64);
        }
    }
    for (scope, replay) in &record.message_delivery_replays {
        if write.expected_revision.is_some()
            && record
                .message_observation_revisions
                .get(&replay.message_id)
                .copied()
                .unwrap_or(0)
                <= write.previous_message_revision
        {
            continue;
        }
        connection.execute("INSERT INTO workflow_session_deliveries(session_id,scope_key,message_id,fingerprint) VALUES(?1,?2,?3,?4) ON CONFLICT(session_id,scope_key) DO NOTHING", params![record.session_id,scope,replay.message_id,replay.payload_fingerprint]).map_err(io::Error::other)?;
        let matches: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM workflow_session_deliveries d JOIN workflow_session_messages m ON m.message_id=d.message_id WHERE d.session_id=?1 AND d.scope_key=?2 AND d.message_id=?3 AND d.fingerprint=?4 AND m.session_id=d.session_id)", params![record.session_id,scope,replay.message_id,replay.payload_fingerprint], |row| row.get(0)).map_err(io::Error::other)?;
        if !matches {
            return Err(error("Session delivery identity conflict"));
        }
    }
    Ok(bytes)
}

fn state_only(record: &PersistedSessionRecord) -> PersistedSessionRecord {
    let mut state = record.clone();
    state.events.clear();
    state.messages.clear();
    state.message_delivery_replays.clear();
    state.message_observation_revisions.clear();
    state.assignment_history_floors.clear();
    state.completion_assignment_fence_fingerprints.clear();
    state
}

// Indexed authority and JSON projections must agree. Never admit a second
// identity, or accidentally reintroduce an embedded history in current-state RAM.
fn state_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<(u64, PersistedSessionRecord)> {
    let decode = || -> io::Result<_> {
        let revision = u64::try_from(row.get::<_, i64>(0).map_err(error)?).map_err(error)?;
        let state: String = row.get(1).map_err(error)?;
        let record: PersistedSessionRecord = serde_json::from_str(&state).map_err(|_| {
            error("invalid current Session state or unsupported historical Project identity")
        })?;
        if revision == 0
            || record.session_id != row.get::<_, String>(2).map_err(error)?
            || record.project != row.get::<_, Option<String>>(3).map_err(error)?
            || record.owner_authority_fingerprint != row.get::<_, String>(4).map_err(error)?
            || !valid_persisted_project_identity(
                record.project.as_deref(),
                record.project_identity.as_ref(),
            )
            || record.lifecycle.as_str() != row.get::<_, String>(5).map_err(error)?
            || record.created_at != row.get::<_, i64>(6).map_err(error)?
            || record.updated_at != row.get::<_, i64>(7).map_err(error)?
            || !is_valid_session_id(&record.session_id)
            || !is_lower_hex_sha256(&record.owner_authority_fingerprint)
            || record.event_history_start > record.events_observed
            || !record.events.is_empty()
            || !record.messages.is_empty()
            || !record.message_delivery_replays.is_empty()
            || !record.message_observation_revisions.is_empty()
            || !record.assignment_history_floors.is_empty()
            || !record.completion_assignment_fence_fingerprints.is_empty()
        {
            return Err(error("invalid normalized Session state"));
        }
        Ok((revision, record))
    };
    decode().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Text, Box::new(error))
    })
}
