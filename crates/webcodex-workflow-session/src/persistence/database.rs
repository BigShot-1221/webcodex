//! Additive SQLite Workflow Session persistence (Phase 2 batch contract).
//!
//! `SessionDatabase` is opened, restored, and seeded on the Stage E production
//! startup path (`SessionStore::with_persistence_database_limits`); SQLite is
//! the production durable backend. The module stays crate-private to this crate.
//! Legacy `sessions.json` is used only for one-time `import_legacy` and by JSON
//! compatibility constructors — not for steady-state production writes.
//!
//! **Backups** must include committed WAL state (`-wal` / `-shm` siblings or a
//! checkpoint). Copying only the main `webcodex.db` file can miss recent commits.
//!
//! **Retirement revision CAS:** `SessionDatabaseRetirement::expected_revision` is
//! enforced with a single-row delete that matches identity columns and the durable
//! `revision` counter. Concurrent writers must lose the race and fail closed.
//!
//! **Store/Writer acknowledgement:** After a successful durable write for a Session,
//! the Store must advance any in-flight retirement's `expected_revision` for that
//! Session to the newly committed revision before the retirement batch is admitted.
//! Otherwise a write acknowledgement followed by retirement would fail the CAS even
//! when the retire authority still matches the live row.
use std::collections::{BTreeMap, HashMap, HashSet};
use std::error::Error;
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde_json::Value;

use crate::events::is_valid_session_id;
use crate::incarnation::workflow_session_incarnation_fingerprint;
use crate::model::{
    PersistedSessionRecord, SessionEvent, SessionMessage, SessionMessageDeliveryReplay,
    SessionProjectIdentity, SessionRetentionTombstone, SessionRecord, EVENT_ID_PREFIX,
    DEFAULT_MAX_EVENTS_PER_SESSION, DEFAULT_MAX_MESSAGES_PER_SESSION,
};

use super::is_lower_hex_sha256;

fn sqlite_i64(value: u64) -> io::Result<i64> {
    i64::try_from(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("SQLite integer overflow for persisted u64 value {value}"),
        )
    })
}

fn row_u64(row: &rusqlite::Row<'_>, column: usize) -> rusqlite::Result<u64> {
    let value = row.get::<_, i64>(column)?;
    u64::try_from(value).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(column, value))
}

fn sqlite_u64(value: i64) -> io::Result<u64> {
    u64::try_from(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("SQLite integer underflow for persisted i64 value {value}"),
        )
    })
}

/// Supported `workflow_session_storage.schema_version`. Fail closed when the
/// on-disk marker version does not match this constant exactly.
pub(super) const WORKFLOW_SESSION_STORAGE_SCHEMA_VERSION: u32 = 1;

/// Default on-disk revision for a row first admitted from the legacy JSON ledger.
pub(in crate::persistence) const LEGACY_IMPORTED_SESSION_REVISION: u64 = 1;

/// Compact locator for one Session's current durable state without event/message history.
#[derive(Debug, Clone)]
pub(crate) struct SessionDatabaseRecord {
    pub revision: u64,
    /// Committed message-observation high-water mark from durable storage.
    pub previous_message_revision: u64,
    pub state: PersistedSessionRecord,
}

/// One optimistic transactional Session mutation.
#[derive(Debug, Clone)]
pub(crate) struct SessionDatabaseWrite {
    pub revision: u64,
    pub expected_revision: Option<u64>,
    /// Previous committed message-observation watermark used to filter durable deltas.
    pub previous_message_revision: u64,
    pub record: PersistedSessionRecord,
    /// Retained-event upserts (append or in-place field updates) for this write.
    /// `sync_events` applies these via `upsert_retained_event`; full message/replay
    /// state lives in `record`.
    pub appended_events: Vec<Arc<SessionEvent>>,
}

/// Immutable identity evidence required to retire one Session durably.
#[derive(Debug, Clone)]
pub(crate) struct SessionDatabaseRetirement {
    pub session_id: String,
    pub created_at: i64,
    pub project: Option<String>,
    pub owner_authority_fingerprint: String,
    pub project_identity: Option<SessionProjectIdentity>,
    pub expected_revision: Option<u64>,
    pub retention_tombstone: Option<SessionRetentionTombstone>,
}

/// Tombstone evidence required to delete a pruned retention tombstone row.
#[derive(Debug, Clone)]
pub(crate) struct SessionDatabaseTombstoneDelete {
    pub session_id: String,
    pub owner_authority_fingerprint: String,
    pub incarnation_fingerprint: String,
}

/// One durable removal of an evicted delivery-replay scope key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionDatabaseDeliveryReplayDeletion {
    pub session_id: String,
    pub scope_key: String,
}

/// One transactional batch suitable for the Fork writer's SQLite flush loop.
#[derive(Debug, Default, Clone)]
pub(crate) struct SessionDatabaseBatch {
    pub writes: Vec<SessionDatabaseWrite>,
    pub retirements: Vec<SessionDatabaseRetirement>,
    pub tombstone_upserts: Vec<SessionRetentionTombstone>,
    pub tombstone_deletes: Vec<SessionDatabaseTombstoneDelete>,
    pub delivery_replay_deletions: Vec<SessionDatabaseDeliveryReplayDeletion>,
}

impl SessionDatabaseBatch {
    pub(crate) fn is_empty(&self) -> bool {
        self.writes.is_empty()
            && self.retirements.is_empty()
            && self.tombstone_upserts.is_empty()
            && self.tombstone_deletes.is_empty()
            && self.delivery_replay_deletions.is_empty()
    }
}

/// Writer-facing `apply_batch` outcome with rusqlite error codes preserved for
/// transient-vs-permanent classification (Stage G).
#[derive(Debug)]
pub(crate) enum ApplyBatchError {
    ConnectionMutexPoisoned,
    Validation(io::Error),
    Transient(rusqlite::Error),
    Permanent(rusqlite::Error),
    /// In-transaction SQL step failed with bounded transient SQLite lock contention.
    TransientStep(io::Error),
    PermanentStep(io::Error),
}

const DURABLE_STEP_SQLITE_SOURCE_MAX_DEPTH: usize = 8;

fn durable_step_sqlite_source(io_err: &io::Error) -> Option<&rusqlite::Error> {
    let mut current: Option<&dyn Error> = Some(io_err);
    for _ in 0..DURABLE_STEP_SQLITE_SOURCE_MAX_DEPTH {
        let err = current?;
        if let Some(sqlite) = err.downcast_ref::<rusqlite::Error>() {
            return Some(sqlite);
        }
        current = err.source();
    }
    None
}

fn map_durable_step<T>(result: io::Result<T>) -> Result<T, ApplyBatchError> {
    result.map_err(|io_err| {
        if let Some(sqlite_err) = durable_step_sqlite_source(&io_err) {
            if rusqlite_error_is_transient_lock(sqlite_err) {
                return ApplyBatchError::TransientStep(io_err);
            }
        }
        ApplyBatchError::PermanentStep(io_err)
    })
}

pub(crate) fn rusqlite_error_is_transient_lock(err: &rusqlite::Error) -> bool {
    match err {
        rusqlite::Error::SqliteFailure(failure, _) => {
            matches!(
                failure.code,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
            ) || matches!(
                failure.extended_code,
                rusqlite::ffi::SQLITE_BUSY
                    | rusqlite::ffi::SQLITE_BUSY_SNAPSHOT
                    | rusqlite::ffi::SQLITE_BUSY_RECOVERY
                    | rusqlite::ffi::SQLITE_BUSY_TIMEOUT
                    | rusqlite::ffi::SQLITE_LOCKED
                    | rusqlite::ffi::SQLITE_LOCKED_SHAREDCACHE
            )
        }
        _ => false,
    }
}

pub(crate) fn summarize_rusqlite_error(err: &rusqlite::Error) -> String {
    match err {
        rusqlite::Error::SqliteFailure(failure, message) => {
            let detail = message.as_deref().unwrap_or("");
            format!(
                "sqlite: {:?} (extended={}): {detail}",
                failure.code, failure.extended_code
            )
        }
        other => other.to_string(),
    }
}

fn classify_rusqlite_error(err: rusqlite::Error) -> ApplyBatchError {
    if rusqlite_error_is_transient_lock(&err) {
        ApplyBatchError::Transient(err)
    } else {
        ApplyBatchError::Permanent(err)
    }
}

fn flush_connection_transaction_state_uncertain(phase: &str) -> ApplyBatchError {
    ApplyBatchError::PermanentStep(io::Error::other(format!(
        "workflow session SQLite connection transaction state uncertain or poisoned after {phase}"
    )))
}

fn flush_batch_durable_steps(
    tx: &Transaction<'_>,
    batch: &SessionDatabaseBatch,
) -> Result<u64, ApplyBatchError> {
    for delete in &batch.tombstone_deletes {
        map_durable_step(apply_tombstone_delete(tx, delete))?;
    }
    for deletion in &batch.delivery_replay_deletions {
        map_durable_step(apply_delivery_replay_deletion(tx, deletion))?;
    }
    for retirement in &batch.retirements {
        map_durable_step(apply_retirement(tx, retirement))?;
    }
    let mut committed_revision = 0_u64;
    for write in &batch.writes {
        map_durable_step(apply_write(tx, write))?;
        committed_revision = committed_revision.max(write.revision);
    }
    for tombstone in &batch.tombstone_upserts {
        map_durable_step(ensure_no_live_state_row(tx, &tombstone.session_id))?;
        map_durable_step(upsert_retention_tombstone(tx, tombstone))?;
    }
    Ok(committed_revision)
}

fn flush_step_failure_outcome(
    conn: &Connection,
    step_err: ApplyBatchError,
) -> Result<u64, ApplyBatchError> {
    if !conn.is_autocommit() {
        return Err(flush_connection_transaction_state_uncertain(
            "explicit rollback following durable step failure",
        ));
    }
    Err(step_err)
}

fn flush_commit_failure_outcome(
    conn: &Connection,
    commit_err: rusqlite::Error,
) -> Result<u64, ApplyBatchError> {
    if !conn.is_autocommit() {
        return Err(flush_connection_transaction_state_uncertain(
            "transaction drop rollback following commit failure",
        ));
    }
    Err(classify_rusqlite_error(commit_err))
}

#[derive(Debug)]
pub(crate) struct SessionDatabase {
    connection: Mutex<Connection>,
}

impl SessionDatabase {
    pub(crate) fn open(path: &Path) -> io::Result<Self> {
        if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        let connection = Connection::open(path).map_err(|err| io::Error::other(err))?;
        preflight_schema_version(&connection)?;
        connection
            .execute_batch(
                "
                PRAGMA journal_mode = WAL;
                PRAGMA synchronous = FULL;
                PRAGMA foreign_keys = ON;
                PRAGMA busy_timeout = 5000;
                PRAGMA cache_size = -2048;
                ",
            )
            .map_err(|err| io::Error::other(err))?;
        let db = Self {
            connection: Mutex::new(connection),
        };
        db.initialize_schema()?;
        Ok(db)
    }

    pub(crate) fn write_batch(
        &self,
        writes: &[SessionDatabaseWrite],
        tombstones: &[SessionRetentionTombstone],
    ) -> io::Result<u64> {
        self.apply_batch(&SessionDatabaseBatch {
            writes: writes.to_vec(),
            tombstone_upserts: tombstones.to_vec(),
            ..SessionDatabaseBatch::default()
        })
    }

    /// Applies one Phase-2 batch in a single immediate SQLite transaction.
    ///
    /// Ordering: tombstone deletes, delivery-replay deletes, retirements, writes,
    /// then standalone tombstone upserts.
    pub(crate) fn apply_batch(&self, batch: &SessionDatabaseBatch) -> io::Result<u64> {
        if batch.is_empty() {
            return Ok(0);
        }
        validate_batch(batch)?;
        let mut conn = self.lock_connection()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|err| io::Error::other(err))?;
        for delete in &batch.tombstone_deletes {
            apply_tombstone_delete(&tx, delete)?;
        }
        for deletion in &batch.delivery_replay_deletions {
            apply_delivery_replay_deletion(&tx, deletion)?;
        }
        for retirement in &batch.retirements {
            apply_retirement(&tx, retirement)?;
        }
        let mut committed_revision = 0_u64;
        for write in &batch.writes {
            apply_write(&tx, write)?;
            committed_revision = committed_revision.max(write.revision);
        }
        for tombstone in &batch.tombstone_upserts {
            ensure_no_live_state_row(&tx, &tombstone.session_id)?;
            upsert_retention_tombstone(&tx, tombstone)?;
        }
        tx.commit().map_err(|err| io::Error::other(err))?;
        Ok(committed_revision)
    }

    /// Classified batch apply for the background writer — preserves rusqlite codes
    /// for bounded transient retry vs permanent writer-fatal handling.
    pub(crate) fn apply_batch_for_flush(
        &self,
        batch: &SessionDatabaseBatch,
    ) -> Result<u64, ApplyBatchError> {
        if batch.is_empty() {
            return Ok(0);
        }
        validate_batch(batch).map_err(ApplyBatchError::Validation)?;
        let mut conn = self
            .lock_connection()
            .map_err(|_| ApplyBatchError::ConnectionMutexPoisoned)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(classify_rusqlite_error)?;
        match flush_batch_durable_steps(&tx, batch) {
            Ok(committed_revision) => match tx.commit() {
                Ok(()) => Ok(committed_revision),
                Err(commit_err) => flush_commit_failure_outcome(&conn, commit_err),
            },
            Err(step_err) => {
                let _ = tx.rollback();
                flush_step_failure_outcome(&conn, step_err)
            }
        }
    }

    pub(crate) fn inventory(&self) -> io::Result<Vec<SessionDatabaseRecord>> {
        let conn = self.lock_connection()?;
        let mut stmt = conn
            .prepare(
                "SELECT session_id, project, owner_authority_fingerprint, created_at,
                        revision, previous_message_revision, state_json
                 FROM workflow_session_state
                 WHERE session_id NOT IN (
                     SELECT session_id FROM workflow_session_retention_tombstones
                 )
                 ORDER BY updated_at ASC, session_id ASC",
            )
            .map_err(|err| io::Error::other(err))?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row_u64(row, 4)?,
                    row_u64(row, 5)?,
                    row.get::<_, String>(6)?,
                ))
            })
            .map_err(|err| io::Error::other(err))?;
        let mut records = Vec::new();
        for row in rows {
            let (session_id, project, owner, created_at, revision, previous_message_revision, state_json) =
                row.map_err(|err| io::Error::other(err))?;
            let state = serde_json::from_str::<PersistedSessionRecord>(&state_json)
                .map_err(io::Error::other)?;
            validate_identity_columns(
                &session_id,
                project.as_deref(),
                &owner,
                created_at,
                revision,
                &state,
            )?;
            records.push(SessionDatabaseRecord {
                revision,
                previous_message_revision,
                state,
            });
        }
        Ok(records)
    }

    pub(crate) fn load(
        &self,
        row: &SessionDatabaseRecord,
        recent_event_limit: usize,
    ) -> io::Result<PersistedSessionRecord> {
        let mut conn = self.lock_connection()?;
        let tx = conn
            .transaction()
            .map_err(|err| io::Error::other(err))?;
        let tombstoned: bool = tx
            .query_row(
                "SELECT 1 FROM workflow_session_retention_tombstones WHERE session_id = ?1",
                params![row.state.session_id],
                |_| Ok(()),
            )
            .optional()
            .map_err(|err| io::Error::other(err))?
            .is_some();
        if tombstoned {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "Session {} is retention-tombstoned and must not be loaded",
                    row.state.session_id
                ),
            ));
        }
        let identity = tx
            .query_row(
                "SELECT session_id, project, owner_authority_fingerprint, created_at, revision,
                        previous_message_revision, message_observation_revision, events_observed,
                        state_json
                 FROM workflow_session_state
                 WHERE session_id = ?1",
                params![row.state.session_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                        row_u64(row, 4)?,
                        row_u64(row, 5)?,
                        row_u64(row, 6)?,
                        row_u64(row, 7)?,
                        row.get::<_, String>(8)?,
                    ))
                },
            )
            .optional()
            .map_err(|err| io::Error::other(err))?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Session {} is not present in SQLite state", row.state.session_id),
                )
            })?;
        let (
            session_id,
            project,
            owner,
            created_at,
            revision,
            previous_message_revision,
            message_observation_revision,
            events_observed,
            state_json,
        ) = identity;
        if revision != row.revision {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Session revision locator does not match SQLite state",
            ));
        }
        if message_observation_revision != row.previous_message_revision {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Session message-revision locator does not match SQLite committed watermark",
            ));
        }
        if previous_message_revision != message_observation_revision {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Session message-revision columns are inconsistent",
            ));
        }
        let mut record = serde_json::from_str::<PersistedSessionRecord>(&state_json)
            .map_err(io::Error::other)?;
        validate_identity_columns(
            &session_id,
            project.as_deref(),
            &owner,
            created_at,
            revision,
            &record,
        )?;
        if record.message_observation_revision != message_observation_revision
            || record.events_observed != events_observed
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Session state_json does not match indexed observation metadata",
            ));
        }
        record.events = load_recent_events(&tx, &session_id, recent_event_limit)?;
        let (messages, message_revisions) = load_messages_with_revisions(&tx, &session_id)?;
        validate_loaded_message_revisions(&record, &message_revisions)?;
        record.messages = messages;
        let retained_message_ids: std::collections::HashSet<String> = record
            .messages
            .iter()
            .map(|message| message.message_id.clone())
            .collect();
        record.message_delivery_replays =
            load_delivery_replays(&tx, &session_id, &retained_message_ids)?;
        let sanitized = sanitize_loaded_record(record, recent_event_limit)?;
        tx.commit().map_err(|err| io::Error::other(err))?;
        Ok(sanitized)
    }

    pub(crate) fn tombstones(&self) -> io::Result<Vec<SessionRetentionTombstone>> {
        let conn = self.lock_connection()?;
        let mut stmt = conn
            .prepare(
                "SELECT session_id, owner_authority_fingerprint, incarnation_fingerprint,
                        expiry_ordinal
                 FROM workflow_session_retention_tombstones
                 ORDER BY expiry_ordinal ASC, session_id ASC",
            )
            .map_err(|err| io::Error::other(err))?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row_u64(row, 3)?,
                ))
            })
            .map_err(|err| io::Error::other(err))?;
        let mut tombstones = Vec::new();
        for row in rows {
            let (session_id, owner, incarnation, expiry_ordinal) =
                row.map_err(|err| io::Error::other(err))?;
            if !is_valid_session_id(&session_id)
                || !is_lower_hex_sha256(&owner)
                || !is_lower_hex_sha256(&incarnation)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "retention tombstone row has invalid identity evidence",
                ));
            }
            if expiry_ordinal == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "retention tombstone expiry ordinal must be positive",
                ));
            }
            tombstones.push(SessionRetentionTombstone {
                session_id,
                owner_authority_fingerprint: owner,
                incarnation_fingerprint: incarnation,
                expiry_ordinal,
            });
        }
        Ok(tombstones)
    }

    pub(crate) fn import_legacy(&self, path: &Path) -> io::Result<()> {
        super::legacy_import::import_legacy(self, path)
    }

    pub(in crate::persistence) fn lock_connection(&self) -> io::Result<MutexGuard<'_, Connection>> {
        self.connection
            .lock()
            .map_err(|_| io::Error::other("workflow session SQLite connection mutex poisoned"))
    }

    pub(in crate::persistence) fn legacy_imported_in_transaction(
        tx: &Transaction<'_>,
    ) -> io::Result<bool> {
        tx.query_row(
            "SELECT legacy_imported FROM workflow_session_storage WHERE id = 1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map(|value| value != 0)
        .map_err(|err| io::Error::other(err))
    }

    pub(in crate::persistence) fn mark_legacy_imported_in_transaction(
        tx: &Transaction<'_>,
    ) -> io::Result<()> {
        tx.execute(
            "UPDATE workflow_session_storage SET legacy_imported = 1 WHERE id = 1",
            [],
        )
        .map_err(|err| io::Error::other(err))?;
        Ok(())
    }

    fn initialize_schema(&self) -> io::Result<()> {
        let mut conn = self.lock_connection()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|err| io::Error::other(err))?;
        tx.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS workflow_session_storage (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                schema_version INTEGER NOT NULL,
                legacy_imported INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS workflow_session_state (
                session_id TEXT PRIMARY KEY,
                project TEXT,
                owner_authority_fingerprint TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                revision INTEGER NOT NULL,
                previous_message_revision INTEGER NOT NULL DEFAULT 0,
                message_observation_revision INTEGER NOT NULL,
                events_observed INTEGER NOT NULL,
                state_json TEXT NOT NULL,
                CHECK (revision > 0)
            );
            CREATE INDEX IF NOT EXISTS idx_workflow_session_state_updated
                ON workflow_session_state (updated_at, session_id);

            CREATE TABLE IF NOT EXISTS workflow_session_events (
                session_id TEXT NOT NULL
                    REFERENCES workflow_session_state(session_id) ON DELETE CASCADE,
                ordinal INTEGER NOT NULL,
                event_id TEXT NOT NULL,
                event_json TEXT NOT NULL,
                PRIMARY KEY (session_id, ordinal),
                UNIQUE (event_id)
            );
            CREATE INDEX IF NOT EXISTS idx_workflow_session_events_session_ordinal
                ON workflow_session_events (session_id, ordinal);

            CREATE TABLE IF NOT EXISTS workflow_session_messages (
                session_id TEXT NOT NULL
                    REFERENCES workflow_session_state(session_id) ON DELETE CASCADE,
                message_id TEXT NOT NULL,
                observation_revision INTEGER NOT NULL DEFAULT 0,
                message_json TEXT NOT NULL,
                PRIMARY KEY (session_id, message_id)
            );

            CREATE TABLE IF NOT EXISTS workflow_session_message_delivery_replays (
                session_id TEXT NOT NULL
                    REFERENCES workflow_session_state(session_id) ON DELETE CASCADE,
                scope_key TEXT NOT NULL,
                payload_fingerprint TEXT NOT NULL,
                message_id TEXT NOT NULL,
                PRIMARY KEY (session_id, scope_key),
                FOREIGN KEY (session_id, message_id)
                    REFERENCES workflow_session_messages(session_id, message_id)
            );

            CREATE TABLE IF NOT EXISTS workflow_session_retention_tombstones (
                session_id TEXT PRIMARY KEY,
                owner_authority_fingerprint TEXT NOT NULL,
                incarnation_fingerprint TEXT NOT NULL,
                expiry_ordinal INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_workflow_session_retention_tombstones_expiry
                ON workflow_session_retention_tombstones (expiry_ordinal);
            ",
        )
        .map_err(|err| io::Error::other(err))?;
        let marker_present: bool = tx
            .query_row(
                "SELECT 1 FROM workflow_session_storage WHERE id = 1",
                [],
                |_| Ok(()),
            )
            .optional()
            .map_err(|err| io::Error::other(err))?
            .is_some();
        if marker_present {
            let version: u32 = tx
                .query_row(
                    "SELECT schema_version FROM workflow_session_storage WHERE id = 1",
                    [],
                    |row| row.get(0),
                )
                .map_err(|err| io::Error::other(err))?;
            if version != WORKFLOW_SESSION_STORAGE_SCHEMA_VERSION {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!(
                        "workflow_session_storage schema version {version} is not supported (expected {})",
                        WORKFLOW_SESSION_STORAGE_SCHEMA_VERSION
                    ),
                ));
            }
        } else {
            tx.execute(
                "INSERT INTO workflow_session_storage (id, schema_version, legacy_imported)
                 VALUES (1, ?1, 0)",
                params![WORKFLOW_SESSION_STORAGE_SCHEMA_VERSION],
            )
            .map_err(|err| io::Error::other(err))?;
        }
        tx.commit().map_err(|err| io::Error::other(err))?;
        Ok(())
    }
}

fn preflight_schema_version(connection: &Connection) -> io::Result<()> {
    let marker_exists: bool = connection
        .query_row(
            "SELECT 1 FROM sqlite_master
             WHERE type = 'table' AND name = 'workflow_session_storage'",
            [],
            |_| Ok(()),
        )
        .optional()
        .map_err(|err| io::Error::other(err))?
        .is_some();
    if marker_exists {
        let version: u32 = connection
            .query_row(
                "SELECT schema_version FROM workflow_session_storage WHERE id = 1",
                [],
                |row| row.get(0),
            )
            .map_err(|err| io::Error::other(err))?;
        if version != WORKFLOW_SESSION_STORAGE_SCHEMA_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "workflow_session_storage schema version {version} is not supported (expected {})",
                    WORKFLOW_SESSION_STORAGE_SCHEMA_VERSION
                ),
            ));
        }
        return Ok(());
    }
    if partial_workflow_session_schema_without_marker(connection)? {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "partial Workflow Session SQLite schema without workflow_session_storage marker",
        ));
    }
    Ok(())
}

fn partial_workflow_session_schema_without_marker(connection: &Connection) -> io::Result<bool> {
    let count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type = 'table'
               AND name LIKE 'workflow_session_%'
               AND name != 'workflow_session_storage'",
            [],
            |row| row.get(0),
        )
        .map_err(|err| io::Error::other(err))?;
    Ok(count > 0)
}

fn validate_batch(batch: &SessionDatabaseBatch) -> io::Result<()> {
    use std::collections::HashSet;

    let mut write_ids = HashSet::new();
    for write in &batch.writes {
        if !write_ids.insert(write.record.session_id.as_str()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "write batch contains duplicate Session {}",
                    write.record.session_id
                ),
            ));
        }
    }
    let mut retirement_ids = HashSet::new();
    for retirement in &batch.retirements {
        if !retirement_ids.insert(retirement.session_id.as_str()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "write batch contains duplicate retirement for Session {}",
                    retirement.session_id
                ),
            ));
        }
        validate_retirement_evidence(retirement)?;
        if let Some(tombstone) = &retirement.retention_tombstone {
            validate_retirement_tombstone_identity(retirement, tombstone)?;
        }
    }
    let mut tombstone_upsert_ids = HashSet::new();
    for tombstone in &batch.tombstone_upserts {
        if !tombstone_upsert_ids.insert(tombstone.session_id.as_str()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "write batch contains duplicate tombstone upsert for Session {}",
                    tombstone.session_id
                ),
            ));
        }
    }
    let mut tombstone_delete_ids = HashSet::new();
    for delete in &batch.tombstone_deletes {
        if !tombstone_delete_ids.insert(delete.session_id.as_str()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "write batch contains duplicate tombstone delete for Session {}",
                    delete.session_id
                ),
            ));
        }
    }
    let mut replay_keys = HashSet::new();
    for deletion in &batch.delivery_replay_deletions {
        let key = (deletion.session_id.as_str(), deletion.scope_key.as_str());
        if !replay_keys.insert(key) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "write batch contains duplicate delivery-replay deletion for Session {} scope {}",
                    deletion.session_id, deletion.scope_key
                ),
            ));
        }
        if !is_valid_delivery_replay_scope(&deletion.scope_key) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "delivery-replay deletion scope {} is malformed",
                    deletion.scope_key
                ),
            ));
        }
    }

    for session_id in write_ids.iter() {
        if retirement_ids.contains(session_id) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "write batch cannot retire and mutate Session {session_id} in one transaction",
                ),
            ));
        }
    }

    let tombstone_deleted: HashSet<&str> = tombstone_delete_ids.clone();
    for session_id in &tombstone_upsert_ids {
        if write_ids.contains(session_id) && !tombstone_deleted.contains(session_id) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "write batch cannot upsert a retention tombstone and mutate live Session {session_id}",
                ),
            ));
        }
    }
    for session_id in &retirement_ids {
        if tombstone_upsert_ids.contains(session_id) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "write batch cannot retire Session {session_id} and upsert a duplicate tombstone row",
                ),
            ));
        }
        if tombstone_delete_ids.contains(session_id) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "write batch cannot retire Session {session_id} and delete its retention tombstone in one transaction",
                ),
            ));
        }
    }
    for deletion in &batch.delivery_replay_deletions {
        if retirement_ids.contains(deletion.session_id.as_str()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "write batch cannot retire Session {} and delete delivery-replay scopes in one transaction",
                    deletion.session_id
                ),
            ));
        }
    }

    Ok(())
}

/// Structural `SessionProjectIdentity` checks shared with legacy JSON import.
/// Legacy path-only rows deserialize but must not carry malformed partial fields.
pub(in crate::persistence) fn validate_project_identity(
    identity: &SessionProjectIdentity,
) -> io::Result<()> {
    if !SessionProjectIdentity::valid_registration_incarnation(
        identity.registration_incarnation.trim(),
    ) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "project_identity registration_incarnation is malformed".to_string(),
        ));
    }
    if let Some(root_fingerprint) = identity.root_fingerprint.as_deref() {
        if !root_fingerprint.is_empty()
            && !SessionProjectIdentity::valid_root_fingerprint(root_fingerprint)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "project_identity root_fingerprint is malformed".to_string(),
            ));
        }
    }
    if let Some(registered_path) = identity.registered_path.as_deref() {
        if registered_path.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "project_identity registered_path is empty".to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_retirement_evidence(retirement: &SessionDatabaseRetirement) -> io::Result<()> {
    if !is_valid_session_id(&retirement.session_id) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "Session {} retirement has malformed session id",
                retirement.session_id
            ),
        ));
    }
    if !is_lower_hex_sha256(&retirement.owner_authority_fingerprint) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "Session {} retirement has malformed owner authority fingerprint",
                retirement.session_id
            ),
        ));
    }
    if retirement.project_identity.is_some() && retirement.project.is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "Session {} retirement has project_identity without canonical project metadata",
                retirement.session_id
            ),
        ));
    }
    if let Some(identity) = &retirement.project_identity {
        validate_project_identity(identity)?;
    }
    Ok(())
}

fn validate_retirement_tombstone_identity(
    retirement: &SessionDatabaseRetirement,
    tombstone: &SessionRetentionTombstone,
) -> io::Result<()> {
    if tombstone.session_id != retirement.session_id
        || tombstone.owner_authority_fingerprint != retirement.owner_authority_fingerprint
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "retirement tombstone identity does not match retired Session evidence",
        ));
    }
    let expected_incarnation = workflow_session_incarnation_fingerprint(
        &retirement.session_id,
        retirement.created_at,
        retirement.project.as_deref(),
        &retirement.owner_authority_fingerprint,
    );
    if tombstone.incarnation_fingerprint != expected_incarnation {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "retirement tombstone incarnation does not match immutable Session evidence",
        ));
    }
    Ok(())
}

fn ensure_no_live_state_row(tx: &Transaction<'_>, session_id: &str) -> io::Result<()> {
    let exists: bool = tx
        .query_row(
            "SELECT 1 FROM workflow_session_state WHERE session_id = ?1",
            params![session_id],
            |_| Ok(()),
        )
        .optional()
        .map_err(|err| io::Error::other(err))?
        .is_some();
    if exists {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "retention tombstone for Session {session_id} cannot coexist with live SQLite state",
            ),
        ));
    }
    Ok(())
}

fn apply_tombstone_delete(tx: &Transaction<'_>, delete: &SessionDatabaseTombstoneDelete) -> io::Result<()> {
    let existing: Option<(String, String)> = tx
        .query_row(
            "SELECT owner_authority_fingerprint, incarnation_fingerprint
             FROM workflow_session_retention_tombstones WHERE session_id = ?1",
            params![delete.session_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|err| io::Error::other(err))?;
    if let Some((owner, incarnation)) = &existing {
        if owner != &delete.owner_authority_fingerprint
            || incarnation != &delete.incarnation_fingerprint
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "tombstone delete for Session {} conflicts with persisted identity",
                    delete.session_id
                ),
            ));
        }
    }
    if let Some((created_at, project, state_owner)) =
        load_state_identity_columns(tx, &delete.session_id)?
    {
        let state_incarnation = workflow_session_incarnation_fingerprint(
            &delete.session_id,
            created_at,
            project.as_deref(),
            &state_owner,
        );
        if state_incarnation == delete.incarnation_fingerprint
            && state_owner == delete.owner_authority_fingerprint
        {
            let changed = tx
                .execute(
                    "DELETE FROM workflow_session_state
                     WHERE session_id = ?1
                       AND owner_authority_fingerprint = ?2
                       AND created_at = ?3
                       AND COALESCE(project, '') = COALESCE(?4, '')",
                    params![
                        delete.session_id,
                        delete.owner_authority_fingerprint,
                        created_at,
                        project,
                    ],
                )
                .map_err(|err| io::Error::other(err))?;
            if changed > 1 {
                return Err(io::Error::other(
                    "stale Session state delete removed multiple rows",
                ));
            }
        }
    }
    if existing.is_some() {
        let changed = tx
            .execute(
                "DELETE FROM workflow_session_retention_tombstones
                 WHERE session_id = ?1
                   AND owner_authority_fingerprint = ?2
                   AND incarnation_fingerprint = ?3",
                params![
                    delete.session_id,
                    delete.owner_authority_fingerprint,
                    delete.incarnation_fingerprint,
                ],
            )
            .map_err(|err| io::Error::other(err))?;
        if changed != 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "tombstone delete for Session {} did not remove durable row",
                    delete.session_id
                ),
            ));
        }
    }
    Ok(())
}

fn load_state_identity_columns(
    tx: &Transaction<'_>,
    session_id: &str,
) -> io::Result<Option<(i64, Option<String>, String)>> {
    tx.query_row(
        "SELECT created_at, project, owner_authority_fingerprint
         FROM workflow_session_state WHERE session_id = ?1",
        params![session_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )
    .optional()
    .map_err(|err| io::Error::other(err))
}

fn apply_delivery_replay_deletion(
    tx: &Transaction<'_>,
    deletion: &SessionDatabaseDeliveryReplayDeletion,
) -> io::Result<()> {
    let changed = tx
        .execute(
            "DELETE FROM workflow_session_message_delivery_replays
             WHERE session_id = ?1 AND scope_key = ?2",
            params![deletion.session_id, deletion.scope_key],
        )
        .map_err(|err| io::Error::other(err))?;
    if changed > 1 {
        return Err(io::Error::other(
            "delivery-replay deletion removed multiple durable rows",
        ));
    }
    Ok(())
}

fn load_persisted_state_row(
    tx: &Transaction<'_>,
    session_id: &str,
) -> io::Result<Option<(PersistedSessionRecord, u64)>> {
    let row: Option<(i64, String)> = tx
        .query_row(
            "SELECT revision, state_json
             FROM workflow_session_state WHERE session_id = ?1",
            params![session_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|err| io::Error::other(err))?;
    let Some((revision_i64, state_json)) = row else {
        return Ok(None);
    };
    let state =
        serde_json::from_str::<PersistedSessionRecord>(&state_json).map_err(io::Error::other)?;
    let revision = sqlite_u64(revision_i64)?;
    Ok(Some((state, revision)))
}

fn assert_retirement_identity_matches(
    retirement: &SessionDatabaseRetirement,
    state: &PersistedSessionRecord,
) -> io::Result<()> {
    if state.session_id != retirement.session_id
        || state.created_at != retirement.created_at
        || state.owner_authority_fingerprint != retirement.owner_authority_fingerprint
        || state.project != retirement.project
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Session {} retirement identity does not match durable SQLite state",
                retirement.session_id
            ),
        ));
    }
    if state.project_identity != retirement.project_identity {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Session {} retirement project_identity does not match durable SQLite state",
                retirement.session_id
            ),
        ));
    }
    Ok(())
}

fn apply_retirement(tx: &Transaction<'_>, retirement: &SessionDatabaseRetirement) -> io::Result<()> {
    if let Some(expected) = retirement.expected_revision {
        let Some((state, revision)) = load_persisted_state_row(tx, &retirement.session_id)? else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Session {} retirement revision CAS failed at expected revision {expected}",
                    retirement.session_id
                ),
            ));
        };
        if revision != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Session {} retirement revision CAS failed at expected revision {expected}",
                    retirement.session_id
                ),
            ));
        }
        assert_retirement_identity_matches(retirement, &state)?;
        let expected_i64 = sqlite_i64(expected)?;
        let deleted = tx
            .execute(
                "DELETE FROM workflow_session_state
                 WHERE session_id = ?1
                   AND owner_authority_fingerprint = ?2
                   AND created_at = ?3
                   AND COALESCE(project, '') = COALESCE(?4, '')
                   AND revision = ?5",
                params![
                    retirement.session_id,
                    retirement.owner_authority_fingerprint,
                    retirement.created_at,
                    retirement.project,
                    expected_i64,
                ],
            )
            .map_err(|err| io::Error::other(err))?;
        if deleted != 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Session {} retirement revision CAS failed at expected revision {expected}",
                    retirement.session_id
                ),
            ));
        }
    } else if let Some((state, _revision)) = load_persisted_state_row(tx, &retirement.session_id)? {
        assert_retirement_identity_matches(retirement, &state)?;
        let changed = tx
            .execute(
                "DELETE FROM workflow_session_state
                 WHERE session_id = ?1
                   AND owner_authority_fingerprint = ?2
                   AND created_at = ?3
                   AND COALESCE(project, '') = COALESCE(?4, '')",
                params![
                    retirement.session_id,
                    retirement.owner_authority_fingerprint,
                    retirement.created_at,
                    retirement.project,
                ],
            )
            .map_err(|err| io::Error::other(err))?;
        if changed != 1 {
            return Err(io::Error::other(
                "Session retirement delete did not remove durable state row",
            ));
        }
    }
    if let Some(tombstone) = &retirement.retention_tombstone {
        upsert_retention_tombstone(tx, tombstone)?;
    }
    Ok(())
}

fn apply_write(tx: &Transaction<'_>, write: &SessionDatabaseWrite) -> io::Result<()> {
    if write.revision == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Session database revision must be positive",
        ));
    }
    if session_retention_tombstoned(tx, &write.record.session_id)? {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "Session {} is retention-tombstoned and cannot be written",
                write.record.session_id
            ),
        ));
    }
    match write.expected_revision {
        None => insert_state_row(tx, write)?,
        Some(expected) => update_state_row(tx, write, expected)?,
    }
    sync_events(
        tx,
        &write.record.session_id,
        write.record.events_observed,
        &write.record.events,
        &write.appended_events,
    )?;
    let initial_insert = write.expected_revision.is_none();
    sync_messages(
        tx,
        &write.record.session_id,
        &write.record.messages,
        &write.record.message_observation_revisions,
        initial_insert,
        write.previous_message_revision,
    )?;
    sync_delivery_replays(
        tx,
        &write.record.session_id,
        &write.record.message_delivery_replays,
        initial_insert,
        write.previous_message_revision,
        &write.record.message_observation_revisions,
    )?;
    Ok(())
}

fn session_retention_tombstoned(tx: &Transaction<'_>, session_id: &str) -> io::Result<bool> {
    tx.query_row(
        "SELECT 1 FROM workflow_session_retention_tombstones WHERE session_id = ?1",
        params![session_id],
        |_| Ok(()),
    )
    .optional()
    .map_err(|err| io::Error::other(err))
    .map(|row| row.is_some())
}

fn insert_state_row(tx: &Transaction<'_>, write: &SessionDatabaseWrite) -> io::Result<()> {
    if write.previous_message_revision != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "new Session write must supply previous_message_revision zero",
        ));
    }
    let record = &write.record;
    let exists: bool = tx
        .query_row(
            "SELECT 1 FROM workflow_session_state WHERE session_id = ?1",
            params![record.session_id],
            |_| Ok(()),
        )
        .optional()
        .map_err(|err| io::Error::other(err))?
        .is_some();
    if exists {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "Session {} already exists; expected_revision is required",
                record.session_id
            ),
        ));
    }
    let state_json = compact_state_json(record)?;
    let revision = sqlite_i64(write.revision)?;
    let message_observation_revision = sqlite_i64(record.message_observation_revision)?;
    let events_observed = sqlite_i64(record.events_observed)?;
    let changed = tx
        .execute(
            "INSERT INTO workflow_session_state (
                session_id, project, owner_authority_fingerprint, created_at, updated_at,
                revision, previous_message_revision, message_observation_revision, events_observed,
                state_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                record.session_id,
                record.project,
                record.owner_authority_fingerprint,
                record.created_at,
                record.updated_at,
                revision,
                message_observation_revision,
                message_observation_revision,
                events_observed,
                state_json,
            ],
        )
        .map_err(|err| io::Error::other(err))?;
    if changed != 1 {
        return Err(io::Error::other("failed to insert workflow session state row"));
    }
    Ok(())
}

fn update_state_row(
    tx: &Transaction<'_>,
    write: &SessionDatabaseWrite,
    expected_revision: u64,
) -> io::Result<()> {
    if write.revision <= expected_revision {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Session database revision must increase on update",
        ));
    }
    let record = &write.record;
    let expected_revision_i64 = sqlite_i64(expected_revision)?;
    let immutable = tx
        .query_row(
            "SELECT session_id, project, owner_authority_fingerprint, created_at,
                    message_observation_revision, events_observed
             FROM workflow_session_state WHERE session_id = ?1 AND revision = ?2",
            params![record.session_id, expected_revision_i64],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row_u64(row, 4)?,
                    row_u64(row, 5)?,
                ))
            },
        )
        .optional()
        .map_err(|err| io::Error::other(err))?
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Session {} revision CAS failed: expected revision {expected_revision}",
                    record.session_id
                ),
            )
        })?;
    let (session_id, project, owner, created_at, committed_message_revision, committed_events) =
        immutable;
    if session_id != record.session_id
        || project != record.project
        || owner != record.owner_authority_fingerprint
        || created_at != record.created_at
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "immutable Session identity columns cannot change",
        ));
    }
    if write.previous_message_revision != committed_message_revision {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Session write previous_message_revision does not match committed message watermark",
        ));
    }
    if record.message_observation_revision < committed_message_revision {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Session message_observation_revision cannot decrease",
        ));
    }
    if record.events_observed < committed_events {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Session events_observed cannot decrease",
        ));
    }
    let state_json = compact_state_json(record)?;
    let revision = sqlite_i64(write.revision)?;
    let message_observation_revision = sqlite_i64(record.message_observation_revision)?;
    let events_observed = sqlite_i64(record.events_observed)?;
    let changed = tx
        .execute(
            "UPDATE workflow_session_state
             SET updated_at = ?1, revision = ?2, previous_message_revision = ?3,
                 message_observation_revision = ?4, events_observed = ?5, state_json = ?6
             WHERE session_id = ?7 AND revision = ?8",
            params![
                record.updated_at,
                revision,
                message_observation_revision,
                message_observation_revision,
                events_observed,
                state_json,
                record.session_id,
                expected_revision_i64,
            ],
        )
        .map_err(|err| io::Error::other(err))?;
    if changed != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Session {} revision CAS lost race at expected revision {expected_revision}",
                record.session_id
            ),
        ));
    }
    Ok(())
}

pub(in crate::persistence) fn compact_state_json(record: &PersistedSessionRecord) -> io::Result<String> {
    let mut value = serde_json::to_value(record).map_err(io::Error::other)?;
    let Some(object) = value.as_object_mut() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Session state is not a JSON object",
        ));
    };
    object.insert("events".to_string(), Value::Array(Vec::new()));
    object.insert("messages".to_string(), Value::Array(Vec::new()));
    object.insert(
        "message_delivery_replays".to_string(),
        Value::Object(Default::default()),
    );
    serde_json::to_string(&value).map_err(io::Error::other)
}

pub(in crate::persistence) fn event_ordinal_base(events_observed: u64, retained_count: usize) -> u64 {
    if retained_count == 0 {
        return events_observed.saturating_add(1);
    }
    events_observed
        .saturating_sub(retained_count as u64)
        .saturating_add(1)
}

fn retained_event_ordinals(
    events_observed: u64,
    retained_events: &[Arc<SessionEvent>],
) -> HashMap<String, u64> {
    let base = event_ordinal_base(events_observed, retained_events.len());
    retained_events
        .iter()
        .enumerate()
        .map(|(index, event)| (event.event_id.clone(), base.saturating_add(index as u64)))
        .collect()
}

fn validate_retained_events(
    session_id: &str,
    events_observed: u64,
    retained_events: &[Arc<SessionEvent>],
) -> io::Result<()> {
    if retained_events.len() > DEFAULT_MAX_EVENTS_PER_SESSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "Session {session_id} retained event tail exceeds durable cap {}",
                DEFAULT_MAX_EVENTS_PER_SESSION
            ),
        ));
    }
    if (retained_events.len() as u64) > events_observed {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Session {session_id} retained event tail ({}) exceeds events_observed ({events_observed})",
                retained_events.len()
            ),
        ));
    }
    let mut seen_event_ids = HashSet::new();
    for event in retained_events {
        if event.session_id != session_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "retained event {} does not belong to Session {session_id}",
                    event.event_id
                ),
            ));
        }
        if !event.event_id.starts_with(EVENT_ID_PREFIX) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("retained event {} has malformed event id", event.event_id),
            ));
        }
        if !seen_event_ids.insert(event.event_id.as_str()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "retained event tail contains duplicate event id {}",
                    event.event_id
                ),
            ));
        }
    }
    Ok(())
}

fn upsert_retained_event(
    tx: &Transaction<'_>,
    session_id: &str,
    event: &SessionEvent,
    expected_ordinal: u64,
) -> io::Result<()> {
    let event_json = serde_json::to_string(event).map_err(io::Error::other)?;
    let existing: Option<(String, u64, String)> = tx
        .query_row(
            "SELECT session_id, ordinal, event_json FROM workflow_session_events WHERE event_id = ?1",
            params![event.event_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row_u64(row, 1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()
        .map_err(|err| io::Error::other(err))?;
    if let Some((existing_session, existing_ordinal, existing_json)) = existing {
        if existing_session != session_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("event id {} is already bound to another Session", event.event_id),
            ));
        }
        if existing_ordinal != expected_ordinal {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "event id {} durable ordinal {existing_ordinal} does not match retained ordinal {expected_ordinal}",
                    event.event_id
                ),
            ));
        }
        if existing_json == event_json {
            return Ok(());
        }
        let changed = tx
            .execute(
                "UPDATE workflow_session_events
                 SET event_json = ?1
                 WHERE event_id = ?2 AND session_id = ?3",
                params![event_json, event.event_id, session_id],
            )
            .map_err(|err| io::Error::other(err))?;
        if changed != 1 {
            return Err(io::Error::other(format!(
                "event id {} upsert did not update durable row",
                event.event_id
            )));
        }
        return Ok(());
    }
    let ordinal_i64 = sqlite_i64(expected_ordinal)?;
    let occupied: Option<String> = tx
        .query_row(
            "SELECT event_id FROM workflow_session_events
             WHERE session_id = ?1 AND ordinal = ?2",
            params![session_id, ordinal_i64],
            |row| row.get(0),
        )
        .optional()
        .map_err(|err| io::Error::other(err))?;
    if let Some(occupied_event_id) = occupied {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "event ordinal {expected_ordinal} for Session {session_id} is already occupied by {}",
                occupied_event_id
            ),
        ));
    }
    let changed = tx
        .execute(
            "INSERT INTO workflow_session_events
                 (session_id, ordinal, event_id, event_json)
             VALUES (?1, ?2, ?3, ?4)",
            params![session_id, ordinal_i64, event.event_id, event_json],
        )
        .map_err(|err| io::Error::other(err))?;
    if changed != 1 {
        return Err(io::Error::other(format!(
            "event id {} append did not insert durable row",
            event.event_id
        )));
    }
    Ok(())
}

fn prune_retained_event_tail(
    tx: &Transaction<'_>,
    session_id: &str,
    events_observed: u64,
    retained_events: &[Arc<SessionEvent>],
) -> io::Result<()> {
    if retained_events.is_empty() {
        tx.execute(
            "DELETE FROM workflow_session_events WHERE session_id = ?1",
            params![session_id],
        )
        .map_err(|err| io::Error::other(err))?;
        return Ok(());
    }
    let min_ordinal = event_ordinal_base(events_observed, retained_events.len());
    let max_ordinal = min_ordinal
        .saturating_add(retained_events.len() as u64)
        .saturating_sub(1);
    let min_i64 = sqlite_i64(min_ordinal)?;
    let max_i64 = sqlite_i64(max_ordinal)?;
    tx.execute(
        "DELETE FROM workflow_session_events
         WHERE session_id = ?1 AND (ordinal < ?2 OR ordinal > ?3)",
        params![session_id, min_i64, max_i64],
    )
    .map_err(|err| io::Error::other(err))?;
    let retained_ids: HashSet<&str> = retained_events
        .iter()
        .map(|event| event.event_id.as_str())
        .collect();
    let mut stmt = tx
        .prepare("SELECT event_id FROM workflow_session_events WHERE session_id = ?1")
        .map_err(|err| io::Error::other(err))?;
    let stale_ids: Vec<String> = stmt
        .query_map(params![session_id], |row| row.get::<_, String>(0))
        .map_err(|err| io::Error::other(err))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| io::Error::other(err))?
        .into_iter()
        .filter(|event_id| !retained_ids.contains(event_id.as_str()))
        .collect();
    for event_id in stale_ids {
        let changed = tx
            .execute(
                "DELETE FROM workflow_session_events
                 WHERE session_id = ?1 AND event_id = ?2",
                params![session_id, event_id],
            )
            .map_err(|err| io::Error::other(err))?;
        if changed > 1 {
            return Err(io::Error::other(
                "retained event tail prune removed multiple rows",
            ));
        }
    }
    Ok(())
}

fn verify_retained_event_coverage(
    tx: &Transaction<'_>,
    session_id: &str,
    ordinals: &HashMap<String, u64>,
) -> io::Result<()> {
    if ordinals.is_empty() {
        let remaining: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM workflow_session_events WHERE session_id = ?1",
                params![session_id],
                |row| row.get(0),
            )
            .map_err(|err| io::Error::other(err))?;
        if remaining != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Session {session_id} retained no events but durable storage still holds {remaining} rows"
                ),
            ));
        }
        return Ok(());
    }
    for (event_id, expected_ordinal) in ordinals {
        let durable: Option<(String, u64)> = tx
            .query_row(
                "SELECT session_id, ordinal FROM workflow_session_events WHERE event_id = ?1",
                params![event_id],
                |row| Ok((row.get(0)?, row_u64(row, 1)?)),
            )
            .optional()
            .map_err(|err| io::Error::other(err))?;
        let Some((durable_session_id, durable_ordinal)) = durable else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "retained event {event_id} is missing from durable storage for Session {session_id}"
                ),
            ));
        };
        if durable_session_id != session_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("retained event {event_id} is bound to another Session in durable storage"),
            ));
        }
        if durable_ordinal != *expected_ordinal {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "retained event {event_id} durable ordinal {durable_ordinal} does not match expected {expected_ordinal}",
                ),
            ));
        }
    }
    Ok(())
}

pub(in crate::persistence) fn sync_events(
    tx: &Transaction<'_>,
    session_id: &str,
    events_observed: u64,
    retained_events: &[Arc<SessionEvent>],
    appended_events: &[Arc<SessionEvent>],
) -> io::Result<()> {
    validate_retained_events(session_id, events_observed, retained_events)?;
    let ordinals = retained_event_ordinals(events_observed, retained_events);
    let retained_ids: HashSet<&str> = ordinals.keys().map(|event_id| event_id.as_str()).collect();
    for event in appended_events {
        if !retained_ids.contains(event.event_id.as_str()) {
            continue;
        }
        if event.session_id != session_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "appended event {} does not belong to Session {session_id}",
                    event.event_id
                ),
            ));
        }
        let expected_ordinal = ordinals.get(&event.event_id).copied().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "appended event {} is missing from the retained tail map",
                    event.event_id
                ),
            )
        })?;
        upsert_retained_event(tx, session_id, event.as_ref(), expected_ordinal)?;
    }
    prune_retained_event_tail(tx, session_id, events_observed, retained_events)?;
    verify_retained_event_coverage(tx, session_id, &ordinals)?;
    Ok(())
}

pub(in crate::persistence) fn sync_messages(
    tx: &Transaction<'_>,
    session_id: &str,
    messages: &[Arc<SessionMessage>],
    observation_revisions: &BTreeMap<String, u64>,
    initial_insert: bool,
    previous_message_revision: u64,
) -> io::Result<()> {
    if messages.len() > DEFAULT_MAX_MESSAGES_PER_SESSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "Session {session_id} retained message tail exceeds durable cap {}",
                DEFAULT_MAX_MESSAGES_PER_SESSION
            ),
        ));
    }
    for message in messages {
        let observation_revision = observation_revisions
            .get(&message.message_id)
            .copied()
            .unwrap_or(0);
        if !initial_insert && observation_revision <= previous_message_revision {
            verify_message_row(tx, session_id, message, observation_revision)?;
            continue;
        }
        upsert_message_row(tx, session_id, message, observation_revision)?;
    }
    prune_messages_not_retained(tx, session_id, messages)?;
    Ok(())
}

fn delete_delivery_replays_for_message(
    tx: &Transaction<'_>,
    session_id: &str,
    message_id: &str,
) -> io::Result<()> {
    tx.execute(
        "DELETE FROM workflow_session_message_delivery_replays
         WHERE session_id = ?1 AND message_id = ?2",
        params![session_id, message_id],
    )
    .map_err(|err| io::Error::other(err))?;
    Ok(())
}

fn prune_messages_not_retained(
    tx: &Transaction<'_>,
    session_id: &str,
    retained_messages: &[Arc<SessionMessage>],
) -> io::Result<()> {
    let retained_ids: HashSet<&str> = retained_messages
        .iter()
        .map(|message| message.message_id.as_str())
        .collect();
    let mut stmt = tx
        .prepare("SELECT message_id FROM workflow_session_messages WHERE session_id = ?1")
        .map_err(|err| io::Error::other(err))?;
    let stale_ids: Vec<String> = stmt
        .query_map(params![session_id], |row| row.get::<_, String>(0))
        .map_err(|err| io::Error::other(err))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| io::Error::other(err))?
        .into_iter()
        .filter(|message_id| !retained_ids.contains(message_id.as_str()))
        .collect();
    for message_id in &stale_ids {
        delete_delivery_replays_for_message(tx, session_id, message_id)?;
    }
    for message_id in stale_ids {
        let changed = tx
            .execute(
                "DELETE FROM workflow_session_messages
                 WHERE session_id = ?1 AND message_id = ?2",
                params![session_id, message_id],
            )
            .map_err(|err| io::Error::other(err))?;
        if changed > 1 {
            return Err(io::Error::other(
                "retained message tail prune removed multiple rows",
            ));
        }
    }
    Ok(())
}

fn verify_message_row(
    tx: &Transaction<'_>,
    session_id: &str,
    message: &Arc<SessionMessage>,
    observation_revision: u64,
) -> io::Result<()> {
    let message_json = serde_json::to_string(message.as_ref()).map_err(io::Error::other)?;
    let existing: Option<(u64, String)> = tx
        .query_row(
            "SELECT observation_revision, message_json
             FROM workflow_session_messages
             WHERE session_id = ?1 AND message_id = ?2",
            params![session_id, message.message_id],
            |row| {
                Ok((
                    row_u64(row, 0)?,
                    row.get::<_, String>(1)?,
                ))
            },
        )
        .optional()
        .map_err(|err| io::Error::other(err))?;
    let Some((existing_revision, existing_json)) = existing else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "message {} below delta watermark is missing durable row",
                message.message_id
            ),
        ));
    };
    if existing_revision != observation_revision || existing_json != message_json {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "message {} below delta watermark does not match durable row",
                message.message_id
            ),
        ));
    }
    Ok(())
}

fn upsert_message_row(
    tx: &Transaction<'_>,
    session_id: &str,
    message: &Arc<SessionMessage>,
    observation_revision: u64,
) -> io::Result<()> {
    let message_json = serde_json::to_string(message.as_ref()).map_err(io::Error::other)?;
    let observation_revision_i64 = sqlite_i64(observation_revision)?;
    let existing: Option<(u64, String)> = tx
        .query_row(
            "SELECT observation_revision, message_json
             FROM workflow_session_messages
             WHERE session_id = ?1 AND message_id = ?2",
            params![session_id, message.message_id],
            |row| {
                Ok((
                    row_u64(row, 0)?,
                    row.get::<_, String>(1)?,
                ))
            },
        )
        .optional()
        .map_err(|err| io::Error::other(err))?;
    match existing {
        None => {
            let changed = tx
                .execute(
                    "INSERT INTO workflow_session_messages
                     (session_id, message_id, observation_revision, message_json)
                 VALUES (?1, ?2, ?3, ?4)",
                    params![
                        session_id,
                        message.message_id,
                        observation_revision_i64,
                        message_json
                    ],
                )
                .map_err(|err| io::Error::other(err))?;
            if changed != 1 {
                return Err(io::Error::other("failed to insert workflow session message row"));
            }
        }
        Some((existing_revision, existing_json)) => {
            if existing_revision == observation_revision && existing_json == message_json {
                return Ok(());
            }
            if observation_revision > existing_revision {
                let changed = tx
                    .execute(
                        "UPDATE workflow_session_messages
                     SET observation_revision = ?1, message_json = ?2
                     WHERE session_id = ?3 AND message_id = ?4 AND observation_revision < ?1",
                        params![
                            observation_revision_i64,
                            message_json,
                            session_id,
                            message.message_id,
                        ],
                    )
                    .map_err(|err| io::Error::other(err))?;
                if changed != 1 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "message {} observation revision update lost race at revision {observation_revision}",
                            message.message_id
                        ),
                    ));
                }
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "message {} observation revision conflict at revision {observation_revision}",
                        message.message_id
                    ),
                ));
            }
        }
    }
    Ok(())
}

pub(in crate::persistence) fn sync_delivery_replays(
    tx: &Transaction<'_>,
    session_id: &str,
    replays: &BTreeMap<String, SessionMessageDeliveryReplay>,
    _initial_insert: bool,
    _previous_message_revision: u64,
    _observation_revisions: &BTreeMap<String, u64>,
) -> io::Result<()> {
    let mut stmt = tx
        .prepare(
            "SELECT scope_key FROM workflow_session_message_delivery_replays WHERE session_id = ?1",
        )
        .map_err(|err| io::Error::other(err))?;
    let stale_scope_keys: Vec<String> = stmt
        .query_map(params![session_id], |row| row.get::<_, String>(0))
        .map_err(|err| io::Error::other(err))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| io::Error::other(err))?
        .into_iter()
        .filter(|scope_key| !replays.contains_key(scope_key))
        .collect();
    for scope_key in stale_scope_keys {
        if !is_valid_delivery_replay_scope(&scope_key) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("delivery replay scope {scope_key} is malformed"),
            ));
        }
        let changed = tx
            .execute(
                "DELETE FROM workflow_session_message_delivery_replays
                 WHERE session_id = ?1 AND scope_key = ?2",
                params![session_id, scope_key],
            )
            .map_err(|err| io::Error::other(err))?;
        if changed > 1 {
            return Err(io::Error::other(
                "delivery-replay reconciliation removed multiple durable rows",
            ));
        }
    }
    for (scope_key, replay) in replays {
        upsert_delivery_replay_row(tx, session_id, scope_key, replay)?;
    }
    Ok(())
}

fn upsert_delivery_replay_row(
    tx: &Transaction<'_>,
    session_id: &str,
    scope_key: &str,
    replay: &SessionMessageDeliveryReplay,
) -> io::Result<()> {
    ensure_message_row_exists(tx, session_id, &replay.message_id)?;
    let existing: Option<(String, String)> = tx
        .query_row(
            "SELECT payload_fingerprint, message_id
             FROM workflow_session_message_delivery_replays
             WHERE session_id = ?1 AND scope_key = ?2",
            params![session_id, scope_key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|err| io::Error::other(err))?;
    match existing {
        None => {
            tx.execute(
                "INSERT INTO workflow_session_message_delivery_replays
                     (session_id, scope_key, payload_fingerprint, message_id)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    session_id,
                    scope_key,
                    replay.payload_fingerprint,
                    replay.message_id
                ],
            )
            .map_err(|err| io::Error::other(err))?;
        }
        Some((existing_fingerprint, existing_message_id)) => {
            if existing_fingerprint == replay.payload_fingerprint
                && existing_message_id == replay.message_id
            {
                return Ok(());
            }
            if existing_message_id == replay.message_id {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "delivery replay scope {scope_key} cannot change payload fingerprint for retained message {existing_message_id}",
                    ),
                ));
            }
            if message_row_exists(tx, session_id, &existing_message_id)? {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "delivery replay scope {scope_key} cannot rebind from retained message {existing_message_id} to {}",
                        replay.message_id,
                    ),
                ));
            }
            let deleted = tx
                .execute(
                    "DELETE FROM workflow_session_message_delivery_replays
                     WHERE session_id = ?1 AND scope_key = ?2",
                    params![session_id, scope_key],
                )
                .map_err(|err| io::Error::other(err))?;
            if deleted != 1 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("delivery replay scope {scope_key} orphan removal did not apply"),
                ));
            }
            let inserted = tx
                .execute(
                    "INSERT INTO workflow_session_message_delivery_replays
                         (session_id, scope_key, payload_fingerprint, message_id)
                     VALUES (?1, ?2, ?3, ?4)",
                    params![
                        session_id,
                        scope_key,
                        replay.payload_fingerprint,
                        replay.message_id
                    ],
                )
                .map_err(|err| io::Error::other(err))?;
            if inserted != 1 {
                return Err(io::Error::other(
                    "delivery replay orphan replacement insert failed",
                ));
            }
        }
    }
    Ok(())
}

fn message_row_exists(
    tx: &Transaction<'_>,
    session_id: &str,
    message_id: &str,
) -> io::Result<bool> {
    tx.query_row(
        "SELECT 1 FROM workflow_session_messages WHERE session_id = ?1 AND message_id = ?2",
        params![session_id, message_id],
        |_| Ok(()),
    )
    .optional()
    .map_err(|err| io::Error::other(err))
    .map(|row| row.is_some())
}

fn ensure_message_row_exists(
    tx: &Transaction<'_>,
    session_id: &str,
    message_id: &str,
) -> io::Result<()> {
    if !message_row_exists(tx, session_id, message_id)? {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "delivery replay references missing message {message_id} for Session {session_id}"
            ),
        ));
    }
    Ok(())
}

pub(in crate::persistence) fn upsert_retention_tombstone(
    tx: &Transaction<'_>,
    tombstone: &SessionRetentionTombstone,
) -> io::Result<()> {
    let expiry_ordinal = sqlite_i64(tombstone.expiry_ordinal)?;
    let existing: Option<(String, String, u64)> = tx
        .query_row(
            "SELECT owner_authority_fingerprint, incarnation_fingerprint, expiry_ordinal
             FROM workflow_session_retention_tombstones WHERE session_id = ?1",
            params![tombstone.session_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row_u64(row, 2)?,
                ))
            },
        )
        .optional()
        .map_err(|err| io::Error::other(err))?;
    match existing {
        None => {
            let changed = tx
                .execute(
                    "INSERT INTO workflow_session_retention_tombstones
                         (session_id, owner_authority_fingerprint, incarnation_fingerprint, expiry_ordinal)
                     VALUES (?1, ?2, ?3, ?4)",
                    params![
                        tombstone.session_id,
                        tombstone.owner_authority_fingerprint,
                        tombstone.incarnation_fingerprint,
                        expiry_ordinal,
                    ],
                )
                .map_err(|err| io::Error::other(err))?;
            if changed != 1 {
                return Err(io::Error::other("failed to insert retention tombstone"));
            }
        }
        Some((owner, incarnation, _previous_ordinal)) => {
            if owner != tombstone.owner_authority_fingerprint
                || incarnation != tombstone.incarnation_fingerprint
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "retention tombstone for Session {} conflicts with persisted identity",
                        tombstone.session_id
                    ),
                ));
            }
            let changed = tx
                .execute(
                    "UPDATE workflow_session_retention_tombstones
                     SET expiry_ordinal = ?1
                     WHERE session_id = ?2
                       AND owner_authority_fingerprint = ?3
                       AND incarnation_fingerprint = ?4",
                    params![
                        expiry_ordinal,
                        tombstone.session_id,
                        tombstone.owner_authority_fingerprint,
                        tombstone.incarnation_fingerprint,
                    ],
                )
                .map_err(|err| io::Error::other(err))?;
            if changed != 1 {
                return Err(io::Error::other(
                    "failed to update retention tombstone expiry ordinal",
                ));
            }
        }
    }
    Ok(())
}

pub(in crate::persistence) fn insert_session_snapshot(
    tx: &Transaction<'_>,
    record: &PersistedSessionRecord,
    events: Vec<Arc<SessionEvent>>,
    revision: u64,
) -> io::Result<()> {
    if revision == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "imported Session revision must be positive",
        ));
    }
    let exists: bool = tx
        .query_row(
            "SELECT 1 FROM workflow_session_state WHERE session_id = ?1",
            params![record.session_id],
            |_| Ok(()),
        )
        .optional()
        .map_err(|err| io::Error::other(err))?
        .is_some();
    if exists {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("duplicate Session id during import: {}", record.session_id),
        ));
    }
    let tombstoned: bool = tx
        .query_row(
            "SELECT 1 FROM workflow_session_retention_tombstones WHERE session_id = ?1",
            params![record.session_id],
            |_| Ok(()),
        )
        .optional()
        .map_err(|err| io::Error::other(err))?
        .is_some();
    if tombstoned {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Session {} cannot be imported because a retention tombstone already exists",
                record.session_id
            ),
        ));
    }
    let state_json = compact_state_json(record)?;
    let revision_i64 = sqlite_i64(revision)?;
    let message_observation_revision = sqlite_i64(record.message_observation_revision)?;
    let events_observed = sqlite_i64(record.events_observed)?;
    tx.execute(
        "INSERT INTO workflow_session_state (
            session_id, project, owner_authority_fingerprint, created_at, updated_at,
            revision, previous_message_revision, message_observation_revision, events_observed,
            state_json
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            record.session_id,
            record.project,
            record.owner_authority_fingerprint,
            record.created_at,
            record.updated_at,
            revision_i64,
            message_observation_revision,
            message_observation_revision,
            events_observed,
            state_json,
        ],
    )
    .map_err(|err| io::Error::other(err))?;
    sync_events(
        tx,
        &record.session_id,
        record.events_observed,
        &events,
        &events,
    )?;
    sync_messages(
        tx,
        &record.session_id,
        &record.messages,
        &record.message_observation_revisions,
        true,
        0,
    )?;
    sync_delivery_replays(
        tx,
        &record.session_id,
        &record.message_delivery_replays,
        true,
        0,
        &record.message_observation_revisions,
    )?;
    Ok(())
}

fn validate_identity_columns(
    session_id: &str,
    project: Option<&str>,
    owner_authority_fingerprint: &str,
    created_at: i64,
    revision: u64,
    state: &PersistedSessionRecord,
) -> io::Result<()> {
    if state.session_id != session_id
        || state.project.as_deref() != project
        || state.owner_authority_fingerprint != owner_authority_fingerprint
        || state.created_at != created_at
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "indexed Session identity columns do not match state_json",
        ));
    }
    if revision == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Session revision must be positive",
        ));
    }
    Ok(())
}

fn load_recent_events(
    connection: &Connection,
    session_id: &str,
    recent_event_limit: usize,
) -> io::Result<Vec<Arc<SessionEvent>>> {
    if recent_event_limit == 0 {
        return Ok(Vec::new());
    }
    let limit = i64::try_from(recent_event_limit).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "recent event limit exceeds SQLite integer range",
        )
    })?;
    let mut stmt = connection
        .prepare(
            "SELECT event_id, session_id, event_json FROM workflow_session_events
             WHERE session_id = ?1
             ORDER BY ordinal DESC
             LIMIT ?2",
        )
        .map_err(|err| io::Error::other(err))?;
    let rows = stmt
        .query_map(params![session_id, limit], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(|err| io::Error::other(err))?;
    let mut events = Vec::new();
    for row in rows {
        let (event_id, indexed_session_id, event_json) =
            row.map_err(|err| io::Error::other(err))?;
        let event: SessionEvent = serde_json::from_str(&event_json).map_err(io::Error::other)?;
        if indexed_session_id != session_id
            || event.session_id != session_id
            || event.event_id != event_id
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "persisted event identity does not match indexed Session row",
            ));
        }
        events.push(Arc::new(event));
    }
    events.reverse();
    Ok(events)
}

fn load_messages_with_revisions(
    connection: &Connection,
    session_id: &str,
) -> io::Result<(Vec<Arc<SessionMessage>>, BTreeMap<String, u64>)> {
    let limit = i64::try_from(DEFAULT_MAX_MESSAGES_PER_SESSION).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "message retention limit exceeds SQLite integer range",
        )
    })?;
    let mut stmt = connection
        .prepare(
            "SELECT message_id, observation_revision, message_json
             FROM workflow_session_messages
             WHERE session_id = ?1
             ORDER BY rowid DESC
             LIMIT ?2",
        )
        .map_err(|err| io::Error::other(err))?;
    let rows = stmt
        .query_map(params![session_id, limit], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row_u64(row, 1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(|err| io::Error::other(err))?;
    let mut messages = Vec::new();
    let mut revisions = BTreeMap::new();
    for row in rows {
        let (message_id, observation_revision, message_json) =
            row.map_err(|err| io::Error::other(err))?;
        let message: SessionMessage = serde_json::from_str(&message_json).map_err(io::Error::other)?;
        if message.session_id != session_id || message.message_id != message_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "persisted message identity does not match indexed Session row",
            ));
        }
        revisions.insert(message_id, observation_revision);
        messages.push(Arc::new(message));
    }
    messages.reverse();
    Ok((messages, revisions))
}

fn validate_loaded_message_revisions(
    record: &PersistedSessionRecord,
    stored_revisions: &BTreeMap<String, u64>,
) -> io::Result<()> {
    for (message_id, expected) in &record.message_observation_revisions {
        let Some(stored) = stored_revisions.get(message_id) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "state_json message {message_id} is missing durable observation revision"
                ),
            ));
        };
        if *stored != *expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "message {message_id} observation revision metadata does not match durable storage"
                ),
            ));
        }
        if *expected > record.message_observation_revision {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "message {message_id} observation revision exceeds Session watermark"
                ),
            ));
        }
    }
    Ok(())
}

fn is_valid_delivery_replay_scope(scope_key: &str) -> bool {
    let Some((scope, key)) = scope_key.split_once(':') else {
        return false;
    };
    is_lower_hex_sha256(scope) && is_lower_hex_sha256(key)
}

fn load_delivery_replays(
    connection: &Connection,
    session_id: &str,
    retained_message_ids: &std::collections::HashSet<String>,
) -> io::Result<BTreeMap<String, SessionMessageDeliveryReplay>> {
    let mut stmt = connection
        .prepare(
            "SELECT scope_key, payload_fingerprint, message_id
             FROM workflow_session_message_delivery_replays
             WHERE session_id = ?1",
        )
        .map_err(|err| io::Error::other(err))?;
    let rows = stmt
        .query_map(params![session_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(|err| io::Error::other(err))?;
    let mut replays = BTreeMap::new();
    for row in rows {
        let (scope_key, payload_fingerprint, message_id) =
            row.map_err(|err| io::Error::other(err))?;
        if !is_valid_delivery_replay_scope(&scope_key) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("delivery replay scope {scope_key} is malformed"),
            ));
        }
        if !is_lower_hex_sha256(&payload_fingerprint) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("delivery replay scope {scope_key} has malformed fingerprint"),
            ));
        }
        if !retained_message_ids.contains(&message_id) {
            continue;
        }
        if replays.contains_key(&scope_key) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("duplicate delivery replay scope {scope_key}"),
            ));
        }
        replays.insert(
            scope_key,
            SessionMessageDeliveryReplay {
                payload_fingerprint,
                message_id,
            },
        );
    }
    Ok(replays)
}

pub(in crate::persistence) fn sanitize_loaded_record(
    loaded: PersistedSessionRecord,
    recent_event_limit: usize,
) -> io::Result<PersistedSessionRecord> {
    let materialized = MaterializedCounts::from(&loaded);
    let Some(session_record) = loaded.into_record(recent_event_limit) else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "loaded Session row failed JSON restore sanitization",
        ));
    };
    let after_sanitize = MaterializedCounts::from_session(&session_record);
    if materialized != after_sanitize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "loaded Session row dropped durable evidence during sanitization",
        ));
    }
    Ok(PersistedSessionRecord::from_record(
        &session_record,
        recent_event_limit,
    ))
}

#[derive(Debug, PartialEq, Eq)]
struct MaterializedCounts {
    session_id: String,
    owner_authority_fingerprint: String,
    project: Option<String>,
    project_identity: Option<SessionProjectIdentity>,
    events: usize,
    messages: usize,
    message_delivery_replays: usize,
    materialized_validation_job_ids: usize,
    assignment_history_floors: usize,
    completion_assignment_fence_fingerprints: usize,
    events_observed: u64,
    message_observation_revision: u64,
}

impl MaterializedCounts {
    fn from(record: &PersistedSessionRecord) -> Self {
        Self {
            session_id: record.session_id.clone(),
            owner_authority_fingerprint: record.owner_authority_fingerprint.clone(),
            project: record.project.clone(),
            project_identity: record.project_identity.clone(),
            events: record.events.len(),
            messages: record.messages.len(),
            message_delivery_replays: record.message_delivery_replays.len(),
            materialized_validation_job_ids: record.materialized_validation_job_ids.len(),
            assignment_history_floors: record.assignment_history_floors.len(),
            completion_assignment_fence_fingerprints: record
                .completion_assignment_fence_fingerprints
                .len(),
            events_observed: record.events_observed,
            message_observation_revision: record.message_observation_revision,
        }
    }

    fn from_session(record: &SessionRecord) -> Self {
        Self {
            session_id: record.session_id.clone(),
            owner_authority_fingerprint: record.owner_authority_fingerprint.clone(),
            project: record.project.clone(),
            project_identity: record.project_identity.clone(),
            events: record.events.len(),
            messages: record.messages.len(),
            message_delivery_replays: record.message_delivery_replays.len(),
            materialized_validation_job_ids: record.materialized_validation_job_ids.len(),
            assignment_history_floors: record.assignment_history_floors.len(),
            completion_assignment_fence_fingerprints: record
                .completion_assignment_fence_fingerprints
                .len(),
            events_observed: record.events_observed,
            message_observation_revision: record.message_observation_revision,
        }
    }
}

#[cfg(test)]
mod upsert_message_row_tests {
    use super::*;
    use crate::model::{SessionMessageKind, SessionMessagePriority, SessionMessageStatus};
    use std::sync::Arc;

    const TEST_SESSION_ID: &str = "wc_sess_abcdefghijklmnopqrstu";
    const TEST_OWNER: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn open_test_db(label: &str) -> (tempfile::TempDir, SessionDatabase) {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join(format!("{label}.db"));
        let db = SessionDatabase::open(&path).expect("open sqlite");
        (temp, db)
    }

    fn minimal_message(message_id: &str, body: &str) -> Arc<SessionMessage> {
        Arc::new(SessionMessage {
            message_id: message_id.to_string(),
            session_id: TEST_SESSION_ID.to_string(),
            created_at: 1,
            kind: SessionMessageKind::Note,
            status: SessionMessageStatus::Open,
            priority: SessionMessagePriority::Normal,
            message: body.to_string(),
            tags: Vec::new(),
            reply_to: None,
            requires_ack: false,
            first_ack_observed_at: None,
            author_session_id: None,
            resolved_at: None,
            resolution: None,
            closure_kind: None,
            superseded_by_message_id: None,
            supersedes_message_id: None,
            resolved_by_message_id: None,
            completion_id: None,
        })
    }

    #[test]
    fn upsert_message_row_advances_observation_revision_on_update() {
        let (_temp, db) = open_test_db("observation_revision_update");
        let mut conn = db.lock_connection().expect("lock");
        let tx = conn.transaction().expect("begin tx");
        tx.execute(
            "INSERT INTO workflow_session_state (
                session_id, project, owner_authority_fingerprint, created_at, updated_at,
                revision, previous_message_revision, message_observation_revision, events_observed,
                state_json
             ) VALUES (?1, NULL, ?2, 1, 1, 1, 0, 0, 0, ?3)",
            params![
                TEST_SESSION_ID,
                TEST_OWNER,
                r#"{"session_id":"wc_sess_abcdefghijklmnopqrstu","owner_authority_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","created_at":1,"updated_at":1,"events_observed":0,"message_observation_revision":0,"message_observation_revisions":{}}"#,
            ],
        )
        .expect("seed session row");
        let message = minimal_message("wc_msg_testnote00000000000001", "first body");
        upsert_message_row(&tx, TEST_SESSION_ID, &message, 1).expect("initial insert");
        let updated = minimal_message("wc_msg_testnote00000000000001", "revised body");
        upsert_message_row(&tx, TEST_SESSION_ID, &updated, 2).expect("revision bump update");
        let stored_revision: i64 = tx
            .query_row(
                "SELECT observation_revision FROM workflow_session_messages
                 WHERE session_id = ?1 AND message_id = ?2",
                params![TEST_SESSION_ID, "wc_msg_testnote00000000000001"],
                |row| row.get(0),
            )
            .expect("read stored revision");
        assert_eq!(stored_revision, 2);
        tx.commit().expect("commit");
    }
}

#[cfg(test)]
mod batch_tests {
    use super::*;
    use crate::model::{
        SessionExecutionContext, SessionGuards, SessionLifecycle, SessionMessageKind,
        SessionMessagePriority, SessionMessageStatus, SessionMode,
    };
    use std::collections::BTreeMap;

    const TEST_OWNER: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const TEST_OWNER_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const REPLAY_SCOPE: &str =
        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
    const REPLAY_FINGERPRINT: &str =
        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";

    fn open_test_db(label: &str) -> (tempfile::TempDir, SessionDatabase) {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join(format!("{label}.db"));
        let db = SessionDatabase::open(&path).expect("open sqlite");
        (temp, db)
    }

    fn minimal_record(
        session_id: &str,
        owner: &str,
        created_at: i64,
        lifecycle: SessionLifecycle,
    ) -> PersistedSessionRecord {
        PersistedSessionRecord {
            session_id: session_id.to_string(),
            project: None,
            project_identity: None,
            owner_authority_fingerprint: owner.to_string(),
            title: None,
            mode: SessionMode::Normal,
            guards: SessionGuards::default(),
            execution_context: SessionExecutionContext::default(),
            git_baseline_tree: None,
            repository_edit_observed: false,
            lifecycle,
            created_at,
            updated_at: created_at,
            events: Vec::new(),
            messages: Vec::new(),
            message_delivery_replays: BTreeMap::new(),
            message_observation_revision: 0,
            message_observation_floor: 0,
            message_observation_revisions: BTreeMap::new(),
            assignment_history_floors: BTreeMap::new(),
            assignment_history_tracking_complete: false,
            completion_assignment_fence_fingerprints: BTreeMap::new(),
            completion_assignment_fence_tracking_complete: false,
            events_observed: 0,
            legacy_context_revision: None,
            materialized_validation_job_ids: Vec::new(),
        }
    }

    fn tombstone_for_record(record: &PersistedSessionRecord, expiry_ordinal: u64) -> SessionRetentionTombstone {
        SessionRetentionTombstone {
            session_id: record.session_id.clone(),
            owner_authority_fingerprint: record.owner_authority_fingerprint.clone(),
            incarnation_fingerprint: workflow_session_incarnation_fingerprint(
                &record.session_id,
                record.created_at,
                record.project.as_deref(),
                &record.owner_authority_fingerprint,
            ),
            expiry_ordinal,
        }
    }

    #[test]
    fn retirement_cascades_session_rows_and_excludes_inventory() {
        let (_temp, db) = open_test_db("retirement_cascade");
        let session_id = "wc_sess_retirecascadetest00001";
        let record = minimal_record(session_id, TEST_OWNER, 10, SessionLifecycle::Closed);
        let mut conn = db.lock_connection().expect("lock");
        let tx = conn.transaction().expect("begin");
        insert_session_snapshot(&tx, &record, Vec::new(), 1).expect("seed");
        tx.execute(
            "INSERT INTO workflow_session_messages
                 (session_id, message_id, observation_revision, message_json)
             VALUES (?1, 'wc_msg_retirecascadenote00001', 1, ?2)",
            params![
                session_id,
                r#"{"message_id":"wc_msg_retirecascadenote00001","session_id":"wc_sess_retirecascadetest00001","created_at":10,"kind":"note","status":"open","priority":"normal","message":"x","tags":[]}"#
            ],
        )
        .expect("seed message");
        tx.commit().expect("commit");

        let tombstone = tombstone_for_record(&record, 1);
        db.apply_batch(&SessionDatabaseBatch {
            retirements: vec![SessionDatabaseRetirement {
                session_id: session_id.to_string(),
                created_at: record.created_at,
                project: record.project.clone(),
                owner_authority_fingerprint: record.owner_authority_fingerprint.clone(),
                project_identity: None,
                expected_revision: Some(1),
                retention_tombstone: Some(tombstone.clone()),
            }],
            ..SessionDatabaseBatch::default()
        })
        .expect("retire");

        assert!(db.inventory().expect("inventory").is_empty());
        assert_eq!(db.tombstones().expect("tombstones"), vec![tombstone]);
        let conn = db.lock_connection().expect("lock");
        let message_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM workflow_session_messages WHERE session_id = ?1",
                params![session_id],
                |row| row.get(0),
            )
            .expect("count messages");
        assert_eq!(message_count, 0);
    }

    #[test]
    fn tombstone_delete_clears_stale_state_and_allows_session_id_reuse() {
        let (_temp, db) = open_test_db("tombstone_reuse");
        let session_id = "wc_sess_tombstonereusetest0001";
        let old_record = minimal_record(session_id, TEST_OWNER, 10, SessionLifecycle::Closed);
        let tombstone = tombstone_for_record(&old_record, 1);
        let mut conn = db.lock_connection().expect("lock");
        let tx = conn.transaction().expect("begin");
        insert_session_snapshot(&tx, &old_record, Vec::new(), 1).expect("stale state");
        upsert_retention_tombstone(&tx, &tombstone).expect("tombstone");
        tx.commit().expect("commit");

        db.apply_batch(&SessionDatabaseBatch {
            tombstone_deletes: vec![SessionDatabaseTombstoneDelete {
                session_id: session_id.to_string(),
                owner_authority_fingerprint: tombstone.owner_authority_fingerprint.clone(),
                incarnation_fingerprint: tombstone.incarnation_fingerprint.clone(),
            }],
            writes: vec![SessionDatabaseWrite {
                revision: 1,
                expected_revision: None,
                previous_message_revision: 0,
                record: minimal_record(session_id, TEST_OWNER_B, 20, SessionLifecycle::Active),
                appended_events: Vec::new(),
            }],
            ..SessionDatabaseBatch::default()
        })
        .expect("prune and recreate");

        let inventory = db.inventory().expect("inventory");
        assert_eq!(inventory.len(), 1);
        assert_eq!(inventory[0].state.owner_authority_fingerprint, TEST_OWNER_B);
        assert_eq!(inventory[0].state.created_at, 20);
        assert!(db.tombstones().expect("tombstones").is_empty());
    }

    #[test]
    fn tombstone_only_batch_upserts_without_live_state() {
        let (_temp, db) = open_test_db("tombstone_only");
        let session_id = "wc_sess_tombstoneonlybatch0001";
        let record = minimal_record(session_id, TEST_OWNER, 42, SessionLifecycle::Closed);
        let tombstone = tombstone_for_record(&record, 3);
        db.apply_batch(&SessionDatabaseBatch {
            tombstone_upserts: vec![tombstone.clone()],
            ..SessionDatabaseBatch::default()
        })
        .expect("tombstone upsert");

        assert!(db.inventory().expect("inventory").is_empty());
        assert_eq!(db.tombstones().expect("tombstones"), vec![tombstone]);
    }

    #[test]
    fn batch_rejects_retirement_and_tombstone_delete_same_session() {
        let (_temp, db) = open_test_db("retire_tombstone_delete");
        let session_id = "wc_sess_reiretombstonedelete01";
        let err = db
            .apply_batch(&SessionDatabaseBatch {
                retirements: vec![SessionDatabaseRetirement {
                    session_id: session_id.to_string(),
                    created_at: 1,
                    project: None,
                    owner_authority_fingerprint: TEST_OWNER.to_string(),
                    project_identity: None,
                    expected_revision: Some(1),
                    retention_tombstone: None,
                }],
                tombstone_deletes: vec![SessionDatabaseTombstoneDelete {
                    session_id: session_id.to_string(),
                    owner_authority_fingerprint: TEST_OWNER.to_string(),
                    incarnation_fingerprint: workflow_session_incarnation_fingerprint(
                        session_id,
                        1,
                        None,
                        TEST_OWNER,
                    ),
                }],
                ..SessionDatabaseBatch::default()
            })
            .expect_err("retirement plus tombstone delete must fail validation");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn delivery_replay_deletion_allows_scope_reuse() {
        let (_temp, db) = open_test_db("replay_scope_reuse");
        let session_id = "wc_sess_deliveryreplayreuse001";
        let message_id = "wc_msg_deliveryreplayreuse0001";
        let mut record = minimal_record(session_id, TEST_OWNER, 5, SessionLifecycle::Active);
        record.messages.push(Arc::new(SessionMessage {
            message_id: message_id.to_string(),
            session_id: session_id.to_string(),
            created_at: 5,
            kind: SessionMessageKind::Note,
            status: SessionMessageStatus::Open,
            priority: SessionMessagePriority::Normal,
            message: "seed".to_string(),
            tags: Vec::new(),
            reply_to: None,
            requires_ack: false,
            first_ack_observed_at: None,
            author_session_id: None,
            resolved_at: None,
            resolution: None,
            closure_kind: None,
            superseded_by_message_id: None,
            supersedes_message_id: None,
            resolved_by_message_id: None,
            completion_id: None,
        }));
        record.message_observation_revisions.insert(message_id.to_string(), 1);
        record.message_observation_revision = 1;
        record.message_delivery_replays.insert(
            REPLAY_SCOPE.to_string(),
            SessionMessageDeliveryReplay {
                payload_fingerprint: REPLAY_FINGERPRINT.to_string(),
                message_id: message_id.to_string(),
            },
        );
        let mut conn = db.lock_connection().expect("lock");
        let tx = conn.transaction().expect("begin");
        insert_session_snapshot(&tx, &record, Vec::new(), 1).expect("seed replay");
        tx.commit().expect("commit");

        let replacement_message = "wc_msg_deliveryreplayreuse0002";
        let mut updated = record.clone();
        updated.updated_at = 6;
        updated.messages = vec![Arc::new(SessionMessage {
            message_id: replacement_message.to_string(),
            session_id: session_id.to_string(),
            created_at: 6,
            kind: SessionMessageKind::Note,
            status: SessionMessageStatus::Open,
            priority: SessionMessagePriority::Normal,
            message: "replacement".to_string(),
            tags: Vec::new(),
            reply_to: None,
            requires_ack: false,
            first_ack_observed_at: None,
            author_session_id: None,
            resolved_at: None,
            resolution: None,
            closure_kind: None,
            superseded_by_message_id: None,
            supersedes_message_id: None,
            resolved_by_message_id: None,
            completion_id: None,
        })];
        updated.message_observation_revisions = BTreeMap::from([(replacement_message.to_string(), 2)]);
        updated.message_observation_revision = 2;
        updated.message_delivery_replays.insert(
            REPLAY_SCOPE.to_string(),
            SessionMessageDeliveryReplay {
                payload_fingerprint: REPLAY_FINGERPRINT.to_string(),
                message_id: replacement_message.to_string(),
            },
        );

        db.apply_batch(&SessionDatabaseBatch {
            delivery_replay_deletions: vec![SessionDatabaseDeliveryReplayDeletion {
                session_id: session_id.to_string(),
                scope_key: REPLAY_SCOPE.to_string(),
            }],
            writes: vec![SessionDatabaseWrite {
                revision: 2,
                expected_revision: Some(1),
                previous_message_revision: 1,
                record: updated,
                appended_events: Vec::new(),
            }],
            ..SessionDatabaseBatch::default()
        })
        .expect("delete and rewrite replay");

        let row = db
            .inventory()
            .expect("inventory")
            .into_iter()
            .next()
            .expect("session row");
        let restored = db.load(&row, 0).expect("load");
        assert_eq!(
            restored
                .message_delivery_replays
                .get(REPLAY_SCOPE)
                .map(|replay| replay.message_id.as_str()),
            Some(replacement_message)
        );
    }
}

#[cfg(test)]
mod bounded_ledger_tests {
    use super::*;
    use crate::model::{
        SessionExecutionContext, SessionGuards, SessionLifecycle, SessionMessageKind,
        SessionMessagePriority, SessionMessageStatus, SessionMode,
    };
    use webcodex_core::runner_protocol::PROJECT_ROOT_FINGERPRINT_PREFIX;
    use std::collections::BTreeMap;

    const TEST_OWNER: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const REPLAY_SCOPE_A: &str =
        "1111111111111111111111111111111111111111111111111111111111111111:2222222222222222222222222222222222222222222222222222222222222222";
    const REPLAY_SCOPE_B: &str =
        "3333333333333333333333333333333333333333333333333333333333333333:4444444444444444444444444444444444444444444444444444444444444444";
    const REPLAY_FINGERPRINT: &str =
        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";

    fn open_test_db(label: &str) -> (tempfile::TempDir, SessionDatabase) {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join(format!("{label}.db"));
        let db = SessionDatabase::open(&path).expect("open sqlite");
        (temp, db)
    }

    fn minimal_event(session_id: &str, event_id: &str, timestamp: i64) -> Arc<SessionEvent> {
        Arc::new(SessionEvent {
            event_id: event_id.to_string(),
            call_id: None,
            logical_invocation_id: None,
            logical_invocation_role: None,
            session_id: session_id.to_string(),
            kind: "note".to_string(),
            legacy_context_revision: None,
            context_result_summary: None,
            timestamp,
            transport: "test".to_string(),
            tool_name: "test".to_string(),
            project: None,
            resolved_project: None,
            risk_class: "read".to_string(),
            read_like: true,
            write_like: false,
            shell_like: false,
            git_like: false,
            change_summary_like: false,
            diff_review_like: false,
            started_at: None,
            finished_at: None,
            duration_ms: None,
            status: None,
            exit_code: None,
            failure_kind: None,
            error_kind: None,
            expected_failure: None,
            expected_failure_kind: None,
            result_expectation: None,
            accepted_exit_codes: Vec::new(),
            assertion_name: None,
            actual_failure_kind: None,
            failure_expectation_result: None,
            warning_kind: None,
            session_project: None,
            request_project: None,
            error_message_summary: None,
            changed_paths: Vec::new(),
            observed_paths: Vec::new(),
            job_id: None,
            persistent_shell: None,
            effect_evidence: None,
            input_summary: None,
            validation_output_summary: None,
            permission: None,
            instruction: None,
            requested_mode: None,
            previous_mode: None,
            requested_guards: None,
            previous_guards: None,
            capability_changed: None,
            context_refreshed: None,
            execution_context: None,
            previous_execution_context: None,
            execution_context_changed: None,
        })
    }

    fn minimal_record(session_id: &str) -> PersistedSessionRecord {
        PersistedSessionRecord {
            session_id: session_id.to_string(),
            project: None,
            project_identity: None,
            owner_authority_fingerprint: TEST_OWNER.to_string(),
            title: None,
            mode: SessionMode::Normal,
            guards: SessionGuards::default(),
            execution_context: SessionExecutionContext::default(),
            git_baseline_tree: None,
            repository_edit_observed: false,
            lifecycle: SessionLifecycle::Active,
            created_at: 1,
            updated_at: 1,
            events: Vec::new(),
            messages: Vec::new(),
            message_delivery_replays: BTreeMap::new(),
            message_observation_revision: 0,
            message_observation_floor: 0,
            message_observation_revisions: BTreeMap::new(),
            assignment_history_floors: BTreeMap::new(),
            assignment_history_tracking_complete: false,
            completion_assignment_fence_fingerprints: BTreeMap::new(),
            completion_assignment_fence_tracking_complete: false,
            events_observed: 0,
            legacy_context_revision: None,
            materialized_validation_job_ids: Vec::new(),
        }
    }

    fn minimal_message(session_id: &str, message_id: &str, created_at: i64) -> Arc<SessionMessage> {
        Arc::new(SessionMessage {
            message_id: message_id.to_string(),
            session_id: session_id.to_string(),
            created_at,
            kind: SessionMessageKind::Note,
            status: SessionMessageStatus::Open,
            priority: SessionMessagePriority::Normal,
            message: "note".to_string(),
            tags: Vec::new(),
            reply_to: None,
            requires_ack: false,
            first_ack_observed_at: None,
            author_session_id: None,
            resolved_at: None,
            resolution: None,
            closure_kind: None,
            superseded_by_message_id: None,
            supersedes_message_id: None,
            resolved_by_message_id: None,
            completion_id: None,
        })
    }

    #[test]
    fn event_tail_prunes_outside_ordinal_window_even_without_deltas() {
        let (_temp, db) = open_test_db("event_tail_prune");
        let session_id = "wc_sess_eventtailprunetest00001";
        let events: Vec<Arc<SessionEvent>> = (1_u64..=5)
            .map(|index| {
                minimal_event(
                    session_id,
                    &format!("evt_eventtailprunetest{:014x}", index),
                    index as i64,
                )
            })
            .collect();
        let mut record = minimal_record(session_id);
        record.events = events.clone();
        record.events_observed = 5;
        let mut conn = db.lock_connection().expect("lock");
        let tx = conn.transaction().expect("begin");
        insert_session_snapshot(&tx, &record, events, 1).expect("seed events");
        tx.commit().expect("commit");

        let retained: Vec<Arc<SessionEvent>> = events.into_iter().skip(3).collect();
        let mut updated = minimal_record(session_id);
        updated.events = retained.clone();
        updated.events_observed = 5;
        updated.updated_at = 2;
        db.apply_batch(&SessionDatabaseBatch {
            writes: vec![SessionDatabaseWrite {
                revision: 2,
                expected_revision: Some(1),
                previous_message_revision: 0,
                record: updated,
                appended_events: Vec::new(),
            }],
            ..SessionDatabaseBatch::default()
        })
        .expect("prune event tail");

        let conn = db.lock_connection().expect("lock");
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM workflow_session_events WHERE session_id = ?1",
                params![session_id],
                |row| row.get(0),
            )
            .expect("count events");
        assert_eq!(count, 2);
        let first_ordinal: i64 = conn
            .query_row(
                "SELECT MIN(ordinal) FROM workflow_session_events WHERE session_id = ?1",
                params![session_id],
                |row| row.get(0),
            )
            .expect("min ordinal");
        assert_eq!(first_ordinal, 4);
    }

    #[test]
    fn evicted_pending_event_is_not_inserted_on_flush() {
        let (_temp, db) = open_test_db("evicted_pending_event");
        let session_id = "wc_sess_evictedpendingevent0001";
        let events: Vec<Arc<SessionEvent>> = (1_u64..=3)
            .map(|index| {
                minimal_event(
                    session_id,
                    &format!("evt_evictedpendingevent{:014x}", index),
                    index as i64,
                )
            })
            .collect();
        let mut record = minimal_record(session_id);
        record.events = events.clone();
        record.events_observed = 3;
        let mut conn = db.lock_connection().expect("lock");
        let tx = conn.transaction().expect("begin");
        insert_session_snapshot(&tx, &record, events.clone(), 1).expect("seed");
        tx.commit().expect("commit");

        let evicted = events[2].clone();
        let evicted_id = evicted.event_id.clone();
        let retained = events[..2].to_vec();
        let mut updated = minimal_record(session_id);
        updated.events = retained;
        updated.events_observed = 4;
        updated.updated_at = 2;
        db.apply_batch(&SessionDatabaseBatch {
            writes: vec![SessionDatabaseWrite {
                revision: 2,
                expected_revision: Some(1),
                previous_message_revision: 0,
                record: updated,
                appended_events: vec![evicted],
            }],
            ..SessionDatabaseBatch::default()
        })
        .expect("flush without evicted insert");

        let conn = db.lock_connection().expect("lock");
        let evicted_present: bool = conn
            .query_row(
                "SELECT 1 FROM workflow_session_events WHERE session_id = ?1 AND event_id = ?2",
                params![session_id, evicted_id],
                |_| Ok(()),
            )
            .optional()
            .expect("lookup evicted")
            .is_some();
        assert!(!evicted_present);
    }

    #[test]
    fn message_prune_prevents_evicted_message_resurrection() {
        let (_temp, db) = open_test_db("message_no_resurrection");
        let session_id = "wc_sess_messagenoresurrection01";
        let evicted_id = "wc_msg_messagenoresurrection0001";
        let retained_id = "wc_msg_messagenoresurrection0002";
        let mut record = minimal_record(session_id);
        record.messages = vec![
            minimal_message(session_id, evicted_id, 1),
            minimal_message(session_id, retained_id, 2),
        ];
        record.message_observation_revisions =
            BTreeMap::from([(evicted_id.to_string(), 1), (retained_id.to_string(), 2)]);
        record.message_observation_revision = 2;
        let mut conn = db.lock_connection().expect("lock");
        let tx = conn.transaction().expect("begin");
        insert_session_snapshot(&tx, &record, Vec::new(), 1).expect("seed");
        tx.commit().expect("commit");

        let mut updated = record;
        updated.messages = vec![minimal_message(session_id, retained_id, 2)];
        updated.message_observation_revisions = BTreeMap::from([(retained_id.to_string(), 2)]);
        updated.updated_at = 3;
        db.apply_batch(&SessionDatabaseBatch {
            writes: vec![SessionDatabaseWrite {
                revision: 2,
                expected_revision: Some(1),
                previous_message_revision: 2,
                record: updated,
                appended_events: Vec::new(),
            }],
            ..SessionDatabaseBatch::default()
        })
        .expect("prune messages");

        let row = db
            .inventory()
            .expect("inventory")
            .into_iter()
            .next()
            .expect("session");
        let restored = db.load(&row, DEFAULT_MAX_EVENTS_PER_SESSION).expect("load");
        assert!(restored
            .messages
            .iter()
            .all(|message| message.message_id != evicted_id));
        let conn = db.lock_connection().expect("lock");
        let evicted_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM workflow_session_messages
                 WHERE session_id = ?1 AND message_id = ?2",
                params![session_id, evicted_id],
                |row| row.get(0),
            )
            .expect("count evicted");
        assert_eq!(evicted_count, 0);
    }

    #[test]
    fn message_prune_deletes_delivery_replays_before_evicted_message_rows() {
        let (_temp, db) = open_test_db("message_prune_replay_fk");
        let session_id = "wc_sess_messageprunereplayfk01";
        let evicted_id = "wc_msg_messageprunereplayfk001";
        let retained_id = "wc_msg_messageprunereplayfk002";
        let mut record = minimal_record(session_id);
        record.messages = vec![
            minimal_message(session_id, evicted_id, 1),
            minimal_message(session_id, retained_id, 2),
        ];
        record.message_observation_revisions =
            BTreeMap::from([(evicted_id.to_string(), 1), (retained_id.to_string(), 2)]);
        record.message_observation_revision = 2;
        record.message_delivery_replays.insert(
            REPLAY_SCOPE_A.to_string(),
            SessionMessageDeliveryReplay {
                payload_fingerprint: REPLAY_FINGERPRINT.to_string(),
                message_id: evicted_id.to_string(),
            },
        );
        let mut conn = db.lock_connection().expect("lock");
        let tx = conn.transaction().expect("begin");
        insert_session_snapshot(&tx, &record, Vec::new(), 1).expect("seed");
        tx.commit().expect("commit");

        let mut updated = record;
        updated.messages = vec![minimal_message(session_id, retained_id, 2)];
        updated.message_observation_revisions = BTreeMap::from([(retained_id.to_string(), 2)]);
        updated.message_delivery_replays = BTreeMap::new();
        updated.updated_at = 3;
        db.apply_batch(&SessionDatabaseBatch {
            writes: vec![SessionDatabaseWrite {
                revision: 2,
                expected_revision: Some(1),
                previous_message_revision: 2,
                record: updated,
                appended_events: Vec::new(),
            }],
            ..SessionDatabaseBatch::default()
        })
        .expect("prune evicted message and dependent replay rows");

        let conn = db.lock_connection().expect("lock");
        let replay_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM workflow_session_message_delivery_replays
                 WHERE session_id = ?1",
                params![session_id],
                |row| row.get(0),
            )
            .expect("count replays");
        assert_eq!(replay_count, 0);
        let evicted_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM workflow_session_messages
                 WHERE session_id = ?1 AND message_id = ?2",
                params![session_id, evicted_id],
                |row| row.get(0),
            )
            .expect("count evicted");
        assert_eq!(evicted_count, 0);
    }

    #[test]
    fn delivery_replay_reconcile_prunes_stale_scope_and_allows_reuse() {
        let (_temp, db) = open_test_db("replay_reconcile");
        let session_id = "wc_sess_deliveryreplayrecon001";
        let message_a = "wc_msg_deliveryreplayrecon0001";
        let message_b = "wc_msg_deliveryreplayrecon0002";
        let mut record = minimal_record(session_id);
        record.messages = vec![minimal_message(session_id, message_a, 1)];
        record.message_observation_revisions = BTreeMap::from([(message_a.to_string(), 1)]);
        record.message_observation_revision = 1;
        record.message_delivery_replays.insert(
            REPLAY_SCOPE_A.to_string(),
            SessionMessageDeliveryReplay {
                payload_fingerprint: REPLAY_FINGERPRINT.to_string(),
                message_id: message_a.to_string(),
            },
        );
        let mut conn = db.lock_connection().expect("lock");
        let tx = conn.transaction().expect("begin");
        insert_session_snapshot(&tx, &record, Vec::new(), 1).expect("seed");
        tx.commit().expect("commit");

        let mut updated = record;
        updated.messages = vec![minimal_message(session_id, message_b, 2)];
        updated.message_observation_revisions = BTreeMap::from([(message_b.to_string(), 2)]);
        updated.message_observation_revision = 2;
        updated.message_delivery_replays = BTreeMap::from([(
            REPLAY_SCOPE_A.to_string(),
            SessionMessageDeliveryReplay {
                payload_fingerprint: REPLAY_FINGERPRINT.to_string(),
                message_id: message_b.to_string(),
            },
        )]);
        updated.updated_at = 2;
        db.apply_batch(&SessionDatabaseBatch {
            writes: vec![SessionDatabaseWrite {
                revision: 2,
                expected_revision: Some(1),
                previous_message_revision: 1,
                record: updated,
                appended_events: Vec::new(),
            }],
            ..SessionDatabaseBatch::default()
        })
        .expect("reconcile replay map");

        let row = db
            .inventory()
            .expect("inventory")
            .into_iter()
            .next()
            .expect("session");
        let restored = db.load(&row, 0).expect("load");
        assert_eq!(
            restored
                .message_delivery_replays
                .get(REPLAY_SCOPE_A)
                .map(|replay| replay.message_id.as_str()),
            Some(message_b)
        );

        let mut scope_b_record = restored;
        scope_b_record.messages = vec![minimal_message(session_id, message_b, 2)];
        scope_b_record.message_delivery_replays.insert(
            REPLAY_SCOPE_B.to_string(),
            SessionMessageDeliveryReplay {
                payload_fingerprint: REPLAY_FINGERPRINT.to_string(),
                message_id: message_b.to_string(),
            },
        );
        db.apply_batch(&SessionDatabaseBatch {
            writes: vec![SessionDatabaseWrite {
                revision: 3,
                expected_revision: Some(2),
                previous_message_revision: 2,
                record: scope_b_record,
                appended_events: Vec::new(),
            }],
            ..SessionDatabaseBatch::default()
        })
        .expect("add scope b");

        let conn = db.lock_connection().expect("lock");
        let scope_a_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM workflow_session_message_delivery_replays
                 WHERE session_id = ?1 AND scope_key = ?2",
                params![session_id, REPLAY_SCOPE_A],
                |row| row.get(0),
            )
            .expect("scope a count");
        assert_eq!(scope_a_count, 0);
    }

    #[test]
    fn retirement_rejects_project_identity_mismatch() {
        let (_temp, db) = open_test_db("retirement_project_identity");
        let session_id = "wc_sess_retireprojectidentity1";
        let durable_identity = SessionProjectIdentity {
            root_fingerprint: Some(format!(
                "{PROJECT_ROOT_FINGERPRINT_PREFIX}{}",
                "a".repeat(64)
            )),
            registered_path: None,
            registration_incarnation: "550e8400-e29b-41d4-a716-446655440000".to_string(),
        };
        let mismatched_identity = SessionProjectIdentity {
            root_fingerprint: Some(format!(
                "{PROJECT_ROOT_FINGERPRINT_PREFIX}{}",
                "b".repeat(64)
            )),
            registered_path: None,
            registration_incarnation: "660e8400-e29b-41d4-a716-446655440000".to_string(),
        };
        let mut record = minimal_record(session_id);
        record.project_identity = Some(durable_identity);
        let mut conn = db.lock_connection().expect("lock");
        let tx = conn.transaction().expect("begin");
        insert_session_snapshot(&tx, &record, Vec::new(), 1).expect("seed");
        tx.commit().expect("commit");

        let err = db
            .apply_batch(&SessionDatabaseBatch {
                retirements: vec![SessionDatabaseRetirement {
                    session_id: session_id.to_string(),
                    created_at: record.created_at,
                    project: record.project.clone(),
                    owner_authority_fingerprint: record.owner_authority_fingerprint.clone(),
                    project_identity: Some(mismatched_identity),
                    expected_revision: Some(1),
                    retention_tombstone: None,
                }],
                ..SessionDatabaseBatch::default()
            })
            .expect_err("identity mismatch must fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn delivery_replay_fingerprint_change_fails_closed() {
        let (temp, db) = open_test_db("replay_fingerprint");
        let session_id = "wc_sess_replayfingerprint00001";
        let message_id = "wc_msg_replayfingerprint00001";
        let mut record = minimal_record(session_id);
        record.messages = vec![minimal_message(session_id, message_id, 1)];
        record.message_observation_revisions = BTreeMap::from([(message_id.to_string(), 1)]);
        record.message_observation_revision = 1;
        record.message_delivery_replays.insert(
            REPLAY_SCOPE_A.to_string(),
            SessionMessageDeliveryReplay {
                payload_fingerprint: REPLAY_FINGERPRINT.to_string(),
                message_id: message_id.to_string(),
            },
        );
        {
            let mut conn = db.lock_connection().expect("lock");
            let tx = conn.transaction().expect("begin");
            insert_session_snapshot(&tx, &record, Vec::new(), 1).expect("seed");
            tx.commit().expect("commit");
        }
        let mut updated = record.clone();
        updated.message_delivery_replays.insert(
            REPLAY_SCOPE_A.to_string(),
            SessionMessageDeliveryReplay {
                payload_fingerprint: "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                    .to_string(),
                message_id: message_id.to_string(),
            },
        );
        updated.updated_at = 2;
        let err = db
            .apply_batch(&SessionDatabaseBatch {
                writes: vec![SessionDatabaseWrite {
                    revision: 2,
                    expected_revision: Some(1),
                    previous_message_revision: 1,
                    record: updated,
                    appended_events: Vec::new(),
                }],
                ..SessionDatabaseBatch::default()
            })
            .expect_err("fingerprint change must fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        drop(db);
        drop(temp);
    }

    #[test]
    fn configurable_event_tail_above_default_cap_persists() {
        let (temp, db) = open_test_db("event_tail_above_default");
        let session_id = "wc_sess_eventtailabovedefault01";
        let tail_len = DEFAULT_MAX_EVENTS_PER_SESSION + 1;
        let events: Vec<Arc<SessionEvent>> = (1_u64..=tail_len as u64)
            .map(|index| {
                minimal_event(
                    session_id,
                    &format!("evt_eventtailabovedefault{:012x}", index),
                    index as i64,
                )
            })
            .collect();
        let mut record = minimal_record(session_id);
        record.events = events.clone();
        record.events_observed = tail_len as u64;
        db.apply_batch(&SessionDatabaseBatch {
            writes: vec![SessionDatabaseWrite {
                revision: 1,
                expected_revision: None,
                previous_message_revision: 0,
                record,
                appended_events: events,
            }],
            ..SessionDatabaseBatch::default()
        })
        .expect("persist tail above default cap");

        let conn = db.lock_connection().expect("lock");
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM workflow_session_events WHERE session_id = ?1",
                params![session_id],
                |row| row.get(0),
            )
            .expect("count events");
        assert_eq!(count, tail_len as i64);
        drop(conn);
        drop(db);
        drop(temp);
    }

    #[test]
    fn delivery_replay_rebind_fails_while_original_message_retained() {
        let (temp, db) = open_test_db("replay_rebind_retained");
        let session_id = "wc_sess_replayrebindretained01";
        let message_a = "wc_msg_replayrebindretained0001";
        let message_b = "wc_msg_replayrebindretained0002";
        let mut record = minimal_record(session_id);
        record.messages = vec![
            minimal_message(session_id, message_a, 1),
            minimal_message(session_id, message_b, 2),
        ];
        record.message_observation_revisions =
            BTreeMap::from([(message_a.to_string(), 1), (message_b.to_string(), 2)]);
        record.message_observation_revision = 2;
        record.message_delivery_replays.insert(
            REPLAY_SCOPE_A.to_string(),
            SessionMessageDeliveryReplay {
                payload_fingerprint: REPLAY_FINGERPRINT.to_string(),
                message_id: message_a.to_string(),
            },
        );
        {
            let mut conn = db.lock_connection().expect("lock");
            let tx = conn.transaction().expect("begin");
            insert_session_snapshot(&tx, &record, Vec::new(), 1).expect("seed");
            tx.commit().expect("commit");
        }

        let mut updated = record.clone();
        updated.message_delivery_replays.insert(
            REPLAY_SCOPE_A.to_string(),
            SessionMessageDeliveryReplay {
                payload_fingerprint: REPLAY_FINGERPRINT.to_string(),
                message_id: message_b.to_string(),
            },
        );
        updated.updated_at = 3;
        let err = db
            .apply_batch(&SessionDatabaseBatch {
                writes: vec![SessionDatabaseWrite {
                    revision: 2,
                    expected_revision: Some(1),
                    previous_message_revision: 2,
                    record: updated,
                    appended_events: Vec::new(),
                }],
                ..SessionDatabaseBatch::default()
            })
            .expect_err("rebind while original message retained must fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        drop(db);
        drop(temp);
    }

    #[test]
    fn tombstone_delete_is_idempotent_when_row_already_absent() {
        let (_temp, db) = open_test_db("tombstone_idempotent");
        let session_id = "wc_sess_tombstoneidempotent0001";
        let record = minimal_record(session_id);
        let tombstone = SessionRetentionTombstone {
            session_id: session_id.to_string(),
            owner_authority_fingerprint: record.owner_authority_fingerprint.clone(),
            incarnation_fingerprint: workflow_session_incarnation_fingerprint(
                &record.session_id,
                record.created_at,
                record.project.as_deref(),
                &record.owner_authority_fingerprint,
            ),
            expiry_ordinal: 1,
        };
        db.apply_batch(&SessionDatabaseBatch {
            tombstone_deletes: vec![SessionDatabaseTombstoneDelete {
                session_id: session_id.to_string(),
                owner_authority_fingerprint: tombstone.owner_authority_fingerprint.clone(),
                incarnation_fingerprint: tombstone.incarnation_fingerprint.clone(),
            }],
            ..SessionDatabaseBatch::default()
        })
        .expect("first absent tombstone delete");
        db.apply_batch(&SessionDatabaseBatch {
            tombstone_deletes: vec![SessionDatabaseTombstoneDelete {
                session_id: session_id.to_string(),
                owner_authority_fingerprint: tombstone.owner_authority_fingerprint,
                incarnation_fingerprint: tombstone.incarnation_fingerprint,
            }],
            ..SessionDatabaseBatch::default()
        })
        .expect("second absent tombstone delete");
    }
}
