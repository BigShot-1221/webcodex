//! In-memory SessionStore: create sessions, record events, and trigger persistence.
//!
//! All durable session-map mutations flow through `SessionStoreInner` helpers.
//! Callers outside this module use `SessionStore` methods only.
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use webcodex_core::runner_job_lifecycle::RunnerJobLifecycle;
use webcodex_core::validation_identity::{
    assertion_validation_identity, is_validation_execution_identity,
};
use webcodex_core::workflow_session_contract::{is_safe_job_id, PermissionDecision, SessionMode};
use webcodex_tool_contracts::{
    runtime_tool_activity_semantics, runtime_tool_session_evidence_policy, ToolActivityKind,
    ToolSessionLifecycleEffect,
};

use super::assignment::{
    assignment_fence_fingerprint, assignment_fence_from_state, current_assignment_state,
    open_assignment_state, validate_assignment_fence,
};
use super::console::{
    build_detail as build_console_detail, build_list_item as build_console_list_item,
    normalize_console_activity_limit, normalize_console_session_limit, CachedConsoleList,
    ConsoleValidationHooks, WorkflowSessionConsoleDetail, WorkflowSessionConsoleList,
    WorkflowSessionConsoleListItem,
};
use super::events::{
    actual_failure_kind_for_tool_result, changed_paths_for_tool_call,
    changed_paths_for_tool_result, classify_error_message, classify_failure_expectation,
    context_result_summary_for_tool_result, diff_review_like_for_tool, extract_job_id,
    extract_project, is_valid_session_id, observed_input_paths_for_tool,
    observed_paths_for_successful_result, persistent_shell_event_evidence_for_tool_result,
    sanitize_tool_execution_state, session_input_summary_for_tool,
    validation_output_summary_for_tool_result, SessionToolContract,
};
use self::dirty::{event_ordinal_for_id, retained_event_start_for_record, retirement_from_stored_session};
use super::incarnation::workflow_session_incarnation_fingerprint;
use super::model::{
    CodingSessionError, CodingSessionOutcome, CodingSessionRequest, ColdSessionRecord,
    CompleteSessionMessageInput, CompleteSessionMessageOutcome, PersistedRetentionTombstoneRow,
    PersistedSessionLedger, PersistedSessionRecord, PersistedSessionSnapshot,
    PersistentShellEventEvidence, PostSessionMessageInput, ReplaceSessionMessageInput,
    ReplaceSessionMessageOutcome, SessionCloseError, SessionCloseOutcome, SessionCounts,
    SessionCreateOptions, SessionDiscoveryItem, SessionDiscoveryPage, SessionEvent,
    SessionExecutionContext, SessionExecutionContextUpdateError,
    SessionExecutionContextUpdateOutcome, SessionGuardDenial, SessionGuards, SessionLifecycle,
    SessionLifecycleDenial, SessionMessage, SessionMessageClosureKind, SessionMessageDelivery,
    SessionMessageDeliveryOutcome, SessionMessageDeliveryReplay, SessionMessageError,
    SessionMessagePriority, SessionMessageStatus, SessionProjectIdentity, SessionRecord,
    SessionRetentionTombstone,
    SessionStoreStatus, SessionSummary, SessionTransport, StoredSession, ToolCallExpectation,
    ToolCallRecorderMetadata, ToolCallStart, ToolEffectEventEvidence,
    WithdrawSessionMessageOutcome, CALL_ID_PREFIX, DEFAULT_MAX_EVENTS_PER_SESSION,
    DEFAULT_MAX_MESSAGES_PER_SESSION, DEFAULT_MAX_RETAINED_CLOSED_SESSIONS, DEFAULT_MAX_SESSIONS,
    DEFAULT_SUMMARY_LIMIT, EVENT_ID_PREFIX, MAX_CODING_INSTRUCTION_CHARS, MAX_INPUT_ARRAY_ITEMS,
    MAX_MATERIALIZED_VALIDATION_JOB_IDS, MAX_MESSAGE_DELIVERY_KEY_CHARS, MAX_SUMMARY_LIMIT,
    MESSAGE_ID_PREFIX, SESSION_ID_PREFIX, SESSION_LEDGER_VERSION,
};
use super::persistence::{
    cold_session_from_persisted, is_lower_hex_sha256, load_persisted_ledger,
    materialize_cold_session, restore_session_database, write_ledger_atomic,
};
use super::persistence::database::SessionDatabase;
use super::query::{
    build_messages_summary, is_valid_completion_id, validate_message_tags, validate_message_text,
    validate_resolution_text,
};
use super::util::{
    bound_event_error_summary, bound_summary_string, now_ts, redact_and_bound_instruction,
    redact_and_bound_value,
};

#[cfg(test)]
#[path = "identifier_tests.rs"]
mod identifier_tests;

mod recency;
mod residency;
mod dirty;
mod batch_plan;

pub(crate) use batch_plan::{
    acknowledge_batch, has_bounded_flush_unit, has_deferred_retention_work,
    mark_planned_writes_in_flight, sqlite_retention_ram_within_bounds,
    unrecoverable_retention_stall_summary, abandon_in_flight_writes, AcknowledgeBatchOutcome,
    BatchPlan, BatchPlanError, SessionWritePlan, SQLITE_BATCH_MAX_UNITS,
};
pub(crate) use dirty::{DatabasePersistenceQueue, SeedDatabasePersistenceError, SessionDirtyState};

use recency::SessionRecency;
mod writer;
use writer::{
    durable_write_uses_sqlite, flush_sqlite_batch, flush_sqlite_batch_shutdown_drain,
    ledger_io_error_outcome, record_persist_outcome, transient_backoff,
    LedgerWriterGuard, PersistFlushDisposition, PersistFlushOutcome,
    MAX_CONSECUTIVE_TRANSIENT_FAILURES,
};

#[cfg(test)]
mod borrowed_projection_tests;
#[cfg(test)]
mod event_tests;
#[cfg(test)]
mod scale_tests;

/// High bit: admission closed for new business mutations and observation barriers.
/// Low bits: in-flight admitted business mutation count (Stage J.6).
const MUTATION_GATE_CLOSED: usize = 1_usize << (usize::BITS - 1);
const MUTATION_GATE_COUNT_MASK: usize = MUTATION_GATE_CLOSED - 1;

#[derive(Debug)]
struct SessionDurability {
    /// Holds the synchronous fallback lane from admission through the
    /// spawned blocking write so cancellation cannot skip an admitted mutation.
    fallback_admission: Arc<tokio::sync::Mutex<()>>,
    /// Atomic in-flight mutation admission gate: closed bit plus active count.
    mutation_gate: AtomicUsize,
    drain_notify: tokio::sync::Notify,
}

impl SessionDurability {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            fallback_admission: Arc::new(tokio::sync::Mutex::new(())),
            mutation_gate: AtomicUsize::new(0),
            drain_notify: tokio::sync::Notify::new(),
        })
    }

    fn close_mutation_admission(&self) {
        self.mutation_gate
            .fetch_or(MUTATION_GATE_CLOSED, Ordering::AcqRel);
        if self.mutation_gate.load(Ordering::Acquire) & MUTATION_GATE_COUNT_MASK == 0 {
            self.drain_notify.notify_waiters();
        }
    }

    fn refuses_mutation_admission(&self) -> bool {
        self.mutation_gate.load(Ordering::Acquire) & MUTATION_GATE_CLOSED != 0
    }

    fn try_admit_mutation(&self) -> Result<(), ()> {
        loop {
            let current = self.mutation_gate.load(Ordering::Acquire);
            if current & MUTATION_GATE_CLOSED != 0 {
                return Err(());
            }
            let count = current & MUTATION_GATE_COUNT_MASK;
            if count >= MUTATION_GATE_COUNT_MASK {
                return Err(());
            }
            let next = current + 1;
            if self
                .mutation_gate
                .compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(());
            }
        }
    }

    fn release_mutation_token(&self) {
        let previous = self.mutation_gate.fetch_sub(1, Ordering::Release);
        let count_after = previous.wrapping_sub(1) & MUTATION_GATE_COUNT_MASK;
        if previous & MUTATION_GATE_CLOSED != 0 && count_after == 0 {
            self.drain_notify.notify_waiters();
        }
    }

    async fn await_mutation_drain(&self) {
        loop {
            let mut notified = std::pin::pin!(self.drain_notify.notified());
            notified.as_mut().enable();
            let current = self.mutation_gate.load(Ordering::Acquire);
            if current & MUTATION_GATE_CLOSED == 0 {
                return;
            }
            if current & MUTATION_GATE_COUNT_MASK == 0 {
                return;
            }
            notified.await;
        }
    }
}

/// RAII lease for one admitted durable business mutation. Drop decrements the
/// in-flight gate so shutdown can drain after close. In-memory stores use a
/// no-op token that does not touch the gate.
pub(super) struct DurableWorkToken {
    durability: Option<Arc<SessionDurability>>,
}

impl DurableWorkToken {
    fn noop() -> Self {
        Self { durability: None }
    }
}

impl Drop for DurableWorkToken {
    fn drop(&mut self) {
        if let Some(durability) = self.durability.as_ref() {
            durability.release_mutation_token();
        }
    }
}

/// Gate policy for synchronous persistence writes under `persistence_write_mutex`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SyncPersistWriteMode {
    /// Ordinary sync write after `ensure_mutation_persistence_admitted`; respects mutation-admission close.
    Normal,
    /// Durable work already counted by the shutdown in-flight gate (`DurableWorkToken`) and/or
    /// caller-held `fallback_admission` (async sync-fallback blocking lane); bypasses admission
    /// close but still requires no persistence fatal.
    AdmittedWork,
    /// Shutdown final flush while `fallback_admission` and `persistence_write_mutex` are held.
    ShutdownFinalFlushUnderLock,
}

/// Held across a durable business mutation and its completion barrier.
pub(super) struct DurableMutationAdmission {
    work_token: DurableWorkToken,
    writer: Option<Arc<LedgerWriterGuard>>,
    fallback_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

/// Durable observation barrier (assignment read, message revision fence) without
/// counting as an in-flight business mutation.
pub(super) struct DurableObservationAdmission {
    writer: Option<Arc<LedgerWriterGuard>>,
    fallback_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

#[derive(Debug, Clone)]
pub struct SessionStore {
    /// Shared session map and LRU metadata.
    /// `pub(super)` so sibling modules can lock and call `SessionStoreInner`
    /// transition helpers without touching the maps directly.
    pub(super) inner: Arc<Mutex<SessionStoreInner>>,
    persistence_write_mutex: Arc<Mutex<()>>,
    session_durability: Arc<SessionDurability>,
    /// Background durable writer for persistent stores (SQLite batch or JSON
    /// snapshot per backend). `None` for in-memory stores or when the writer
    /// thread could not be spawned (mutations then use the synchronous path).
    writer: Option<Arc<LedgerWriterGuard>>,
    /// Process-local wake signal only; durable observation truth remains in the
    /// persisted per-Session revision bookkeeping.
    pub(super) message_observation_notify: tokio::sync::watch::Sender<u64>,
    /// One-shot terminal persistence failure for supervision (Stage G).
    persistence_fatal: tokio::sync::watch::Sender<Option<String>>,
    #[cfg(any(test, feature = "root-test-support"))]
    fail_next_coding_continuity_precommit: Arc<std::sync::atomic::AtomicBool>,
}

#[derive(Debug)]
pub(super) struct SessionStoreInner {
    /// Durable workflow sessions. Mutated only via the helpers below.
    sessions: HashMap<String, StoredSession>,
    lru: SessionRecency,
    hot_session_capacity_target: usize,
    historical_session_retention_limit: usize,
    capacity_evictions: u64,
    /// Closed-session retention identities, keyed by monotonic expiry ordinal.
    tombstones: BTreeMap<u64, SessionRetentionTombstone>,
    tombstone_ids: HashMap<String, u64>,
    next_expiry_ordinal: u64,
    expiry_ordinal_exhausted: bool,
    max_events_per_session: usize,
    persistence: Option<SessionPersistence>,
    pub(crate) database_persistence: DatabasePersistenceQueue,
}

#[derive(Debug, Clone)]
struct SessionPersistence {
    /// Legacy JSON ledger path: one-time import input for DB-backed startup,
    /// JSON compatibility durable writes when `database` is unset, and migration reads.
    path: PathBuf,
    restored_sessions: usize,
    last_persist_error: Option<String>,
    /// SQLite durable backend; installed on production startup and used for
    /// incremental `apply_batch` flushes (steady state does not rewrite JSON).
    pub(super) database: Option<Arc<SessionDatabase>>,
    /// On-disk SQLite path for writer cost observation when `database` is installed.
    pub(super) durable_db_path: Option<PathBuf>,
}

impl SessionPersistence {
    pub(super) fn cost_observation_path(&self) -> &std::path::Path {
        self.durable_db_path
            .as_deref()
            .unwrap_or(self.path.as_path())
    }
}

impl Default for SessionStore {
    fn default() -> Self {
        Self::new_in_memory_with_limits(
            DEFAULT_MAX_SESSIONS,
            DEFAULT_MAX_RETAINED_CLOSED_SESSIONS,
            DEFAULT_MAX_EVENTS_PER_SESSION,
        )
    }
}

/// Result of a successful Cold-session rewrite for SQLite dirty registration.
pub(super) enum ColdRewriteOutcome {
    Changed {
        retained_event_start: u64,
    },
    EventUpsert {
        ordinal: u64,
        event: Arc<SessionEvent>,
        retained_event_start: u64,
    },
}

/// Cold rewrite CAS probe result (no `sessions` entry borrow held).
enum ColdRewriteCas {
    Matches,
    Stale(ColdSessionRecord),
    /// Defensive fail-closed: Active Cold is promoted before rewrite; callers on Closed Cold never promote.
    OccupiedHot,
    Missing,
}

impl SessionStore {
    pub fn new(max_sessions: usize, max_events_per_session: usize) -> Self {
        Self::new_in_memory(max_sessions, max_events_per_session)
    }

    pub fn new_in_memory(max_sessions: usize, max_events_per_session: usize) -> Self {
        Self::new_in_memory_with_limits(max_sessions, max_sessions, max_events_per_session)
    }

    pub fn new_in_memory_with_limits(
        hot_session_capacity_target: usize,
        historical_session_retention_limit: usize,
        max_events_per_session: usize,
    ) -> Self {
        let (message_observation_notify, _) = tokio::sync::watch::channel(0_u64);
        let (persistence_fatal, _) = tokio::sync::watch::channel(None);
        Self {
            inner: Arc::new(Mutex::new(SessionStoreInner {
                sessions: HashMap::<String, StoredSession>::new(),
                lru: SessionRecency::default(),
                hot_session_capacity_target,
                historical_session_retention_limit,
                capacity_evictions: 0,
                tombstones: BTreeMap::new(),
                tombstone_ids: HashMap::new(),
                next_expiry_ordinal: 1,
                expiry_ordinal_exhausted: false,
                max_events_per_session,
                persistence: None,
                database_persistence: DatabasePersistenceQueue::default(),
            })),
            persistence_write_mutex: Arc::new(Mutex::new(())),
            session_durability: SessionDurability::new(),
            writer: None,
            message_observation_notify,
            persistence_fatal,
            #[cfg(any(test, feature = "root-test-support"))]
            fail_next_coding_continuity_precommit: Arc::new(std::sync::atomic::AtomicBool::new(
                false,
            )),
        }
    }

    pub fn with_persistence(
        path: impl Into<PathBuf>,
        max_sessions: usize,
        max_events_per_session: usize,
    ) -> Self {
        Self::with_persistence_limits(path, max_sessions, max_sessions, max_events_per_session)
    }

    pub fn with_persistence_limits(
        path: impl Into<PathBuf>,
        hot_session_capacity_target: usize,
        historical_session_retention_limit: usize,
        max_events_per_session: usize,
    ) -> Self {
        let path = path.into();
        let restored = load_persisted_ledger(&path, max_events_per_session);
        let mut lru = SessionRecency::default();
        for id in &restored.lru {
            if let Some(record) = restored.sessions.get(id) {
                lru.touch(id, record.lifecycle() == SessionLifecycle::Closed);
            }
        }
        let mut state = SessionStoreInner {
            sessions: restored.sessions,
            lru,
            hot_session_capacity_target,
            historical_session_retention_limit,
            capacity_evictions: 0,
            tombstones: restored.tombstones,
            tombstone_ids: restored.tombstone_ids,
            next_expiry_ordinal: restored.next_expiry_ordinal,
            expiry_ordinal_exhausted: restored.expiry_ordinal_exhausted,
            max_events_per_session,
            persistence: Some(SessionPersistence {
                path,
                restored_sessions: 0,
                last_persist_error: restored.last_persist_error,
                database: None,
                durable_db_path: None,
            }),
            database_persistence: DatabasePersistenceQueue::default(),
        };
        // One prune path covers ordinary mutation and restore. Count Closed-row
        // removal here, then publish the post-prune session count.
        state.enforce_historical_retention_bound();
        state.enforce_tombstone_bound();
        if let Some(persistence) = state.persistence.as_mut() {
            persistence.restored_sessions = state.sessions.len();
        }
        let inner = Arc::new(Mutex::new(state));
        let persistence_write_mutex = Arc::new(Mutex::new(()));
        let (persistence_fatal, _) = tokio::sync::watch::channel(None);
        // Prefer the background writer so mutation paths never park a Tokio
        // worker on full-ledger serialize + disk I/O. If the OS thread cannot
        // be spawned, fall back to the synchronous write path.
        let writer = LedgerWriterGuard::spawn(
            Arc::clone(&inner),
            Arc::clone(&persistence_write_mutex),
            persistence_fatal.clone(),
        );
        let (message_observation_notify, _) = tokio::sync::watch::channel(0_u64);
        Self {
            inner,
            persistence_write_mutex,
            session_durability: SessionDurability::new(),
            writer,
            message_observation_notify,
            persistence_fatal,
            #[cfg(any(test, feature = "root-test-support"))]
            fail_next_coding_continuity_precommit: Arc::new(std::sync::atomic::AtomicBool::new(
                false,
            )),
        }
    }

    pub fn with_persistence_database(
        db_path: impl Into<PathBuf>,
        legacy_ledger_path: impl Into<PathBuf>,
        max_sessions: usize,
        max_events_per_session: usize,
    ) -> io::Result<Self> {
        Self::with_persistence_database_limits(
            db_path,
            legacy_ledger_path,
            max_sessions,
            max_sessions,
            max_events_per_session,
        )
    }

    /// Persist startup retention/tombstone queues before the background writer
    /// is published. Matches writer lock order: `persistence_write_mutex` then
    /// store inner (via `flush_sqlite_batch`). No-op when nothing is queued.
    fn flush_startup_sqlite_retention(
        inner: &Arc<Mutex<SessionStoreInner>>,
        persistence_write_mutex: &Arc<Mutex<()>>,
        persistence_fatal: &tokio::sync::watch::Sender<Option<String>>,
    ) -> io::Result<()> {
        {
            let mut guard = Self::lock_store_arc_inner(inner, persistence_fatal)?;
            if !sqlite_retention_ram_within_bounds(&guard) {
                guard.reenforce_sqlite_retention_bounds_after_durable_ack();
            }
            if !guard.database_persistence.has_pending_work() {
                if let Some(summary) = unrecoverable_retention_stall_summary(&guard) {
                    return Err(io::Error::new(io::ErrorKind::Other, summary));
                }
                if !sqlite_retention_ram_within_bounds(&guard) {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "session startup persistence flush blocked: retention RAM bounds exceeded",
                    ));
                }
                return Ok(());
            }
        }
        let _write_guard = persistence_write_mutex.lock().map_err(|_| {
            Self::signal_persistence_fatal_sender(
                persistence_fatal,
                "session persistence mutex poisoned",
            );
            io::Error::new(io::ErrorKind::Other, "session persistence mutex poisoned")
        })?;
        let database = {
            let guard = Self::lock_store_arc_inner(inner, persistence_fatal)?;
            if !durable_write_uses_sqlite(&guard) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "session startup persistence flush requires sqlite backend",
                ));
            }
            guard
                .persistence
                .as_ref()
                .and_then(|persistence| persistence.database.clone())
        };
        let Some(database) = database else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "session database handle missing for startup retention flush",
            ));
        };
        let mut consecutive_startup_transient_failures = 0u32;
        loop {
            {
                let guard = Self::lock_store_arc_inner(inner, persistence_fatal)?;
                if !guard.database_persistence.has_pending_work() {
                    if let Some(summary) = unrecoverable_retention_stall_summary(&guard) {
                        return Err(io::Error::new(io::ErrorKind::Other, summary));
                    }
                    if !sqlite_retention_ram_within_bounds(&guard) {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            "session startup persistence flush blocked: retention RAM bounds exceeded",
                        ));
                    }
                    return Ok(());
                }
            }
            let (_, outcome) = flush_sqlite_batch_shutdown_drain(inner, database.as_ref());
            if record_persist_outcome(inner, &outcome).is_err() {
                Self::signal_persistence_fatal_sender(
                    persistence_fatal,
                    "session store mutex poisoned",
                );
            }
            if matches!(
                outcome.disposition,
                PersistFlushDisposition::WriterFatal | PersistFlushDisposition::CommittedButUnacked
            ) {
                if let Some(message) = outcome.error_summary.as_deref() {
                    Self::signal_persistence_fatal_sender(persistence_fatal, message);
                }
            }
            match outcome.disposition {
                PersistFlushDisposition::Success => {
                    consecutive_startup_transient_failures = 0;
                    if let Some(err) = outcome.allocation_error {
                        return Err(io::Error::new(io::ErrorKind::Other, err));
                    }
                    if !outcome.more_pending {
                        let guard = Self::lock_store_arc_inner(inner, persistence_fatal)?;
                        if let Some(summary) = unrecoverable_retention_stall_summary(&guard) {
                            return Err(io::Error::new(io::ErrorKind::Other, summary));
                        }
                        if !sqlite_retention_ram_within_bounds(&guard) {
                            return Err(io::Error::new(
                                io::ErrorKind::Other,
                                "session startup persistence flush blocked: retention RAM bounds exceeded",
                            ));
                        }
                        return Ok(());
                    }
                }
                PersistFlushDisposition::TransientRetry => {
                    let backoff = transient_backoff(consecutive_startup_transient_failures);
                    consecutive_startup_transient_failures =
                        consecutive_startup_transient_failures.saturating_add(1);
                    if consecutive_startup_transient_failures
                        >= MAX_CONSECUTIVE_TRANSIENT_FAILURES
                    {
                        let message = outcome
                            .error_summary
                            .map(|summary| {
                                format!(
                                    "session startup persistence transient retry budget exhausted: {summary}"
                                )
                            })
                            .unwrap_or_else(|| {
                                "session startup persistence transient retry budget exhausted"
                                    .to_string()
                            });
                        return Err(io::Error::new(io::ErrorKind::Other, message));
                    }
                    std::thread::sleep(backoff);
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        outcome.error_summary.unwrap_or_else(|| {
                            "session startup persistence flush failed".to_string()
                        }),
                    ));
                }
            }
        }
    }

    /// SQLite-backed durable store: restore from shared `webcodex.db`, seed
    /// revision/CAS state, then enforce RAM retention bounds. Fails closed on
    /// any restore or seed error — no JSON ledger fallback.
    pub fn with_persistence_database_limits(
        db_path: impl Into<PathBuf>,
        legacy_ledger_path: impl Into<PathBuf>,
        hot_session_capacity_target: usize,
        historical_session_retention_limit: usize,
        max_events_per_session: usize,
    ) -> io::Result<Self> {
        let db_path = db_path.into();
        let legacy_ledger_path = legacy_ledger_path.into();
        let restored = restore_session_database(
            &db_path,
            &legacy_ledger_path,
            max_events_per_session,
        )?;
        let ledger = restored.ledger;
        let mut lru = SessionRecency::default();
        for id in &ledger.lru {
            if let Some(record) = ledger.sessions.get(id) {
                lru.touch(id, record.lifecycle() == SessionLifecycle::Closed);
            }
        }
        let mut state = SessionStoreInner {
            sessions: ledger.sessions,
            lru,
            hot_session_capacity_target,
            historical_session_retention_limit,
            capacity_evictions: 0,
            tombstones: ledger.tombstones,
            tombstone_ids: ledger.tombstone_ids,
            next_expiry_ordinal: ledger.next_expiry_ordinal,
            expiry_ordinal_exhausted: ledger.expiry_ordinal_exhausted,
            max_events_per_session,
            persistence: Some(SessionPersistence {
                path: legacy_ledger_path,
                restored_sessions: 0,
                last_persist_error: ledger.last_persist_error,
                database: Some(Arc::clone(&restored.database)),
                durable_db_path: Some(db_path),
            }),
            database_persistence: DatabasePersistenceQueue::default(),
        };
        state
            .seed_database_persistence_state(
                &restored.committed_revisions,
                &restored.committed_message_revisions,
                restored.durable_revision_high_water,
            )
            .map_err(|err| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("session database persistence seed failed: {err:?}"),
                )
            })?;
        state.enforce_historical_retention_bound();
        state.enforce_tombstone_bound();
        if let Some(persistence) = state.persistence.as_mut() {
            persistence.restored_sessions = state.sessions.len();
        }
        let inner = Arc::new(Mutex::new(state));
        let persistence_write_mutex = Arc::new(Mutex::new(()));
        let (persistence_fatal, _) = tokio::sync::watch::channel(None);
        Self::flush_startup_sqlite_retention(
            &inner,
            &persistence_write_mutex,
            &persistence_fatal,
        )?;
        let writer = LedgerWriterGuard::spawn(
            Arc::clone(&inner),
            Arc::clone(&persistence_write_mutex),
            persistence_fatal.clone(),
        );
        let (message_observation_notify, _) = tokio::sync::watch::channel(0_u64);
        Ok(Self {
            inner,
            persistence_write_mutex,
            session_durability: SessionDurability::new(),
            writer,
            message_observation_notify,
            persistence_fatal,
            #[cfg(any(test, feature = "root-test-support"))]
            fail_next_coding_continuity_precommit: Arc::new(std::sync::atomic::AtomicBool::new(
                false,
            )),
        })
    }

    /// Subscribe to the one-shot terminal persistence failure signal, if any.
    pub fn persistence_fatal_receiver(&self) -> tokio::sync::watch::Receiver<Option<String>> {
        self.persistence_fatal.subscribe()
    }

    /// Idempotently shut down the background ledger writer, waiting for the bounded
    /// shutdown flush budget on a blocking pool thread. When the writer thread was
    /// not spawned, performs one bounded synchronous flush before the health check.
    /// When terminal persistence failure is already signaled, still joins an active
    /// writer on a blocking pool thread (no sync fallback replay) before the health check.
    pub async fn shutdown_persistence(&self) -> io::Result<()> {
        self.session_durability.close_mutation_admission();
        self.session_durability.await_mutation_drain().await;

        if let Some(fatal_reason) = self.persistence_fatal_reason() {
            if let Some(writer) = self.writer.as_ref() {
                let writer = Arc::clone(writer);
                let join_result = tokio::task::spawn_blocking(move || {
                    writer.enter_terminal_flush_failure(fatal_reason);
                    writer.join_after_shutdown()
                })
                .await
                .map_err(|_| io::Error::other("session persistence shutdown task failed"))?;
                if join_result.is_err() {
                    return join_result;
                }
            } else if self.uses_sync_fallback_persistence() {
                let store = self.clone();
                tokio::task::spawn_blocking(move || {
                    store.shutdown_sync_fallback_drain_in_flight()
                })
                .await
                .map_err(|_| io::Error::other("session persistence shutdown task failed"))??;
            }
            return self.shutdown_persistence_health_check();
        }
        let join_result = if let Some(writer) = self.writer.as_ref() {
            let writer = Arc::clone(writer);
            tokio::task::spawn_blocking(move || writer.join_after_shutdown())
                .await
                .map_err(|_| io::Error::other("session persistence shutdown task failed"))?
        } else if self.uses_sync_fallback_persistence() {
            let store = self.clone();
            tokio::task::spawn_blocking(move || store.shutdown_sync_fallback_final_flush())
                .await
                .map_err(|_| io::Error::other("session persistence shutdown task failed"))??;
            Ok(())
        } else {
            Ok(())
        };
        if join_result.is_err() {
            return join_result;
        }
        if self.persistence_fatal_reason().is_some() {
            return self.shutdown_persistence_health_check();
        }
        self.shutdown_persistence_health_check()
    }

    /// Durable store on the synchronous fallback path (`writer` absent, persistence enabled).
    fn uses_sync_fallback_persistence(&self) -> bool {
        self.writer.is_none()
            && self
                .try_lock_inner()
                .and_then(|inner| inner.persistence.as_ref().map(|_| ()))
                .is_some()
    }

    /// Drain admitted fallback mutations and in-flight sync writes (no new flush).
    fn shutdown_sync_fallback_drain_in_flight(&self) -> io::Result<()> {
        let _admission = self.session_durability.fallback_admission.blocking_lock();
        self.persistence_write_mutex
            .lock()
            .map_err(|_| {
                self.signal_persistence_fatal("session persistence mutex poisoned");
                io::Error::new(
                    io::ErrorKind::Other,
                    "session persistence mutex poisoned",
                )
            })
            .map(|_| ())
    }

    /// One bounded final sync flush for shutdown when no background writer is active.
    /// Lock order: `fallback_admission`, then `persistence_write_mutex` (same as durable path).
    fn shutdown_sync_fallback_final_flush(&self) -> io::Result<()> {
        let _admission = self.session_durability.fallback_admission.blocking_lock();
        let _write_guard = self.persistence_write_mutex.lock().map_err(|_| {
            self.signal_persistence_fatal("session persistence mutex poisoned");
            io::Error::new(
                io::ErrorKind::Other,
                "session persistence mutex poisoned",
            )
        })?;
        if self.persistence_fatal_reason().is_some() {
            return Ok(());
        }
        self.sync_persist_write_after_lock(SyncPersistWriteMode::ShutdownFinalFlushUnderLock)
            .map_err(|()| io::Error::other("session shutdown final flush failed"))?;
        self.sync_persist_write_body(write_ledger_atomic)
            .map_err(|()| io::Error::other("session persistence sync fallback flush failed"))
    }

    fn shutdown_persistence_health_check(&self) -> io::Result<()> {
        if self.persistence_health_after_barrier().is_ok() {
            return Ok(());
        }
        let message = self
            .persistence_fatal_reason()
            .or_else(|| {
                self.try_lock_inner().and_then(|inner| {
                    inner
                        .persistence
                        .as_ref()
                        .and_then(|persistence| persistence.last_persist_error.clone())
                })
            })
            .unwrap_or_else(|| "session persistence health check failed".to_string());
        Err(io::Error::other(message))
    }

    pub(super) fn signal_persistence_fatal_sender(
        sender: &tokio::sync::watch::Sender<Option<String>>,
        message: impl Into<String>,
    ) {
        let message = message.into();
        let _ = sender.send_if_modified(|current| {
            if current.is_none() {
                *current = Some(message);
                true
            } else {
                false
            }
        });
    }

    fn signal_persistence_fatal(&self, message: impl Into<String>) {
        Self::signal_persistence_fatal_sender(&self.persistence_fatal, message);
    }

    pub(super) fn borrow_persistence_fatal(
        sender: &tokio::sync::watch::Sender<Option<String>>,
    ) -> Option<String> {
        sender.subscribe().borrow().clone()
    }

    fn persistence_fatal_reason(&self) -> Option<String> {
        Self::borrow_persistence_fatal(&self.persistence_fatal)
    }

    pub const SESSION_PERSISTENCE_UNAVAILABLE: &str = "session_persistence_unavailable";
    pub const SESSION_PERSISTENCE_UNCERTAIN: &str = "session_persistence_uncertain";

    fn durable_persistence_configured(&self) -> bool {
        self.try_lock_inner()
            .and_then(|inner| inner.persistence.as_ref().map(|_| ()))
            .is_some()
    }

    /// Fail-closed admission for new Session mutations when durable persistence
    /// is unavailable. In-memory stores and read-only paths skip this check.
    ///
    /// `persistence_fatal` is checked before and after the durable probe because
    /// `durable_persistence_configured` uses `try_lock_inner`, which signals fatal on poison.
    pub(super) fn ensure_mutation_persistence_admitted(&self) -> Result<DurableWorkToken, ()> {
        if self.persistence_fatal_reason().is_some() {
            return Err(());
        }
        if !self.durable_persistence_configured() {
            if self.persistence_fatal_reason().is_some() {
                return Err(());
            }
            return Ok(DurableWorkToken::noop());
        }
        if self.persistence_fatal_reason().is_some() {
            return Err(());
        }
        if let Some(writer) = &self.writer {
            if writer.refuses_new_dirty_marks() {
                return Err(());
            }
        }
        if self.try_lock_inner().is_some_and(|inner| {
            inner.database_persistence.is_seeded()
                && inner
                    .database_persistence
                    .retention_queues_prevent_mutation_admission()
        }) {
            return Err(());
        }
        if self.session_durability.refuses_mutation_admission() {
            return Err(());
        }
        self.session_durability.try_admit_mutation()?;
        if self.persistence_fatal_reason().is_some() {
            self.session_durability.release_mutation_token();
            return Err(());
        }
        if let Some(writer) = &self.writer {
            if writer.refuses_new_dirty_marks() {
                self.session_durability.release_mutation_token();
                return Err(());
            }
        }
        if self.try_lock_inner().is_some_and(|inner| {
            inner.database_persistence.is_seeded()
                && inner
                    .database_persistence
                    .retention_queues_prevent_mutation_admission()
        }) {
            self.session_durability.release_mutation_token();
            return Err(());
        }
        Ok(DurableWorkToken {
            durability: Some(Arc::clone(&self.session_durability)),
        })
    }

    /// Fail-closed admission for observation-only durable barriers. Does not
    /// increment the business mutation in-flight count.
    pub(super) fn ensure_observation_persistence_admitted(&self) -> Result<(), ()> {
        if self.persistence_fatal_reason().is_some() {
            return Err(());
        }
        if !self.durable_persistence_configured() {
            if self.persistence_fatal_reason().is_some() {
                return Err(());
            }
            return Ok(());
        }
        if self.persistence_fatal_reason().is_some() {
            return Err(());
        }
        if let Some(writer) = &self.writer {
            if writer.refuses_new_dirty_marks() {
                return Err(());
            }
        }
        if self.session_durability.refuses_mutation_admission() {
            return Err(());
        }
        Ok(())
    }

    pub(super) fn effective_last_persist_error(
        fatal: Option<String>,
        persistence: Option<&SessionPersistence>,
    ) -> Option<String> {
        fatal.or_else(|| persistence.and_then(|state| state.last_persist_error.clone()))
    }

    /// Fail-fast store lock for production mutation/query paths; emits `persistence_fatal` once on poison.
    pub(super) fn lock_inner(&self) -> std::sync::MutexGuard<'_, SessionStoreInner> {
        match self.inner.lock() {
            Ok(guard) => guard,
            Err(_) => {
                self.signal_persistence_fatal("session store mutex poisoned");
                panic!("session store mutex poisoned");
            }
        }
    }

    /// Optional store lock; returns `None` and emits `persistence_fatal` once on poison.
    pub(super) fn try_lock_inner(&self) -> Option<std::sync::MutexGuard<'_, SessionStoreInner>> {
        match self.inner.lock() {
            Ok(guard) => Some(guard),
            Err(_) => {
                self.signal_persistence_fatal("session store mutex poisoned");
                None
            }
        }
    }

    pub(super) fn lock_store_arc_inner<'a>(
        inner: &'a Arc<Mutex<SessionStoreInner>>,
        persistence_fatal: &tokio::sync::watch::Sender<Option<String>>,
    ) -> Result<std::sync::MutexGuard<'a, SessionStoreInner>, io::Error> {
        inner.lock().map_err(|_| {
            Self::signal_persistence_fatal_sender(
                persistence_fatal,
                "session store mutex poisoned",
            );
            io::Error::new(io::ErrorKind::Other, "session store mutex poisoned")
        })
    }

    fn record_writer_fatal_outcome(&self, outcome: &PersistFlushOutcome) {
        if !matches!(
            outcome.disposition,
            PersistFlushDisposition::WriterFatal | PersistFlushDisposition::CommittedButUnacked
        ) {
            return;
        }
        let message = outcome
            .error_summary
            .clone()
            .unwrap_or_else(|| "session persistence writer fatal".to_string());
        if let Some(writer) = self.writer.as_ref() {
            writer.enter_terminal_flush_failure(message);
        } else {
            self.signal_persistence_fatal(message);
        }
    }

    /// Block until every pending ledger mutation has been written to disk.
    /// No-op for in-memory stores. Required before re-opening the ledger file
    /// from another `SessionStore` (background writes are otherwise deferred).
    #[cfg(any(test, feature = "root-test-support"))]
    pub fn flush_persistence(&self) {
        if let Some(writer) = &self.writer {
            writer.flush();
        }
    }

    pub fn status(&self) -> SessionStoreStatus {
        let fatal = self.persistence_fatal_reason();
        let inner = self.lock_inner();
        let (persistence, restored_sessions, last_persist_error) = match &inner.persistence {
            Some(persistence) => (
                "enabled".to_string(),
                persistence.restored_sessions,
                Self::effective_last_persist_error(fatal, Some(persistence)),
            ),
            None => ("disabled".to_string(), 0, fatal),
        };
        let active_sessions = inner
            .sessions
            .values()
            .filter(|session| session.lifecycle() == SessionLifecycle::Active)
            .count();
        let closed_sessions = inner.sessions.len().saturating_sub(active_sessions);
        let hot_sessions = inner
            .sessions
            .values()
            .filter(|session| matches!(session, StoredSession::Hot(_)))
            .count();
        let cold_sessions = inner.sessions.len().saturating_sub(hot_sessions);
        SessionStoreStatus {
            persistence,
            restored_sessions,
            max_sessions: inner.hot_session_capacity_target,
            retained_sessions: inner.sessions.len(),
            active_sessions,
            closed_sessions,
            hot_sessions,
            cold_sessions,
            active_cold_sessions: inner.sessions.values().filter(|entry| {
                matches!(entry, StoredSession::Cold(cold) if cold.lifecycle.allows_mutation())
            }).count(),
            cold_payload_bytes: inner.sessions.values().filter_map(|entry| {
                match entry {
                    StoredSession::Cold(cold) => Some(cold.raw.get().len()),
                    StoredSession::Hot(_) => None,
                }
            }).sum(),
            hot_session_capacity_target: inner.hot_session_capacity_target,
            historical_session_retention_limit: inner.historical_session_retention_limit,
            capacity_evictions: inner.capacity_evictions,
            retention_tombstones: inner.tombstones.len(),
            max_events_per_session: inner.max_events_per_session,
            max_messages_per_session: DEFAULT_MAX_MESSAGES_PER_SESSION,
            last_persist_error,
        }
    }

    #[cfg(any(test, feature = "root-test-support"))]
    pub fn active_session_count_for_test(&self, project: Option<&str>) -> usize {
        let inner = self.lock_inner();
        inner
            .sessions
            .values()
            .filter(|record| {
                record.lifecycle() == SessionLifecycle::Active
                    && project.is_none_or(|project| record.project() == Some(project))
            })
            .count()
    }

    #[cfg(any(test, feature = "root-test-support"))]
    pub fn hot_payload_entry_count_for_test(&self, session_id: &str) -> Option<usize> {
        self.lock_inner()
            .sessions
            .get(session_id)?
            .hot()
            .map(|record| record.events.len() + record.messages.len())
    }

    #[cfg(any(test, feature = "root-test-support"))]
    pub fn cold_payload_bytes_for_test(&self, session_id: &str) -> Option<usize> {
        let inner = self.lock_inner();
        match inner.sessions.get(session_id)? {
            StoredSession::Hot(_) => None,
            StoredSession::Cold(record) => Some(record.raw.get().len()),
        }
    }

    /// Thin convenience wrapper — creation always goes through
    /// [`Self::start_session_with_options`].
    #[cfg(any(test, feature = "root-test-support"))]
    pub fn start_session(&self, project: Option<String>, title: Option<String>) -> SessionSummary {
        self.start_session_with_guards(
            project,
            title,
            SessionMode::Normal,
            SessionGuards::default(),
        )
    }

    /// Thin convenience wrapper — creation always goes through
    /// [`Self::start_session_with_options`].
    #[cfg(any(test, feature = "root-test-support"))]
    pub fn start_session_with_guards(
        &self,
        project: Option<String>,
        title: Option<String>,
        mode: SessionMode,
        guards: SessionGuards,
    ) -> SessionSummary {
        self.start_session_with_options(SessionCreateOptions::new(project, title, mode, guards))
            .expect("default Session execution context must be valid")
    }

    /// Sole create entry point for workflow sessions.
    ///
    /// Stores session-creation inputs (including project instructions) on the
    /// `SessionRecord`. Convenience wrappers above all delegate here.
    pub fn start_session_with_options(
        &self,
        mut opts: SessionCreateOptions,
    ) -> Result<SessionSummary, String> {
        opts.execution_context = opts.execution_context.validated()?;
        #[cfg(any(test, feature = "root-test-support"))]
        if opts.owner_authority_fingerprint.is_none() {
            // Direct store fixtures do not have an AuthContext. Give them an
            // explicit cfg(test)-only authority so production invariants never
            // depend on an optional creation-time fingerprint.
            opts.owner_authority_fingerprint =
                Some(super::TEST_ONLY_PROJECT_SESSION_AUTHORITY_FINGERPRINT.to_string());
        }
        if opts.project.is_none() && !opts.execution_context.is_empty() {
            return Err(
                "execution_context requires a Workflow Session bound to a registered project"
                    .to_string(),
            );
        }
        let work_token = self
            .ensure_mutation_persistence_admitted()
            .map_err(|()| Self::SESSION_PERSISTENCE_UNAVAILABLE.to_string())?;
        let summary = {
            let mut inner = self.lock_inner();
            let session_id = inner
                .allocate_session_id(webcodex_core::compact::random_suffix::<12>)
                .ok_or_else(|| "session_id_allocation_exhausted".to_string())?;
            let now = now_ts();
            let guards = SessionGuards::effective(opts.mode, opts.guards);
            let owner_authority_fingerprint =
                opts.owner_authority_fingerprint.ok_or_else(|| {
                    "Workflow Session creation requires a canonical authority fingerprint"
                        .to_string()
                })?;
            let record = SessionRecord {
                session_id: session_id.clone(),
                project: opts.project,
                project_identity: opts.project_identity.clone(),
                owner_authority_fingerprint,
                title: opts.title,
                mode: opts.mode,
                guards,
                execution_context: opts.execution_context,
                // Create always yields Active; only explicit close transitions later.
                lifecycle: SessionLifecycle::Active,
                created_at: now,
                updated_at: now,
                messages: VecDeque::new(),
                message_delivery_replays: Default::default(),
                events: VecDeque::new(),
                events_observed: 0,
                git_baseline_tree: None,
                repository_edit_observed: false,
                materialized_validation_job_ids: VecDeque::new(),
                message_observation_revision: 0,
                message_observation_floor: 0,
                message_observation_revisions: Default::default(),
                assignment_history_floors: Default::default(),
                assignment_history_tracking_complete: true,
                completion_assignment_fence_fingerprints: Default::default(),
                completion_assignment_fence_tracking_complete: true,
                project_instructions: opts.project_instructions,
            };
            inner
                .try_insert_session(record)
                .map_err(|()| "session_revision_exhausted".to_string())?
        };
        if self.persist_after_mutation(&work_token).is_err() {
            return Err(format!(
                "{}:{}",
                Self::SESSION_PERSISTENCE_UNCERTAIN,
                summary.session_id
            ));
        }
        Ok(summary)
    }

    /// Commit the coding context and accepted instruction under the in-memory
    /// store lock. Persistent stores then queue a configured durable backend
    /// mutation (production SQLite via the background writer; JSON compatibility
    /// constructors write `sessions.json`); success does not imply disk flush.
    ///
    /// Every fallible check happens before the in-memory commit. Once mutation
    /// begins, session creation or capability update and the instruction event
    /// are applied under the same store lock and enter one persistence generation.
    pub fn ensure_coding_session(
        &self,
        request: CodingSessionRequest,
    ) -> Result<CodingSessionOutcome, CodingSessionError> {
        self.ensure_coding_session_with_git_baseline(request, None)
    }

    /// Coding bootstrap variant used by `work_on_project` to bind one fresh
    /// startup Git tree. Exact continuation ignores the supplied candidate and
    /// preserves the durable Session baseline already recorded at creation.
    pub fn ensure_coding_session_with_git_baseline(
        &self,
        request: CodingSessionRequest,
        git_baseline_tree: Option<String>,
    ) -> Result<CodingSessionOutcome, CodingSessionError> {
        let explicit_resume_session_id = match request.resume_session_id.as_deref() {
            Some(session_id)
                if session_id != session_id.trim() || !is_valid_session_id(session_id) =>
            {
                return Err(CodingSessionError::InvalidResumeSessionId);
            }
            Some(session_id) => Some(session_id.to_string()),
            None => None,
        };
        let work_token = self
            .ensure_mutation_persistence_admitted()
            .map_err(|()| CodingSessionError::PersistenceUnavailable)?;
        let explicit_resume = explicit_resume_session_id.is_some();
        let now = now_ts();
        let new_event_id = format!("{EVENT_ID_PREFIX}{}", uuid::Uuid::new_v4().simple());
        let requested_guards = SessionGuards::effective(request.mode, request.guards);
        let requested_execution_context = request
            .execution_context
            .clone()
            .map(SessionExecutionContext::validated)
            .transpose()
            .map_err(CodingSessionError::InvalidExecutionContext)?;
        let instruction = request
            .instruction
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| redact_and_bound_instruction(value, MAX_CODING_INSTRUCTION_CHARS));

        let outcome = {
            let mut inner = self.lock_inner();
            let reusable_session_id = if let Some(session_id) = explicit_resume_session_id {
                let Some(stored) = inner.sessions.get(&session_id) else {
                    if inner
                        .owned_retention_incarnation(
                            &session_id,
                            request.authority_fingerprint.as_str(),
                        )
                        .is_some()
                    {
                        return Err(CodingSessionError::ResumeRetentionExpired { session_id });
                    }
                    return Err(CodingSessionError::UnknownResumeSession { session_id });
                };
                let lifecycle = stored.lifecycle();
                if !lifecycle.allows_mutation() {
                    return Err(CodingSessionError::ResumeSessionNotActive {
                        session_id,
                        lifecycle,
                    });
                }
                if stored.project() != Some(request.project.as_str()) {
                    return Err(CodingSessionError::ResumeProjectMismatch {
                        session_id,
                        session_project: stored.project().map(str::to_string),
                        request_project: request.project.clone(),
                    });
                }
                let stored_identity = stored.project_identity().cloned();
                let identity_ok = matches!(
                    (stored_identity.as_ref(), request.project_identity.as_ref()),
                    (Some(stored), Some(current)) if stored == current
                );
                if !identity_ok {
                    return Err(CodingSessionError::ResumeProjectIdentityMismatch {
                        session_id,
                    });
                }
                Some(session_id)
            } else {
                None
            };

            if let Some(session_id) = reusable_session_id.as_deref() {
                let stored_fingerprint = inner
                    .sessions
                    .get(session_id)
                    .expect("reusable session must exist")
                    .owner_authority_fingerprint();
                #[cfg(any(test, feature = "root-test-support"))]
                let synthetic_test_fixture =
                    stored_fingerprint == super::TEST_ONLY_PROJECT_SESSION_AUTHORITY_FINGERPRINT;
                #[cfg(not(any(test, feature = "root-test-support")))]
                let synthetic_test_fixture = false;
                if !synthetic_test_fixture
                    && stored_fingerprint != request.authority_fingerprint.as_str()
                {
                    return Err(CodingSessionError::ResumeAuthorityMismatch {
                        session_id: session_id.to_string(),
                    });
                }
            }

            if let Some(session_id) = reusable_session_id {
                if !inner.prepare_active_session(&session_id) {
                    return Err(CodingSessionError::CommitFailed);
                }

                let (previous_mode, previous_guards, previous_execution_context) = {
                    let record = inner
                        .sessions
                        .get(&session_id)
                        .and_then(StoredSession::hot)
                        .expect("active reusable session must stay hot");
                    (record.mode, record.guards, record.execution_context.clone())
                };
                // Repeating a mode preserves any stricter explicit guard from
                // the root task. A real mode transition applies the newly
                // requested effective guard profile.
                let next_guards = if previous_mode == request.mode {
                    SessionGuards {
                        deny_write_tools: previous_guards.deny_write_tools
                            || requested_guards.deny_write_tools,
                        deny_shell_tools: previous_guards.deny_shell_tools
                            || requested_guards.deny_shell_tools,
                    }
                } else {
                    requested_guards
                };
                let capability_changed =
                    previous_mode != request.mode || previous_guards != next_guards;
                let next_execution_context = requested_execution_context
                    .clone()
                    .unwrap_or_else(|| previous_execution_context.clone());
                let execution_context_changed =
                    next_execution_context != previous_execution_context;
                if previous_guards.deny_write_tools
                    && !next_guards.deny_write_tools
                    && !request.write_scope_verified
                {
                    return Err(CodingSessionError::WriteScopeRequired);
                }
                if self.take_coding_continuity_fault() {
                    return Err(CodingSessionError::CommitFailed);
                }

                // Snapshot the previous attempt *before* appending the new
                // task_instruction. Continuation feedback projects over this so
                // it describes what the previous attempt did, not the empty new
                // attempt. Use the per-session evidence cap so a long previous
                // attempt's work is retained for the projection.
                let pre_instruction_summary = inner
                    .summary(&session_id, Some(DEFAULT_MAX_EVENTS_PER_SESSION))
                    .expect("reusable session must summarize before instruction append");

                let event = coding_instruction_event(
                    &new_event_id,
                    &session_id,
                    &request.project,
                    instruction.clone(),
                    request.transport,
                    request.mode,
                    Some(previous_mode),
                    requested_guards,
                    Some(previous_guards),
                    capability_changed,
                    request.context_refreshed,
                    requested_execution_context.clone(),
                    Some(previous_execution_context.clone()),
                    execution_context_changed,
                    true,
                    explicit_resume,
                    now,
                );
                {
                    let record = inner
                        .sessions
                        .get_mut(&session_id)
                        .and_then(StoredSession::hot_mut)
                        .expect("active reusable session must stay hot before commit");
                    record.mode = request.mode;
                    record.guards = next_guards;
                    record.execution_context = next_execution_context;
                    record.updated_at = now;
                    if let Some(project_instructions) = request.project_instructions {
                        record.project_instructions = Some(
                            project_instructions
                                .retain_unavailable_scopes(record.project_instructions.as_ref()),
                        );
                    }
                }
                inner.append_event_mark_dirty(&session_id, event);
                inner.touch(&session_id);
                let summary = inner
                    .summary(&session_id, Some(DEFAULT_SUMMARY_LIMIT))
                    .expect("continued session must summarize");
                CodingSessionOutcome {
                    project_instructions: inner
                        .sessions
                        .get(&session_id)
                        .and_then(StoredSession::hot)
                        .and_then(|record| record.project_instructions.clone()),
                    summary,
                    pre_instruction_summary: Some(pre_instruction_summary),
                    reused: true,
                    previous_mode: Some(previous_mode),
                    previous_guards: Some(previous_guards),
                    capability_changed,
                    execution_context_changed,
                }
            } else {
                if self.take_coding_continuity_fault() {
                    return Err(CodingSessionError::CommitFailed);
                }
                let new_session_id = inner
                    .allocate_session_id(webcodex_core::compact::random_suffix::<12>)
                    .ok_or(CodingSessionError::CommitFailed)?;
                let execution_context = requested_execution_context.clone().unwrap_or_default();
                let execution_context_changed = !execution_context.is_empty();
                let event = coding_instruction_event(
                    &new_event_id,
                    &new_session_id,
                    &request.project,
                    instruction.clone(),
                    request.transport,
                    request.mode,
                    None,
                    requested_guards,
                    None,
                    false,
                    request.context_refreshed,
                    requested_execution_context,
                    None,
                    execution_context_changed,
                    false,
                    false,
                    now,
                );
                let record = SessionRecord {
                    session_id: new_session_id.clone(),
                    project: Some(request.project.clone()),
                    project_identity: request.project_identity.clone(),
                    owner_authority_fingerprint: request.authority_fingerprint.clone(),
                    // The first accepted instruction remains the root title.
                    // Follow-up instructions never overwrite it.
                    title: instruction,
                    mode: request.mode,
                    guards: requested_guards,
                    execution_context,
                    lifecycle: SessionLifecycle::Active,
                    created_at: now,
                    updated_at: now,
                    messages: VecDeque::new(),
                    message_delivery_replays: Default::default(),
                    events: VecDeque::from([Arc::new(event)]),
                    events_observed: 1,
                    git_baseline_tree,
                    repository_edit_observed: false,
                    materialized_validation_job_ids: VecDeque::new(),
                    message_observation_revision: 0,
                    message_observation_floor: 0,
                    message_observation_revisions: Default::default(),
                    assignment_history_floors: Default::default(),
                    assignment_history_tracking_complete: true,
                    completion_assignment_fence_fingerprints: Default::default(),
                    completion_assignment_fence_tracking_complete: true,
                    project_instructions: request.project_instructions,
                };
                let project_instructions = record.project_instructions.clone();
                let summary = inner
                    .try_insert_session(record)
                    .map_err(|()| CodingSessionError::CommitFailed)?;
                CodingSessionOutcome {
                    project_instructions,
                    summary,
                    pre_instruction_summary: None,
                    reused: false,
                    previous_mode: None,
                    previous_guards: None,
                    capability_changed: false,
                    execution_context_changed,
                }
            }
        };
        let session_id = outcome.summary.session_id.clone();
        self.persist_after_mutation(&work_token)
            .map_err(|()| CodingSessionError::PersistenceUncertain { session_id })?;
        Ok(outcome)
    }

    #[cfg(any(test, feature = "root-test-support"))]
    /// Inject a failure before any in-memory continuity mutation. This does
    /// not model background ledger persistence failure or rollback.
    pub fn fail_next_coding_continuity_precommit_for_test(&self) {
        self.fail_next_coding_continuity_precommit
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(any(test, feature = "root-test-support"))]
    fn take_coding_continuity_fault(&self) -> bool {
        self.fail_next_coding_continuity_precommit
            .swap(false, std::sync::atomic::Ordering::SeqCst)
    }

    #[cfg(not(any(test, feature = "root-test-support")))]
    fn take_coding_continuity_fault(&self) -> bool {
        false
    }

    pub fn summary(&self, session_id: &str, limit: Option<usize>) -> Option<SessionSummary> {
        self.with_record_for_query(session_id, |record, cold| {
            summarize_record(record, limit, cold)
        })
    }

    /// Exact retained changed-path evidence for recovery attribution. Unlike
    /// `summary`, this scans the full bounded durable event ledger instead of the
    /// model-facing 200-event tail, so ordinary presentation truncation does not
    /// masquerade as history loss. The boolean is true only when the retained
    /// ledger and each event's changed-path projection are known complete.
    pub fn retained_changed_path_evidence(&self, session_id: &str) -> Option<(Vec<String>, bool)> {
        self.with_record_for_query(session_id, |record, _cold| {
            let mut paths = BTreeSet::new();
            let mut complete = record.events_observed <= record.events.len() as u64;
            // Every persisted event sanitizes changed_paths to MAX_INPUT_ARRAY_ITEMS,
            // and runtime audit arguments can already be bounded before the Store
            // observes them. Equality to that durable bound therefore cannot prove
            // the original path set was complete, even while the Session is hot.
            for event in super::events::canonical_tool_call_finished_event_refs(
                record.events.iter().map(Arc::as_ref),
            ) {
                // Path attribution is consequence evidence, not an attempted-write
                // list. A failed/no-op edit can name the same path without proving
                // that this Session caused the current dirty state.
                if !event_observes_repository_edit(event) {
                    continue;
                }
                if event.changed_paths.len() >= MAX_INPUT_ARRAY_ITEMS {
                    complete = false;
                }
                paths.extend(event.changed_paths.iter().cloned());
            }
            (paths.into_iter().collect(), complete)
        })
    }

    /// Latest retained task-instruction identity at an exact Session event-count
    /// snapshot. Unlike `summary`, this scans the full bounded durable ledger so
    /// the model-facing 200-event tail cannot erase the current attempt identity.
    /// `through_events_total` fences the lookup against a later instruction racing
    /// the caller's already-observed Session snapshot. Durable eviction remains
    /// fail-closed and therefore returns `None`.
    pub fn retained_task_instruction_event_id_at(
        &self,
        session_id: &str,
        through_events_total: usize,
    ) -> Option<String> {
        self.with_record_for_query(session_id, |record, _cold| {
            let observed_total = usize::try_from(record.events_observed).unwrap_or(usize::MAX);
            if through_events_total > observed_total {
                return None;
            }
            let retained_len = record.events.len();
            let first_retained_sequence = observed_total.saturating_sub(retained_len);
            if through_events_total <= first_retained_sequence {
                return None;
            }
            let retained_end = through_events_total
                .saturating_sub(first_retained_sequence)
                .min(retained_len);
            record
                .events
                .iter()
                .take(retained_end)
                .rev()
                .find(|event| event.kind == "task_instruction")
                .map(|event| event.event_id.clone())
        })
        .flatten()
    }

    /// Discover only identities in the already-authorized exact Project and
    /// creation-time authority group. Filter before counting/pagination; foreign
    /// Sessions cannot affect totals or hide authorized rows behind a page limit.
    /// Inventory ordering is presentation only, never an implicit resume rule.
    pub fn discover_sessions(
        &self,
        project: &str,
        owner_authority_fingerprint: &str,
        lifecycle: Option<SessionLifecycle>,
        offset: usize,
        limit: usize,
    ) -> SessionDiscoveryPage {
        let limit = limit.clamp(1, 20);
        let mut candidates = {
            let inner = self.lock_inner();
            inner
                .sessions
                .values()
                .filter(|record| {
                    record.project() == Some(project)
                        && record.owner_authority_fingerprint() == owner_authority_fingerprint
                        && lifecycle.is_none_or(|state| record.lifecycle() == state)
                })
                .map(|record| (record.session_id().to_string(), record.updated_at()))
                .collect::<Vec<_>>()
        };
        candidates.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| right.0.cmp(&left.0)));
        let total = candidates.len();
        let end = offset.saturating_add(limit).min(total);
        let sessions = candidates
            .into_iter()
            .skip(offset)
            .take(limit)
            .filter_map(|(session_id, _)| {
                self.with_record_for_query(&session_id, |record, _| {
                    if record.project.as_deref() != Some(project)
                        || record.owner_authority_fingerprint != owner_authority_fingerprint
                        || lifecycle.is_some_and(|state| record.lifecycle != state)
                    {
                        return None;
                    }
                    let title = record.title.as_deref().map(|title| {
                        redact_and_bound_instruction(title, 240)
                            .chars()
                            .take(240)
                            .collect::<String>()
                    });
                    let title_truncated = title.as_deref() != record.title.as_deref();
                    Some(SessionDiscoveryItem {
                        session_id: record.session_id.clone(),
                        title,
                        title_truncated,
                        lifecycle: record.lifecycle,
                        created_at: record.created_at,
                        updated_at: record.updated_at,
                    })
                })
                .flatten()
            })
            .collect();
        SessionDiscoveryPage {
            total,
            offset,
            next_offset: (end < total).then_some(end),
            sessions,
        }
    }

    /// Bounded, read-only Workflow Session rows for one exact runtime project.
    /// The project is authoritative caller context, never request-controlled UI state.
    pub fn console_list_for_project(
        &self,
        project: &str,
        limit: Option<usize>,
        validation: ConsoleValidationHooks,
    ) -> WorkflowSessionConsoleList {
        self.console_lists_for_projects(&[project], limit, validation)
            .remove(project)
            .expect("requested project has a console list")
    }

    /// Scan retained identities once for an already-authorized set of projects.
    /// Each project keeps its own ordering and limit. Only immutable Closed
    /// records may reuse a small base Console projection across repeated reads.
    pub fn console_lists_for_projects(
        &self,
        projects: &[&str],
        limit: Option<usize>,
        validation: ConsoleValidationHooks,
    ) -> HashMap<String, WorkflowSessionConsoleList> {
        let limit = normalize_console_session_limit(limit);
        let mut candidates: HashMap<&str, Vec<(String, i64)>> = projects
            .iter()
            .map(|project| (*project, Vec::new()))
            .collect();
        if candidates.is_empty() {
            return HashMap::new();
        }
        {
            let inner = self.lock_inner();
            for session in inner.sessions.values() {
                if let Some(rows) = session
                    .project()
                    .and_then(|project| candidates.get_mut(project))
                {
                    rows.push((session.session_id().to_string(), session.updated_at()));
                }
            }
        }
        // Sort and materialize outside the inventory lock. Cold records retain
        // their ordinary query path, including the exact-project recheck.
        candidates
            .into_iter()
            .map(|(project, mut candidates)| {
                candidates
                    .sort_by(|left, right| right.1.cmp(&left.1).then_with(|| right.0.cmp(&left.0)));
                let total = candidates.len();
                candidates.truncate(limit);
                let sessions = candidates
                    .into_iter()
                    .filter_map(|(session_id, _)| {
                        self.console_list_item_for_project(&session_id, project, validation)
                    })
                    .collect::<Vec<_>>();
                (
                    project.to_string(),
                    WorkflowSessionConsoleList {
                        returned: sessions.len(),
                        truncated: total > limit,
                        total,
                        sessions,
                    },
                )
            })
            .collect()
    }

    /// Reuse only a Closed Session's immutable base row (never a live Job
    /// overlay). Cache identity includes the cold raw allocation and both
    /// validation hooks, so replacements and alternate validation projections
    /// cannot return stale evidence. One row maximum per retained Closed identity.
    fn console_list_item_for_project(
        &self,
        session_id: &str,
        project: &str,
        validation: ConsoleValidationHooks,
    ) -> Option<WorkflowSessionConsoleListItem> {
        let (cold, max_events_per_session) = {
            let mut inner = self.lock_inner();
            inner.touch(session_id);
            let max_events_per_session = inner.max_events_per_session;
            match inner.sessions.get(session_id)? {
                StoredSession::Hot(record) => {
                    return (record.project.as_deref() == Some(project))
                        .then(|| build_console_list_item(record, project, validation));
                }
                StoredSession::Cold(record) => (record.clone(), max_events_per_session),
            }
        };

        if cold.project.as_deref() != Some(project) {
            return None;
        }
        let immutable = cold.lifecycle == SessionLifecycle::Closed;
        if immutable {
            let cache = cold
                .console_list_cache
                .lock()
                .expect("cold Console projection cache mutex poisoned");
            if let Some(cached) = cache.as_ref() {
                if Arc::ptr_eq(&cached.raw, &cold.raw) && cached.hooks.same_callbacks(validation) {
                    return Some(cached.row.clone());
                }
            }
        }

        let record = materialize_cold_session(&cold, max_events_per_session)?;
        if record.project.as_deref() != Some(project) {
            return None;
        }
        let row = build_console_list_item(&record, project, validation);
        if immutable {
            *cold
                .console_list_cache
                .lock()
                .expect("cold Console projection cache mutex poisoned") = Some(CachedConsoleList {
                raw: cold.raw.clone(),
                hooks: validation,
                row: row.clone(),
            });
        }
        Some(row)
    }

    /// Bounded, read-only human timeline for one exact project-scoped Session.
    /// Unknown and wrong-project ids intentionally collapse to the same `None`.
    pub fn console_detail_for_project(
        &self,
        project: &str,
        session_id: &str,
        limit: Option<usize>,
        validation: ConsoleValidationHooks,
    ) -> Option<WorkflowSessionConsoleDetail> {
        let allowed = {
            let inner = self.lock_inner();
            inner
                .sessions
                .get(session_id)
                .is_some_and(|session| session.project() == Some(project))
        };
        if !allowed {
            return None;
        }
        let limit = normalize_console_activity_limit(limit);
        self.with_record_for_query(session_id, |record, _| {
            (record.project.as_deref() == Some(project))
                .then(|| build_console_detail(record, project, limit, validation))
        })
        .flatten()
    }

    pub(super) fn with_record_for_query<T>(
        &self,
        session_id: &str,
        query: impl FnOnce(&SessionRecord, Option<&ColdSessionRecord>) -> T,
    ) -> Option<T> {
        let (cold, max_events_per_session) = {
            let mut inner = self.lock_inner();
            inner.touch(session_id);
            let max_events_per_session = inner.max_events_per_session;
            let stored = inner.sessions.get(session_id)?;
            match stored {
                StoredSession::Hot(record) => return Some(query(record, None)),
                StoredSession::Cold(record) => (record.clone(), max_events_per_session),
            }
        };
        let record = materialize_cold_session(&cold, max_events_per_session)?;
        Some(query(&record, Some(&cold)))
    }

    fn coldify_closed_session(&self, session_id: &str) {
        let (snapshot, project_instructions) = {
            let inner = self.lock_inner();
            let Some(StoredSession::Hot(record)) = inner.sessions.get(session_id) else {
                return;
            };
            if record.lifecycle.allows_mutation() {
                return;
            }
            (
                PersistedSessionRecord::from_record(record, inner.max_events_per_session),
                record
                    .project_instructions
                    .as_ref()
                    .map(|instructions| instructions.to_summary()),
            )
        };
        let cold = match cold_session_from_persisted(&snapshot, project_instructions) {
            Ok(cold) => cold,
            Err(err) => {
                tracing::warn!("session cold serialization failed: {err}");
                return;
            }
        };
        let mut inner = self.lock_inner();
        let Some(stored) = inner.sessions.get_mut(session_id) else {
            return;
        };
        let Some(record) = stored.hot() else {
            return;
        };
        if !snapshot.still_matches_record(record) {
            return;
        }
        debug_assert!(!cold.lifecycle.allows_mutation());
        *stored = StoredSession::Cold(cold);
    }

    pub fn contains_session(&self, session_id: &str) -> bool {
        let inner = self.lock_inner();
        inner.contains_session(session_id)
    }

    /// Internal consistency fence for assembling recovery evidence. Event mutations
    /// advance events_observed; collaboration mutations advance
    /// message_observation_revision. Never serialized or accepted from a caller.
    pub fn handoff_revision(&self, session_id: &str) -> Option<(u64, u64)> {
        self.retained_revisions(session_id)
    }

    /// Cold JSON is canonical and immutable after admission, so scalar
    /// revisions need not materialize its entire (up to 2,000-event) body.
    /// Keep the same LRU read touch as with_record_for_query. If an internal
    /// repair/test replaced raw bytes, fall through to the original checked
    /// materialization path rather than returning stale revision metadata.
    pub(super) fn retained_revisions(&self, session_id: &str) -> Option<(u64, u64)> {
        {
            let mut inner = self.lock_inner();
            inner.touch(session_id);
            match inner.sessions.get(session_id)? {
                StoredSession::Hot(record) => {
                    return Some((record.events_observed, record.message_observation_revision));
                }
                StoredSession::Cold(cold)
                    if std::ptr::eq(
                        std::sync::Weak::as_ptr(&cold.revision_raw),
                        Arc::as_ptr(&cold.raw),
                    ) =>
                {
                    return Some((
                        cold.retained_events_observed,
                        cold.retained_message_revision,
                    ));
                }
                StoredSession::Cold(_) => {}
            }
        }
        self.with_record_for_query(session_id, |record, _| {
            (record.events_observed, record.message_observation_revision)
        })
    }

    pub fn session_project(&self, session_id: &str) -> Option<Option<String>> {
        let inner = self.lock_inner();
        inner.session_project(session_id)
    }

    pub fn session_target_authority(&self, session_id: &str) -> Option<(Option<String>, String)> {
        let inner = self.lock_inner();
        inner.session_target_authority(session_id)
    }

    pub fn session_project_identity(
        &self,
        session_id: &str,
    ) -> Option<Option<SessionProjectIdentity>> {
        let inner = self.lock_inner();
        inner
            .sessions
            .get(session_id)
            .map(|record| record.project_identity().cloned())
    }

    /// Complete the outstanding background ledger generation after an explicit
    /// low-frequency durability promise.
    pub async fn await_persistence_barrier(&self) -> Result<(), ()> {
        self.persist_after_mutation_durable().await
    }

    /// Synchronous revalidation boundary for an already authorized current-work
    /// target. Identity and creation authority remain explicit; this closure
    /// cannot close/reassign a Session or confer authority through a Window.
    /// The callback must not reenter SessionStore or perform asynchronous work.
    pub fn with_active_session_authority_fence<T>(
        &self,
        session_id: &str,
        expected_project: &str,
        expected_owner_authority_fingerprint: &str,
        commit: impl FnOnce() -> T,
    ) -> Option<T> {
        let inner = self.try_lock_inner()?;
        let record = inner.sessions.get(session_id)?;
        if !record.lifecycle().allows_mutation()
            || record.project() != Some(expected_project)
            || record.owner_authority_fingerprint() != expected_owner_authority_fingerprint
        {
            return None;
        }
        Some(commit())
    }

    /// Return inherited defaults only for an active Session whose registered
    /// project exactly matches the already-resolved request project.
    pub fn execution_context_for_project(
        &self,
        session_id: &str,
        resolved_project: &str,
    ) -> Option<SessionExecutionContext> {
        self.with_record_for_query(session_id, |record, _| {
            (record.lifecycle.allows_mutation()
                && record.project.as_deref() == Some(resolved_project))
            .then(|| record.execution_context.clone())
        })
        .flatten()
    }

    pub fn guard_state(&self, session_id: &str) -> Option<(SessionMode, SessionGuards)> {
        let inner = self.lock_inner();
        inner.guard_state(session_id)
    }

    pub fn lifecycle_state(&self, session_id: &str) -> Option<SessionLifecycle> {
        let inner = self.lock_inner();
        inner.lifecycle_state(session_id)
    }

    /// Incarnation fingerprint of a retention tombstone owned by this principal.
    /// A different principal and a missing tombstone are both `None`.
    pub fn owned_retention_incarnation(
        &self,
        session_id: &str,
        owner_authority_fingerprint: &str,
    ) -> Option<String> {
        let inner = self.lock_inner();
        inner.owned_retention_incarnation(session_id, owner_authority_fingerprint)
    }

    #[cfg(any(test, feature = "root-test-support"))]
    pub fn retention_tombstone_for_test(
        &self,
        session_id: &str,
    ) -> Option<SessionRetentionTombstone> {
        let inner = self.lock_inner();
        let ordinal = inner.tombstone_ids.get(session_id).copied()?;
        inner.tombstones.get(&ordinal).cloned()
    }

    /// Try one fixed suffix sixteen times. A retained tombstone occupies that id.
    #[cfg(any(test, feature = "root-test-support"))]
    pub fn allocate_fixed_session_suffix_for_test(&self, suffix: &str) -> Option<String> {
        let inner = self.lock_inner();
        inner.allocate_session_id(|| suffix.to_string())
    }

    /// Explicit close: `Active → Closed`. Idempotent for already-closed sessions.
    ///
    /// Never creates a session for an unknown id. Emits a single
    /// `session_closed` ledger event only on a real `Active → Closed` transition.
    pub fn close_session(
        &self,
        session_id: &str,
    ) -> Result<SessionCloseOutcome, SessionCloseError> {
        let session_id = session_id.trim();
        if session_id.is_empty() || !is_valid_session_id(session_id) {
            return Err(SessionCloseError::UnknownSession);
        }
        let work_token = self
            .ensure_mutation_persistence_admitted()
            .map_err(|()| SessionCloseError::PersistenceUnavailable)?;
        let committed = {
            let mut inner = self.lock_inner();
            inner.close_session(session_id)?
        };
        let outcome = committed.unwrap_or_else(|| SessionCloseOutcome {
            summary: self
                .summary(session_id, Some(DEFAULT_SUMMARY_LIMIT))
                .expect("known cold closed session must materialize for summary"),
            already_closed: true,
        });
        self.coldify_closed_session(session_id);
        {
            let mut inner = self.lock_inner();
            inner.enforce_historical_retention_bound();
        }
        self.persist_after_mutation(&work_token)
            .map_err(|()| SessionCloseError::PersistenceUncertain)?;
        Ok(outcome)
    }

    /// Replace the complete execution context and append one safe metadata
    /// event together under the in-memory store lock. Persistent stores then
    /// queue a configured durable backend mutation (production SQLite; JSON
    /// compatibility constructors). `{}` clears all execution defaults.
    pub fn update_execution_context(
        &self,
        session_id: &str,
        execution_context: SessionExecutionContext,
        transport: SessionTransport,
    ) -> Result<SessionExecutionContextUpdateOutcome, SessionExecutionContextUpdateError> {
        let session_id = session_id.trim();
        if session_id.is_empty() || !is_valid_session_id(session_id) {
            return Err(SessionExecutionContextUpdateError::UnknownSession);
        }
        let work_token = self
            .ensure_mutation_persistence_admitted()
            .map_err(|()| SessionExecutionContextUpdateError::PersistenceUnavailable)?;
        let outcome = {
            let mut inner = self.lock_inner();
            if !inner.prepare_active_session(session_id) {
                return Err(SessionExecutionContextUpdateError::UnknownSession);
            }
            let stored = inner
                .sessions
                .get(session_id)
                .ok_or(SessionExecutionContextUpdateError::UnknownSession)?;
            let lifecycle = stored.lifecycle();
            if !lifecycle.allows_mutation() {
                return Err(SessionExecutionContextUpdateError::SessionNotActive { lifecycle });
            }
            let record = stored
                .hot()
                .expect("active execution-context session must stay hot");
            let project = record
                .project
                .clone()
                .ok_or(SessionExecutionContextUpdateError::SessionHasNoProject)?;
            let previous_execution_context = record.execution_context.clone();
            let execution_context = execution_context
                .validated()
                .map_err(SessionExecutionContextUpdateError::InvalidExecutionContext)?;
            let changed = execution_context != previous_execution_context;
            let now = now_ts();
            let event = session_execution_context_updated_event(
                session_id,
                &project,
                transport,
                execution_context.clone(),
                previous_execution_context.clone(),
                changed,
                now,
            );
            {
                let record = inner
                    .sessions
                    .get_mut(session_id)
                    .and_then(StoredSession::hot_mut)
                    .expect("validated active Session must stay hot under store lock");
                record.execution_context = execution_context;
                record.updated_at = now;
            }
            inner.append_event_mark_dirty(session_id, event);
            inner.touch(session_id);
            let summary = inner
                .summary(session_id, Some(DEFAULT_SUMMARY_LIMIT))
                .expect("updated Session must summarize");
            SessionExecutionContextUpdateOutcome {
                summary,
                previous_execution_context,
                changed,
            }
        };
        self.persist_after_mutation(&work_token)
            .map_err(|()| SessionExecutionContextUpdateError::PersistenceUncertain)?;
        Ok(outcome)
    }

    /// Authoritative lifecycle check used by dispatch/kernel before mutation.
    ///
    /// Closed sessions deny write-like, shell-like, and a small set of
    /// session-local mutations (messages, checkpoint create/restore/delete).
    /// Query tools and pure reads remain allowed. `close_session` itself is
    /// never denied so repeated close stays idempotent.
    pub fn lifecycle_denial(
        &self,
        session_id: &str,
        tool_name: &str,
        contract: SessionToolContract,
    ) -> Option<SessionLifecycleDenial> {
        let lifecycle = self.lifecycle_state(session_id)?;
        if lifecycle.allows_mutation() {
            return None;
        }
        let lifecycle_effect = runtime_tool_session_evidence_policy(tool_name).lifecycle;
        if lifecycle_effect == ToolSessionLifecycleEffect::IdempotentClose {
            return None;
        }
        if lifecycle_blocks_tool(contract, lifecycle_effect) {
            return Some(SessionLifecycleDenial { lifecycle });
        }
        None
    }

    /// Authoritative guard check used by dispatch/kernel before mutation.
    pub fn guard_denial(
        &self,
        session_id: &str,
        contract: SessionToolContract,
    ) -> Option<SessionGuardDenial> {
        let (mode, guards) = self.guard_state(session_id)?;
        let classification = contract;
        if guards.deny_write_tools && classification.write_like {
            return Some(SessionGuardDenial {
                mode,
                guard: "deny_write_tools",
            });
        }
        if guards.deny_shell_tools && classification.shell_like {
            return Some(SessionGuardDenial {
                mode,
                guard: "deny_shell_tools",
            });
        }
        None
    }

    #[cfg(any(test, feature = "root-test-support"))]
    pub fn record_tool_call_started(
        &self,
        session_id: Option<&str>,
        transport: SessionTransport,
        tool_name: &str,
        arguments: &Value,
        contract: SessionToolContract,
    ) -> Option<ToolCallStart> {
        self.record_tool_call_started_with_options(
            session_id, transport, tool_name, arguments, None, contract,
        )
    }

    pub fn record_tool_call_started_with_options(
        &self,
        session_id: Option<&str>,
        transport: SessionTransport,
        tool_name: &str,
        arguments: &Value,
        resolved_project: Option<String>,
        contract: SessionToolContract,
    ) -> Option<ToolCallStart> {
        self.record_tool_call_started_with_metadata(
            session_id,
            transport,
            tool_name,
            arguments,
            resolved_project,
            ToolCallRecorderMetadata::from_business_arguments(arguments),
            contract,
        )
    }

    /// Sole entry for appending a `tool_call_started` ledger event.
    pub fn record_tool_call_started_with_metadata(
        &self,
        session_id: Option<&str>,
        transport: SessionTransport,
        tool_name: &str,
        arguments: &Value,
        resolved_project: Option<String>,
        metadata: ToolCallRecorderMetadata,
        contract: SessionToolContract,
    ) -> Option<ToolCallStart> {
        let session_id = session_id?.trim();
        if !is_valid_session_id(session_id) {
            return None;
        }
        // Preserve the existing fail-closed Session boundary. Evicted or unknown
        // Sessions cannot be revived by appending a tool event.
        if !self.contains_session(session_id) {
            return None;
        }
        let now = now_ts();
        let event_id = format!("{EVENT_ID_PREFIX}{}", uuid::Uuid::new_v4().simple());
        let project = extract_project(arguments);
        let call_id = format!("{CALL_ID_PREFIX}{}", uuid::Uuid::new_v4().simple());
        let classification = contract;
        let risk_class = classification.risk_class.to_string();
        let changed_paths = changed_paths_for_tool_call(contract, arguments);
        let observed_paths = observed_input_paths_for_tool(tool_name, contract, arguments);
        let diff_review_like = diff_review_like_for_tool(tool_name, arguments);
        let input_summary = Some(session_input_summary_for_tool(tool_name, arguments));
        let expectation = metadata.expectation;
        let start = ToolCallStart {
            event_id,
            call_id,
            logical_invocation_id: metadata.logical_invocation_id,
            logical_invocation_role: metadata.logical_invocation_role,
            session_id: session_id.to_string(),
            transport,
            tool_name: tool_name.to_string(),
            project,
            resolved_project,
            risk_class,
            read_like: classification.read_like,
            write_like: classification.write_like,
            shell_like: classification.shell_like,
            git_like: classification.git_like,
            change_summary_like: classification.change_summary_like,
            diff_review_like,
            changed_paths,
            observed_paths,
            started_at: now,
            started_instant: Instant::now(),
            permission: None,
            expectation,
        };
        if !self.push_event(SessionEvent {
            input_summary,
            ..tool_call_event(start.clone(), "tool_call_started", now)
        }) {
            return None;
        }
        Some(start)
    }

    /// Returns whether admission, event update, and post-mutation persistence all succeeded.
    pub fn record_permission_decision(
        &self,
        start: &mut ToolCallStart,
        permission: PermissionDecision,
    ) -> bool {
        let work_token = match self.ensure_mutation_persistence_admitted() {
            Ok(token) => token,
            Err(()) => return false,
        };
        start.permission = Some(permission.clone());
        let persisted = self.set_event_permission(&start.session_id, &start.event_id, permission);
        if !persisted {
            return false;
        }
        self.persist_after_mutation(&work_token).is_ok()
    }

    /// Attach the bounded outcome of automatic persistent-shell cleanup to the
    /// single `session_closed` system event. Phase one permits at most one
    /// active shell per Session, so this remains a scalar evidence record.
    pub fn record_session_close_persistent_shell_evidence(
        &self,
        session_id: &str,
        shell_id: &str,
        shell_state: &str,
        execution_state: &str,
        error_code: Option<&str>,
        already_closed: bool,
    ) {
        let evidence = persistent_shell_event_evidence_for_tool_result(
            "close_session",
            &serde_json::json!({
                "shell_id": shell_id,
                "shell_state": shell_state,
                "execution_state": execution_state,
                "error_code": error_code,
                "command_started": false,
                "command_completed": false,
                "already_closed": already_closed,
            }),
        );
        let Some(evidence) = evidence else {
            return;
        };
        let work_token = match self.ensure_mutation_persistence_admitted() {
            Ok(token) => token,
            Err(()) => return,
        };
        let persisted = self.set_session_close_persistent_shell_evidence(session_id, evidence);
        if persisted {
            let _ = self.persist_after_mutation(&work_token);
        }
    }

    fn tool_effect_event_evidence_for_result(output: &Value) -> Option<ToolEffectEventEvidence> {
        let state_changed = output.get("state_changed").and_then(Value::as_bool);
        let command_started = output.get("command_started").and_then(Value::as_bool);
        let command_completed = output.get("command_completed").and_then(Value::as_bool);
        let execution_state = output
            .get("execution_state")
            .and_then(Value::as_str)
            .and_then(sanitize_tool_execution_state);
        if state_changed.is_none()
            && command_started.is_none()
            && command_completed.is_none()
            && execution_state.is_none()
        {
            return None;
        }
        Some(ToolEffectEventEvidence {
            state_changed,
            command_started,
            command_completed,
            execution_state,
        })
    }

    /// Append a finished tool-call ledger event through the canonical event path.
    pub fn record_tool_call_finished(
        &self,
        start: Option<ToolCallStart>,
        success: bool,
        output: &Value,
        error: Option<&str>,
        error_kind: Option<&str>,
    ) -> Option<String> {
        let event = Self::tool_call_finished_event(start, success, output, error, error_kind)?;
        let event_id = event.event_id.clone();
        if !self.push_event(event) {
            return None;
        }
        Some(event_id)
    }

    fn tool_call_finished_event(
        start: Option<ToolCallStart>,
        success: bool,
        output: &Value,
        error: Option<&str>,
        error_kind: Option<&str>,
    ) -> Option<SessionEvent> {
        let start = start?;
        let finished_at = now_ts();
        let duration_ms = start
            .started_instant
            .elapsed()
            .as_millis()
            .min(u64::MAX as u128) as u64;
        let event_id = format!("{EVENT_ID_PREFIX}{}", uuid::Uuid::new_v4().simple());
        let failure_kind = output
            .get("failure_kind")
            .and_then(Value::as_str)
            .map(str::to_string);
        // A caller-supplied kind or output failure_kind stays authoritative.
        // Only a retention-expired identity replaces the generic runtime_error
        // default, so evidence on another live Session keeps that exact kind.
        let classified_retention = error
            .map(classify_error_message)
            .filter(|kind| kind == "session_retention_expired");
        let error_kind = error_kind
            .or_else(|| error.and_then(|_| output.get("failure_kind").and_then(Value::as_str)))
            .or(classified_retention.as_deref());
        let actual_failure_kind = actual_failure_kind_for_tool_result(output, error, error_kind);
        let failure_expectation_result = classify_failure_expectation(
            success,
            &start.expectation,
            actual_failure_kind.as_deref(),
            output,
        );
        let warning_kind = output
            .get("warning_kind")
            .and_then(Value::as_str)
            .map(str::to_string);
        let session_project = output
            .get("session_project")
            .and_then(Value::as_str)
            .map(str::to_string);
        let request_project = output
            .get("request_project")
            .and_then(Value::as_str)
            .map(str::to_string);
        let error_message_summary =
            error.map(|message| bound_event_error_summary(message, start.shell_like));
        let validation_output_summary =
            validation_output_summary_for_tool_result(&start.tool_name, output);
        let observed_paths = if success {
            observed_paths_for_successful_result(
                &start.tool_name,
                start.observed_paths.clone(),
                output,
            )
        } else {
            Vec::new()
        };
        let persistent_shell =
            persistent_shell_event_evidence_for_tool_result(&start.tool_name, output);
        let effect_evidence = Self::tool_effect_event_evidence_for_result(output);
        let mut changed_paths = start.changed_paths.clone();
        for path in changed_paths_for_tool_result(&start.tool_name, output) {
            if !changed_paths.iter().any(|existing| existing == &path) {
                changed_paths.push(path);
            }
        }
        let event = SessionEvent {
            event_id,
            context_result_summary: context_result_summary_for_tool_result(
                &start.tool_name,
                output,
            ),
            finished_at: Some(finished_at),
            duration_ms: Some(duration_ms),
            status: Some(if success { "succeeded" } else { "failed" }.to_string()),
            exit_code: output.get("exit_code").and_then(Value::as_i64),
            failure_kind,
            error_kind: error.map(|_| error_kind.unwrap_or("runtime_error").to_string()),
            actual_failure_kind,
            failure_expectation_result: Some(failure_expectation_result.to_string()),
            warning_kind,
            session_project,
            request_project,
            error_message_summary,
            changed_paths,
            observed_paths,
            job_id: extract_job_id(output),
            persistent_shell,
            effect_evidence,
            validation_output_summary,
            ..tool_call_event(start, "tool_call_finished", finished_at)
        };
        Some(event)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_validation_job_terminal(
        &self,
        session_id: &str,
        job_id: &str,
        retained_terminal_job_ids: &[&str],
        tool_name: &str,
        contract: SessionToolContract,
        project: Option<String>,
        validation_target_id: &str,
        assertion_name: Option<&str>,
        job_status: &str,
        exit_code: Option<i64>,
        validation_passed: Option<bool>,
        started_at: Option<i64>,
        finished_at: Option<i64>,
        duration_ms: Option<u64>,
        validation_output_summary: Option<Value>,
    ) -> bool {
        let session_id = session_id.trim();
        let job_id = job_id.trim();
        let valid_target = is_validation_execution_identity(validation_target_id);
        let assertion_name = assertion_name
            .and_then(|value| super::events::safe_model_facing_assertion_name(tool_name, value))
            .filter(|value| assertion_validation_identity(value) == validation_target_id);
        let Some(timestamp) = finished_at else {
            // Reconciliation must never substitute wall-clock read time for
            // authoritative execution activity.
            return false;
        };
        let work_token = match self.ensure_mutation_persistence_admitted() {
            Ok(token) => token,
            Err(()) => return false,
        };
        let Ok(job_lifecycle) = RunnerJobLifecycle::from_wire(job_status) else {
            return false;
        };
        if !is_valid_session_id(session_id)
            || !is_safe_job_id(job_id)
            || retained_terminal_job_ids.len() > MAX_MATERIALIZED_VALIDATION_JOB_IDS
            || retained_terminal_job_ids
                .iter()
                .any(|candidate| *candidate != candidate.trim() || !is_safe_job_id(candidate))
            || !retained_terminal_job_ids.contains(&job_id)
            || !matches!(
                tool_name,
                "project_validate"
                    | "cargo_fmt"
                    | "cargo_check"
                    | "cargo_test"
                    | "go_test"
                    | "run_process"
                    | "run_script"
                    | "run_shell"
                    | "run_job"
            )
            || !valid_target
            || !job_lifecycle.is_terminal()
        {
            return false;
        }
        let inherited_expectation = self
            .summary(session_id, Some(MAX_SUMMARY_LIMIT))
            .and_then(|summary| {
                summary
                    .events
                    .iter()
                    .rev()
                    .find(|event| {
                        event.kind == "tool_call_finished"
                            && event.job_id.as_deref() == Some(job_id)
                            && event.tool_name == tool_name
                            && event
                                .resolved_project
                                .as_deref()
                                .or(event.project.as_deref())
                                == project.as_deref()
                    })
                    .map(|event| ToolCallExpectation {
                        expected_failure: event.expected_failure == Some(true),
                        expected_failure_kind: event.expected_failure_kind.clone(),
                        result_expectation: event.result_expectation.clone(),
                        accepted_exit_codes: event.accepted_exit_codes.clone(),
                        assertion_name: event.assertion_name.clone(),
                    })
            })
            .unwrap_or_default();
        let process_succeeded =
            job_lifecycle == RunnerJobLifecycle::Completed && exit_code == Some(0);
        let succeeded = process_succeeded && validation_passed.unwrap_or(true);
        let failure_kind = (!succeeded).then(|| match job_lifecycle {
            RunnerJobLifecycle::Timeout | RunnerJobLifecycle::TimedOut => "timeout".to_string(),
            RunnerJobLifecycle::Stopped | RunnerJobLifecycle::Cancelled => "cancelled".to_string(),
            RunnerJobLifecycle::Lost => "execution_lost".to_string(),
            _ if process_succeeded => "validation_failed".to_string(),
            _ => "command_exit_nonzero".to_string(),
        });
        let terminal_execution_state = match job_lifecycle {
            RunnerJobLifecycle::Completed | RunnerJobLifecycle::Failed => "completed",
            RunnerJobLifecycle::Timeout | RunnerJobLifecycle::TimedOut => "timed_out",
            RunnerJobLifecycle::Stopped | RunnerJobLifecycle::Cancelled => "cancelled",
            RunnerJobLifecycle::Lost => "outcome_unknown",
            _ => "outcome_unknown",
        };
        let expectation_output = serde_json::json!({
            "job_id": job_id,
            "exit_code": exit_code,
            "execution_state": terminal_execution_state,
            "command_completed": matches!(
                job_lifecycle,
                RunnerJobLifecycle::Completed | RunnerJobLifecycle::Failed
            ) && exit_code.is_some(),
        });
        let failure_expectation_result = classify_failure_expectation(
            succeeded,
            &inherited_expectation,
            failure_kind.as_deref(),
            &expectation_output,
        );
        let classification = contract;
        let mut input_summary = serde_json::json!({
            "execution_identity": validation_target_id,
        });
        if validation_target_id.starts_with("target:") {
            input_summary["validation_target_id"] = serde_json::json!(validation_target_id);
        }
        if let Some(validation_tool) = validation_output_summary
            .as_ref()
            .and_then(|summary| summary.get("validation_tool"))
            .and_then(Value::as_str)
            .filter(|tool| matches!(*tool, "cargo_fmt" | "cargo_check" | "cargo_test"))
        {
            input_summary["validation_tool"] = serde_json::json!(validation_tool);
        }
        let inherited_expected_failure = inherited_expectation.expected_failure;
        let inherited_expected_failure_kind = inherited_expectation.expected_failure_kind.clone();
        let inherited_result_expectation = inherited_expectation.result_expectation.clone();
        let inherited_accepted_exit_codes = inherited_expectation.accepted_exit_codes.clone();
        let inherited_assertion_name = inherited_expectation.assertion_name.clone();
        let event = SessionEvent {
            project: project.clone(),
            resolved_project: project.clone(),
            read_like: classification.read_like,
            write_like: classification.write_like,
            shell_like: classification.shell_like,
            git_like: classification.git_like,
            change_summary_like: classification.change_summary_like,
            started_at,
            finished_at: Some(timestamp),
            duration_ms,
            status: Some(if succeeded { "succeeded" } else { "failed" }.to_string()),
            exit_code,
            failure_kind: failure_kind.clone(),
            expected_failure: inherited_expected_failure.then_some(true),
            expected_failure_kind: inherited_expected_failure_kind,
            result_expectation: inherited_result_expectation,
            accepted_exit_codes: inherited_accepted_exit_codes,
            assertion_name: assertion_name.or(inherited_assertion_name),
            actual_failure_kind: failure_kind,
            failure_expectation_result: Some(failure_expectation_result.to_string()),
            job_id: Some(job_id.to_string()),
            input_summary: Some(input_summary),
            validation_output_summary,
            ..session_event(
                format!("{EVENT_ID_PREFIX}{}", uuid::Uuid::new_v4().simple()),
                session_id.to_string(),
                "validation_job_terminal".to_string(),
                timestamp,
                "job_terminal".to_string(),
                tool_name.to_string(),
                classification.risk_class.to_string(),
            )
        };

        let (cold, hot_closed, recorded) = {
            let mut inner = self.lock_inner();
            if !inner.prepare_active_session(session_id) {
                return false;
            }

            let max_events = inner.max_events_per_session;
            enum ValidationTerminalBranch {
                Hot {
                    hot_closed: bool,
                    recorded: bool,
                    dirty: Option<ColdRewriteOutcome>,
                },
                Cold(ColdSessionRecord),
            }
            let branch = {
                let Some(stored) = inner.sessions.get_mut(session_id) else {
                    return false;
                };
                match stored {
                    StoredSession::Hot(record) => {
                        let recorded = Self::append_validation_job_terminal_to_record(
                            record,
                            job_id,
                            retained_terminal_job_ids,
                            project.as_deref(),
                            &event,
                            timestamp,
                            max_events,
                        );
                        let hot_closed = recorded && !record.lifecycle.allows_mutation();
                        let dirty = if recorded {
                            let retained_event_start = retained_event_start_for_record(record);
                            if let Some(back) = record.events.back() {
                                Some(ColdRewriteOutcome::EventUpsert {
                                    ordinal: record.events_observed,
                                    event: Arc::clone(back),
                                    retained_event_start,
                                })
                            } else {
                                Some(ColdRewriteOutcome::Changed {
                                    retained_event_start,
                                })
                            }
                        } else {
                            None
                        };
                        ValidationTerminalBranch::Hot {
                            hot_closed,
                            recorded,
                            dirty,
                        }
                    }
                    StoredSession::Cold(record) => {
                        ValidationTerminalBranch::Cold(record.clone())
                    }
                }
            };
            match branch {
                ValidationTerminalBranch::Hot {
                    hot_closed,
                    recorded,
                    dirty,
                } => {
                    if let Some(outcome) = dirty {
                        match outcome {
                            ColdRewriteOutcome::Changed {
                                retained_event_start,
                            } => inner.mark_session_state_dirty(session_id, retained_event_start),
                            ColdRewriteOutcome::EventUpsert {
                                ordinal,
                                event,
                                retained_event_start,
                            } => inner.mark_session_event_upsert(
                                session_id,
                                ordinal,
                                event,
                                retained_event_start,
                            ),
                        };
                    }
                    (None, hot_closed, recorded)
                }
                ValidationTerminalBranch::Cold(record) => (Some(record), false, false),
            }
        };
        let recorded = match cold {
            Some(cold) => {
                self.rewrite_cold_record(session_id, cold, false, |record, max_events| {
                    if !Self::append_validation_job_terminal_to_record(
                        record,
                        job_id,
                        retained_terminal_job_ids,
                        project.as_deref(),
                        &event,
                        timestamp,
                        max_events,
                    ) {
                        return None;
                    }
                    let retained_event_start = retained_event_start_for_record(record);
                    if let Some(snapshot) = record.events.back() {
                        let ordinal = record.events_observed;
                        Some(ColdRewriteOutcome::EventUpsert {
                            ordinal,
                            event: Arc::clone(snapshot),
                            retained_event_start,
                        })
                    } else {
                        Some(ColdRewriteOutcome::Changed {
                            retained_event_start,
                        })
                    }
                })
            }
            None => recorded,
        };
        if !recorded {
            return false;
        }
        if hot_closed {
            self.coldify_closed_session(session_id);
        }
        self.persist_after_mutation(&work_token).is_ok()
    }

    fn append_validation_job_terminal_to_record(
        record: &mut SessionRecord,
        job_id: &str,
        retained_terminal_job_ids: &[&str],
        project: Option<&str>,
        event: &SessionEvent,
        timestamp: i64,
        max_events: usize,
    ) -> bool {
        if record.project.as_deref() != project
            || record
                .materialized_validation_job_ids
                .iter()
                .any(|materialized| materialized == job_id)
        {
            return false;
        }

        // The Runner can retain at most 64 authoritative terminal Jobs. Keep an
        // exact marker for every Job still present in this reconciliation snapshot;
        // if stale markers fill the bound, discard one that the authoritative
        // snapshot can no longer name before inserting the new identity.
        while record.materialized_validation_job_ids.len() >= MAX_MATERIALIZED_VALIDATION_JOB_IDS {
            let Some(stale_index) =
                record
                    .materialized_validation_job_ids
                    .iter()
                    .position(|materialized| {
                        !retained_terminal_job_ids.contains(&materialized.as_str())
                    })
            else {
                // A complete valid terminal snapshot cannot name more than the
                // bound. Fail closed rather than evicting a still-retained Job.
                return false;
            };
            record.materialized_validation_job_ids.remove(stale_index);
        }
        record
            .materialized_validation_job_ids
            .push_back(job_id.to_string());
        record.updated_at = record.updated_at.max(timestamp);
        append_event(record, event.clone(), max_events);
        true
    }

    /// Append bounded recorder-only CodingAgentRun lifecycle evidence. The
    /// explicit Workflow Session is provenance only: this path grants no Run
    /// authority and intentionally stores no prompt, ACP session id, event body,
    /// reasoning, tool payload, credential, or idempotency key.
    pub fn record_coding_agent_lifecycle_evidence(
        &self,
        session_id: &str,
        project: &str,
        run_id: &str,
        provider_id: &str,
        kind: &str,
        state: &str,
        execution_state: &str,
        terminal_stop_reason: Option<&str>,
        terminal_error_code: Option<&str>,
    ) -> bool {
        let project_matches = self
            .with_record_for_query(session_id, |record, _| {
                record.project.as_deref() == Some(project)
            })
            .unwrap_or(false);
        if !project_matches {
            return false;
        }
        let now = now_ts();
        self.push_event(coding_agent_lifecycle_event(
            session_id,
            project,
            run_id,
            provider_id,
            kind,
            state,
            execution_state,
            terminal_stop_reason,
            terminal_error_code,
            now,
        ))
    }

    /// Sole entry for appending a session ledger event.
    fn push_event(&self, event: SessionEvent) -> bool {
        let work_token = match self.ensure_mutation_persistence_admitted() {
            Ok(token) => token,
            Err(()) => return false,
        };
        let session_id = event.session_id.clone();
        let mut event = Some(event);
        let (cold, hot_closed) = {
            let mut inner = self.lock_inner();
            if !inner.prepare_active_session(&session_id) {
                return false;
            }

            enum PushEventBranch {
                Hot {
                    hot_closed: bool,
                    event: SessionEvent,
                },
                Cold(ColdSessionRecord),
            }
            let branch = {
                let Some(stored) = inner.sessions.get_mut(&session_id) else {
                    return false;
                };
                match stored {
                    StoredSession::Hot(record) => {
                        record.updated_at = now_ts();
                        let event = event.take().unwrap();
                        if event_observes_repository_edit(&event) {
                            record.repository_edit_observed = true;
                        }
                        let hot_closed = !record.lifecycle.allows_mutation();
                        PushEventBranch::Hot { hot_closed, event }
                    }
                    StoredSession::Cold(record) => PushEventBranch::Cold(record.clone()),
                }
            };
            match branch {
                PushEventBranch::Hot { hot_closed, event } => {
                    inner.append_event_mark_dirty(&session_id, event);
                    inner.touch(&session_id);
                    (None, hot_closed)
                }
                PushEventBranch::Cold(record) => (Some(record), false),
            }
        };
        let persisted = match cold {
            Some(cold) => {
                let event = event.as_ref().expect("cold append keeps source event");
                self.rewrite_cold_record(&session_id, cold, true, |record, max_events| {
                    record.updated_at = now_ts();
                    if event_observes_repository_edit(event) {
                        record.repository_edit_observed = true;
                    }
                    append_event(record, event.clone(), max_events);
                    let retained_event_start = retained_event_start_for_record(record);
                    if let Some(snapshot) = record.events.back() {
                        let ordinal = record.events_observed;
                        Some(ColdRewriteOutcome::EventUpsert {
                            ordinal,
                            event: Arc::clone(snapshot),
                            retained_event_start,
                        })
                    } else {
                        Some(ColdRewriteOutcome::Changed {
                            retained_event_start,
                        })
                    }
                })
            }
            None => true,
        };
        if persisted {
            if hot_closed {
                self.coldify_closed_session(&session_id);
            }
            return self.persist_after_mutation(&work_token).is_ok();
        }
        persisted
    }

    fn max_events_per_session(&self) -> usize {
        self.lock_inner().max_events_per_session
    }

    fn rewrite_cold_record(
        &self,
        session_id: &str,
        mut cold: ColdSessionRecord,
        touch: bool,
        mutate: impl Fn(&mut SessionRecord, usize) -> Option<ColdRewriteOutcome>,
    ) -> bool {
        let max_events_per_session = self.max_events_per_session();
        loop {
            let Some(mut record) = materialize_cold_session(&cold, max_events_per_session) else {
                tracing::warn!(session_id, "cold session materialization failed");
                return false;
            };
            let Some(outcome) = mutate(&mut record, max_events_per_session) else {
                return false;
            };
            let persisted = PersistedSessionRecord::from_record(&record, max_events_per_session);
            let next =
                match cold_session_from_persisted(&persisted, cold.project_instructions.clone()) {
                    Ok(next) => next,
                    Err(err) => {
                        tracing::warn!(
                            session_id,
                            "cold session rewrite serialization failed: {err}"
                        );
                        return false;
                    }
                };
            let mut inner = self.lock_inner();
            let cas = match inner.sessions.get(session_id) {
                None => ColdRewriteCas::Missing,
                Some(StoredSession::Hot(_)) => ColdRewriteCas::OccupiedHot,
                Some(StoredSession::Cold(current)) if Arc::ptr_eq(&current.raw, &cold.raw) => {
                    ColdRewriteCas::Matches
                }
                Some(StoredSession::Cold(current)) => ColdRewriteCas::Stale(current.clone()),
            };
            match cas {
                ColdRewriteCas::Matches => {
                    if !inner
                        .database_persistence
                        .ensure_revision_capacity(session_id)
                    {
                        inner.note_revision_admission_failure(session_id);
                        return false;
                    }
                    let admitted = match &outcome {
                        ColdRewriteOutcome::Changed {
                            retained_event_start,
                        } => {
                            inner.mark_session_state_dirty(session_id, *retained_event_start)
                        }
                        ColdRewriteOutcome::EventUpsert {
                            ordinal,
                            event,
                            retained_event_start,
                        } => inner.mark_session_event_upsert(
                            session_id,
                            *ordinal,
                            Arc::clone(event),
                            *retained_event_start,
                        ),
                    };
                    if !admitted {
                        inner.note_revision_admission_failure(session_id);
                        return false;
                    }
                    inner
                        .sessions
                        .insert(session_id.to_owned(), StoredSession::Cold(next));
                    if touch {
                        inner.touch(session_id);
                    }
                    return true;
                }
                ColdRewriteCas::Stale(latest) => cold = latest,
                ColdRewriteCas::OccupiedHot | ColdRewriteCas::Missing => return false,
            }
        }
    }

    fn set_event_permission(
        &self,
        session_id: &str,
        event_id: &str,
        permission: PermissionDecision,
    ) -> bool {
        self.update_retained_record(session_id, permission, |record, permission| {
            let ordinal = event_ordinal_for_id(record, event_id)?;
            let event = record
                .events
                .iter_mut()
                .rev()
                .find(|event| event.event_id == event_id)?;
            Arc::make_mut(event).permission = Some(permission);
            Some((ordinal, Arc::clone(event)))
        })
    }

    fn set_session_close_persistent_shell_evidence(
        &self,
        session_id: &str,
        evidence: PersistentShellEventEvidence,
    ) -> bool {
        self.update_retained_record(session_id, evidence, |record, evidence| {
            let event = record.events.iter().rev().find(|event| {
                event.kind == "session_closed" && event.tool_name == "close_session"
            })?;
            let ordinal = event_ordinal_for_id(record, event.event_id.as_str())?;
            let event = record
                .events
                .iter_mut()
                .rev()
                .find(|event| {
                    event.kind == "session_closed" && event.tool_name == "close_session"
                })?;
            Arc::make_mut(event).persistent_shell = Some(evidence);
            record.updated_at = now_ts();
            Some((ordinal, Arc::clone(event)))
        })
    }

    pub(super) fn persist_after_mutation(&self, _work_token: &DurableWorkToken) -> Result<(), ()> {
        if !self.durable_persistence_configured() {
            // `durable_persistence_configured` may signal fatal on poison; do not treat that as in-memory.
            if self.persistence_fatal_reason().is_some() {
                return Err(());
            }
            return Ok(());
        }
        if self.persistence_fatal_reason().is_some() {
            return Err(());
        }
        if let Some(writer) = &self.writer {
            // Fire-and-forget: the dedicated writer thread flushes the
            // configured durable backend (SQLite batch or JSON snapshot).
            // Narrow restart-safe operations use `persist_after_mutation_durable`.
            writer.mark_dirty()?;
            return Ok(());
        }
        self.persist_after_mutation_with_admitted(write_ledger_atomic)
    }

    /// Reserve a durability admission lane before a durable business mutation.
    pub(super) async fn prepare_durable_mutation(
        &self,
    ) -> Result<DurableMutationAdmission, ()> {
        let work_token = self.ensure_mutation_persistence_admitted()?;
        if let Some(writer) = &self.writer {
            return Ok(DurableMutationAdmission {
                work_token,
                writer: Some(Arc::clone(writer)),
                fallback_guard: None,
            });
        }
        let fallback_guard = Arc::clone(&self.session_durability.fallback_admission)
            .lock_owned()
            .await;
        if self.persistence_fatal_reason().is_some() {
            return Err(());
        }
        Ok(DurableMutationAdmission {
            work_token,
            writer: None,
            fallback_guard: Some(fallback_guard),
        })
    }

    /// Reserve an observation durability lane without business mutation in-flight count.
    pub(super) async fn prepare_durable_observation_barrier(
        &self,
    ) -> Result<DurableObservationAdmission, ()> {
        self.ensure_observation_persistence_admitted()?;
        if let Some(writer) = &self.writer {
            return Ok(DurableObservationAdmission {
                writer: Some(Arc::clone(writer)),
                fallback_guard: None,
            });
        }
        let fallback_guard = Arc::clone(&self.session_durability.fallback_admission)
            .lock_owned()
            .await;
        self.ensure_observation_persistence_admitted()?;
        Ok(DurableObservationAdmission {
            writer: None,
            fallback_guard: Some(fallback_guard),
        })
    }

    async fn complete_durable_barrier_inner(
        &self,
        writer: Option<Arc<LedgerWriterGuard>>,
        fallback_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
        work_token: Option<DurableWorkToken>,
    ) -> Result<(), ()> {
        match (writer, fallback_guard) {
            (Some(writer), None) => {
                let _work_token = work_token;
                let generation = writer.mark_dirty().map_err(|()| ())?;
                if writer.flush_through(generation).await.is_err() {
                    return Err(());
                }
            }
            (None, Some(guard)) => {
                let store = self.clone();
                let join = tokio::task::spawn_blocking(move || {
                    let _guard = guard;
                    let _work_token = work_token;
                    store.persist_after_mutation_with_admitted(write_ledger_atomic)
                });
                match join.await {
                    Ok(Ok(())) => {}
                    _ => return Err(()),
                }
            }
            (Some(_), Some(_)) | (None, None) => return Err(()),
        }
        self.persistence_health_after_barrier()
    }

    /// Complete a durable barrier using admission reserved before the mutation.
    pub(super) async fn complete_durable_mutation(
        &self,
        admission: DurableMutationAdmission,
    ) -> Result<(), ()> {
        let DurableMutationAdmission {
            work_token,
            writer,
            fallback_guard,
        } = admission;
        self.complete_durable_barrier_inner(writer, fallback_guard, Some(work_token))
            .await
    }

    /// Complete an observation-only durable barrier.
    pub(super) async fn complete_durable_observation_barrier(
        &self,
        admission: DurableObservationAdmission,
    ) -> Result<(), ()> {
        let DurableObservationAdmission {
            writer,
            fallback_guard,
        } = admission;
        self.complete_durable_barrier_inner(writer, fallback_guard, None)
            .await
    }

    fn persistence_health_after_barrier(&self) -> Result<(), ()> {
        if self.persistence_fatal_reason().is_some() {
            return Err(());
        }
        let Some(inner) = self.try_lock_inner() else {
            return Err(());
        };
        if inner
            .persistence
            .as_ref()
            .and_then(|persistence| persistence.last_persist_error.as_ref())
            .is_some()
        {
            Err(())
        } else {
            Ok(())
        }
    }

    /// Persist this exact mutation generation before returning success. Used
    /// only by low-frequency operations whose success response promises
    /// restart-safe idempotency; ordinary Session events remain asynchronous.
    pub(crate) async fn persist_after_mutation_durable(&self) -> Result<(), ()> {
        let admission = self.prepare_durable_observation_barrier().await?;
        self.complete_durable_observation_barrier(admission).await
    }

    /// Blocking durable barrier for tests, synchronous tool paths, and
    /// root-test-support where async flush would pin a worker.
    #[cfg(any(test, feature = "root-test-support"))]
    pub(super) fn persist_after_mutation_durable_blocking(&self) -> Result<(), ()> {
        self.ensure_observation_persistence_admitted()?;
        if let Some(writer) = &self.writer {
            let generation = writer.mark_dirty()?;
            writer.flush_through_blocking(generation)?;
        } else {
            self.persist_after_mutation_with_admitted(write_ledger_atomic)?;
        }
        self.persistence_health_after_barrier()
    }

    fn sync_persist_write_before_lock(&self, mode: SyncPersistWriteMode) -> Result<(), ()> {
        if self.persistence_fatal_reason().is_some() {
            return Err(());
        }
        if mode == SyncPersistWriteMode::Normal
            && self.session_durability.refuses_mutation_admission()
        {
            return Err(());
        }
        Ok(())
    }

    fn sync_persist_write_after_lock(&self, mode: SyncPersistWriteMode) -> Result<(), ()> {
        if self.persistence_fatal_reason().is_some() {
            return Err(());
        }
        if mode == SyncPersistWriteMode::Normal
            && self.session_durability.refuses_mutation_admission()
        {
            return Err(());
        }
        Ok(())
    }

    fn sync_persist_write_body(
        &self,
        write_ledger: impl FnOnce(&PathBuf, &PersistedSessionLedger) -> io::Result<()>,
    ) -> Result<(), ()> {
        let sqlite_database = {
            let Some(inner) = self.try_lock_inner() else {
                return Err(());
            };
            if durable_write_uses_sqlite(&inner) {
                inner
                    .persistence
                    .as_ref()
                    .and_then(|persistence| persistence.database.clone())
            } else {
                None
            }
        };
        if let Some(database) = sqlite_database {
            loop {
                let (_, outcome) = flush_sqlite_batch(&self.inner, database.as_ref());
                if record_persist_outcome(&self.inner, &outcome).is_err() {
                    self.signal_persistence_fatal("session store mutex poisoned");
                    return Err(());
                }
                self.record_writer_fatal_outcome(&outcome);
                if outcome.disposition != PersistFlushDisposition::Success {
                    return Err(());
                }
                if !outcome.more_pending {
                    break;
                }
            }
            return Ok(());
        }
        let Some((path, ledger)) = ({
            let Some(inner) = self.try_lock_inner() else {
                return Err(());
            };
            inner
                .persistence
                .as_ref()
                .map(|persistence| (persistence.path.clone(), inner.to_persisted_ledger()))
        }) else {
            return Ok(());
        };
        let outcome = match write_ledger(&path, &ledger) {
            Ok(()) => PersistFlushOutcome {
                disposition: PersistFlushDisposition::Success,
                error_summary: None,
                allocation_error: None,
                written_bytes: None,
                more_pending: false,
                partial_chunk_success: false,
            },
            Err(err) => ledger_io_error_outcome(path.as_path(), err),
        };
        if record_persist_outcome(&self.inner, &outcome).is_err() {
            self.signal_persistence_fatal("session store mutex poisoned");
            return Err(());
        }
        self.record_writer_fatal_outcome(&outcome);
        if outcome.disposition != PersistFlushDisposition::Success {
            Err(())
        } else {
            Ok(())
        }
    }

    pub(super) fn persist_after_mutation_with(
        &self,
        write_ledger: impl FnOnce(&PathBuf, &PersistedSessionLedger) -> io::Result<()>,
    ) -> Result<(), ()> {
        self.sync_persist_write_before_lock(SyncPersistWriteMode::Normal)?;
        let _write_guard = match self.persistence_write_mutex.lock() {
            Ok(guard) => guard,
            Err(_) => {
                self.signal_persistence_fatal("session persistence mutex poisoned");
                return Err(());
            }
        };
        self.sync_persist_write_after_lock(SyncPersistWriteMode::Normal)?;
        self.sync_persist_write_body(write_ledger)
    }

    /// Synchronous persist body using [`SyncPersistWriteMode::AdmittedWork`] (see enum docs).
    fn persist_after_mutation_with_admitted(
        &self,
        write_ledger: impl FnOnce(&PathBuf, &PersistedSessionLedger) -> io::Result<()>,
    ) -> Result<(), ()> {
        self.sync_persist_write_before_lock(SyncPersistWriteMode::AdmittedWork)?;
        let _write_guard = match self.persistence_write_mutex.lock() {
            Ok(guard) => guard,
            Err(_) => {
                self.signal_persistence_fatal("session persistence mutex poisoned");
                return Err(());
            }
        };
        self.sync_persist_write_after_lock(SyncPersistWriteMode::AdmittedWork)?;
        self.sync_persist_write_body(write_ledger)
    }

    /// Installs the SQLite durable backend handle used by incremental writer flush.
    /// Production startup installs it via `with_persistence_database_limits` after
    /// restore and seed; tests may attach manually after seeding dirty state.
    pub(crate) fn attach_session_database(
        &self,
        database: Arc<SessionDatabase>,
        durable_db_path: PathBuf,
    ) {
        let mut inner = self.lock_inner();
        let Some(persistence) = inner.persistence.as_mut() else {
            return;
        };
        persistence.database = Some(database);
        persistence.durable_db_path = Some(durable_db_path);
    }

    #[cfg(feature = "root-test-support")]
    pub fn persist_after_mutation_with_for_test(
        &self,
        write_ledger: impl FnOnce(&PathBuf, &PersistedSessionLedger) -> io::Result<()>,
    ) -> Result<(), ()> {
        self.persist_after_mutation_with(write_ledger)
    }
}

/// Authoritative in-memory transitions for workflow session state.
///
/// Map fields stay private; sibling modules must use these helpers so create,
/// message, and event mutations cannot bypass the store.
/// Tools blocked on Closed workflow sessions.
///
/// Query and pure-read tools remain allowed. Message-board mutations and
/// session-scoped checkpoint mutations are blocked even when metadata marks
/// them read-only (they still change durable session/project evidence).
fn lifecycle_blocks_tool(
    classification: SessionToolContract,
    lifecycle_effect: ToolSessionLifecycleEffect,
) -> bool {
    classification.write_like
        || classification.shell_like
        || lifecycle_effect == ToolSessionLifecycleEffect::Mutation
}

/// Construct a new event without inventing identity, time, or outcome facts.
/// This is deliberately private: the public/persisted model keeps its existing
/// required fields and serde compatibility rules.
fn session_event(
    event_id: String,
    session_id: String,
    kind: String,
    timestamp: i64,
    transport: String,
    tool_name: String,
    risk_class: String,
) -> SessionEvent {
    SessionEvent {
        event_id,
        call_id: None,
        logical_invocation_id: None,
        logical_invocation_role: None,
        session_id,
        kind,
        legacy_context_revision: None,
        context_result_summary: None,
        timestamp,
        transport,
        tool_name,
        project: None,
        resolved_project: None,
        risk_class,
        read_like: false,
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
    }
}

/// Both halves of a tool call inherit the same accepted metadata. Completion
/// supplies its new event id and result evidence at the call site.
fn tool_call_event(start: ToolCallStart, kind: &str, timestamp: i64) -> SessionEvent {
    SessionEvent {
        call_id: Some(start.call_id),
        logical_invocation_id: start.logical_invocation_id,
        logical_invocation_role: start.logical_invocation_role,
        project: start.project,
        resolved_project: start.resolved_project,
        read_like: start.read_like,
        write_like: start.write_like,
        shell_like: start.shell_like,
        git_like: start.git_like,
        change_summary_like: start.change_summary_like,
        diff_review_like: start.diff_review_like,
        started_at: Some(start.started_at),
        expected_failure: start.expectation.expected_failure.then_some(true),
        expected_failure_kind: start.expectation.expected_failure_kind,
        result_expectation: start.expectation.result_expectation,
        accepted_exit_codes: start.expectation.accepted_exit_codes,
        assertion_name: start.expectation.assertion_name,
        changed_paths: start.changed_paths,
        observed_paths: start.observed_paths,
        permission: start.permission,
        ..session_event(
            start.event_id,
            start.session_id,
            kind.to_string(),
            timestamp,
            start.transport.as_str().to_string(),
            start.tool_name,
            start.risk_class,
        )
    }
}

/// Caller retains the store lock and owns activity/persistence ordering.
fn append_event(record: &mut SessionRecord, event: SessionEvent, max_events: usize) {
    record.events.push_back(Arc::new(event));
    record.events_observed = record.events_observed.saturating_add(1);
    while record.events.len() > max_events {
        record.events.pop_front();
    }
}

impl SessionStoreInner {
    pub(super) fn append_event_mark_dirty(&mut self, session_id: &str, event: SessionEvent) {
        let (ordinal, appended, retained_event_start) = {
            let record = self
                .sessions
                .get_mut(session_id)
                .and_then(StoredSession::hot_mut)
                .expect("append_event_mark_dirty requires a hot session record");
            append_event(record, event, self.max_events_per_session);
            let ordinal = record.events_observed;
            let appended = record.events.back().map(Arc::clone);
            let retained_event_start = retained_event_start_for_record(record);
            (ordinal, appended, retained_event_start)
        };
        match appended {
            Some(event) => {
                self.mark_session_event_upsert(session_id, ordinal, event, retained_event_start)
            }
            None => self.mark_session_state_dirty(session_id, retained_event_start),
        };
    }
}

#[allow(clippy::too_many_arguments)]
fn coding_instruction_event(
    event_id: &str,
    session_id: &str,
    project: &str,
    instruction: Option<String>,
    transport: SessionTransport,
    requested_mode: SessionMode,
    previous_mode: Option<SessionMode>,
    requested_guards: SessionGuards,
    previous_guards: Option<SessionGuards>,
    capability_changed: bool,
    context_refreshed: bool,
    execution_context: Option<SessionExecutionContext>,
    previous_execution_context: Option<SessionExecutionContext>,
    execution_context_changed: bool,
    reused: bool,
    explicit_resume: bool,
    now: i64,
) -> SessionEvent {
    SessionEvent {
        project: Some(project.to_string()),
        resolved_project: Some(project.to_string()),
        read_like: true,
        started_at: Some(now),
        finished_at: Some(now),
        duration_ms: Some(0),
        status: Some("succeeded".to_string()),
        input_summary: Some(redact_and_bound_value(&serde_json::json!({
            "requested_mode": requested_mode.as_str(),
            "requested_guards": requested_guards,
            "capability_changed": capability_changed,
            "context_refreshed": context_refreshed,
            "execution_context_provided": execution_context.is_some(),
            "execution_context_changed": execution_context_changed,
            "execution_context": execution_context,
            "previous_execution_context": previous_execution_context,
            "session_reused": reused,
            "explicit_resume": explicit_resume,
        }))),
        instruction,
        requested_mode: Some(requested_mode.as_str().to_string()),
        previous_mode: previous_mode.map(|mode| mode.as_str().to_string()),
        requested_guards: Some(requested_guards),
        previous_guards,
        capability_changed: Some(capability_changed),
        context_refreshed: Some(context_refreshed),
        execution_context,
        previous_execution_context,
        execution_context_changed: Some(execution_context_changed),
        ..session_event(
            event_id.to_string(),
            session_id.to_string(),
            "task_instruction".to_string(),
            now,
            transport.as_str().to_string(),
            "work_on_project".to_string(),
            "read_only".to_string(),
        )
    }
}

fn coding_agent_lifecycle_event(
    session_id: &str,
    project: &str,
    run_id: &str,
    provider_id: &str,
    kind: &str,
    state: &str,
    execution_state: &str,
    terminal_stop_reason: Option<&str>,
    terminal_error_code: Option<&str>,
    now: i64,
) -> SessionEvent {
    let input_summary = serde_json::json!({
        "run_id": bound_summary_string(run_id),
        "provider_id": bound_summary_string(provider_id),
        "state": bound_summary_string(state),
        "execution_state": bound_summary_string(execution_state),
        "terminal_stop_reason": terminal_stop_reason.map(bound_summary_string),
        "terminal_error_code": terminal_error_code.map(bound_summary_string),
    });
    SessionEvent {
        project: Some(project.to_string()),
        resolved_project: Some(project.to_string()),
        started_at: Some(now),
        finished_at: Some(now),
        duration_ms: Some(0),
        status: Some(bound_summary_string(state)),
        error_kind: terminal_error_code.map(bound_summary_string),
        input_summary: Some(input_summary),
        ..session_event(
            format!("{EVENT_ID_PREFIX}{}", uuid::Uuid::new_v4().simple()),
            session_id.to_string(),
            bound_summary_string(kind),
            now,
            "system".to_string(),
            "start_coding_agent".to_string(),
            "job_run".to_string(),
        )
    }
}

fn session_closed_system_event(session_id: &str, now: i64) -> SessionEvent {
    SessionEvent {
        read_like: true,
        started_at: Some(now),
        finished_at: Some(now),
        duration_ms: Some(0),
        status: Some("succeeded".to_string()),
        ..session_event(
            format!("{EVENT_ID_PREFIX}{}", uuid::Uuid::new_v4().simple()),
            session_id.to_string(),
            "session_closed".to_string(),
            now,
            "system".to_string(),
            "close_session".to_string(),
            "read_only".to_string(),
        )
    }
}

fn session_execution_context_updated_event(
    session_id: &str,
    project: &str,
    transport: SessionTransport,
    execution_context: SessionExecutionContext,
    previous_execution_context: SessionExecutionContext,
    changed: bool,
    now: i64,
) -> SessionEvent {
    SessionEvent {
        project: Some(project.to_string()),
        resolved_project: Some(project.to_string()),
        read_like: true,
        started_at: Some(now),
        finished_at: Some(now),
        duration_ms: Some(0),
        status: Some("succeeded".to_string()),
        input_summary: Some(redact_and_bound_value(&serde_json::json!({
            "execution_context": execution_context,
            "previous_execution_context": previous_execution_context,
            "execution_context_changed": changed,
        }))),
        execution_context: Some(execution_context),
        previous_execution_context: Some(previous_execution_context),
        execution_context_changed: Some(changed),
        ..session_event(
            format!("{EVENT_ID_PREFIX}{}", uuid::Uuid::new_v4().simple()),
            session_id.to_string(),
            "session_execution_context_updated".to_string(),
            now,
            transport.as_str().to_string(),
            "update_session_context".to_string(),
            "read_only".to_string(),
        )
    }
}

fn event_observes_repository_edit(event: &SessionEvent) -> bool {
    event.kind == "tool_call_finished"
        && event.status.as_deref() == Some("succeeded")
        && event
            .effect_evidence
            .as_ref()
            .and_then(|evidence| evidence.state_changed)
            == Some(true)
        && runtime_tool_activity_semantics(&event.tool_name).kind == ToolActivityKind::Edit
}

fn summarize_record(
    record: &SessionRecord,
    limit: Option<usize>,
    cold: Option<&ColdSessionRecord>,
) -> SessionSummary {
    let limit = limit
        .unwrap_or(DEFAULT_SUMMARY_LIMIT)
        .clamp(0, MAX_SUMMARY_LIMIT);
    let finished_events = super::events::canonical_tool_call_finished_event_refs(
        record.events.iter().map(Arc::as_ref),
    );
    let counts = SessionCounts {
        tool_calls: finished_events.len(),
        succeeded: finished_events
            .iter()
            .filter(|event| event.status.as_deref() == Some("succeeded"))
            .count(),
        failed: finished_events
            .iter()
            .filter(|event| event.status.as_deref() == Some("failed"))
            .count(),
        read_like: finished_events
            .iter()
            .filter(|event| event.read_like)
            .count(),
        write_like: finished_events
            .iter()
            .filter(|event| event.write_like)
            .count(),
        shell_like: finished_events
            .iter()
            .filter(|event| event.shell_like)
            .count(),
        git_like: finished_events
            .iter()
            .filter(|event| event.git_like)
            .count(),
        change_summary_like: finished_events
            .iter()
            .filter(|event| event.change_summary_like)
            .count(),
    };
    let retained_total = record.events.len();
    let observed_total = record.events_observed.max(retained_total as u64) as usize;
    let skip = retained_total.saturating_sub(limit);
    let events: Vec<SessionEvent> = record
        .events
        .iter()
        .skip(skip)
        .map(|event| event.as_ref().clone())
        .collect();
    let events_returned = events.len();
    let project_instructions = match cold {
        Some(cold) => cold.project_instructions.clone(),
        None => record
            .project_instructions
            .as_ref()
            .map(|snapshot| snapshot.to_summary()),
    };
    let retained_unclosed_mutating_tool_call =
        crate::continuation::retained_history_has_unclosed_mutating_tool_calls(
            record.events.iter().map(|event| event.as_ref()),
            &record.session_id,
        );
    SessionSummary {
        session_id: record.session_id.clone(),
        project: record.project.clone(),
        title: record.title.clone(),
        mode: record.mode,
        guards: record.guards,
        execution_context: record.execution_context.clone(),
        lifecycle: record.lifecycle,
        git_baseline_tree: record.git_baseline_tree.clone(),
        repository_edit_observed: record.repository_edit_observed,
        created_at: record.created_at,
        updated_at: record.updated_at,
        counts,
        events,
        events_total: observed_total,
        events_retained: retained_total,
        events_evicted: observed_total.saturating_sub(retained_total),
        retention_truncated: observed_total > retained_total,
        retained_unclosed_mutating_tool_call,
        ledger_first_retained_sequence: observed_total.saturating_sub(retained_total),
        events_returned,
        events_truncated: observed_total > events_returned,
        first_retained_sequence: observed_total.saturating_sub(events_returned),
        project_instructions,
        messages: build_messages_summary(record),
    }
}

fn session_message_delivery_identity(
    delivery: &SessionMessageDelivery,
    kind: super::model::SessionMessageKind,
    message: &str,
    tags: &[String],
    reply_to: Option<&str>,
    priority: SessionMessagePriority,
    requires_ack: bool,
) -> Result<(String, String), SessionMessageError> {
    if delivery.sender_scope.len() != 64
        || !delivery
            .sender_scope
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(SessionMessageError::InvalidInput(
            "message delivery sender scope is unavailable".to_string(),
        ));
    }
    let delivery_key = delivery.delivery_key.trim();
    if delivery_key.is_empty() || delivery_key.chars().count() > MAX_MESSAGE_DELIVERY_KEY_CHARS {
        return Err(SessionMessageError::InvalidInput(format!(
            "delivery_key must contain 1..={MAX_MESSAGE_DELIVERY_KEY_CHARS} characters"
        )));
    }
    let mut key_hasher = Sha256::new();
    key_hasher.update(b"webcodex.session-message-delivery-key.v1\0");
    key_hasher.update(delivery_key.as_bytes());
    let scope_key = format!("{}:{:x}", delivery.sender_scope, key_hasher.finalize());

    fn hash_field(hasher: &mut Sha256, value: &[u8]) {
        hasher.update((value.len() as u64).to_be_bytes());
        hasher.update(value);
    }
    let mut payload = Sha256::new();
    payload.update(b"webcodex.session-message-delivery-payload.v1\0");
    hash_field(&mut payload, kind.as_str().as_bytes());
    hash_field(&mut payload, message.as_bytes());
    payload.update((tags.len() as u64).to_be_bytes());
    for tag in tags {
        hash_field(&mut payload, tag.as_bytes());
    }
    hash_field(&mut payload, reply_to.unwrap_or_default().as_bytes());
    let priority = match priority {
        SessionMessagePriority::Low => "low",
        SessionMessagePriority::Normal => "normal",
        SessionMessagePriority::High => "high",
    };
    hash_field(&mut payload, priority.as_bytes());
    payload.update([u8::from(requires_ack)]);
    Ok((scope_key, format!("{:x}", payload.finalize())))
}

impl SessionStoreInner {
    // --- create / lifecycle ---

    /// Sole map-insert path for a newly created session.
    // Called under the store mutex; every fresh-session path uses this allocator.
    pub(super) fn allocate_session_id(&self, mut suffix: impl FnMut() -> String) -> Option<String> {
        for _ in 0..16 {
            let id = format!("{SESSION_ID_PREFIX}{}", suffix());
            if !self.session_id_occupied(&id) {
                return Some(id);
            }
        }
        None
    }

    /// Production session create with SQLite revision preflight. Refuses RAM insert when
    /// revision admission fails (unseeded/in-memory always admits).
    pub(super) fn try_insert_session(
        &mut self,
        record: SessionRecord,
    ) -> Result<SessionSummary, ()> {
        assert!(
            !self.session_id_occupied(&record.session_id),
            "Session allocation must not replace an existing ledger"
        );
        let session_id = record.session_id.clone();
        if !self
            .database_persistence
            .ensure_new_session_revision_capacity(&session_id)
        {
            self.note_revision_admission_failure(&session_id);
            return Err(());
        }
        self.sessions
            .insert(session_id.clone(), StoredSession::Hot(record));
        self.touch(&session_id);
        if !self.mark_session_created_dirty(&session_id) {
            self.sessions.remove(&session_id);
            self.lru.remove(&session_id);
            self.database_persistence.session_dirty.remove(&session_id);
            self.note_revision_admission_failure(&session_id);
            return Err(());
        }
        self.enforce_historical_retention_bound();
        self.compact_hot_sessions(&session_id);

        Ok(self
            .summary(&session_id, Some(DEFAULT_SUMMARY_LIMIT))
            .expect("newly inserted session must summarize"))
    }

    pub(super) fn insert_session(&mut self, record: SessionRecord) -> SessionSummary {
        self.try_insert_session(record).expect(
            "insert_session assumes revision admission succeeds (in-memory/tests)",
        )
    }

    /// Explicit lifecycle close. Unknown ids fail without create.
    pub(super) fn close_session(
        &mut self,
        session_id: &str,
    ) -> Result<Option<SessionCloseOutcome>, SessionCloseError> {
        if !self.prepare_active_session(session_id) {
            return Err(SessionCloseError::UnknownSession);
        }
        self.touch(session_id);

        let lifecycle = self
            .sessions
            .get(session_id)
            .map(StoredSession::lifecycle)
            .ok_or(SessionCloseError::UnknownSession)?;
        match lifecycle {
            SessionLifecycle::Closed => {
                let Some(record) = self.sessions.get(session_id).and_then(StoredSession::hot)
                else {
                    return Ok(None);
                };
                Ok(Some(SessionCloseOutcome {
                    summary: summarize_record(record, Some(DEFAULT_SUMMARY_LIMIT), None),
                    already_closed: true,
                }))
            }
            SessionLifecycle::Active => {
                let now = now_ts();
                let event = session_closed_system_event(session_id, now);
                {
                    let record = self
                        .sessions
                        .get_mut(session_id)
                        .and_then(StoredSession::hot_mut)
                        .expect("active session must stay hot through close commit");
                    record.lifecycle = SessionLifecycle::Closed;
                    record.updated_at = now;
                }
                self.append_event_mark_dirty(session_id, event);
                // Update eviction eligibility in the same locked lifecycle commit.
                self.touch(session_id);
                let record = self
                    .sessions
                    .get(session_id)
                    .and_then(StoredSession::hot)
                    .expect("just-closed session remains hot until outer coldification");
                Ok(Some(SessionCloseOutcome {
                    summary: summarize_record(record, Some(DEFAULT_SUMMARY_LIMIT), None),
                    already_closed: false,
                }))
            }
        }
    }

    // --- messages ---

    pub(super) fn post_message(
        &mut self,
        input: PostSessionMessageInput,
        requires_ack: bool,
        delivery: Option<SessionMessageDelivery>,
    ) -> Result<SessionMessageDeliveryOutcome, SessionMessageError> {
        if !self.prepare_active_session(&input.session_id) {
            return Err(SessionMessageError::InvalidObservationState);
        }
        self.touch(&input.session_id);
        let Some(stored) = self.sessions.get_mut(&input.session_id) else {
            return Err(SessionMessageError::UnknownSession);
        };
        let lifecycle = stored.lifecycle();
        if !lifecycle.allows_mutation() {
            return Err(SessionMessageError::SessionClosed { lifecycle });
        }
        let record = stored
            .hot_mut()
            .expect("active touched session message mutation must stay hot");
        let message = validate_message_text(input.message)?;
        let tags = validate_message_tags(input.tags)?;
        if let Some(reply_to) = input.reply_to.as_deref() {
            let found = record
                .messages
                .iter()
                .any(|message| message.message_id == reply_to);
            if !found {
                return Err(SessionMessageError::UnknownMessage);
            }
        }
        let delivery_identity = delivery
            .as_ref()
            .map(|delivery| {
                session_message_delivery_identity(
                    delivery,
                    input.kind,
                    &message,
                    &tags,
                    input.reply_to.as_deref(),
                    input.priority,
                    requires_ack,
                )
            })
            .transpose()?;
        if let Some((scope_key, payload_fingerprint)) = delivery_identity.as_ref() {
            if let Some(replay) = record.message_delivery_replays.get(scope_key) {
                if replay.payload_fingerprint != *payload_fingerprint {
                    return Err(SessionMessageError::DeliveryKeyConflict);
                }
                let Some(message) = record
                    .messages
                    .iter()
                    .find(|message| message.message_id == replay.message_id)
                else {
                    return Err(SessionMessageError::DeliveryKeyConflict);
                };
                return Ok(SessionMessageDeliveryOutcome {
                    message: message.as_ref().clone(),
                    replayed: true,
                    state_changed: false,
                });
            }
        }
        let now = now_ts();
        let message_id = allocate_message_id(record, webcodex_core::compact::random_suffix::<12>)?;
        let message = SessionMessage {
            message_id,
            session_id: input.session_id.clone(),
            created_at: now,
            kind: input.kind,
            status: SessionMessageStatus::Open,
            priority: input.priority,
            message,
            tags,
            reply_to: input.reply_to,
            requires_ack,
            first_ack_observed_at: None,
            author_session_id: None,
            resolved_at: None,
            resolution: None,
            closure_kind: None,
            superseded_by_message_id: None,
            supersedes_message_id: None,
            resolved_by_message_id: None,
            completion_id: None,
        };
        let revision = Self::next_message_observation_revision(record)?;
        record.updated_at = now;
        record.messages.push_back(Arc::new(message.clone()));
        record
            .message_observation_revisions
            .insert(message.message_id.clone(), revision);
        if let Some((scope_key, payload_fingerprint)) = delivery_identity {
            record.message_delivery_replays.insert(
                scope_key,
                SessionMessageDeliveryReplay {
                    payload_fingerprint,
                    message_id: message.message_id.clone(),
                },
            );
        }
        while record.messages.len() > DEFAULT_MAX_MESSAGES_PER_SESSION {
            if let Some(evicted) = record.messages.pop_front() {
                record
                    .message_delivery_replays
                    .retain(|_, replay| replay.message_id != evicted.message_id);
                Self::note_evicted_message_observation(record, evicted.as_ref());
            }
        }
        self.mark_session_dirty(&input.session_id);
        Ok(SessionMessageDeliveryOutcome {
            message,
            replayed: false,
            state_changed: true,
        })
    }

    pub(super) fn observe_message_acks(
        &mut self,
        session_id: &str,
        message_ids: &[String],
    ) -> super::model::SessionAckObservation {
        if !self.prepare_active_session(session_id) {
            return super::model::SessionAckObservation {
                ignored_count: message_ids.len(),
                ..Default::default()
            };
        }

        let Some(stored) = self.sessions.get_mut(session_id) else {
            return super::model::SessionAckObservation {
                ignored_count: message_ids.len(),
                ..Default::default()
            };
        };
        let Some(record) = stored.hot_mut() else {
            return super::model::SessionAckObservation {
                ignored_count: message_ids.len(),
                ..Default::default()
            };
        };
        let mut outcome = super::model::SessionAckObservation::default();
        let mut seen = std::collections::HashSet::new();
        let now = now_ts();
        for message_id in message_ids {
            if !seen.insert(message_id.as_str()) {
                continue;
            }
            let Some(index) = record.messages.iter().position(|message| {
                message.message_id == *message_id
                    && message.status == SessionMessageStatus::Open
                    && message.requires_ack
            }) else {
                outcome.ignored_count += 1;
                continue;
            };
            outcome.accepted_count += 1;
            outcome.accepted_ids.push(message_id.clone());
            if record.messages[index].first_ack_observed_at.is_none() {
                let Ok(revision) = Self::next_message_observation_revision(record) else {
                    outcome.accepted_ids.pop();
                    outcome.accepted_count = outcome.accepted_count.saturating_sub(1);
                    outcome.ignored_count += 1;
                    continue;
                };
                let message = Arc::make_mut(&mut record.messages[index]);
                message.first_ack_observed_at = Some(now);
                record
                    .message_observation_revisions
                    .insert(message.message_id.clone(), revision);
                record.updated_at = now;
                outcome.first_observed_count += 1;
            }
        }
        if outcome.first_observed_count > 0 {
            self.mark_session_dirty(session_id);
        }
        outcome
    }

    pub(super) fn withdraw_message(
        &mut self,
        session_id: &str,
        message_id: &str,
    ) -> Result<WithdrawSessionMessageOutcome, SessionMessageError> {
        if !self.prepare_active_session(session_id) {
            return Err(SessionMessageError::InvalidObservationState);
        }
        self.touch(session_id);
        let Some(stored) = self.sessions.get_mut(session_id) else {
            return Err(SessionMessageError::UnknownSession);
        };
        let lifecycle = stored.lifecycle();
        if !lifecycle.allows_mutation() {
            return Err(SessionMessageError::SessionClosed { lifecycle });
        }
        let record = stored
            .hot_mut()
            .expect("active session message mutation must stay hot");
        let Some(message_index) = record
            .messages
            .iter()
            .position(|message| message.message_id == message_id)
        else {
            return Err(SessionMessageError::UnknownMessage);
        };
        let snapshot = record.messages[message_index].as_ref().clone();
        if snapshot.status == SessionMessageStatus::Resolved {
            if snapshot.closure_kind == Some(SessionMessageClosureKind::Withdrawn) {
                return Ok(WithdrawSessionMessageOutcome {
                    message: snapshot,
                    replayed: true,
                });
            }
            return Err(SessionMessageError::MessageNotOpen);
        }
        if !matches!(
            snapshot.kind,
            super::model::SessionMessageKind::Note
                | super::model::SessionMessageKind::Guidance
                | super::model::SessionMessageKind::Question
                | super::model::SessionMessageKind::Todo
        ) {
            return Err(SessionMessageError::InvalidInput(
                "message kind cannot be withdrawn by the Runtime Console".to_string(),
            ));
        }
        if snapshot.closure_kind.is_some()
            || snapshot.superseded_by_message_id.is_some()
            || snapshot.completion_id.is_some()
            || snapshot.resolved_by_message_id.is_some()
        {
            return Err(SessionMessageError::MessageNotOpen);
        }
        let revision = Self::next_message_observation_revision(record)?;
        let now = now_ts();
        let message = Arc::make_mut(&mut record.messages[message_index]);
        message.status = SessionMessageStatus::Resolved;
        message.resolved_at = Some(now);
        message.closure_kind = Some(SessionMessageClosureKind::Withdrawn);
        record
            .message_observation_revisions
            .insert(message.message_id.clone(), revision);
        record.updated_at = now;
        let message = message.clone();
        self.mark_session_dirty(session_id);
        Ok(WithdrawSessionMessageOutcome {
            message,
            replayed: false,
        })
    }

    pub(super) fn replace_message(
        &mut self,
        input: ReplaceSessionMessageInput,
    ) -> Result<ReplaceSessionMessageOutcome, SessionMessageError> {
        if !self.prepare_active_session(&input.session_id) {
            return Err(SessionMessageError::InvalidObservationState);
        }
        self.touch(&input.session_id);
        let Some(stored) = self.sessions.get_mut(&input.session_id) else {
            return Err(SessionMessageError::UnknownSession);
        };
        let lifecycle = stored.lifecycle();
        if !lifecycle.allows_mutation() {
            return Err(SessionMessageError::SessionClosed { lifecycle });
        }
        let replacement_text = validate_message_text(input.message)?;
        let record = stored
            .hot_mut()
            .expect("active session message mutation must stay hot");
        let Some(original_index) = record
            .messages
            .iter()
            .position(|message| message.message_id == input.message_id)
        else {
            return Err(SessionMessageError::UnknownMessage);
        };
        let original_snapshot = record.messages[original_index].as_ref().clone();
        if original_snapshot.status == SessionMessageStatus::Resolved {
            if original_snapshot.closure_kind != Some(SessionMessageClosureKind::Superseded) {
                return Err(SessionMessageError::MessageNotOpen);
            }
            let replacement_id = original_snapshot
                .superseded_by_message_id
                .as_deref()
                .ok_or(SessionMessageError::IdempotencyConflict)?;
            let replacement = record
                .messages
                .iter()
                .find(|message| message.message_id == replacement_id)
                .ok_or(SessionMessageError::IdempotencyConflict)?;
            let relation_is_canonical = replacement.supersedes_message_id.as_deref()
                == Some(input.message_id.as_str())
                && replacement.kind == original_snapshot.kind
                && replacement.priority == original_snapshot.priority
                && replacement.tags == original_snapshot.tags
                && replacement.reply_to == original_snapshot.reply_to
                && replacement.requires_ack == original_snapshot.requires_ack;
            if !relation_is_canonical || replacement.message != replacement_text {
                return Err(SessionMessageError::IdempotencyConflict);
            }
            return Ok(ReplaceSessionMessageOutcome {
                original: original_snapshot,
                replacement: replacement.as_ref().clone(),
                replayed: true,
            });
        }
        if !matches!(
            original_snapshot.kind,
            super::model::SessionMessageKind::Note
                | super::model::SessionMessageKind::Guidance
                | super::model::SessionMessageKind::Question
                | super::model::SessionMessageKind::Todo
        ) {
            return Err(SessionMessageError::InvalidInput(
                "message kind cannot be replaced by the Runtime Console".to_string(),
            ));
        }
        if original_snapshot.closure_kind.is_some()
            || original_snapshot.superseded_by_message_id.is_some()
            || original_snapshot.completion_id.is_some()
            || original_snapshot.resolved_by_message_id.is_some()
        {
            return Err(SessionMessageError::MessageNotOpen);
        }

        let message_id = allocate_message_id(record, webcodex_core::compact::random_suffix::<12>)?;
        let original_revision = record
            .message_observation_revision
            .checked_add(1)
            .ok_or(SessionMessageError::InvalidObservationState)?;
        let replacement_revision = original_revision
            .checked_add(1)
            .ok_or(SessionMessageError::InvalidObservationState)?;
        record.message_observation_revision = replacement_revision;
        let now = now_ts();
        let replacement = SessionMessage {
            message_id,
            session_id: input.session_id.clone(),
            created_at: now,
            kind: original_snapshot.kind,
            status: SessionMessageStatus::Open,
            priority: original_snapshot.priority,
            message: replacement_text,
            tags: original_snapshot.tags.clone(),
            reply_to: original_snapshot.reply_to.clone(),
            requires_ack: original_snapshot.requires_ack,
            first_ack_observed_at: None,
            author_session_id: None,
            resolved_at: None,
            resolution: None,
            closure_kind: None,
            superseded_by_message_id: None,
            supersedes_message_id: Some(input.message_id.clone()),
            resolved_by_message_id: None,
            completion_id: None,
        };
        {
            let original = Arc::make_mut(&mut record.messages[original_index]);
            original.status = SessionMessageStatus::Resolved;
            original.resolved_at = Some(now);
            original.closure_kind = Some(SessionMessageClosureKind::Superseded);
            original.superseded_by_message_id = Some(replacement.message_id.clone());
        }
        record.messages.push_back(Arc::new(replacement.clone()));
        record
            .message_observation_revisions
            .insert(input.message_id.clone(), original_revision);
        record
            .message_observation_revisions
            .insert(replacement.message_id.clone(), replacement_revision);
        while record.messages.len() > DEFAULT_MAX_MESSAGES_PER_SESSION {
            let protected_original_id = input.message_id.as_str();
            let protected_replacement_id = replacement.message_id.as_str();
            let Some(remove_index) = record.messages.iter().position(|message| {
                message.message_id != protected_original_id
                    && message.message_id != protected_replacement_id
            }) else {
                return Err(SessionMessageError::InvalidObservationState);
            };
            if let Some(evicted) = record.messages.remove(remove_index) {
                Self::note_evicted_message_observation(record, evicted.as_ref());
            }
        }
        record.updated_at = now;
        let original = record
            .messages
            .iter()
            .find(|message| message.message_id == input.message_id)
            .expect("superseded source retained")
            .as_ref()
            .clone();
        self.mark_session_dirty(&input.session_id);
        Ok(ReplaceSessionMessageOutcome {
            original,
            replacement,
            replayed: false,
        })
    }

    /// Resolve an open message. Already-resolved messages stay resolved
    /// (status is not reopened); an optional new resolution text may update.
    pub(super) fn resolve_message(
        &mut self,
        session_id: &str,
        message_id: &str,
        resolution: Option<String>,
    ) -> Result<(SessionMessage, bool), SessionMessageError> {
        if !self.prepare_active_session(session_id) {
            return Err(SessionMessageError::InvalidObservationState);
        }
        self.touch(session_id);
        let Some(stored) = self.sessions.get_mut(session_id) else {
            return Err(SessionMessageError::UnknownSession);
        };
        let lifecycle = stored.lifecycle();
        if !lifecycle.allows_mutation() {
            return Err(SessionMessageError::SessionClosed { lifecycle });
        }
        let record = stored
            .hot_mut()
            .expect("active session message mutation must stay hot");
        let Some(message_index) = record
            .messages
            .iter()
            .position(|message| message.message_id == message_id)
        else {
            return Err(SessionMessageError::UnknownMessage);
        };
        if record.messages[message_index].closure_kind.is_some() {
            return Err(SessionMessageError::MessageNotOpen);
        }
        let resolution = match resolution {
            Some(value) => Some(validate_resolution_text(value)?),
            None => None,
        };
        let snapshot = record.messages[message_index].as_ref().clone();
        let changed = snapshot.status == SessionMessageStatus::Open
            || resolution.as_ref().is_some_and(|resolution| {
                snapshot.resolution.as_ref() != Some(resolution)
            });
        if !changed {
            return Ok((snapshot, false));
        }
        let revision = Self::next_message_observation_revision(record)?;
        let now = now_ts();
        let message = Arc::make_mut(&mut record.messages[message_index]);
        if message.status == SessionMessageStatus::Open {
            message.status = SessionMessageStatus::Resolved;
            message.resolved_at = Some(now);
        }
        if let Some(resolution) = resolution {
            message.resolution = Some(resolution);
        }
        record.updated_at = now;
        record
            .message_observation_revisions
            .insert(message.message_id.clone(), revision);
        let message = message.clone();
        self.mark_session_dirty(session_id);
        Ok((message, true))
    }

    pub(super) fn resolve_message_from_wrapper(
        &mut self,
        session_id: &str,
        message_id: &str,
        resolution: String,
        current_request_acknowledged: bool,
    ) -> Result<(SessionMessage, bool), SessionMessageError> {
        if !self.prepare_active_session(session_id) {
            return Err(SessionMessageError::InvalidObservationState);
        }
        self.touch(session_id);
        let Some(stored) = self.sessions.get_mut(session_id) else {
            return Err(SessionMessageError::UnknownSession);
        };
        let lifecycle = stored.lifecycle();
        if !lifecycle.allows_mutation() {
            return Err(SessionMessageError::SessionClosed { lifecycle });
        }
        let resolution = validate_resolution_text(resolution)?;
        if resolution.is_empty() {
            return Err(SessionMessageError::InvalidInput(
                "session_message_resolution.resolution must not be empty".to_string(),
            ));
        }
        let record = stored
            .hot_mut()
            .expect("active session message mutation must stay hot");
        let Some(message_index) = record
            .messages
            .iter()
            .position(|message| message.message_id == message_id)
        else {
            return Err(SessionMessageError::UnknownMessage);
        };
        let snapshot = record.messages[message_index].as_ref().clone();
        if snapshot.closure_kind.is_some() {
            return Err(SessionMessageError::MessageNotOpen);
        }
        if snapshot.kind == super::model::SessionMessageKind::Todo {
            return Err(SessionMessageError::InvalidInput(
                "todo messages require complete_session_message rather than session_message_resolution"
                    .to_string(),
            ));
        }
        if snapshot.status == SessionMessageStatus::Resolved {
            if snapshot.resolution.as_deref() == Some(resolution.as_str()) {
                return Ok((snapshot, false));
            }
            return Err(SessionMessageError::IdempotencyConflict);
        }
        if snapshot.requires_ack && !current_request_acknowledged {
            return Err(SessionMessageError::InvalidInput(
                "requires_ack message must be acknowledged on the same request before wrapper resolution"
                    .to_string(),
            ));
        }

        let revision = Self::next_message_observation_revision(record)?;
        let now = now_ts();
        let message = Arc::make_mut(&mut record.messages[message_index]);
        message.status = SessionMessageStatus::Resolved;
        message.resolved_at = Some(now);
        message.resolution = Some(resolution);
        record.updated_at = now;
        record
            .message_observation_revisions
            .insert(message.message_id.clone(), revision);
        let message = message.clone();
        self.mark_session_dirty(session_id);
        Ok((message, true))
    }

    pub(super) fn complete_message(
        &mut self,
        input: CompleteSessionMessageInput,
    ) -> Result<CompleteSessionMessageOutcome, SessionMessageError> {
        if !self.prepare_active_session(&input.session_id) {
            return Err(SessionMessageError::InvalidObservationState);
        }
        self.touch(&input.session_id);
        let Some(stored) = self.sessions.get_mut(&input.session_id) else {
            return Err(SessionMessageError::UnknownSession);
        };
        let lifecycle = stored.lifecycle();
        if !lifecycle.allows_mutation() {
            return Err(SessionMessageError::SessionClosed { lifecycle });
        }
        if !is_valid_completion_id(&input.completion_id) {
            return Err(SessionMessageError::InvalidInput(
                "completion identity is invalid".to_string(),
            ));
        }
        if input
            .author_session_id
            .as_deref()
            .is_some_and(|author_session_id| !super::events::is_valid_session_id(author_session_id))
        {
            return Err(SessionMessageError::InvalidInput(
                "author session identity is invalid".to_string(),
            ));
        }
        let answer_text = validate_message_text(input.answer)?;
        let tags = validate_message_tags(input.tags)?;
        let record = stored
            .hot_mut()
            .expect("active session message mutation must stay hot");
        let Some(todo_index) = record
            .messages
            .iter()
            .position(|message| message.message_id == input.message_id)
        else {
            return Err(SessionMessageError::UnknownMessage);
        };
        if record.messages[todo_index].kind != super::model::SessionMessageKind::Todo {
            return Err(SessionMessageError::NotTodo);
        }

        let todo_snapshot = record.messages[todo_index].as_ref().clone();
        validate_assignment_fence(&input.expected_assignment_fence)?;
        let provided_assignment_fence_fingerprint =
            assignment_fence_fingerprint(&input.expected_assignment_fence);
        if todo_snapshot.status == SessionMessageStatus::Resolved {
            match (
                todo_snapshot.completion_id.as_deref(),
                todo_snapshot.resolved_by_message_id.as_deref(),
            ) {
                (None, None) => {
                    return Err(SessionMessageError::AssignmentStale {
                        current: current_assignment_state(record, &input.message_id)?,
                        fresh_assignment_fence: None,
                    });
                }
                (Some(completion_id), Some(answer_message_id)) => {
                    let Some(answer) = record.messages.iter().find(|message| {
                        message.message_id == answer_message_id
                            && message.kind == super::model::SessionMessageKind::Answer
                            && message.reply_to.as_deref() == Some(input.message_id.as_str())
                    }) else {
                        return Err(SessionMessageError::InvalidCompletionState);
                    };
                    if completion_id != input.completion_id {
                        return Err(SessionMessageError::AlreadyCompleted {
                            answer_message_id: Some(answer.message_id.clone()),
                            completion_id: Some(completion_id.to_string()),
                        });
                    }
                    if answer.message != answer_text
                        || answer.tags != tags
                        || answer.priority != input.priority
                    {
                        return Err(SessionMessageError::IdempotencyConflict);
                    }
                    match record
                        .completion_assignment_fence_fingerprints
                        .get(&input.message_id)
                    {
                        Some(Some(stored_fingerprint)) => {
                            if stored_fingerprint != &provided_assignment_fence_fingerprint
                                || answer.author_session_id != input.author_session_id
                            {
                                return Err(SessionMessageError::IdempotencyConflict);
                            }
                        }
                        Some(None) => {
                            // Historical completion with known no-fence provenance.
                            // Current callers must never replay it as if their newly
                            // supplied fence had been part of the original intent.
                            return Err(SessionMessageError::IdempotencyConflict);
                        }
                        None if record.completion_assignment_fence_tracking_complete => {
                            return Err(SessionMessageError::InvalidCompletionState);
                        }
                        None => {
                            // Historical completion predating fence metadata. Preserve
                            // it for query/restore, but live replay cannot prove a match.
                            return Err(SessionMessageError::IdempotencyConflict);
                        }
                    }
                    return Ok(CompleteSessionMessageOutcome {
                        todo: todo_snapshot,
                        answer: answer.as_ref().clone(),
                        replayed: true,
                    });
                }
                _ => return Err(SessionMessageError::InvalidCompletionState),
            }
        }
        if todo_snapshot.completion_id.is_some() || todo_snapshot.resolved_by_message_id.is_some() {
            return Err(SessionMessageError::InvalidCompletionState);
        }
        let current_assignment = open_assignment_state(record, &input.message_id)?;
        let fresh_assignment_fence =
            assignment_fence_from_state(&input.session_id, &input.message_id, &current_assignment);
        if fresh_assignment_fence != input.expected_assignment_fence {
            let current = current_assignment_state(record, &input.message_id)?;
            return Err(SessionMessageError::AssignmentStale {
                current,
                fresh_assignment_fence: Some(fresh_assignment_fence),
            });
        }

        let message_id = allocate_message_id(record, webcodex_core::compact::random_suffix::<12>)?;
        let todo_revision = record
            .message_observation_revision
            .checked_add(1)
            .ok_or(SessionMessageError::InvalidObservationState)?;
        let answer_revision = todo_revision
            .checked_add(1)
            .ok_or(SessionMessageError::InvalidObservationState)?;
        record.message_observation_revision = answer_revision;

        let now = now_ts();
        let answer = SessionMessage {
            message_id,
            session_id: input.session_id.clone(),
            created_at: now,
            kind: super::model::SessionMessageKind::Answer,
            status: SessionMessageStatus::Open,
            priority: input.priority,
            message: answer_text,
            tags,
            reply_to: Some(input.message_id.clone()),
            requires_ack: false,
            first_ack_observed_at: None,
            author_session_id: input.author_session_id,
            resolved_at: None,
            resolution: None,
            closure_kind: None,
            superseded_by_message_id: None,
            supersedes_message_id: None,
            resolved_by_message_id: None,
            completion_id: None,
        };
        {
            let todo = Arc::make_mut(&mut record.messages[todo_index]);
            todo.status = SessionMessageStatus::Resolved;
            todo.resolved_at = Some(now);
            todo.resolved_by_message_id = Some(answer.message_id.clone());
            todo.completion_id = Some(input.completion_id);
        }
        record.messages.push_back(Arc::new(answer.clone()));
        record
            .message_observation_revisions
            .insert(input.message_id.clone(), todo_revision);
        record
            .message_observation_revisions
            .insert(answer.message_id.clone(), answer_revision);
        record.completion_assignment_fence_fingerprints.insert(
            input.message_id.clone(),
            Some(provided_assignment_fence_fingerprint),
        );
        let protect_fenced_assignment_replies = true;
        while record.messages.len() > DEFAULT_MAX_MESSAGES_PER_SESSION {
            let protected_answer_id = answer.message_id.as_str();
            let protected_todo_id = input.message_id.as_str();
            let Some(remove_index) = record.messages.iter().position(|message| {
                message.message_id != protected_todo_id
                    && message.message_id != protected_answer_id
                    && (!protect_fenced_assignment_replies
                        || message.reply_to.as_deref() != Some(protected_todo_id))
            }) else {
                return Err(SessionMessageError::InvalidCompletionState);
            };
            if let Some(evicted) = record.messages.remove(remove_index) {
                Self::note_evicted_message_observation(record, evicted.as_ref());
            }
        }
        record.updated_at = now;
        let todo = record
            .messages
            .iter()
            .find(|message| message.message_id == input.message_id)
            .expect("completed todo retained")
            .as_ref()
            .clone();
        self.mark_session_dirty(&input.session_id);
        Ok(CompleteSessionMessageOutcome {
            todo,
            answer,
            replayed: false,
        })
    }

    fn next_message_observation_revision(
        record: &mut SessionRecord,
    ) -> Result<u64, SessionMessageError> {
        let revision = record
            .message_observation_revision
            .checked_add(1)
            .ok_or(SessionMessageError::InvalidObservationState)?;
        record.message_observation_revision = revision;
        Ok(revision)
    }

    fn note_evicted_message_observation(record: &mut SessionRecord, message: &SessionMessage) {
        let Some(revision) = record
            .message_observation_revisions
            .remove(&message.message_id)
        else {
            record.assignment_history_tracking_complete = false;
            return;
        };
        record.message_observation_floor = record.message_observation_floor.max(revision);

        if message.kind == super::model::SessionMessageKind::Todo {
            record.assignment_history_floors.remove(&message.message_id);
            record
                .completion_assignment_fence_fingerprints
                .remove(&message.message_id);
            return;
        }
        let Some(todo_id) = message.reply_to.as_deref() else {
            return;
        };
        if record.messages.iter().any(|candidate| {
            candidate.message_id == todo_id
                && candidate.kind == super::model::SessionMessageKind::Todo
        }) {
            record
                .assignment_history_floors
                .entry(todo_id.to_string())
                .and_modify(|floor| *floor = (*floor).max(revision))
                .or_insert(revision);
        }
    }

    // --- reads / housekeeping ---

    pub(super) fn contains_session(&self, session_id: &str) -> bool {
        self.sessions.contains_key(session_id)
    }

    pub(super) fn session_project(&self, session_id: &str) -> Option<Option<String>> {
        self.sessions
            .get(session_id)
            .map(|record| record.project().map(str::to_string))
    }

    pub(super) fn session_target_authority(
        &self,
        session_id: &str,
    ) -> Option<(Option<String>, String)> {
        self.sessions.get(session_id).map(|record| {
            (
                record.project().map(str::to_string),
                record.owner_authority_fingerprint().to_string(),
            )
        })
    }

    pub(super) fn guard_state(&self, session_id: &str) -> Option<(SessionMode, SessionGuards)> {
        self.sessions
            .get(session_id)
            .map(StoredSession::mode_guards)
    }

    pub(super) fn lifecycle_state(&self, session_id: &str) -> Option<SessionLifecycle> {
        self.sessions.get(session_id).map(StoredSession::lifecycle)
    }

    fn to_persisted_ledger(&self) -> PersistedSessionLedger {
        let mut sessions: Vec<PersistedSessionSnapshot> = self
            .lru
            .iter()
            .filter_map(|session_id| self.sessions.get(session_id))
            .map(|record| match record {
                StoredSession::Hot(record) => PersistedSessionSnapshot::Hot(
                    PersistedSessionRecord::from_record(record, self.max_events_per_session),
                ),
                StoredSession::Cold(record) => {
                    PersistedSessionSnapshot::Cold(Arc::clone(&record.raw))
                }
            })
            .collect();
        sessions.extend(self.tombstones.values().cloned().map(|tombstone| {
            PersistedSessionSnapshot::Tombstone(PersistedRetentionTombstoneRow {
                retention_tombstone: tombstone,
            })
        }));
        PersistedSessionLedger {
            version: SESSION_LEDGER_VERSION,
            sessions,
        }
    }

    pub(super) fn touch(&mut self, session_id: &str) {
        if let Some(session) = self.sessions.get(session_id) {
            self.lru
                .touch(session_id, session.lifecycle() == SessionLifecycle::Closed);
        }
    }

    fn session_id_occupied(&self, session_id: &str) -> bool {
        self.sessions.contains_key(session_id)
            || self.tombstone_ids.contains_key(session_id)
            || self
                .database_persistence
                .pending_retirements
                .iter()
                .any(|retirement| retirement.session_id == session_id)
    }

    fn owned_retention_incarnation(
        &self,
        session_id: &str,
        owner_authority_fingerprint: &str,
    ) -> Option<String> {
        let ordinal = self.tombstone_ids.get(session_id).copied()?;
        let tombstone = self.tombstones.get(&ordinal)?;
        if tombstone.session_id != session_id
            || tombstone.owner_authority_fingerprint != owner_authority_fingerprint
        {
            return None;
        }
        Some(tombstone.incarnation_fingerprint.clone())
    }

    /// `expiry_ordinal` is relative order. A restored `u64::MAX` must not
    /// permanently stop later tombstones, and it must not wrap.
    fn needs_ordinal_compaction(&self) -> bool {
        if self.expiry_ordinal_exhausted {
            return true;
        }
        self.tombstones
            .keys()
            .next_back()
            .is_some_and(|max| self.next_expiry_ordinal <= *max)
    }

    fn compact_expiry_ordinals(&mut self) {
        let ordered: Vec<SessionRetentionTombstone> = self.tombstones.values().cloned().collect();
        let Some(count) = u64::try_from(ordered.len())
            .ok()
            .filter(|count| *count < u64::MAX)
        else {
            self.next_expiry_ordinal = u64::MAX;
            self.expiry_ordinal_exhausted = true;
            return;
        };
        self.tombstones.clear();
        self.tombstone_ids.clear();
        for (index, mut tombstone) in ordered.into_iter().enumerate() {
            let ordinal = u64::try_from(index).expect("compacted tombstone index fits in u64") + 1;
            tombstone.expiry_ordinal = ordinal;
            self.tombstone_ids
                .insert(tombstone.session_id.clone(), ordinal);
            self.tombstones.insert(ordinal, tombstone);
        }
        self.next_expiry_ordinal = count + 1;
        self.expiry_ordinal_exhausted = false;
        self.queue_all_tombstones_dirty();
    }

    fn allocate_expiry_ordinal(&mut self) -> Option<u64> {
        if self.needs_ordinal_compaction() {
            self.compact_expiry_ordinals();
        }
        if self.expiry_ordinal_exhausted {
            return None;
        }
        let ordinal = self.next_expiry_ordinal;
        if self
            .tombstones
            .keys()
            .next_back()
            .is_some_and(|max| ordinal <= *max)
        {
            self.expiry_ordinal_exhausted = true;
            return None;
        }
        match ordinal.checked_add(1) {
            Some(next) => self.next_expiry_ordinal = next,
            None => self.expiry_ordinal_exhausted = true,
        }
        Some(ordinal)
    }

    fn insert_tombstone(&mut self, tombstone: SessionRetentionTombstone) {
        if self.sessions.contains_key(&tombstone.session_id) {
            return;
        }
        if let Some(existing) = self.tombstone_ids.get(&tombstone.session_id).copied() {
            if tombstone.expiry_ordinal <= existing {
                return;
            }
            self.tombstones.remove(&existing);
        }
        if self.tombstones.contains_key(&tombstone.expiry_ordinal) {
            return;
        }
        self.tombstone_ids
            .insert(tombstone.session_id.clone(), tombstone.expiry_ordinal);
        self.tombstones.insert(tombstone.expiry_ordinal, tombstone.clone());
        self.queue_retention_tombstone_upsert(tombstone);
        self.enforce_tombstone_bound();
    }

    /// Re-queue historical/tombstone retention after a partial durable ack opened queue headroom.
    pub(crate) fn reenforce_sqlite_retention_bounds_after_durable_ack(&mut self) {
        if !self.database_persistence.is_seeded() {
            return;
        }
        self.enforce_historical_retention_bound();
        self.enforce_tombstone_bound();
    }

    fn enforce_tombstone_bound(&mut self) {
        while self.tombstones.len() > self.historical_session_retention_limit {
            let Some((ordinal, tombstone)) = self.tombstones.pop_first() else {
                break;
            };
            if !self.queue_tombstone_bound_delete(&tombstone) {
                self.tombstones.insert(ordinal, tombstone.clone());
                self.note_retention_queue_admission_failure("tombstone bound delete");
                break;
            }
            if self.tombstone_ids.get(&tombstone.session_id) == Some(&ordinal) {
                self.tombstone_ids.remove(&tombstone.session_id);
            }
        }
    }

    fn tombstone_from_closed_session(
        &mut self,
        session_id: &str,
    ) -> Option<SessionRetentionTombstone> {
        let (id, created_at, project, owner) = {
            let stored = self.sessions.get(session_id)?;
            incarnation_inputs(stored, self.max_events_per_session)?
        };
        if id != session_id {
            return None;
        }
        let expiry_ordinal = self.allocate_expiry_ordinal()?;
        Some(SessionRetentionTombstone {
            incarnation_fingerprint: workflow_session_incarnation_fingerprint(
                &id,
                created_at,
                project.as_deref(),
                &owner,
            ),
            session_id: id,
            owner_authority_fingerprint: owner,
            expiry_ordinal,
        })
    }

    fn enforce_historical_retention_bound(&mut self) {
        while self.lru.closed_count() > self.historical_session_retention_limit {
            let session_id = self
                .lru
                .oldest_closed()
                .expect("closed count has an oldest entry")
                .to_owned();
            // Lifecycle remains authoritative; a recency index cannot authorize
            // deleting an Active identity even if an internal invariant regresses.
            if !self
                .sessions
                .get(&session_id)
                .is_some_and(|session| session.lifecycle() == SessionLifecycle::Closed)
            {
                self.lru.remove(&session_id);
                self.touch(&session_id);
                continue;
            }
            if self
                .database_persistence
                .retention_queues_prevent_new_retirement_enqueue()
            {
                self.note_retention_queue_admission_failure("historical retention retirement");
                tracing::warn!(
                    "historical retention eviction blocked: pending retirement queue saturated"
                );
                break;
            }
            if self
                .database_persistence
                .retention_queues_prevent_new_tombstone_delete_enqueue()
            {
                self.note_retention_queue_admission_failure(
                    "historical retention tombstone delete headroom",
                );
                tracing::warn!(
                    "historical retention eviction blocked: pending tombstone delete queue saturated"
                );
                break;
            }
            let Some(tombstone) = self.tombstone_from_closed_session(&session_id) else {
                tracing::warn!(
                    session_id = %session_id,
                    "historical retention eviction blocked: retention tombstone unavailable"
                );
                break;
            };
            let expected_revision = self
                .database_persistence
                .session_dirty
                .get(&session_id)
                .and_then(|state| state.committed_revision);
            let stored = match self.sessions.get(&session_id) {
                Some(stored) => stored,
                None => {
                    self.lru.remove(&session_id);
                    continue;
                }
            };
            let Some(retirement) = retirement_from_stored_session(
                stored,
                self.max_events_per_session,
                tombstone.clone(),
                expected_revision,
            ) else {
                tracing::warn!(
                    session_id = %session_id,
                    "historical retention eviction blocked: retirement evidence unavailable"
                );
                break;
            };
            let stored = self.sessions.remove(&session_id);
            match self.database_persistence.session_dirty.get_mut(&session_id) {
                Some(state) if state.in_flight_revision.is_some() => {
                    state.dirty_revision = None;
                    state.pending_event_upserts.clear();
                }
                Some(_) => {
                    self.database_persistence.session_dirty.remove(&session_id);
                }
                None => {}
            }
            self.lru.remove(&session_id);
            self.capacity_evictions = self.capacity_evictions.saturating_add(1);
            debug_assert!(stored.is_some(), "retirement proven before eviction remove");
            if !self.queue_session_retirement(retirement) {
                self.sessions.insert(session_id.clone(), stored.expect("stored"));
                self.lru.touch(&session_id, true);
                self.note_retention_queue_admission_failure("historical retention retirement enqueue");
                break;
            }
            self.insert_tombstone(tombstone);
        }
    }

    pub(super) fn summary(&self, session_id: &str, limit: Option<usize>) -> Option<SessionSummary> {
        let record = self.sessions.get(session_id)?.hot()?;
        Some(summarize_record(record, limit, None))
    }
}

/// Fingerprint inputs for a Closed row that can still prove its incarnation.
/// A corrupt Cold payload that cannot be checked against its metadata yields
/// no tombstone.
fn incarnation_inputs(
    stored: &StoredSession,
    max_events: usize,
) -> Option<(String, i64, Option<String>, String)> {
    match stored {
        StoredSession::Hot(record) => {
            if record.lifecycle != SessionLifecycle::Closed {
                return None;
            }
            validated_incarnation_inputs(
                &record.session_id,
                record.created_at,
                record.project.clone(),
                &record.owner_authority_fingerprint,
            )
        }
        StoredSession::Cold(record) => {
            if record.lifecycle != SessionLifecycle::Closed {
                return None;
            }
            let materialized = materialize_cold_session(record, max_events)?;
            if materialized.session_id != record.session_id
                || materialized.owner_authority_fingerprint != record.owner_authority_fingerprint
                || materialized.lifecycle != SessionLifecycle::Closed
            {
                return None;
            }
            validated_incarnation_inputs(
                &materialized.session_id,
                materialized.created_at,
                materialized.project,
                &materialized.owner_authority_fingerprint,
            )
        }
    }
}

fn validated_incarnation_inputs(
    session_id: &str,
    created_at: i64,
    project: Option<String>,
    owner: &str,
) -> Option<(String, i64, Option<String>, String)> {
    if !is_valid_session_id(session_id) || !is_lower_hex_sha256(owner) {
        return None;
    }
    Some((
        session_id.to_string(),
        created_at,
        project,
        owner.to_string(),
    ))
}

// No retained identity or historical retained link may silently retarget.
pub(super) fn allocate_message_id(
    record: &SessionRecord,
    mut suffix: impl FnMut() -> String,
) -> Result<String, SessionMessageError> {
    for _ in 0..16 {
        let id = format!("{MESSAGE_ID_PREFIX}{}", suffix());
        if !record.messages.iter().any(|m| {
            m.message_id == id
                || m.reply_to.as_deref() == Some(&id)
                || m.resolved_by_message_id.as_deref() == Some(&id)
                || m.superseded_by_message_id.as_deref() == Some(&id)
                || m.supersedes_message_id.as_deref() == Some(&id)
        }) {
            return Ok(id);
        }
    }
    Err(SessionMessageError::InvalidObservationState)
}
