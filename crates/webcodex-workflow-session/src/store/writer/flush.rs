//! Durable flush bodies for the background writer and synchronous fallback.
//!
//! Stage J.7: live replan bounded chunks — each unit is planned from the latest inner
//! state, applied, and acknowledged before the next unit is planned.

use std::io;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::super::{
    abandon_in_flight_writes, acknowledge_batch, bound_summary_string,
    has_bounded_flush_unit, has_deferred_retention_work, mark_planned_writes_in_flight,
    unrecoverable_retention_stall_summary, AcknowledgeBatchOutcome, BatchPlan, BatchPlanError,
    SessionStoreInner, SQLITE_BATCH_MAX_UNITS,
};
use crate::persistence::database::{
    ApplyBatchError, SessionDatabase, summarize_rusqlite_error,
};
use crate::persistence::write_ledger_atomic_measured;

/// Bounded SQLite units per writer attempt before releasing `persistence_write_mutex` (J.7).
pub(crate) const MAX_FLUSH_CHUNKS_PER_ATTEMPT: u32 = 4;

pub(crate) struct FlushMetrics {
    pub lock_wait: Duration,
    pub lock_hold: Duration,
    pub rows: usize,
    pub chunks_committed: u32,
}

/// How the writer should treat one flush attempt (Stage G + J.7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PersistFlushDisposition {
    /// Durable apply + store acknowledgement succeeded (allocation health may still warn).
    Success,
    /// Explicit transient I/O or SQLite BUSY/LOCKED — bounded backoff retry.
    TransientRetry,
    /// Revision allocation health only (`RevisionExhausted` / per-session allocation).
    /// Durable SQLite/JSON permanent failures use `WriterFatal`, not this variant.
    PermanentHealth,
    /// SQLite transaction committed; Store mutex poisoned before ack — no abandon/replay.
    CommittedButUnacked,
    /// Stop the writer thread; wake all flush/barrier waiters; no in-process replay.
    WriterFatal,
}

#[derive(Debug, Clone)]
pub(crate) struct PersistFlushOutcome {
    pub disposition: PersistFlushDisposition,
    /// Canonical error summary for `last_persist_error` when not `Success`.
    pub error_summary: Option<String>,
    pub allocation_error: Option<String>,
    /// JSON ledger bytes on success; always `None` for SQLite flush.
    pub written_bytes: Option<u64>,
    /// Additional bounded units remain after this attempt (J.7).
    pub more_pending: bool,
    /// At least one chunk committed but generation flush is incomplete (J.7).
    pub partial_chunk_success: bool,
}

impl PersistFlushOutcome {
    pub(crate) fn success(allocation_error: Option<String>, more_pending: bool) -> Self {
        Self {
            disposition: PersistFlushDisposition::Success,
            error_summary: None,
            allocation_error,
            written_bytes: None,
            more_pending,
            partial_chunk_success: false,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SqliteFlushAttemptBudget {
    pub max_chunks: u32,
}

impl SqliteFlushAttemptBudget {
    pub(crate) fn ordinary_attempt() -> Self {
        Self {
            max_chunks: MAX_FLUSH_CHUNKS_PER_ATTEMPT,
        }
    }

    pub(crate) fn shutdown_drain() -> Self {
        Self {
            max_chunks: u32::MAX,
        }
    }
}

pub(crate) fn durable_write_uses_sqlite(inner: &SessionStoreInner) -> bool {
    inner
        .persistence
        .as_ref()
        .is_some_and(|persistence| persistence.database.is_some())
}

pub(crate) fn flush_json_ledger(
    store_inner: &Arc<Mutex<SessionStoreInner>>,
) -> (FlushMetrics, PersistFlushOutcome) {
    let waiting = Instant::now();
    let plan_capture = {
        let inner = match store_inner.lock() {
            Ok(inner) => inner,
            Err(_) => {
                return (
                    FlushMetrics {
                        lock_wait: Duration::ZERO,
                        lock_hold: Duration::ZERO,
                        rows: 0,
                        chunks_committed: 0,
                    },
                    fatal_store_poison_outcome("json snapshot"),
                );
            }
        };
        let acquired = Instant::now();
        let snapshot = inner.persistence.as_ref().and_then(|persistence| {
            if durable_write_uses_sqlite(&inner) {
                None
            } else {
                Some((
                    persistence.path.clone(),
                    inner.to_persisted_ledger(),
                    inner.sessions.len(),
                ))
            }
        });
        match snapshot {
            Some(values) => Ok((acquired, values)),
            None => Err(acquired),
        }
    };
    let (acquired, path, ledger, rows) = match plan_capture {
        Ok((acquired, (path, ledger, rows))) => (acquired, path, ledger, rows),
        Err(acquired) => {
            return (
                FlushMetrics {
                    lock_wait: acquired.duration_since(waiting),
                    lock_hold: acquired.elapsed(),
                    rows: 0,
                    chunks_committed: 0,
                },
                PersistFlushOutcome {
                    disposition: PersistFlushDisposition::Success,
                    error_summary: None,
                    allocation_error: None,
                    written_bytes: None,
                    more_pending: false,
                    partial_chunk_success: false,
                },
            );
        }
    };
    let lock_wait = acquired.duration_since(waiting);
    let lock_hold = acquired.elapsed();
    let metrics = FlushMetrics {
        lock_wait,
        lock_hold,
        rows,
        chunks_committed: 0,
    };
    match write_ledger_atomic_measured(&path, &ledger) {
        Ok(bytes) => (
            metrics,
            PersistFlushOutcome {
                disposition: PersistFlushDisposition::Success,
                error_summary: None,
                allocation_error: None,
                written_bytes: Some(bytes),
                more_pending: false,
                partial_chunk_success: false,
            },
        ),
        Err(err) => (metrics, ledger_io_error_outcome(&path, err)),
    }
}

enum SqliteChunkCapture {
    NoWork {
        lock_wait: Duration,
        lock_hold: Duration,
        allocation_error: Option<String>,
        more_pending: bool,
    },
    Plan {
        lock_wait: Duration,
        lock_hold: Duration,
        rows: usize,
        plan: BatchPlan,
    },
}

fn capture_sqlite_bounded_chunk(
    store_inner: &Arc<Mutex<SessionStoreInner>>,
) -> Result<SqliteChunkCapture, PersistFlushOutcome> {
    let waiting = Instant::now();
    let mut inner = match store_inner.lock() {
        Ok(inner) => inner,
        Err(_) => return Err(fatal_store_poison_outcome("sqlite bounded plan")),
    };
    if !durable_write_uses_sqlite(&inner) {
        return Ok(SqliteChunkCapture::NoWork {
            lock_wait: waiting.elapsed(),
            lock_hold: Duration::ZERO,
            allocation_error: None,
            more_pending: false,
        });
    }
    let acquired = Instant::now();
    match BatchPlan::plan_bounded_flush_unit_from_inner(&inner, SQLITE_BATCH_MAX_UNITS) {
        Ok(None) => {
            if let Some(summary) = unrecoverable_retention_stall_summary(&inner) {
                return Err(PersistFlushOutcome {
                    disposition: PersistFlushDisposition::WriterFatal,
                    error_summary: Some(summary),
                    allocation_error: None,
                    written_bytes: None,
                    more_pending: false,
                    partial_chunk_success: false,
                });
            }
            let allocation_error = revision_allocation_persist_error_from_inner(&inner);
            let more_pending = sqlite_flush_has_continuable_work(&inner);
            Ok(SqliteChunkCapture::NoWork {
                lock_wait: acquired.duration_since(waiting),
                lock_hold: acquired.elapsed(),
                allocation_error,
                more_pending,
            })
        }
        Ok(Some(plan)) => {
            let rows = plan.session_writes.len();
            if !mark_planned_writes_in_flight(&mut inner, &plan) {
                if let Some(summary) = unrecoverable_retention_stall_summary(&inner) {
                    return Err(PersistFlushOutcome {
                        disposition: PersistFlushDisposition::WriterFatal,
                        error_summary: Some(summary),
                        allocation_error: None,
                        written_bytes: None,
                        more_pending: false,
                        partial_chunk_success: false,
                    });
                }
                let allocation_error = revision_allocation_persist_error_from_inner(&inner);
                let more_pending = sqlite_flush_has_continuable_work(&inner);
                if inner.database_persistence.has_pending_work() && !more_pending {
                    return Err(PersistFlushOutcome {
                        disposition: PersistFlushDisposition::WriterFatal,
                        error_summary: Some(bound_summary_string(
                            "persist_failed: sqlite_flush_mark_in_flight_invariant",
                        )),
                        allocation_error,
                        written_bytes: None,
                        more_pending: false,
                        partial_chunk_success: false,
                    });
                }
                return Ok(SqliteChunkCapture::NoWork {
                    lock_wait: acquired.duration_since(waiting),
                    lock_hold: acquired.elapsed(),
                    allocation_error,
                    more_pending,
                });
            }
            Ok(SqliteChunkCapture::Plan {
                lock_wait: acquired.duration_since(waiting),
                lock_hold: acquired.elapsed(),
                rows,
                plan,
            })
        }
        Err(err) => Err(batch_plan_flush_outcome(&err)),
    }
}

fn sqlite_flush_has_continuable_work(inner: &SessionStoreInner) -> bool {
    if !durable_write_uses_sqlite(inner) {
        return false;
    }
    has_bounded_flush_unit(inner) || has_deferred_retention_work(inner)
}

fn apply_ack_sqlite_chunk(
    store_inner: &Arc<Mutex<SessionStoreInner>>,
    database: &SessionDatabase,
    plan: BatchPlan,
) -> PersistFlushOutcome {
    let batch = plan.clone().into_batch();
    let apply_result = database.apply_batch_for_flush(&batch);
    match apply_result {
        Ok(_) => match store_inner.lock() {
            Ok(mut inner) => match acknowledge_batch(&mut inner, &plan) {
                AcknowledgeBatchOutcome::Complete => {
                    if let Some(summary) = unrecoverable_retention_stall_summary(&inner) {
                        return PersistFlushOutcome {
                            disposition: PersistFlushDisposition::WriterFatal,
                            error_summary: Some(summary),
                            allocation_error: None,
                            written_bytes: None,
                            more_pending: false,
                            partial_chunk_success: true,
                        };
                    }
                    let allocation_error = revision_allocation_persist_error_from_inner(&inner);
                    let more_pending = sqlite_flush_has_continuable_work(&inner);
                    PersistFlushOutcome {
                        disposition: PersistFlushDisposition::Success,
                        error_summary: None,
                        allocation_error,
                        written_bytes: None,
                        more_pending,
                        partial_chunk_success: more_pending,
                    }
                }
                AcknowledgeBatchOutcome::RetentionQueueInvariantBroken => PersistFlushOutcome {
                    disposition: PersistFlushDisposition::WriterFatal,
                    error_summary: Some(bound_summary_string(
                        "persist_failed: sqlite_retention_queue_invariant_after_commit",
                    )),
                    allocation_error: None,
                    written_bytes: None,
                    more_pending: true,
                    partial_chunk_success: true,
                },
            },
            Err(_) => PersistFlushOutcome {
                disposition: PersistFlushDisposition::CommittedButUnacked,
                error_summary: Some(bound_summary_string(
                    "persist_failed: sqlite_committed_but_unacked: session store mutex poisoned after commit; restart from SQLite",
                )),
                allocation_error: None,
                written_bytes: None,
                more_pending: true,
                partial_chunk_success: true,
            },
        },
        Err(err) => {
            let (mut disposition, mut summary) = apply_batch_error_outcome(&err);
            if !matches!(
                disposition,
                PersistFlushDisposition::WriterFatal
                    | PersistFlushDisposition::CommittedButUnacked
            ) {
                match store_inner.lock() {
                    Ok(mut inner) => abandon_in_flight_writes(&mut inner, &plan),
                    Err(_) => {
                        disposition = PersistFlushDisposition::WriterFatal;
                        summary = bound_summary_string(
                            "persist_failed: session store mutex poisoned; cannot abandon in-flight sqlite writes",
                        );
                    }
                }
            }
            let (allocation_error, more_pending) = match store_inner.lock() {
                Ok(inner) => (
                    revision_allocation_persist_error_from_inner(&inner),
                    sqlite_flush_has_continuable_work(&inner),
                ),
                Err(_) => {
                    disposition = PersistFlushDisposition::WriterFatal;
                    summary = bound_summary_string(
                        "persist_failed: session store mutex poisoned after sqlite batch failure",
                    );
                    (None, false)
                }
            };
            PersistFlushOutcome {
                disposition,
                error_summary: Some(summary),
                allocation_error,
                written_bytes: None,
                more_pending,
                partial_chunk_success: false,
            }
        }
    }
}

pub(crate) fn flush_sqlite_batch_with_budget(
    store_inner: &Arc<Mutex<SessionStoreInner>>,
    database: &SessionDatabase,
    budget: SqliteFlushAttemptBudget,
) -> (FlushMetrics, PersistFlushOutcome) {
    let mut total_lock_wait = Duration::ZERO;
    let mut total_lock_hold = Duration::ZERO;
    let mut total_rows = 0usize;
    let mut chunks_committed = 0u32;
    let mut last_outcome = PersistFlushOutcome::success(None, false);
    let mut any_chunk_committed = false;

    for _ in 0..budget.max_chunks {
        match capture_sqlite_bounded_chunk(store_inner) {
            Ok(SqliteChunkCapture::NoWork {
                lock_wait,
                lock_hold,
                allocation_error,
                more_pending,
            }) => {
                total_lock_wait += lock_wait;
                total_lock_hold += lock_hold;
                last_outcome = PersistFlushOutcome {
                    disposition: PersistFlushDisposition::Success,
                    error_summary: None,
                    allocation_error,
                    written_bytes: None,
                    more_pending,
                    partial_chunk_success: any_chunk_committed && more_pending,
                };
                break;
            }
            Ok(SqliteChunkCapture::Plan {
                lock_wait,
                lock_hold,
                rows,
                plan,
            }) => {
                total_lock_wait += lock_wait;
                total_lock_hold += lock_hold;
                total_rows += rows;
                let outcome = apply_ack_sqlite_chunk(store_inner, database, plan);
                if outcome.disposition == PersistFlushDisposition::Success {
                    chunks_committed = chunks_committed.saturating_add(1);
                    any_chunk_committed = true;
                }
                last_outcome = outcome;
                if last_outcome.disposition != PersistFlushDisposition::Success {
                    break;
                }
                if !last_outcome.more_pending {
                    break;
                }
            }
            Err(outcome) => {
                last_outcome = outcome;
                break;
            }
        }
    }

    if any_chunk_committed && last_outcome.disposition == PersistFlushDisposition::Success {
        last_outcome.partial_chunk_success = last_outcome.more_pending;
    }

    (
        FlushMetrics {
            lock_wait: total_lock_wait,
            lock_hold: total_lock_hold,
            rows: total_rows,
            chunks_committed,
        },
        last_outcome,
    )
}

pub(crate) fn flush_sqlite_batch(
    store_inner: &Arc<Mutex<SessionStoreInner>>,
    database: &SessionDatabase,
) -> (FlushMetrics, PersistFlushOutcome) {
    flush_sqlite_batch_with_budget(store_inner, database, SqliteFlushAttemptBudget::ordinary_attempt())
}

pub(crate) fn flush_sqlite_batch_shutdown_drain(
    store_inner: &Arc<Mutex<SessionStoreInner>>,
    database: &SessionDatabase,
) -> (FlushMetrics, PersistFlushOutcome) {
    flush_sqlite_batch_with_budget(store_inner, database, SqliteFlushAttemptBudget::shutdown_drain())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordPersistOutcomeError {
    StoreMutexPoisoned,
}

pub(crate) fn record_persist_outcome(
    store_inner: &Arc<Mutex<SessionStoreInner>>,
    outcome: &PersistFlushOutcome,
) -> Result<(), RecordPersistOutcomeError> {
    let mut inner = match store_inner.lock() {
        Ok(inner) => inner,
        Err(_) => return Err(RecordPersistOutcomeError::StoreMutexPoisoned),
    };
    let Some(persistence) = inner.persistence.as_mut() else {
        return Ok(());
    };
    match outcome.disposition {
        PersistFlushDisposition::Success => {
            if let Some(error) = outcome.allocation_error.as_deref() {
                tracing::warn!("session sqlite persistence allocation failures remain: {}", error);
                persistence.last_persist_error = Some(error.to_string());
            } else if outcome.more_pending && outcome.partial_chunk_success {
                persistence.last_persist_error = None;
            } else {
                persistence.last_persist_error = None;
            }
        }
        PersistFlushDisposition::TransientRetry
        | PersistFlushDisposition::PermanentHealth => {
            if let Some(error) = outcome.error_summary.as_ref() {
                tracing::warn!("session persistence failed: {}", error);
                persistence.last_persist_error = Some(error.clone());
            }
            if let Some(error) = outcome.allocation_error.as_deref() {
                tracing::warn!("session sqlite persistence allocation failures remain: {}", error);
                persistence.last_persist_error = Some(error.to_string());
            }
        }
        PersistFlushDisposition::CommittedButUnacked | PersistFlushDisposition::WriterFatal => {
            if let Some(error) = outcome.error_summary.as_ref() {
                tracing::error!("session persistence writer fatal: {}", error);
                persistence.last_persist_error = Some(error.clone());
            }
        }
    }
    Ok(())
}

fn fatal_store_poison_outcome(context: &str) -> PersistFlushOutcome {
    PersistFlushOutcome {
        disposition: PersistFlushDisposition::WriterFatal,
        error_summary: Some(bound_summary_string(&format!(
            "persist_failed: session store mutex poisoned ({context})"
        ))),
        allocation_error: None,
        written_bytes: None,
        more_pending: false,
        partial_chunk_success: false,
    }
}

fn batch_plan_flush_outcome(error: &BatchPlanError) -> PersistFlushOutcome {
    let summary = batch_plan_error_message(error);
    let disposition = match error {
        BatchPlanError::NotSeeded | BatchPlanError::SessionRecordMaterializationFailed { .. } => {
            PersistFlushDisposition::WriterFatal
        }
        BatchPlanError::RevisionExhausted => PersistFlushDisposition::PermanentHealth,
    };
    PersistFlushOutcome {
        disposition,
        error_summary: Some(summary),
        allocation_error: None,
        written_bytes: None,
        more_pending: false,
        partial_chunk_success: false,
    }
}

pub(crate) fn io_error_is_explicit_transient(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
    )
}

pub(crate) fn disposition_for_io_error(err: &io::Error) -> PersistFlushDisposition {
    if io_error_is_explicit_transient(err) {
        PersistFlushDisposition::TransientRetry
    } else {
        PersistFlushDisposition::WriterFatal
    }
}

pub(crate) fn ledger_io_error_outcome(path: &std::path::Path, err: io::Error) -> PersistFlushOutcome {
    let disposition = disposition_for_io_error(&err);
    PersistFlushOutcome {
        disposition,
        error_summary: Some(bound_summary_string(&format!(
            "persist_failed: {}: {err}",
            path.display()
        ))),
        allocation_error: None,
        written_bytes: None,
        more_pending: false,
        partial_chunk_success: false,
    }
}

fn io_error_flush_disposition(io_err: &io::Error) -> PersistFlushDisposition {
    disposition_for_io_error(io_err)
}

fn summarize_durable_step_sqlite_error(io_err: &io::Error) -> String {
    io_err
        .get_ref()
        .and_then(|source| source.downcast_ref::<rusqlite::Error>())
        .map(summarize_rusqlite_error)
        .unwrap_or_else(|| io_err.to_string())
}

fn apply_batch_error_outcome(err: &ApplyBatchError) -> (PersistFlushDisposition, String) {
    match err {
        ApplyBatchError::ConnectionMutexPoisoned => (
            PersistFlushDisposition::WriterFatal,
            bound_summary_string(
                "persist_failed: workflow session SQLite connection mutex poisoned",
            ),
        ),
        ApplyBatchError::Validation(io_err) => {
            let disposition = io_error_flush_disposition(io_err);
            (
                disposition,
                bound_summary_string(&format!("persist_failed: sqlite validation: {io_err}")),
            )
        }
        ApplyBatchError::Transient(sqlite_err) => (
            PersistFlushDisposition::TransientRetry,
            bound_summary_string(&format!(
                "persist_failed: sqlite transient: {}",
                summarize_rusqlite_error(sqlite_err)
            )),
        ),
        ApplyBatchError::Permanent(sqlite_err) => (
            PersistFlushDisposition::WriterFatal,
            bound_summary_string(&format!(
                "persist_failed: sqlite: {}",
                summarize_rusqlite_error(sqlite_err)
            )),
        ),
        ApplyBatchError::TransientStep(io_err) => (
            PersistFlushDisposition::TransientRetry,
            bound_summary_string(&format!(
                "persist_failed: sqlite transient: {}",
                summarize_durable_step_sqlite_error(io_err)
            )),
        ),
        ApplyBatchError::PermanentStep(io_err) => {
            let disposition = io_error_flush_disposition(io_err);
            (
                disposition,
                bound_summary_string(&format!("persist_failed: sqlite: {io_err}")),
            )
        }
    }
}

fn revision_allocation_persist_error_from_inner(inner: &SessionStoreInner) -> Option<String> {
    if !inner.database_persistence.has_revision_allocation_failed() {
        return None;
    }
    let revision_exhausted = inner.database_persistence.is_revision_exhausted();
    let sessions: Vec<_> = inner
        .database_persistence
        .revision_allocation_failed_session_ids()
        .map(str::to_string)
        .collect();
    let summary = if revision_exhausted {
        format!(
            "sqlite_revision_exhausted: sessions={}",
            sessions.join(",")
        )
    } else {
        format!(
            "sqlite_revision_allocation_failed: sessions={}",
            sessions.join(",")
        )
    };
    Some(bound_summary_string(&summary))
}

fn batch_plan_error_message(error: &BatchPlanError) -> String {
    let summary = match error {
        BatchPlanError::NotSeeded => "sqlite_batch_not_seeded".to_string(),
        BatchPlanError::RevisionExhausted => "sqlite_revision_exhausted".to_string(),
        BatchPlanError::SessionRecordMaterializationFailed { session_id } => {
            format!("sqlite_session_materialization_failed: session={session_id}")
        }
    };
    bound_summary_string(&summary)
}
