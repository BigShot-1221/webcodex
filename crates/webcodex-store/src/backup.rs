//! Online SQLite backup for the shared `webcodex.db` store file (Store + Workflow Session).
//!
//! # Caller / worker lifecycle
//!
//! Production callers use [`crate::BackupMaintenanceOwner::start_online_backup`] as the entry point;
//! it delegates to the crate-private [`Database::start_online_backup`] on an [`Arc<Database>`] with a
//! matching [`Arc<ServerInstanceGuard>`](crate::server_instance::ServerInstanceGuard). The owner
//! retains the [`BackupTask`] join handle and returns only [`BackupControl`]. That spawns one **named**
//! blocking worker thread (`webcodex-online-backup`); copy, validation, and publication run entirely
//! on the worker.
//!
//! The worker **moves** the guard `Arc` into its thread and holds it until SQLite handles are
//! closed, partial cleanup finishes, and panic handling completes — even if the caller drops its
//! own `Arc` or [`BackupTask::drop`] only sets cancel without joining. That pins the cross-process
//! instance lock for the backup lifetime without a global registry and without blocking store
//! writers. Future [`OfflineRestoreOwner::acquire`](crate::restore::OfflineRestoreOwner::acquire)
//! and an in-flight backup worker remain mutually exclusive via the same instance lock file.
//!
//! - [`BackupTask::snapshot`] / [`BackupControl::snapshot`] read coarse progress under a mutex.
//!   Terminal phases (`Completed`, `Failed`, `Cancelled`) appear **only after** the worker thread
//!   finishes and writes status; they do not imply the thread or SQLite handles are gone.
//! - [`BackupTask::cancel`] / [`BackupControl::cancel`] set the cooperative cancel flag and return
//!   `false` once [`BackupPhase::Publishing`] or any terminal phase is observed under the status
//!   mutex. Cancel does **not** set [`BackupPhase::Cancelled`] early; the worker records terminal
//!   failure/cancel when it actually stops.
//! - [`BackupTask::join`] (or [`BackupTask::cancel_and_join`]) is the only way to release the
//!   worker thread and join handle. [`BackupTask::is_finished`] mirrors [`JoinHandle::is_finished`]
//!   only; it is not resource release.
//! - Dropping an unjoined [`BackupTask`] sets the cancel flag only — it does **not** join. Callers
//!   that need a bounded shutdown must [`BackupTask::join`] (or `cancel_and_join`) before dropping
//!   the last handle. Rust `join` has no built-in timeout; SQLite steps may block without a strict
//!   wall-clock bound.
//! - [`Database::start_online_backup`] and [`BackupTask`] are `pub(crate)`; [`BackupControl`] (and
//!   status/options types) are the supported observation surface returned to production callers.
//!   Runtime now wires [`crate::BackupMaintenanceOwner`] and performs bounded shutdown drain on exit,
//!   but there is still **no production backup trigger, API, or scheduler**; backup creation is not
//!   scheduled or automatically invoked.
//!
//! The worker never acquires the canonical writer mutex, History reader, Session writer, or K.2a
//! diagnostic connection. It opens a **dedicated** source SQLite handle, streams via
//! [`rusqlite::backup::Backup`] into a unique same-directory `.partial` file, validates the
//! closed candidate read-only, then publishes with a same-directory `hard_link` (never `rename`).
//!
//! Backup directory serialization uses an `fs2` exclusive lock on a stable lock file under
//! `data_dir/backups`; that lock is independent of database writer / session locks.
//!
//! # Limits
//!
//! - One positive-page `step` per loop iteration; no `step(-1)` / unbounded `run_to_completion`.
//! - `BUSY` / `LOCKED` [`StepResult`] values use bounded exponential backoff capped by
//!   [`BackupOptions::max_busy_backoff`], plus the overall [`BackupOptions::timeout`].
//! - Source writes may restart backup progress; `remaining` is not assumed monotonic.
//! - Status error strings are bounded for observability surfaces.

use super::Database;
use crate::server_instance::ServerInstanceGuard;
use anyhow::{anyhow, Context};
use chrono::Utc;
use fs2::FileExt;
use rusqlite::backup::{Backup, StepResult};
use rusqlite::{Connection, ErrorCode, OpenFlags};
use serde::Serialize;
use std::ffi::OsString;
use std::fs::{self, DirBuilder, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use uuid::Uuid;

const BACKUP_THREAD_NAME: &str = "webcodex-online-backup";
const BACKUP_DIR_NAME: &str = "backups";
const BACKUP_DIR_LOCK_NAME: &str = ".online-backup.lock";
const PARTIAL_NAME_PREFIX: &str = "webcodex-";
const PARTIAL_NAME_SUFFIX: &str = ".db.partial";
const MAX_STATUS_ERROR_CHARS: usize = 240;
// Includes unrelated directory entries: an oversized directory fails closed
// rather than silently widening startup/backup maintenance work.
const MAX_BACKUP_SCAN_ENTRIES: usize = 4096;

/// Options for one online backup run.
#[derive(Debug, Clone)]
pub struct BackupOptions {
    /// Wall-clock budget for the entire worker (lock, copy, validation, publish).
    pub timeout: Duration,
    /// Positive page count passed to each `Backup::step` call.
    pub pages_per_step: u32,
    /// Upper bound for exponential backoff after transient `Busy` / `Locked` steps.
    pub max_busy_backoff: Duration,
}

impl Default for BackupOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(5 * 60),
            pages_per_step: 128,
            max_busy_backoff: Duration::from_millis(250),
        }
    }
}

/// Coarse lifecycle phase for status snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BackupPhase {
    Queued,
    AcquiringLock,
    Copying,
    Finalizing,
    Validating,
    Publishing,
    Completed,
    Failed,
    Cancelled,
}

/// Bounded error category for failed or cancelled backups.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BackupErrorKind {
    Options,
    LockUnavailable,
    Io,
    Sqlite,
    BusyExhausted,
    Deadline,
    Validation,
    Publish,
    Cleanup,
    Cancelled,
    Internal,
}

/// Serializable point-in-time backup progress (safe for diagnostics JSON).
#[derive(Debug, Clone, Serialize)]
pub struct BackupStatusSnapshot {
    pub phase: BackupPhase,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagecount: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remaining: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_kind: Option<BackupErrorKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_detail: Option<String>,
}

/// Cloneable handle to request cancellation and read status without joining the worker.
#[derive(Debug, Clone)]
pub struct BackupControl {
    cancel: Arc<AtomicBool>,
    status: Arc<Mutex<BackupStatusSnapshot>>,
}

impl BackupControl {
    /// Request cooperative cancellation.
    ///
    /// Sets the cancel flag only; does not transition status to a terminal phase. Returns `false`
    /// once [`BackupPhase::Publishing`] or a terminal phase is already recorded (publication cannot
    /// be cooperatively cancelled). The status mutex is held for the phase check and the
    /// [`AtomicBool`] store so the worker cannot advance to [`BackupPhase::Publishing`] between
    /// them (see the pre-publish gate in [`backup_worker`]).
    pub fn cancel(&self) -> bool {
        let guard = lock_status(&self.status);
        if matches!(
            guard.phase,
            BackupPhase::Publishing
                | BackupPhase::Completed
                | BackupPhase::Failed
                | BackupPhase::Cancelled
        ) {
            return false;
        }
        // Keep `guard` alive: linearize with pre-publish `Publishing` under the same mutex.
        self.cancel.store(true, Ordering::Release);
        true
    }

    /// Clone the latest status snapshot.
    pub fn snapshot(&self) -> BackupStatusSnapshot {
        lock_status(&self.status).clone()
    }

    fn cancel_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.cancel)
    }
}

/// Published on-disk backup artifact (hard-linked final name in `data_dir/backups`).
#[derive(Debug, Clone)]
pub struct BackupArtifact {
    pub path: PathBuf,
    pub created_at_unix_ms: i64,
}

/// In-flight backup worker handle.
///
/// [`Drop`] sets the cancel flag when the join handle is still held; it does not join the worker.
/// Use [`BackupTask::join`] or [`BackupTask::cancel_and_join`] to drain the thread before release
/// when shutdown ordering matters.
pub(crate) struct BackupTask {
    control: BackupControl,
    join_handle: Option<JoinHandle<BackupWorkerOutcome>>,
}

impl BackupTask {
    pub(crate) fn cancel(&self) -> bool {
        self.control.cancel()
    }

    pub(crate) fn snapshot(&self) -> BackupStatusSnapshot {
        self.control.snapshot()
    }

    pub(crate) fn control(&self) -> BackupControl {
        self.control.clone()
    }

    /// Whether the worker thread has finished (`JoinHandle::is_finished`).
    ///
    /// A `true` value does not release the join handle or SQLite resources; only [`Self::join`]
    /// does. Status may still show a non-terminal phase until the worker writes its final snapshot.
    pub(crate) fn is_finished(&self) -> bool {
        self.join_handle
            .as_ref()
            .is_some_and(JoinHandle::is_finished)
    }

    /// Bounded wait for [`Self::is_finished`] using `park_timeout` slices (no busy-spin).
    ///
    /// Returns `true` only when the worker thread has exited. Does not call [`Self::join`].
    pub(crate) fn wait_until_finished(&self, timeout: Duration) -> bool {
        if self.is_finished() {
            return true;
        }
        if timeout.is_zero() {
            return self.is_finished();
        }
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            return self.is_finished();
        };
        const POLL_CAP: Duration = Duration::from_millis(50);
        while !self.is_finished() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            std::thread::park_timeout(remaining.min(POLL_CAP));
        }
        true
    }

    /// Request cooperative cancel, then block until the worker finishes (same as [`Self::join`]).
    pub(crate) fn cancel_and_join(self) -> anyhow::Result<BackupArtifact> {
        let _ = self.cancel();
        self.join()
    }

    /// Block until the worker finishes and return the published artifact on success.
    pub(crate) fn join(mut self) -> anyhow::Result<BackupArtifact> {
        let handle = self
            .join_handle
            .take()
            .context("backup task already joined")?;
        let outcome = match handle.join() {
            Ok(outcome) => outcome,
            Err(_) => {
                set_failure(
                    &self.control,
                    BackupErrorKind::Internal,
                    bounded_error("backup worker thread panicked".to_string()),
                );
                return Err(anyhow!("backup worker panicked"));
            }
        };
        outcome.into_result()
    }
}

impl Drop for BackupTask {
    fn drop(&mut self) {
        if self.join_handle.is_some() {
            let _ = self.control.cancel();
        }
    }
}

#[derive(Debug)]
enum BackupWorkerOutcome {
    Success(BackupArtifact),
    Failure {
        kind: BackupErrorKind,
        detail: String,
    },
}

impl BackupWorkerOutcome {
    fn into_result(self) -> anyhow::Result<BackupArtifact> {
        match self {
            Self::Success(artifact) => Ok(artifact),
            Self::Failure { kind, detail } => Err(anyhow!("online backup {kind:?}: {detail}")),
        }
    }
}

impl Database {
    /// Start one online backup on a dedicated blocking worker thread.
    ///
    /// `instance_owner` must be the [`ServerInstanceGuard`] for `self` ([`ServerInstanceGuard::owns_database`]).
    /// Its `Arc` is moved into the worker and held until the worker exits (including after panic).
    ///
    /// Does not lock the canonical store writer or Session database. Concurrent store writes may
    /// extend copy duration or reset backup page progress within the configured timeout.
    ///
    /// Prefer [`crate::BackupMaintenanceOwner::start_online_backup`] in production. On shutdown, Runtime
    /// requests cooperative cancellation and waits up to its bounded drain deadline before joining
    /// in-flight work. SQLite native operations still have no strict wall-clock bound, and if the
    /// drain deadline expires before the worker exits, the worker's `Arc` of [`ServerInstanceGuard`]
    /// remains pinned until the thread finishes.
    pub(crate) fn start_online_backup(
        self: &Arc<Self>,
        instance_owner: Arc<ServerInstanceGuard>,
        options: BackupOptions,
    ) -> anyhow::Result<BackupTask> {
        if !instance_owner.owns_database(self.as_ref()) {
            anyhow::bail!("instance lock does not match database state path");
        }
        validate_backup_options(&options)?;
        let control = BackupControl {
            cancel: Arc::new(AtomicBool::new(false)),
            status: Arc::new(Mutex::new(BackupStatusSnapshot {
                phase: BackupPhase::Queued,
                pagecount: None,
                remaining: None,
                error_kind: None,
                error_detail: None,
            })),
        };
        let db = Arc::clone(self);
        let worker_control = control.clone();
        let join_handle = thread::Builder::new()
            .name(BACKUP_THREAD_NAME.to_string())
            .spawn(move || {
                let _owner_lease = instance_owner;
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    backup_worker(db.state_path(), options, worker_control.clone())
                }))
                .unwrap_or_else(|_| {
                    let detail = bounded_error("backup worker panicked".to_string());
                    set_failure(&worker_control, BackupErrorKind::Internal, detail.clone());
                    BackupWorkerOutcome::Failure {
                        kind: BackupErrorKind::Internal,
                        detail,
                    }
                })
            })
            .context("spawn online backup worker")?;
        Ok(BackupTask {
            control,
            join_handle: Some(join_handle),
        })
    }
}

/// Offline maintenance already owns the instance lock and holds an IMMEDIATE
/// source transaction (no writes yet). Reuse the canonical bounded SQLite
/// backup, verification and hardened artifact publication path without opening
/// or upgrading the general Store before its rollback snapshot exists.
pub(crate) fn create_offline_migration_backup(
    owner: &ServerInstanceGuard,
) -> anyhow::Result<BackupArtifact> {
    let options = BackupOptions::default();
    validate_backup_options(&options)?;
    let control = BackupControl {
        cancel: Arc::new(AtomicBool::new(false)),
        status: Arc::new(Mutex::new(BackupStatusSnapshot {
            phase: BackupPhase::Queued,
            pagecount: None,
            remaining: None,
            error_kind: None,
            error_detail: None,
        })),
    };
    backup_worker(owner.database_path(), options, control).into_result()
}

fn validate_backup_options(options: &BackupOptions) -> anyhow::Result<()> {
    if options.pages_per_step == 0 {
        anyhow::bail!("backup pages_per_step must be positive");
    }
    if options.timeout.is_zero() {
        anyhow::bail!("backup timeout must be positive");
    }
    if options.max_busy_backoff.is_zero() {
        anyhow::bail!("backup max_busy_backoff must be positive");
    }
    if Instant::now().checked_add(options.timeout).is_none() {
        anyhow::bail!("backup timeout overflows Instant deadline");
    }
    Ok(())
}

/// Removes this operation's partial backup and sidecars on unwind or early return unless disarmed.
struct PartialCleanupGuard<'a> {
    control: &'a BackupControl,
    path: Option<PathBuf>,
    created_file: File,
}

impl<'a> PartialCleanupGuard<'a> {
    fn new(control: &'a BackupControl, path: PathBuf, created_file: File) -> Self {
        Self {
            control,
            path: Some(path),
            created_file,
        }
    }

    fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    fn disarm(&mut self) {
        self.path = None;
    }
}

impl Drop for PartialCleanupGuard<'_> {
    fn drop(&mut self) {
        let Some(path) = self.path.take() else {
            return;
        };
        if let Err(detail) = remove_partial_db_and_sidecars_result(&path, &self.created_file) {
            note_cleanup_failure(self.control, detail);
        }
    }
}

fn backup_worker_fail(
    control: &BackupControl,
    kind: BackupErrorKind,
    detail: String,
    partial_guard: &mut Option<PartialCleanupGuard<'_>>,
) -> BackupWorkerOutcome {
    let mut detail = bounded_error(detail);
    if let Some(guard) = partial_guard.as_mut() {
        if let Some(path) = guard.path() {
            if let Err(cleanup_detail) =
                remove_partial_db_and_sidecars_result(path, &guard.created_file)
            {
                detail = merge_cleanup_into_detail(detail, cleanup_detail);
            }
        }
        guard.disarm();
    }
    partial_guard.take();
    let effective_kind = effective_failure_kind(control, kind);
    set_failure(control, effective_kind, detail.clone());
    BackupWorkerOutcome::Failure {
        kind: effective_kind,
        detail,
    }
}

fn backup_worker(
    state_path: &Path,
    options: BackupOptions,
    control: BackupControl,
) -> BackupWorkerOutcome {
    let deadline = match Instant::now().checked_add(options.timeout) {
        Some(deadline) => deadline,
        None => {
            return backup_worker_fail(
                &control,
                BackupErrorKind::Options,
                "backup timeout overflows Instant deadline".to_string(),
                &mut None,
            );
        }
    };

    // Locals drop in reverse declaration order: retain the directory lock while
    // the partial guard cleans up, including during panic unwinding.
    let lock_file;
    let mut partial_guard: Option<PartialCleanupGuard<'_>> = None;

    if let Some(outcome) = early_abort_if_stopped(&control, deadline, &mut partial_guard) {
        return outcome;
    }

    update_phase(&control, BackupPhase::AcquiringLock, None, None);

    let backup_dir = match ensure_hardened_backup_dir(state_path) {
        Ok(dir) => dir,
        Err(error) => {
            return backup_worker_fail(
                &control,
                BackupErrorKind::Io,
                format!("ensure backup directory: {error}"),
                &mut partial_guard,
            );
        }
    };

    if let Some(outcome) = early_abort_if_stopped(&control, deadline, &mut partial_guard) {
        return outcome;
    }

    let lock_path = backup_dir.join(BACKUP_DIR_LOCK_NAME);
    if let Err(error) = validate_lock_target(&lock_path, &backup_dir) {
        return backup_worker_fail(
            &control,
            BackupErrorKind::Io,
            format!("validate backup dir lock path: {error}"),
            &mut partial_guard,
        );
    }

    lock_file = match open_backup_dir_lock(&lock_path) {
        Ok(file) => file,
        Err(error) => {
            return backup_worker_fail(
                &control,
                BackupErrorKind::Io,
                format!("open backup dir lock: {error}"),
                &mut partial_guard,
            );
        }
    };

    if let Err(error) = validate_opened_lock_file(&lock_file, &lock_path) {
        return backup_worker_fail(
            &control,
            BackupErrorKind::Io,
            format!("backup dir lock file type: {error}"),
            &mut partial_guard,
        );
    }

    match lock_file.try_lock_exclusive() {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
            return backup_worker_fail(
                &control,
                BackupErrorKind::LockUnavailable,
                "another online backup holds the backup-dir lock".to_string(),
                &mut partial_guard,
            );
        }
        Err(error) => {
            return backup_worker_fail(
                &control,
                BackupErrorKind::Io,
                format!("acquire backup dir lock: {error}"),
                &mut partial_guard,
            );
        }
    }

    if let Some(outcome) = early_abort_if_stopped(&control, deadline, &mut partial_guard) {
        return outcome;
    }

    // Acquisition must still refer to the file named by the stable lock path.
    if let Err(error) = validate_opened_lock_file(&lock_file, &lock_path) {
        return backup_worker_fail(
            &control,
            BackupErrorKind::Io,
            format!("verify acquired backup dir lock: {error}"),
            &mut partial_guard,
        );
    }

    if let Err(error) = cleanup_stale_module_partials(&backup_dir, &control, deadline) {
        let kind = if Instant::now() >= deadline {
            BackupErrorKind::Deadline
        } else {
            BackupErrorKind::Cleanup
        };
        return backup_worker_fail(
            &control,
            kind,
            format!("stale partial cleanup: {error}"),
            &mut partial_guard,
        );
    }

    let operation_id = Uuid::new_v4();
    let partial = backup_dir.join(format!(
        "{PARTIAL_NAME_PREFIX}{operation_id}{PARTIAL_NAME_SUFFIX}"
    ));
    let final_name = backup_dir.join(format!(
        "webcodex-{}-{operation_id}.db",
        Utc::now().timestamp_millis()
    ));

    let created_file = match open_new_partial_backup_file(&partial) {
        Ok(file) => file,
        Err(error) => {
            return backup_worker_fail(
                &control,
                BackupErrorKind::Io,
                format!("create partial backup file: {error}"),
                &mut partial_guard,
            )
        }
    };

    partial_guard = Some(PartialCleanupGuard::new(
        &control,
        partial.clone(),
        created_file,
    ));

    if let Err(error) = sync_new_partial_and_parent(&partial) {
        return backup_worker_fail(
            &control,
            BackupErrorKind::Io,
            format!("sync new partial backup: {error}"),
            &mut partial_guard,
        );
    }

    let source = match open_backup_source(state_path) {
        Ok(conn) => conn,
        Err(error) => {
            return backup_worker_fail(
                &control,
                BackupErrorKind::Sqlite,
                format!("open backup source connection: {error}"),
                &mut partial_guard,
            );
        }
    };

    let mut destination = match open_backup_destination(&partial) {
        Ok(conn) => conn,
        Err(error) => {
            let _ = close_sqlite_connection(source);
            return backup_worker_fail(
                &control,
                BackupErrorKind::Sqlite,
                format!("open backup destination connection: {error}"),
                &mut partial_guard,
            );
        }
    };

    update_phase(&control, BackupPhase::Copying, None, None);

    match run_backup_copy(&control, &source, &mut destination, &options, deadline).map(drop) {
        Ok(()) => {}
        Err(kind) => {
            let detail = match kind {
                BackupErrorKind::Cancelled => "backup cancelled during copy",
                BackupErrorKind::Deadline => "backup deadline exceeded during copy",
                BackupErrorKind::BusyExhausted => {
                    "backup busy/locked backoff exhausted before deadline"
                }
                BackupErrorKind::Sqlite => "sqlite error during backup copy",
                _ => "backup copy failed",
            };
            let _ = close_sqlite_connection(destination);
            let _ = close_sqlite_connection(source);
            return backup_worker_fail(&control, kind, detail.to_string(), &mut partial_guard);
        }
    };

    update_phase(&control, BackupPhase::Finalizing, None, None);

    if let Err(error) = ensure_destination_durability(&destination) {
        let _ = close_sqlite_connection(destination);
        let _ = close_sqlite_connection(source);
        return backup_worker_fail(
            &control,
            BackupErrorKind::Sqlite,
            format!("configure destination durability: {error}"),
            &mut partial_guard,
        );
    }

    if let Err(error) = close_sqlite_connection(destination) {
        let _ = close_sqlite_connection(source);
        return backup_worker_fail(
            &control,
            BackupErrorKind::Sqlite,
            format!("close destination connection: {error}"),
            &mut partial_guard,
        );
    }

    if let Err(error) = close_sqlite_connection(source) {
        return backup_worker_fail(
            &control,
            BackupErrorKind::Sqlite,
            format!("close source connection: {error}"),
            &mut partial_guard,
        );
    }

    if let Err(detail) = require_no_sidecars(&partial) {
        return backup_worker_fail(
            &control,
            BackupErrorKind::Validation,
            detail,
            &mut partial_guard,
        );
    }

    update_phase(&control, BackupPhase::Validating, None, None);

    match validate_closed_partial_candidate(&partial, &control, deadline) {
        Ok(()) => {}
        Err(kind) => {
            let detail = match kind {
                BackupErrorKind::Cancelled => "backup cancelled during validation",
                BackupErrorKind::Deadline => "backup deadline exceeded during validation",
                BackupErrorKind::Validation => "backup validation failed",
                BackupErrorKind::Sqlite => "sqlite error during validation",
                _ => "validation failed",
            };
            return backup_worker_fail(&control, kind, detail.to_string(), &mut partial_guard);
        }
    }

    if let Err(error) = sync_partial_and_parent(&partial) {
        return backup_worker_fail(
            &control,
            BackupErrorKind::Io,
            format!("sync partial backup: {error}"),
            &mut partial_guard,
        );
    }

    enum PrePublishGate {
        Proceed,
        Abort(BackupErrorKind, String),
    }

    let pre_publish = {
        let mut guard = lock_status(&control.status);
        if cancellation_requested(&control) {
            PrePublishGate::Abort(
                BackupErrorKind::Cancelled,
                "backup cancelled before publish".to_string(),
            )
        } else if matches!(
            guard.phase,
            BackupPhase::Completed | BackupPhase::Failed | BackupPhase::Publishing
        ) {
            PrePublishGate::Abort(
                BackupErrorKind::Internal,
                "backup status advanced unexpectedly before publish".to_string(),
            )
        } else if Instant::now() >= deadline {
            PrePublishGate::Abort(
                BackupErrorKind::Deadline,
                "backup deadline exceeded before publish".to_string(),
            )
        } else {
            guard.phase = BackupPhase::Publishing;
            PrePublishGate::Proceed
        }
    };

    if let PrePublishGate::Abort(kind, detail) = pre_publish {
        return backup_worker_fail(&control, kind, detail, &mut partial_guard);
    }

    let publication = publish_partial_hard_link(
        &partial,
        &final_name,
        &partial_guard
            .as_ref()
            .expect("created partial owner before publication")
            .created_file,
    );
    match publication {
        Ok((artifact, cleanup_warning)) => {
            // Publication already attempted identity-checked cleanup. A warning
            // may mean the path changed; an unconditional Drop retry is unsafe.
            if let Some(guard) = partial_guard.as_mut() {
                guard.disarm();
            }
            partial_guard.take();
            let mut guard = lock_status(&control.status);
            guard.phase = BackupPhase::Completed;
            guard.error_kind = cleanup_warning.as_ref().map(|_| BackupErrorKind::Cleanup);
            guard.error_detail = cleanup_warning;
            BackupWorkerOutcome::Success(artifact)
        }
        Err((kind, detail)) => {
            // Publication errors can include identity uncertainty. Preserve both
            // names for diagnosis; do not unlink a path that may have retargeted.
            if let Some(guard) = partial_guard.as_mut() {
                guard.disarm();
            }
            backup_worker_fail(&control, kind, detail, &mut partial_guard)
        }
    }
}

fn run_backup_copy<'source, 'destination>(
    control: &BackupControl,
    source: &'source Connection,
    destination: &'destination mut Connection,
    options: &BackupOptions,
    deadline: Instant,
) -> Result<Backup<'source, 'destination>, BackupErrorKind> {
    let mut backup = Backup::new(source, destination).map_err(|_| BackupErrorKind::Sqlite)?;
    let pages = i32::try_from(options.pages_per_step).map_err(|_| BackupErrorKind::Options)?;
    let mut busy_backoff = Duration::from_millis(1).min(options.max_busy_backoff);

    loop {
        if cancellation_requested(control) {
            return Err(BackupErrorKind::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(BackupErrorKind::Deadline);
        }

        match backup.step(pages) {
            Ok(StepResult::Done) => {
                let progress = backup.progress();
                let (pagecount, remaining) = backup_progress_counts(&progress);
                update_phase(control, BackupPhase::Copying, pagecount, remaining);
                return Ok(backup);
            }
            Ok(StepResult::More) => {
                let progress = backup.progress();
                let (pagecount, remaining) = backup_progress_counts(&progress);
                update_phase(control, BackupPhase::Copying, pagecount, remaining);
                std::thread::yield_now();
                busy_backoff = Duration::from_millis(1).min(options.max_busy_backoff);
            }
            Ok(StepResult::Busy | StepResult::Locked) => {
                let exhausted = match Instant::now().checked_add(busy_backoff) {
                    None => true,
                    Some(wake_at) => wake_at >= deadline,
                };
                if exhausted {
                    return Err(BackupErrorKind::BusyExhausted);
                }
                std::thread::sleep(busy_backoff);
                busy_backoff = next_busy_backoff(busy_backoff, options.max_busy_backoff);
            }
            Ok(_) => return Err(BackupErrorKind::Internal),
            Err(_) => return Err(BackupErrorKind::Sqlite),
        }
    }
}

fn next_busy_backoff(current: Duration, cap: Duration) -> Duration {
    let doubled = current.saturating_mul(2);
    if doubled > cap {
        cap
    } else {
        doubled
    }
}

fn backup_progress_counts(progress: &rusqlite::backup::Progress) -> (Option<u32>, Option<u32>) {
    (
        u32::try_from(progress.pagecount).ok(),
        u32::try_from(progress.remaining).ok(),
    )
}

fn open_backup_source(state_path: &Path) -> anyhow::Result<Connection> {
    let connection = Connection::open_with_flags(
        state_path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .context("open backup source")?;
    connection
        .execute_batch("PRAGMA query_only=ON; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=0;")?;
    Ok(connection)
}

fn open_backup_destination(partial: &Path) -> anyhow::Result<Connection> {
    let connection = Connection::open_with_flags(
        partial,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .context("open backup destination")?;
    connection.execute_batch(
        "PRAGMA synchronous=FULL; PRAGMA journal_mode=DELETE; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=0;",
    )?;
    let mode: String = connection.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
    if !mode.eq_ignore_ascii_case("delete") {
        anyhow::bail!("destination journal_mode is {mode}, expected delete");
    }
    let sync: i64 = connection.query_row("PRAGMA synchronous", [], |row| row.get(0))?;
    if sync != 2 {
        anyhow::bail!("destination synchronous is {sync}, expected FULL (2)");
    }
    Ok(connection)
}

fn open_backup_dir_lock(lock_path: &Path) -> anyhow::Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    set_nofollow_options(&mut options)?;
    let file = options
        .open(lock_path)
        .context("open backup directory lock file")?;
    validate_opened_lock_file(&file, lock_path)?;
    Ok(file)
}

fn set_nofollow_options(options: &mut OpenOptions) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC);
        Ok(())
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
        Ok(())
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = options;
        anyhow::bail!("no-follow backup file opens are unsupported on this platform")
    }
}

fn open_nofollow_regular_file(path: &Path, write: bool) -> anyhow::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(write);
    set_nofollow_options(&mut options)?;
    let file = options
        .open(path)
        .with_context(|| format!("open backup file {}", path.display()))?;
    let meta = file.metadata().context("stat opened backup file")?;
    if !meta.is_file() || meta.file_type().is_symlink() || is_reparse_point(&meta) {
        anyhow::bail!(
            "backup file is not a regular no-follow file: {}",
            path.display()
        );
    }
    Ok(file)
}

fn same_open_file_object(left: &File, right: &File) -> anyhow::Result<bool> {
    // same-file owns the platform-specific dev/inode or Windows volume/file-id
    // comparison; clone handles rather than reopening paths that could retarget.
    let left = same_file::Handle::from_file(left.try_clone()?)?;
    let right = same_file::Handle::from_file(right.try_clone()?)?;
    Ok(left == right)
}

fn ensure_open_file_matches_path(file: &File, path: &Path) -> anyhow::Result<()> {
    let meta = file
        .metadata()
        .context("stat retained backup file handle")?;
    if !meta.is_file() || meta.file_type().is_symlink() || is_reparse_point(&meta) {
        anyhow::bail!("retained backup handle is not a regular no-follow file");
    }
    let current = open_nofollow_regular_file(path, false)?;
    if !same_open_file_object(file, &current)? {
        anyhow::bail!(
            "backup path no longer names retained file object: {}",
            path.display()
        );
    }
    Ok(())
}

fn ensure_hardened_backup_dir(state_path: &Path) -> anyhow::Result<PathBuf> {
    let data_dir = data_directory(state_path)?;
    let backup_dir = data_dir.join(BACKUP_DIR_NAME);
    match fs::symlink_metadata(&backup_dir) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            create_hardened_backup_dir(&backup_dir)?;
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("stat backup directory {}", backup_dir.display()));
        }
        Ok(meta) => {
            validate_backup_dir_metadata(&meta)?;
            enforce_hardened_backup_dir_permissions(&backup_dir, &meta)?;
        }
    }
    canonical_dir_under_data_root(&backup_dir, state_path)
}

fn create_hardened_backup_dir(backup_dir: &Path) -> anyhow::Result<()> {
    let mut builder = DirBuilder::new();
    builder.recursive(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    match builder.create(backup_dir) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let meta = fs::symlink_metadata(backup_dir)
                .with_context(|| format!("stat backup directory {}", backup_dir.display()))?;
            validate_backup_dir_metadata(&meta)?;
            enforce_hardened_backup_dir_permissions(backup_dir, &meta)
        }
        Err(error) => {
            Err(error).with_context(|| format!("create backup directory {}", backup_dir.display()))
        }
    }
}

fn validate_backup_dir_metadata(meta: &fs::Metadata) -> anyhow::Result<()> {
    if meta.file_type().is_symlink() {
        anyhow::bail!("backup directory must not be a symlink");
    }
    if is_reparse_point(meta) {
        anyhow::bail!("backup directory must not be a reparse point");
    }
    if !meta.is_dir() {
        anyhow::bail!("backup path is not a directory");
    }
    Ok(())
}

#[cfg(unix)]
fn enforce_hardened_backup_dir_permissions(
    backup_dir: &Path,
    meta: &fs::Metadata,
) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mode = meta.permissions().mode();
    if mode & 0o077 != 0 {
        fs::set_permissions(backup_dir, fs::Permissions::from_mode(0o700))
            .with_context(|| format!("harden backup directory {}", backup_dir.display()))?;
    }
    Ok(())
}

#[cfg(windows)]
fn enforce_hardened_backup_dir_permissions(
    _backup_dir: &Path,
    _meta: &fs::Metadata,
) -> anyhow::Result<()> {
    Ok(())
}

fn open_new_partial_backup_file(partial: &Path) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(partial)
}

fn early_abort_if_stopped(
    control: &BackupControl,
    deadline: Instant,
    partial_guard: &mut Option<PartialCleanupGuard<'_>>,
) -> Option<BackupWorkerOutcome> {
    if cancellation_requested(control) {
        return Some(backup_worker_fail(
            control,
            BackupErrorKind::Cancelled,
            "backup cancelled before backup-dir work".to_string(),
            partial_guard,
        ));
    }
    if Instant::now() >= deadline {
        return Some(backup_worker_fail(
            control,
            BackupErrorKind::Deadline,
            "backup deadline exceeded before backup-dir work".to_string(),
            partial_guard,
        ));
    }
    None
}

fn effective_failure_kind(control: &BackupControl, kind: BackupErrorKind) -> BackupErrorKind {
    if kind == BackupErrorKind::Cancelled || cancellation_requested(control) {
        BackupErrorKind::Cancelled
    } else {
        kind
    }
}

fn merge_cleanup_into_detail(primary: String, cleanup: String) -> String {
    bounded_error(format!("{primary}; cleanup: {cleanup}"))
}

fn append_bounded_cleanup_note(primary: &mut String, cleanup: String) {
    let suffix = format!("; cleanup: {cleanup}");
    let combined = format!("{primary}{suffix}");
    *primary = bounded_error(combined);
}

fn data_directory(state_path: &Path) -> anyhow::Result<PathBuf> {
    let parent = state_path
        .parent()
        .context("database state path has no parent directory")?;
    fs::canonicalize(parent).context("canonicalize data directory")
}

fn canonical_dir_under_data_root(backup_dir: &Path, state_path: &Path) -> anyhow::Result<PathBuf> {
    let data_dir = state_path
        .parent()
        .and_then(|parent| fs::canonicalize(parent).ok())
        .context("canonicalize data directory root")?;
    let canonical_backup = fs::canonicalize(backup_dir)
        .with_context(|| format!("canonicalize backup directory {}", backup_dir.display()))?;
    if !path_starts_with(&canonical_backup, &data_dir) {
        anyhow::bail!("backup directory escapes data directory");
    }
    let meta = fs::symlink_metadata(&canonical_backup)
        .with_context(|| format!("stat backup directory {}", canonical_backup.display()))?;
    if meta.file_type().is_symlink() {
        anyhow::bail!("backup directory must not be a symlink");
    }
    if is_reparse_point(&meta) {
        anyhow::bail!("backup directory must not be a reparse point");
    }
    if !meta.is_dir() {
        anyhow::bail!("backup directory is not a directory");
    }
    Ok(canonical_backup)
}

fn path_starts_with(path: &Path, prefix: &Path) -> bool {
    path.starts_with(prefix)
}

fn validate_lock_target(lock_path: &Path, backup_dir: &Path) -> anyhow::Result<()> {
    if !path_starts_with(lock_path, backup_dir) {
        anyhow::bail!("backup lock path escapes backup directory");
    }
    let meta = match fs::symlink_metadata(lock_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error).with_context(|| format!("stat backup lock {}", lock_path.display()));
        }
        Ok(meta) => meta,
    };
    if meta.file_type().is_symlink() {
        anyhow::bail!("backup lock file must not be a symlink");
    }
    if is_reparse_point(&meta) {
        anyhow::bail!("backup lock file must not be a reparse point");
    }
    if !meta.is_file() {
        anyhow::bail!("backup lock path is not a regular file");
    }
    Ok(())
}

fn validate_opened_lock_file(lock_file: &File, lock_path: &Path) -> anyhow::Result<()> {
    let meta = fs::symlink_metadata(lock_path)
        .with_context(|| format!("stat opened backup lock {}", lock_path.display()))?;
    if meta.file_type().is_symlink() {
        anyhow::bail!("backup lock file must not be a symlink");
    }
    if is_reparse_point(&meta) {
        anyhow::bail!("backup lock file must not be a reparse point");
    }
    if !meta.is_file() {
        anyhow::bail!("backup lock file is not a regular file");
    }
    ensure_open_file_matches_path(lock_file, lock_path)
}

fn is_reparse_point(meta: &fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
        (meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT) != 0
    }
    #[cfg(not(windows))]
    {
        let _ = meta;
        false
    }
}

fn is_module_partial_file_name(name: &str) -> bool {
    let Some(body) = name.strip_prefix(PARTIAL_NAME_PREFIX) else {
        return false;
    };
    let Some(uuid_str) = body.strip_suffix(PARTIAL_NAME_SUFFIX) else {
        return false;
    };
    Uuid::parse_str(uuid_str).is_ok_and(|operation_id| operation_id.to_string() == uuid_str)
}

fn cleanup_stale_module_partials(
    backup_dir: &Path,
    control: &BackupControl,
    deadline: Instant,
) -> anyhow::Result<()> {
    for (index, entry) in fs::read_dir(backup_dir)
        .with_context(|| format!("read backup directory {}", backup_dir.display()))?
        .enumerate()
    {
        if cancellation_requested(control) {
            anyhow::bail!("backup cancelled during stale partial scan");
        }
        if Instant::now() >= deadline {
            anyhow::bail!("backup deadline exceeded during stale partial scan");
        }
        if index >= MAX_BACKUP_SCAN_ENTRIES {
            anyhow::bail!("backup directory scan exceeded {MAX_BACKUP_SCAN_ENTRIES} entries");
        }
        let entry = entry.with_context(|| format!("read backup directory entry"))?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        if !is_module_partial_file_name(name) {
            continue;
        }
        let file_type = entry
            .file_type()
            .with_context(|| format!("stat partial backup {}", entry.path().display()))?;
        if file_type.is_symlink() {
            continue;
        }
        let meta = fs::symlink_metadata(entry.path())
            .with_context(|| format!("stat partial backup {}", entry.path().display()))?;
        if is_reparse_point(&meta) {
            continue;
        }
        if !file_type.is_file() {
            continue;
        }
        let candidate = open_nofollow_regular_file(&entry.path(), false)?;
        ensure_open_file_matches_path(&candidate, &entry.path())?;
        remove_partial_db_and_sidecars_result(&entry.path(), &candidate)
            .map_err(anyhow::Error::msg)?;
    }
    Ok(())
}

fn ensure_destination_durability(destination: &Connection) -> anyhow::Result<()> {
    destination.execute_batch("PRAGMA synchronous=FULL; PRAGMA journal_mode=DELETE;")?;
    let mode: String = destination.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
    if !mode.eq_ignore_ascii_case("delete") {
        anyhow::bail!("destination journal_mode is {mode}, expected delete");
    }
    let sync: i64 = destination.query_row("PRAGMA synchronous", [], |row| row.get(0))?;
    if sync != 2 {
        anyhow::bail!("destination synchronous is {sync}, expected FULL (2)");
    }
    Ok(())
}

fn close_sqlite_connection(connection: Connection) -> anyhow::Result<()> {
    match connection.close() {
        Ok(()) => Ok(()),
        Err((_connection, error)) => Err(error.into()),
    }
}

fn require_no_sidecars(base: &Path) -> Result<(), String> {
    for suffix in ["-wal", "-shm", "-journal"] {
        let sidecar = sidecar_path(base, suffix);
        match fs::symlink_metadata(&sidecar) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(bounded_error(format!(
                    "stat destination sidecar {}: {error}",
                    sidecar.display()
                )));
            }
            Ok(_) => {
                return Err(bounded_error(format!(
                    "destination sidecar {} must not exist after close",
                    sidecar.display()
                )));
            }
        }
    }
    Ok(())
}

fn sidecar_path(base: &Path, suffix: &str) -> PathBuf {
    let mut path = OsString::from(base.as_os_str());
    path.push(suffix);
    PathBuf::from(path)
}

fn validate_closed_partial_candidate(
    partial: &Path,
    control: &BackupControl,
    deadline: Instant,
) -> Result<(), BackupErrorKind> {
    let mut connection = Connection::open_with_flags(
        partial,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|_| BackupErrorKind::Sqlite)?;
    connection
        .execute_batch("PRAGMA query_only=ON; PRAGMA foreign_keys=ON;")
        .map_err(|_| BackupErrorKind::Sqlite)?;

    let cancel = control.cancel_flag();
    let progress_result = (|| -> Result<(), BackupErrorKind> {
        connection
            .progress_handler(
                100,
                Some(move || {
                    if cancel.load(Ordering::Acquire) {
                        return true;
                    }
                    Instant::now() >= deadline
                }),
            )
            .map_err(|_| BackupErrorKind::Sqlite)?;

        let quick_check: String = connection
            .query_row("PRAGMA quick_check(1)", [], |row| row.get(0))
            .map_err(|error| match error {
                rusqlite::Error::QueryReturnedNoRows => BackupErrorKind::Validation,
                _ => classification_for_sqlite(control, deadline, &error),
            })?;
        if quick_check != "ok" {
            return Err(BackupErrorKind::Validation);
        }

        {
            let mut stmt = connection
                .prepare("PRAGMA foreign_key_check")
                .map_err(|error| classification_for_sqlite(control, deadline, &error))?;
            let mut rows = stmt
                .query([])
                .map_err(|error| classification_for_sqlite(control, deadline, &error))?;
            if rows
                .next()
                .map_err(|error| classification_for_sqlite(control, deadline, &error))?
                .is_some()
            {
                return Err(BackupErrorKind::Validation);
            }
        }

        crate::schema::validate_shared_database_backup_schema(&connection).map_err(|error| {
            // The shared Session detector reports io::Error while retaining
            // the original SQLite error. Keep BUSY/cancellation/deadline
            // classification identical to the other Store schema validators.
            let sqlite = error.chain().find_map(|cause| {
                cause.downcast_ref::<rusqlite::Error>().or_else(|| {
                    cause
                        .downcast_ref::<std::io::Error>()
                        .and_then(std::io::Error::get_ref)
                        .and_then(|inner| inner.downcast_ref::<rusqlite::Error>())
                })
            });
            if let Some(sqlite) = sqlite {
                return classification_for_sqlite(control, deadline, sqlite);
            }
            BackupErrorKind::Validation
        })
    })();

    let clear_progress = connection.progress_handler(0, None::<fn() -> bool>);
    let close_result = close_sqlite_connection(connection);

    match progress_result {
        Err(kind) => Err(kind),
        Ok(()) => {
            clear_progress.map_err(|_| BackupErrorKind::Sqlite)?;
            close_result.map_err(|_| BackupErrorKind::Sqlite)?;
            Ok(())
        }
    }
}

fn classification_for_sqlite(
    control: &BackupControl,
    deadline: Instant,
    error: &rusqlite::Error,
) -> BackupErrorKind {
    if matches!(
        error,
        rusqlite::Error::SqliteFailure(code, _)
            if code.code == ErrorCode::OperationInterrupted
    ) {
        if cancellation_requested(control) {
            BackupErrorKind::Cancelled
        } else if Instant::now() >= deadline {
            BackupErrorKind::Deadline
        } else {
            BackupErrorKind::Sqlite
        }
    } else {
        BackupErrorKind::Sqlite
    }
}

fn sync_new_partial_and_parent(partial: &Path) -> anyhow::Result<()> {
    let file = open_nofollow_regular_file(partial, true)?;
    ensure_open_file_matches_path(&file, partial)?;
    file.sync_all()
        .with_context(|| format!("sync new partial {}", partial.display()))?;
    sync_parent_directory(partial)
}

fn sync_partial_and_parent(partial: &Path) -> anyhow::Result<()> {
    let file = open_nofollow_regular_file(partial, true)?;
    ensure_open_file_matches_path(&file, partial)?;
    file.sync_all()
        .with_context(|| format!("sync partial backup file {}", partial.display()))?;
    sync_parent_directory(partial)
}

fn sync_parent_directory(path: &Path) -> anyhow::Result<()> {
    let parent = path.parent().context("partial path has no parent")?;
    #[cfg(windows)]
    {
        let _ = parent;
        Ok(())
    }
    #[cfg(not(windows))]
    {
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .context("sync backup parent directory")?;
        Ok(())
    }
}

fn publish_partial_hard_link(
    partial: &Path,
    final_path: &Path,
    partial_file: &File,
) -> Result<(BackupArtifact, Option<String>), (BackupErrorKind, String)> {
    ensure_open_file_matches_path(partial_file, partial).map_err(|error| {
        (
            BackupErrorKind::Publish,
            format!("verify partial before publish: {error}"),
        )
    })?;
    match fs::hard_link(partial, final_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err((
                BackupErrorKind::Publish,
                "backup final name already exists".to_string(),
            ));
        }
        Err(error) => {
            return Err((
                BackupErrorKind::Publish,
                format!("hard_link publish failed (fail closed; rename not used): {error}"),
            ));
        }
    }

    let final_file = open_nofollow_regular_file(final_path, true).map_err(|error| {
        (
            BackupErrorKind::Publish,
            format!("open published identity; names preserved: {error}"),
        )
    })?;
    let identities_match = same_open_file_object(partial_file, &final_file).and_then(|same| {
        if !same {
            anyhow::bail!("published and retained partial handles name different file objects");
        }
        ensure_open_file_matches_path(partial_file, partial)?;
        ensure_open_file_matches_path(&final_file, final_path)
    });
    identities_match.map_err(|error| {
        (
            BackupErrorKind::Publish,
            format!("published identity uncertain; names preserved: {error}"),
        )
    })?;
    sync_published_artifact(&final_file, final_path).map_err(|error| {
        (
            BackupErrorKind::Io,
            format!(
                "publication durability unconfirmed; names preserved at {}: {error}",
                final_path.display()
            ),
        )
    })?;

    let artifact = BackupArtifact {
        path: final_path.to_path_buf(),
        created_at_unix_ms: Utc::now().timestamp_millis(),
    };

    // Publication is already durable. Cleanup failure must retain artifact success
    // while reporting the independent cleanup outcome to the owner.
    let cleanup_warning = ensure_open_file_matches_path(partial_file, partial)
        .map_err(|error| bounded_error(format!("partial identity changed: {error}")))
        .and_then(|()| remove_partial_db_and_sidecars_result(partial, partial_file))
        .err()
        .map(|error| bounded_error(format!("valid backup published; cleanup failed: {error}")));

    Ok((artifact, cleanup_warning))
}

fn sync_published_artifact(file: &File, final_path: &Path) -> anyhow::Result<()> {
    file.sync_all()
        .with_context(|| format!("sync published backup {}", final_path.display()))?;
    ensure_open_file_matches_path(file, final_path)?;
    sync_parent_directory(final_path)
}

fn remove_partial_db_and_sidecars_result(
    partial: &Path,
    expected_file: &File,
) -> Result<(), String> {
    ensure_open_file_matches_path(expected_file, partial)
        .map_err(|error| bounded_error(format!("verify partial cleanup ownership: {error}")))?;
    // Keep the main partial as the stale scanner's retry anchor until every
    // sidecar is gone. Never erase that anchor after a sidecar removal failure.
    let mut last_error = None;
    for suffix in ["-wal", "-shm", "-journal"] {
        let sidecar = sidecar_path(partial, suffix);
        if let Err(error) = fs::remove_file(&sidecar) {
            if error.kind() != std::io::ErrorKind::NotFound {
                last_error = Some(bounded_error(format!(
                    "remove sidecar {}: {error}",
                    sidecar.display()
                )));
            }
        }
    }
    if let Some(error) = last_error {
        return Err(error);
    }
    // Persist sidecar removals before dropping the scanner's anchor. If this
    // sync fails, leave the main partial available for the next cleanup pass.
    sync_parent_directory(partial)
        .map_err(|error| bounded_error(format!("sync sidecar cleanup directory: {error}")))?;
    ensure_open_file_matches_path(expected_file, partial)
        .map_err(|error| bounded_error(format!("recheck partial cleanup ownership: {error}")))?;
    if let Err(error) = fs::remove_file(partial) {
        if error.kind() != std::io::ErrorKind::NotFound {
            return Err(bounded_error(format!(
                "remove partial backup {}: {error}",
                partial.display()
            )));
        }
    }
    // Persist both sidecar and main-name removals. On Windows this helper
    // preserves the existing platform contract (directory fsync unavailable).
    sync_parent_directory(partial)
        .map_err(|error| bounded_error(format!("sync partial cleanup directory: {error}")))
}

fn note_cleanup_failure(control: &BackupControl, detail: String) {
    let mut guard = lock_status(&control.status);
    let detail = bounded_error(detail);
    if let Some(existing) = guard.error_detail.as_mut() {
        append_bounded_cleanup_note(existing, detail);
        if guard.error_kind.is_none() {
            guard.error_kind = Some(BackupErrorKind::Cleanup);
        }
    } else {
        guard.error_kind = Some(BackupErrorKind::Cleanup);
        guard.error_detail = Some(detail);
    }
}

fn cancellation_requested(control: &BackupControl) -> bool {
    control.cancel.load(Ordering::Acquire)
}

fn lock_status(status: &Arc<Mutex<BackupStatusSnapshot>>) -> MutexGuard<'_, BackupStatusSnapshot> {
    status
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn update_phase(
    control: &BackupControl,
    phase: BackupPhase,
    pagecount: Option<u32>,
    remaining: Option<u32>,
) {
    let mut guard = lock_status(&control.status);
    if matches!(
        guard.phase,
        BackupPhase::Completed
            | BackupPhase::Failed
            | BackupPhase::Cancelled
            | BackupPhase::Publishing
    ) {
        return;
    }
    guard.phase = phase;
    if let Some(pagecount) = pagecount {
        guard.pagecount = Some(pagecount);
    }
    if let Some(remaining) = remaining {
        guard.remaining = Some(remaining);
    }
}

fn set_failure(control: &BackupControl, kind: BackupErrorKind, detail: String) {
    let mut guard = lock_status(&control.status);
    if matches!(guard.phase, BackupPhase::Completed) {
        return;
    }
    if matches!(guard.phase, BackupPhase::Failed | BackupPhase::Cancelled) {
        let detail = bounded_error(detail);
        if let Some(existing) = guard.error_detail.as_mut() {
            append_bounded_cleanup_note(existing, detail);
        } else {
            guard.error_detail = Some(detail);
        }
        if guard.error_kind.is_none() {
            guard.error_kind = Some(
                if guard.phase == BackupPhase::Cancelled
                    || kind == BackupErrorKind::Cancelled
                    || cancellation_requested(control)
                {
                    BackupErrorKind::Cancelled
                } else {
                    kind
                },
            );
        }
        return;
    }

    let mut detail = bounded_error(detail);
    if let Some(prior) = guard.error_detail.take() {
        detail = merge_cleanup_into_detail(detail, prior);
    }

    let effective_kind = if kind == BackupErrorKind::Cancelled || cancellation_requested(control) {
        BackupErrorKind::Cancelled
    } else {
        kind
    };

    if effective_kind == BackupErrorKind::Cancelled {
        guard.phase = BackupPhase::Cancelled;
    } else {
        guard.phase = BackupPhase::Failed;
    }
    guard.error_kind = Some(effective_kind);
    guard.error_detail = Some(detail);
}

fn bounded_error(message: String) -> String {
    if message.len() <= MAX_STATUS_ERROR_CHARS {
        return message;
    }
    let mut end = MAX_STATUS_ERROR_CHARS;
    while end > 0 && !message.is_char_boundary(end) {
        end -= 1;
    }
    message[..end].to_string()
}
