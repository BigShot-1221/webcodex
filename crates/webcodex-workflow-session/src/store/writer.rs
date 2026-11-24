//! Background full-ledger persistence. Ordinary dirty notifications coalesce
//! behind one cost-aware fixed deadline; explicit flush and shutdown bypass it.
//! Transient flush failures requeue with bounded exponential backoff on the
//! shared condition variable. During ordinary operation,
//! [`MAX_CONSECUTIVE_TRANSIENT_FAILURES`] caps consecutive transient attempts
//! (independent of shutdown's [`SHUTDOWN_TRANSIENT_RETRY_BUDGET`]); exhaustion
//! enters terminal writer failure instead of unbounded retry. Shutdown performs
//! a capped immediate retry budget that does not advance the consecutive counter.
//! This changes write scheduling only, not ledger format or durable identity.
use super::SessionStoreInner;
use serde::Serialize;

mod flush;
pub(super) use flush::{
    durable_write_uses_sqlite, flush_json_ledger, flush_sqlite_batch,
    flush_sqlite_batch_shutdown_drain, flush_sqlite_batch_with_budget, ledger_io_error_outcome,
    record_persist_outcome, PersistFlushDisposition, PersistFlushOutcome, SqliteFlushAttemptBudget,
};
use std::io;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, watch};

const ORDINARY_COALESCE: Duration = Duration::from_millis(20);
const LARGE_LEDGER_BYTES: u64 = 4 * 1024 * 1024;
const MAX_COST_COALESCE: Duration = Duration::from_secs(1);
const TRANSIENT_BACKOFF_BASE: Duration = Duration::from_millis(50);
const TRANSIENT_BACKOFF_MAX: Duration = Duration::from_secs(2);
const TRANSIENT_BACKOFF_SHIFT_CAP: u32 = 6;
/// Ordinary-operation cap on consecutive transient flush failures. With the
/// session database `busy_timeout` (5s), six attempts bound worst-case SQLite
/// lock wait plus capped backoff before terminal writer failure. Independent
/// from [`SHUTDOWN_TRANSIENT_RETRY_BUDGET`], which only applies after shutdown.
pub(super) const MAX_CONSECUTIVE_TRANSIENT_FAILURES: u32 = 6;
const SHUTDOWN_TRANSIENT_RETRY_BUDGET: u32 = 2;

pub(super) fn transient_backoff(consecutive_failures: u32) -> Duration {
    let shift = consecutive_failures.min(TRANSIENT_BACKOFF_SHIFT_CAP);
    TRANSIENT_BACKOFF_BASE
        .saturating_mul(1u32 << shift)
        .min(TRANSIENT_BACKOFF_MAX)
}

fn cost_coalesce(bytes: u64, duration: Duration) -> Duration {
    if bytes < LARGE_LEDGER_BYTES {
        return ORDINARY_COALESCE;
    }
    duration
        .saturating_mul(4)
        .max(Duration::from_millis(250))
        .min(MAX_COST_COALESCE)
}

pub(super) struct LedgerWriterGuard {
    shared: Arc<LedgerWriterShared>,
    join: Mutex<Option<std::thread::JoinHandle<()>>>,
    persistence_fatal: watch::Sender<Option<String>>,
}

struct LedgerWriterShared {
    state: Mutex<LedgerWriterState>,
    cvar: Condvar,
    flush_notify: Notify,
    #[cfg(test)]
    pre_completion_hold: Mutex<Option<Arc<(Mutex<bool>, Condvar)>>>,
    #[cfg(test)]
    flush_wait_registered: Mutex<Option<std::sync::mpsc::Sender<()>>>,
}

#[derive(Default)]
struct LedgerWriterState {
    // First pending mark owns the deadline; later progress never extends it.
    dirty_since: Option<Instant>,
    // Captured by the first mark; later work and cost updates cannot extend it.
    dirty_cost: Duration,
    next_cost: Duration,

    dirty_generation: u64,
    // A completed cycle is an attempted write, not necessarily a successful one.
    // SessionPersistence.last_persist_error remains the success/error authority.
    writes_completed: u64,
    flush_generation: u64,
    shutdown: bool,
    write_attempts: u64,
    failed_attempts: u64,
    last_attempt: Option<WriteAttemptObservation>,
    consecutive_transient_failures: u32,
    retry_not_before: Option<Instant>,
    shutdown_transient_retries_remaining: u32,
    /// Terminal writer failure; no further flush attempts or transient backoff.
    writer_fatal: Option<String>,
    /// Prior SQLite attempt committed bounded chunks but more units remain (J.7).
    flush_backlog: bool,
    #[cfg(test)]
    write_cycles: usize,
}

impl LedgerWriterState {
    fn mark_dirty(&mut self, now: Instant) -> u64 {
        if self.dirty_since.is_none() {
            self.dirty_since = Some(now);
            self.dirty_cost = self.next_cost;
        }
        self.dirty_generation = self.dirty_generation.saturating_add(1);
        self.dirty_generation
    }

    fn bypasses_write_backoff(&self) -> bool {
        self.shutdown || self.flush_generation > self.writes_completed || self.flush_backlog
    }

    fn coalescing_wait(&self, now: Instant, window: Duration) -> Option<Duration> {
        if self.shutdown || self.flush_generation > self.writes_completed || self.flush_backlog {
            return None;
        }
        self.dirty_since.and_then(|since| {
            window
                .max(self.dirty_cost)
                .checked_sub(now.saturating_duration_since(since))
                .filter(|remaining| !remaining.is_zero())
        })
    }

    /// Requeue a failed transient write without clobbering a newer `dirty_since`
    /// established while the prior I/O was running.
    ///
    /// Returns `true` when ordinary-operation consecutive transient failures
    /// reached [`MAX_CONSECUTIVE_TRANSIENT_FAILURES`]; the caller must enter
    /// terminal writer failure. Shutdown retries use
    /// [`shutdown_transient_retries_remaining`] only and never advance
    /// `consecutive_transient_failures`.
    fn schedule_transient_retry(&mut self, now: Instant) -> bool {
        if self.dirty_since.is_none() {
            self.dirty_since = Some(now);
            self.dirty_cost = self.next_cost;
        }
        if self.shutdown {
            if self.shutdown_transient_retries_remaining > 0 {
                self.shutdown_transient_retries_remaining =
                    self.shutdown_transient_retries_remaining.saturating_sub(1);
                self.retry_not_before = None;
            } else {
                self.dirty_since = None;
            }
            return false;
        }
        let backoff = transient_backoff(self.consecutive_transient_failures);
        self.consecutive_transient_failures =
            self.consecutive_transient_failures.saturating_add(1);
        if self.consecutive_transient_failures >= MAX_CONSECUTIVE_TRANSIENT_FAILURES {
            return true;
        }
        self.retry_not_before = Some(now + backoff);
        false
    }

    fn clear_transient_retry_state(&mut self) {
        self.consecutive_transient_failures = 0;
        self.retry_not_before = None;
    }
}

impl std::fmt::Debug for LedgerWriterGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LedgerWriterGuard").finish_non_exhaustive()
    }
}

fn signal_persistence_fatal_once(
    persistence_fatal: &watch::Sender<Option<String>>,
    message: &str,
) {
    super::SessionStore::signal_persistence_fatal_sender(persistence_fatal, message);
}

const WRITER_THREAD_PANIC_SUMMARY: &str = "session ledger writer thread panicked";

fn join_writer_thread_after_poison_aware_take(
    persistence_fatal: &watch::Sender<Option<String>>,
    join: &Mutex<Option<std::thread::JoinHandle<()>>>,
) -> io::Result<()> {
    match join.lock() {
        Ok(mut guard) => {
            if let Some(handle) = guard.take() {
                handle.join().map_err(|_| {
                    io::Error::other(WRITER_THREAD_PANIC_SUMMARY)
                })?;
            }
            Ok(())
        }
        Err(poison) => {
            signal_persistence_fatal_once(
                persistence_fatal,
                "session ledger writer join mutex poisoned",
            );
            let mut guard = poison.into_inner();
            if let Some(handle) = guard.take() {
                let _ = handle.join();
            }
            Err(io::Error::other(
                "session ledger writer join mutex poisoned",
            ))
        }
    }
}

impl LedgerWriterGuard {
    pub(super) fn spawn(
        store_inner: Arc<Mutex<SessionStoreInner>>,
        write_mutex: Arc<Mutex<()>>,
        persistence_fatal: watch::Sender<Option<String>>,
    ) -> Option<Arc<Self>> {
        Self::spawn_with_policy_fatal(
            store_inner,
            write_mutex,
            ORDINARY_COALESCE,
            true,
            persistence_fatal,
        )
    }

    // The window is private policy, not a deployment knob. Tests inject a long
    // window to prove flush/drop bypass without short timing assertions.
    #[cfg(any(test, feature = "root-test-support"))]
    pub(super) fn spawn_with_window(
        store_inner: Arc<Mutex<SessionStoreInner>>,
        write_mutex: Arc<Mutex<()>>,
        window: Duration,
    ) -> Option<Arc<Self>> {
        let (persistence_fatal, _) = watch::channel(None);
        Self::spawn_with_policy_fatal(
            store_inner,
            write_mutex,
            window,
            true,
            persistence_fatal,
        )
    }

    #[cfg(any(test, feature = "root-test-support"))]
    pub(super) fn spawn_with_policy(
        store_inner: Arc<Mutex<SessionStoreInner>>,
        write_mutex: Arc<Mutex<()>>,
        window: Duration,
        cost_aware: bool,
    ) -> Option<Arc<Self>> {
        let (persistence_fatal, _) = watch::channel(None);
        Self::spawn_with_policy_fatal(
            store_inner,
            write_mutex,
            window,
            cost_aware,
            persistence_fatal,
        )
    }

    // Private comparison seam: production always enables cost-aware scheduling.
    fn spawn_with_policy_fatal(
        store_inner: Arc<Mutex<SessionStoreInner>>,
        write_mutex: Arc<Mutex<()>>,
        window: Duration,
        cost_aware: bool,
        persistence_fatal: watch::Sender<Option<String>>,
    ) -> Option<Arc<Self>> {
        let bytes = match store_inner.lock() {
            Ok(inner) => inner
                .persistence
                .as_ref()
                .map(|persistence| persistence.cost_observation_path())
                .and_then(|path| std::fs::metadata(path).ok())
                .map_or(0, |meta| meta.len()),
            Err(_) => {
                signal_persistence_fatal_once(
                    &persistence_fatal,
                    "session store mutex poisoned",
                );
                return None;
            }
        };
        let shared = Arc::new(LedgerWriterShared {
            state: Mutex::new(LedgerWriterState {
                next_cost: if cost_aware {
                    cost_coalesce(bytes, Duration::ZERO)
                } else {
                    Duration::ZERO
                },
                ..Default::default()
            }),
            cvar: Condvar::new(),
            flush_notify: Notify::new(),
            #[cfg(test)]
            pre_completion_hold: Mutex::new(None),
            #[cfg(test)]
            flush_wait_registered: Mutex::new(None),
        });
        let shared_thread = Arc::clone(&shared);
        let persistence_fatal_thread = persistence_fatal.clone();
        let join = std::thread::Builder::new()
            .name("session-ledger-writer".to_string())
            .spawn(move || {
                let loop_result = catch_unwind(AssertUnwindSafe(|| {
                    ledger_writer_loop(
                        Arc::clone(&shared_thread),
                        store_inner,
                        write_mutex,
                        window,
                        cost_aware,
                        persistence_fatal_thread.clone(),
                    );
                }));
                if loop_result.is_err() {
                    if let Ok(mut state) = shared_thread.state.lock() {
                        enter_writer_fatal(
                            &shared_thread,
                            &persistence_fatal_thread,
                            &mut state,
                            WRITER_THREAD_PANIC_SUMMARY.to_string(),
                        );
                    } else {
                        signal_persistence_fatal_once(
                            &persistence_fatal_thread,
                            WRITER_THREAD_PANIC_SUMMARY,
                        );
                        shared_thread.cvar.notify_all();
                        shared_thread.flush_notify.notify_waiters();
                    }
                }
            })
            .ok()?;
        Some(Arc::new(Self {
            shared,
            join: Mutex::new(Some(join)),
            persistence_fatal,
        }))
    }

    /// True when new dirty marks must be rejected (terminal failure or shutdown drain).
    pub(super) fn refuses_new_dirty_marks(&self) -> bool {
        match self.shared.state.lock() {
            Ok(state) => state.writer_fatal.is_some() || state.shutdown,
            Err(_) => {
                signal_persistence_fatal_once(
                    &self.persistence_fatal,
                    "session ledger writer state mutex poisoned",
                );
                true
            }
        }
    }

    fn writer_thread_joined(&self) -> bool {
        match self.join.lock() {
            Ok(guard) => guard.is_none(),
            Err(_) => {
                signal_persistence_fatal_once(
                    &self.persistence_fatal,
                    "session ledger writer join mutex poisoned",
                );
                true
            }
        }
    }

    fn flush_generation_impossible(state: &LedgerWriterState, generation: u64) -> bool {
        state.writer_fatal.is_some()
            || (state.shutdown
                && state.dirty_since.is_none()
                && !state.flush_backlog
                && state.writes_completed < generation)
            || (state.dirty_generation < generation
                && state.dirty_since.is_none()
                && !state.flush_backlog
                && state.writes_completed < generation)
    }

    pub(super) fn mark_dirty(&self) -> Result<u64, ()> {
        let generation = match self.shared.state.lock() {
            Ok(mut state) => {
                if state.writer_fatal.is_some() || state.shutdown {
                    return Err(());
                }
                state.mark_dirty(Instant::now())
            }
            Err(_) => {
                signal_persistence_fatal_once(
                    &self.persistence_fatal,
                    "session ledger writer state mutex poisoned",
                );
                return Err(());
            }
        };
        // The same condition variable has writer and flush waiters. notify_one
        // can wake a flusher instead of the only thread able to perform I/O.
        self.shared.cvar.notify_all();
        Ok(generation)
    }

    /// Wait for the exact requested generation, not later concurrent mutations.
    /// Returns `Err(())` when the writer entered a terminal failure state.
    pub(super) async fn flush_through(&self, generation: u64) -> Result<(), ()> {
        loop {
            let mut notified = std::pin::pin!(self.shared.flush_notify.notified());
            notified.as_mut().enable();
            let (done, fatal) = {
                let mut state = match self.shared.state.lock() {
                    Ok(state) => state,
                    Err(_) => {
                        signal_persistence_fatal_once(
                            &self.persistence_fatal,
                            "session ledger writer state mutex poisoned",
                        );
                        return Err(());
                    }
                };
                state.flush_generation = state.flush_generation.max(generation);
                self.shared.cvar.notify_all();
                let impossible = Self::flush_generation_impossible(&state, generation);
                (
                    state.writes_completed >= generation,
                    state.writer_fatal.is_some() || impossible,
                )
            };
            if fatal {
                self.shared.flush_notify.notify_waiters();
                return Err(());
            }
            if done {
                return Ok(());
            }
            if self.writer_thread_joined() {
                let impossible = match self.shared.state.lock() {
                    Ok(state) => {
                        Self::flush_generation_impossible(&state, generation)
                            || state.writes_completed < generation
                    }
                    Err(_) => {
                        signal_persistence_fatal_once(
                            &self.persistence_fatal,
                            "session ledger writer state mutex poisoned",
                        );
                        true
                    }
                };
                if impossible {
                    self.shared.flush_notify.notify_waiters();
                    return Err(());
                }
            }
            #[cfg(test)]
            if let Some(tx) = self
                .shared
                .flush_wait_registered
                .lock()
                .unwrap()
                .take()
            {
                let _ = tx.send(());
            }
            notified.await;
        }
    }

    /// Terminal failure from a synchronous flush that raced with or bypassed the
    /// writer loop. Idempotent; stops further background flush attempts.
    pub(super) fn enter_terminal_flush_failure(&self, message: impl Into<String>) {
        let message = message.into();
        match self.shared.state.lock() {
            Ok(mut state) => {
                enter_writer_fatal(&self.shared, &self.persistence_fatal, &mut state, message);
            }
            Err(_) => {
                signal_persistence_fatal_once(
                    &self.persistence_fatal,
                    "session ledger writer state mutex poisoned",
                );
                self.shared.cvar.notify_all();
                self.shared.flush_notify.notify_waiters();
            }
        }
    }

    /// Request bounded shutdown drain; idempotent after join handle is taken.
    pub(super) fn request_shutdown(&self) {
        match self.shared.state.lock() {
            Ok(mut state) => {
                state.shutdown = true;
                state.shutdown_transient_retries_remaining = state
                    .shutdown_transient_retries_remaining
                    .max(SHUTDOWN_TRANSIENT_RETRY_BUDGET);
                self.shared.cvar.notify_all();
            }
            Err(_) => {
                signal_persistence_fatal_once(
                    &self.persistence_fatal,
                    "session ledger writer state mutex poisoned",
                );
                self.shared.cvar.notify_all();
                self.shared.flush_notify.notify_waiters();
            }
        }
    }

    /// Join the writer thread after `request_shutdown`. Returns flush/terminal failure.
    pub(super) fn join_after_shutdown(&self) -> io::Result<()> {
        self.request_shutdown();
        join_writer_thread_after_poison_aware_take(&self.persistence_fatal, &self.join)?;
        match self.shared.state.lock() {
            Ok(state) => {
                if let Some(message) = state.writer_fatal.clone() {
                    return Err(io::Error::other(message));
                }
                if state.shutdown
                    && state
                        .last_attempt
                        .as_ref()
                        .is_some_and(|attempt| !attempt.success)
                {
                    return Err(io::Error::other(
                        "session persistence shutdown completed with flush failure",
                    ));
                }
                Ok(())
            }
            Err(_) => {
                signal_persistence_fatal_once(
                    &self.persistence_fatal,
                    "session ledger writer state mutex poisoned",
                );
                Err(io::Error::other(
                    "session ledger writer state mutex poisoned",
                ))
            }
        }
    }

    /// Blocking flush for synchronous fallback, tests, and root-test-support.
    #[cfg(any(test, feature = "root-test-support"))]
    pub(super) fn flush_through_blocking(&self, generation: u64) -> Result<(), ()> {
        let mut state = match self.shared.state.lock() {
            Ok(state) => state,
            Err(_) => {
                signal_persistence_fatal_once(
                    &self.persistence_fatal,
                    "session ledger writer state mutex poisoned",
                );
                return Err(());
            }
        };
        if Self::flush_generation_impossible(&state, generation) {
            return Err(());
        }
        state.flush_generation = state.flush_generation.max(generation);
        self.shared.cvar.notify_all();
        while state.writes_completed < generation {
            if Self::flush_generation_impossible(&state, generation) || self.writer_thread_joined() {
                return Err(());
            }
            state = match self.shared.cvar.wait(state) {
                Ok(state) => state,
                Err(_) => {
                    signal_persistence_fatal_once(
                        &self.persistence_fatal,
                        "session ledger writer state mutex poisoned",
                    );
                    return Err(());
                }
            };
        }
        Ok(())
    }

    #[cfg(any(test, feature = "root-test-support"))]
    pub(super) fn flush(&self) {
        let generation = match self.shared.state.lock() {
            Ok(state) => state.dirty_generation,
            Err(_) => {
                signal_persistence_fatal_once(
                    &self.persistence_fatal,
                    "session ledger writer state mutex poisoned",
                );
                return;
            }
        };
        let _ = self.flush_through_blocking(generation);
    }

    #[cfg(test)]
    pub(super) fn write_cycles(&self) -> usize {
        self.shared.state.lock().unwrap().write_cycles
    }

    #[cfg(test)]
    pub(super) fn install_pre_completion_hold(&self, hold: Arc<(Mutex<bool>, Condvar)>) {
        *self.shared.pre_completion_hold.lock().unwrap() = Some(hold);
    }

    #[cfg(test)]
    pub(super) fn set_flush_wait_registered_sender(&self, tx: std::sync::mpsc::Sender<()>) {
        *self.shared.flush_wait_registered.lock().unwrap() = Some(tx);
    }
}

impl Drop for LedgerWriterGuard {
    fn drop(&mut self) {
        self.request_shutdown();
        let _ = join_writer_thread_after_poison_aware_take(&self.persistence_fatal, &self.join);
    }
}

fn enter_writer_fatal(
    shared: &LedgerWriterShared,
    persistence_fatal: &watch::Sender<Option<String>>,
    state: &mut LedgerWriterState,
    message: String,
) {
    if state.writer_fatal.is_some() {
        return;
    }
    state.writer_fatal = Some(message.clone());
    state.dirty_since = None;
    state.flush_backlog = false;
    state.clear_transient_retry_state();
    signal_persistence_fatal_once(persistence_fatal, &message);
    shared.cvar.notify_all();
    shared.flush_notify.notify_waiters();
}

fn ledger_writer_loop(
    shared: Arc<LedgerWriterShared>,
    store_inner: Arc<Mutex<SessionStoreInner>>,
    write_mutex: Arc<Mutex<()>>,
    window: Duration,
    cost_aware: bool,
    persistence_fatal: watch::Sender<Option<String>>,
) {
    loop {
        let generation = {
            let mut state = match shared.state.lock() {
                Ok(state) => state,
                Err(_) => {
                    signal_persistence_fatal_once(
                        &persistence_fatal,
                        "session ledger writer state mutex poisoned",
                    );
                    shared.cvar.notify_all();
                    shared.flush_notify.notify_waiters();
                    break;
                }
            };
            if state.writer_fatal.is_some() {
                break;
            }
            while state.dirty_since.is_none()
                && !state.shutdown
                && !state.flush_backlog
                && state.writer_fatal.is_none()
            {
                state = match shared.cvar.wait(state) {
                    Ok(state) => state,
                    Err(_) => {
                        signal_persistence_fatal_once(
                            &persistence_fatal,
                            "session ledger writer state mutex poisoned",
                        );
                        shared.cvar.notify_all();
                        shared.flush_notify.notify_waiters();
                        return;
                    }
                };
            }
            if (state.dirty_since.is_none() && !state.flush_backlog) || state.writer_fatal.is_some() {
                break;
            }
            loop {
                if state.bypasses_write_backoff() {
                    break;
                }
                let now = Instant::now();
                match state.retry_not_before {
                    Some(not_before) if now < not_before => {
                        (state, _) = match shared.cvar.wait_timeout(state, not_before - now) {
                            Ok(waited) => waited,
                            Err(_) => {
                                signal_persistence_fatal_once(
                                    &persistence_fatal,
                                    "session ledger writer state mutex poisoned",
                                );
                                shared.cvar.notify_all();
                                shared.flush_notify.notify_waiters();
                                return;
                            }
                        };
                    }
                    _ => break,
                }
            }
            while let Some(remaining) = state.coalescing_wait(Instant::now(), window) {
                (state, _) = match shared.cvar.wait_timeout(state, remaining) {
                    Ok(waited) => waited,
                    Err(_) => {
                        signal_persistence_fatal_once(
                            &persistence_fatal,
                            "session ledger writer state mutex poisoned",
                        );
                        shared.cvar.notify_all();
                        shared.flush_notify.notify_waiters();
                        return;
                    }
                };
            }
            let generation = state.dirty_generation;
            state.dirty_since = None;
            generation
        };

        // Preserve the shared write lock and snapshot order: no old background
        // write may race past a synchronous write or durable commit barrier.
        let _write_guard = match write_mutex.lock() {
            Ok(guard) => guard,
            Err(_) => {
                let message =
                    "session persistence mutex poisoned".to_string();
                if let Ok(mut state) = shared.state.lock() {
                    enter_writer_fatal(&shared, &persistence_fatal, &mut state, message);
                } else {
                    signal_persistence_fatal_once(
                        &persistence_fatal,
                        "session persistence mutex poisoned",
                    );
                }
                break;
            }
        };
        let database = store_inner
            .lock()
            .ok()
            .and_then(|inner| {
                if !durable_write_uses_sqlite(&inner) {
                    return None;
                }
                inner
                    .persistence
                    .as_ref()
                    .and_then(|persistence| persistence.database.clone())
            });
        let write_started = Instant::now();
        let sqlite_flush = database.is_some();
        let shutdown_drain = match shared.state.lock() {
            Ok(state) => state.shutdown,
            Err(_) => true,
        };
        let (lock_wait, lock_hold, rows, flush_outcome, json_written_bytes) =
            if let Some(database) = database {
                let budget = if shutdown_drain {
                    SqliteFlushAttemptBudget::shutdown_drain()
                } else {
                    SqliteFlushAttemptBudget::ordinary_attempt()
                };
                let (metrics, outcome) =
                    flush_sqlite_batch_with_budget(&store_inner, database.as_ref(), budget);
                (
                    metrics.lock_wait,
                    metrics.lock_hold,
                    metrics.rows,
                    outcome,
                    None,
                )
            } else {
                let (metrics, outcome) = flush_json_ledger(&store_inner);
                let written_bytes = outcome.written_bytes;
                (
                    metrics.lock_wait,
                    metrics.lock_hold,
                    metrics.rows,
                    outcome,
                    written_bytes,
                )
            };
        tracing::debug!(target: "webcodex::session_cost", phase="snapshot",
            wait_ms=lock_wait.as_secs_f64()*1000.0, hold_ms=lock_hold.as_secs_f64()*1000.0,
            rows,
            "session snapshot lock cost");
        let write_duration = write_started.elapsed();
        if record_persist_outcome(&store_inner, &flush_outcome).is_err() {
            signal_persistence_fatal_once(
                &persistence_fatal,
                "session store mutex poisoned",
            );
        }
        #[cfg(test)]
        if let Some(hold) = shared.pre_completion_hold.lock().unwrap().as_ref() {
            let mut blocked = hold.0.lock().unwrap();
            while *blocked {
                blocked = hold.1.wait(blocked).unwrap();
            }
        }
        {
            let mut state = match shared.state.lock() {
                Ok(state) => state,
                Err(_) => {
                    signal_persistence_fatal_once(
                        &persistence_fatal,
                        "session ledger writer state mutex poisoned",
                    );
                    shared.cvar.notify_all();
                    shared.flush_notify.notify_waiters();
                    break;
                }
            };
            state.write_attempts = state.write_attempts.saturating_add(1);
            let persist_success =
                flush_outcome.disposition == PersistFlushDisposition::Success;
            let allocation_only = persist_success
                && flush_outcome.allocation_error.is_some();
            if !persist_success || allocation_only {
                state.failed_attempts = state.failed_attempts.saturating_add(1);
            }
            state.last_attempt = Some(WriteAttemptObservation {
                success: persist_success && !allocation_only,
                written_bytes: if sqlite_flush {
                    None
                } else {
                    json_written_bytes
                },
                write_ms: write_duration.as_secs_f64() * 1000.0,
                snapshot_lock_wait_ms: lock_wait.as_secs_f64() * 1000.0,
                snapshot_lock_hold_ms: lock_hold.as_secs_f64() * 1000.0,
                dirty_notifications: generation.saturating_sub(state.writes_completed),
            });
            if persist_success {
                state.flush_backlog = flush_outcome.more_pending;
                if flush_outcome.more_pending {
                    if state.dirty_since.is_none() {
                        state.dirty_since = Some(Instant::now());
                    }
                } else {
                    state.writes_completed = generation;
                    state.flush_backlog = false;
                }
            } else {
                state.flush_backlog = flush_outcome.more_pending;
                if state.flush_backlog && state.dirty_since.is_none() {
                    state.dirty_since = Some(Instant::now());
                }
            }
            match flush_outcome.disposition {
                PersistFlushDisposition::Success => {
                    state.clear_transient_retry_state();
                }
                PersistFlushDisposition::TransientRetry => {
                    if state.schedule_transient_retry(Instant::now()) {
                        let message = flush_outcome
                            .error_summary
                            .clone()
                            .map(|summary| {
                                format!(
                                    "session persistence transient retry budget exhausted: {summary}"
                                )
                            })
                            .unwrap_or_else(|| {
                                "session persistence transient retry budget exhausted".to_string()
                            });
                        enter_writer_fatal(&shared, &persistence_fatal, &mut state, message);
                    }
                }
                PersistFlushDisposition::PermanentHealth => {
                    state.clear_transient_retry_state();
                }
                PersistFlushDisposition::CommittedButUnacked | PersistFlushDisposition::WriterFatal => {
                    let message = flush_outcome
                        .error_summary
                        .clone()
                        .unwrap_or_else(|| "session persistence writer fatal".to_string());
                    enter_writer_fatal(&shared, &persistence_fatal, &mut state, message);
                }
            }
            if cost_aware && persist_success {
                let bytes = if sqlite_flush {
                    match store_inner.lock() {
                        Ok(inner) => inner
                            .persistence
                            .as_ref()
                            .and_then(|persistence| {
                                std::fs::metadata(persistence.cost_observation_path()).ok()
                            })
                            .map_or(0, |metadata| metadata.len()),
                        Err(_) => {
                            signal_persistence_fatal_once(
                                &persistence_fatal,
                                "session store mutex poisoned",
                            );
                            0
                        }
                    }
                } else {
                    json_written_bytes.unwrap_or(0)
                };
                state.next_cost = cost_coalesce(bytes, write_duration);
            }

            #[cfg(test)]
            {
                state.write_cycles += 1;
            }
            shared.cvar.notify_all();
            shared.flush_notify.notify_waiters();
            if state.writer_fatal.is_some()
                || (state.shutdown && state.dirty_since.is_none() && !state.flush_backlog)
            {
                break;
            }
        }
        drop(_write_guard);
    }
}

/// Bounded diagnostics only; never consulted by write admission, flush or retry.
#[derive(Debug, Clone, Serialize)]
pub struct WriteAttemptObservation {
    pub success: bool,
    /// JSON ledger bytes on success; `None` on failure or SQLite durable writes
    /// (`apply_batch` commits revisions, not a byte count).
    pub written_bytes: Option<u64>,
    pub write_ms: f64,
    pub snapshot_lock_wait_ms: f64,
    pub snapshot_lock_hold_ms: f64,
    /// Coalesced dirty marks, not distinct Sessions or a business-mutation count.
    pub dirty_notifications: u64,
}

#[derive(Debug, Serialize)]
pub struct WriterObservation {
    pub write_attempts: u64,
    pub failed_attempts: u64,
    /// Dirty marks not yet attempted. Zero does not imply successful durability.
    pub pending_notifications: u64,
    pub last_attempt: Option<WriteAttemptObservation>,
}

#[derive(Debug, Serialize)]
pub struct PersistenceObservation {
    pub mode: &'static str,
    pub ledger_observation: &'static str,
    pub ledger_bytes: Option<u64>,
    pub writer: Option<WriterObservation>,
    /// Terminal persistence failure or the latest durable flush error summary.
    pub last_persist_error: Option<String>,
}

impl super::SessionStore {
    /// Explicit detailed-status observation. Clone only the configured path under
    /// the store lock, then stat outside it. No payload read, flush or dirty mark.
    /// Counters/path metadata are sequential observations, not a commit receipt.
    pub fn persistence_observation(&self) -> PersistenceObservation {
        let fatal = super::SessionStore::borrow_persistence_fatal(&self.persistence_fatal);
        let (path, inner_last_persist_error) = match self.try_lock_inner() {
            Some(inner) => (
                inner
                    .persistence
                    .as_ref()
                    .map(|state| state.cost_observation_path().to_path_buf()),
                inner
                    .persistence
                    .as_ref()
                    .and_then(|state| state.last_persist_error.clone()),
            ),
            None => {
                return PersistenceObservation {
                    mode: "unavailable",
                    ledger_observation: "unavailable",
                    ledger_bytes: None,
                    writer: None,
                    last_persist_error: fatal,
                };
            }
        };
        let last_persist_error = fatal.or(inner_last_persist_error);
        let persistence_unhealthy = last_persist_error.is_some();
        let mode = match (&path, &self.writer) {
            (None, _) => "memory",
            (Some(_), Some(_)) => "background",
            (Some(_), None) => "synchronous_fallback",
        };
        let ledger_bytes = path
            .as_ref()
            .and_then(|path| std::fs::metadata(path).ok())
            .filter(|metadata| metadata.is_file())
            .map(|metadata| metadata.len());
        let writer = self.writer.as_ref().and_then(|writer| {
            match writer.shared.state.lock() {
                Ok(state) => Some(WriterObservation {
                    write_attempts: state.write_attempts,
                    failed_attempts: state.failed_attempts,
                    pending_notifications: state
                        .dirty_generation
                        .saturating_sub(state.writes_completed),
                    last_attempt: state.last_attempt.clone(),
                }),
                Err(_) => {
                    signal_persistence_fatal_once(
                        &writer.persistence_fatal,
                        "session ledger writer state mutex poisoned",
                    );
                    None
                }
            }
        });
        PersistenceObservation {
            mode,
            ledger_observation: if path.is_none() {
                "disabled"
            } else if persistence_unhealthy {
                "unavailable"
            } else if ledger_bytes.is_some() {
                "available"
            } else {
                "unavailable"
            },
            ledger_bytes,
            writer,
            last_persist_error,
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod async_tests;

#[cfg(test)]
mod observation_tests;
