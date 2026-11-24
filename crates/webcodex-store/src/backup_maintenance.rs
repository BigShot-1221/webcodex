//! Production registry for at-most-one online backup worker (K.2c maintenance owner).
//!
//! Holds the sole [`BackupTask`] join handle so callers cannot drop observation/cancel handles while
//! the worker keeps running. Shutdown uses bounded wait before join.

use super::{
    BackupArtifact, BackupControl, BackupErrorKind, BackupOptions, BackupPhase,
    BackupStatusSnapshot, Database,
};
use crate::backup::BackupTask;
use crate::server_instance::ServerInstanceGuard;
use anyhow::{anyhow, Context};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tracing;

struct OwnerState {
    shutdown_requested: bool,
    task: Option<BackupTask>,
}

/// Process-lifetime owner for online backup workers on one production [`Database`] instance.
pub struct BackupMaintenanceOwner {
    db: Arc<Database>,
    instance_owner: Arc<ServerInstanceGuard>,
    state: Mutex<OwnerState>,
}

impl BackupMaintenanceOwner {
    /// Construct an owner for `db` when `instance_owner` matches ([`ServerInstanceGuard::owns_database`]).
    pub fn new(db: Arc<Database>, instance_owner: Arc<ServerInstanceGuard>) -> anyhow::Result<Self> {
        if !instance_owner.owns_database(db.as_ref()) {
            anyhow::bail!("instance lock does not match database state path");
        }
        Ok(Self {
            db,
            instance_owner,
            state: Mutex::new(OwnerState {
                shutdown_requested: false,
                task: None,
            }),
        })
    }

    /// Clone the active worker's [`BackupControl`] when a task is registered (finished or in-flight).
    pub fn active_control(&self) -> anyhow::Result<Option<BackupControl>> {
        let guard = self.lock_state()?;
        Ok(guard.task.as_ref().map(|task| task.control()))
    }

    /// Start one online backup when admission is open and no other worker is in flight.
    ///
    /// Returns [`BackupControl`] only; the [`BackupTask`] join handle remains in this owner.
    pub fn start_online_backup(&self, options: BackupOptions) -> anyhow::Result<BackupControl> {
        let mut guard = self.lock_state()?;
        if guard.shutdown_requested {
            anyhow::bail!("backup maintenance owner shut down");
        }
        if let Some(ref task) = guard.task {
            if !task.is_finished() {
                anyhow::bail!("online backup already in progress");
            }
            let finished = guard.task.take().expect("finished task slot");
            let snapshot = finished.snapshot();
            let shutdown_was_requested = guard.shutdown_requested;
            observe_worker_join(snapshot, finished.join(), shutdown_was_requested);
            if guard.shutdown_requested {
                anyhow::bail!("backup maintenance owner shut down");
            }
        }

        let task = self
            .db
            .start_online_backup(Arc::clone(&self.instance_owner), options)
            .context("spawn online backup worker")?;

        if guard.shutdown_requested {
            let _ = task.cancel();
            guard.task = Some(task);
            anyhow::bail!("backup maintenance owner shut down");
        }
        let control = task.control();
        guard.task = Some(task);
        Ok(control)
    }

    /// Close future admission and cooperatively cancel any active worker. Idempotent.
    pub fn request_shutdown(&self) -> anyhow::Result<()> {
        let mut guard = self.lock_state()?;
        request_shutdown_locked(&mut guard);
        Ok(())
    }

    /// Close admission, cancel the active worker, and join it within `timeout`.
    ///
    /// On timeout the in-flight [`BackupTask`] stays registered so the join handle is not lost.
    pub fn shutdown_and_drain(&self, timeout: Duration) -> anyhow::Result<()> {
        {
            let mut guard = self.lock_state()?;
            request_shutdown_locked(&mut guard);
        }

        if !self.wait_for_active_task_finished(timeout)? {
            return Err(anyhow!(
                "backup maintenance drain timed out after {:?}; worker still running",
                timeout
            ));
        }

        let task = {
            let mut guard = self.lock_state()?;
            let Some(task) = guard.task.take() else {
                return Ok(());
            };
            if !task.is_finished() {
                guard.task = Some(task);
                return Err(anyhow!(
                    "backup drain internal error: task not finished before join"
                ));
            }
            task
        };

        let shutdown_was_requested = self
            .lock_state()
            .map(|g| g.shutdown_requested)
            .unwrap_or(true);
        let snapshot = task.snapshot();
        observe_worker_join(snapshot, task.join(), shutdown_was_requested);
        Ok(())
    }
}

impl BackupMaintenanceOwner {
    fn lock_state(&self) -> anyhow::Result<MutexGuard<'_, OwnerState>> {
        self.state
            .lock()
            .map_err(|_| anyhow!("backup maintenance registry mutex poisoned"))
    }

    /// Poll [`BackupTask::is_finished`] with the registry mutex released between waits.
    fn wait_for_active_task_finished(&self, timeout: Duration) -> anyhow::Result<bool> {
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            return Ok(matches!(
                self.task_finished_snapshot()?,
                TaskPresence::Absent | TaskPresence::Finished
            ));
        };
        const POLL_CAP: Duration = Duration::from_millis(50);
        loop {
            match self.task_finished_snapshot()? {
                TaskPresence::Absent => return Ok(true),
                TaskPresence::Finished => return Ok(true),
                TaskPresence::InFlight => {}
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(false);
            }
            std::thread::park_timeout(remaining.min(POLL_CAP));
        }
    }

    fn task_finished_snapshot(&self) -> anyhow::Result<TaskPresence> {
        let guard = self.lock_state()?;
        Ok(match guard.task.as_ref() {
            None => TaskPresence::Absent,
            Some(task) if task.is_finished() => TaskPresence::Finished,
            Some(_) => TaskPresence::InFlight,
        })
    }
}

enum TaskPresence {
    Absent,
    Finished,
    InFlight,
}

fn request_shutdown_locked(guard: &mut OwnerState) {
    guard.shutdown_requested = true;
    if let Some(task) = guard.task.as_ref() {
        let _ = task.cancel();
    }
}

fn observe_worker_join(
    snapshot: BackupStatusSnapshot,
    outcome: anyhow::Result<BackupArtifact>,
    shutdown_was_requested: bool,
) {
    if let Some(detail) = snapshot.error_detail.as_deref() {
        if snapshot.error_kind == Some(BackupErrorKind::Cleanup) {
            tracing::info!(detail = %detail, "online backup cleanup detail");
        }
    }

    match outcome {
        Ok(artifact) => {
            tracing::info!(
                path = %artifact.path.display(),
                created_at_unix_ms = artifact.created_at_unix_ms,
                phase = ?snapshot.phase,
                "online backup worker joined successfully"
            );
        }
        Err(error) => {
            let cancelled = snapshot.phase == BackupPhase::Cancelled
                || snapshot.error_kind == Some(BackupErrorKind::Cancelled);
            if shutdown_was_requested && cancelled {
                tracing::info!(
                    error = %error,
                    phase = ?snapshot.phase,
                    "online backup cancelled during maintenance shutdown"
                );
            } else {
                tracing::warn!(
                    error = %error,
                    phase = ?snapshot.phase,
                    ?snapshot.error_kind,
                    "online backup worker finished with error"
                );
            }
        }
    }
}