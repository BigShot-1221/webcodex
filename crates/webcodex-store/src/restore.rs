//! Offline restore for the shared Store database (K.2d).
//!
//! - Phases 2–3: **preflight** of backup candidates ([`OfflineRestoreOwner::preflight_candidate`]).
//! - Phase 4: byte-for-byte **original preservation** ([`OfflineRestoreOwner::preserve_original`]
//!   in `preserve` — rollback vs forensic `-shm` policy documented there).
//! - Phase 5: **candidate staging and production file swap** ([`OfflineRestoreOwner::restore_from_backup`]
//!   in `staging`, guarded by the durable restore journal in `journal`).
//! - Phase 6: **startup recovery gate** before `Database::open` and post-open **commit**
//!   ([`OfflineRestoreOwner::recover_startup_before_database_open`] and [`PendingRestoreStartup`] in `recovery`).
//!
//! There is **no** HTTP surface, public API handler, or scheduler job that triggers restore; the
//! host must stop production writers, acquire [`ServerInstanceGuard`](crate::server_instance::ServerInstanceGuard),
//! and invoke these offline entry points explicitly. The guard lock is **not** a system-wide drain of
//! every SQLite handle on the production files — external tools, stale backup workers, or an
//! unprepared shutdown may still hold handles until the operator stops them.
//!
//! # Authority and limits
//!
//! - [`OfflineRestoreOwner::acquire`] or [`OfflineRestoreOwner::from_shared_guard`] keeps an
//!   [`Arc`]`<`[`ServerInstanceGuard`](crate::server_instance::ServerInstanceGuard)`>` so this process
//!   does not race another WebCodex Server opening the same production DB path. Callers that already
//!   hold a guard must use [`from_shared_guard`] and must **not** call [`acquire`] for the same path.
//! - Without a bounded drain of all Store / Session / backup workers, **swap safety is not proven**.
//! - [`RestorePreflightFacts`] records what was true **at preflight time** only. It is **not** an
//!   immutable restore permit. The candidate file can change after preflight (**TOCTOU**); staging and
//!   swap re-validate immediately before replacing production data.
//! - SQLite preflight (`quick_check`, `foreign_key_check`, schema) has **no strict wall-clock bound**;
//!   it is cooperative read-only work only (no `progress_handler`). Large or pathological databases
//!   may keep preflight running until the host process is stopped externally.
//! - Journal, preservation, and staging publishes use platform-specific file durability where
//!   implemented; on Windows, **parent directory-entry** durability remains **best-effort** (no
//!   portable directory fsync in this stack).
//!
//! # Candidate location
//!
//! Only published online-backup artifacts are accepted: a **regular file** that is a **direct child**
//! of `data_dir/backups` (same layout as [`super::backup`] publish names `webcodex-<time>-<uuid>.db`).
//! Owner construction records the **expected** `data_dir/backups` path even when that directory does
//! not exist yet (fresh install); [`OfflineRestoreOwner::preflight_candidate`] hardens and requires
//! the backups directory to exist, avoid symlinks/reparse points, and be a canonical direct child of
//! `data_dir`.
//!
//! # Workflow Session domain policy
//!
//! K.2c [`crate::schema::validate_shared_database_backup_schema`] accepts a **tri-state** Workflow
//! Session layout: absent, exact personal Fork v1, or exact current v1. Both v1 formats are distinct.
//! Isolated worktrees that open the ledger with `with_session_ledger` may legally omit Session tables;
//! production Server startup on `main` uses `with_session_database`, so production restore through
//! [`OfflineRestoreOwner::restore_from_backup`] uses [`RestoreWorkflowSessionRequirement::RequirePresent`].
//! Preflight and staging success do **not** imply swap safety without the offline guard lease and
//! operator-prepared shutdown.

mod journal;
mod preserve;
mod recovery;
mod session_migration;
mod staging;

pub use recovery::PendingRestoreStartup;

pub(crate) use journal::{RestoreJournal, RestoreJournalPayload, RestorePhase};
pub(crate) use preserve::{
    OriginalPreservation, OriginalPreservationManifest, PreservedFileRecord, SourceFileIdentity,
};

use crate::server_instance::ServerInstanceGuard;
use anyhow::{bail, Context};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use std::ffi::OsString;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

const BACKUP_DIR_NAME: &str = "backups";
const PUBLISHED_BACKUP_PREFIX: &str = "webcodex-";
const PUBLISHED_BACKUP_EXTENSION: &str = ".db";
const SIDECAR_SUFFIXES: [&str; 3] = ["-wal", "-shm", "-journal"];

/// Process-lifetime offline restore lease on one production database path (preflight through commit).
///
/// Drop releases the server instance lock when this is the last [`Arc`] holder; do not use while a
/// production Server is running unless the host already holds the sole guard for startup recovery.
pub struct OfflineRestoreOwner {
    guard: Arc<ServerInstanceGuard>,
    production_db_path: PathBuf,
    data_dir: PathBuf,
    /// Trusted expected `data_dir/backups` path; hardened at preflight when the directory exists.
    backups_dir: PathBuf,
}

/// Caller-selected Workflow Session presence policy for restore preflight.
///
/// No default: every [`OfflineRestoreOwner::preflight_candidate`] invocation must choose explicitly.
/// [`OfflineRestoreOwner::restore_from_backup`] always uses [`RequirePresent`](Self::RequirePresent).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreWorkflowSessionRequirement {
    /// Accept K.2c Store-only backups (0/6 Workflow Session tables).
    AllowAbsent,
    /// Require the `workflow_session_storage` marker table after shared schema validation.
    RequirePresent,
}

/// Read-only outcomes observed during one successful [`OfflineRestoreOwner::preflight_candidate`].
///
/// **Not** sufficient to authorize swap without repeating validation on the staged bytes/path used
/// at swap time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestorePreflightFacts {
    /// Canonical candidate path at validation time (may change on disk afterward).
    pub candidate_path: PathBuf,
    pub file_size_bytes: u64,
    pub quick_check_ok: bool,
    pub foreign_key_check_empty: bool,
    pub shared_backup_schema_ok: bool,
    /// Objective read-only observation: `workflow_session_storage` existed in `sqlite_master`.
    pub workflow_session_domain_present: bool,
}

impl OfflineRestoreOwner {
    /// Acquire exclusive server-instance ownership for `db_path` before any production open.
    ///
    /// Acquires [`ServerInstanceGuard`] once and shares it inside this owner via [`Arc`]. Prefer
    /// [`from_shared_guard`] when the host already holds the guard for the same database path.
    pub fn acquire(db_path: &Path) -> anyhow::Result<Self> {
        let guard = Arc::new(ServerInstanceGuard::acquire_for_path(db_path)?);
        Self::from_shared_guard(guard)
    }

    /// Build an offline restore owner from an already-acquired [`ServerInstanceGuard`].
    ///
    /// The guard must already own `db_path`; do **not** call [`acquire`] for the same path afterward.
    pub fn from_shared_guard(guard: Arc<ServerInstanceGuard>) -> anyhow::Result<Self> {
        let production_db_path = guard.database_path().to_path_buf();
        let data_dir = canonical_data_directory(&production_db_path)?;
        let backups_dir = expected_backups_directory(&data_dir);
        Ok(Self {
            guard,
            production_db_path,
            data_dir,
            backups_dir,
        })
    }

    /// Production database path identity used when this owner was acquired.
    pub fn production_database_path(&self) -> &Path {
        &self.production_db_path
    }

    /// Shared server-instance guard; retain for the whole offline maintenance window.
    pub fn server_instance_guard(&self) -> &ServerInstanceGuard {
        self.guard.as_ref()
    }

    /// Expected `data_dir/backups` path for this instance (may not exist until preflight hardens it).
    pub fn backups_directory(&self) -> &Path {
        &self.backups_dir
    }

    /// Read-only validation of one backup candidate. Does not modify any file.
    pub fn preflight_candidate(
        &self,
        candidate: &Path,
        workflow_session_requirement: RestoreWorkflowSessionRequirement,
    ) -> anyhow::Result<RestorePreflightFacts> {
        let resolved = self.resolve_and_harden_candidate(candidate)?;
        if resolved == self.production_db_path {
            bail!(
                "restore candidate must not be the production database {}",
                self.production_db_path.display()
            );
        }
        ensure_candidate_not_same_file_as_production(&resolved, &self.production_db_path)?;

        let meta = fs::symlink_metadata(&resolved)
            .with_context(|| format!("stat restore candidate {}", resolved.display()))?;
        if meta.len() == 0 {
            bail!("restore candidate must not be empty");
        }
        let file_size_bytes = meta.len();

        require_no_sidecars(&resolved)?;

        let connection = open_readonly_restore_candidate(&resolved)?;
        let (
            quick_check_ok,
            foreign_key_check_empty,
            shared_backup_schema_ok,
            workflow_session_domain_present,
        ) = match run_sqlite_preflight(&connection, workflow_session_requirement) {
            Ok(outcome) => outcome,
            Err(error) => {
                let _ = close_readonly_connection(connection);
                return Err(error);
            }
        };
        close_readonly_connection(connection).context("close restore preflight connection")?;

        Ok(RestorePreflightFacts {
            candidate_path: resolved,
            file_size_bytes,
            quick_check_ok,
            foreign_key_check_empty,
            shared_backup_schema_ok,
            workflow_session_domain_present,
        })
    }

    fn resolve_and_harden_candidate(&self, candidate: &Path) -> anyhow::Result<PathBuf> {
        let absolute = absolute_lexical_path(candidate)?;
        let requested_meta = fs::symlink_metadata(&absolute)
            .with_context(|| format!("stat restore candidate {}", absolute.display()))?;
        if requested_meta.file_type().is_symlink() || is_reparse_point(&requested_meta) {
            bail!("restore candidate path must not be a symlink or reparse point");
        }
        if !requested_meta.is_file() {
            bail!("restore candidate path is not a regular file");
        }

        let canonical = fs::canonicalize(&absolute)
            .with_context(|| format!("canonicalize restore candidate {}", absolute.display()))?;
        let canonical_meta = fs::symlink_metadata(&canonical)
            .with_context(|| format!("stat canonical restore candidate {}", canonical.display()))?;
        if canonical_meta.file_type().is_symlink() || is_reparse_point(&canonical_meta) {
            bail!("restore candidate must not be a symlink or reparse point");
        }
        if !canonical_meta.is_file() {
            bail!("restore candidate is not a regular file");
        }

        let Some(parent) = canonical.parent() else {
            bail!("restore candidate has no parent directory");
        };
        let backups_dir = canonical_backups_dir(&self.data_dir)?;
        if parent != backups_dir {
            bail!(
                "restore candidate must be a direct child of {} (got parent {})",
                backups_dir.display(),
                parent.display()
            );
        }

        validate_trusted_directory_parent(&self.data_dir)?;
        validate_trusted_directory_parent(&backups_dir)?;
        validate_trusted_directory_parent(parent)?;

        let Some(file_name) = canonical.file_name().and_then(|name| name.to_str()) else {
            bail!("restore candidate file name is not UTF-8");
        };
        validate_published_backup_file_name(file_name)?;

        Ok(canonical)
    }
}

const QUICK_CHECK_FAILURE_MSG_CAP: usize = 256;

pub(crate) fn run_sqlite_preflight(
    connection: &Connection,
    workflow_session_requirement: RestoreWorkflowSessionRequirement,
) -> anyhow::Result<(bool, bool, bool, bool)> {
    let quick_check_row: String = connection
        .query_row("PRAGMA quick_check(1)", [], |row| row.get(0))
        .context("quick_check(1)")?;
    if quick_check_row != "ok" {
        let detail = bounded_sqlite_diag_message(&quick_check_row, QUICK_CHECK_FAILURE_MSG_CAP);
        bail!("restore candidate quick_check failed: {detail}");
    }
    let quick_check_ok = true;

    {
        let mut stmt = connection
            .prepare("PRAGMA foreign_key_check")
            .context("prepare foreign_key_check")?;
        let mut rows = stmt.query([]).context("query foreign_key_check")?;
        if rows.next().context("read foreign_key_check")?.is_some() {
            bail!("restore candidate foreign_key_check reported violations");
        }
    }
    let foreign_key_check_empty = true;

    crate::schema::validate_shared_database_backup_schema(connection)
        .context("shared database backup schema validation")?;
    let shared_backup_schema_ok = true;

    let workflow_session_domain_present = workflow_session_storage_marker_present(connection)?;
    if matches!(
        workflow_session_requirement,
        RestoreWorkflowSessionRequirement::RequirePresent
    ) && !workflow_session_domain_present
    {
        bail!(
            "restore candidate missing Workflow Session domain (`workflow_session_storage` absent); \
             use RequirePresent only when Session history must be restored"
        );
    }

    Ok((
        quick_check_ok,
        foreign_key_check_empty,
        shared_backup_schema_ok,
        workflow_session_domain_present,
    ))
}

/// Marker-table probe only; full 6/6 + marker version checks remain in [`crate::schema`].
fn workflow_session_storage_marker_present(connection: &Connection) -> anyhow::Result<bool> {
    let present = connection
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
            ["workflow_session_storage"],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    Ok(present)
}

fn bounded_sqlite_diag_message(message: &str, cap: usize) -> String {
    if message.len() <= cap {
        return message.to_string();
    }
    let mut end = cap;
    while end > 0 && !message.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &message[..end])
}

fn open_readonly_restore_candidate(path: &Path) -> anyhow::Result<Connection> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("open restore candidate {}", path.display()))?;
    connection
        .execute_batch("PRAGMA query_only=ON; PRAGMA foreign_keys=ON;")
        .context("configure read-only restore candidate pragmas")?;
    Ok(connection)
}

fn close_readonly_connection(connection: Connection) -> anyhow::Result<()> {
    match connection.close() {
        Ok(()) => Ok(()),
        Err((_connection, error)) => Err(error.into()),
    }
}

fn canonical_data_directory(production_db_path: &Path) -> anyhow::Result<PathBuf> {
    let parent = production_db_path
        .parent()
        .context("production database path has no parent directory")?;
    let data_dir = fs::canonicalize(parent)
        .with_context(|| format!("canonicalize data directory {}", parent.display()))?;
    validate_trusted_directory_parent(&data_dir)?;
    let meta = fs::symlink_metadata(&data_dir)
        .with_context(|| format!("stat data directory {}", data_dir.display()))?;
    if meta.file_type().is_symlink() || is_reparse_point(&meta) {
        bail!("data directory must not be a symlink or reparse point");
    }
    if !meta.is_dir() {
        bail!("data directory path is not a directory");
    }
    if !path_starts_with(production_db_path, &data_dir) {
        bail!("production database path escapes data directory");
    }
    Ok(data_dir)
}

fn expected_backups_directory(data_dir: &Path) -> PathBuf {
    data_dir.join(BACKUP_DIR_NAME)
}

fn canonical_backups_dir(data_dir: &Path) -> anyhow::Result<PathBuf> {
    let backup_dir = data_dir.join(BACKUP_DIR_NAME);
    let meta = fs::symlink_metadata(&backup_dir)
        .with_context(|| format!("stat backup directory {}", backup_dir.display()))?;
    if meta.file_type().is_symlink() || is_reparse_point(&meta) {
        bail!("backup directory must not be a symlink or reparse point");
    }
    if !meta.is_dir() {
        bail!("backup directory is not a directory");
    }
    let canonical_backup = fs::canonicalize(&backup_dir)
        .with_context(|| format!("canonicalize backup directory {}", backup_dir.display()))?;
    if !path_starts_with(&canonical_backup, data_dir) {
        bail!("backup directory escapes data directory");
    }
    Ok(canonical_backup)
}

fn validate_published_backup_file_name(name: &str) -> anyhow::Result<()> {
    if name.contains(".partial") || name.ends_with(".db.partial") {
        bail!("restore candidate must not be a partial backup file");
    }
    if !name.ends_with(PUBLISHED_BACKUP_EXTENSION) {
        bail!("restore candidate must use {PUBLISHED_BACKUP_EXTENSION} extension");
    }
    let Some(body) = name.strip_prefix(PUBLISHED_BACKUP_PREFIX) else {
        bail!("restore candidate file name must start with {PUBLISHED_BACKUP_PREFIX}");
    };
    let Some(body) = body.strip_suffix(PUBLISHED_BACKUP_EXTENSION) else {
        bail!("restore candidate file name must end with {PUBLISHED_BACKUP_EXTENSION}");
    };
    let Some((timestamp, uuid_part)) = body.split_once('-') else {
        bail!("restore candidate file name does not match webcodex-<time>-<uuid>.db");
    };
    if timestamp.parse::<i64>().is_err() {
        bail!("restore candidate timestamp is not a valid integer millisecond value");
    }
    if uuid::Uuid::parse_str(uuid_part).is_err() {
        bail!("restore candidate uuid suffix is not a valid UUID");
    }
    Ok(())
}

fn require_no_sidecars(base: &Path) -> anyhow::Result<()> {
    for suffix in SIDECAR_SUFFIXES {
        let sidecar = sidecar_path(base, suffix);
        match fs::symlink_metadata(&sidecar) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("stat restore candidate sidecar {}", sidecar.display())
                });
            }
            Ok(_) => {
                bail!(
                    "restore candidate sidecar {} must not exist for offline restore",
                    sidecar.display()
                );
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

fn validate_trusted_directory_parent(dir: &Path) -> anyhow::Result<()> {
    let meta =
        fs::symlink_metadata(dir).with_context(|| format!("stat directory {}", dir.display()))?;
    if meta.file_type().is_symlink() || is_reparse_point(&meta) {
        bail!(
            "directory {} must not be a symlink or reparse point",
            dir.display()
        );
    }
    if !meta.is_dir() {
        bail!("path {} is not a directory", dir.display());
    }
    let Some(parent) = dir.parent() else {
        return Ok(());
    };
    if parent.as_os_str().is_empty() {
        return Ok(());
    }
    let parent_meta = fs::symlink_metadata(parent)
        .with_context(|| format!("stat parent of directory {}", dir.display()))?;
    if parent_meta.file_type().is_symlink() || is_reparse_point(&parent_meta) {
        bail!(
            "parent of directory {} must not be a symlink or reparse point",
            dir.display()
        );
    }
    if !parent_meta.is_dir() {
        bail!("parent of directory {} is not a directory", dir.display());
    }
    Ok(())
}

fn absolute_lexical_path(path: &Path) -> anyhow::Result<PathBuf> {
    let candidate = if path.is_absolute() {
        path.to_path_buf()
    } else {
        let cwd = std::env::current_dir().context("resolve working directory for path")?;
        cwd.join(path)
    };
    lexical_normalize_absolute(&candidate)
}

fn lexical_normalize_absolute(path: &Path) -> anyhow::Result<PathBuf> {
    if !path.is_absolute() {
        bail!("path must resolve to an absolute location");
    }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(Path::new("/")),
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() {
                    bail!("path escapes its filesystem root");
                }
            }
            Component::Normal(value) => normalized.push(value),
        }
    }
    Ok(normalized)
}

fn path_starts_with(path: &Path, prefix: &Path) -> bool {
    path.starts_with(prefix)
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

/// Reject restore when the candidate path names the same on-disk file object as production.
///
/// Path inequality alone does not establish distinct files (e.g. hard links under `backups/`).
fn ensure_candidate_not_same_file_as_production(
    candidate: &Path,
    production: &Path,
) -> anyhow::Result<()> {
    let production_meta = match fs::symlink_metadata(production) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "stat production database {} for same-file identity check",
                    production.display()
                )
            });
        }
        Ok(meta) => meta,
    };

    if production_meta.file_type().is_symlink() || is_reparse_point(&production_meta) {
        bail!(
            "production database {} must not be a symlink or reparse point",
            production.display()
        );
    }
    if !production_meta.is_file() {
        bail!(
            "production database {} is not a regular file",
            production.display()
        );
    }

    match same_file::is_same_file(candidate, production) {
        Ok(true) => {
            bail!(
                "restore candidate is the same underlying file as the production database {} \
                 (hard link or identical file object; distinct paths do not imply distinct files)",
                production.display()
            );
        }
        Ok(false) => Ok(()),
        Err(error) => Err(error).with_context(|| {
            format!(
                "compare restore candidate {} with production database {} for same-file identity",
                candidate.display(),
                production.display()
            )
        }),
    }
}
