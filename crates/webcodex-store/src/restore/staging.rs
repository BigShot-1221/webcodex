//! K.2d Phase 5 — candidate staging and production file swap under a durable restore journal.
//!
//! [`OfflineRestoreOwner::restore_from_backup`] validates a published backup, stages a private
//! candidate snapshot beside production data, preserves the original production files (Phase 4),
//! records **Prepared** intent in the restore journal, then performs sidecar removal and an atomic
//! same-directory install through **CandidateInstalled**.
//!
//! **Committed** remains the responsibility of real Server startup (`Database::open` +
//! `with_session_database`).
//!
//! # Durability (platform limits)
//!
//! Staged bytes are flushed with `sync_all`; on Unix the parent `data_dir` directory entry is
//! synced after stage publication and after sidecar removal. On Windows, same-directory renames use
//! `MoveFileExW` with `MOVEFILE_WRITE_THROUGH`; portable parent-directory fsync is **not**
//! available, so `data_dir` directory-entry durability is **best-effort** (same as journal and
//! preservation publish).

use super::journal::{RestoreJournal, RestoreJournalPayload, RestorePhase};
use super::SourceFileIdentity;
use super::{
    open_readonly_restore_candidate, run_sqlite_preflight, OfflineRestoreOwner,
    RestoreWorkflowSessionRequirement,
};
use anyhow::{bail, Context};
use sha2::{Digest, Sha256};
use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use uuid::Uuid;

const SIDECAR_SUFFIXES: [&str; 3] = ["-wal", "-shm", "-journal"];
const COPY_BUFFER_BYTES: usize = 1024 * 1024;
const SHA256_HEX_LEN: usize = 64;

/// Phase 5 entry: stage a validated backup, preserve originals, journal, and install the candidate.
impl OfflineRestoreOwner {
    /// Stages `candidate`, preserves production originals, journals **Prepared**, and completes file
    /// install through **CandidateInstalled** with [`RestoreWorkflowSessionRequirement::RequirePresent`].
    ///
    /// Returns the restore `operation_id` for startup recovery and commit. Does not open SQLite on
    /// the installed production database or transition to **Committed**.
    pub fn restore_from_backup(&self, candidate: &Path) -> anyhow::Result<Uuid> {
        self.restore_from_backup_with_schema(candidate, false)
    }

    /// Explicitly authorized old-Fork restore. Convert only the private stage,
    /// not the source backup or the production database. The existing journal
    /// preserves/rolls back production exactly as for an ordinary restore.
    pub fn restore_from_fork_backup(&self, candidate: &Path) -> anyhow::Result<Uuid> {
        self.restore_from_backup_with_schema(candidate, true)
    }

    fn restore_from_backup_with_schema(
        &self,
        candidate: &Path,
        migrate_fork: bool,
    ) -> anyhow::Result<Uuid> {
        use webcodex_workflow_session::{detect_workflow_session_schema, WorkflowSessionSchema};
        let workflow_session_requirement = RestoreWorkflowSessionRequirement::RequirePresent;
        let preflight = self.preflight_candidate(candidate, workflow_session_requirement)?;
        let backup_path = preflight.candidate_path;
        let operation_id = Uuid::new_v4();
        let data_dir = canonical_data_directory(self.production_database_path())?;
        super::validate_trusted_directory_parent(&data_dir)?;

        let stage_file_name = candidate_stage_file_name(operation_id);
        let stage_path = data_dir.join(&stage_file_name);
        ensure_direct_child_of_data_dir(&data_dir, &stage_path, "candidate stage")?;

        let mut stage_cleanup = StageCleanupGuard::new(stage_path.clone());

        let (mut candidate_size_bytes, mut candidate_sha256_hex) = stage_candidate_from_backup(
            &backup_path,
            &stage_path,
            preflight.file_size_bytes,
            &mut stage_cleanup,
        )?;

        validate_stage_sqlite(&stage_path, workflow_session_requirement)?;
        verify_staged_regular_file(&stage_path, candidate_size_bytes, &candidate_sha256_hex)?;
        super::require_no_sidecars(&stage_path)?;
        let staged_main_identity = stage_cleanup.verify_owned_stage()?;
        let stage_schema = {
            let connection = open_readonly_restore_candidate(&stage_path)?;
            let schema = detect_workflow_session_schema(&connection);
            super::close_readonly_connection(connection)?;
            schema?
        };
        match (stage_schema, migrate_fork) {
            (WorkflowSessionSchema::CurrentV1, false) => {}
            (WorkflowSessionSchema::ForkV1, true) => {
                // The original candidate hash above fenced the input copy.
                // A new hash fences the explicitly converted output bytes.
                if let Err(err) =
                    super::session_migration::migrate_private_restore_stage(&stage_path)
                {
                    // If SQLite left a sidecar, retain the owned stage for
                    // diagnosis instead of deleting its main file alone.
                    if super::require_no_sidecars(&stage_path).is_err() {
                        stage_cleanup.disarm();
                    }
                    return Err(err.context(
                        "private Fork backup conversion failed; production not replaced",
                    ));
                }
                if stage_cleanup.verify_owned_stage()? != staged_main_identity {
                    bail!("converted stage identity changed");
                }
                let mut file = open_nofollow_read(&stage_path)?;
                candidate_size_bytes = file.metadata()?.len();
                candidate_sha256_hex = hash_open_file(&mut file, candidate_size_bytes)?;
                drop(file);
                validate_stage_sqlite(&stage_path, workflow_session_requirement)?;
                verify_staged_regular_file(
                    &stage_path,
                    candidate_size_bytes,
                    &candidate_sha256_hex,
                )?;
                super::require_no_sidecars(&stage_path)?;
                sync_parent_directory(&stage_path)?;
            }
            (WorkflowSessionSchema::ForkV1, false) => {
                bail!("Fork backup requires explicit restore_from_fork_backup authorization")
            }
            _ => bail!("restore candidate does not match the selected Workflow Session format"),
        }

        let preservation = self
            .preserve_original(operation_id)
            .context("preserve original production files before restore journal")?;

        let production_db_file_name = production_db_basename(self.production_database_path())?;
        let candidate_backup_file_name = backup_basename(&backup_path)?;
        ensure_preservation_matches_staging(&preservation, operation_id, &production_db_file_name)?;

        let journal_payload = RestoreJournalPayload::new_prepared(
            operation_id,
            production_db_file_name,
            candidate_backup_file_name,
            stage_file_name,
            candidate_size_bytes,
            candidate_sha256_hex.clone(),
            preservation.manifest_sha256_hex().to_string(),
            preservation.original_db_present(),
            preservation
                .original_db_sha256_hex()
                .map(|hash| hash.to_string()),
            matches!(
                workflow_session_requirement,
                RestoreWorkflowSessionRequirement::RequirePresent
            ),
            staged_main_identity,
        );

        let guard = self.server_instance_guard();
        let mut journal = match RestoreJournal::create(guard, journal_payload) {
            Ok(journal) => {
                stage_cleanup.disarm();
                journal
            }
            Err(error) => {
                // Publication may have succeeded before directory sync failed. In that case
                // ownership has passed to recovery even though create returned an error.
                match RestoreJournal::load_for_guard(guard) {
                    Ok(None) => {}
                    Ok(Some(_)) => {
                        stage_cleanup.disarm();
                        tracing::warn!(stage = %stage_path.display(),
                            "restore journal present after create error; candidate retained for recovery");
                    }
                    Err(reload_error) => {
                        stage_cleanup.disarm();
                        tracing::error!(error = %reload_error, stage = %stage_path.display(),
                            "restore journal publication uncertain; candidate retained for diagnosis");
                    }
                }
                return Err(error.context("create Prepared restore journal"));
            }
        };

        verify_candidate_stage_for_install(&stage_path, journal.payload())?;
        journal
            .transition(guard, RestorePhase::RemovingSidecars)
            .context("transition restore journal to RemovingSidecars")?;
        remove_production_sidecars(self.production_database_path(), &data_dir)?;

        journal
            .transition(guard, RestorePhase::InstallingCandidate)
            .context("transition restore journal to InstallingCandidate")?;
        atomic_install_stage_over_production(
            &stage_path,
            self.production_database_path(),
            &data_dir,
            journal.payload(),
        )?;
        verify_installed_production_database(
            self.production_database_path(),
            candidate_size_bytes,
            &candidate_sha256_hex,
            journal.payload().candidate_production_db_identity(),
        )?;

        journal
            .transition(guard, RestorePhase::CandidateInstalled)
            .context("transition restore journal to CandidateInstalled")?;

        Ok(operation_id)
    }
}

/// Removes the staged candidate only while no journal exists (pre-journal failure cleanup).
struct StageCleanupGuard {
    stage_path: PathBuf,
    identity: Option<SourceFileIdentity>,
}

impl StageCleanupGuard {
    fn new(stage_path: PathBuf) -> Self {
        Self {
            stage_path,
            identity: None,
        }
    }

    fn disarm(&mut self) {
        self.identity = None;
    }

    fn verify_owned_stage(&self) -> anyhow::Result<SourceFileIdentity> {
        let expected = self
            .identity
            .as_ref()
            .context("candidate stage has no creation identity")?;
        let file = open_nofollow_read(&self.stage_path)?;
        ensure_single_link_file(&file, &self.stage_path)?;
        if SourceFileIdentity::from_open_file(&file)? != *expected
            || SourceFileIdentity::from_source_path(&self.stage_path)? != *expected
        {
            bail!("candidate stage no longer names the file created by this operation");
        }
        Ok(expected.clone())
    }
}

impl Drop for StageCleanupGuard {
    fn drop(&mut self) {
        let Some(identity) = self.identity.as_ref() else {
            return;
        };
        if let Err(error) = best_effort_remove_stage(&self.stage_path, identity) {
            tracing::error!(error = %error, stage = %self.stage_path.display(),
                "pre-journal candidate cleanup incomplete; inspect stage and directory durability");
        }
    }
}

fn best_effort_remove_stage(
    stage_path: &Path,
    expected_identity: &SourceFileIdentity,
) -> anyhow::Result<()> {
    match fs::symlink_metadata(stage_path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| {
            format!(
                "stat candidate stage {} during pre-journal cleanup",
                stage_path.display()
            )
        }),
        Ok(meta) => {
            if meta.file_type().is_symlink() || super::is_reparse_point(&meta) {
                bail!(
                    "refusing to delete candidate stage {} (symlink or reparse point)",
                    stage_path.display()
                );
            }
            if !meta.is_file() {
                bail!(
                    "refusing to delete candidate stage {} (not a regular file)",
                    stage_path.display()
                );
            }
            let file = open_nofollow_read(stage_path)?;
            ensure_single_link_file(&file, stage_path)?;
            if SourceFileIdentity::from_open_file(&file)? != *expected_identity {
                bail!("candidate stage identity changed; refusing pre-journal cleanup");
            }
            if SourceFileIdentity::from_source_path(stage_path)? != *expected_identity {
                bail!("candidate stage path changed; refusing pre-journal cleanup");
            }
            fs::remove_file(stage_path)
                .with_context(|| format!("remove candidate stage {}", stage_path.display()))?;
            sync_parent_directory(stage_path)
        }
    }
}

fn candidate_stage_file_name(operation_id: Uuid) -> String {
    format!(".webcodex-restore-{}.candidate", operation_id.simple())
}

fn canonical_data_directory(production_db_path: &Path) -> anyhow::Result<PathBuf> {
    let parent = production_db_path
        .parent()
        .context("production database path has no parent directory")?;
    let data_dir = fs::canonicalize(parent)
        .with_context(|| format!("canonicalize data directory {}", parent.display()))?;
    super::validate_trusted_directory_parent(&data_dir)?;
    let meta = fs::symlink_metadata(&data_dir)
        .with_context(|| format!("stat data directory {}", data_dir.display()))?;
    if meta.file_type().is_symlink() || super::is_reparse_point(&meta) {
        bail!("data directory must not be a symlink or reparse point");
    }
    if !meta.is_dir() {
        bail!("data directory path is not a directory");
    }
    if !super::path_starts_with(production_db_path, &data_dir) {
        bail!("production database path escapes data directory");
    }
    Ok(data_dir)
}

fn ensure_direct_child_of_data_dir(
    data_dir: &Path,
    child: &Path,
    label: &str,
) -> anyhow::Result<()> {
    if child.parent() != Some(data_dir) {
        bail!(
            "{label} path must be a direct child of {}",
            data_dir.display()
        );
    }
    let Some(name) = child.file_name().and_then(OsStr::to_str) else {
        bail!("{label} file name is not UTF-8");
    };
    validate_single_path_component(name, label)?;
    Ok(())
}

fn production_db_basename(production_db_path: &Path) -> anyhow::Result<String> {
    production_db_path
        .file_name()
        .and_then(OsStr::to_str)
        .map(str::to_string)
        .context("production database file name is not UTF-8")
}

fn backup_basename(backup_path: &Path) -> anyhow::Result<String> {
    let name = backup_path
        .file_name()
        .and_then(OsStr::to_str)
        .context("backup candidate file name is not UTF-8")?;
    validate_single_path_component(name, "candidate_backup_file_name")?;
    super::validate_published_backup_file_name(name)?;
    Ok(name.to_string())
}

fn ensure_preservation_matches_staging(
    preservation: &super::OriginalPreservation,
    operation_id: Uuid,
    production_db_file_name: &str,
) -> anyhow::Result<()> {
    if preservation.operation_id() != operation_id {
        bail!("preservation operation_id does not match restore operation_id");
    }
    if preservation.manifest().production_db_file_name() != production_db_file_name {
        bail!("preservation production_db_file_name does not match production database");
    }
    Ok(())
}

fn stage_candidate_from_backup(
    backup_path: &Path,
    stage_path: &Path,
    expected_source_size: u64,
    cleanup: &mut StageCleanupGuard,
) -> anyhow::Result<(u64, String)> {
    let backup_meta = fs::symlink_metadata(backup_path)
        .with_context(|| format!("stat backup candidate {}", backup_path.display()))?;
    if backup_meta.file_type().is_symlink() || super::is_reparse_point(&backup_meta) {
        bail!(
            "backup candidate {} must not be a symlink or reparse point",
            backup_path.display()
        );
    }
    if !backup_meta.is_file() {
        bail!(
            "backup candidate {} is not a regular file",
            backup_path.display()
        );
    }
    if backup_meta.len() != expected_source_size {
        bail!(
            "backup candidate {} size changed since preflight (expected {expected_source_size}, got {})",
            backup_path.display(),
            backup_meta.len()
        );
    }
    if expected_source_size == 0 {
        bail!("backup candidate must not be empty");
    }

    super::require_no_sidecars(backup_path)?;

    let mut source = open_nofollow_read(backup_path)?;
    ensure_single_link_file(&source, backup_path)?;
    let identity_start = file_identity_from_open(&source)?;
    let identity_path = file_identity_from_path(backup_path)?;
    if identity_path != identity_start {
        bail!(
            "backup candidate {} path/handle identity mismatch",
            backup_path.display()
        );
    }

    let mut destination = open_create_new_stage(stage_path)?;
    // Arm only after create_new succeeds: a collision never grants deletion authority.
    cleanup.identity = Some(
        SourceFileIdentity::from_open_file(&destination).with_context(|| {
            format!(
                "capture newly created candidate stage identity {}; preserve on failure",
                stage_path.display()
            )
        })?,
    );
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; COPY_BUFFER_BYTES];
    let mut copied: u64 = 0;

    source
        .seek(SeekFrom::Start(0))
        .context("rewind backup candidate for staging copy")?;

    loop {
        let limit = (expected_source_size
            .saturating_sub(copied)
            .saturating_add(1))
        .min(buffer.len() as u64) as usize;
        let read = source
            .read(&mut buffer[..limit])
            .with_context(|| format!("read backup candidate {}", backup_path.display()))?;
        if read == 0 {
            break;
        }
        if read as u64 > expected_source_size.saturating_sub(copied) {
            bail!("backup candidate grew beyond preflight size during staging copy");
        }
        destination
            .write_all(&buffer[..read])
            .with_context(|| format!("write candidate stage {}", stage_path.display()))?;
        hasher.update(&buffer[..read]);
        copied = copied.saturating_add(read as u64);
    }

    if copied != expected_source_size {
        bail!(
            "staging copy size mismatch for {} (expected {expected_source_size}, copied {copied})",
            backup_path.display()
        );
    }

    let candidate_sha256_hex = format_sha256_lower(&hasher.finalize());
    verify_source_stable_after_copy(
        &mut source,
        backup_path,
        &identity_start,
        expected_source_size,
        &candidate_sha256_hex,
    )?;

    destination
        .sync_all()
        .with_context(|| format!("sync candidate stage {}", stage_path.display()))?;
    drop(destination);

    sync_parent_directory(stage_path)?;
    verify_staged_regular_file(stage_path, copied, &candidate_sha256_hex)?;
    Ok((copied, candidate_sha256_hex))
}

fn validate_stage_sqlite(
    stage_path: &Path,
    workflow_session_requirement: RestoreWorkflowSessionRequirement,
) -> anyhow::Result<()> {
    let connection = open_readonly_restore_candidate(stage_path)?;
    let outcome = run_sqlite_preflight(&connection, workflow_session_requirement);
    let close = super::close_readonly_connection(connection);
    outcome?;
    close.context("close staged candidate SQLite connection")?;
    Ok(())
}

fn verify_staged_regular_file(
    path: &Path,
    expected_size: u64,
    expected_sha256: &str,
) -> anyhow::Result<()> {
    validate_sha256_hex(expected_sha256, "candidate_sha256_hex")?;

    let meta = fs::symlink_metadata(path)
        .with_context(|| format!("stat staged candidate {}", path.display()))?;
    if meta.file_type().is_symlink() || super::is_reparse_point(&meta) {
        bail!(
            "staged candidate {} must not be a symlink or reparse point",
            path.display()
        );
    }
    if !meta.is_file() {
        bail!("staged candidate {} is not a regular file", path.display());
    }
    if meta.len() != expected_size {
        bail!(
            "staged candidate {} size mismatch (expected {expected_size}, got {})",
            path.display(),
            meta.len()
        );
    }

    let mut file = open_nofollow_read(path)?;
    ensure_single_link_file(&file, path)?;
    if file.metadata()?.len() != expected_size {
        bail!(
            "staged candidate {} open handle size mismatch",
            path.display()
        );
    }
    let identity = file_identity_from_open(&file)?;
    let actual = hash_open_file(&mut file, expected_size)?;
    if file_identity_from_path(path)? != identity || file.metadata()?.len() != expected_size {
        bail!("staged candidate path or size changed during verification");
    }
    if actual != expected_sha256 {
        bail!(
            "staged candidate {} SHA-256 mismatch after validation",
            path.display()
        );
    }
    Ok(())
}

fn remove_production_sidecars(production_db_path: &Path, data_dir: &Path) -> anyhow::Result<()> {
    for suffix in SIDECAR_SUFFIXES {
        let sidecar = sidecar_path(production_db_path, suffix);
        ensure_direct_child_of_data_dir(data_dir, &sidecar, "production sidecar")?;
        safe_remove_production_sidecar(&sidecar)?;
    }
    sync_parent_directory(production_db_path)
}

fn safe_remove_production_sidecar(path: &Path) -> anyhow::Result<()> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => {
            Err(error).with_context(|| format!("stat production sidecar {}", path.display()))
        }
        Ok(meta) => {
            if meta.file_type().is_symlink() || super::is_reparse_point(&meta) {
                bail!(
                    "production sidecar {} must not be a symlink or reparse point",
                    path.display()
                );
            }
            if !meta.is_file() {
                bail!(
                    "production sidecar {} is not a regular file",
                    path.display()
                );
            }
            let file = open_nofollow_read(path)?;
            ensure_single_link_file(&file, path)?;
            fs::remove_file(path)
                .with_context(|| format!("remove production sidecar {}", path.display()))
        }
    }
}

fn atomic_install_stage_over_production(
    stage_path: &Path,
    production_db_path: &Path,
    data_dir: &Path,
    payload: &RestoreJournalPayload,
) -> anyhow::Result<()> {
    ensure_direct_child_of_data_dir(data_dir, stage_path, "candidate stage")?;
    ensure_direct_child_of_data_dir(data_dir, production_db_path, "production database")?;

    let stage_meta = fs::symlink_metadata(stage_path).with_context(|| {
        format!(
            "stat candidate stage before install {}",
            stage_path.display()
        )
    })?;
    if stage_meta.file_type().is_symlink() || super::is_reparse_point(&stage_meta) {
        bail!(
            "candidate stage {} must not be a symlink or reparse point",
            stage_path.display()
        );
    }
    if !stage_meta.is_file() {
        bail!(
            "candidate stage {} is not a regular file",
            stage_path.display()
        );
    }
    let stage_file = open_nofollow_read(stage_path)?;
    ensure_single_link_file(&stage_file, stage_path)?;

    verify_candidate_stage_for_install(stage_path, payload)?;
    let expected_identity = payload
        .candidate_production_db_identity()
        .context("candidate install missing journaled identity")?;
    if SourceFileIdentity::from_open_file(&stage_file)? != *expected_identity {
        bail!("candidate install handle does not match journaled identity");
    }

    if let Ok(true) = same_file::is_same_file(stage_path, production_db_path) {
        bail!(
            "candidate stage must not be the same underlying file as production {}",
            production_db_path.display()
        );
    }

    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Storage::FileSystem::{
            MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
        };

        let wide = |path: &Path| {
            path.as_os_str()
                .encode_wide()
                .chain(Some(0))
                .collect::<Vec<u16>>()
        };
        let dest_exists = fs::symlink_metadata(production_db_path).is_ok();
        let flags = if dest_exists {
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH
        } else {
            MOVEFILE_WRITE_THROUGH
        };
        let ok = unsafe {
            MoveFileExW(
                wide(stage_path).as_ptr(),
                wide(production_db_path).as_ptr(),
                flags,
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error()).with_context(|| {
                format!(
                    "install candidate stage {} -> {}",
                    stage_path.display(),
                    production_db_path.display()
                )
            });
        }
    }
    #[cfg(unix)]
    {
        fs::rename(stage_path, production_db_path).with_context(|| {
            format!(
                "install candidate stage {} -> {}",
                stage_path.display(),
                production_db_path.display()
            )
        })?;
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (stage_path, production_db_path);
        bail!("candidate install is not supported on this platform");
    }

    sync_parent_directory(production_db_path)
}

fn verify_candidate_stage_for_install(
    stage_path: &Path,
    payload: &RestoreJournalPayload,
) -> anyhow::Result<()> {
    let expected = payload
        .candidate_production_db_identity()
        .context("candidate stage missing journaled identity")?;
    let actual = super::recovery::verify_hardened_regular_file_sha256(
        stage_path,
        payload.candidate_size_bytes,
        &payload.candidate_sha256_hex,
        "candidate stage before production mutation",
    )?;
    if actual != *expected {
        bail!("candidate stage identity changed before production mutation");
    }
    super::require_no_sidecars(stage_path)
}

fn verify_installed_production_database(
    production_db_path: &Path,
    expected_size: u64,
    expected_sha256: &str,
    expected_identity: Option<&SourceFileIdentity>,
) -> anyhow::Result<()> {
    let actual_identity = super::recovery::verify_hardened_regular_file_sha256(
        production_db_path,
        expected_size,
        expected_sha256,
        "installed production database",
    )?;
    super::require_no_sidecars(production_db_path)?;
    let Some(expected_identity) = expected_identity else {
        bail!("restore journal missing candidate_production_db_identity after candidate install");
    };
    if actual_identity != *expected_identity {
        bail!(
            "installed production database {} file identity mismatch",
            production_db_path.display()
        );
    }
    Ok(())
}

// --- hardened file helpers (aligned with restore/preserve patterns) ---

fn sidecar_path(base: &Path, suffix: &str) -> PathBuf {
    let mut path = OsString::from(base.as_os_str());
    path.push(suffix);
    PathBuf::from(path)
}

fn open_nofollow_read(path: &Path) -> anyhow::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    options
        .open(path)
        .with_context(|| format!("open {} without following", path.display()))
}

fn open_create_new_stage(path: &Path) -> anyhow::Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(path)
        .with_context(|| format!("create candidate stage {}", path.display()))
}

fn ensure_single_link_file(file: &File, path: &Path) -> anyhow::Result<()> {
    let meta = file
        .metadata()
        .with_context(|| format!("stat opened file {}", path.display()))?;
    if !meta.is_file() {
        bail!("opened path {} is not a regular file", path.display());
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
        if meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            bail!("opened path {} must not be a reparse point", path.display());
        }
    }

    let links = hard_link_count(file)?;
    if links != 1 {
        bail!(
            "file {} must have hard-link count exactly one (got {links})",
            path.display()
        );
    }
    Ok(())
}

fn hard_link_count(file: &File) -> anyhow::Result<u64> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(file.metadata()?.nlink())
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Foundation::HANDLE;
        use windows_sys::Win32::Storage::FileSystem::{
            GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
        };

        let handle = file.as_raw_handle();
        let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
        let ok = unsafe { GetFileInformationByHandle(handle as HANDLE, &mut info) };
        if ok == 0 {
            return Err(io::Error::last_os_error()).context("read hard-link count");
        }
        Ok(u64::from(info.nNumberOfLinks))
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = file;
        bail!("hard-link inspection is not supported on this platform");
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FileObjectIdentity {
    #[cfg(unix)]
    unix_device: u64,
    #[cfg(unix)]
    unix_inode: u64,
    #[cfg(windows)]
    windows_volume_serial: u32,
    #[cfg(windows)]
    windows_file_index: u64,
}

fn file_identity_from_open(file: &File) -> anyhow::Result<FileObjectIdentity> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let meta = file.metadata().context("stat opened file for identity")?;
        Ok(FileObjectIdentity {
            unix_device: meta.dev(),
            unix_inode: meta.ino(),
        })
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Foundation::HANDLE;
        use windows_sys::Win32::Storage::FileSystem::{
            GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
        };

        let handle = file.as_raw_handle();
        let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
        let ok = unsafe { GetFileInformationByHandle(handle as HANDLE, &mut info) };
        if ok == 0 {
            return Err(io::Error::last_os_error()).context("read file identity from handle");
        }
        let file_index = u64::from(info.nFileIndexHigh) << 32 | u64::from(info.nFileIndexLow);
        Ok(FileObjectIdentity {
            windows_volume_serial: info.dwVolumeSerialNumber,
            windows_file_index: file_index,
        })
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = file;
        bail!("file identity capture is not supported on this platform");
    }
}

fn file_identity_from_path(path: &Path) -> anyhow::Result<FileObjectIdentity> {
    let file = open_nofollow_read(path)?;
    file_identity_from_open(&file)
}

fn verify_source_stable_after_copy(
    source: &mut File,
    source_path: &Path,
    identity_start: &FileObjectIdentity,
    size_start: u64,
    expected_sha256: &str,
) -> anyhow::Result<()> {
    ensure_single_link_file(source, source_path)?;

    let identity_path = file_identity_from_path(source_path)?;
    if identity_path != *identity_start {
        bail!(
            "backup candidate {} no longer names the same file object after staging copy",
            source_path.display()
        );
    }

    let identity_handle = file_identity_from_open(source)?;
    if identity_handle != *identity_start {
        bail!(
            "backup candidate handle for {} no longer matches opening identity after staging copy",
            source_path.display()
        );
    }

    let size_now = source.metadata()?.len();
    if size_now != size_start {
        bail!(
            "backup candidate {} changed size during staging copy ({size_start} -> {size_now})",
            source_path.display()
        );
    }

    source
        .seek(SeekFrom::Start(0))
        .context("rewind backup candidate for post-copy stability hash")?;
    let actual = hash_open_file(source, size_start)?;
    if file_identity_from_path(source_path)? != *identity_start
        || source.metadata()?.len() != size_start
    {
        bail!("backup candidate path or size changed during post-copy verification");
    }
    if actual != expected_sha256 {
        bail!(
            "backup candidate {} content changed during staging copy (hash mismatch)",
            source_path.display()
        );
    }
    Ok(())
}

fn hash_open_file(file: &mut File, expected_size: u64) -> anyhow::Result<String> {
    file.seek(SeekFrom::Start(0))
        .context("rewind file for SHA-256")?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; COPY_BUFFER_BYTES];
    let mut hashed = 0u64;
    loop {
        let limit = (expected_size.saturating_sub(hashed).saturating_add(1))
            .min(buffer.len() as u64) as usize;
        let read = file
            .read(&mut buffer[..limit])
            .context("read file for SHA-256")?;
        if read == 0 {
            break;
        }
        if read as u64 > expected_size.saturating_sub(hashed) {
            bail!("file grew beyond expected size during SHA-256 verification");
        }
        hashed += read as u64;
        hasher.update(&buffer[..read]);
    }
    if hashed != expected_size {
        bail!("file size changed during SHA-256 verification");
    }
    Ok(format_sha256_lower(&hasher.finalize()))
}

fn format_sha256_lower(digest: impl AsRef<[u8]>) -> String {
    let digest = digest.as_ref();
    let mut out = String::with_capacity(SHA256_HEX_LEN);
    for &byte in digest {
        out.push(hex_char(byte >> 4));
        out.push(hex_char(byte & 0x0f));
    }
    out
}

fn hex_char(nibble: u8) -> char {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    HEX[nibble as usize] as char
}

fn validate_sha256_hex(value: &str, label: &str) -> anyhow::Result<()> {
    if value.len() != SHA256_HEX_LEN {
        bail!("{label} must be exactly {SHA256_HEX_LEN} lowercase hex characters");
    }
    if !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("{label} must be lowercase hexadecimal");
    }
    if value.bytes().any(|byte| byte.is_ascii_uppercase()) {
        bail!("{label} must use lowercase hexadecimal");
    }
    Ok(())
}

fn validate_single_path_component(name: &str, label: &str) -> anyhow::Result<()> {
    if name.is_empty() {
        bail!("{label} must not be empty");
    }
    if name.contains('/') || name.contains('\\') {
        bail!("{label} must not contain path separators");
    }
    let path = Path::new(name);
    let mut components = path.components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(_)), None) => Ok(()),
        _ => bail!("{label} must be a single normal path component"),
    }
}

/// Unix: fsync directory containing `path`; Windows: best-effort no-op for directory entry.
fn sync_parent_directory(path: &Path) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .context("path has no parent directory for sync")?;
    #[cfg(windows)]
    {
        let _ = parent;
        Ok(())
    }
    #[cfg(not(windows))]
    {
        File::open(parent)
            .with_context(|| format!("open parent directory for sync {}", parent.display()))?
            .sync_all()
            .with_context(|| format!("sync parent directory {}", parent.display()))
    }
}
