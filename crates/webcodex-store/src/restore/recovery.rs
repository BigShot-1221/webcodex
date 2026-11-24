//! K.2d Phase 6 — startup recovery gate, crash rollback, and post-open commit.
//!
//! Runs **before** [`crate::Database::open`] while [`super::OfflineRestoreOwner`] holds
//! [`ServerInstanceGuard`](crate::server_instance::ServerInstanceGuard). Pristine **CandidateInstalled**
//! journals transition to **StartupAttempting** before open; resumed **StartupAttempting** journals
//! validate production **file identity** (not immutable post-open SHA-256) and tolerate safe SQLite
//! sidecars after crash. Commit after `with_session_database` re-checks identity and runs bounded SQLite
//! pre-commit validation on the opened [`Database`].
//!
//! # Platform durability
//!
//! Journal and preservation modules use `MoveFileExW` with `MOVEFILE_WRITE_THROUGH` on Windows for
//! same-directory publishes. Parent **directory-entry** durability on Windows is **best-effort**
//! (no portable directory fsync in this stack), matching journal/preservation/staging behavior.
//!
//! # Caller ordering
//!
//! 1. [`OfflineRestoreOwner::acquire`], then [`OfflineRestoreOwner::recover_startup_before_database_open`].
//! 2. If `Some(PendingRestoreStartup)`, keep it alive across `Database::open` and
//!    `with_session_database`, then [`PendingRestoreStartup::commit_after_session_database`].
//! 3. Drop [`PendingRestoreStartup`] only after production `Database`, `SessionStore`, writer, and
//!    backup runtime locals are dropped so RAII rollback does not race open handles.
//!
//! `Ok(None)` consumes and drops [`OfflineRestoreOwner`] (releasing the guard). Integrators that need
//! the guard through `Database::open` when no pending restore exists must hold
//! [`ServerInstanceGuard`](crate::server_instance::ServerInstanceGuard) outside this API or extend
//! wiring in the parent module.

use super::journal::{RestoreJournal, RestoreJournalPayload, RestorePhase};
use super::{
    run_sqlite_preflight, OfflineRestoreOwner, OriginalPreservation,
    RestoreWorkflowSessionRequirement, SourceFileIdentity,
};
use crate::connection_observation::StoreDomain;
use crate::Database;
use anyhow::{bail, Context};
use sha2::{Digest, Sha256};
use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};
use uuid::Uuid;

const COPY_BUFFER_BYTES: usize = 1024 * 1024;
const SHA256_HEX_LEN: usize = 64;
const SQLITE_SIDECAR_SUFFIXES: [&str; 3] = ["-wal", "-shm", "-journal"];
const UNJOURNALED_SCAN_MAX_ENTRIES: usize = 4096;
const UNJOURNALED_SCAN_MAX_DIAGNOSTICS: usize = 8;

/// Startup token returned when a **StartupAttempting** restore journal survives crash recovery.
///
/// Holds the offline owner (and its guard lease), journal, and verified preservation until commit
/// or RAII rollback on drop.
#[must_use = "restore startup commit/rollback must complete or PendingRestoreStartup will roll back on drop"]
pub struct PendingRestoreStartup {
    owner: OfflineRestoreOwner,
    journal: Option<RestoreJournal>,
    preservation: OriginalPreservation,
    committed: bool,
}

impl OfflineRestoreOwner {
    /// Crash recovery and startup gate immediately before production `Database::open`.
    ///
    /// - No journal: `Ok(None)` (owner dropped).
    /// - Corrupt or unverifiable journal: `Err` (fail closed — do not open the database).
    /// - Terminal **Committed** / **RolledBack**: remove journal only; preservation is retained; `Ok(None)`.
    /// - Non-terminal: verify preservation, durably enter **StartupAttempting** when appropriate,
    ///   return `Some(PendingRestoreStartup)`, or complete rollback to **RolledBack** and `Ok(None)`.
    ///   Unverifiable **StartupAttempting** / legacy v1 non-pristine states fail closed (`Err`).
    pub fn recover_startup_before_database_open(
        self,
    ) -> anyhow::Result<Option<PendingRestoreStartup>> {
        let guard = self.server_instance_guard();
        let Some(mut journal) = RestoreJournal::load_for_guard(guard)? else {
            // A name alone is not proof of ownership. Preserve legacy pre-journal stages;
            // diagnose them under the instance lease without granting deletion authority.
            if let Err(error) = diagnose_unjournaled_candidate_stages(&self) {
                tracing::warn!(error = %error,
                    "bounded pre-journal candidate diagnosis incomplete; no files removed");
            }
            return Ok(None);
        };

        let phase = journal.payload().phase;
        if is_terminal_phase(phase) {
            journal.remove_terminal(guard)?;
            return Ok(None);
        }

        let payload = journal.payload().clone();
        let preservation = self
            .load_preservation_for_journal(&payload)
            .context("load preservation for startup restore recovery")?;

        let production_path = self.production_database_path();
        match phase {
            RestorePhase::StartupAttempting => {
                validate_startup_attempting_ready(production_path, &journal)
                    .context("validate StartupAttempting restore state before database open")?;
                return Ok(Some(PendingRestoreStartup {
                    owner: self,
                    journal: Some(journal),
                    preservation,
                    committed: false,
                }));
            }
            RestorePhase::CandidateInstalled => {
                let schema_version = journal.payload().schema_version();
                match validate_candidate_installed_ready(production_path, &journal) {
                    Ok(identity) => {
                        journal
                            .enter_startup_attempting(guard, identity)
                            .with_context(|| {
                                if schema_version == 1 {
                                    "upgrade schema v1 CandidateInstalled journal to StartupAttempting"
                                } else {
                                    "transition restore journal to StartupAttempting before database open"
                                }
                            })?;
                        return Ok(Some(PendingRestoreStartup {
                            owner: self,
                            journal: Some(journal),
                            preservation,
                            committed: false,
                        }));
                    }
                    Err(error) => {
                        if schema_version == 1 {
                            bail!(
                                "schema v1 CandidateInstalled journal is not pristine; identity cannot be proven for cross-restart resume: {error}"
                            );
                        }
                        return Err(error).context(
                            "validate CandidateInstalled restore state before database open",
                        );
                    }
                }
            }
            _ => {}
        }

        complete_startup_rollback(&self, &mut journal, &preservation)
            .context("rollback incomplete restore before database open")?;
        let guard = self.server_instance_guard();
        journal.remove_terminal(guard)?;
        Ok(None)
    }
}

impl PendingRestoreStartup {
    /// Persist **Committed** after successful `Database::open` and `with_session_database`.
    ///
    /// Callers must invoke this only after those steps succeed. Verifies `database.state_path()`
    /// matches this owner and [`ServerInstanceGuard::owns_database`]. After **Committed** is
    /// durably persisted, the operation is considered committed even if journal removal fails.
    pub fn commit_after_session_database(&mut self, database: &Database) -> anyhow::Result<()> {
        if self.committed {
            return Ok(());
        }
        let guard = self.owner.server_instance_guard();
        let production_path = self.owner.production_database_path();
        if database.state_path() != production_path {
            bail!(
                "database state_path {} does not match offline restore production path {}",
                database.state_path().display(),
                production_path.display()
            );
        }
        if !guard.owns_database(database) {
            bail!("ServerInstanceGuard does not own the opened Database for restore commit");
        }

        let journal = self
            .journal
            .as_mut()
            .context("restore startup commit missing active journal")?;
        let payload = journal.payload();
        let phase = payload.phase;
        if phase != RestorePhase::StartupAttempting {
            bail!(
                "restore startup commit requires StartupAttempting journal phase (got {phase:?})"
            );
        }

        let expected_identity = payload
            .candidate_production_db_identity()
            .context("restore startup commit missing candidate_production_db_identity")?;
        verify_startup_attempting_main_file(production_path, expected_identity)
            .context("production database identity mismatch at restore commit")?;

        let workflow_session_requirement = if payload.require_workflow_session_domain {
            RestoreWorkflowSessionRequirement::RequirePresent
        } else {
            RestoreWorkflowSessionRequirement::AllowAbsent
        };
        {
            let connection = database.lock_connection(StoreDomain::Schema);
            run_sqlite_preflight(&connection, workflow_session_requirement)
                .context("restore startup SQLite pre-commit validation")?;
            if webcodex_workflow_session::detect_workflow_session_schema(&connection)?
                != webcodex_workflow_session::WorkflowSessionSchema::CurrentV1
            {
                bail!("restored database did not reach the current Workflow Session schema");
            }
        }

        journal
            .transition(guard, RestorePhase::Committed)
            .context("transition restore journal to Committed")?;
        self.committed = true;

        if let Some(journal) = self.journal.take() {
            match journal.remove_terminal(guard) {
                Ok(()) => {}
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        "remove terminal restore journal after Committed failed; journal left for next startup"
                    );
                }
            }
        }
        Ok(())
    }
}

impl Drop for PendingRestoreStartup {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let Some(journal) = self.journal.take() else {
            return;
        };
        let guard = self.owner.server_instance_guard();
        match pending_drop_rollback(&self.owner, journal, &self.preservation, guard) {
            Ok(()) => {}
            Err(RestoreJournalOnDrop {
                error,
                journal: Some(journal),
            }) => {
                tracing::error!(
                    error = %error,
                    "restore startup PendingRestoreStartup drop rollback failed; journal and preservation retained for retry"
                );
                self.journal = Some(journal);
            }
            Err(RestoreJournalOnDrop {
                error,
                journal: None,
            }) => {
                tracing::error!(
                    error = %error,
                    "restore startup PendingRestoreStartup drop rollback failed without journal recovery"
                );
            }
        }
    }
}

struct RestoreJournalOnDrop {
    error: anyhow::Error,
    journal: Option<RestoreJournal>,
}

fn pending_drop_rollback(
    owner: &OfflineRestoreOwner,
    mut journal: RestoreJournal,
    preservation: &OriginalPreservation,
    guard: &crate::ServerInstanceGuard,
) -> Result<(), RestoreJournalOnDrop> {
    if is_terminal_phase(journal.payload().phase) {
        return Ok(());
    }
    if let Err(error) = ensure_rollback_required(&mut journal, guard) {
        return Err(RestoreJournalOnDrop {
            error,
            journal: Some(journal),
        });
    }
    if let Err(error) = verify_rollback_main_authority(owner, &journal, preservation) {
        return Err(RestoreJournalOnDrop {
            error,
            journal: Some(journal),
        });
    }
    if let Err(error) = owner
        .restore_preserved_original(preservation)
        .context("restore preserved original during PendingRestoreStartup drop")
    {
        return Err(RestoreJournalOnDrop {
            error,
            journal: Some(journal),
        });
    }
    if let Err(error) = remove_matching_candidate_stage(&journal, journal.payload()) {
        return Err(RestoreJournalOnDrop {
            error,
            journal: Some(journal),
        });
    }
    if let Err(error) = journal
        .transition(guard, RestorePhase::RolledBack)
        .context("transition restore journal to RolledBack during drop")
    {
        return Err(RestoreJournalOnDrop {
            error,
            journal: Some(journal),
        });
    }
    match journal.remove_terminal(guard) {
        Ok(()) => Ok(()),
        Err(error) => Err(RestoreJournalOnDrop {
            error: error.context("remove terminal journal during drop"),
            journal: None,
        }),
    }
}

// --- startup recovery helpers ---

fn diagnose_unjournaled_candidate_stages(owner: &OfflineRestoreOwner) -> anyhow::Result<()> {
    let data_dir = &owner.data_dir;
    super::validate_trusted_directory_parent(data_dir)?;
    let mut diagnostics = 0usize;
    for (index, entry) in fs::read_dir(data_dir)
        .context("read data directory for pre-journal candidate diagnosis")?
        .enumerate()
    {
        if index >= UNJOURNALED_SCAN_MAX_ENTRIES {
            bail!("pre-journal candidate scan reached its directory-entry bound");
        }
        let entry = entry.context("read pre-journal candidate directory entry")?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(operation) = name
            .strip_prefix(".webcodex-restore-")
            .and_then(|name| name.strip_suffix(".candidate"))
        else {
            continue;
        };
        let Ok(operation_id) = Uuid::parse_str(operation) else {
            continue;
        };
        if operation != operation_id.simple().to_string() {
            continue;
        }
        let path = entry.path();
        ensure_direct_child_of_data_dir(data_dir, &path, "unjournaled candidate")?;
        let inspection = (|| -> anyhow::Result<()> {
            let meta = fs::symlink_metadata(&path)?;
            if meta.file_type().is_symlink() || super::is_reparse_point(&meta) || !meta.is_file() {
                bail!("candidate name does not identify a non-reparse regular file");
            }
            let file = open_nofollow_read(&path)?;
            ensure_single_link_file(&file, &path)?;
            Ok(())
        })();
        match inspection {
            Ok(()) => tracing::warn!(stage = %path.display(),
                "unjournaled candidate retained: no durable staging ownership evidence"),
            Err(error) => tracing::warn!(stage = %path.display(), error = %error,
                "unverifiable unjournaled candidate name retained without modification"),
        }
        diagnostics += 1;
        if diagnostics >= UNJOURNALED_SCAN_MAX_DIAGNOSTICS {
            bail!("pre-journal candidate scan reached its diagnostic bound");
        }
    }
    Ok(())
}

fn journal_production_path(journal: &RestoreJournal) -> anyhow::Result<PathBuf> {
    let name = journal.payload().production_db_file_name.as_str();
    validate_single_path_component(name, "production_db_file_name")?;
    Ok(journal.data_dir().join(name))
}

fn is_terminal_phase(phase: RestorePhase) -> bool {
    matches!(phase, RestorePhase::Committed | RestorePhase::RolledBack)
}

fn complete_startup_rollback(
    owner: &OfflineRestoreOwner,
    journal: &mut RestoreJournal,
    preservation: &OriginalPreservation,
) -> anyhow::Result<()> {
    let guard = owner.server_instance_guard();
    ensure_rollback_required(journal, guard)?;
    verify_rollback_main_authority(owner, journal, preservation)?;
    owner
        .restore_preserved_original(preservation)
        .context("restore preserved original during startup rollback")?;
    remove_matching_candidate_stage(journal, journal.payload())?;
    journal
        .transition(guard, RestorePhase::RolledBack)
        .context("transition restore journal to RolledBack during startup rollback")?;
    Ok(())
}

fn ensure_rollback_required(
    journal: &mut RestoreJournal,
    guard: &crate::ServerInstanceGuard,
) -> anyhow::Result<()> {
    // Same-phase transitions verify the guard and exact on-disk payload without rewriting it.
    // Rollback must never overwrite production based only on a cached in-memory phase.
    journal
        .transition(guard, RestorePhase::RollbackRequired)
        .context("transition restore journal to RollbackRequired")
}

/// A valid journal does not authorize replacing an unrelated database installed at its path.
/// Accept the installed candidate (which SQLite may have changed in place), or exact original
/// bytes from a previous partially completed rollback. Absence has no unrelated file to overwrite.
fn verify_rollback_main_authority(
    owner: &OfflineRestoreOwner,
    journal: &RestoreJournal,
    preservation: &OriginalPreservation,
) -> anyhow::Result<()> {
    let path = owner.production_database_path();
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).context("stat production main before rollback"),
        Ok(meta) => {
            if meta.file_type().is_symlink() || super::is_reparse_point(&meta) || !meta.is_file() {
                bail!("unsafe production main blocks restore rollback");
            }
        }
    }
    let mut file = open_nofollow_read(path)?;
    ensure_single_link_file(&file, path)?;
    let identity = SourceFileIdentity::from_open_file(&file)?;
    if SourceFileIdentity::from_source_path(path)? != identity {
        bail!("production main path/handle identity changed before rollback");
    }
    let payload = journal.payload();
    if payload.candidate_production_db_identity() == Some(&identity) {
        return Ok(());
    }
    let size = file.metadata()?.len();
    let original = preservation
        .preserved_files()
        .iter()
        .find(|record| record.suffix().is_empty() && record.present());
    let original_hash = original
        .filter(|record| record.size_bytes() == Some(size))
        .and_then(|record| record.sha256_hex());
    let legacy_candidate_hash =
        if payload.schema_version() == 1 && payload.candidate_size_bytes == size {
            Some(payload.candidate_sha256_hex.as_str())
        } else {
            None
        };
    if original_hash.is_none() && legacy_candidate_hash.is_none() {
        bail!("unrelated production main blocks restore rollback; recovery evidence retained");
    }
    let actual = hash_open_file_bounded(&mut file, size)?;
    if SourceFileIdentity::from_source_path(path)? != identity {
        bail!("production main path identity changed during rollback verification");
    }
    if original_hash == Some(actual.as_str()) || legacy_candidate_hash == Some(actual.as_str()) {
        return Ok(());
    }
    bail!("unrelated production main blocks restore rollback; recovery evidence retained")
}

fn validate_candidate_installed_ready(
    production_db_path: &Path,
    journal: &RestoreJournal,
) -> anyhow::Result<SourceFileIdentity> {
    if journal.payload().phase != RestorePhase::CandidateInstalled {
        bail!("startup candidate validation requires CandidateInstalled phase");
    }
    let journal_production =
        journal_production_path(journal).context("resolve journal production database path")?;
    if production_db_path != journal_production.as_path() {
        bail!(
            "production database path {} does not match restore journal path {}",
            production_db_path.display(),
            journal_production.display()
        );
    }
    super::validate_trusted_directory_parent(journal.data_dir())?;
    ensure_direct_child_of_data_dir(
        journal.data_dir(),
        production_db_path,
        "production database",
    )?;

    let payload = journal.payload();
    let actual_identity = verify_hardened_regular_file_sha256(
        production_db_path,
        payload.candidate_size_bytes,
        &payload.candidate_sha256_hex,
        "production database",
    )?;
    super::require_no_sidecars(production_db_path)?;

    if payload.schema_version() >= 2 {
        let expected_identity = payload
            .candidate_production_db_identity()
            .context("restore journal schema v2 missing candidate_production_db_identity")?;
        if actual_identity != *expected_identity {
            bail!(
                "production database identity mismatch during CandidateInstalled validation for {}",
                production_db_path.display()
            );
        }
    }

    let stage_path = journal.data_dir().join(&payload.candidate_stage_file_name);
    ensure_direct_child_of_data_dir(journal.data_dir(), &stage_path, "candidate stage")?;
    match fs::symlink_metadata(&stage_path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "stat candidate stage {} during CandidateInstalled validation",
                    stage_path.display()
                )
            });
        }
        Ok(_) => bail!(
            "candidate stage {} must not exist when restore journal is CandidateInstalled",
            stage_path.display()
        ),
    }
    Ok(actual_identity)
}

fn validate_startup_attempting_ready(
    production_db_path: &Path,
    journal: &RestoreJournal,
) -> anyhow::Result<()> {
    if journal.payload().phase != RestorePhase::StartupAttempting {
        bail!("startup attempting validation requires StartupAttempting phase");
    }
    let journal_production =
        journal_production_path(journal).context("resolve journal production database path")?;
    if production_db_path != journal_production.as_path() {
        bail!(
            "production database path {} does not match restore journal path {}",
            production_db_path.display(),
            journal_production.display()
        );
    }
    super::validate_trusted_directory_parent(journal.data_dir())?;
    ensure_direct_child_of_data_dir(
        journal.data_dir(),
        production_db_path,
        "production database",
    )?;

    let payload = journal.payload();
    let expected_identity = payload
        .candidate_production_db_identity()
        .context("StartupAttempting journal missing candidate_production_db_identity")?;
    verify_startup_attempting_main_file(production_db_path, expected_identity)?;
    validate_present_sqlite_sidecars_safe(production_db_path, journal.data_dir())?;
    ensure_candidate_stage_absent(journal)?;
    Ok(())
}

fn verify_startup_attempting_main_file(
    production_db_path: &Path,
    expected_identity: &SourceFileIdentity,
) -> anyhow::Result<()> {
    let meta = fs::symlink_metadata(production_db_path).with_context(|| {
        format!(
            "stat production database {} during StartupAttempting validation",
            production_db_path.display()
        )
    })?;
    if meta.file_type().is_symlink() || super::is_reparse_point(&meta) {
        bail!(
            "production database {} must not be a symlink or reparse point",
            production_db_path.display()
        );
    }
    if !meta.is_file() {
        bail!(
            "production database {} is not a regular file",
            production_db_path.display()
        );
    }

    let file = open_nofollow_read(production_db_path)?;
    ensure_single_link_file(&file, production_db_path)?;
    let actual = SourceFileIdentity::from_open_file(&file)
        .context("read StartupAttempting production database identity from open handle")?;
    if actual != *expected_identity {
        bail!(
            "StartupAttempting production database identity mismatch for {}",
            production_db_path.display()
        );
    }
    Ok(())
}

fn validate_present_sqlite_sidecars_safe(
    production_db_path: &Path,
    data_dir: &Path,
) -> anyhow::Result<()> {
    for suffix in SQLITE_SIDECAR_SUFFIXES {
        let sidecar = sqlite_sidecar_path(production_db_path, suffix);
        ensure_direct_child_of_data_dir(data_dir, &sidecar, "production SQLite sidecar")?;
        match fs::symlink_metadata(&sidecar) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("stat production SQLite sidecar {}", sidecar.display())
                });
            }
            Ok(meta) => {
                if meta.file_type().is_symlink() || super::is_reparse_point(&meta) {
                    bail!(
                        "production SQLite sidecar {} must not be a symlink or reparse point",
                        sidecar.display()
                    );
                }
                if !meta.is_file() {
                    bail!(
                        "production SQLite sidecar {} is not a regular file",
                        sidecar.display()
                    );
                }
                let file = open_nofollow_read(&sidecar)?;
                ensure_single_link_file(&file, &sidecar)?;
            }
        }
    }
    Ok(())
}

fn ensure_candidate_stage_absent(journal: &RestoreJournal) -> anyhow::Result<()> {
    let payload = journal.payload();
    let stage_path = journal.data_dir().join(&payload.candidate_stage_file_name);
    ensure_direct_child_of_data_dir(journal.data_dir(), &stage_path, "candidate stage")?;
    match fs::symlink_metadata(&stage_path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| {
            format!(
                "stat candidate stage {} during StartupAttempting validation",
                stage_path.display()
            )
        }),
        Ok(_) => bail!(
            "candidate stage {} must not exist when restore journal is StartupAttempting",
            stage_path.display()
        ),
    }
}

fn sqlite_sidecar_path(base: &Path, suffix: &str) -> PathBuf {
    let mut path = OsString::from(base.as_os_str());
    path.push(suffix);
    PathBuf::from(path)
}

fn remove_matching_candidate_stage(
    journal: &RestoreJournal,
    payload: &RestoreJournalPayload,
) -> anyhow::Result<()> {
    super::validate_trusted_directory_parent(journal.data_dir())?;
    let stage_path = journal.data_dir().join(&payload.candidate_stage_file_name);
    ensure_direct_child_of_data_dir(journal.data_dir(), &stage_path, "candidate stage")?;
    let production_path =
        journal_production_path(journal).context("resolve production path for stage removal")?;
    if stage_path == production_path {
        bail!("candidate stage path must not equal production database path");
    }

    match fs::symlink_metadata(&stage_path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error).with_context(|| {
                format!("stat candidate stage {} for removal", stage_path.display())
            });
        }
        Ok(meta) => {
            if meta.file_type().is_symlink() || super::is_reparse_point(&meta) {
                bail!(
                    "refusing to remove candidate stage {} (symlink or reparse point)",
                    stage_path.display()
                );
            }
            if !meta.is_file() {
                bail!(
                    "refusing to remove candidate stage {} (not a regular file)",
                    stage_path.display()
                );
            }
            if meta.len() != payload.candidate_size_bytes {
                bail!(
                    "candidate stage {} size does not match journal (expected {}, got {})",
                    stage_path.display(),
                    payload.candidate_size_bytes,
                    meta.len()
                );
            }
        }
    }

    let identity = verify_hardened_regular_file_sha256(
        &stage_path,
        payload.candidate_size_bytes,
        &payload.candidate_sha256_hex,
        "candidate stage",
    )?;

    if let Some(expected) = payload.candidate_production_db_identity() {
        if identity != *expected {
            bail!("candidate stage file identity does not match restore journal; file retained");
        }
    }
    if SourceFileIdentity::from_source_path(&stage_path)? != identity {
        bail!("candidate stage path changed before removal; file retained");
    }

    fs::remove_file(&stage_path)
        .with_context(|| format!("remove matching candidate stage {}", stage_path.display()))
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

pub(crate) fn verify_hardened_regular_file_sha256(
    path: &Path,
    expected_size: u64,
    expected_sha256: &str,
    label: &str,
) -> anyhow::Result<SourceFileIdentity> {
    validate_sha256_hex(expected_sha256, &format!("{label} expected SHA-256"))?;
    let meta =
        fs::symlink_metadata(path).with_context(|| format!("stat {label} {}", path.display()))?;
    if meta.file_type().is_symlink() || super::is_reparse_point(&meta) {
        bail!(
            "{label} {} must not be a symlink or reparse point",
            path.display()
        );
    }
    if !meta.is_file() {
        bail!("{label} {} is not a regular file", path.display());
    }
    if meta.len() != expected_size {
        bail!(
            "{label} {} size mismatch (expected {expected_size}, got {})",
            path.display(),
            meta.len()
        );
    }

    let mut file = open_nofollow_read(path)?;
    ensure_single_link_file(&file, path)?;
    if file.metadata()?.len() != expected_size {
        bail!("{label} {} open handle size mismatch", path.display());
    }
    let identity = SourceFileIdentity::from_open_file(&file)
        .with_context(|| format!("read {label} opening identity from handle"))?;
    let actual = hash_open_file_bounded(&mut file, expected_size)?;
    if SourceFileIdentity::from_source_path(path)? != identity
        || file.metadata()?.len() != expected_size
    {
        bail!("{label} path or size changed during SHA-256 verification");
    }
    if actual != expected_sha256 {
        bail!("{label} {} SHA-256 mismatch", path.display());
    }
    Ok(identity)
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
    #[cfg(not(any(unix, windows)))]
    {
        let _ = path;
        bail!("open without follow is not supported on this platform");
    }
    options
        .open(path)
        .with_context(|| format!("open {} without following", path.display()))
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

fn hash_open_file_bounded(file: &mut File, expected_size: u64) -> anyhow::Result<String> {
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
    if hashed != expected_size || file.metadata()?.len() != expected_size {
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
