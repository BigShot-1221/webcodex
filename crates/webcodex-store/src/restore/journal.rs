//! K.2d Phase 5 — durable restore **intent journal** (not filesystem swap or recovery).
//!
//! Records phased restore progress in `data_dir/.webcodex-restore-journal.json` so crash
//! recovery and later swap/startup modules can observe authoritative phase transitions.
//!
//! This module **does not** rename databases, delete sidecars, stage candidates, reinstall
//! preservation copies, or open SQLite. External absence of SQLite handles on production paths
//! remains an operator precondition. Callers must hold [`ServerInstanceGuard`](crate::server_instance::ServerInstanceGuard)
//! (via [`super::OfflineRestoreOwner`]) for every journal operation.
//!
//! # Crash-safety ordering (enforced by later swap/startup modules)
//!
//! - **Prepared** must be durably persisted before any production sidecar or main DB mutation.
//! - **RemovingSidecars** before any sidecar deletion.
//! - **InstallingCandidate** before candidate rename onto production paths.
//! - **CandidateInstalled** after candidate rename completes (pristine swap; journal records
//!   installed production file identity on schema v2).
//! - **StartupAttempting** after startup recovery accepts the swap and **before** production
//!   `Database::open` (tolerates legitimate SQLite WAL/checkpoint mutations on later restarts).
//! - **Committed** only after real server startup has opened `Database` and constructed
//!   `with_session_database` successfully.
//!
//! Phase 4 preservation archives are **not** removed when a terminal journal is deleted.

use super::SourceFileIdentity;
use crate::ServerInstanceGuard;
use anyhow::{bail, Context};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};
use uuid::Uuid;

const JOURNAL_FILE_NAME: &str = ".webcodex-restore-journal.json";
/// Schema written by new restore operations (v2 adds durable production DB file identity).
const JOURNAL_SCHEMA_VERSION: u32 = 2;
const JOURNAL_SCHEMA_VERSION_MIN: u32 = 1;
const JOURNAL_MAX_BYTES: usize = 64 * 1024;
const SHA256_HEX_LEN: usize = 64;
const TEMP_FILE_PREFIX: &str = ".webcodex-restore-journal.";
const TEMP_FILE_SUFFIX: &str = ".tmp";

/// Durable restore lifecycle phase recorded in the journal payload.
///
/// Semantics for crash recovery (swap/startup modules enforce I/O ordering):
///
/// - **Prepared**: production DB and sidecars untouched; candidate staged and original preservation complete.
/// - **RemovingSidecars**: sidecar removal may be partial; recovery must restore originals from preservation.
/// - **InstallingCandidate**: candidate rename may or may not have happened; inspect production/stage hashes.
/// - **CandidateInstalled**: candidate is at the production path with pristine post-swap bytes (no
///   SQLite sidecars); startup recovery transitions to **StartupAttempting** before `Database::open`.
/// - **StartupAttempting**: guarded startup is in progress or resuming after crash; production main
///   file identity must match the journal (byte hash may differ after WAL/checkpoint work).
/// - **RollbackRequired**: restore originals from preservation before any `Database::open`.
/// - **RolledBack** / **Committed**: terminal phases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RestorePhase {
    Prepared,
    RemovingSidecars,
    InstallingCandidate,
    CandidateInstalled,
    StartupAttempting,
    RollbackRequired,
    RolledBack,
    Committed,
}

/// Journal payload (schema versions 1–2). Checksum covers JSON serialization of this struct only.
///
/// Schema v2 appends [`candidate_production_db_identity`](Self::candidate_production_db_identity)
/// at the end so v1 payloads re-serialize without the field (exact legacy field order/checksum).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RestoreJournalPayload {
    schema_version: u32,
    pub(crate) operation_id: Uuid,
    /// Must equal [`operation_id`](Self::operation_id) and the Phase 4 preservation manifest `operation_id`.
    pub(crate) preservation_operation_id: Uuid,
    pub(crate) production_db_file_name: String,
    pub(crate) candidate_backup_file_name: String,
    pub(crate) candidate_stage_file_name: String,
    pub(crate) candidate_size_bytes: u64,
    pub(crate) candidate_sha256_hex: String,
    pub(crate) preservation_manifest_sha256_hex: String,
    pub(crate) original_db_present: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) original_db_sha256_hex: Option<String>,
    pub(crate) require_workflow_session_domain: bool,
    pub(crate) phase: RestorePhase,
    created_at_unix_ms: i64,
    updated_at_unix_ms: i64,
    /// Stable installed production main-file identity (schema v2); omitted on schema v1 journals.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) candidate_production_db_identity: Option<SourceFileIdentity>,
}

impl RestoreJournalPayload {
    /// Build an initial **Prepared** payload for [`RestoreJournal::create`].
    ///
    /// `operation_id` and `preservation_operation_id` (initialized to the same value) must equal the
    /// Phase 4 preservation manifest `operation_id` for the same restore operation;
    /// `preservation_manifest_sha256_hex` must cover that published manifest.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_prepared(
        operation_id: Uuid,
        production_db_file_name: String,
        candidate_backup_file_name: String,
        candidate_stage_file_name: String,
        candidate_size_bytes: u64,
        candidate_sha256_hex: String,
        preservation_manifest_sha256_hex: String,
        original_db_present: bool,
        original_db_sha256_hex: Option<String>,
        require_workflow_session_domain: bool,
        candidate_production_db_identity: SourceFileIdentity,
    ) -> Self {
        let now = Utc::now().timestamp_millis();
        Self {
            schema_version: JOURNAL_SCHEMA_VERSION,
            operation_id,
            preservation_operation_id: operation_id,
            production_db_file_name,
            candidate_backup_file_name,
            candidate_stage_file_name,
            candidate_size_bytes,
            candidate_sha256_hex,
            preservation_manifest_sha256_hex,
            original_db_present,
            original_db_sha256_hex,
            require_workflow_session_domain,
            phase: RestorePhase::Prepared,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
            candidate_production_db_identity: Some(candidate_production_db_identity),
        }
    }

    pub(crate) fn candidate_production_db_identity(&self) -> Option<&SourceFileIdentity> {
        self.candidate_production_db_identity.as_ref()
    }

    pub(crate) fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub(crate) fn created_at_unix_ms(&self) -> i64 {
        self.created_at_unix_ms
    }

    pub(crate) fn updated_at_unix_ms(&self) -> i64 {
        self.updated_at_unix_ms
    }
}

/// On-disk envelope: canonical payload bytes plus lowercase SHA-256 of those bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestoreJournalEnvelope {
    payload: RestoreJournalPayload,
    payload_sha256_hex: String,
}

/// Active or terminal restore journal for one production database under `data_dir`.
#[derive(Debug)]
pub(crate) struct RestoreJournal {
    journal_path: PathBuf,
    data_dir: PathBuf,
    production_db_path: PathBuf,
    payload: RestoreJournalPayload,
}

impl RestoreJournal {
    /// Create and durably publish an initial **Prepared** journal when no active non-terminal journal exists.
    ///
    /// Terminal **Committed** or **RolledBack** journals at the stable path are removed first.
    pub(crate) fn create(
        guard: &ServerInstanceGuard,
        payload: RestoreJournalPayload,
    ) -> anyhow::Result<Self> {
        let guard_db_path = guard.database_path();
        let canonical_data_dir = super::canonical_data_directory(guard_db_path)?;
        validate_guarded_data_dir(&canonical_data_dir)?;
        let production_db_path =
            validate_production_db_under_data_dir(&canonical_data_dir, guard_db_path)?;

        if payload.phase != RestorePhase::Prepared {
            bail!("new restore journal must start in Prepared phase");
        }
        if payload.schema_version != JOURNAL_SCHEMA_VERSION {
            bail!(
                "new restore journal schema_version must be {JOURNAL_SCHEMA_VERSION} (got {})",
                payload.schema_version
            );
        }
        validate_payload_semantics(&payload, &canonical_data_dir, &production_db_path)?;

        let journal_path = journal_path_for_data_dir(&canonical_data_dir);
        reconcile_existing_journal_at_create(&journal_path, guard)?;

        let journal = Self {
            journal_path,
            data_dir: canonical_data_dir,
            production_db_path,
            payload,
        };
        journal.persist_payload(&journal.payload)?;
        Ok(journal)
    }

    /// Load the journal for the guard's production database, or `Ok(None)` when absent.
    ///
    /// Any I/O, parse, schema, checksum, or path validation error fails closed (`Err`).
    pub(crate) fn load_for_guard(guard: &ServerInstanceGuard) -> anyhow::Result<Option<Self>> {
        let guard_db_path = guard.database_path();
        let data_dir = super::canonical_data_directory(guard_db_path)?;
        let journal_path = journal_path_for_data_dir(&data_dir);
        // Only initial absence is optional. Disappearance during open, identity checks,
        // or reading is a failed observation and must not bypass startup recovery.
        match fs::symlink_metadata(&journal_path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("stat restore journal {}", journal_path.display()));
            }
            Ok(_) => {}
        }
        let envelope = read_journal_envelope(&journal_path)?;
        let production_db_path = validate_production_db_under_data_dir(&data_dir, guard_db_path)?;
        let journal = envelope_into_journal(
            envelope,
            journal_path,
            data_dir,
            production_db_path,
        )?;
        verify_journal_guard(guard, &journal)?;
        Ok(Some(journal))
    }

    pub(crate) fn payload(&self) -> &RestoreJournalPayload {
        &self.payload
    }

    pub(crate) fn journal_path(&self) -> &Path {
        &self.journal_path
    }

    pub(crate) fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Advance (or idempotently reaffirm) the journal phase with durable persistence before updating memory.
    pub(crate) fn transition(
        &mut self,
        guard: &ServerInstanceGuard,
        next_phase: RestorePhase,
    ) -> anyhow::Result<()> {
        verify_journal_guard(guard, self)?;
        let current = self.payload.phase;
        if !transition_allowed(current, next_phase) {
            bail!("restore journal transition {current:?} -> {next_phase:?} is not allowed");
        }
        if current == next_phase {
            verify_on_disk_journal_matches(&self.journal_path, &self.payload)?;
            return Ok(());
        }

        let mut next_payload = self.payload.clone();
        next_payload.phase = next_phase;
        next_payload.updated_at_unix_ms = Utc::now().timestamp_millis();
        validate_payload_semantics(&next_payload, &self.data_dir, &self.production_db_path)?;

        verify_on_disk_journal_matches(&self.journal_path, &self.payload)?;

        self.persist_payload(&next_payload)?;
        self.payload = next_payload;
        Ok(())
    }

    /// Durably enter **StartupAttempting** (schema v2 identity required) before production `Database::open`.
    pub(crate) fn enter_startup_attempting(
        &mut self,
        guard: &ServerInstanceGuard,
        identity: SourceFileIdentity,
    ) -> anyhow::Result<()> {
        verify_journal_guard(guard, self)?;
        let current = self.payload.phase;
        if current != RestorePhase::CandidateInstalled {
            bail!(
                "enter_startup_attempting requires CandidateInstalled (got {current:?})"
            );
        }

        let mut next_payload = self.payload.clone();
        if next_payload.schema_version < JOURNAL_SCHEMA_VERSION {
            next_payload.schema_version = JOURNAL_SCHEMA_VERSION;
            next_payload.candidate_production_db_identity = Some(identity);
        } else if let Some(journaled) = next_payload.candidate_production_db_identity.as_ref() {
            if identity != *journaled {
                bail!(
                    "enter_startup_attempting production database identity does not match journaled identity"
                );
            }
        } else {
            bail!("startup attempting requires candidate_production_db_identity on schema v2 journals");
        }
        next_payload.phase = RestorePhase::StartupAttempting;
        next_payload.updated_at_unix_ms = Utc::now().timestamp_millis();
        validate_payload_semantics(&next_payload, &self.data_dir, &self.production_db_path)?;

        verify_on_disk_journal_matches(&self.journal_path, &self.payload)?;
        self.persist_payload(&next_payload)?;
        self.payload = next_payload;
        Ok(())
    }

    /// Remove the journal file when the operation ended in **Committed** or **RolledBack**.
    ///
    /// Missing journal path is idempotent success. On Unix, syncs `data_dir` after removal.
    /// Preservation archives are not deleted.
    pub(crate) fn remove_terminal(self, guard: &ServerInstanceGuard) -> anyhow::Result<()> {
        verify_journal_guard(guard, &self)?;
        if !is_terminal_phase(self.payload.phase) {
            bail!(
                "restore journal remove_terminal requires Committed or RolledBack (got {:?})",
                self.payload.phase
            );
        }
        match fs::symlink_metadata(&self.journal_path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error).with_context(|| {
                format!("stat restore journal {}", self.journal_path.display())
            }),
            Ok(meta) => {
                if meta.file_type().is_symlink() || super::is_reparse_point(&meta) {
                    bail!(
                        "restore journal {} must not be a symlink or reparse point",
                        self.journal_path.display()
                    );
                }
                verify_on_disk_journal_matches(&self.journal_path, &self.payload)?;
                fs::remove_file(&self.journal_path).with_context(|| {
                    format!("remove terminal restore journal {}", self.journal_path.display())
                })?;
                sync_data_directory(&self.data_dir)
            }
        }
    }

    fn persist_payload(&self, payload: &RestoreJournalPayload) -> anyhow::Result<()> {
        let envelope = envelope_from_payload(payload)?;
        let bytes =
            serde_json::to_vec(&envelope).context("serialize restore journal envelope")?;
        if bytes.len() > JOURNAL_MAX_BYTES {
            bail!("restore journal envelope exceeds {JOURNAL_MAX_BYTES} bytes");
        }

        let temp_path = unique_temp_journal_path(&self.data_dir)?;
        let persist_result = (|| -> anyhow::Result<()> {
            {
                let mut file = open_new_private_temp(&temp_path)?;
                file.write_all(&bytes)
                    .with_context(|| format!("write restore journal temp {}", temp_path.display()))?;
                file.sync_all()
                    .with_context(|| format!("sync restore journal temp {}", temp_path.display()))?;
            }
            atomic_replace_same_directory(&temp_path, &self.journal_path)?;
            sync_data_directory(&self.data_dir)
        })();

        if persist_result.is_err() {
            let _ = fs::remove_file(&temp_path);
        }
        persist_result
    }
}

fn journal_path_for_data_dir(data_dir: &Path) -> PathBuf {
    data_dir.join(JOURNAL_FILE_NAME)
}

fn unique_temp_journal_path(data_dir: &Path) -> anyhow::Result<PathBuf> {
    for _ in 0..8 {
        let name = format!(
            "{}{}{}",
            TEMP_FILE_PREFIX,
            Uuid::new_v4().simple(),
            TEMP_FILE_SUFFIX
        );
        let path = data_dir.join(name);
        match fs::symlink_metadata(&path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(path),
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("stat restore journal temp candidate {}", path.display())
                });
            }
            Ok(_) => {}
        }
    }
    bail!("could not allocate unique restore journal temp file name");
}

fn validate_guarded_data_dir(data_dir: &Path) -> anyhow::Result<()> {
    super::validate_trusted_directory_parent(data_dir)?;
    let meta = fs::symlink_metadata(data_dir).with_context(|| {
        format!("stat restore journal data directory {}", data_dir.display())
    })?;
    if meta.file_type().is_symlink() || super::is_reparse_point(&meta) {
        bail!(
            "restore journal data directory {} must not be a symlink or reparse point",
            data_dir.display()
        );
    }
    if !meta.is_dir() {
        bail!(
            "restore journal data directory {} is not a directory",
            data_dir.display()
        );
    }
    Ok(())
}

fn validate_production_db_under_data_dir(
    data_dir: &Path,
    production_db_path: &Path,
) -> anyhow::Result<PathBuf> {
    if production_db_path
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        bail!("production database path must not contain parent-directory (..) components");
    }
    let parent = production_db_path
        .parent()
        .context("production database path has no parent directory")?;
    let Some(file_name) = production_db_path.file_name().and_then(OsStr::to_str) else {
        bail!("production database file name is not UTF-8");
    };
    if file_name.is_empty() {
        bail!("production database file name must not be empty");
    }
    validate_single_path_component(file_name, "production database file name")?;
    if file_name == JOURNAL_FILE_NAME {
        bail!(
            "production database file name {JOURNAL_FILE_NAME:?} is reserved for the restore journal"
        );
    }
    let expected = data_dir.join(file_name);
    if production_db_path != expected.as_path() {
        bail!(
            "production database path must equal data directory joined with its UTF-8 file name (got {}, expected {})",
            production_db_path.display(),
            expected.display()
        );
    }
    let canonical_parent = fs::canonicalize(parent).with_context(|| {
        format!(
            "canonicalize production database parent directory {}",
            parent.display()
        )
    })?;
    if canonical_parent != data_dir {
        bail!(
            "production database parent {} must canonicalize to data directory {} (got {})",
            parent.display(),
            data_dir.display(),
            canonical_parent.display()
        );
    }
    Ok(expected)
}

fn verify_journal_guard(guard: &ServerInstanceGuard, journal: &RestoreJournal) -> anyhow::Result<()> {
    let guard_db_path = guard.database_path();
    if guard_db_path != journal.production_db_path.as_path() {
        bail!(
            "restore journal ServerInstanceGuard database path {} does not match journal production path {}",
            guard_db_path.display(),
            journal.production_db_path.display()
        );
    }
    validate_production_db_under_data_dir(&journal.data_dir, guard_db_path)?;
    Ok(())
}

fn expected_candidate_stage_file_name(operation_id: Uuid) -> String {
    format!(".webcodex-restore-{}.candidate", operation_id.simple())
}

fn verify_on_disk_journal_matches(
    journal_path: &Path,
    expected_payload: &RestoreJournalPayload,
) -> anyhow::Result<()> {
    let envelope = read_journal_envelope(journal_path)?;
    let recomputed = envelope_from_payload(&envelope.payload)?;
    if recomputed.payload_sha256_hex != envelope.payload_sha256_hex {
        bail!("restore journal payload checksum mismatch");
    }
    if envelope.payload != *expected_payload {
        bail!("restore journal on-disk payload does not match in-memory journal");
    }
    Ok(())
}

fn validate_payload_semantics(
    payload: &RestoreJournalPayload,
    data_dir: &Path,
    production_db_path: &Path,
) -> anyhow::Result<()> {
    if payload.schema_version < JOURNAL_SCHEMA_VERSION_MIN
        || payload.schema_version > JOURNAL_SCHEMA_VERSION
    {
        bail!(
            "restore journal schema_version must be {JOURNAL_SCHEMA_VERSION_MIN}..={JOURNAL_SCHEMA_VERSION} (got {})",
            payload.schema_version
        );
    }
    if payload.operation_id.is_nil() {
        bail!("restore journal operation_id must not be nil");
    }
    if payload.preservation_operation_id.is_nil() {
        bail!("restore journal preservation_operation_id must not be nil");
    }
    if payload.preservation_operation_id != payload.operation_id {
        bail!("restore journal preservation_operation_id must equal operation_id");
    }

    let production_name = production_db_path
        .file_name()
        .and_then(OsStr::to_str)
        .context("production database file name is not UTF-8")?;
    if payload.production_db_file_name != production_name {
        bail!(
            "restore journal production_db_file_name must match production database file name"
        );
    }
    validate_single_path_component(
        &payload.production_db_file_name,
        "production_db_file_name",
    )?;
    let expected_stage_name = expected_candidate_stage_file_name(payload.operation_id);
    if payload.candidate_stage_file_name != expected_stage_name {
        bail!(
            "restore journal candidate_stage_file_name must be {}",
            expected_stage_name
        );
    }
    if payload.candidate_stage_file_name == JOURNAL_FILE_NAME {
        bail!("restore journal candidate_stage_file_name must not equal reserved journal file name");
    }
    if payload.candidate_stage_file_name == payload.production_db_file_name {
        bail!(
            "restore journal candidate_stage_file_name must not equal production database file name"
        );
    }
    validate_single_path_component(
        &payload.candidate_stage_file_name,
        "candidate_stage_file_name",
    )?;
    validate_single_path_component(
        &payload.candidate_backup_file_name,
        "candidate_backup_file_name",
    )?;
    if payload.candidate_backup_file_name == payload.production_db_file_name {
        bail!(
            "restore journal candidate_backup_file_name must not equal production database file name"
        );
    }
    super::validate_published_backup_file_name(&payload.candidate_backup_file_name)?;

    validate_sha256_hex_field(&payload.candidate_sha256_hex, "candidate_sha256_hex")?;
    validate_sha256_hex_field(
        &payload.preservation_manifest_sha256_hex,
        "preservation_manifest_sha256_hex",
    )?;

    if payload.candidate_size_bytes == 0 {
        bail!("restore journal candidate_size_bytes must be greater than zero");
    }

    match (
        payload.original_db_present,
        payload.original_db_sha256_hex.as_deref(),
    ) {
        (true, Some(hash)) => validate_sha256_hex_field(hash, "original_db_sha256_hex")?,
        (true, None) => {
            bail!("restore journal original_db_sha256_hex is required when original_db_present is true");
        }
        (false, None) => {}
        (false, Some(_)) => {
            bail!(
                "restore journal original_db_sha256_hex must be absent when original_db_present is false"
            );
        }
    }

    validate_restore_timestamps(payload.created_at_unix_ms, payload.updated_at_unix_ms)?;

    let stage_path = data_dir.join(&payload.candidate_stage_file_name);
    if stage_path.parent() != Some(data_dir) {
        bail!("restore journal candidate_stage_file_name escapes data directory");
    }

    if payload.schema_version == 1 && payload.phase == RestorePhase::StartupAttempting {
        bail!("restore journal schema v1 must not use StartupAttempting phase");
    }

    match payload.schema_version {
        1 => {
            if payload.candidate_production_db_identity.is_some() {
                bail!("restore journal schema v1 must not include candidate_production_db_identity");
            }
        }
        2 => {
            let identity = payload
                .candidate_production_db_identity
                .as_ref()
                .context("restore journal schema v2 requires candidate_production_db_identity")?;
            validate_journal_source_file_identity(identity)?;
        }
        other => {
            bail!("restore journal unsupported schema_version {other}");
        }
    }

    Ok(())
}

fn validate_journal_source_file_identity(identity: &SourceFileIdentity) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        if identity.unix_device().is_none() || identity.unix_inode().is_none() {
            bail!("restore journal candidate_production_db_identity must include unix_device and unix_inode on Unix");
        }
        if identity.windows_volume_serial().is_some() || identity.windows_file_index().is_some() {
            bail!("restore journal candidate_production_db_identity must not include Windows identity fields on Unix");
        }
    }
    #[cfg(windows)]
    {
        if identity.windows_volume_serial().is_none() || identity.windows_file_index().is_none() {
            bail!(
                "restore journal candidate_production_db_identity must include windows_volume_serial and windows_file_index on Windows"
            );
        }
        if identity.unix_device().is_some() || identity.unix_inode().is_some() {
            bail!("restore journal candidate_production_db_identity must not include Unix identity fields on Windows");
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = identity;
        bail!("restore journal candidate_production_db_identity is not supported on this platform");
    }
    Ok(())
}

fn validate_restore_timestamps(created_at_unix_ms: i64, updated_at_unix_ms: i64) -> anyhow::Result<()> {
    if created_at_unix_ms <= 0 {
        bail!("restore journal created_at_unix_ms must be positive");
    }
    if updated_at_unix_ms <= 0 {
        bail!("restore journal updated_at_unix_ms must be positive");
    }
    if updated_at_unix_ms < created_at_unix_ms {
        bail!("restore journal updated_at_unix_ms must not precede created_at_unix_ms");
    }
    Ok(())
}

fn validate_single_path_component(name: &str, label: &str) -> anyhow::Result<()> {
    if name.is_empty() {
        bail!("restore journal {label} must not be empty");
    }
    if name.contains('/') || name.contains('\\') {
        bail!("restore journal {label} must not contain path separators");
    }
    let path = Path::new(name);
    let mut components = path.components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(_)), None) => Ok(()),
        _ => bail!("restore journal {label} must be a single normal path component"),
    }
}

fn validate_sha256_hex_field(value: &str, label: &str) -> anyhow::Result<()> {
    if value.len() != SHA256_HEX_LEN {
        bail!("restore journal {label} must be exactly {SHA256_HEX_LEN} lowercase hex characters");
    }
    if !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("restore journal {label} must be lowercase hexadecimal");
    }
    if value.bytes().any(|byte| byte.is_ascii_uppercase()) {
        bail!("restore journal {label} must use lowercase hexadecimal");
    }
    Ok(())
}

fn sha256_hex_lower(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(SHA256_HEX_LEN);
    for byte in digest {
        out.push(hex_char(byte >> 4));
        out.push(hex_char(byte & 0x0f));
    }
    out
}

fn hex_char(nibble: u8) -> char {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    HEX[nibble as usize] as char
}

fn envelope_from_payload(payload: &RestoreJournalPayload) -> anyhow::Result<RestoreJournalEnvelope> {
    let payload_bytes = serde_json::to_vec(payload).context("serialize restore journal payload")?;
    let payload_sha256_hex = sha256_hex_lower(&payload_bytes);
    Ok(RestoreJournalEnvelope {
        payload: payload.clone(),
        payload_sha256_hex,
    })
}

fn envelope_into_journal(
    envelope: RestoreJournalEnvelope,
    journal_path: PathBuf,
    data_dir: PathBuf,
    production_db_path: PathBuf,
) -> anyhow::Result<RestoreJournal> {
    let recomputed = envelope_from_payload(&envelope.payload)?;
    if recomputed.payload_sha256_hex != envelope.payload_sha256_hex {
        bail!("restore journal payload checksum mismatch");
    }
    validate_payload_semantics(
        &envelope.payload,
        &data_dir,
        &production_db_path,
    )?;
    Ok(RestoreJournal {
        journal_path,
        data_dir,
        production_db_path,
        payload: envelope.payload,
    })
}

fn reconcile_existing_journal_at_create(
    journal_path: &Path,
    guard: &ServerInstanceGuard,
) -> anyhow::Result<()> {
    match fs::symlink_metadata(journal_path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("stat restore journal {}", journal_path.display()));
        }
        Ok(meta) => {
            if meta.file_type().is_symlink() || super::is_reparse_point(&meta) {
                bail!(
                    "restore journal {} must not be a symlink or reparse point",
                    journal_path.display()
                );
            }
            if !meta.is_file() {
                bail!(
                    "restore journal {} is not a regular file",
                    journal_path.display()
                );
            }
        }
    }

    let Some(existing) = RestoreJournal::load_for_guard(guard)? else {
        bail!(
            "restore journal path exists but could not be loaded: {}",
            journal_path.display()
        );
    };

    if is_terminal_phase(existing.payload.phase) {
        existing.remove_terminal(guard)
    } else {
        bail!(
            "active non-terminal restore journal already exists at {}",
            journal_path.display()
        );
    }
}

fn read_journal_envelope(journal_path: &Path) -> anyhow::Result<RestoreJournalEnvelope> {
    let mut file = open_journal_for_read(journal_path)?;
    ensure_single_link_open_journal(&file, journal_path)?;
    verify_journal_open_matches_path(&file, journal_path)?;

    let size_start = file
        .metadata()
        .with_context(|| format!("stat opened restore journal {}", journal_path.display()))?
        .len();
    if size_start > JOURNAL_MAX_BYTES as u64 {
        bail!("restore journal exceeds {JOURNAL_MAX_BYTES} bytes");
    }

    let bytes = read_journal_file_bytes_bounded(&mut file, journal_path, size_start)?;
    verify_journal_open_matches_path(&file, journal_path)?;

    serde_json::from_slice(&bytes).with_context(|| {
        format!("parse restore journal JSON {}", journal_path.display())
    })
}

fn read_journal_file_bytes_bounded(
    file: &mut File,
    journal_path: &Path,
    size_start: u64,
) -> anyhow::Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(size_start as usize);
    (&mut *file).take((JOURNAL_MAX_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .with_context(|| format!("read restore journal {}", journal_path.display()))?;
    if bytes.len() > JOURNAL_MAX_BYTES {
        bail!("restore journal exceeds {JOURNAL_MAX_BYTES} bytes");
    }

    let size_after = file
        .metadata()
        .with_context(|| format!("stat opened restore journal after read {}", journal_path.display()))?
        .len();
    if size_after != size_start {
        bail!(
            "restore journal {} changed size during read ({size_start} -> {size_after})",
            journal_path.display()
        );
    }
    if bytes.len() as u64 != size_start {
        bail!(
            "restore journal {} read length {} does not match file size {size_start}",
            journal_path.display(),
            bytes.len()
        );
    }
    Ok(bytes)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct JournalFileIdentity {
    #[cfg(unix)]
    unix_device: u64,
    #[cfg(unix)]
    unix_inode: u64,
    #[cfg(windows)]
    windows_volume_serial: u32,
    #[cfg(windows)]
    windows_file_index: u64,
}

fn journal_file_identity_from_open(file: &File) -> anyhow::Result<JournalFileIdentity> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let meta = file
            .metadata()
            .context("stat opened restore journal for identity")?;
        Ok(JournalFileIdentity {
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
            return Err(io::Error::last_os_error())
                .context("read restore journal identity from handle");
        }
        let file_index = u64::from(info.nFileIndexHigh) << 32 | u64::from(info.nFileIndexLow);
        Ok(JournalFileIdentity {
            windows_volume_serial: info.dwVolumeSerialNumber,
            windows_file_index: file_index,
        })
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = file;
        bail!("restore journal file identity is not supported on this platform");
    }
}

/// Path-side journal identity without following symlinks or reparse targets.
fn journal_file_identity_from_path(journal_path: &Path) -> anyhow::Result<JournalFileIdentity> {
    let meta = fs::symlink_metadata(journal_path).with_context(|| {
        format!(
            "stat restore journal path for identity {}",
            journal_path.display()
        )
    })?;
    if meta.file_type().is_symlink() || super::is_reparse_point(&meta) {
        bail!(
            "restore journal {} must not be a symlink or reparse point",
            journal_path.display()
        );
    }
    if !meta.is_file() {
        bail!(
            "restore journal {} is not a regular file",
            journal_path.display()
        );
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(JournalFileIdentity {
            unix_device: meta.dev(),
            unix_inode: meta.ino(),
        })
    }
    #[cfg(windows)]
    {
        let file = open_journal_for_read(journal_path)?;
        journal_file_identity_from_open(&file)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = journal_path;
        bail!("restore journal file identity is not supported on this platform");
    }
}

fn verify_journal_open_matches_path(file: &File, journal_path: &Path) -> anyhow::Result<()> {
    let identity_handle = journal_file_identity_from_open(file)?;
    let identity_path = journal_file_identity_from_path(journal_path)?;
    if identity_handle != identity_path {
        bail!(
            "restore journal {} path identity does not match opened handle",
            journal_path.display()
        );
    }
    Ok(())
}

fn open_journal_for_read(journal_path: &Path) -> anyhow::Result<File> {
    let parent_meta = fs::symlink_metadata(
        journal_path
            .parent()
            .context("restore journal path has no parent")?,
    )
    .with_context(|| format!("stat restore journal parent {}", journal_path.display()))?;
    if parent_meta.file_type().is_symlink() || super::is_reparse_point(&parent_meta) {
        bail!("restore journal parent directory must not be a symlink or reparse point");
    }

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
        bail!("restore journal open without following is not supported on this platform");
    }

    options
        .open(journal_path)
        .with_context(|| format!("open restore journal {} without following", journal_path.display()))
}

fn ensure_single_link_open_journal(file: &File, journal_path: &Path) -> anyhow::Result<()> {
    let meta = file
        .metadata()
        .with_context(|| format!("stat opened restore journal {}", journal_path.display()))?;
    if !meta.is_file() {
        bail!(
            "restore journal {} opened handle is not a regular file",
            journal_path.display()
        );
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
        if meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            bail!(
                "restore journal {} opened handle must not be a reparse point",
                journal_path.display()
            );
        }
    }

    let links = hard_link_count_from_open_file(file).with_context(|| {
        format!(
            "inspect hard-link count of restore journal {}",
            journal_path.display()
        )
    })?;
    if links != 1 {
        bail!(
            "restore journal {} must have hard-link count exactly one (got {links})",
            journal_path.display()
        );
    }
    Ok(())
}

fn hard_link_count_from_open_file(file: &File) -> anyhow::Result<u64> {
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
            return Err(io::Error::last_os_error())
                .context("read hard-link count for restore journal");
        }
        Ok(u64::from(info.nNumberOfLinks))
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = file;
        bail!("hard-link inspection is not supported on this platform");
    }
}

fn open_new_private_temp(temp_path: &Path) -> anyhow::Result<File> {
    #[cfg(not(any(unix, windows)))]
    {
        let _ = temp_path;
        bail!("restore journal temp file create is not supported on this platform");
    }
    #[cfg(any(unix, windows))]
    {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        options
            .open(temp_path)
            .with_context(|| format!("create restore journal temp {}", temp_path.display()))
    }
}

fn atomic_replace_same_directory(temp_path: &Path, journal_path: &Path) -> anyhow::Result<()> {
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
        let ok = unsafe {
            MoveFileExW(
                wide(temp_path).as_ptr(),
                wide(journal_path).as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error()).with_context(|| {
                format!(
                    "publish restore journal {} -> {}",
                    temp_path.display(),
                    journal_path.display()
                )
            });
        }
        Ok(())
    }
    #[cfg(unix)]
    {
        fs::rename(temp_path, journal_path).with_context(|| {
            format!(
                "publish restore journal {} -> {}",
                temp_path.display(),
                journal_path.display()
            )
        })
    }
}

/// Directory durability after journal publish (Unix directory `fsync`; Windows best-effort).
///
/// On Windows, `MoveFileExW` with `MOVEFILE_WRITE_THROUGH` flushes file metadata for the rename;
/// there is no portable parent-directory fsync equivalent in this implementation, so directory
/// entry durability on Windows remains **best-effort** (same limitation as backup/preservation publish).
fn sync_data_directory(data_dir: &Path) -> anyhow::Result<()> {
    #[cfg(windows)]
    {
        let _ = data_dir;
        Ok(())
    }
    #[cfg(unix)]
    {
        File::open(data_dir)
            .with_context(|| format!("open data directory for sync {}", data_dir.display()))?
            .sync_all()
            .with_context(|| format!("sync data directory {}", data_dir.display()))
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = data_dir;
        bail!("restore journal directory sync is not supported on this platform");
    }
}

fn is_terminal_phase(phase: RestorePhase) -> bool {
    matches!(phase, RestorePhase::Committed | RestorePhase::RolledBack)
}

fn transition_allowed(current: RestorePhase, next: RestorePhase) -> bool {
    if current == next {
        return true;
    }
    if is_terminal_phase(current) {
        return false;
    }
    match (current, next) {
        (RestorePhase::Prepared, RestorePhase::RemovingSidecars) => true,
        (RestorePhase::RemovingSidecars, RestorePhase::InstallingCandidate) => true,
        (RestorePhase::InstallingCandidate, RestorePhase::CandidateInstalled) => true,
        (RestorePhase::CandidateInstalled, RestorePhase::StartupAttempting) => true,
        (RestorePhase::StartupAttempting, RestorePhase::Committed) => true,
        (RestorePhase::RollbackRequired, RestorePhase::RolledBack) => true,
        (_, RestorePhase::RollbackRequired) if current != RestorePhase::RollbackRequired => true,
        _ => false,
    }
}
