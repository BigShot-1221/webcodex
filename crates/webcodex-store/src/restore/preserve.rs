//! K.2d Phase 4–6 — byte-for-byte preservation, authoritative manifest loading, and original rollback.
//!
//! # Purpose
//!
//! Before any candidate staging or swap, this phase copies the **original** main DB file and
//! optional `-wal`, `-journal`, and `-shm` siblings into
//! `data_dir/restore-preservation/<operation-id>/`. Copies are **not** taken through the SQLite
//! Online Backup API so preservation remains meaningful when the production DB is corrupt or
//! carries a hot rollback journal.
//!
//! # Authority and production impact
//!
//! Phase 4 is **read-only** with respect to production paths: it never opens SQLite on the
//! original, never runs `quick_check`, and never renames, deletes, or truncates production files.
//!
//! [`super::OfflineRestoreOwner::acquire`] holds parent-directory and legacy
//! [`ServerInstanceGuard`](crate::server_instance::ServerInstanceGuard) locks, which excludes
//! managed WebCodex Server and in-crate online-backup workers that respect the same locks.
//! That is **not** a system-wide proof that no other process holds SQLite or file handles on the
//! production database; callers must stop external DB tools before [`preserve_original`].
//!
//! # `-shm` policy
//!
//! `-shm` is preserved **only as forensic evidence**. [`PreservedFileRecord::reinstall_on_rollback`]
//! is `false` for `-shm`. Future rollback (Phase 5+) must **remove** any target `-shm` and allow
//! SQLite to reconstruct it; never reinstall a preserved `-shm`.
//!
//! # Completion and failure
//!
//! Publication of `restore-preserve.json` is the Phase 4 completion marker. Directories without
//! that manifest are **not** authoritative preservation. Incomplete operation directories are left
//! on disk as non-authoritative evidence; this module does not auto-delete completed or failed
//! preservation trees.
//!
//! # Phase 6 — load and rollback
//!
//! [`OfflineRestoreOwner::load_preservation_for_journal`] reloads the published manifest under
//! `data_dir/restore-preservation/<operation-id>/`, checks it against the restore journal, and
//! verifies the archive directory. [`OfflineRestoreOwner::restore_preserved_original`] performs an
//! idempotent production rollback (reinstall main/`-wal`/`-journal`, always remove production
//! `-shm`, never reinstall archived `-shm`). Preservation operation directories are retained.

use super::{OfflineRestoreOwner, RestoreJournalPayload};
use anyhow::{bail, Context};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::ffi::OsStr;
use std::fs::{self, DirBuilder, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use uuid::Uuid;

const RESTORE_PRESERVATION_DIR: &str = "restore-preservation";
const MANIFEST_FILE_NAME: &str = "restore-preserve.json";
const MANIFEST_PARTIAL_SUFFIX: &str = ".partial";
const PRESERVATION_MANIFEST_SCHEMA_VERSION: u32 = 1;
/// Conservative total-byte cap aligned with upgrade snapshot policy (`SNAPSHOT_LIMIT`).
const PRESERVATION_TOTAL_BYTE_CAP: u64 = 16 * 1024 * 1024 * 1024;
const COPY_BUFFER_BYTES: usize = 1024 * 1024;
const PRESERVATION_MANIFEST_MAX_BYTES: usize = 256 * 1024;
const SHA256_HEX_LEN: usize = 64;
const ROLLBACK_TEMP_PREFIX: &str = ".webcodex-restore-rollback-";
const ROLLBACK_TEMP_SUFFIX: &str = ".partial";
/// Upper bound on entries under one preservation operation directory (manifest + four siblings + slack).
const PRESERVATION_OPERATION_DIR_MAX_ENTRIES: usize = 16;

const PRESERVE_SUFFIXES: [&str; 4] = ["", "-wal", "-journal", "-shm"];
/// Crash-retriable production reinstall order (main database last).
const ROLLBACK_REINSTALL_SUFFIX_ORDER: [&str; 3] = ["-journal", "-wal", ""];

/// Successful Phase 4 outcome: published manifest and on-disk copies under one operation directory.
#[derive(Debug, Clone)]
pub(crate) struct OriginalPreservation {
    operation_id: Uuid,
    operation_dir: PathBuf,
    manifest_path: PathBuf,
    manifest: OriginalPreservationManifest,
    /// Lowercase SHA-256 of the published `restore-preserve.json` bytes on disk.
    manifest_sha256_hex: String,
}

impl OriginalPreservation {
    pub(crate) fn operation_id(&self) -> Uuid {
        self.operation_id
    }

    pub(crate) fn operation_directory(&self) -> &Path {
        &self.operation_dir
    }

    pub(crate) fn manifest_path(&self) -> &Path {
        &self.manifest_path
    }

    pub(crate) fn manifest(&self) -> &OriginalPreservationManifest {
        &self.manifest
    }

    pub(crate) fn preserved_files(&self) -> &[PreservedFileRecord] {
        &self.manifest.files
    }

    pub(crate) fn manifest_sha256_hex(&self) -> &str {
        &self.manifest_sha256_hex
    }

    /// Main production database sibling (`""` suffix) presence in the published manifest.
    pub(crate) fn original_db_present(&self) -> bool {
        main_db_record(&self.manifest)
            .map(PreservedFileRecord::present)
            .unwrap_or(false)
    }

    pub(crate) fn original_db_sha256_hex(&self) -> Option<&str> {
        main_db_record(&self.manifest).and_then(|record| record.sha256_hex())
    }
}

/// On-disk manifest written atomically as `restore-preserve.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OriginalPreservationManifest {
    schema_version: u32,
    operation_id: Uuid,
    production_db_file_name: String,
    created_at_unix_ms: i64,
    total_bytes: u64,
    files: Vec<PreservedFileRecord>,
}

impl OriginalPreservationManifest {
    pub(crate) fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub(crate) fn operation_id(&self) -> Uuid {
        self.operation_id
    }

    pub(crate) fn production_db_file_name(&self) -> &str {
        &self.production_db_file_name
    }

    pub(crate) fn created_at_unix_ms(&self) -> i64 {
        self.created_at_unix_ms
    }

    pub(crate) fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
}

/// One production sibling entry in the preservation manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreservedFileRecord {
    suffix: String,
    present: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sha256_hex: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_identity: Option<SourceFileIdentity>,
    reinstall_on_rollback: bool,
}

impl PreservedFileRecord {
    pub(crate) fn suffix(&self) -> &str {
        &self.suffix
    }

    pub(crate) fn present(&self) -> bool {
        self.present
    }

    pub(crate) fn size_bytes(&self) -> Option<u64> {
        self.size_bytes
    }

    pub(crate) fn sha256_hex(&self) -> Option<&str> {
        self.sha256_hex.as_deref()
    }

    pub(crate) fn source_identity(&self) -> Option<&SourceFileIdentity> {
        self.source_identity.as_ref()
    }

    /// When `false` (only `-shm`), the file is forensic-only and must not be reinstalled on rollback.
    pub(crate) fn reinstall_on_rollback(&self) -> bool {
        self.reinstall_on_rollback
    }
}

/// Stable on-disk file identity captured from an open handle (no nightly Windows `MetadataExt`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourceFileIdentity {
    #[serde(skip_serializing_if = "Option::is_none")]
    unix_device: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    unix_inode: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    windows_volume_serial: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    windows_file_index: Option<u64>,
}

impl SourceFileIdentity {
    pub(crate) fn unix_device(&self) -> Option<u64> {
        self.unix_device
    }

    pub(crate) fn unix_inode(&self) -> Option<u64> {
        self.unix_inode
    }

    pub(crate) fn windows_volume_serial(&self) -> Option<u32> {
        self.windows_volume_serial
    }

    pub(crate) fn windows_file_index(&self) -> Option<u64> {
        self.windows_file_index
    }

    pub(crate) fn from_open_file(file: &File) -> anyhow::Result<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let meta = file
                .metadata()
                .context("stat opened preservation source")?;
            Ok(Self {
                unix_device: Some(meta.dev()),
                unix_inode: Some(meta.ino()),
                windows_volume_serial: None,
                windows_file_index: None,
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
                return Err(std::io::Error::last_os_error())
                    .context("read file identity for preservation source");
            }
            let file_index = u64::from(info.nFileIndexHigh) << 32 | u64::from(info.nFileIndexLow);
            Ok(Self {
                unix_device: None,
                unix_inode: None,
                windows_volume_serial: Some(info.dwVolumeSerialNumber),
                windows_file_index: Some(file_index),
            })
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = file;
            bail!("file identity capture is not supported on this platform");
        }
    }

    pub(crate) fn from_source_path(path: &Path) -> anyhow::Result<Self> {
        let file = open_preservation_source(path)?;
        ensure_single_link_open_source(&file, path)?;
        Self::from_open_file(&file)
    }

    pub(crate) fn matches_path(path: &Path, expected: &Self) -> anyhow::Result<()> {
        let actual = Self::from_source_path(path)?;
        if actual != *expected {
            bail!(
                "file {} does not match recorded production database identity",
                path.display()
            );
        }
        Ok(())
    }
}

impl OfflineRestoreOwner {
    /// Phase 4: copy production DB and sidecars byte-for-byte into `restore-preservation/<operation-id>/`.
    ///
    /// Succeeds only after `restore-preserve.json` is published with platform-specific durability:
    /// on Unix, copied files and directory entries are synced where this module opens directories
    /// for `sync_all`; on Windows, same-directory publishes use `MoveFileExW` with
    /// `MOVEFILE_WRITE_THROUGH`, while parent-directory entry durability remains best-effort because
    /// this implementation has no portable directory fsync on Windows. On error, production files are
    /// unchanged; any partial operation directory is non-authoritative.
    pub(crate) fn preserve_original(&self, operation_id: Uuid) -> anyhow::Result<OriginalPreservation> {
        super::validate_trusted_directory_parent(&self.data_dir)?;
        let preservation_root = self.data_dir.join(RESTORE_PRESERVATION_DIR);
        ensure_preservation_parent_directory(&preservation_root)?;

        let operation_dir = preservation_root.join(operation_id.to_string());
        match fs::symlink_metadata(&operation_dir) {
            Ok(_) => {
                bail!(
                    "restore preservation operation directory already exists: {}",
                    operation_dir.display()
                );
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "stat restore preservation operation directory {}",
                        operation_dir.display()
                    )
                });
            }
        }
        create_hardened_operation_directory(&preservation_root, &operation_dir)?;

        let production_db_file_name = self
            .production_db_path
            .file_name()
            .and_then(OsStr::to_str)
            .context("production database file name is not UTF-8")?
            .to_string();

        let manifest_partial_name = format!("{MANIFEST_FILE_NAME}{MANIFEST_PARTIAL_SUFFIX}");
        if production_db_file_name == MANIFEST_FILE_NAME
            || production_db_file_name == manifest_partial_name
        {
            bail!(
                "production database file name {:?} collides with preservation manifest path {:?} or {:?}",
                production_db_file_name,
                MANIFEST_FILE_NAME,
                manifest_partial_name
            );
        }

        let mut total_bytes: u64 = 0;
        let mut file_records = Vec::with_capacity(PRESERVE_SUFFIXES.len());

        for suffix in PRESERVE_SUFFIXES {
            let source_path = if suffix.is_empty() {
                self.production_db_path.clone()
            } else {
                super::sidecar_path(&self.production_db_path, suffix)
            };
            let record = match preserve_one_sibling(
                &source_path,
                suffix,
                &operation_dir,
                &mut total_bytes,
            ) {
                Ok(record) => record,
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!(
                            "preserve production sibling {:?} at {}",
                            suffix,
                            source_path.display()
                        )
                    });
                }
            };
            file_records.push(record);
        }

        let manifest = OriginalPreservationManifest {
            schema_version: PRESERVATION_MANIFEST_SCHEMA_VERSION,
            operation_id,
            production_db_file_name,
            created_at_unix_ms: Utc::now().timestamp_millis(),
            total_bytes,
            files: file_records,
        };

        validate_preservation_manifest_semantics(&manifest)?;

        verify_absent_siblings_unchanged(&self.production_db_path, &manifest.files)?;

        let manifest_path = publish_preservation_manifest(&operation_dir, &manifest)?;
        let manifest_sha256_hex = sha256_hex_of_manifest_path(&manifest_path)?;

        Ok(OriginalPreservation {
            operation_id,
            operation_dir,
            manifest_path,
            manifest,
            manifest_sha256_hex,
        })
    }

    /// Phase 6 (load): authoritative preservation manifest + archive verification for a journal payload.
    pub(crate) fn load_preservation_for_journal(
        &self,
        payload: &RestoreJournalPayload,
    ) -> anyhow::Result<OriginalPreservation> {
        super::validate_trusted_directory_parent(&self.data_dir)?;
        ensure_journal_matches_production_paths(self, payload)?;

        let operation_id = payload.preservation_operation_id;
        let operation_dir = resolve_preservation_operation_dir(&self.data_dir, operation_id)?;
        ensure_hardened_preservation_operation_dir(&operation_dir, &self.data_dir)?;

        let manifest_path = operation_dir.join(MANIFEST_FILE_NAME);
        reject_preservation_manifest_collision_artifacts(&operation_dir)?;

        let (manifest_bytes, manifest_sha256_hex) =
            read_preservation_manifest_bytes(&manifest_path, &operation_dir)?;
        if manifest_sha256_hex != payload.preservation_manifest_sha256_hex {
            bail!("preservation manifest SHA-256 does not match restore journal");
        }

        let manifest: OriginalPreservationManifest =
            serde_json::from_slice(&manifest_bytes).with_context(|| {
                format!(
                    "parse preservation manifest JSON {}",
                    manifest_path.display()
                )
            })?;
        validate_preservation_manifest_semantics(&manifest)?;
        cross_check_manifest_with_journal(&manifest, payload)?;

        verify_preservation_archive_contents(&operation_dir, &manifest)?;

        Ok(OriginalPreservation {
            operation_id,
            operation_dir,
            manifest_path,
            manifest,
            manifest_sha256_hex,
        })
    }

    /// Phase 6 (rollback): idempotently reinstall `reinstall_on_rollback` siblings and remove `-shm`.
    ///
    /// Leaves the preservation operation directory on disk as forensic evidence.
    pub(crate) fn restore_preserved_original(
        &self,
        preservation: &OriginalPreservation,
    ) -> anyhow::Result<()> {
        super::validate_trusted_directory_parent(&self.data_dir)?;
        if preservation.operation_id != preservation.manifest.operation_id() {
            bail!("preservation operation_id does not match manifest operation_id");
        }
        ensure_production_database_under_data_dir(&self.data_dir, &self.production_db_path)?;

        let production_name = preservation.manifest.production_db_file_name();
        if production_db_file_name_from_path(&self.production_db_path)? != production_name {
            bail!("preservation manifest production_db_file_name does not match owner production database");
        }

        let operation_dir = &preservation.operation_dir;
        ensure_hardened_preservation_operation_dir(operation_dir, &self.data_dir)?;
        verify_preservation_archive_contents(operation_dir, &preservation.manifest)?;

        // Always remove production `-shm` before WAL/JOURNAL/DB restores (crash-retriable).
        let shm_path = super::sidecar_path(&self.production_db_path, "-shm");
        safe_remove_if_exists(&shm_path)?;

        for record in preservation.preserved_files() {
            if record.suffix() == "-shm" {
                continue;
            }
            if !record.present() {
                let target = production_path_for_record(&self.production_db_path, production_name, record)?;
                safe_remove_if_exists(&target)?;
            }
        }

        for suffix in ROLLBACK_REINSTALL_SUFFIX_ORDER {
            let Some(record) = preservation
                .preserved_files()
                .iter()
                .find(|record| record.suffix() == suffix)
            else {
                bail!("preservation manifest missing record for suffix {:?}", suffix);
            };
            if !record.present() || !record.reinstall_on_rollback() {
                continue;
            }
            let archive_path =
                archived_path_in_operation_dir(operation_dir, production_name, suffix)?;
            let target =
                production_path_for_record(&self.production_db_path, production_name, record)?;
            install_archive_to_production(
                &archive_path,
                &target,
                record.size_bytes.context("present reinstall record missing size_bytes")?,
                record
                    .sha256_hex()
                    .context("present reinstall record missing sha256_hex")?,
                preservation.operation_id,
                suffix,
                &self.data_dir,
            )?;
        }

        verify_production_matches_manifest(&self.production_db_path, production_name, &preservation.manifest)?;
        sync_directory_for_durability(&self.data_dir)
    }
}

fn preserve_one_sibling(
    source_path: &Path,
    suffix: &str,
    operation_dir: &Path,
    total_bytes: &mut u64,
) -> anyhow::Result<PreservedFileRecord> {
    let reinstall = reinstall_on_rollback(suffix);
    match fs::symlink_metadata(source_path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(PreservedFileRecord {
                suffix: suffix.to_string(),
                present: false,
                size_bytes: None,
                sha256_hex: None,
                source_identity: None,
                reinstall_on_rollback: reinstall,
            });
        }
        Err(error) => {
            return Err(error).with_context(|| format!("stat {}", source_path.display()));
        }
        Ok(meta) => {
            if meta.file_type().is_symlink() || super::is_reparse_point(&meta) {
                bail!(
                    "preservation source {} must not be a symlink or reparse point",
                    source_path.display()
                );
            }
            if !meta.is_file() {
                bail!(
                    "preservation source {} is not a regular file",
                    source_path.display()
                );
            }
        }
    }

    let mut source_file = open_preservation_source(source_path)?;
    ensure_single_link_open_source(&source_file, source_path)?;

    let identity_start = SourceFileIdentity::from_open_file(&source_file)?;
    let size_start = source_file
        .metadata()
        .with_context(|| format!("stat opened {}", source_path.display()))?
        .len();

    let Some(file_name) = source_path.file_name() else {
        bail!("preservation source has no file name");
    };
    let published_name = operation_dir.join(file_name);
    let partial_name = partial_path_for(&published_name);

    let (copied_bytes, sha256_hex) =
        copy_source_to_partial(&mut source_file, &partial_name, total_bytes, size_start)?;

    if copied_bytes != size_start {
        bail!(
            "preservation copy size mismatch for {} (expected {size_start}, copied {copied_bytes})",
            source_path.display()
        );
    }

    atomic_publish_same_directory(&partial_name, &published_name)?;
    sync_operation_directory(operation_dir)?;

    verify_source_stable_during_copy(
        &mut source_file,
        source_path,
        &identity_start,
        size_start,
        &sha256_hex,
    )?;

    Ok(PreservedFileRecord {
        suffix: suffix.to_string(),
        present: true,
        size_bytes: Some(size_start),
        sha256_hex: Some(sha256_hex),
        source_identity: Some(identity_start),
        reinstall_on_rollback: reinstall,
    })
}

fn reinstall_on_rollback(suffix: &str) -> bool {
    matches!(suffix, "" | "-wal" | "-journal")
}

fn open_preservation_source(path: &Path) -> anyhow::Result<File> {
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
        .with_context(|| format!("open preservation source {} without following", path.display()))
}

fn ensure_single_link_open_source(file: &File, path: &Path) -> anyhow::Result<()> {
    let meta = file
        .metadata()
        .with_context(|| format!("stat opened preservation source {}", path.display()))?;
    if !meta.is_file() {
        bail!(
            "preservation source {} opened handle is not a regular file",
            path.display()
        );
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
        if meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            bail!(
                "preservation source {} opened handle must not be a reparse point",
                path.display()
            );
        }
    }

    let links = hard_link_count_from_open_file(file).with_context(|| {
        format!(
            "inspect hard-link count of preservation source {}",
            path.display()
        )
    })?;
    if links != 1 {
        bail!(
            "preservation source {} must have hard-link count exactly one (got {links})",
            path.display()
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
            return Err(std::io::Error::last_os_error())
                .context("read hard-link count for preservation source");
        }
        Ok(u64::from(info.nNumberOfLinks))
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = file;
        bail!("hard-link inspection is not supported on this platform");
    }
}

fn copy_source_to_partial(
    source: &mut File,
    partial: &Path,
    total_bytes: &mut u64,
    expected_size: u64,
) -> anyhow::Result<(u64, String)> {
    total_bytes
        .checked_add(expected_size)
        .filter(|&sum| sum <= PRESERVATION_TOTAL_BYTE_CAP)
        .context("restore preservation total byte cap exceeded before copy")?;
    let mut destination = open_new_partial_destination(partial)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; COPY_BUFFER_BYTES];
    let mut copied: u64 = 0;
    source
        .seek(SeekFrom::Start(0))
        .context("rewind preservation source for copy")?;

    loop {
        let read_limit = (expected_size - copied).min(buffer.len() as u64) as usize;
        if read_limit == 0 {
            ensure_file_eof(source)?;
            break;
        }
        let read = source
            .read(&mut buffer[..read_limit])
            .context("read preservation source")?;
        if read == 0 {
            break;
        }
        let next_total = total_bytes
            .checked_add(read as u64)
            .filter(|&sum| sum <= PRESERVATION_TOTAL_BYTE_CAP)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "restore preservation total byte cap exceeded ({PRESERVATION_TOTAL_BYTE_CAP} bytes)"
                )
            })?;
        destination
            .write_all(&buffer[..read])
            .with_context(|| format!("write preservation partial {}", partial.display()))?;
        hasher.update(&buffer[..read]);
        copied = copied.saturating_add(read as u64);
        *total_bytes = next_total;
    }

    destination
        .sync_all()
        .with_context(|| format!("sync preservation partial {}", partial.display()))?;
    let digest = format!("{:x}", hasher.finalize());
    Ok((copied, digest))
}

fn open_new_partial_destination(partial: &Path) -> anyhow::Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(partial)
        .with_context(|| format!("create preservation partial {}", partial.display()))
}

fn partial_path_for(published: &Path) -> PathBuf {
    let mut name = published.as_os_str().to_os_string();
    name.push(MANIFEST_PARTIAL_SUFFIX);
    PathBuf::from(name)
}

fn verify_absent_siblings_unchanged(
    production_db_path: &Path,
    file_records: &[PreservedFileRecord],
) -> anyhow::Result<()> {
    for record in file_records {
        if record.present() {
            continue;
        }
        let source_path = if record.suffix().is_empty() {
            production_db_path.to_path_buf()
        } else {
            super::sidecar_path(production_db_path, record.suffix())
        };
        match fs::symlink_metadata(&source_path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "re-stat absent preservation sibling {} before manifest publish",
                        source_path.display()
                    )
                });
            }
            Ok(_) => {
                bail!(
                    "preservation sibling {} appeared after being recorded absent",
                    source_path.display()
                );
            }
        }
    }
    Ok(())
}

fn verify_source_stable_during_copy(
    source: &mut File,
    source_path: &Path,
    identity_start: &SourceFileIdentity,
    size_start: u64,
    expected_sha256: &str,
) -> anyhow::Result<()> {
    ensure_single_link_open_source(source, source_path)?;

    let identity_path = SourceFileIdentity::from_source_path(source_path)?;
    if identity_path != *identity_start {
        bail!(
            "preservation source path {} no longer names the same file object",
            source_path.display()
        );
    }

    let identity_handle = SourceFileIdentity::from_open_file(source)?;
    if identity_handle != *identity_start {
        bail!(
            "preservation source handle for {} no longer matches opening identity",
            source_path.display()
        );
    }

    let size_now = source
        .metadata()
        .with_context(|| format!("stat opened source after copy {}", source_path.display()))?
        .len();
    if size_now != size_start {
        bail!(
            "preservation source {} changed size during copy ({size_start} -> {size_now})",
            source_path.display()
        );
    }

    source
        .seek(SeekFrom::Start(0))
        .context("rewind preservation source for stability hash")?;
    let actual = hash_open_file(source, source_path, size_start)?;
    if actual != expected_sha256 {
        bail!(
            "preservation source {} content changed during copy (hash mismatch)",
            source_path.display()
        );
    }

    Ok(())
}

fn publish_preservation_manifest(
    operation_dir: &Path,
    manifest: &OriginalPreservationManifest,
) -> anyhow::Result<PathBuf> {
    let final_path = operation_dir.join(MANIFEST_FILE_NAME);
    let partial_path = partial_path_for(&final_path);

    match (
        fs::symlink_metadata(&partial_path),
        fs::symlink_metadata(&final_path),
    ) {
        (Err(partial_err), Err(final_err))
            if partial_err.kind() == io::ErrorKind::NotFound
                && final_err.kind() == io::ErrorKind::NotFound => {}
        (Ok(_), _) | (_, Ok(_)) => {
            bail!(
                "preservation manifest path already exists under {}",
                operation_dir.display()
            );
        }
        (Err(error), _) => {
            return Err(error).with_context(|| {
                format!(
                    "stat preservation manifest partial {}",
                    partial_path.display()
                )
            });
        }
        (_, Err(error)) => {
            return Err(error).with_context(|| {
                format!(
                    "stat preservation manifest {}",
                    final_path.display()
                )
            });
        }
    }

    {
        let mut file = open_new_partial_destination(&partial_path)?;
        serde_json::to_writer_pretty(&mut file, manifest)
            .with_context(|| format!("serialize {}", partial_path.display()))?;
        file.sync_all()
            .with_context(|| format!("sync manifest partial {}", partial_path.display()))?;
    }
    atomic_publish_same_directory(&partial_path, &final_path)?;
    sync_operation_directory(operation_dir)?;
    Ok(final_path)
}

fn atomic_publish_same_directory(partial: &Path, final_path: &Path) -> anyhow::Result<()> {
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Storage::FileSystem::{MoveFileExW, MOVEFILE_WRITE_THROUGH};

        let wide = |path: &Path| {
            path.as_os_str()
                .encode_wide()
                .chain(Some(0))
                .collect::<Vec<u16>>()
        };
        let ok = unsafe {
            MoveFileExW(
                wide(partial).as_ptr(),
                wide(final_path).as_ptr(),
                MOVEFILE_WRITE_THROUGH,
            )
        };
        if ok == 0 {
            let code = std::io::Error::last_os_error();
            return Err(code).with_context(|| {
                format!(
                    "publish preservation artifact {} -> {}",
                    partial.display(),
                    final_path.display()
                )
            });
        }
        Ok(())
    }
    #[cfg(not(windows))]
    {
        fs::rename(partial, final_path).with_context(|| {
            format!(
                "publish preservation artifact {} -> {}",
                partial.display(),
                final_path.display()
            )
        })
    }
}

/// Directory durability after publishes within the operation directory (Unix only).
///
/// On Windows, `MoveFileExW` with `MOVEFILE_WRITE_THROUGH` flushes file metadata for renames;
/// there is no portable parent-directory fsync equivalent, so operation-directory durability is
/// **best-effort** on Windows (same limitation as backup artifact publication on Windows).
fn sync_operation_directory(operation_dir: &Path) -> anyhow::Result<()> {
    sync_directory_for_durability(operation_dir)
}

/// Sync a directory file descriptor on Unix so directory entry updates are durable.
///
/// On Windows this is a no-op: there is no portable directory fsync in this implementation.
fn sync_directory_for_durability(dir: &Path) -> anyhow::Result<()> {
    #[cfg(windows)]
    {
        let _ = dir;
        Ok(())
    }
    #[cfg(not(windows))]
    {
        File::open(dir)
            .with_context(|| format!("open directory for sync {}", dir.display()))?
            .sync_all()
            .with_context(|| format!("sync directory {}", dir.display()))
    }
}

fn ensure_preservation_parent_directory(preservation_root: &Path) -> anyhow::Result<()> {
    let created_root = match fs::symlink_metadata(preservation_root) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            create_hardened_directory_one_level(preservation_root)?;
            true
        }
        Err(error) => {
            return Err(error).with_context(|| {
                format!("stat preservation root {}", preservation_root.display())
            });
        }
        Ok(meta) => {
            validate_hardened_directory_metadata(preservation_root, &meta)?;
            false
        }
    };

    let meta = fs::symlink_metadata(preservation_root).with_context(|| {
        format!(
            "re-stat preservation root after ensure {}",
            preservation_root.display()
        )
    })?;
    validate_hardened_directory_metadata(preservation_root, &meta)?;

    if created_root {
        if let Some(parent) = preservation_root.parent() {
            sync_directory_for_durability(parent)?;
        }
    }

    super::validate_trusted_directory_parent(preservation_root)
}

fn create_hardened_operation_directory(
    preservation_root: &Path,
    operation_dir: &Path,
) -> anyhow::Result<()> {
    let mut builder = DirBuilder::new();
    builder.recursive(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(operation_dir).with_context(|| {
        format!(
            "create restore preservation operation directory {}",
            operation_dir.display()
        )
    })?;

    let meta = fs::symlink_metadata(operation_dir).with_context(|| {
        format!(
            "stat restore preservation operation directory {}",
            operation_dir.display()
        )
    })?;
    validate_hardened_directory_metadata(operation_dir, &meta)?;

    let parent = operation_dir.parent().with_context(|| {
        format!(
            "preservation operation directory {} has no parent",
            operation_dir.display()
        )
    })?;
    if parent != preservation_root {
        bail!(
            "preservation operation directory parent {} does not match preservation root {}",
            parent.display(),
            preservation_root.display()
        );
    }

    sync_directory_for_durability(preservation_root)
}

fn create_hardened_directory_one_level(dir: &Path) -> anyhow::Result<()> {
    let mut builder = DirBuilder::new();
    builder.recursive(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    match builder.create(dir) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            let meta = fs::symlink_metadata(dir)
                .with_context(|| format!("stat directory {}", dir.display()))?;
            validate_hardened_directory_metadata(dir, &meta)
        }
        Err(error) => Err(error).with_context(|| format!("create directory {}", dir.display())),
    }
}

fn validate_hardened_directory_metadata(dir: &Path, meta: &fs::Metadata) -> anyhow::Result<()> {
    if meta.file_type().is_symlink() || super::is_reparse_point(meta) {
        bail!(
            "preservation directory {} must not be a symlink or reparse point",
            dir.display()
        );
    }
    if !meta.is_dir() {
        bail!("preservation path {} is not a directory", dir.display());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = meta.permissions().mode();
        let owner_rwx = mode & 0o700 == 0o700;
        let no_group_other = mode & 0o077 == 0;
        if !owner_rwx || !no_group_other {
            fs::set_permissions(dir, fs::Permissions::from_mode(0o700)).with_context(|| {
                format!("harden preservation directory {}", dir.display())
            })?;
            let hardened = fs::symlink_metadata(dir).with_context(|| {
                format!(
                    "re-stat preservation directory after permission harden {}",
                    dir.display()
                )
            })?;
            let mode = hardened.permissions().mode();
            if mode & 0o700 != 0o700 || mode & 0o077 != 0 {
                bail!(
                    "preservation directory {} permissions are not owner-only rwx after harden (mode {mode:#o})",
                    dir.display()
                );
            }
        }
    }
    Ok(())
}

fn main_db_record(manifest: &OriginalPreservationManifest) -> Option<&PreservedFileRecord> {
    manifest
        .files
        .iter()
        .find(|record| record.suffix().is_empty())
}

fn production_db_file_name_from_path(production_db_path: &Path) -> anyhow::Result<String> {
    production_db_path
        .file_name()
        .and_then(OsStr::to_str)
        .context("production database file name is not UTF-8")
        .map(str::to_string)
}

fn ensure_journal_matches_production_paths(
    owner: &OfflineRestoreOwner,
    payload: &RestoreJournalPayload,
) -> anyhow::Result<()> {
    let production_name = production_db_file_name_from_path(owner.production_database_path())?;
    if payload.production_db_file_name != production_name {
        bail!("restore journal production_db_file_name does not match offline restore owner");
    }
    if payload.preservation_operation_id != payload.operation_id {
        bail!("restore journal preservation_operation_id must equal operation_id");
    }
    Ok(())
}

fn ensure_production_database_under_data_dir(
    data_dir: &Path,
    production_db_path: &Path,
) -> anyhow::Result<()> {
    let parent = production_db_path
        .parent()
        .context("production database path has no parent directory")?;
    if parent != data_dir {
        bail!(
            "production database parent {} must equal data directory {}",
            parent.display(),
            data_dir.display()
        );
    }
    Ok(())
}

fn resolve_preservation_operation_dir(data_dir: &Path, operation_id: Uuid) -> anyhow::Result<PathBuf> {
    let preservation_root = data_dir.join(RESTORE_PRESERVATION_DIR);
    let operation_dir = preservation_root.join(operation_id.to_string());
    if operation_dir.parent() != Some(&preservation_root) {
        bail!("preservation operation directory escapes preservation root");
    }
    if !super::path_starts_with(&operation_dir, data_dir) {
        bail!("preservation operation directory escapes data directory");
    }
    Ok(operation_dir)
}

fn ensure_hardened_preservation_operation_dir(
    operation_dir: &Path,
    data_dir: &Path,
) -> anyhow::Result<()> {
    let preservation_root = data_dir.join(RESTORE_PRESERVATION_DIR);
    match fs::symlink_metadata(operation_dir) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            bail!(
                "preservation operation directory does not exist: {}",
                operation_dir.display()
            );
        }
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "stat preservation operation directory {}",
                    operation_dir.display()
                )
            });
        }
        Ok(meta) => {
            if meta.file_type().is_symlink() || super::is_reparse_point(&meta) {
                bail!(
                    "preservation operation directory {} must not be a symlink or reparse point",
                    operation_dir.display()
                );
            }
            if !meta.is_dir() {
                bail!(
                    "preservation operation path {} is not a directory",
                    operation_dir.display()
                );
            }
            validate_hardened_directory_metadata(operation_dir, &meta)?;
        }
    }
    if operation_dir.parent() != Some(&preservation_root) {
        bail!("preservation operation directory parent does not match preservation root");
    }
    super::validate_trusted_directory_parent(&preservation_root)?;
    Ok(())
}

fn reject_preservation_manifest_collision_artifacts(operation_dir: &Path) -> anyhow::Result<()> {
    let partial_manifest = operation_dir.join(format!(
        "{MANIFEST_FILE_NAME}{MANIFEST_PARTIAL_SUFFIX}"
    ));
    match fs::symlink_metadata(&partial_manifest) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "stat preservation manifest partial {}",
                    partial_manifest.display()
                )
            });
        }
        Ok(_) => {
            bail!(
                "preservation manifest partial {} indicates incomplete publish",
                partial_manifest.display()
            );
        }
    }
    Ok(())
}

fn read_preservation_manifest_bytes(
    manifest_path: &Path,
    operation_dir: &Path,
) -> anyhow::Result<(Vec<u8>, String)> {
    let parent = manifest_path
        .parent()
        .context("preservation manifest path has no parent")?;
    if parent != operation_dir {
        bail!("preservation manifest path escapes operation directory");
    }

    let mut file = open_preservation_archive_for_read(manifest_path, operation_dir)?;
    ensure_single_link_open_source(&file, manifest_path)?;

    let size = file
        .metadata()
        .with_context(|| format!("stat opened preservation manifest {}", manifest_path.display()))?
        .len();
    if size > PRESERVATION_MANIFEST_MAX_BYTES as u64 {
        bail!("preservation manifest exceeds {PRESERVATION_MANIFEST_MAX_BYTES} bytes");
    }

    let identity_start = SourceFileIdentity::from_open_file(&file)?;
    verify_open_file_matches_path(&file, manifest_path, &identity_start)?;
    let mut bytes = Vec::with_capacity(size as usize);
    (&mut file)
        .take((PRESERVATION_MANIFEST_MAX_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .with_context(|| {
            format!("read preservation manifest {}", manifest_path.display())
        })?;
    if bytes.len() > PRESERVATION_MANIFEST_MAX_BYTES {
        bail!("preservation manifest exceeds {PRESERVATION_MANIFEST_MAX_BYTES} bytes");
    }

    verify_open_file_matches_path(&file, manifest_path, &identity_start)?;
    if file.metadata()?.len() != size || bytes.len() as u64 != size {
        bail!("preservation manifest changed size during read");
    }
    let manifest_sha256_hex = sha256_hex_lower(&bytes);
    Ok((bytes, manifest_sha256_hex))
}

fn sha256_hex_of_manifest_path(manifest_path: &Path) -> anyhow::Result<String> {
    let operation_dir = manifest_path
        .parent()
        .context("preservation manifest path has no parent")?;
    let (bytes, digest) = read_preservation_manifest_bytes(manifest_path, operation_dir)?;
    debug_assert_eq!(digest, sha256_hex_lower(&bytes));
    Ok(digest)
}

fn open_preservation_archive_for_read(path: &Path, operation_dir: &Path) -> anyhow::Result<File> {
    let parent = path
        .parent()
        .context("preservation archive path has no parent")?;
    if parent != operation_dir {
        bail!("preservation archive path escapes operation directory");
    }

    let parent_meta = fs::symlink_metadata(operation_dir).with_context(|| {
        format!(
            "stat preservation operation directory {}",
            operation_dir.display()
        )
    })?;
    if parent_meta.file_type().is_symlink() || super::is_reparse_point(&parent_meta) {
        bail!("preservation operation directory must not be a symlink or reparse point");
    }

    let path_meta = match fs::symlink_metadata(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            bail!(
                "preservation archive file does not exist: {}",
                path.display()
            );
        }
        Err(error) => {
            return Err(error).with_context(|| format!("stat preservation archive {}", path.display()));
        }
        Ok(meta) => meta,
    };
    if path_meta.file_type().is_symlink() || super::is_reparse_point(&path_meta) {
        bail!(
            "preservation archive {} must not be a symlink or reparse point",
            path.display()
        );
    }
    if !path_meta.is_file() {
        bail!(
            "preservation archive {} is not a regular file",
            path.display()
        );
    }

    open_preservation_source(path)
}

fn validate_preservation_manifest_semantics(
    manifest: &OriginalPreservationManifest,
) -> anyhow::Result<()> {
    if manifest.schema_version != PRESERVATION_MANIFEST_SCHEMA_VERSION {
        bail!(
            "preservation manifest schema_version must be {PRESERVATION_MANIFEST_SCHEMA_VERSION} (got {})",
            manifest.schema_version
        );
    }
    if manifest.operation_id.is_nil() {
        bail!("preservation manifest operation_id must not be nil");
    }
    validate_single_path_component(
        &manifest.production_db_file_name,
        "preservation manifest production_db_file_name",
    )?;
    if manifest.production_db_file_name == MANIFEST_FILE_NAME
        || manifest.production_db_file_name
            == format!("{MANIFEST_FILE_NAME}{MANIFEST_PARTIAL_SUFFIX}")
    {
        bail!("preservation manifest production_db_file_name collides with manifest file names");
    }
    validate_preservation_created_at_unix_ms(manifest.created_at_unix_ms)?;

    if manifest.files.len() != PRESERVE_SUFFIXES.len() {
        bail!(
            "preservation manifest must contain exactly {} file records",
            PRESERVE_SUFFIXES.len()
        );
    }

    let mut suffixes = HashSet::new();
    for record in &manifest.files {
        if !suffixes.insert(record.suffix().to_string()) {
            bail!(
                "preservation manifest contains duplicate file suffix {:?}",
                record.suffix()
            );
        }
    }

    let mut summed_present_bytes: u64 = 0;
    for (record, expected_suffix) in manifest.files.iter().zip(PRESERVE_SUFFIXES.iter()) {
        if record.suffix() != *expected_suffix {
            bail!(
                "preservation manifest file suffix {:?} must be {:?}",
                record.suffix(),
                expected_suffix
            );
        }
        validate_preserved_file_record(record)?;

        if record.present() {
            let size = record
                .size_bytes
                .context("present preservation record missing size_bytes")?;
            summed_present_bytes = summed_present_bytes
                .checked_add(size)
                .ok_or_else(|| anyhow::anyhow!("preservation manifest total_bytes overflow"))?;
        }
    }

    if manifest.total_bytes != summed_present_bytes {
        bail!(
            "preservation manifest total_bytes {} does not match summed present sizes {}",
            manifest.total_bytes,
            summed_present_bytes
        );
    }
    if manifest.total_bytes > PRESERVATION_TOTAL_BYTE_CAP {
        bail!(
            "preservation manifest total_bytes exceeds cap ({PRESERVATION_TOTAL_BYTE_CAP})"
        );
    }

    Ok(())
}

/// Rejects non-positive or implausible future `created_at_unix_ms` on loaded manifests.
fn validate_preservation_created_at_unix_ms(created_at_unix_ms: i64) -> anyhow::Result<()> {
    if created_at_unix_ms <= 0 {
        bail!("preservation manifest created_at_unix_ms must be positive");
    }
    const MAX_FUTURE_SKEW_MS: i64 = 5 * 60 * 1_000;
    let now_ms = Utc::now().timestamp_millis();
    if created_at_unix_ms > now_ms.saturating_add(MAX_FUTURE_SKEW_MS) {
        bail!("preservation manifest created_at_unix_ms is unreasonably far in the future");
    }
    Ok(())
}

fn validate_preserved_file_record(record: &PreservedFileRecord) -> anyhow::Result<()> {
    let expected_reinstall = reinstall_on_rollback(record.suffix());
    if record.reinstall_on_rollback != expected_reinstall {
        bail!(
            "preservation record suffix {:?} reinstall_on_rollback must be {expected_reinstall}",
            record.suffix()
        );
    }

    match record.present {
        true => {
            let size = record
                .size_bytes
                .context("present preservation record missing size_bytes")?;
            let hash = record
                .sha256_hex
                .as_deref()
                .context("present preservation record missing sha256_hex")?;
            validate_sha256_hex_field(hash, "preservation record sha256_hex")?;
            let identity = record
                .source_identity
                .as_ref()
                .context("present preservation record missing source_identity")?;
            validate_source_file_identity(identity)?;
            // SQLite may leave empty WAL, rollback-journal, or SHM files.
            // Only the main database must contain bytes.
            if size == 0 && record.suffix().is_empty() {
                bail!("present main database preservation record must not be empty");
            }
        }
        false => {
            if record.size_bytes.is_some() {
                bail!("absent preservation record must not include size_bytes");
            }
            if record.sha256_hex.is_some() {
                bail!("absent preservation record must not include sha256_hex");
            }
            if record.source_identity.is_some() {
                bail!("absent preservation record must not include source_identity");
            }
        }
    }
    Ok(())
}

fn validate_source_file_identity(identity: &SourceFileIdentity) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        if identity.unix_device.is_none() || identity.unix_inode.is_none() {
            bail!("preservation source_identity must include unix_device and unix_inode on Unix");
        }
        if identity.windows_volume_serial.is_some() || identity.windows_file_index.is_some() {
            bail!("preservation source_identity must not include Windows identity fields on Unix");
        }
    }
    #[cfg(windows)]
    {
        if identity.windows_volume_serial.is_none() || identity.windows_file_index.is_none() {
            bail!(
                "preservation source_identity must include windows_volume_serial and windows_file_index on Windows"
            );
        }
        if identity.unix_device.is_some() || identity.unix_inode.is_some() {
            bail!("preservation source_identity must not include Unix identity fields on Windows");
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = identity;
        bail!("preservation source_identity is not supported on this platform");
    }
    Ok(())
}

fn cross_check_manifest_with_journal(
    manifest: &OriginalPreservationManifest,
    payload: &RestoreJournalPayload,
) -> anyhow::Result<()> {
    if manifest.operation_id() != payload.preservation_operation_id {
        bail!("preservation manifest operation_id does not match restore journal preservation_operation_id");
    }
    if manifest.production_db_file_name() != payload.production_db_file_name {
        bail!("preservation manifest production_db_file_name does not match restore journal");
    }

    let main_record =
        main_db_record(manifest).context("preservation manifest missing main database record")?;
    if main_record.present() != payload.original_db_present {
        bail!("preservation manifest main database presence does not match restore journal original_db_present");
    }
    match (main_record.present(), payload.original_db_sha256_hex.as_deref()) {
        (true, Some(journal_hash)) => {
            let manifest_hash = main_record
                .sha256_hex()
                .context("present main database record missing sha256_hex")?;
            if manifest_hash != journal_hash {
                bail!("preservation manifest main database SHA-256 does not match restore journal original_db_sha256_hex");
            }
        }
        (false, None) => {}
        (true, None) => {
            bail!("restore journal original_db_sha256_hex required when original_db_present is true");
        }
        (false, Some(_)) => {
            bail!("restore journal original_db_sha256_hex must be absent when original_db_present is false");
        }
    }
    Ok(())
}

fn verify_preservation_archive_contents(
    operation_dir: &Path,
    manifest: &OriginalPreservationManifest,
) -> anyhow::Result<()> {
    let production_name = manifest.production_db_file_name();
    let mut expected_present: HashSet<String> = HashSet::new();
    expected_present.insert(MANIFEST_FILE_NAME.to_string());

    for record in &manifest.files {
        if !record.present() {
            let archived =
                archived_path_in_operation_dir(operation_dir, production_name, record.suffix())?;
            match fs::symlink_metadata(&archived) {
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!(
                            "stat unexpected preservation archive for absent sibling {}",
                            archived.display()
                        )
                    });
                }
                Ok(_) => {
                    bail!(
                        "preservation archive {} exists for absent manifest record {:?}",
                        archived.display(),
                        record.suffix()
                    );
                }
            }
            continue;
        }

        let archived =
            archived_path_in_operation_dir(operation_dir, production_name, record.suffix())?;
        let file_name = archived
            .file_name()
            .and_then(OsStr::to_str)
            .context("preservation archive file name is not UTF-8")?;
        expected_present.insert(file_name.to_string());

        let size = record
            .size_bytes
            .context("present preservation record missing size_bytes")?;
        let hash = record
            .sha256_hex()
            .context("present preservation record missing sha256_hex")?;

        verify_archive_file_matches_record(&archived, operation_dir, size, hash)?;
    }

    let entries = fs::read_dir(operation_dir).with_context(|| {
        format!(
            "read preservation operation directory {}",
            operation_dir.display()
        )
    })?;
    let mut entry_count = 0usize;
    for entry in entries {
        let entry = entry.with_context(|| {
            format!(
                "read preservation operation directory entry {}",
                operation_dir.display()
            )
        })?;
        entry_count += 1;
        if entry_count > PRESERVATION_OPERATION_DIR_MAX_ENTRIES {
            bail!("preservation operation directory contains too many entries");
        }

        let entry_meta = fs::symlink_metadata(entry.path()).with_context(|| {
            format!(
                "stat preservation operation directory entry {}",
                entry.path().display()
            )
        })?;
        if entry_meta.file_type().is_symlink() || super::is_reparse_point(&entry_meta) {
            bail!(
                "preservation operation directory entry {} must not be a symlink or reparse point",
                entry.path().display()
            );
        }

        let entry_name = entry.file_name();
        let Some(name) = entry_name.to_str() else {
            bail!(
                "preservation operation directory entry {:?} is not UTF-8",
                entry.file_name()
            );
        };

        if name == format!("{MANIFEST_FILE_NAME}{MANIFEST_PARTIAL_SUFFIX}") {
            bail!(
                "preservation operation directory contains incomplete manifest {}",
                entry.path().display()
            );
        }

        if !expected_present.contains(name) {
            bail!(
                "preservation operation directory contains unexpected entry {}",
                entry.path().display()
            );
        }
    }

    Ok(())
}

fn verify_archive_file_matches_record(
    archive_path: &Path,
    operation_dir: &Path,
    expected_size: u64,
    expected_hash: &str,
) -> anyhow::Result<()> {
    let mut file = open_preservation_archive_for_read(archive_path, operation_dir)?;
    ensure_single_link_open_source(&file, archive_path)?;

    let actual_size = file
        .metadata()
        .with_context(|| format!("stat opened preservation archive {}", archive_path.display()))?
        .len();
    if actual_size != expected_size {
        bail!(
            "preservation archive {} size mismatch (expected {expected_size}, got {actual_size})",
            archive_path.display()
        );
    }

    file.seek(SeekFrom::Start(0))
        .with_context(|| format!("rewind preservation archive {}", archive_path.display()))?;
    let actual_hash = hash_open_file(&mut file, archive_path, expected_size)?;
    if actual_hash != expected_hash {
        bail!(
            "preservation archive {} SHA-256 does not match manifest",
            archive_path.display()
        );
    }
    Ok(())
}

fn archived_path_in_operation_dir(
    operation_dir: &Path,
    production_db_file_name: &str,
    suffix: &str,
) -> anyhow::Result<PathBuf> {
    let file_name = archived_file_name(production_db_file_name, suffix)?;
    Ok(operation_dir.join(file_name))
}

fn archived_file_name(production_db_file_name: &str, suffix: &str) -> anyhow::Result<String> {
    if suffix.is_empty() {
        validate_single_path_component(production_db_file_name, "production_db_file_name")?;
        return Ok(production_db_file_name.to_string());
    }
    let combined = format!("{production_db_file_name}{suffix}");
    validate_single_path_component(&combined, "preservation archive file name")?;
    Ok(combined)
}

fn production_path_for_record(
    production_db_path: &Path,
    production_db_file_name: &str,
    record: &PreservedFileRecord,
) -> anyhow::Result<PathBuf> {
    if record.suffix().is_empty() {
        let name = production_db_file_name_from_path(production_db_path)?;
        if name != production_db_file_name {
            bail!("production database file name mismatch");
        }
        return Ok(production_db_path.to_path_buf());
    }
    Ok(super::sidecar_path(production_db_path, record.suffix()))
}

fn install_archive_to_production(
    archive_path: &Path,
    target_path: &Path,
    expected_size: u64,
    expected_hash: &str,
    operation_id: Uuid,
    suffix: &str,
    data_dir: &Path,
) -> anyhow::Result<()> {
    if target_path.parent() != Some(data_dir) {
        bail!("production rollback target escapes data directory");
    }

    if production_target_already_matches(target_path, expected_size, expected_hash)? {
        return Ok(());
    }

    reject_unsafe_existing_production_target(target_path)?;

    let operation_dir = archive_path
        .parent()
        .context("preservation archive path has no parent")?;
    let temp_path = rollback_temp_path(data_dir, operation_id, suffix)?;
    remove_stale_rollback_temp(&temp_path)?;
    let install_result = (|| -> anyhow::Result<()> {
        copy_archive_into_temp_verified(
            archive_path,
            operation_dir,
            &temp_path,
            expected_size,
            expected_hash,
        )?;
        atomic_replace_production_sibling(&temp_path, target_path)?;
        Ok(())
    })();

    if install_result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    install_result
}

fn production_target_already_matches(
    target_path: &Path,
    expected_size: u64,
    expected_hash: &str,
) -> anyhow::Result<bool> {
    let meta = match fs::symlink_metadata(target_path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("stat production rollback target {}", target_path.display()));
        }
        Ok(meta) => meta,
    };
    if meta.file_type().is_symlink() || super::is_reparse_point(&meta) {
        bail!(
            "production rollback target {} must not be a symlink or reparse point",
            target_path.display()
        );
    }
    if !meta.is_file() {
        bail!(
            "production rollback target {} is not a regular file",
            target_path.display()
        );
    }
    if meta.len() != expected_size {
        return Ok(false);
    }

    let mut file = open_preservation_source(target_path)?;
    ensure_single_link_open_source(&file, target_path)?;
    let actual_hash = hash_open_file(&mut file, target_path, expected_size)?;
    Ok(actual_hash == expected_hash)
}

fn reject_unsafe_existing_production_target(target_path: &Path) -> anyhow::Result<()> {
    match fs::symlink_metadata(target_path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| {
            format!(
                "stat production rollback target {}",
                target_path.display()
            )
        }),
        Ok(meta) => {
            if meta.file_type().is_symlink() || super::is_reparse_point(&meta) {
                bail!(
                    "production rollback target {} must not be a symlink or reparse point",
                    target_path.display()
                );
            }
            if !meta.is_file() {
                bail!(
                    "production rollback target {} is not a regular file",
                    target_path.display()
                );
            }
            let file = open_preservation_source(target_path)?;
            ensure_single_link_open_source(&file, target_path)?;
            Ok(())
        }
    }
}

fn copy_archive_into_temp_verified(
    archive_path: &Path,
    operation_dir: &Path,
    temp_path: &Path,
    expected_size: u64,
    expected_hash: &str,
) -> anyhow::Result<()> {
    if expected_size > PRESERVATION_TOTAL_BYTE_CAP {
        bail!("rollback archive exceeds preservation total byte cap");
    }
    let mut source = open_preservation_archive_for_read(archive_path, operation_dir)?;
    ensure_single_link_open_source(&source, archive_path)?;

    if source.metadata()?.len() != expected_size {
        bail!(
            "preservation archive {} size changed before rollback copy",
            archive_path.display()
        );
    }

    let identity = SourceFileIdentity::from_open_file(&source)?;
    verify_open_file_matches_path(&source, archive_path, &identity)?;
    source
        .seek(SeekFrom::Start(0))
        .with_context(|| format!("rewind preservation archive {}", archive_path.display()))?;

    let mut destination = open_new_partial_destination(temp_path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; COPY_BUFFER_BYTES];
    let mut copied: u64 = 0;

    loop {
        let limit = (expected_size - copied).min(buffer.len() as u64) as usize;
        if limit == 0 {
            ensure_file_eof(&mut source)?;
            break;
        }
        let read = source
            .read(&mut buffer[..limit])
            .with_context(|| format!("read preservation archive {}", archive_path.display()))?;
        if read == 0 {
            break;
        }
        destination
            .write_all(&buffer[..read])
            .with_context(|| format!("write rollback temp {}", temp_path.display()))?;
        hasher.update(&buffer[..read]);
        copied = copied.saturating_add(read as u64);
    }

    if copied != expected_size {
        bail!(
            "rollback copy size mismatch for {} (expected {expected_size}, copied {copied})",
            archive_path.display()
        );
    }

    let digest = format!("{:x}", hasher.finalize());
    if digest != expected_hash {
        bail!(
            "rollback copy hash mismatch for preservation archive {}",
            archive_path.display()
        );
    }

    verify_open_file_matches_path(&source, archive_path, &identity)?;
    if source.metadata()?.len() != expected_size {
        bail!("preservation archive changed size during rollback copy");
    }
    destination
        .sync_all()
        .with_context(|| format!("sync rollback temp {}", temp_path.display()))?;
    Ok(())
}

fn remove_stale_rollback_temp(temp_path: &Path) -> anyhow::Result<()> {
    match fs::symlink_metadata(temp_path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("stat rollback temp {}", temp_path.display())),
        Ok(meta) => {
            if meta.file_type().is_symlink() || super::is_reparse_point(&meta) {
                bail!(
                    "rollback temp {} must not be a symlink or reparse point",
                    temp_path.display()
                );
            }
            if !meta.is_file() {
                bail!("rollback temp {} is not a regular file", temp_path.display());
            }
            fs::remove_file(temp_path)
                .with_context(|| format!("remove stale rollback temp {}", temp_path.display()))
        }
    }
}

fn rollback_temp_path(data_dir: &Path, operation_id: Uuid, suffix: &str) -> anyhow::Result<PathBuf> {
    let suffix_label = if suffix.is_empty() {
        "db"
    } else {
        suffix.strip_prefix('-').unwrap_or(suffix)
    };
    let file_name = format!(
        "{ROLLBACK_TEMP_PREFIX}{}-{suffix_label}{ROLLBACK_TEMP_SUFFIX}",
        operation_id.simple()
    );
    validate_single_path_component(&file_name, "rollback temp file name")?;
    let path = data_dir.join(&file_name);
    if path.parent() != Some(data_dir) {
        bail!("rollback temp path escapes data directory");
    }
    Ok(path)
}

fn atomic_replace_production_sibling(temp_path: &Path, target_path: &Path) -> anyhow::Result<()> {
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

        let dest_exists = match fs::symlink_metadata(target_path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => false,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "stat production rollback target {}",
                        target_path.display()
                    )
                });
            }
            Ok(_) => true,
        };

        let flags = if dest_exists {
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH
        } else {
            MOVEFILE_WRITE_THROUGH
        };

        let ok = unsafe {
            MoveFileExW(
                wide(temp_path).as_ptr(),
                wide(target_path).as_ptr(),
                flags,
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error()).with_context(|| {
                format!(
                    "install production rollback {} -> {}",
                    temp_path.display(),
                    target_path.display()
                )
            });
        }
        Ok(())
    }
    #[cfg(not(windows))]
    {
        fs::rename(temp_path, target_path).with_context(|| {
            format!(
                "install production rollback {} -> {}",
                temp_path.display(),
                target_path.display()
            )
        })
    }
}

fn safe_remove_if_exists(target_path: &Path) -> anyhow::Result<()> {
    match fs::symlink_metadata(target_path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("stat production path {}", target_path.display())),
        Ok(meta) => {
            if meta.file_type().is_symlink() || super::is_reparse_point(&meta) {
                bail!(
                    "production path {} must not be a symlink or reparse point",
                    target_path.display()
                );
            }
            if !meta.is_file() {
                bail!("production path {} is not a regular file", target_path.display());
            }
            let file = open_preservation_source(target_path)?;
            ensure_single_link_open_source(&file, target_path)?;
            fs::remove_file(target_path).with_context(|| {
                format!("remove production path {}", target_path.display())
            })
        }
    }
}

fn verify_production_matches_manifest(
    production_db_path: &Path,
    production_db_file_name: &str,
    manifest: &OriginalPreservationManifest,
) -> anyhow::Result<()> {
    let shm_path = super::sidecar_path(production_db_path, "-shm");
    match fs::symlink_metadata(&shm_path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| format!("stat production -shm {}", shm_path.display()));
        }
        Ok(_) => {
            bail!("production -shm {} must not exist after rollback", shm_path.display());
        }
    }

    for record in &manifest.files {
        let target = production_path_for_record(production_db_path, production_db_file_name, record)?;
        if !record.present() {
            match fs::symlink_metadata(&target) {
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!(
                            "stat production path for absent preservation record {}",
                            target.display()
                        )
                    });
                }
                Ok(_) => {
                    bail!(
                        "production path {} must not exist for absent preservation record {:?}",
                        target.display(),
                        record.suffix()
                    );
                }
            }
            continue;
        }

        if record.suffix() == "-shm" || !record.reinstall_on_rollback() {
            continue;
        }

        let size = record
            .size_bytes
            .context("present preservation record missing size_bytes")?;
        let hash = record
            .sha256_hex()
            .context("present preservation record missing sha256_hex")?;
        verify_production_file_matches_record(&target, size, hash)?;
    }
    Ok(())
}

fn verify_production_file_matches_record(
    target_path: &Path,
    expected_size: u64,
    expected_hash: &str,
) -> anyhow::Result<()> {
    let mut file = open_preservation_source(target_path)?;
    ensure_single_link_open_source(&file, target_path)?;

    let actual_size = file
        .metadata()
        .with_context(|| format!("stat opened production path {}", target_path.display()))?
        .len();
    if actual_size != expected_size {
        bail!(
            "production path {} size mismatch after rollback (expected {expected_size}, got {actual_size})",
            target_path.display()
        );
    }

    file.seek(SeekFrom::Start(0))
        .with_context(|| format!("rewind production path {}", target_path.display()))?;
    let actual_hash = hash_open_file(&mut file, target_path, expected_size)?;
    if actual_hash != expected_hash {
        bail!(
            "production path {} SHA-256 does not match preservation manifest after rollback",
            target_path.display()
        );
    }
    Ok(())
}

/// Verify both the opened object and the no-follow path still name the expected file.
fn verify_open_file_matches_path(
    file: &File,
    path: &Path,
    expected_identity: &SourceFileIdentity,
) -> anyhow::Result<()> {
    ensure_single_link_open_source(file, path)?;
    let handle_identity = SourceFileIdentity::from_open_file(file)?;
    let path_identity = SourceFileIdentity::from_source_path(path)?;
    if handle_identity != *expected_identity || path_identity != *expected_identity {
        bail!("preservation file {} changed identity during read", path.display());
    }
    Ok(())
}

/// Read at most one byte beyond a validated size; no unbounded EOF search.
fn ensure_file_eof(file: &mut File) -> anyhow::Result<()> {
    let mut extra = [0u8; 1];
    if file.read(&mut extra).context("check preservation file end")? != 0 {
        bail!("preservation file exceeds its validated size");
    }
    Ok(())
}

fn hash_open_file(file: &mut File, path: &Path, expected_size: u64) -> anyhow::Result<String> {
    if expected_size > PRESERVATION_TOTAL_BYTE_CAP {
        bail!("preservation file exceeds total byte cap");
    }
    let identity = SourceFileIdentity::from_open_file(file)?;
    verify_open_file_matches_path(file, path, &identity)?;
    if file.metadata()?.len() != expected_size {
        bail!("preservation file {} changed size before hash", path.display());
    }
    file.seek(SeekFrom::Start(0))
        .context("rewind file for SHA-256")?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; COPY_BUFFER_BYTES];
    let mut remaining = expected_size;
    while remaining != 0 {
        let limit = remaining.min(buffer.len() as u64) as usize;
        let read = file.read(&mut buffer[..limit]).context("read file for SHA-256")?;
        if read == 0 {
            bail!("preservation file {} ended before its validated size", path.display());
        }
        hasher.update(&buffer[..read]);
        remaining -= read as u64;
    }
    ensure_file_eof(file)?;
    verify_open_file_matches_path(file, path, &identity)?;
    if file.metadata()?.len() != expected_size {
        bail!("preservation file {} changed size during hash", path.display());
    }
    Ok(format!("{:x}", hasher.finalize()))
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

fn validate_sha256_hex_field(value: &str, label: &str) -> anyhow::Result<()> {
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
