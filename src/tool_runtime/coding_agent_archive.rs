//! Immutable Server-side routing receipts for Runner-owned uncertain Runs.
//! Publication precedes acknowledgement. The receipt is not terminal truth:
//! recovery still observes the exact owning Runner, never searches for a new owner.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use webcodex_core::coding_agent::{
    archive_identity_fingerprint, validate_authority_fingerprint, validate_run_id,
    CodingAgentRunSnapshot,
};

const MAX_RECEIPT_BYTES: u64 = 4096;
const RECEIPT_SCHEMA: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ArchiveRoute {
    schema: u32,
    pub(super) run_id: String,
    pub(super) client_id: String,
    pub(super) authority_fingerprint: String,
    pub(super) identity: String,
}

impl ArchiveRoute {
    pub(super) fn from_snapshot(
        client_id: &str,
        run: &CodingAgentRunSnapshot,
    ) -> Result<Self, String> {
        if !run.state.terminal()
            || !run
                .runtime_project_id
                .starts_with(&format!("agent:{client_id}:"))
        {
            return Err("ACP archive receipt has invalid owner or state".into());
        }
        webcodex_core::coding_agent::validate_coding_agent_run_snapshot(run)?;
        Ok(Self {
            schema: RECEIPT_SCHEMA,
            run_id: run.run_id.clone(),
            client_id: client_id.to_owned(),
            authority_fingerprint: run.authority_fingerprint.clone(),
            identity: archive_identity_fingerprint(run),
        })
    }

    pub(super) fn matches(&self, client_id: &str, run: &CodingAgentRunSnapshot) -> bool {
        self.client_id == client_id
            && self.run_id == run.run_id
            && self.authority_fingerprint == run.authority_fingerprint
            && self.identity == archive_identity_fingerprint(run)
    }

    fn validate(&self, run_id: &str) -> Result<(), String> {
        validate_run_id(&self.run_id)?;
        validate_authority_fingerprint(&self.authority_fingerprint)?;
        if self.schema != RECEIPT_SCHEMA
            || self.run_id != run_id
            || self.client_id.is_empty()
            || self.client_id.len() > 512
            || self.identity.len() != 64
            || !self
                .identity
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err("ACP archive routing receipt is invalid".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(super) struct ArchiveRoutes {
    root: PathBuf,
}

impl ArchiveRoutes {
    pub(super) fn new(private_dir: &Path) -> Self {
        Self {
            root: private_dir.join("coding-agent-archive-routes-v1"),
        }
    }

    fn path(&self, run_id: &str) -> PathBuf {
        let key = format!("{:x}", Sha256::digest(run_id.as_bytes()));
        self.root.join(&key[..2]).join(format!("{key}.json"))
    }

    pub(super) fn read(&self, run_id: &str) -> Result<Option<ArchiveRoute>, String> {
        validate_run_id(run_id)?;
        let path = self.path(run_id);
        let shard = path.parent().ok_or("ACP archive route shard unavailable")?;
        let private_dir = self.root.parent().ok_or("ACP private state unavailable")?;
        if !real_directory_exists(private_dir)? {
            return Err("ACP private state directory disappeared".into());
        }
        if !private_directory_exists(&self.root)? || !private_directory_exists(shard)? {
            return Ok(None);
        }
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err("ACP archive routing receipt metadata unavailable".into()),
        };
        if !metadata.is_file()
            || metadata.file_type().is_symlink()
            || metadata.len() == 0
            || metadata.len() > MAX_RECEIPT_BYTES
        {
            return Err("ACP archive routing receipt is not a bounded regular file".into());
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o077 != 0 {
                return Err("ACP archive routing receipt is not private".into());
            }
        }
        #[cfg(windows)]
        webcodex_environment::runtime_entry::validate_windows_env_acl(&path)?;
        let file = File::open(&path).map_err(|_| "ACP archive routing receipt unavailable")?;
        let mut bytes = Vec::new();
        file.take(MAX_RECEIPT_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| "ACP archive routing receipt read failed")?;
        if bytes.len() as u64 > MAX_RECEIPT_BYTES {
            return Err("ACP archive routing receipt exceeds bound".into());
        }
        let route: ArchiveRoute = serde_json::from_slice(&bytes)
            .map_err(|_| "ACP archive routing receipt is malformed")?;
        route.validate(run_id)?;
        Ok(Some(route))
    }

    pub(super) fn persist(&self, route: &ArchiveRoute) -> Result<(), String> {
        route.validate(&route.run_id)?;
        let path = self.path(&route.run_id);
        let shard = path.parent().ok_or("ACP archive route shard unavailable")?;
        if let Some(existing) = self.read(&route.run_id)? {
            if existing != *route {
                return Err("ACP archive routing identity conflict".into());
            }
            sync_dir(self.root.parent().ok_or("ACP private state unavailable")?)?;
            sync_dir(&self.root)?;
            return sync_dir(shard);
        }
        ensure_private_directory(&self.root)?;
        ensure_private_directory(shard)?;
        // Publish the directory ancestry before publishing a receipt. The private
        // state directory itself is owned/created by the Server.
        sync_dir(self.root.parent().ok_or("ACP private state unavailable")?)?;
        sync_dir(&self.root)?;
        let bytes = serde_json::to_vec(route).map_err(|_| "ACP archive route encoding failed")?;
        if bytes.len() as u64 > MAX_RECEIPT_BYTES {
            return Err("ACP archive routing receipt exceeds bound".into());
        }
        let temp = shard.join(format!(".{}.tmp", uuid::Uuid::new_v4().simple()));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let result = (|| -> Result<(), String> {
            let mut file = options
                .open(&temp)
                .map_err(|_| "ACP archive route temp creation failed")?;
            #[cfg(windows)]
            webcodex_environment::runtime_entry::protect_windows_runtime_state_path(&temp)?;
            file.write_all(&bytes)
                .and_then(|_| file.sync_all())
                .map_err(|_| "ACP archive route write failed")?;
            drop(file);
            // Immutable, non-replacing publication. Concurrent exact writes
            // converge; a different receipt must never overwrite a fence.
            if publish_new(&temp, &path).is_err() {
                if self.read(&route.run_id)?.as_ref() != Some(route) {
                    return Err("ACP archive route publication failed or conflicted".into());
                }
            }
            sync_dir(shard)
        })();
        let _ = fs::remove_file(&temp);
        result
    }
}

fn real_directory_exists(path: &Path) -> Result<bool, String> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(false),
        Err(_) => return Err("ACP archive route directory unavailable".into()),
    };
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err("ACP archive route directory is not a real directory".into());
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        if metadata.file_attributes() & 0x400 != 0 {
            return Err("ACP archive route directory is a reparse point".into());
        }
    }
    Ok(true)
}

fn private_directory_exists(path: &Path) -> Result<bool, String> {
    if !real_directory_exists(path)? {
        return Ok(false);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let metadata =
            fs::symlink_metadata(path).map_err(|_| "ACP archive route directory unavailable")?;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err("ACP archive route directory is not private".into());
        }
    }
    #[cfg(windows)]
    webcodex_environment::runtime_entry::validate_windows_env_acl(path)?;
    Ok(true)
}

fn ensure_private_directory(path: &Path) -> Result<(), String> {
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    match builder.create(path) {
        Ok(()) => {
            #[cfg(windows)]
            webcodex_environment::runtime_entry::protect_windows_runtime_state_path(path)?;
        }
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
        Err(_) => return Err("ACP archive route directory creation failed".into()),
    }
    if !private_directory_exists(path)? {
        return Err("ACP archive route directory disappeared".into());
    }
    Ok(())
}

#[cfg(not(windows))]
fn publish_new(temp: &Path, path: &Path) -> std::io::Result<()> {
    fs::hard_link(temp, path)
}

#[cfg(windows)]
fn publish_new(temp: &Path, path: &Path) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{MoveFileExW, MOVEFILE_WRITE_THROUGH};
    let source: Vec<u16> = temp.as_os_str().encode_wide().chain(Some(0)).collect();
    let destination: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    if unsafe {
        MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_WRITE_THROUGH,
        )
    } == 0
    {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

fn sync_dir(_path: &Path) -> Result<(), String> {
    #[cfg(unix)]
    File::open(_path)
        .and_then(|file| file.sync_all())
        .map_err(|_| "ACP archive route directory sync failed")?;
    Ok(())
}
