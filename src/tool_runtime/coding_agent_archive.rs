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

/// The claim occupies the original route slot. Older Servers reject its
/// unknown schema rather than treating a not-yet-advertised Run as absent.
/// Terminal receipts for claimed Runs are separately immutable; legacy
/// schema-1 routes remain readable in their original location.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AdmissionClaim {
    schema: u32,
    pub(super) run_id: String,
    pub(super) client_id: String,
    pub(super) runner_instance_id: String,
    pub(super) authority_fingerprint: String,
    pub(super) runtime_project_id: String,
    pub(super) intent_fingerprint: String,
    pub(super) provider_id: String,
    pub(super) provider_instance_id: String,
    pub(super) replay_window: Option<webcodex_core::coding_agent::retention::CodingAgentReplayWindow>,
}

impl AdmissionClaim {
    pub(super) fn from_prepared(prepared: &super::CodingAgentPreparedStart) -> Self {
        Self {
            schema: 2,
            run_id: prepared.run_id.clone(),
            client_id: prepared.client.client_id.clone(),
            runner_instance_id: prepared.client.runner_instance_id.clone(),
            authority_fingerprint: prepared.authority_fingerprint.clone(),
            runtime_project_id: prepared.runtime_project_id.clone(),
            intent_fingerprint: prepared.intent_fingerprint.clone(),
            provider_id: prepared.provider_id.clone(),
            provider_instance_id: prepared.provider_instance_id.clone(),
            replay_window: prepared.replay_window,
        }
    }

    fn validate(&self, run_id: &str) -> Result<(), String> {
        validate_run_id(&self.run_id)?;
        validate_authority_fingerprint(&self.authority_fingerprint)?;
        webcodex_core::coding_agent::validate_intent_fingerprint(&self.intent_fingerprint)?;
        if self.schema != 2
            || self.run_id != run_id
            || !self.runtime_project_id.starts_with(&format!("agent:{}:", self.client_id))
            || self.runtime_project_id.len() > 1024
            || [&self.client_id, &self.runner_instance_id, &self.provider_id, &self.provider_instance_id]
                .iter().any(|value| value.is_empty() || value.len() > 512)
            || self.replay_window.is_some_and(|window| window.expires_at().is_err())
        {
            return Err("ACP admission claim is invalid".into());
        }
        Ok(())
    }

    pub(super) fn matches_snapshot(&self, client_id: &str, run: &CodingAgentRunSnapshot) -> bool {
        self.client_id == client_id
            && self.run_id == run.run_id
            && self.authority_fingerprint == run.authority_fingerprint
            && self.runtime_project_id == run.runtime_project_id
            && self.intent_fingerprint == run.intent_fingerprint
            && self.provider_id == run.provider_id
            && self.provider_instance_id == run.provider_instance_id
            && self.replay_window == run.replay_window
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
enum RouteSlot {
    Receipt(ArchiveRoute),
    Claim(AdmissionClaim),
}

impl RouteSlot {
    fn validate(&self, run_id: &str) -> Result<(), String> {
        match self {
            Self::Receipt(route) => route.validate(run_id),
            Self::Claim(claim) => claim.validate(run_id),
        }
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

    pub(super) fn lookup(&self, run_id: &str) -> Result<(Option<AdmissionClaim>, Option<ArchiveRoute>), String> {
        validate_run_id(run_id)?;
        let path = self.path(run_id);
        let receipt_path = path.with_extension("receipt.json");
        match self.read_slot(run_id, &path)? {
            Some(RouteSlot::Claim(claim)) => {
                let receipt = match self.read_slot(run_id, &receipt_path)? {
                    Some(RouteSlot::Receipt(receipt))
                        if receipt.client_id == claim.client_id
                            && receipt.authority_fingerprint == claim.authority_fingerprint => Some(receipt),
                    None => None,
                    _ => return Err("ACP admission claim conflicts with terminal receipt".into()),
                };
                Ok((Some(claim), receipt))
            }
            Some(RouteSlot::Receipt(receipt)) => Ok((None, Some(receipt))),
            None => {
                if self.read_slot(run_id, &receipt_path)?.is_some() {
                    return Err("ACP admission claim is missing for retained receipt".into());
                }
                Ok((None, None))
            }
        }
    }

    pub(super) fn read(&self, run_id: &str) -> Result<Option<ArchiveRoute>, String> {
        self.lookup(run_id).map(|(_, receipt)| receipt)
    }

    pub(super) fn claim(&self, claim: &AdmissionClaim, allow_exact_retry: bool) -> Result<(), String> {
        claim.validate(&claim.run_id)?;
        let (existing, receipt) = self.lookup(&claim.run_id)?;
        if receipt.is_some() || existing.as_ref().is_some_and(|existing| !allow_exact_retry || existing != claim) {
            return Err("ACP Run is already bound to a different admission or terminal receipt; reconcile the original Run".into());
        }
        self.persist_slot(&claim.run_id, &self.path(&claim.run_id), &RouteSlot::Claim(claim.clone()), allow_exact_retry)
    }

    fn read_slot(&self, run_id: &str, path: &Path) -> Result<Option<RouteSlot>, String> {
        let shard = path.parent().ok_or("ACP archive route shard unavailable")?;
        let private_dir = self.root.parent().ok_or("ACP private state unavailable")?;
        if !real_directory_exists(private_dir)? {
            return Err("ACP private state directory disappeared".into());
        }
        if !private_directory_exists(&self.root)? || !private_directory_exists(shard)? {
            return Ok(None);
        }
        let metadata = match fs::symlink_metadata(path) {
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
        {
            use std::os::windows::fs::MetadataExt;
            if metadata.file_attributes() & 0x400 != 0 {
                return Err("ACP archive route is a reparse point".into());
            }
            webcodex_environment::runtime_entry::validate_windows_env_acl(path)?;
        }
        let file = File::open(path).map_err(|_| "ACP archive routing receipt unavailable")?;
        let mut bytes = Vec::new();
        file.take(MAX_RECEIPT_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| "ACP archive routing receipt read failed")?;
        if bytes.len() as u64 > MAX_RECEIPT_BYTES {
            return Err("ACP archive routing receipt exceeds bound".into());
        }
        let slot: RouteSlot = serde_json::from_slice(&bytes)
            .map_err(|_| "ACP archive routing receipt is malformed")?;
        slot.validate(run_id)?;
        Ok(Some(slot))
    }

    pub(super) fn persist(&self, route: &ArchiveRoute, run: &CodingAgentRunSnapshot) -> Result<(), String> {
        route.validate(&route.run_id)?;
        if !route.matches(&route.client_id, run) || !run.state.terminal() {
            return Err("ACP terminal receipt does not match the observed Run".into());
        }
        let (claim, receipt) = self.lookup(&route.run_id)?;
        if receipt.as_ref().is_some_and(|existing| existing != route) {
            return Err("ACP archive routing identity conflict".into());
        }
        let mut path = self.path(&route.run_id);
        if let Some(claim) = claim {
            if !claim.matches_snapshot(&route.client_id, run) {
                return Err("ACP terminal receipt conflicts with admission claim".into());
            }
            path = path.with_extension("receipt.json");
        }
        self.persist_slot(&route.run_id, &path, &RouteSlot::Receipt(route.clone()), true)
    }

    fn persist_slot(&self, run_id: &str, path: &Path, slot: &RouteSlot, accept_existing: bool) -> Result<(), String> {
        slot.validate(run_id)?;
        let shard = path.parent().ok_or("ACP archive route shard unavailable")?;
        if let Some(existing) = self.read_slot(run_id, path)? {
            if !accept_existing || existing != *slot {
                return Err("ACP archive routing identity conflict".into());
            }
            sync_dir(self.root.parent().ok_or("ACP private state unavailable")?)?;
            sync_dir(&self.root)?;
            return sync_dir(shard);
        }
        ensure_private_directory(&self.root)?;
        ensure_private_directory(shard)?;
        sync_dir(self.root.parent().ok_or("ACP private state unavailable")?)?;
        sync_dir(&self.root)?;
        let bytes = serde_json::to_vec(slot).map_err(|_| "ACP archive route encoding failed")?;
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
            // Claims and receipts are immutable. Concurrent exact writes
            // converge; different identities cannot overwrite the fence.
            if publish_new(&temp, path).is_err() {
                if !accept_existing || self.read_slot(run_id, path)?.as_ref() != Some(slot) {
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
