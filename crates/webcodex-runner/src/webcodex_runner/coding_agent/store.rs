//! Bounded durable Run records and atomic state-file publication.

use super::*;

pub(super) const STORE_SCHEMA_VERSION: u32 = 1;
const STORE_FILE: &str = "state.json";
const STORE_ARCHIVE_MARKER: &str = "archive-required-v1";
const STORE_ARCHIVE_MARKER_BYTES: &[u8] = b"webcodex-acp-archive-v1\n";
const STORE_MAX_BYTES: usize = 64 * 1024;
/// One recoverable overflow directory above the normal inventory bound.
const STORE_MAX_RUN_DIR_SCAN: usize = CODING_AGENT_MAX_INVENTORY_RUNS + 1;
// A separate, sharded replay ledger. The hot inventory limit is not a limit
// on historical uncertain identities. Each record is size-bounded; lookup
// never scans the archive. Storage failures, not an arbitrary count of
// uncertain identities, are the admission boundary.
/// Must match `SERVER_TERMINAL_RETENTION_SECS` in `src/tool_runtime/coding_agent.rs`.
pub(super) const STORE_RETENTION_SECS: i64 = 24 * 60 * 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum DurableDispatchPhase {
    BeforePromptBarrier,
    PromptDispatchMayHaveOccurred,
    Terminal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DurableRunRecord {
    pub(super) schema_version: u32,
    pub(super) run_id: String,
    pub(super) intent_fingerprint: String,
    pub(super) authority_fingerprint: String,
    pub(super) runtime_project_id: String,
    pub(super) provider_id: String,
    pub(super) provider_instance_id: String,
    pub(super) state: CodingAgentRunState,
    pub(super) execution_state: CodingAgentExecutionState,
    pub(super) dispatch_phase: DurableDispatchPhase,
    pub(super) created_at: i64,
    pub(super) updated_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) terminal: Option<CodingAgentTerminal>,
}

impl DurableRunRecord {
    pub(super) fn snapshot(&self, observation_revision: u64) -> CodingAgentRunSnapshot {
        CodingAgentRunSnapshot {
            run_id: self.run_id.clone(),
            intent_fingerprint: self.intent_fingerprint.clone(),
            authority_fingerprint: self.authority_fingerprint.clone(),
            runtime_project_id: self.runtime_project_id.clone(),
            provider_id: self.provider_id.clone(),
            provider_instance_id: self.provider_instance_id.clone(),
            state: self.state.clone(),
            execution_state: self.execution_state,
            observation_revision,
            created_at: self.created_at,
            updated_at: self.updated_at,
            terminal: self.terminal.clone(),
            pending_permission: None,
            live_confirmed_effective_config: None,
        }
    }
}

#[cfg(test)]
#[derive(Debug, Default)]
struct TerminalWriteGateState {
    reached: bool,
    released: bool,
}

#[cfg(test)]
#[derive(Debug, Default)]
pub(super) struct TerminalWriteGate {
    state: Mutex<TerminalWriteGateState>,
    changed: Condvar,
}

#[cfg(test)]
impl TerminalWriteGate {
    fn block_writer(&self) {
        let mut state = self.state.lock().unwrap();
        state.reached = true;
        self.changed.notify_all();
        while !state.released {
            state = self.changed.wait(state).unwrap();
        }
    }

    #[cfg(unix)]
    pub(super) fn wait_until_reached(&self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut state = self.state.lock().unwrap();
        while !state.reached {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "terminal durable write gate was never reached"
            );
            let (next, _) = self.changed.wait_timeout(state, remaining).unwrap();
            state = next;
        }
    }

    #[cfg(unix)]
    pub(super) fn release(&self) {
        let mut state = self.state.lock().unwrap();
        state.released = true;
        self.changed.notify_all();
    }
}

#[cfg(test)]
#[derive(Debug, Default)]
struct DurableRunStoreTestControl {
    fail_next_terminal_writes: std::sync::atomic::AtomicUsize,
    fail_next_removes: std::sync::atomic::AtomicUsize,
    terminal_write_gate: Mutex<Option<Arc<TerminalWriteGate>>>,
}

#[derive(Debug, Clone)]
pub(super) struct DurableRetentionListing {
    pub(super) records: Vec<DurableRunRecord>,
    pub(super) entry_count: usize,
}

#[derive(Debug, Clone)]
pub(super) struct DurableRunStore {
    pub(super) root: PathBuf,
    #[cfg(test)]
    test_control: Arc<DurableRunStoreTestControl>,
}

impl DurableRunStore {
    pub(super) fn new(root: PathBuf) -> Self {
        Self {
            root,
            #[cfg(test)]
            test_control: Arc::new(DurableRunStoreTestControl::default()),
        }
    }

    pub(super) fn default_root(client_id: &str, server_url: &str) -> Result<PathBuf, String> {
        let server_url = server_url.trim().trim_end_matches('/');
        if client_id.trim().is_empty() || server_url.is_empty() {
            return Err("ACP durable store requires non-empty Runner identity".to_string());
        }
        let mut hasher = Sha256::new();
        hasher.update(b"webcodex-coding-agent-store-runner-v1\0");
        hasher.update(client_id.as_bytes());
        hasher.update(b"\0");
        hasher.update(server_url.as_bytes());
        let namespace = format!("{:x}", hasher.finalize());
        Ok(
            webcodex_runner_config::paths::default_client_state_base_dir()?
                .join("runner-coding-agent-runs-v1")
                .join(namespace),
        )
    }

    fn run_dir(&self, run_id: &str) -> PathBuf {
        let mut hasher = Sha256::new();
        hasher.update(run_id.as_bytes());
        self.root.join(format!("{:x}", hasher.finalize()))
    }

    pub(super) fn state_path(&self, run_id: &str) -> PathBuf {
        self.run_dir(run_id).join(STORE_FILE)
    }

    fn archive_dir(&self, run_id: &str) -> PathBuf {
        let key = format!("{:x}", Sha256::digest(run_id.as_bytes()));
        self.root
            .with_extension("archive-v1")
            .join(&key[..2])
            .join(key)
    }

    pub(super) fn read(&self, run_id: &str) -> Result<Option<DurableRunRecord>, String> {
        self.archive_required()?;
        let hot = if real_directory_or_absent(&self.root)? {
            self.read_run_dir(run_id, &self.run_dir(run_id))?
        } else {
            None
        };
        let archived = self.read_archived(run_id)?;
        match (hot, archived) {
            (Some(_), Some(_)) => Err(
                "ACP Run exists in both hot and archive storage; reconcile before retrying".into(),
            ),
            (Some(record), None) | (None, Some(record)) => Ok(Some(record)),
            (None, None) => Ok(None),
        }
    }

    /// Used when no live RunEntry exists. A durable active claim in that
    /// situation is not a controllable process and must remain uncertain.
    pub(super) fn read_retained_terminal(
        &self,
        run_id: &str,
    ) -> Result<Option<DurableRunRecord>, String> {
        let record = self.read(run_id)?;
        if record
            .as_ref()
            .is_some_and(|record| record.dispatch_phase != DurableDispatchPhase::Terminal)
        {
            return Err(
                "ACP Run has a durable active claim but no live owner; reconcile before retrying"
                    .to_string(),
            );
        }
        Ok(record)
    }

    pub(super) fn read_archived(&self, run_id: &str) -> Result<Option<DurableRunRecord>, String> {
        let dir = self.archive_dir(run_id);
        let shard = dir.parent().expect("archive record has shard");
        if !real_directory_or_absent(shard.parent().expect("archive shard has root"))?
            || !real_directory_or_absent(shard)?
        {
            return Ok(None);
        }
        let record = self.read_run_dir(run_id, &dir)?;
        if record
            .as_ref()
            .is_some_and(|record| record.dispatch_phase != DurableDispatchPhase::Terminal)
        {
            return Err("ACP archived Run is not terminal; replay is unsafe".to_string());
        }
        Ok(record)
    }

    fn read_run_dir(&self, run_id: &str, dir: &Path) -> Result<Option<DurableRunRecord>, String> {
        let path = dir.join(STORE_FILE);
        if !real_directory_or_absent(dir)? {
            return Ok(None);
        }
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| format!("failed to inspect ACP Run state: {error}"))?;
        if !metadata.is_file() || is_state_link(&metadata) {
            return Err("ACP Run state must be a real regular file".into());
        }
        let file = match File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return match fs::symlink_metadata(dir) {
                    Ok(_) => Err(
                        "ACP Run state is missing from an existing Run state directory".to_string(),
                    ),
                    Err(dir_error) if dir_error.kind() == std::io::ErrorKind::NotFound => Ok(None),
                    Err(dir_error) => {
                        Err(format!("failed to inspect ACP Run state dir: {dir_error}"))
                    }
                };
            }
            Err(error) => return Err(format!("failed to open ACP Run state: {error}")),
        };
        let len = file
            .metadata()
            .map_err(|error| format!("failed to inspect ACP Run state: {error}"))?
            .len() as usize;
        if len == 0 || len > STORE_MAX_BYTES {
            return Err("ACP Run state has invalid bounded size".to_string());
        }
        let mut bytes = Vec::with_capacity(len);
        file.take(STORE_MAX_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| format!("failed to read ACP Run state: {error}"))?;
        if bytes.len() > STORE_MAX_BYTES {
            return Err("ACP Run state exceeds durable bound".into());
        }
        let record: DurableRunRecord =
            serde_json::from_slice(&bytes).map_err(|_| "ACP Run state is malformed".to_string())?;
        validate_durable_record(&record)?;
        if record.run_id != run_id {
            return Err("ACP Run state identity mismatch".to_string());
        }
        Ok(Some(record))
    }

    // Older Runners reject this non-directory entry instead of silently
    // forgetting archived replay fences after a downgrade.
    pub(super) fn archive_required(&self) -> Result<bool, String> {
        if !real_directory_or_absent(&self.root)? {
            return Ok(false);
        }
        let path = self.root.join(STORE_ARCHIVE_MARKER);
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(format!("ACP archive marker unavailable: {error}")),
        };
        if !metadata.is_file()
            || is_state_link(&metadata)
            || metadata.len() != STORE_ARCHIVE_MARKER_BYTES.len() as u64
            || fs::read(&path).map_err(|error| format!("ACP archive marker unreadable: {error}"))?
                != STORE_ARCHIVE_MARKER_BYTES
        {
            return Err("ACP archive marker is invalid".into());
        }
        if !real_directory_or_absent(&self.root.with_extension("archive-v1"))? {
            return Err("ACP archive marker exists but its archive root is missing; restore the paired state before admission".into());
        }
        Ok(true)
    }

    fn ensure_archive_marker(&self, shard: &Path) -> Result<(), String> {
        if !self.archive_required()? {
            let temp = shard.join(format!("marker.{}.tmp", Uuid::new_v4().simple()));
            let result = (|| {
                let mut file = File::options()
                    .write(true)
                    .create_new(true)
                    .open(&temp)
                    .map_err(|error| format!("ACP archive marker creation failed: {error}"))?;
                file.write_all(STORE_ARCHIVE_MARKER_BYTES)
                    .and_then(|_| file.sync_all())
                    .map_err(|error| format!("ACP archive marker write failed: {error}"))?;
                drop(file);
                publish_state_file(&temp, &self.root.join(STORE_ARCHIVE_MARKER))
            })();
            if result.is_err() {
                let _ = fs::remove_file(&temp);
            }
            result?;
        }
        sync_parent(&self.root)
    }

    /// Move a validated, immutable terminal claim out of the hot inventory.
    /// No deletion/re-creation gap: the directory rename is on the same volume.
    /// The archived record remains a fail-closed idempotency fence forever
    /// unless an explicit future reconciliation/migration protocol retires it.
    pub(super) fn archive_terminal(&self, record: &DurableRunRecord) -> Result<(), String> {
        validate_durable_record(record)?;
        if record.dispatch_phase != DurableDispatchPhase::Terminal {
            return Err("cannot archive a non-terminal ACP Run".to_string());
        }
        let source = self.run_dir(&record.run_id);
        let destination = self.archive_dir(&record.run_id);
        let shard = destination.parent().expect("archive record has shard");
        if let Some(existing) = self.read_archived(&record.run_id)? {
            if self.read_run_dir(&record.run_id, &source)?.is_none()
                && existing.snapshot(0) == record.snapshot(0)
            {
                self.ensure_archive_marker(shard)?;
                sync_parent(shard)?;
                return sync_parent(&self.root);
            }
            return Err("ACP archive already contains this Run identity".to_string());
        }
        let current = self
            .read_run_dir(&record.run_id, &source)?
            .ok_or_else(|| "ACP Run disappeared before archival".to_string())?;
        if current.snapshot(0) != record.snapshot(0) {
            return Err("ACP Run changed before archival".to_string());
        }
        let archive_root = shard.parent().expect("archive shard has root");
        real_directory_or_absent(archive_root)?;
        real_directory_or_absent(shard)?;
        fs::create_dir_all(shard)
            .map_err(|error| format!("failed to create ACP replay archive: {error}"))?;
        if !real_directory_or_absent(archive_root)? || !real_directory_or_absent(shard)? {
            return Err("ACP replay archive directory disappeared".into());
        }
        sync_parent(shard.parent().expect("archive shard has parent"))?;
        sync_parent(self.root.parent().expect("Run store has parent"))?;
        self.ensure_archive_marker(shard)?;
        move_to_archive(&source, &destination)?;
        sync_parent(shard)?;
        sync_parent(&self.root)?;
        Ok(())
    }

    pub(super) fn write(&self, record: &DurableRunRecord) -> Result<(), String> {
        validate_durable_record(record)?;
        let bytes =
            serde_json::to_vec(record).map_err(|_| "failed to encode ACP Run state".to_string())?;
        if bytes.len() > STORE_MAX_BYTES {
            return Err("ACP Run state exceeds durable bound".to_string());
        }
        #[cfg(test)]
        if record.dispatch_phase == DurableDispatchPhase::Terminal {
            if let Some(gate) = self
                .test_control
                .terminal_write_gate
                .lock()
                .unwrap()
                .clone()
            {
                gate.block_writer();
            }
            if self
                .test_control
                .fail_next_terminal_writes
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    (remaining > 0).then(|| remaining - 1)
                })
                .is_ok()
            {
                return Err("injected ACP terminal durable write failure".to_string());
            }
        }
        let dir = self.run_dir(&record.run_id);
        fs::create_dir_all(&dir)
            .map_err(|error| format!("failed to create ACP Run state dir: {error}"))?;
        let temp = dir.join(format!("state.{}.tmp", Uuid::new_v4().simple()));
        let state_path = self.state_path(&record.run_id);
        let result = (|| {
            let mut file = File::options()
                .write(true)
                .create_new(true)
                .open(&temp)
                .map_err(|error| format!("failed to create ACP Run temp state: {error}"))?;
            file.write_all(&bytes)
                .and_then(|_| file.sync_all())
                .map_err(|error| format!("failed to persist ACP Run state: {error}"))?;
            drop(file);
            publish_state_file(&temp, &state_path)?;
            sync_parent(&dir)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temp);
        }
        result
    }

    pub(super) fn scan(&self) -> Result<Vec<DurableRunRecord>, String> {
        let listing = self.list_records_for_retention()?;
        if listing.entry_count > CODING_AGENT_MAX_INVENTORY_RUNS {
            return Err("ACP Run durable state exceeds bounded record count".to_string());
        }
        Ok(listing.records)
    }

    pub(super) fn list_records_for_retention(&self) -> Result<DurableRetentionListing, String> {
        real_directory_or_absent(&self.root)?;
        let entries = match fs::read_dir(&self.root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(DurableRetentionListing {
                    records: Vec::new(),
                    entry_count: 0,
                });
            }
            Err(error) => return Err(format!("failed to list ACP Run state: {error}")),
        };
        let mut records = Vec::new();
        let mut entry_count = 0usize;
        for entry in entries {
            let entry =
                entry.map_err(|error| format!("failed to inspect ACP Run state: {error}"))?;
            if entry.file_name() == std::ffi::OsStr::new(STORE_ARCHIVE_MARKER) {
                self.archive_required()?;
                continue;
            }
            let metadata = fs::symlink_metadata(entry.path())
                .map_err(|error| format!("failed to inspect ACP Run state: {error}"))?;
            if !metadata.is_dir() || is_state_link(&metadata) {
                return Err("ACP Run state root contains an unexpected entry".to_string());
            }
            entry_count = entry_count
                .checked_add(1)
                .ok_or_else(|| "ACP Run durable state exceeds bounded record count".to_string())?;
            if entry_count > STORE_MAX_RUN_DIR_SCAN {
                return Err("ACP Run durable state exceeds bounded record count".to_string());
            }
            let path = entry.path().join(STORE_FILE);
            let len = match fs::symlink_metadata(&path) {
                Ok(metadata) if metadata.is_file() && !is_state_link(&metadata) => {
                    metadata.len() as usize
                }
                Ok(_) => {
                    tracing::error!(state_path = %path.display(), "ACP Run durable state is not a real regular file; preserving tombstone");
                    continue;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    tracing::error!(state_path = %path.display(), "ACP Run durable state is missing from an existing Run state directory; preserving tombstone");
                    continue;
                }
                Err(error) => {
                    tracing::error!(state_path = %path.display(), error = %error, "ACP Run durable state unavailable during recovery; preserving tombstone");
                    continue;
                }
            };
            if len == 0 || len > STORE_MAX_BYTES {
                tracing::error!(state_path = %path.display(), "ACP Run durable state has invalid bounded size; preserving tombstone");
                continue;
            }
            let file = match File::open(&path) {
                Ok(file) => file,
                Err(error) => {
                    tracing::error!(state_path = %path.display(), error = %error, "ACP Run durable state unavailable during recovery; preserving tombstone");
                    continue;
                }
            };
            let mut bytes = Vec::with_capacity(len);
            if let Err(error) = file
                .take(STORE_MAX_BYTES as u64 + 1)
                .read_to_end(&mut bytes)
            {
                tracing::error!(state_path = %path.display(), error = %error, "ACP Run durable state unavailable during recovery; preserving tombstone");
                continue;
            }
            if bytes.len() > STORE_MAX_BYTES {
                tracing::error!(state_path = %path.display(), "ACP Run durable state exceeds size bound; preserving tombstone");
                continue;
            }
            let record: DurableRunRecord = match serde_json::from_slice(&bytes) {
                Ok(record) => record,
                Err(_) => {
                    tracing::error!(state_path = %path.display(), "ACP Run durable state is malformed; preserving tombstone");
                    continue;
                }
            };
            if let Err(error) = validate_durable_record(&record) {
                tracing::error!(state_path = %path.display(), error = %error, "ACP Run durable state is invalid; preserving tombstone");
                continue;
            }
            if self.run_dir(&record.run_id) != entry.path() {
                tracing::error!(state_path = %path.display(), "ACP Run durable state directory identity mismatch; preserving tombstone");
                continue;
            }
            records.push(record);
        }
        records.sort_by(|a, b| a.run_id.cmp(&b.run_id));
        Ok(DurableRetentionListing {
            records,
            entry_count,
        })
    }

    pub(super) fn remove(&self, run_id: &str) {
        let _ = fs::remove_dir_all(self.run_dir(run_id));
    }

    pub(super) fn remove_checked(&self, run_id: &str) -> Result<(), String> {
        #[cfg(test)]
        if self
            .test_control
            .fail_next_removes
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                (remaining > 0).then(|| remaining - 1)
            })
            .is_ok()
        {
            return Err("injected ACP durable run removal failure".to_string());
        }
        match fs::remove_dir_all(self.run_dir(run_id)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(format!("failed to remove ACP Run state dir: {error}")),
        }
    }

    #[cfg(test)]
    pub(super) fn fail_next_terminal_writes(&self, count: usize) {
        self.test_control
            .fail_next_terminal_writes
            .store(count, Ordering::SeqCst);
    }

    #[cfg(all(test, unix))]
    pub(super) fn set_terminal_write_gate(&self, gate: Option<Arc<TerminalWriteGate>>) {
        *self.test_control.terminal_write_gate.lock().unwrap() = gate;
    }

    #[cfg(test)]
    pub(super) fn fail_next_removes(&self, count: usize) {
        self.test_control
            .fail_next_removes
            .store(count, Ordering::SeqCst);
    }
}

#[cfg(windows)]
fn move_to_archive(source: &Path, destination: &Path) -> Result<(), String> {
    let source = source
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let destination = destination
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    if unsafe {
        MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_WRITE_THROUGH,
        )
    } == 0
    {
        return Err(format!(
            "failed to archive ACP Run state: {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok(())
}

#[cfg(not(windows))]
fn move_to_archive(source: &Path, destination: &Path) -> Result<(), String> {
    fs::rename(source, destination)
        .map_err(|error| format!("failed to archive ACP Run state: {error}"))
}

#[cfg(unix)]
fn publish_state_file(temp: &Path, state_path: &Path) -> Result<(), String> {
    fs::rename(temp, state_path)
        .map_err(|error| format!("failed to publish ACP Run state: {error}"))
}

#[cfg(windows)]
fn publish_state_file(temp: &Path, state_path: &Path) -> Result<(), String> {
    let from = temp
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let to = state_path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let retry_deadline = Instant::now() + Duration::from_millis(500);
    loop {
        if unsafe {
            MoveFileExW(
                from.as_ptr(),
                to.as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            )
        } != 0
        {
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        let retryable = matches!(error.raw_os_error(), Some(5) | Some(32) | Some(33));
        if !retryable || Instant::now() >= retry_deadline {
            return Err(format!("failed to publish ACP Run state: {error}"));
        }
        thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(not(any(unix, windows)))]
fn publish_state_file(_temp: &Path, _state_path: &Path) -> Result<(), String> {
    Err("ACP Run durable state is unsupported on this platform".to_string())
}

fn sync_parent(_dir: &Path) -> Result<(), String> {
    #[cfg(unix)]
    {
        File::open(_dir)
            .and_then(|file| file.sync_all())
            .map_err(|error| format!("failed to sync ACP Run state dir: {error}"))?;
    }
    Ok(())
}

fn is_state_link(metadata: &fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        metadata.file_type().is_symlink() || metadata.file_attributes() & 0x400 != 0
    }
    #[cfg(not(windows))]
    metadata.file_type().is_symlink()
}

fn real_directory_or_absent(path: &Path) -> Result<bool, String> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !is_state_link(&metadata) => Ok(true),
        Ok(_) => Err("ACP durable state path must be a real directory".into()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(format!("ACP durable state directory unavailable: {error}")),
    }
}

fn validate_durable_record(record: &DurableRunRecord) -> Result<(), String> {
    if record.schema_version != STORE_SCHEMA_VERSION {
        return Err("ACP Run durable record schema is invalid".to_string());
    }
    validate_coding_agent_run_snapshot(&record.snapshot(0))
        .map_err(|error| format!("ACP Run durable snapshot is invalid: {error}"))?;
    if (record.dispatch_phase == DurableDispatchPhase::Terminal) != record.state.terminal() {
        return Err("ACP Run durable phase/state terminal truth is inconsistent".to_string());
    }
    Ok(())
}

pub(super) fn durable_record_from_snapshot(
    run_id: &str,
    snapshot: &CodingAgentRunSnapshot,
    phase: DurableDispatchPhase,
) -> Result<DurableRunRecord, String> {
    if snapshot.run_id != run_id {
        return Err("ACP Run identity changed unexpectedly".to_string());
    }
    Ok(DurableRunRecord {
        schema_version: STORE_SCHEMA_VERSION,
        run_id: snapshot.run_id.clone(),
        intent_fingerprint: snapshot.intent_fingerprint.clone(),
        authority_fingerprint: snapshot.authority_fingerprint.clone(),
        runtime_project_id: snapshot.runtime_project_id.clone(),
        provider_id: snapshot.provider_id.clone(),
        provider_instance_id: snapshot.provider_instance_id.clone(),
        state: snapshot.state.clone(),
        execution_state: snapshot.execution_state,
        dispatch_phase: phase,
        created_at: snapshot.created_at,
        updated_at: snapshot.updated_at,
        terminal: snapshot.terminal.clone(),
    })
}
