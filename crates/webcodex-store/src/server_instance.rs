use crate::Database;
use anyhow::{anyhow, bail, Context};
use fs2::FileExt;
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::path::{Component, Path, PathBuf};

/// Process-lifetime ownership proof for one standalone Control Server database.
///
/// Hard-linked production database files are **unsupported**: if the database file
/// already exists, it must have hard-link count exactly one. Aliases created after
/// acquisition are detected on [`Self::owns_database`] re-checks.
///
/// Ownership is **one WebCodex Server or offline-maintenance actor per database
/// directory**, not per arbitrary database filename. A stable parent-directory lock
/// (`.webcodex-server-instance.lock`) enforces that contract; the legacy
/// `<database>.server-instance.lock` sidecar remains for mutual exclusion with older
/// binaries that only acquire the path-suffix lock.
///
/// The lock files intentionally contain no PID, credential, path metadata, or other
/// authority claim. Their OS-level exclusive locks are the proof; empty sidecars merely
/// provide stable cross-platform lock targets. Process exit releases the locks even when
/// normal Rust destructors do not run.
///
/// **Residual assumption:** a privileged **file** bind mount that grafts the same path
/// onto different backing inodes in separate mount namespaces is out of scope; ordinary
/// directory bind mounts share the parent-directory lock file naturally.
pub struct ServerInstanceGuard {
    parent_directory_lock: File,
    legacy_path_lock: File,
    database_path: PathBuf,
}

const PARENT_DIRECTORY_LOCK_NAME: &str = ".webcodex-server-instance.lock";

impl ServerInstanceGuard {
    /// Acquire exclusive ownership using only a database path, before SQLite opens
    /// the file.
    pub fn acquire_for_path(db_path: &Path) -> anyhow::Result<Self> {
        let database_path = resolve_database_state_path(db_path)?;
        Self::acquire_lock_for_resolved_path(database_path)
    }

    pub fn acquire(db: &Database) -> anyhow::Result<Self> {
        Self::acquire_lock_for_resolved_path(db.state_path().to_path_buf())
    }

    fn acquire_lock_for_resolved_path(database_path: PathBuf) -> anyhow::Result<Self> {
        ensure_existing_production_database_single_link(&database_path)?;

        let parent_lock_path = parent_directory_lock_path(&database_path)?;
        let parent_directory_lock = acquire_exclusive_empty_lock_file(
            &parent_lock_path,
            "data-directory",
            "another WebCodex Server already owns this database directory",
        )?;

        let legacy_lock_path = lock_path_for_database(&database_path);
        let legacy_path_lock = match acquire_exclusive_empty_lock_file(
            &legacy_lock_path,
            "standalone Server ownership",
            "another WebCodex Server already owns this standalone state",
        ) {
            Ok(file) => file,
            Err(error) => {
                let _ = FileExt::unlock(&parent_directory_lock);
                return Err(error);
            }
        };

        ensure_existing_production_database_single_link(&database_path)?;

        Ok(Self {
            parent_directory_lock,
            legacy_path_lock,
            database_path,
        })
    }

    /// Canonical production database path this guard holds the instance lock for.
    pub fn database_path(&self) -> &Path {
        &self.database_path
    }

    /// Returns whether this guard's pre-open canonical lock identity matches the
    /// opened [`Database`] state path (post-open ownership verification).
    pub fn owns_database(&self, db: &Database) -> bool {
        if self.database_path != db.state_path() {
            return false;
        }
        production_database_identity_is_exclusive(db.state_path()).unwrap_or(false)
    }
}

impl Drop for ServerInstanceGuard {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.legacy_path_lock);
        let _ = FileExt::unlock(&self.parent_directory_lock);
    }
}

fn lock_path_for_database(database_path: &Path) -> PathBuf {
    let mut lock_path = OsString::from(database_path.as_os_str());
    lock_path.push(".server-instance.lock");
    PathBuf::from(lock_path)
}

fn parent_directory_lock_path(database_path: &Path) -> anyhow::Result<PathBuf> {
    if database_path
        .file_name()
        .is_some_and(|name| name == PARENT_DIRECTORY_LOCK_NAME)
    {
        bail!(
            "database filename must not equal the reserved parent-directory lock name ({PARENT_DIRECTORY_LOCK_NAME})"
        );
    }
    let Some(parent) = database_path.parent() else {
        bail!("database state path has no parent directory");
    };
    Ok(parent.join(PARENT_DIRECTORY_LOCK_NAME))
}

fn acquire_exclusive_empty_lock_file(
    lock_path: &Path,
    open_context: &str,
    busy_message: &str,
) -> anyhow::Result<File> {
    ensure_lock_parent_is_safe(lock_path)?;
    let file = open_empty_lock_target(lock_path).with_context(|| {
        format!("open {open_context} lock {}", lock_path.display())
    })?;
    ensure_opened_lock_file_is_exclusive_regular_file(&file, lock_path)?;
    file.try_lock_exclusive()
        .map_err(|_| anyhow!("{busy_message}"))?;
    Ok(file)
}

fn open_empty_lock_target(lock_path: &Path) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
        options.custom_flags(libc::O_NOFOLLOW);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    options.open(lock_path)
}

fn ensure_existing_production_database_single_link(database_path: &Path) -> anyhow::Result<()> {
    match std::fs::symlink_metadata(database_path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() || is_reparse_point(&meta) {
                bail!("database path must not be a symlink or reparse point");
            }
            if !meta.is_file() {
                bail!("database path is not a regular file");
            }
            let links = hard_link_count_nofollow(database_path).with_context(|| {
                format!(
                    "inspect hard-link count of database {}",
                    database_path.display()
                )
            })?;
            if links != 1 {
                bail!(
                    "database file must not be hard-linked (link count {links}); hard-linked production databases are unsupported"
                );
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "stat production database {}",
                    database_path.display()
                )
            });
        }
    }
    Ok(())
}

fn production_database_identity_is_exclusive(database_path: &Path) -> anyhow::Result<bool> {
    let meta = match std::fs::symlink_metadata(database_path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(error).with_context(|| {
                format!("stat database {}", database_path.display())
            });
        }
    };
    if meta.file_type().is_symlink() || is_reparse_point(&meta) {
        return Ok(false);
    }
    if !meta.is_file() {
        return Ok(false);
    }
    let links = hard_link_count_nofollow(database_path).with_context(|| {
        format!(
            "inspect hard-link count of database {}",
            database_path.display()
        )
    })?;
    Ok(links == 1)
}

fn hard_link_count_nofollow(path: &Path) -> anyhow::Result<u64> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
            .with_context(|| format!("open database path {} without following", path.display()))?;
        let meta = file
            .metadata()
            .with_context(|| format!("stat opened database {}", path.display()))?;
        if meta.file_type().is_symlink() {
            bail!("database path must not be a symlink");
        }
        if !meta.is_file() {
            bail!("database path is not a regular file");
        }
        Ok(meta.nlink())
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Foundation::HANDLE;
        use windows_sys::Win32::Storage::FileSystem::{
            GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION, FILE_FLAG_OPEN_REPARSE_POINT,
        };

        let file = OpenOptions::new()
            .read(true)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
            .open(path)
            .with_context(|| format!("open database path {} without following", path.display()))?;
        let meta = file
            .metadata()
            .with_context(|| format!("stat opened database {}", path.display()))?;
        if is_reparse_point(&meta) {
            bail!("database path must not be a reparse point");
        }
        if !meta.is_file() {
            bail!("database path is not a regular file");
        }

        let handle = file.as_raw_handle();
        let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
        let ok = unsafe { GetFileInformationByHandle(handle as HANDLE, &mut info) };
        if ok == 0 {
            let code = std::io::Error::last_os_error();
            return Err(code).with_context(|| {
                format!(
                    "read hard-link count for database {}",
                    path.display()
                )
            });
        }
        Ok(u64::from(info.nNumberOfLinks))
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = path;
        bail!("hard-link inspection is not supported on this platform");
    }
}

fn ensure_opened_lock_file_is_exclusive_regular_file(file: &File, lock_path: &Path) -> anyhow::Result<()> {
    let meta = file.metadata().with_context(|| {
        format!(
            "stat standalone Server ownership lock {}",
            lock_path.display()
        )
    })?;
    if meta.file_type().is_symlink() || is_reparse_point(&meta) {
        bail!("standalone Server ownership lock must not be a symlink or reparse point");
    }
    if !meta.is_file() {
        bail!("standalone Server ownership lock is not a regular file");
    }
    if meta.len() != 0 {
        bail!("standalone Server ownership lock must be empty");
    }
    let links = hard_link_count_from_open_file(file).with_context(|| {
        format!(
            "inspect hard-link count of standalone Server ownership lock {}",
            lock_path.display()
        )
    })?;
    if links != 1 {
        bail!("standalone Server ownership lock must not be hard-linked");
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
        use windows_sys::Win32::Storage::FileSystem::{GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION};

        let handle = file.as_raw_handle();
        let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
        let ok = unsafe { GetFileInformationByHandle(handle as HANDLE, &mut info) };
        if ok == 0 {
            return Err(std::io::Error::last_os_error()).context("read hard-link count for lock file");
        }
        Ok(u64::from(info.nNumberOfLinks))
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = file;
        bail!("hard-link inspection is not supported on this platform");
    }
}

fn resolve_database_state_path(db_path: &Path) -> anyhow::Result<PathBuf> {
    let absolute = absolute_lexical_database_path(db_path)?;
    let resolved = canonicalize_with_missing_tail(&absolute)?;
    validate_resolved_database_path(db_path, &resolved)?;
    Ok(resolved)
}

fn absolute_lexical_database_path(db_path: &Path) -> anyhow::Result<PathBuf> {
    let candidate = if db_path.is_absolute() {
        db_path.to_path_buf()
    } else {
        let cwd =
            std::env::current_dir().context("resolve working directory for database path")?;
        cwd.join(db_path)
    };
    lexical_normalize_absolute(&candidate)
}

fn lexical_normalize_absolute(path: &Path) -> anyhow::Result<PathBuf> {
    if !path.is_absolute() {
        bail!("database path must resolve to an absolute location");
    }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(Path::new("/")),
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() {
                    bail!("database path escapes its filesystem root");
                }
            }
            Component::Normal(value) => normalized.push(value),
        }
    }
    Ok(normalized)
}

fn canonicalize_with_missing_tail(path: &Path) -> anyhow::Result<PathBuf> {
    let mut probe = path.to_path_buf();
    let mut missing = Vec::new();
    loop {
        match std::fs::symlink_metadata(&probe) {
            Ok(_) => {
                let mut resolved = std::fs::canonicalize(&probe).with_context(|| {
                    format!("canonicalize database path {}", probe.display())
                })?;
                if !missing.is_empty() {
                    let parent_meta = std::fs::symlink_metadata(&resolved).with_context(|| {
                        format!("stat database parent directory {}", resolved.display())
                    })?;
                    if parent_meta.file_type().is_symlink() || is_reparse_point(&parent_meta) {
                        bail!("database parent directory must not be a symlink or reparse point");
                    }
                    if !parent_meta.is_dir() {
                        bail!("database parent path is not a directory");
                    }
                }
                for component in missing.iter().rev() {
                    resolved.push(component);
                }
                return Ok(resolved);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let Some(name) = probe.file_name().map(|name| name.to_os_string()) else {
                    bail!("database path has no usable parent for canonicalization");
                };
                missing.push(name);
                if !probe.pop() {
                    bail!("database path has no usable parent for canonicalization");
                }
            }
            Err(error) => {
                return Err(error).with_context(|| format!("stat database path {}", probe.display()));
            }
        }
    }
}

fn validate_resolved_database_path(requested: &Path, resolved: &Path) -> anyhow::Result<()> {
    match std::fs::symlink_metadata(requested) {
        Ok(meta) => {
            if meta.file_type().is_symlink() || is_reparse_point(&meta) {
                bail!("database path must not be a symlink or reparse point");
            }
            let canonical = std::fs::canonicalize(requested).with_context(|| {
                format!("canonicalize requested database path {}", requested.display())
            })?;
            if canonical != resolved {
                bail!("database path does not resolve to a stable canonical identity");
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| {
                format!("stat requested database path {}", requested.display())
            });
        }
    }

    let Some(parent) = resolved.parent() else {
        bail!("database state path has no parent directory");
    };
    let parent_meta = std::fs::symlink_metadata(parent).with_context(|| {
        format!("stat database parent directory {}", parent.display())
    })?;
    if parent_meta.file_type().is_symlink() || is_reparse_point(&parent_meta) {
        bail!("database parent directory must not be a symlink or reparse point");
    }
    Ok(())
}

fn ensure_lock_parent_is_safe(lock_path: &Path) -> anyhow::Result<()> {
    let Some(parent) = lock_path.parent() else {
        bail!("database lock path has no parent directory");
    };
    if parent.as_os_str().is_empty() {
        return Ok(());
    }
    let meta = std::fs::symlink_metadata(parent)
        .with_context(|| format!("stat database directory {}", parent.display()))?;
    if meta.file_type().is_symlink() || is_reparse_point(&meta) {
        bail!("database directory must not be a symlink or reparse point");
    }
    if !meta.is_dir() {
        bail!("database directory is not a directory");
    }
    Ok(())
}

fn is_reparse_point(meta: &std::fs::Metadata) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ownership_is_exclusive_per_database_directory_and_released_on_drop() {
        let temp = tempfile::tempdir().unwrap();
        let first_db = Database::open(&temp.path().join("first.db")).unwrap();
        let unrelated_db = Database::open(&temp.path().join("unrelated.db")).unwrap();

        let first_owner = ServerInstanceGuard::acquire(&first_db).unwrap();
        assert!(ServerInstanceGuard::acquire(&first_db).is_err());
        assert!(ServerInstanceGuard::acquire(&unrelated_db).is_err());

        drop(first_owner);
        let successor = ServerInstanceGuard::acquire(&first_db).unwrap();
        assert!(successor.owns_database(&first_db));

        let other_temp = tempfile::tempdir().unwrap();
        let other_db = Database::open(&other_temp.path().join("other.db")).unwrap();
        let unrelated_owner = ServerInstanceGuard::acquire(&other_db).unwrap();
        assert!(unrelated_owner.owns_database(&other_db));
        assert!(!unrelated_owner.owns_database(&first_db));
    }

    #[test]
    fn ownership_sidecar_contains_no_metadata() {
        let temp = tempfile::tempdir().unwrap();
        let db = Database::open(&temp.path().join("metadata.db")).unwrap();
        let guard = ServerInstanceGuard::acquire(&db).unwrap();
        assert_eq!(guard.parent_directory_lock.metadata().unwrap().len(), 0);
        assert_eq!(guard.legacy_path_lock.metadata().unwrap().len(), 0);
        drop(guard);
    }
}