//! Explicit offline migration owner. Normal Server startup cannot invoke this.
//! A current-format database is an idempotent no-op; an old-format database
//! requires the same exclusive guard as Server/restore and a verified backup.
use super::*;
use rusqlite::TransactionBehavior;
use webcodex_workflow_session::{
    detect_workflow_session_schema, migrate_fork_session_schema, WorkflowSessionSchema,
};

impl OfflineRestoreOwner {
    /// Migrate the exact selected database under offline instance ownership.
    /// Operator must stop all other SQLite users, including external tools.
    /// The published backup is a rollback artifact of the same pre-write
    /// snapshot. A failed or uncertain commit never deletes that artifact.
    pub fn migrate_fork_workflow_sessions(&self) -> anyhow::Result<Option<crate::BackupArtifact>> {
        let guard = self.server_instance_guard();
        if RestoreJournal::load_for_guard(guard)?.is_some() {
            bail!("restore journal exists; resolve startup restore recovery before migration");
        }
        let path = self.production_database_path();
        let meta = fs::symlink_metadata(path).context("inspect migration database")?;
        if !meta.is_file() || meta.file_type().is_symlink() || is_reparse_point(&meta) {
            bail!("migration requires an existing regular database");
        }
        let identity = SourceFileIdentity::from_source_path(path)?;
        let mut conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)
            .context("open existing offline migration database")?;
        conn.execute_batch(
            "PRAGMA foreign_keys=ON; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;",
        )?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .context("acquire exclusive migration write reservation")?;
        if SourceFileIdentity::from_source_path(path)? != identity {
            bail!("migration database identity changed");
        }
        match detect_workflow_session_schema(&tx)? {
            WorkflowSessionSchema::CurrentV1 => {
                run_sqlite_preflight(&tx, RestoreWorkflowSessionRequirement::RequirePresent)?;
                return Ok(None);
            }
            WorkflowSessionSchema::Absent => bail!("no Fork Workflow Session domain to migrate"),
            WorkflowSessionSchema::ForkV1 => {}
        }
        run_sqlite_preflight(&tx, RestoreWorkflowSessionRequirement::RequirePresent)?;
        // The IMMEDIATE transaction prevents another SQLite writer changing the
        // snapshot between validation, online backup and migration commit.
        let backup = crate::backup::create_offline_migration_backup(guard)
            .context("create verified pre-migration rollback backup")?;
        self.preflight_candidate(
            &backup.path,
            RestoreWorkflowSessionRequirement::RequirePresent,
        )
        .context("verify published rollback backup")?;
        let backup_connection = open_readonly_restore_candidate(&backup.path)?;
        let backup_schema = detect_workflow_session_schema(&backup_connection);
        close_readonly_connection(backup_connection)?;
        if backup_schema? != WorkflowSessionSchema::ForkV1 {
            bail!("rollback backup no longer has the original Fork schema; migration refused");
        }
        if SourceFileIdentity::from_source_path(path)? != identity
            || RestoreJournal::load_for_guard(guard)?.is_some()
        {
            bail!("migration ownership changed after backup; no migration committed");
        }
        if let Err(err) = migrate_fork_session_schema(&tx)
            .map_err(anyhow::Error::from)
            .and_then(|_| {
                run_sqlite_preflight(&tx, RestoreWorkflowSessionRequirement::RequirePresent)
                    .map(|_| ())
            })
        {
            tx.rollback().with_context(|| {
                format!(
                    "migration rollback not confirmed; preserve backup at {}",
                    backup.path.display()
                )
            })?;
            if !conn.is_autocommit()
                || detect_workflow_session_schema(&conn).ok() != Some(WorkflowSessionSchema::ForkV1)
                || SourceFileIdentity::from_source_path(path)? != identity
            {
                bail!(
                    "migration rollback outcome not verified; preserve backup at {}",
                    backup.path.display()
                );
            }
            return Err(err.context(format!(
                "migration rolled back; original backup retained at {}",
                backup.path.display()
            )));
        }
        if let Err(err) = tx.commit() {
            return Err(anyhow::Error::new(err).context(format!(
                "migration commit outcome not confirmed; do not retry until database format is inspected; backup retained at {}",
                backup.path.display()
            )));
        }
        // Recheck committed state, not merely the in-transaction projection.
        if detect_workflow_session_schema(&conn).ok() != Some(WorkflowSessionSchema::CurrentV1)
            || SourceFileIdentity::from_source_path(path)? != identity
        {
            bail!("migration committed format or file identity could not be verified; backup retained at {}", backup.path.display());
        }
        run_sqlite_preflight(&conn, RestoreWorkflowSessionRequirement::RequirePresent)
            .with_context(|| {
                format!(
                    "post-commit validation failed; backup retained at {}",
                    backup.path.display()
                )
            })?;
        Ok(Some(backup))
    }
}

/// Convert an isolated, exclusively owned restore stage, never its source
/// backup. DELETE journaling plus FULL synchronous makes the result a single
/// standalone database file before its identity/hash are put into the journal.
pub(super) fn migrate_private_restore_stage(path: &Path) -> anyhow::Result<()> {
    let mut conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    let mode: String = conn.query_row("PRAGMA journal_mode=DELETE", [], |r| r.get(0))?;
    if !mode.eq_ignore_ascii_case("delete") {
        bail!("restore migration requires standalone DELETE journal mode");
    }
    conn.execute_batch(
        "PRAGMA foreign_keys=ON; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;",
    )?;
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    migrate_fork_session_schema(&tx)?;
    run_sqlite_preflight(&tx, RestoreWorkflowSessionRequirement::RequirePresent)?;
    tx.commit()
        .context("commit private restore stage migration")?;
    if detect_workflow_session_schema(&conn)? != WorkflowSessionSchema::CurrentV1 {
        bail!("restore stage migration format could not be verified");
    }
    run_sqlite_preflight(&conn, RestoreWorkflowSessionRequirement::RequirePresent)?;
    conn.close().map_err(|(_, error)| error)?;
    require_no_sidecars(path)?;
    Ok(())
}
