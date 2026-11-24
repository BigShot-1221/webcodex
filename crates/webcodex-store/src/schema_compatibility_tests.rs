use super::Database;
use rusqlite::Connection;

#[test]
fn absent_optional_domain_initializes_and_current_domains_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("supported-domains.db");
    let db = Database::open(&path).unwrap();
    db.conn_for_tests()
        .execute("DROP TABLE wc_agent_attention_events", [])
        .unwrap();
    drop(db);
    drop(Database::open(&path).unwrap());
}

#[test]
fn pre_v046_domain_formats_are_rejected_without_writing() {
    for (table, retained_columns) in [
        ("wc_agent_waits", "goal_id TEXT"),
        ("wc_agent_waits", "mode TEXT"),
        ("wc_agent_endpoints", "mcp_app_recovery_fingerprint TEXT"),
        ("wc_agent_endpoints", "mcp_app_client_window_key TEXT"),
        ("wc_goals", "controller_agent_id TEXT"),
        ("wc_goals", "plan_json TEXT"),
        ("wc_agent_attention_events", "event_id TEXT"),
        ("wc_artifact_handoff_acceptances", "acceptance_id TEXT"),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("pre-v046.db");
        {
            let conn = Connection::open(&path).unwrap();
            // Only the retired migration's discriminator columns matter here.
            // A retained row also makes unintended rebuild/backfill writes visible.
            conn.execute_batch(&format!(
                "CREATE TABLE {table} (retained_value TEXT NOT NULL, {retained_columns});
                 INSERT INTO {table} (retained_value) VALUES ('keep');"
            ))
            .unwrap();
        }
        let before = std::fs::read(&path).unwrap();
        let error = Database::open(&path)
            .err()
            .expect("pre-v0.4.6 domain must be rejected")
            .to_string();
        assert!(
            error.contains(&format!("unsupported pre-v0.4.6 {table} schema")),
            "{table} ({retained_columns}): {error}"
        );
        assert!(error.contains("upgrade through v0.4.6 first"));
        assert_eq!(
            std::fs::read(&path).unwrap(),
            before,
            "rejected {table} ({retained_columns}) must remain byte-for-byte intact"
        );
        assert!(!temp.path().join("pre-v046.db-wal").exists());
    }
}

// Fixture is the exact released personal Fork schema, not a new live writer.
#[test]
fn offline_fork_schema_backup_and_explicit_restore_use_the_same_format_contract() {
    use crate::{OfflineRestoreOwner, RestoreWorkflowSessionRequirement};
    use webcodex_workflow_session::{
        detect_workflow_session_schema, SessionStore, WorkflowSessionSchema,
    };
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("state.db");
    drop(Database::open(&path).unwrap());
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            r#"
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS workflow_session_storage (
id INTEGER PRIMARY KEY CHECK (id = 1),
schema_version INTEGER NOT NULL,
legacy_imported INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS workflow_session_state (
session_id TEXT PRIMARY KEY,
project TEXT,
owner_authority_fingerprint TEXT NOT NULL,
created_at INTEGER NOT NULL,
updated_at INTEGER NOT NULL,
revision INTEGER NOT NULL,
previous_message_revision INTEGER NOT NULL DEFAULT 0,
message_observation_revision INTEGER NOT NULL,
events_observed INTEGER NOT NULL,
state_json TEXT NOT NULL,
CHECK (revision > 0)
);
CREATE INDEX IF NOT EXISTS idx_workflow_session_state_updated
ON workflow_session_state (updated_at, session_id);

CREATE TABLE IF NOT EXISTS workflow_session_events (
session_id TEXT NOT NULL
REFERENCES workflow_session_state(session_id) ON DELETE CASCADE,
ordinal INTEGER NOT NULL,
event_id TEXT NOT NULL,
event_json TEXT NOT NULL,
PRIMARY KEY (session_id, ordinal),
UNIQUE (event_id)
);
CREATE INDEX IF NOT EXISTS idx_workflow_session_events_session_ordinal
ON workflow_session_events (session_id, ordinal);

CREATE TABLE IF NOT EXISTS workflow_session_messages (
session_id TEXT NOT NULL
REFERENCES workflow_session_state(session_id) ON DELETE CASCADE,
message_id TEXT NOT NULL,
observation_revision INTEGER NOT NULL DEFAULT 0,
message_json TEXT NOT NULL,
PRIMARY KEY (session_id, message_id)
);

CREATE TABLE IF NOT EXISTS workflow_session_message_delivery_replays (
session_id TEXT NOT NULL
REFERENCES workflow_session_state(session_id) ON DELETE CASCADE,
scope_key TEXT NOT NULL,
payload_fingerprint TEXT NOT NULL,
message_id TEXT NOT NULL,
PRIMARY KEY (session_id, scope_key),
FOREIGN KEY (session_id, message_id)
REFERENCES workflow_session_messages(session_id, message_id)
);

CREATE TABLE IF NOT EXISTS workflow_session_retention_tombstones (
session_id TEXT PRIMARY KEY,
owner_authority_fingerprint TEXT NOT NULL,
incarnation_fingerprint TEXT NOT NULL,
expiry_ordinal INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_workflow_session_retention_tombstones_expiry
ON workflow_session_retention_tombstones (expiry_ordinal);
INSERT INTO workflow_session_storage(id,schema_version,legacy_imported) VALUES(1,1);
"#,
        )
        .unwrap();
    assert_eq!(
        detect_workflow_session_schema(&connection).unwrap(),
        WorkflowSessionSchema::ForkV1
    );
    drop(connection);
    assert!(Database::open(&path).is_err());
    let owner = OfflineRestoreOwner::acquire(&path).unwrap();
    let backup = owner.migrate_fork_workflow_sessions().unwrap().unwrap();
    assert!(owner.migrate_fork_workflow_sessions().unwrap().is_none());
    let facts = owner
        .preflight_candidate(
            &backup.path,
            RestoreWorkflowSessionRequirement::RequirePresent,
        )
        .unwrap();
    assert!(facts.workflow_session_domain_present);
    let backup_connection =
        Connection::open_with_flags(&backup.path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    assert_eq!(
        detect_workflow_session_schema(&backup_connection).unwrap(),
        WorkflowSessionSchema::ForkV1
    );
    drop(backup_connection);
    // The ordinary restore API must not silently authorize a conversion.
    assert!(owner.restore_from_backup(&backup.path).is_err());
    owner.restore_from_fork_backup(&backup.path).unwrap();
    let mut pending = owner
        .recover_startup_before_database_open()
        .unwrap()
        .unwrap();
    let db = Database::open(&path).unwrap();
    let sessions = SessionStore::open_database(&path, None, 1024, 20).unwrap();
    pending.commit_after_session_database(&db).unwrap();
    drop(sessions);
    drop(db);
    drop(pending);
    let backup_connection =
        Connection::open_with_flags(&backup.path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    assert_eq!(
        detect_workflow_session_schema(&backup_connection).unwrap(),
        WorkflowSessionSchema::ForkV1
    );
}
