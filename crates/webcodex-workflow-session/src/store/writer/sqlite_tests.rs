use super::tests::{append_read_fixture, persistent_fixture};
use super::*;
use crate::{
    CodingSessionRequest, SessionGuards, SessionTransport,
    TEST_ONLY_PROJECT_SESSION_AUTHORITY_FINGERPRINT,
};
use webcodex_core::workflow_session_contract::SessionMode;

#[test]
fn long_history_is_not_rewritten_and_cold_entries_do_not_own_it() {
    let (temporary, store) = persistent_fixture(Duration::from_secs(60));
    let id = store.start_session(None, None).session_id;
    for n in 0..5_100 {
        append_read_fixture(&store, &id, n);
    }
    store.flush_persistence();
    let connection = rusqlite::Connection::open(temporary.path().join("sessions.sqlite3")).unwrap();
    let before: i64 = connection
        .query_row(
            "SELECT count(*) FROM workflow_session_events WHERE session_id=?1",
            [&id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(before, 10_200);
    let protected = connection
        .query_row(
            "SELECT sum(length(payload)) FROM workflow_session_events",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap();
    connection.execute_batch("CREATE TRIGGER forbid_history_update BEFORE UPDATE ON workflow_session_events BEGIN SELECT RAISE(ABORT, 'unrelated history rewritten'); END;").unwrap();
    append_read_fixture(&store, &id, 6_000);
    store.flush_persistence();
    assert!(store.status().last_persist_error.is_none());
    let attempt = store
        .persistence_observation()
        .writer
        .unwrap()
        .last_attempt
        .unwrap();
    assert!(
        attempt.logical_payload_bytes.unwrap() < 20_000,
        "{:?}",
        attempt.logical_payload_bytes
    );
    assert!(protected > 1_000_000);
    {
        let mut inner = store.inner.lock().unwrap();
        let record = inner.sessions.get(&id).unwrap().hot().unwrap();
        assert!(record.pending_events.is_empty());
        let cold = record.durable.clone().unwrap();
        assert!(cold.raw.get().len() < 4_000);
        inner
            .sessions
            .replace_residency(id.clone(), super::super::StoredSession::Cold(cold));
    }
    assert!(store.cold_payload_bytes_for_test(&id).unwrap() < 4_000);
    let summary = store.summary(&id, Some(5)).unwrap();
    assert_eq!(summary.events_total, 10_202);
    assert_eq!(summary.events.len(), 5);
    drop(store);
    let restored =
        super::super::SessionStore::with_persistence(temporary.path().join("sessions.json"), 1, 20);
    assert_eq!(restored.summary(&id, Some(5)).unwrap().events_total, 10_202);
    assert_eq!(restored.status().hot_sessions, 0);
}

#[tokio::test]
async fn durable_observation_of_unchanged_state_does_not_write() {
    let (_temporary, store) = persistent_fixture(Duration::from_secs(60));
    store.start_session(None, None);
    store.flush().await.unwrap();
    let before = store
        .persistence_observation()
        .writer
        .unwrap()
        .write_attempts;
    for _ in 0..100 {
        store.flush().await.unwrap();
    }
    assert_eq!(
        store
            .persistence_observation()
            .writer
            .unwrap()
            .write_attempts,
        before
    );
}

#[test]
fn latest_instruction_and_closed_identity_survive_history_and_session_churn() {
    let (temporary, store) = persistent_fixture(Duration::from_secs(60));
    let request = CodingSessionRequest {
        project_identity: None,
        project: "agent:scale:populated".into(),
        authority_fingerprint: TEST_ONLY_PROJECT_SESSION_AUTHORITY_FINGERPRINT.into(),
        resume_session_id: None,
        instruction: Some("Keep this instruction across ten thousand operations".into()),
        mode: SessionMode::Normal,
        guards: SessionGuards::default(),
        execution_context: None,
        project_instructions: None,
        transport: SessionTransport::Api,
        context_refreshed: true,
        write_scope_verified: true,
    };
    let id = store
        .ensure_coding_session(request)
        .unwrap()
        .summary
        .session_id;
    for n in 0..80 {
        append_read_fixture(&store, &id, n);
    }
    store.close_session(&id).unwrap();
    for _ in 0..150 {
        let newer = store.start_session(None, None).session_id;
        store.close_session(&newer).unwrap();
    }
    store.flush_persistence();
    drop(store);
    let restored =
        super::super::SessionStore::with_persistence(temporary.path().join("sessions.json"), 1, 20);
    let summary = restored.summary(&id, Some(5)).unwrap();
    assert_eq!(summary.lifecycle, super::super::SessionLifecycle::Closed);
    assert_eq!(
        summary
            .current_instruction
            .as_ref()
            .unwrap()
            .instruction
            .as_deref(),
        Some("Keep this instruction across ten thousand operations")
    );
    assert_eq!(restored.status().retained_sessions, 151);
}

#[test]
fn stale_second_writer_fails_closed_instead_of_overwriting_a_newer_commit() {
    let (temporary, store) = persistent_fixture(Duration::from_secs(60));
    let id = store.start_session(None, None).session_id;
    store.flush_persistence();
    let stale =
        super::super::SessionStore::with_persistence(temporary.path().join("sessions.json"), 1, 20);
    append_read_fixture(&store, &id, 1);
    store.flush_persistence();
    // The stale cache refuses hydration of a different disk version.
    assert!(stale.summary(&id, Some(5)).is_none());
    assert!(stale.contains_session(&id));
    assert!(stale.session_created_at(&id).is_some());
    assert_eq!(store.summary(&id, Some(5)).unwrap().events_total, 2);
}

#[test]
fn mutation_after_snapshot_remains_dirty_until_its_own_commit() {
    let (temporary, store) = persistent_fixture(Duration::from_secs(60));
    let id = store.start_session(None, None).session_id;
    append_read_fixture(&store, &id, 1);
    let captured = store.inner.lock().unwrap().sessions.generation;
    let (generation, committed, _, _) =
        persist_batch_observed(&store.inner, &store.persistence_write_mutex, |writes| {
            assert_eq!(writes[0].events.len(), 2);
            // Deterministically interleave a later mutation after releasing the
            // snapshot lock but before committing/acknowledging the earlier batch.
            append_read_fixture(&store, &id, 2);
            assert_eq!(writes[0].events.len(), 2);
        });
    assert_eq!(generation, captured);
    committed.unwrap();
    let connection = rusqlite::Connection::open(temporary.path().join("sessions.sqlite3")).unwrap();
    let count = || {
        connection
            .query_row("SELECT count(*) FROM workflow_session_events", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap()
    };
    assert_eq!(count(), 2);
    {
        let inner = store.inner.lock().unwrap();
        assert!(inner.sessions.dirty[&id] > captured);
        assert_eq!(
            inner
                .sessions
                .get(&id)
                .unwrap()
                .hot()
                .unwrap()
                .pending_events
                .len(),
            2
        );
    }
    store.flush_persistence();
    assert_eq!(count(), 4);
    assert!(store.inner.lock().unwrap().sessions.dirty.is_empty());
    drop(store);
    let restored =
        super::super::SessionStore::with_persistence(temporary.path().join("sessions.json"), 1, 20);
    assert_eq!(restored.summary(&id, Some(8)).unwrap().events_total, 4);
}

#[tokio::test]
async fn a_failed_multi_session_transaction_publishes_nothing_and_can_retry() {
    let (temporary, store) = persistent_fixture(Duration::from_secs(60));
    let mut ids = [
        store.start_session(None, None).session_id,
        store.start_session(None, None).session_id,
    ];
    ids.sort();
    for id in &ids {
        append_read_fixture(&store, id, 1);
    }
    let connection = rusqlite::Connection::open(temporary.path().join("sessions.sqlite3")).unwrap();
    connection.execute_batch(&format!("CREATE TRIGGER fail_second BEFORE INSERT ON workflow_sessions WHEN NEW.session_id='{}' BEGIN SELECT RAISE(ABORT, 'injected second row failure'); END;", ids[1])).unwrap();
    assert!(store.flush().await.is_err());
    for table in ["workflow_sessions", "workflow_session_events"] {
        assert_eq!(
            connection
                .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }
    assert_eq!(store.inner.lock().unwrap().sessions.dirty.len(), 2);
    connection
        .execute_batch("DROP TRIGGER fail_second")
        .unwrap();
    store.flush().await.unwrap();
    assert_eq!(
        connection
            .query_row("SELECT count(*) FROM workflow_session_events", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        4
    );
}

#[tokio::test]
async fn message_delivery_identity_survives_five_hundred_newer_messages_and_restart() {
    use crate::{
        ListSessionMessagesFilter, PostSessionMessageInput, SessionMessageDelivery,
        SessionMessageKind, SessionMessagePriority,
    };
    let (temporary, store) = persistent_fixture(Duration::from_secs(60));
    let id = store.start_session(None, None).session_id;
    let input = PostSessionMessageInput {
        session_id: id.clone(),
        kind: SessionMessageKind::Note,
        message: "original durable note".into(),
        tags: Vec::new(),
        reply_to: None,
        priority: SessionMessagePriority::Normal,
    };
    let delivery = || {
        Some(SessionMessageDelivery {
            sender_scope: "a".repeat(64),
            delivery_key: "stable-first-delivery".into(),
        })
    };
    let first = store
        .post_message_with_ack_and_delivery(input.clone(), false, delivery())
        .await
        .unwrap();
    for n in 0..500 {
        let mut newer = input.clone();
        newer.message = format!("newer note {n}");
        store.post_message(newer).unwrap();
    }
    store.flush().await.unwrap();
    drop(store);
    let restored =
        super::super::SessionStore::with_persistence(temporary.path().join("sessions.json"), 1, 20);
    let replay = restored
        .post_message_with_ack_and_delivery(input, false, delivery())
        .await
        .unwrap();
    assert!(replay.replayed);
    assert!(!replay.state_changed);
    assert_eq!(replay.message.message_id, first.message.message_id);
    let exact = restored
        .list_messages(
            &id,
            ListSessionMessagesFilter {
                message_id: Some(first.message.message_id),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(exact.len(), 1);
    let connection = rusqlite::Connection::open(temporary.path().join("sessions.sqlite3")).unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT count(*) FROM workflow_session_messages",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        501
    );
}

#[test]
fn import_is_atomic_once_and_preserves_the_original_source() {
    let temporary = tempfile::tempdir().unwrap();
    let legacy = temporary.path().join("sessions.json");
    let source = super::super::SessionStore::new(1, 20);
    let id = source.start_session(None, None).session_id;
    let original =
        serde_json::to_string(&source.inner.lock().unwrap().to_persisted_ledger()).unwrap();
    std::fs::write(&legacy, &original).unwrap();
    let store = super::super::SessionStore::with_persistence(&legacy, 1, 20);
    append_read_fixture(&store, &id, 1);
    store.flush_persistence();
    assert_eq!(std::fs::read_to_string(&legacy).unwrap(), original);
    drop(store);
    std::fs::write(&legacy, "now corrupt: must not be imported twice").unwrap();
    let restored = super::super::SessionStore::with_persistence(&legacy, 1, 20);
    assert_eq!(restored.summary(&id, Some(5)).unwrap().events_total, 2);
}

#[test]
fn mismatched_indexed_authority_is_rejected_on_restart() {
    let (temporary, store) = persistent_fixture(Duration::from_secs(60));
    store.start_session(None, None);
    store.flush_persistence();
    drop(store);
    let path = temporary.path().join("sessions.sqlite3");
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute("UPDATE workflow_sessions SET owner=?1", ["a".repeat(64)])
        .unwrap();
    assert!(super::super::SessionStore::open_database(&path, None, 1, 20).is_err());
}

// The exact personal v1 layout predates the current normalized schema even
// though both use version 1. Fixture DDL is frozen compatibility evidence.
fn create_fork_v1_fixture_schema(connection: &rusqlite::Connection) {
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
INSERT INTO workflow_session_storage(id,schema_version,legacy_imported) VALUES(1,1,1);
"#,
        )
        .unwrap();
}

#[tokio::test]
async fn fork_sqlite_migration_preserves_retained_history_and_replay_authority() {
    use crate::{
        detect_workflow_session_schema, migrate_fork_session_schema, model::PersistedSessionRecord,
        CompleteSessionMessageInput, PostSessionMessageInput, SessionMessageDelivery,
        SessionMessageKind, SessionMessagePriority, SessionProjectIdentity, WorkflowSessionSchema,
    };
    use rusqlite::{params, Connection, TransactionBehavior};
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("fork.sqlite3");
    let legacy = temporary.path().join("sessions.json");
    let store = super::super::SessionStore::new(16, 20);
    let identity = SessionProjectIdentity::from_runner_inventory(
        Some(format!(
            "{}{}",
            webcodex_core::runner_protocol::PROJECT_ROOT_FINGERPRINT_PREFIX,
            "a".repeat(64)
        )),
        String::new(),
        "12345678-1234-4234-8234-123456789abc".into(),
    )
    .unwrap();
    let id = store
        .ensure_coding_session(CodingSessionRequest {
            project: "agent:fork:repo".into(),
            project_identity: Some(identity.clone()),
            authority_fingerprint: TEST_ONLY_PROJECT_SESSION_AUTHORITY_FINGERPRINT.into(),
            resume_session_id: None,
            instruction: Some("preserve old instruction".into()),
            mode: SessionMode::Normal,
            guards: SessionGuards::default(),
            execution_context: None,
            project_instructions: None,
            transport: SessionTransport::Api,
            context_refreshed: true,
            write_scope_verified: true,
        })
        .unwrap()
        .summary
        .session_id;
    for n in 0..4 {
        append_read_fixture(&store, &id, n);
    }
    let input = PostSessionMessageInput {
        session_id: id.clone(),
        kind: SessionMessageKind::Note,
        message: "durable original".into(),
        tags: vec![],
        reply_to: None,
        priority: SessionMessagePriority::Normal,
    };
    let delivery = SessionMessageDelivery {
        sender_scope: "b".repeat(64),
        delivery_key: "fork-delivery".into(),
    };
    let first = store
        .post_message_with_ack_and_delivery(input.clone(), true, Some(delivery.clone()))
        .await
        .unwrap();
    let todo = store
        .post_message(PostSessionMessageInput {
            kind: SessionMessageKind::Todo,
            message: "complete exact assignment".into(),
            ..input.clone()
        })
        .unwrap();
    let assignment = store.get_assignment(&id, &todo.message_id).await.unwrap();
    let completion = CompleteSessionMessageInput {
        session_id: id.clone(),
        message_id: todo.message_id.clone(),
        answer: "done".into(),
        tags: vec![],
        priority: SessionMessagePriority::Normal,
        completion_id: "e".repeat(crate::model::MESSAGE_COMPLETION_FINGERPRINT_HEX_CHARS),
        author_session_id: None,
        expected_assignment_fence: assignment.assignment_fence,
    };
    let completed = store.complete_message(completion.clone()).await.unwrap();
    let mut record = {
        let inner = store.inner.lock().unwrap();
        PersistedSessionRecord::from_record(
            inner.sessions.get(&id).unwrap().hot().unwrap(),
            usize::MAX,
        )
    };
    // The old writer used one-based retained ordinals and a cumulative count.
    // Seven events are already gone; conversion cannot invent them.
    record.events_observed += 7;
    let mut state = serde_json::to_value(&record).unwrap();
    let object = state.as_object_mut().unwrap();
    object.remove("current_instruction");
    object.remove("event_history_start");
    object.insert("events".into(), serde_json::json!([]));
    object.insert("messages".into(), serde_json::json!([]));
    object.insert("message_delivery_replays".into(), serde_json::json!({}));
    let mut connection = Connection::open(&path).unwrap();
    create_fork_v1_fixture_schema(&connection);
    connection
        .execute(
            "INSERT INTO workflow_session_state VALUES(?1,?2,?3,?4,?5,?6,?7,?7,?8,?9)",
            params![
                id,
                record.project,
                record.owner_authority_fingerprint,
                record.created_at,
                record.updated_at,
                41_i64,
                record.message_observation_revision as i64,
                record.events_observed as i64,
                state.to_string()
            ],
        )
        .unwrap();
    for (n, event) in record.events.iter().enumerate() {
        connection
            .execute(
                "INSERT INTO workflow_session_events VALUES(?1,?2,?3,?4)",
                params![
                    id,
                    8_i64 + n as i64,
                    event.event_id,
                    serde_json::to_string(event).unwrap()
                ],
            )
            .unwrap();
    }
    for message in &record.messages {
        connection
            .execute(
                "INSERT INTO workflow_session_messages VALUES(?1,?2,?3,?4)",
                params![
                    id,
                    message.message_id,
                    record.message_observation_revisions[&message.message_id] as i64,
                    serde_json::to_string(message).unwrap()
                ],
            )
            .unwrap();
    }
    for (scope, replay) in &record.message_delivery_replays {
        connection
            .execute(
                "INSERT INTO workflow_session_message_delivery_replays VALUES(?1,?2,?3,?4)",
                params![id, scope, replay.payload_fingerprint, replay.message_id],
            )
            .unwrap();
    }
    let tombstone = store.start_session(None, None).session_id;
    connection
        .execute(
            "INSERT INTO workflow_session_retention_tombstones VALUES(?1,?2,?3,?4)",
            params![tombstone, "c".repeat(64), "d".repeat(64), 17_i64],
        )
        .unwrap();
    drop(store);
    // Ordinary open must not perform the conversion implicitly.
    assert!(super::super::SessionStore::open_database(&path, None, 1024, 4).is_err());
    assert_eq!(
        detect_workflow_session_schema(&connection).unwrap(),
        WorkflowSessionSchema::ForkV1
    );
    // Deliberately corrupt an ordinal inside a transaction, then exercise
    // migration rejection and rollback of all table renames/insertions.
    {
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        tx.execute(
            "UPDATE workflow_session_events SET ordinal=ordinal+1000",
            [],
        )
        .unwrap();
        assert!(migrate_fork_session_schema(&tx).is_err());
        tx.rollback().unwrap();
    }
    assert_eq!(
        detect_workflow_session_schema(&connection).unwrap(),
        WorkflowSessionSchema::ForkV1
    );
    {
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        migrate_fork_session_schema(&tx).unwrap();
        tx.commit().unwrap();
    }
    assert_eq!(
        detect_workflow_session_schema(&connection).unwrap(),
        WorkflowSessionSchema::CurrentV1
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT min(ordinal) FROM workflow_session_events",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        7
    );
    assert_eq!(
        connection
            .query_row("SELECT revision FROM workflow_sessions", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        41
    );
    assert_eq!(
        connection
            .query_row("SELECT count(*) FROM workflow_session_events", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        record.events.len() as i64
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT count(*) FROM workflow_session_messages WHERE completion_fence IS NOT NULL",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
    drop(connection);
    // Preserved imported marker prevents old JSON from being read or replayed.
    std::fs::write(&legacy, "obsolete source must not be parsed").unwrap();
    let restored =
        super::super::SessionStore::open_database(&path, Some(&legacy), 16 * 1024 * 1024, 4)
            .unwrap();
    let summary = restored.summary(&id, Some(4)).unwrap();
    assert_eq!(summary.events_total as u64, record.events_observed);
    assert_eq!(summary.ledger_first_retained_sequence, 7);
    assert!(summary.retention_truncated);
    assert_eq!(restored.session_project_identity(&id), Some(Some(identity)));
    assert_eq!(
        restored
            .retention_tombstone_for_test(&tombstone)
            .unwrap()
            .expiry_ordinal,
        17
    );
    assert!(!restored.contains_session(&tombstone));
    let replay = restored
        .post_message_with_ack_and_delivery(input, true, Some(delivery))
        .await
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.message.message_id, first.message.message_id);
    let completion_replay = restored.complete_message(completion).await.unwrap();
    assert!(completion_replay.replayed);
    assert_eq!(
        completion_replay.answer.message_id,
        completed.answer.message_id
    );
}

#[test]
fn abrupt_process_exit_recovers_committed_wal_and_excludes_unconfirmed_queue() {
    use std::process::{Command, Stdio};
    let directory = tempfile::tempdir().unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "store::writer::sqlite_tests::abrupt_exit_child",
            "--ignored",
            "--nocapture",
        ])
        .env("WEBCODEX_SESSION_CRASH_FIXTURE", directory.path())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let id = stdout
        .lines()
        .find_map(|line| line.strip_prefix("committed-session:"))
        .expect("child confirmed its exact commit");
    let wal = directory.path().join("sessions.sqlite3-wal");
    assert!(
        std::fs::metadata(wal).unwrap().len() > 0,
        "child must exit without closing/checkpointing SQLite"
    );
    let store =
        super::super::SessionStore::with_persistence(directory.path().join("sessions.json"), 1, 20);
    let summary = store.summary(id, Some(10)).unwrap();
    assert_eq!(summary.session_id, id);
    assert_eq!(summary.events_total, 2);
    assert_eq!(summary.events.len(), 2);
    assert_eq!(store.status().retained_sessions, 1);
}

#[test]
#[ignore = "subprocess fixture; invoked only by abrupt_process_exit_recovers_committed_wal_and_excludes_unconfirmed_queue"]
fn abrupt_exit_child() {
    let path =
        std::env::var_os("WEBCODEX_SESSION_CRASH_FIXTURE").expect("isolated child fixture path");
    let mut store = super::super::SessionStore::with_persistence(
        std::path::PathBuf::from(path).join("sessions.json"),
        1,
        20,
    );
    drop(store.writer.take());
    store.writer = LedgerWriterGuard::spawn_with_window(
        store.inner.clone(),
        store.persistence_write_mutex.clone(),
        Duration::from_secs(60),
    );
    let id = store.start_session(None, None).session_id;
    append_read_fixture(&store, &id, 1);
    store.flush_persistence();
    assert!(store.status().last_persist_error.is_none());
    println!("committed-session:{id}");
    append_read_fixture(&store, &id, 2);
    // Intentionally skip all Rust destructors and the final Session writer drain.
    // This exercises process death, not a claim about hardware power-loss safety.
    std::process::exit(0);
}
