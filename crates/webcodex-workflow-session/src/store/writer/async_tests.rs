use super::super::{SessionPersistence, SessionStore};
use super::*;
use serde_json::Value;
use std::path::Path;
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::time::Duration;

use crate::{PostSessionMessageInput, SessionMessageDelivery, SessionMessageKind, SessionMessagePriority};

const CANCELLATION_SURVIVOR_TEXT: &str = "must survive cancellation";

fn count_persisted_messages_with_text(
    ledger: &Path,
    session_id: &str,
    message_text: &str,
) -> Option<usize> {
    let bytes = std::fs::read(ledger).ok()?;
    let raw: Value = serde_json::from_slice(&bytes).ok()?;
    let sessions = raw.get("sessions")?.as_array()?;
    let record = sessions
        .iter()
        .find(|record| record.get("session_id").and_then(Value::as_str) == Some(session_id))?;
    let messages = record.get("messages")?.as_array()?;
    Some(
        messages
            .iter()
            .filter(|message| {
                message.get("message").and_then(Value::as_str) == Some(message_text)
            })
            .count(),
    )
}

fn sync_fallback_store(path: &std::path::Path) -> SessionStore {
    let mut store = SessionStore::new_in_memory_with_limits(16, 16, 20);
    store.inner.lock().unwrap().persistence = Some(SessionPersistence {
        path: path.to_path_buf(),
        restored_sessions: 0,
        last_persist_error: None,
        database: None,
        durable_db_path: None,
    });
    store.writer = None;
    store
}

fn persistent_writer_fixture(window: Duration) -> (tempfile::TempDir, SessionStore) {
    let temporary = tempfile::tempdir().unwrap();
    let mut store = SessionStore::new_in_memory_with_limits(16, 16, 20);
    store.inner.lock().unwrap().persistence = Some(SessionPersistence {
        path: temporary.path().join("sessions.json"),
        restored_sessions: 0,
        last_persist_error: None,
        database: None,
        durable_db_path: None,
    });
    store.writer = LedgerWriterGuard::spawn_with_window(
        store.inner.clone(),
        store.persistence_write_mutex.clone(),
        window,
    );
    assert!(store.writer.is_some());
    (temporary, store)
}

#[tokio::test]
async fn async_flush_registers_waiter_before_writer_completion_notify() {
    let (_temporary, store) = persistent_writer_fixture(Duration::from_secs(60));
    let writer = store.writer.as_ref().expect("persistent writer").clone();
    let hold = Arc::new((Mutex::new(true), Condvar::new()));
    writer.install_pre_completion_hold(Arc::clone(&hold));
    let (registered_tx, registered_rx) = mpsc::channel();
    writer.set_flush_wait_registered_sender(registered_tx);

    store.start_session(None, Some("hold-reg".to_string()));
    let generation = writer
        .mark_dirty()
        .expect("persistent writer must admit dirty mark");

    let flush = {
        let writer = Arc::clone(&writer);
        tokio::spawn(async move { writer.flush_through(generation).await })
    };
    tokio::task::spawn_blocking(move || {
        registered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("flush waiter must register before writer completion notify")
    })
    .await
    .expect("registration waiter join");
    {
        let mut blocked = hold.0.lock().unwrap();
        *blocked = false;
        hold.1.notify_all();
    }
    tokio::time::timeout(Duration::from_secs(5), flush)
        .await
        .expect("flush must not hang after notify once waiter was registered")
        .expect("flush task join");
}

#[tokio::test]
async fn async_flush_through_waits_exact_generation() {
    let (_temporary, store) = persistent_writer_fixture(Duration::from_secs(60));
    let worker = store.clone();
    let session_id = worker.start_session(None, None).session_id;
    store
        .persist_after_mutation_durable()
        .await
        .expect("async durable barrier");
    assert!(store.contains_session(&session_id));
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_durable_wait_does_not_skip_admitted_fallback_write() {
    let temporary = tempfile::tempdir().unwrap();
    let ledger = temporary.path().join("sessions.json");
    let store = sync_fallback_store(&ledger);
    let session = store.start_session(None, Some("async-cancel".to_string()));
    let delivery = SessionMessageDelivery {
        sender_scope: "d".repeat(64),
        delivery_key: "cancel-key".to_string(),
    };
    let input = PostSessionMessageInput {
        session_id: session.session_id.clone(),
        kind: SessionMessageKind::Progress,
        message: CANCELLATION_SURVIVOR_TEXT.to_string(),
        tags: Vec::new(),
        reply_to: None,
        priority: SessionMessagePriority::Normal,
    };

    // Hold the same mutex the admitted blocking write will take so cancellation
    // cannot complete persistence until we release it. In-memory visibility after
    // yield proves spawn_blocking was scheduled: post_message is synchronous and
    // complete_durable_mutation has no await before spawning the write.
    let write_guard = store
        .persistence_write_mutex
        .lock()
        .expect("session persistence mutex poisoned");
    let worker = store.clone();
    let session_id = session.session_id.clone();
    let handle = tokio::spawn(async move {
        worker
            .post_message_with_ack_and_delivery(input, false, Some(delivery))
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if store
                .list_messages(&session_id, Default::default())
                .map(|messages| !messages.is_empty())
                .unwrap_or(false)
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("in-memory message must appear while write mutex is held");
    handle.abort();
    let _ = handle.await;
    drop(write_guard);

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if count_persisted_messages_with_text(&ledger, &session_id, CANCELLATION_SURVIVOR_TEXT)
                .is_some_and(|count| count == 1)
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("admitted fallback write must finish after releasing write mutex");

    let count =
        count_persisted_messages_with_text(&ledger, &session.session_id, CANCELLATION_SURVIVOR_TEXT)
        .expect("ledger must contain persisted session after fallback write");
    assert_eq!(count, 1);
}
