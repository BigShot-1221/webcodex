use crate::Database;
use std::sync::{Arc, Barrier};

#[test]
fn bundled_sqlite_contains_wal_reset_fix() {
    // https://sqlite.org/wal.html#the_wal_reset_bug -- fixed on this release line
    // in 3.51.3. This is a dependency fence, not a reproduction of the rare race.
    assert!(
        rusqlite::version_number() >= 3_051_003,
        "WAL fix missing: {}",
        rusqlite::version()
    );
    eprintln!(
        "SQLITE_RUNTIME version={} version_number={}",
        rusqlite::version(),
        rusqlite::version_number()
    );
}

#[test]
fn concurrent_wal_checkpoint_and_writer_preserve_all_committed_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("wal.db");
    let db = Database::open(&path).unwrap();
    db.conn_for_tests()
        .execute_batch("CREATE TABLE checkpoint_probe(id INTEGER PRIMARY KEY);")
        .unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let writer_path = path.clone();
    let writer_barrier = barrier.clone();
    let writer = std::thread::spawn(move || {
        let mut conn = rusqlite::Connection::open(writer_path).unwrap();
        conn.busy_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        writer_barrier.wait();
        for batch in 0..64 {
            let tx = conn.transaction().unwrap();
            for item in 0..8 {
                tx.execute(
                    "INSERT INTO checkpoint_probe VALUES (?1)",
                    [batch * 8 + item],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }
    });
    let checkpoint = rusqlite::Connection::open(&path).unwrap();
    barrier.wait();
    for _ in 0..64 {
        let _: (i64, i64, i64) = checkpoint
            .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })
            .unwrap();
        std::thread::yield_now();
    }
    writer.join().unwrap();
    assert_eq!(
        checkpoint
            .query_row("SELECT count(*) FROM checkpoint_probe", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        512
    );
    assert_eq!(
        checkpoint
            .query_row("PRAGMA quick_check", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
}

#[test]
fn shared_database_wal_observation_reports_uncheckpointed_frames_without_noop_checkpoint() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("noop-observe.db");
    let db = Database::open(&path).unwrap();
    {
        let mut conn = db.conn_for_tests();
        conn.execute_batch("PRAGMA wal_autocheckpoint = 0;")
            .unwrap();
        let tx = conn.transaction().unwrap();
        tx.execute_batch(
            "CREATE TABLE wal_noop_probe(id INTEGER PRIMARY KEY);
             INSERT INTO wal_noop_probe VALUES (1), (2), (3);",
        )
        .unwrap();
        tx.commit().unwrap();
    }

    let first = db.shared_database_wal_observation();
    assert_eq!(first.status, "available");
    assert_eq!(first.checkpoint_busy, Some(false));
    assert!(first.error_kind.is_none());
    assert!(first.log_frames.is_some());
    assert!(first.checkpointed_frames.is_some());
    let uncheckpointed = first
        .uncheckpointed_frames
        .expect("NOOP exposes frame metrics when available");
    assert!(
        uncheckpointed > 0,
        "expected uncheckpointed WAL frames after disabled autocheckpoint: {first:?}"
    );
    assert!(first.wal_bytes.unwrap_or(0) > 0);

    let second = db.shared_database_wal_observation();
    assert_eq!(second.status, "available");
    assert_eq!(second.checkpointed_frames, first.checkpointed_frames);
    assert_eq!(second.uncheckpointed_frames, first.uncheckpointed_frames);
    assert_eq!(second.checkpoint_busy, Some(false));
}

#[test]
fn shared_database_wal_observation_busy_when_diagnostic_mutex_held() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Database::open(&tmp.path().join("diag-mutex.db")).unwrap();
    let _ = db.shared_database_wal_observation();
    let diagnostic = db
        .wal_diagnostic_connection
        .get()
        .expect("lazy diagnostic connection initialized by first observation");
    let _guard = diagnostic.lock().unwrap();
    let observation = db.shared_database_wal_observation();
    assert_eq!(observation.status, "busy");
    assert!(observation.checkpoint_busy.is_none());
    assert!(observation.error_kind.is_none());
}
