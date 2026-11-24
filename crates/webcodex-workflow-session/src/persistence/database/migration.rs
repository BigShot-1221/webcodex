//! Offline, one-transaction migration of the personal Fork v1 schema.
//! The caller must hold exclusive instance ownership, ensure no live writers,
//! and verify a recoverable backup before calling this function. Restore may
//! instead operate on an exclusively owned private staging copy.
use super::*;

const RENAMES: &[(&str, &str)] = &[
    ("workflow_session_storage", "wc_session_migration_storage"),
    ("workflow_session_state", "wc_session_migration_state"),
    ("workflow_session_events", "wc_session_migration_events"),
    ("workflow_session_messages", "wc_session_migration_messages"),
    (
        "workflow_session_message_delivery_replays",
        "wc_session_migration_deliveries",
    ),
    (
        "workflow_session_retention_tombstones",
        "wc_session_migration_tombstones",
    ),
];

/// A transaction-scoped primitive, deliberately not called by ordinary open.
/// Any error must cause the caller to roll back the entire transaction. A
/// successful return alone does not certify COMMIT; the owner must confirm it.
pub fn migrate_fork_session_schema(tx: &rusqlite::Transaction<'_>) -> io::Result<()> {
    if detect_workflow_session_schema(tx)? != WorkflowSessionSchema::ForkV1 {
        return Err(error("migration requires the complete Fork v1 schema"));
    }
    let foreign_keys: bool = tx
        .query_row("PRAGMA foreign_keys", [], |r| r.get(0))
        .map_err(error)?;
    if !foreign_keys {
        return Err(error("migration requires foreign key enforcement"));
    }
    // Renaming or dropping old tables must not change a non-Session domain
    // through custom triggers, views or inbound foreign keys.
    let external_dependency: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type IN ('view','trigger')
           AND lower(coalesce(sql,'')) LIKE '%workflow_session%')
         OR EXISTS(SELECT 1 FROM sqlite_master s JOIN pragma_foreign_key_list(s.name) f
           WHERE s.type='table' AND s.name NOT GLOB 'workflow_session_*'
             AND s.name <> 'workflow_sessions' AND f.[table] GLOB 'workflow_session_*')",
            [],
            |r| r.get(0),
        )
        .map_err(error)?;
    if external_dependency {
        return Err(error(
            "unsupported external dependency on Fork Session schema",
        ));
    }
    let imported: i64 = tx
        .query_row(
            "SELECT legacy_imported FROM workflow_session_storage WHERE id=1",
            [],
            |r| r.get(0),
        )
        .map_err(error)?;
    if imported == 0 && tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM workflow_session_state) OR EXISTS(SELECT 1 FROM workflow_session_retention_tombstones)",
        [], |r| r.get::<_, bool>(0),
    ).map_err(error)? {
        return Err(error("Fork history exists with an unset legacy-import marker; migration cannot prove import authority"));
    }
    for &(from, to) in RENAMES {
        tx.execute_batch(&format!("ALTER TABLE {from} RENAME TO {to}"))
            .map_err(error)?;
    }
    initialize_current_schema(tx)?;
    // Per-Session bounded materialization: the previous Fork persisted only
    // bounded tails, but this code copies every surviving row, never applying
    // another count limit. The old ordinal is one-based; current is zero-based.
    let mut states = tx
        .prepare(
            "SELECT session_id,project,owner_authority_fingerprint,created_at,updated_at,
         revision,previous_message_revision,message_observation_revision,events_observed,state_json
         FROM wc_session_migration_state ORDER BY session_id",
        )
        .map_err(error)?;
    let mut rows = states.query([]).map_err(error)?;
    while let Some(row) = rows.next().map_err(error)? {
        let id: String = row.get(0).map_err(error)?;
        let project: Option<String> = row.get(1).map_err(error)?;
        let owner: String = row.get(2).map_err(error)?;
        let created: i64 = row.get(3).map_err(error)?;
        let updated: i64 = row.get(4).map_err(error)?;
        let revision = nonnegative(row.get(5).map_err(error)?)?;
        let previous = nonnegative(row.get(6).map_err(error)?)?;
        let observed_messages = nonnegative(row.get(7).map_err(error)?)?;
        let observed_events = nonnegative(row.get(8).map_err(error)?)?;
        let payload: String = row.get(9).map_err(error)?;
        let mut record: PersistedSessionRecord =
            serde_json::from_str(&payload).map_err(|_| error("invalid Fork Session state JSON"))?;
        if record.session_id != id
            || record.project != project
            || record.owner_authority_fingerprint != owner
            || record.created_at != created
            || record.updated_at != updated
            || revision == 0
            || previous != observed_messages
            || record.message_observation_revision != observed_messages
            || record.events_observed != observed_events
            || record.event_history_start != 0
            || record.current_instruction.is_some()
            || !record.events.is_empty()
            || !record.messages.is_empty()
            || !record.message_delivery_replays.is_empty()
            || !is_valid_session_id(&id)
            || !is_lower_hex_sha256(&owner)
            || !valid_persisted_project_identity(
                project.as_deref(),
                record.project_identity.as_ref(),
            )
        {
            return Err(error(
                "Fork Session indexed identity or state watermark mismatch",
            ));
        }
        read_fork_events(tx, &mut record)?;
        read_fork_messages(tx, &mut record)?;
        // Use the same strict restoration boundary as normal loading. Do not
        // migrate a corrupt record by dropping evidence or repairing authority.
        let validated = record
            .clone()
            .into_record(usize::MAX)
            .ok_or_else(|| error("Fork Session cannot be safely restored"))?;
        let mut canonical = PersistedSessionRecord::from_record(&validated, usize::MAX);
        canonical.event_history_start = record.event_history_start;
        if serde_json::to_value(&canonical).map_err(error)?
            != serde_json::to_value(&record).map_err(error)?
        {
            return Err(error(
                "Fork Session restoration would change retained evidence or metadata",
            ));
        }
        let events = record.events.clone();
        write_row(
            tx,
            &SessionWrite {
                revision,
                expected_revision: None,
                previous_message_revision: 0,
                record,
                events,
            },
        )?;
    }
    drop(rows);
    drop(states);

    let mut tombstones = tx.prepare(
        "SELECT session_id,owner_authority_fingerprint,incarnation_fingerprint,expiry_ordinal FROM wc_session_migration_tombstones"
    ).map_err(error)?;
    let mut rows = tombstones.query([]).map_err(error)?;
    let mut tombstone_ordinals = HashSet::new();
    while let Some(row) = rows.next().map_err(error)? {
        let tombstone = SessionRetentionTombstone {
            session_id: row.get(0).map_err(error)?,
            owner_authority_fingerprint: row.get(1).map_err(error)?,
            incarnation_fingerprint: row.get(2).map_err(error)?,
            expiry_ordinal: nonnegative(row.get(3).map_err(error)?)?,
        };
        if tombstone.expiry_ordinal == 0
            || !tombstone_ordinals.insert(tombstone.expiry_ordinal)
            || !is_valid_session_id(&tombstone.session_id)
            || !is_lower_hex_sha256(&tombstone.owner_authority_fingerprint)
            || !is_lower_hex_sha256(&tombstone.incarnation_fingerprint)
            || tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM workflow_sessions WHERE session_id=?1)",
                    [&tombstone.session_id],
                    |r| r.get::<_, bool>(0),
                )
                .map_err(error)?
        {
            return Err(error("invalid or overlapping Fork Session tombstone"));
        }
        tx.execute(
            "INSERT INTO workflow_session_tombstones(session_id,payload) VALUES(?1,?2)",
            params![
                tombstone.session_id,
                serde_json::to_string(&tombstone).map_err(error)?
            ],
        )
        .map_err(error)?;
    }
    drop(rows);
    drop(tombstones);
    // Compare every domain count and exact indexed identities before dropping
    // the old tables. Constraint collisions abort rather than silently merge.
    for (old, new) in [
        ("wc_session_migration_state", "workflow_sessions"),
        ("wc_session_migration_events", "workflow_session_events"),
        ("wc_session_migration_messages", "workflow_session_messages"),
        (
            "wc_session_migration_deliveries",
            "workflow_session_deliveries",
        ),
        (
            "wc_session_migration_tombstones",
            "workflow_session_tombstones",
        ),
    ] {
        let equal: bool = tx
            .query_row(
                &format!("SELECT (SELECT count(*) FROM {old})=(SELECT count(*) FROM {new})"),
                [],
                |r| r.get(0),
            )
            .map_err(error)?;
        if !equal {
            return Err(error("Fork migration row-count mismatch"));
        }
    }
    let indexed_difference: bool = tx.query_row(
        "SELECT EXISTS(SELECT session_id,project,owner_authority_fingerprint,created_at,updated_at,revision FROM wc_session_migration_state
                       EXCEPT SELECT session_id,project,owner,created_at,updated_at,revision FROM workflow_sessions)
         OR EXISTS(SELECT session_id,ordinal-1,event_id FROM wc_session_migration_events
                   EXCEPT SELECT session_id,ordinal,event_id FROM workflow_session_events)
         OR EXISTS(SELECT session_id,message_id,observation_revision FROM wc_session_migration_messages
                   EXCEPT SELECT session_id,message_id,revision FROM workflow_session_messages)
         OR EXISTS(SELECT session_id,scope_key,message_id,payload_fingerprint FROM wc_session_migration_deliveries
                   EXCEPT SELECT session_id,scope_key,message_id,fingerprint FROM workflow_session_deliveries)
         OR EXISTS(SELECT session_id,owner_authority_fingerprint,incarnation_fingerprint,expiry_ordinal FROM wc_session_migration_tombstones
                   EXCEPT SELECT session_id,json_extract(payload,'$.owner_authority_fingerprint'),
                          json_extract(payload,'$.incarnation_fingerprint'),json_extract(payload,'$.expiry_ordinal') FROM workflow_session_tombstones)",
        [], |r| r.get(0),
    ).map_err(error)?;
    if indexed_difference {
        return Err(error("Fork migration indexed identity comparison failed"));
    }
    // Reverse dependency order: delivery rows before messages, then events,
    // then state. The old tables disappear only in this atomic transaction.
    for table in [
        "wc_session_migration_deliveries",
        "wc_session_migration_messages",
        "wc_session_migration_events",
        "wc_session_migration_tombstones",
        "wc_session_migration_state",
        "wc_session_migration_storage",
    ] {
        tx.execute_batch(&format!("DROP TABLE {table}"))
            .map_err(error)?;
    }
    tx.execute(
        "UPDATE workflow_session_storage SET imported=?1 WHERE singleton=1",
        [imported],
    )
    .map_err(error)?;
    if detect_workflow_session_schema(tx)? != WorkflowSessionSchema::CurrentV1 {
        return Err(error(
            "migration did not produce the canonical Session schema",
        ));
    }
    let mut check = tx.prepare("PRAGMA foreign_key_check").map_err(error)?;
    if check
        .query([])
        .map_err(error)?
        .next()
        .map_err(error)?
        .is_some()
    {
        return Err(error("migration foreign key validation failed"));
    }
    Ok(())
}

fn nonnegative(value: i64) -> io::Result<u64> {
    u64::try_from(value).map_err(|_| error("negative Fork Session revision or ordinal"))
}

fn read_fork_events(tx: &Connection, record: &mut PersistedSessionRecord) -> io::Result<()> {
    let count: i64 = tx
        .query_row(
            "SELECT count(*) FROM wc_session_migration_events WHERE session_id=?1",
            [&record.session_id],
            |r| r.get(0),
        )
        .map_err(error)?;
    let count = nonnegative(count)?;
    record.event_history_start = record
        .events_observed
        .checked_sub(count)
        .ok_or_else(|| error("Fork event count exceeds observation watermark"))?;
    let mut stmt = tx.prepare(
        "SELECT ordinal,event_id,event_json FROM wc_session_migration_events WHERE session_id=?1 ORDER BY ordinal"
    ).map_err(error)?;
    let mut rows = stmt.query([&record.session_id]).map_err(error)?;
    while let Some(row) = rows.next().map_err(error)? {
        let ordinal = nonnegative(row.get(0).map_err(error)?)?;
        let event_id: String = row.get(1).map_err(error)?;
        let payload: String = row.get(2).map_err(error)?;
        let event: SessionEvent =
            serde_json::from_str(&payload).map_err(|_| error("invalid Fork event JSON"))?;
        let expected = record
            .event_history_start
            .checked_add(record.events.len() as u64)
            .and_then(|ordinal| ordinal.checked_add(1))
            .ok_or_else(|| error("Fork ordinal overflow"))?;
        if ordinal != expected
            || event.session_id != record.session_id
            || event.event_id != event_id
        {
            return Err(error("Fork event ordinal gap or identity mismatch"));
        }
        let event = Arc::new(event);
        if event.kind == "task_instruction" {
            record.current_instruction = Some(event.clone());
        }
        record.events.push(event);
    }
    if record.events.len() as u64 != count {
        return Err(error("Fork event count changed"));
    }
    Ok(())
}

fn read_fork_messages(tx: &Connection, record: &mut PersistedSessionRecord) -> io::Result<()> {
    let mut stmt = tx.prepare(
        "SELECT message_id,observation_revision,message_json FROM wc_session_migration_messages WHERE session_id=?1 ORDER BY rowid"
    ).map_err(error)?;
    let mut rows = stmt.query([&record.session_id]).map_err(error)?;
    let mut revisions = BTreeMap::new();
    while let Some(row) = rows.next().map_err(error)? {
        let id: String = row.get(0).map_err(error)?;
        let revision = nonnegative(row.get(1).map_err(error)?)?;
        let payload: String = row.get(2).map_err(error)?;
        let message: SessionMessage =
            serde_json::from_str(&payload).map_err(|_| error("invalid Fork message JSON"))?;
        if message.session_id != record.session_id
            || message.message_id != id
            || revision > record.message_observation_revision
            || revisions.insert(id, revision).is_some()
        {
            return Err(error("Fork message identity or revision mismatch"));
        }
        record.messages.push(Arc::new(message));
    }
    if revisions != record.message_observation_revisions {
        return Err(error("Fork message revision index does not match state"));
    }
    let mut stmt = tx.prepare(
        "SELECT scope_key,payload_fingerprint,message_id FROM wc_session_migration_deliveries WHERE session_id=?1"
    ).map_err(error)?;
    let mut rows = stmt.query([&record.session_id]).map_err(error)?;
    while let Some(row) = rows.next().map_err(error)? {
        let scope: String = row.get(0).map_err(error)?;
        let fingerprint: String = row.get(1).map_err(error)?;
        let message_id: String = row.get(2).map_err(error)?;
        if !scope
            .split_once(':')
            .is_some_and(|(scope, key)| is_lower_hex_sha256(scope) && is_lower_hex_sha256(key))
            || !is_lower_hex_sha256(&fingerprint)
            || !revisions.contains_key(&message_id)
        {
            return Err(error("Fork message replay identity mismatch"));
        }
        if record
            .message_delivery_replays
            .insert(
                scope,
                super::super::super::model::SessionMessageDeliveryReplay {
                    message_id,
                    payload_fingerprint: fingerprint,
                },
            )
            .is_some()
        {
            return Err(error("duplicate Fork message replay key"));
        }
    }
    Ok(())
}
