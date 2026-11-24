//! One-time strict legacy JSON v2 ledger import. Any malformed document/row aborts
//! the surrounding transaction without partial Session rows.
use std::collections::HashSet;
use std::fmt;
use std::fs::File;
use std::io::{self, BufReader};
use std::path::Path;

use rusqlite::TransactionBehavior;
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde_json::{self, Value};

use crate::events::is_valid_session_id;
use crate::model::{
    PersistedRetentionTombstoneRow, PersistedSessionRecord, SessionProjectIdentity,
    DEFAULT_MAX_EVENTS_PER_SESSION, SESSION_LEDGER_VERSION,
};

use super::database::{
    insert_session_snapshot, sanitize_loaded_record, upsert_retention_tombstone, SessionDatabase,
    LEGACY_IMPORTED_SESSION_REVISION,
};

use super::is_lower_hex_sha256;

pub(in crate::persistence) fn import_legacy(db: &SessionDatabase, path: &Path) -> io::Result<()> {
    let mut conn = db.lock_connection()?;
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| io::Error::other(err.to_string()))?;
    if super::database::SessionDatabase::legacy_imported_in_transaction(&tx)? {
        tx.commit().map_err(|err| io::Error::other(err.to_string()))?;
        return Ok(());
    }
    let file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            tracing::info!(
                path = %path.display(),
                "legacy session ledger missing; import deferred (legacy_imported unset, retry on next startup)"
            );
            tx.commit().map_err(|err| io::Error::other(err.to_string()))?;
            return Ok(());
        }
        Err(err) => {
            return Err(io::Error::new(
                err.kind(),
                format!("legacy session ledger open failed ({}): {err}", path.display()),
            ));
        }
    };
    let mut context = StreamingImportContext::new();
    let mut deserializer = serde_json::Deserializer::from_reader(BufReader::new(file));
    StreamingLedgerSeed {
        tx: &tx,
        context: &mut context,
    }
    .deserialize(&mut deserializer)
    .map_err(io::Error::other)?;
    deserializer.end().map_err(io::Error::other)?;
    super::database::SessionDatabase::mark_legacy_imported_in_transaction(&tx)?;
    tx.commit().map_err(|err| io::Error::other(err.to_string()))?;
    Ok(())
}

struct StreamingImportContext {
    version: Option<u32>,
    sessions_seen: bool,
    seen_sessions: HashSet<String>,
    seen_tombstone_sessions: HashSet<String>,
    seen_event_ids: HashSet<String>,
    seen_tombstone_ordinals: HashSet<u64>,
}

impl StreamingImportContext {
    fn new() -> Self {
        Self {
            version: None,
            sessions_seen: false,
            seen_sessions: HashSet::new(),
            seen_tombstone_sessions: HashSet::new(),
            seen_event_ids: HashSet::new(),
            seen_tombstone_ordinals: HashSet::new(),
        }
    }
}

struct StreamingLedgerSeed<'a, 'conn> {
    tx: &'a rusqlite::Transaction<'conn>,
    context: &'a mut StreamingImportContext,
}

impl<'de, 'a, 'conn> DeserializeSeed<'de> for StreamingLedgerSeed<'a, 'conn> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_map(self)
    }
}

impl<'de, 'a, 'conn> Visitor<'de> for StreamingLedgerSeed<'a, 'conn> {
    type Value = ();
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("the v2 Session ledger")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "version" => {
                    if self.context.version.is_some() {
                        return Err(de::Error::duplicate_field("version"));
                    }
                    let value = map.next_value::<u32>()?;
                    if value != SESSION_LEDGER_VERSION {
                        return Err(de::Error::custom(format!(
                            "unsupported session ledger version {value}"
                        )));
                    }
                    self.context.version = Some(value);
                }
                "sessions" => {
                    if self.context.sessions_seen {
                        return Err(de::Error::duplicate_field("sessions"));
                    }
                    self.context.sessions_seen = true;
                    map.next_value_seed(StreamingRowsSeed {
                        tx: self.tx,
                        context: self.context,
                    })?;
                }
                _ => return Err(de::Error::custom("unknown Session ledger field")),
            }
        }
        if self.context.version.is_none() {
            return Err(de::Error::missing_field("version"));
        }
        if !self.context.sessions_seen {
            return Err(de::Error::missing_field("sessions"));
        }
        Ok(())
    }
}

struct StreamingRowsSeed<'a, 'conn> {
    tx: &'a rusqlite::Transaction<'conn>,
    context: &'a mut StreamingImportContext,
}

impl<'de, 'a, 'conn> DeserializeSeed<'de> for StreamingRowsSeed<'a, 'conn> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_seq(self)
    }
}

impl<'de, 'a, 'conn> Visitor<'de> for StreamingRowsSeed<'a, 'conn> {
    type Value = ();
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a Session row array")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut rows: A) -> Result<(), A::Error> {
        while let Some(value) = rows.next_element::<Value>()? {
            if value.get("retention_tombstone").is_some() {
                import_tombstone_row(self.tx, self.context, value).map_err(de::Error::custom)?;
                continue;
            }
            import_session_row(self.tx, self.context, value).map_err(de::Error::custom)?;
        }
        Ok(())
    }
}

fn import_tombstone_row(
    tx: &rusqlite::Transaction<'_>,
    context: &mut StreamingImportContext,
    value: Value,
) -> Result<(), String> {
    let row = serde_json::from_value::<PersistedRetentionTombstoneRow>(value)
        .map_err(|err| err.to_string())?;
    let tombstone = row.retention_tombstone;
    if !is_valid_session_id(&tombstone.session_id)
        || !is_lower_hex_sha256(&tombstone.owner_authority_fingerprint)
        || !is_lower_hex_sha256(&tombstone.incarnation_fingerprint)
    {
        return Err("invalid retention tombstone identity evidence".to_string());
    }
    if !context.seen_tombstone_ordinals.insert(tombstone.expiry_ordinal) {
        return Err(format!(
            "duplicate retention tombstone ordinal {}",
            tombstone.expiry_ordinal
        ));
    }
    if context.seen_sessions.contains(&tombstone.session_id) {
        return Err(format!(
            "retention tombstone collides with Session row for {}",
            tombstone.session_id
        ));
    }
    if !context
        .seen_tombstone_sessions
        .insert(tombstone.session_id.clone())
    {
        return Err(format!(
            "duplicate retention tombstone Session id {}",
            tombstone.session_id
        ));
    }
    upsert_retention_tombstone(tx, &tombstone).map_err(|err| err.to_string())
}

fn import_session_row(
    tx: &rusqlite::Transaction<'_>,
    context: &mut StreamingImportContext,
    value: Value,
) -> Result<(), String> {
    validate_session_row_shape(&value).map_err(|err| err.to_string())?;
    let record = serde_json::from_value::<PersistedSessionRecord>(value)
        .map_err(|err| err.to_string())?;
    if !is_valid_session_id(&record.session_id)
        || !is_lower_hex_sha256(&record.owner_authority_fingerprint)
    {
        return Err("Session row has invalid canonical identity".to_string());
    }
    if record.project_identity.is_some() && record.project.is_none() {
        return Err(
            "Session row has project_identity without canonical project metadata".to_string(),
        );
    }
    if let Some(identity) = &record.project_identity {
        super::database::validate_project_identity(identity)
            .map_err(|err| err.to_string())?;
    }
    if context.seen_tombstone_sessions.contains(&record.session_id) {
        return Err(format!(
            "Session row collides with retention tombstone for {}",
            record.session_id
        ));
    }
    if !context.seen_sessions.insert(record.session_id.clone()) {
        return Err(format!(
            "duplicate Session id in legacy ledger: {}",
            record.session_id
        ));
    }
    let mut message_ids = HashSet::new();
    for message in &record.messages {
        if message.session_id != record.session_id {
            return Err(format!(
                "message {} session_id does not match owning Session {}",
                message.message_id, record.session_id
            ));
        }
        if !message_ids.insert(message.message_id.clone()) {
            return Err(format!(
                "duplicate message id in legacy Session {}: {}",
                record.session_id, message.message_id
            ));
        }
    }
    for (scope_key, replay) in &record.message_delivery_replays {
        if !is_valid_delivery_replay_scope(scope_key) {
            return Err(format!(
                "delivery replay scope {scope_key} is malformed in legacy Session {}",
                record.session_id
            ));
        }
        if !is_lower_hex_sha256(&replay.payload_fingerprint) {
            return Err(format!(
                "delivery replay scope {scope_key} has malformed fingerprint in legacy Session {}",
                record.session_id
            ));
        }
        if !message_ids.contains(&replay.message_id) {
            return Err(format!(
                "delivery replay scope {scope_key} references missing message {} in legacy Session {}",
                replay.message_id, record.session_id
            ));
        }
    }
    for (message_id, revision) in &record.message_observation_revisions {
        if !message_ids.contains(message_id) {
            return Err(format!(
                "message observation revision references missing message {message_id} in legacy Session {}",
                record.session_id
            ));
        }
        if *revision > record.message_observation_revision {
            return Err(format!(
                "message {message_id} observation revision exceeds Session watermark in legacy Session {}",
                record.session_id
            ));
        }
    }
    for event in &record.events {
        if !context.seen_event_ids.insert(event.event_id.clone()) {
            return Err(format!(
                "duplicate event id in legacy ledger: {}",
                event.event_id
            ));
        }
        if event.session_id != record.session_id {
            return Err("legacy event session_id does not match owning Session row".to_string());
        }
    }
    let sanitized = sanitize_loaded_record(record, DEFAULT_MAX_EVENTS_PER_SESSION)
        .map_err(|err| err.to_string())?;
    insert_session_snapshot(
        tx,
        &sanitized,
        sanitized.events.clone(),
        LEGACY_IMPORTED_SESSION_REVISION,
    )
    .map_err(|err| err.to_string())
}

fn is_valid_delivery_replay_scope(scope_key: &str) -> bool {
    let Some((scope, key)) = scope_key.split_once(':') else {
        return false;
    };
    is_lower_hex_sha256(scope) && is_lower_hex_sha256(key)
}

fn validate_session_row_shape(value: &Value) -> Result<(), String> {
    if !v2_record_has_canonical_logical_invocation_shape(value) {
        return Err(
            "Session row event correlation shape is partial or malformed".to_string(),
        );
    }
    Ok(())
}

fn v2_record_has_canonical_logical_invocation_shape(value: &Value) -> bool {
    value
        .get("events")
        .and_then(Value::as_array)
        .is_some_and(|events| {
            events.iter().all(|event| {
                event.as_object().is_some_and(|object| {
                    match (
                        object.get("logical_invocation_id"),
                        object.get("logical_invocation_role"),
                    ) {
                        (None, None) => true,
                        (Some(Value::String(id)), Some(Value::String(role))) => {
                            crate::events::is_valid_logical_invocation_id(id)
                                && crate::events::is_valid_logical_invocation_role(role)
                        }
                        _ => false,
                    }
                })
            })
        })
}
