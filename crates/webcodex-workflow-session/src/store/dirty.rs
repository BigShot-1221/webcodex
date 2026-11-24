//! Per-session SQLite dirty state for Phase 2. Stage E production persistence uses
//! `BatchPlan` + `apply_batch`; JSON ledger constructors remain for tests and compatibility.
//!
//! SQLite seeded mode (`DatabasePersistenceQueue::seeded`): every business mutation must
//! pass revision admission via [`SessionStoreInner::prepare_active_session`] or
//! [`SessionStoreInner::try_insert_session`] (`ensure_new_session_revision_capacity`)
//! before RAM changes. JSON/in-memory stores are
//! not subject to this gate (`seeded=false`).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::bound_summary_string;
use super::SessionStoreInner;
use crate::model::{
    PersistedSessionRecord, SessionEvent, SessionLifecycle, SessionRetentionTombstone,
    StoredSession,
};
use crate::persistence::materialize_cold_session;
use crate::persistence::database::{
    SessionDatabaseRetirement, SessionDatabaseTombstoneDelete,
};

/// Maximum durable `workflow_session_state.revision` representable in SQLite INTEGER.
pub(crate) const SQLITE_REVISION_MAX: u64 = i64::MAX as u64;

/// Hard cap on durable retirement rows awaiting SQLite acknowledgement (Stage J.7).
/// Sized to ~2.5× default historical closed-session retention so a startup burst can
/// queue without unbounded growth while still bounding churn faster than the writer.
pub(crate) const MAX_PENDING_RETIREMENTS: usize = 256;

/// Hard cap on durable tombstone delete intents awaiting acknowledgement (Stage J.7).
pub(crate) const MAX_PENDING_TOMBSTONE_DELETES: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SeedDatabasePersistenceError {
    UnknownCommittedSession { session_id: String },
    UnknownCommittedMessageSession { session_id: String },
    MissingCommittedRevision { session_id: String },
    MissingCommittedMessageRevision { session_id: String },
    CommittedMessageRevisionMismatch {
        session_id: String,
        committed: u64,
        observed: u64,
    },
    SessionRecordMaterializationFailed { session_id: String },
    DurableRevisionHighWaterOutOfRange,
    DurableRevisionHighWaterTooLow { observed: u64, required: u64 },
}

/// Retained-event SQLite delta tagged with the session dirty revision that produced it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingEventUpsert {
    pub ordinal: u64,
    pub event: Arc<SessionEvent>,
    pub dirty_revision: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct SessionDirtyState {
    pub committed_revision: Option<u64>,
    pub committed_message_revision: u64,
    pub dirty_revision: Option<u64>,
    /// Revision captured in the last `BatchPlan` and currently being written by Stage D.
    /// Distinct from `dirty_revision`: eviction may clear dirty payload while a write
    /// remains in flight; retirement ack must not retain dirty rows without this marker.
    pub in_flight_revision: Option<u64>,
    /// Last durably acknowledged `events_observed` snapshot (session row watermark).
    pub committed_event_ordinal: u64,
    pub pending_event_upserts: Vec<PendingEventUpsert>,
}

impl Default for SessionDirtyState {
    fn default() -> Self {
        Self {
            committed_revision: None,
            committed_message_revision: 0,
            dirty_revision: None,
            in_flight_revision: None,
            committed_event_ordinal: 0,
            pending_event_upserts: Vec::new(),
        }
    }
}

impl SessionDirtyState {
    pub(crate) fn prune_pending_event_upserts(&mut self, retained_event_start: u64) {
        self.pending_event_upserts
            .retain(|upsert| upsert.ordinal >= retained_event_start);
    }

    pub(crate) fn register_pending_event_upsert(
        &mut self,
        ordinal: u64,
        event: Arc<SessionEvent>,
        dirty_revision: u64,
        retained_event_start: u64,
    ) {
        self.prune_pending_event_upserts(retained_event_start);
        if let Some(existing) = self
            .pending_event_upserts
            .iter_mut()
            .find(|upsert| upsert.ordinal == ordinal)
        {
            existing.event = event;
            existing.dirty_revision = dirty_revision;
            return;
        }
        self.pending_event_upserts.push(PendingEventUpsert {
            ordinal,
            event,
            dirty_revision,
        });
    }
}

#[derive(Debug, Default)]
pub(crate) struct DatabasePersistenceQueue {
    pub(super) session_dirty: HashMap<String, SessionDirtyState>,
    /// Last durable revision observed at restore (or zero). The next allocation uses
    /// `checked_add(1)` and is always strictly greater than this value.
    ///
    /// When inventory is empty after restore, `durable_revision_high_water` is zero and
    /// the first live insert uses revision `1` with `expected_revision=None`. That is
    /// safe: session ids remain unique and per-session CAS still separates concurrent
    /// writers; empty inventory does not imply revision reuse across distinct sessions.
    next_revision: u64,
    /// Durable high water reached `SQLITE_REVISION_MAX`; no further revisions can be allocated.
    revision_exhausted: bool,
    /// Sessions whose latest revision allocation attempt failed (per-session; unrelated writes proceed).
    revision_allocation_failed_sessions: HashSet<String>,
    seeded: bool,
    /// Tombstone rows known durable at seed or after a successful upsert acknowledgement.
    durable_tombstone_keys: HashSet<TombstoneDurableKey>,
    pub(crate) pending_retirements: Vec<SessionDatabaseRetirement>,
    pub(crate) pending_tombstone_upserts: Vec<SessionRetentionTombstone>,
    pub(crate) pending_tombstone_deletes: Vec<SessionDatabaseTombstoneDelete>,
    /// Reconcile-delete slots reserved for tombstone upserts/retirements in flight (J.7).
    tombstone_delete_flush_reserved: usize,
    /// Rotated lexicographic pick cursor for bounded dirty-session planning (J.7.3).
    /// Advanced only after a successful durable session-write ack.
    dirty_session_pick_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TombstoneDurableKey {
    session_id: String,
    owner_authority_fingerprint: String,
    incarnation_fingerprint: String,
}

impl From<&SessionRetentionTombstone> for TombstoneDurableKey {
    fn from(tombstone: &SessionRetentionTombstone) -> Self {
        Self {
            session_id: tombstone.session_id.clone(),
            owner_authority_fingerprint: tombstone.owner_authority_fingerprint.clone(),
            incarnation_fingerprint: tombstone.incarnation_fingerprint.clone(),
        }
    }
}

impl SessionStoreInner {
    pub(crate) fn seed_database_persistence_state(
        &mut self,
        committed_revisions: &HashMap<String, u64>,
        committed_message_revisions: &HashMap<String, u64>,
        durable_revision_high_water: u64,
    ) -> Result<(), SeedDatabasePersistenceError> {
        if durable_revision_high_water > SQLITE_REVISION_MAX {
            return Err(SeedDatabasePersistenceError::DurableRevisionHighWaterOutOfRange);
        }

        let live_ids: HashSet<_> = self.sessions.keys().cloned().collect();
        for session_id in &live_ids {
            let Some(&revision) = committed_revisions.get(session_id) else {
                return Err(SeedDatabasePersistenceError::MissingCommittedRevision {
                    session_id: session_id.clone(),
                });
            };
            let Some(&message_revision) = committed_message_revisions.get(session_id) else {
                return Err(SeedDatabasePersistenceError::MissingCommittedMessageRevision {
                    session_id: session_id.clone(),
                });
            };
            if revision > durable_revision_high_water {
                return Err(SeedDatabasePersistenceError::DurableRevisionHighWaterTooLow {
                    observed: durable_revision_high_water,
                    required: revision,
                });
            }
            if revision > SQLITE_REVISION_MAX {
                return Err(SeedDatabasePersistenceError::DurableRevisionHighWaterOutOfRange);
            }
            let stored = self
                .sessions
                .get(session_id)
                .expect("live id must resolve");
            let Some(record) =
                persisted_record_for_session(stored, self.max_events_per_session)
            else {
                return Err(SeedDatabasePersistenceError::SessionRecordMaterializationFailed {
                    session_id: session_id.clone(),
                });
            };
            if record.message_observation_revision != message_revision {
                return Err(SeedDatabasePersistenceError::CommittedMessageRevisionMismatch {
                    session_id: session_id.clone(),
                    committed: message_revision,
                    observed: record.message_observation_revision,
                });
            }
            self.database_persistence.session_dirty.insert(
                session_id.clone(),
                SessionDirtyState {
                    committed_revision: Some(revision),
                    committed_message_revision: message_revision,
                    dirty_revision: None,
                    in_flight_revision: None,
                    committed_event_ordinal: record.events_observed,
                    pending_event_upserts: Vec::new(),
                },
            );
        }
        for session_id in committed_revisions.keys() {
            if !live_ids.contains(session_id) {
                return Err(SeedDatabasePersistenceError::UnknownCommittedSession {
                    session_id: session_id.clone(),
                });
            }
        }
        for session_id in committed_message_revisions.keys() {
            if !live_ids.contains(session_id) {
                return Err(SeedDatabasePersistenceError::UnknownCommittedMessageSession {
                    session_id: session_id.clone(),
                });
            }
        }
        self.database_persistence.next_revision = durable_revision_high_water;
        self.database_persistence.revision_exhausted = durable_revision_high_water >= SQLITE_REVISION_MAX;
        self.database_persistence.seeded = true;
        self.database_persistence.durable_tombstone_keys = self
            .tombstones
            .values()
            .map(TombstoneDurableKey::from)
            .collect();
        self.database_persistence
            .pending_tombstone_upserts
            .clear();
        self.database_persistence
            .pending_tombstone_deletes
            .clear();
        self.database_persistence.pending_retirements.clear();
        Ok(())
    }

    pub(super) fn mark_session_dirty(&mut self, session_id: &str) {
        if !self.database_persistence.seeded {
            return;
        }
        let floor = self.session_dirty_floor(session_id);
        let _ = self
            .database_persistence
            .ensure_session_dirty(session_id, floor);
    }

    /// Allocate one canonical dirty revision (if needed) and register a retained-event upsert.
    ///
    /// Returns `true` when revision admission succeeded or SQLite seeding is inactive.
    pub(super) fn mark_session_event_upsert(
        &mut self,
        session_id: &str,
        ordinal: u64,
        event: Arc<SessionEvent>,
        retained_event_start: u64,
    ) -> bool {
        if !self.database_persistence.seeded {
            return true;
        }
        let floor = self.session_dirty_floor(session_id);
        let Some(revision) = self
            .database_persistence
            .ensure_session_dirty(session_id, floor)
        else {
            return false;
        };
        let state = self
            .database_persistence
            .session_dirty
            .get_mut(session_id)
            .expect("dirty state exists after ensure");
        state.register_pending_event_upsert(ordinal, event, revision, retained_event_start);
        debug_assert_eq!(Some(revision), state.dirty_revision);
        true
    }

    /// Session-row delta without a retained tail event (e.g. `max_events = 0` after append).
    ///
    /// Returns `true` when revision admission succeeded or SQLite seeding is inactive.
    /// Pending event upserts are pruned only after a successful dirty revision allocation.
    pub(super) fn mark_session_state_dirty(
        &mut self,
        session_id: &str,
        retained_event_start: u64,
    ) -> bool {
        if !self.database_persistence.seeded {
            return true;
        }
        let floor = self.session_dirty_floor(session_id);
        let Some(_revision) = self
            .database_persistence
            .ensure_session_dirty(session_id, floor)
        else {
            return false;
        };
        if let Some(state) = self.database_persistence.session_dirty.get_mut(session_id) {
            state.prune_pending_event_upserts(retained_event_start);
        }
        true
    }

    /// Returns `true` when the first dirty revision was registered (or SQLite seeding is inactive).
    pub(super) fn mark_session_created_dirty(&mut self, session_id: &str) -> bool {
        if !self.database_persistence.seeded {
            return true;
        }
        let (pending_events, retained_event_start) = {
            let Some(record) = self.sessions.get(session_id).and_then(StoredSession::hot) else {
                return false;
            };
            let retained_event_start =
                retained_event_start(record.events_observed, record.events.len());
            let len = record.events.len();
            let pending_events = if len == 0 {
                Vec::new()
            } else {
                let base = record
                    .events_observed
                    .saturating_sub(len as u64)
                    .saturating_add(1);
                record
                    .events
                    .iter()
                    .enumerate()
                    .map(|(index, event)| {
                        (
                            base.saturating_add(index as u64),
                            Arc::clone(event),
                        )
                    })
                    .collect()
            };
            (pending_events, retained_event_start)
        };
        let floor = self.session_dirty_floor(session_id);
        let Some(revision) = self
            .database_persistence
            .ensure_session_dirty(session_id, floor)
        else {
            return false;
        };
        let state = self
            .database_persistence
            .session_dirty
            .get_mut(session_id)
            .expect("dirty state exists after ensure");
        if pending_events.is_empty() {
            state.prune_pending_event_upserts(retained_event_start);
            return true;
        }
        for (ordinal, event) in pending_events {
            state.register_pending_event_upsert(ordinal, event, revision, retained_event_start);
        }
        true
    }

    pub(super) fn queue_retention_tombstone_upsert(&mut self, tombstone: SessionRetentionTombstone) {
        if !self.database_persistence.seeded {
            return;
        }
        self.database_persistence
            .cancel_pending_tombstone_delete(&tombstone);
        self.database_persistence
            .upsert_pending_tombstone_by_session_id(tombstone);
    }

    pub(super) fn queue_tombstone_bound_delete(&mut self, tombstone: &SessionRetentionTombstone) -> bool {
        if !self.database_persistence.seeded {
            return true;
        }
        let key = TombstoneDurableKey::from(tombstone);
        let delete = SessionDatabaseTombstoneDelete {
            session_id: tombstone.session_id.clone(),
            owner_authority_fingerprint: tombstone.owner_authority_fingerprint.clone(),
            incarnation_fingerprint: tombstone.incarnation_fingerprint.clone(),
        };
        if !self
            .database_persistence
            .preflight_tombstone_bound_delete(tombstone, &delete)
        {
            return false;
        }
        self.database_persistence
            .clear_matching_retention_tombstone_from_retirements(tombstone);
        self.database_persistence
            .cancel_pending_tombstone_upsert(tombstone);
        if self
            .database_persistence
            .durable_tombstone_keys
            .contains(&key)
        {
            if !self
                .database_persistence
                .try_push_pending_tombstone_delete(delete)
            {
                return false;
            }
        }
        true
    }

    pub(super) fn queue_session_retirement(&mut self, retirement: SessionDatabaseRetirement) -> bool {
        if !self.database_persistence.seeded {
            return true;
        }
        self.database_persistence.try_push_pending_retirement(retirement)
    }

    pub(super) fn queue_all_tombstones_dirty(&mut self) {
        if !self.database_persistence.seeded {
            return;
        }
        for tombstone in self.tombstones.values() {
            self.database_persistence
                .sync_pending_retirement_tombstone(tombstone);
            self.database_persistence
                .upsert_pending_tombstone_by_session_id(tombstone.clone());
        }
    }

    fn session_dirty_floor(&self, session_id: &str) -> Option<u64> {
        self.database_persistence
            .session_dirty
            .get(session_id)
            .and_then(|state| state.committed_revision)
    }

    /// Surfaces revision admission failure through durable store status without requiring a
    /// writer generation. Does not overwrite a more specific `last_persist_error`.
    pub(super) fn note_retention_queue_admission_failure(&mut self, detail: &str) {
        let Some(persistence) = self.persistence.as_mut() else {
            return;
        };
        if persistence.last_persist_error.is_some() {
            return;
        }
        let summary = format!("sqlite_retention_queue_saturated: {detail}");
        persistence.last_persist_error = Some(bound_summary_string(&summary));
    }

    pub(super) fn note_revision_admission_failure(&mut self, session_id: &str) {
        let Some(persistence) = self.persistence.as_mut() else {
            return;
        };
        if persistence.last_persist_error.is_some() {
            return;
        }
        let summary = if self.database_persistence.is_revision_exhausted() {
            "sqlite_revision_exhausted".to_string()
        } else {
            format!("sqlite_revision_allocation_failed: sessions={session_id}")
        };
        persistence.last_persist_error = Some(bound_summary_string(&summary));
    }
}

impl DatabasePersistenceQueue {
    /// Preflight revision admission for one canonical business mutation. Does not allocate
    /// a dirty revision or touch per-session dirty/in-flight/upsert/watermark state.
    pub(crate) fn ensure_revision_capacity(&mut self, session_id: &str) -> bool {
        if !self.seeded {
            return true;
        }
        if self.revision_allocation_failed_sessions.contains(session_id) {
            return false;
        }
        if self.revision_exhausted {
            self.revision_allocation_failed_sessions
                .insert(session_id.to_string());
            return false;
        }
        let effective_next = self.effective_next_revision_for_session(session_id);
        if effective_next >= SQLITE_REVISION_MAX {
            self.revision_exhausted = true;
            self.revision_allocation_failed_sessions
                .insert(session_id.to_string());
            return false;
        }
        match effective_next.checked_add(1) {
            Some(revision) if revision <= SQLITE_REVISION_MAX => true,
            _ => {
                self.revision_exhausted = true;
                self.revision_allocation_failed_sessions
                    .insert(session_id.to_string());
                false
            }
        }
    }

    /// Preflight revision admission for a session id that is not yet present in RAM or
    /// `session_dirty`. Exhaustion sets the global `revision_exhausted` flag only; it does not
    /// grow `revision_allocation_failed_sessions` for ids that were never admitted.
    pub(crate) fn ensure_new_session_revision_capacity(&mut self, session_id: &str) -> bool {
        if !self.seeded {
            return true;
        }
        if self.revision_allocation_failed_sessions.contains(session_id) {
            return false;
        }
        if self.revision_exhausted {
            return false;
        }
        let effective_next = self.effective_next_revision_for_session(session_id);
        if effective_next >= SQLITE_REVISION_MAX {
            self.revision_exhausted = true;
            return false;
        }
        match effective_next.checked_add(1) {
            Some(revision) if revision <= SQLITE_REVISION_MAX => true,
            _ => {
                self.revision_exhausted = true;
                false
            }
        }
    }

    fn effective_next_revision_for_session(&self, session_id: &str) -> u64 {
        let mut next = self.next_revision;
        if let Some(floor) = self
            .session_dirty
            .get(session_id)
            .and_then(|state| state.committed_revision)
        {
            if next < floor {
                next = floor;
            }
        }
        next
    }

    pub(crate) fn is_seeded(&self) -> bool {
        self.seeded
    }

    pub(crate) fn is_revision_exhausted(&self) -> bool {
        self.revision_exhausted
    }

    pub(crate) fn has_revision_allocation_failed(&self) -> bool {
        !self.revision_allocation_failed_sessions.is_empty()
    }

    pub(crate) fn revision_allocation_failed_session_ids(&self) -> impl Iterator<Item = &str> {
        self.revision_allocation_failed_sessions
            .iter()
            .map(String::as_str)
    }

    pub(crate) fn is_session_revision_allocation_failed(&self, session_id: &str) -> bool {
        self.revision_allocation_failed_sessions.contains(session_id)
    }

    pub(crate) fn clear_revision_allocation_failure(&mut self, session_id: &str) {
        self.revision_allocation_failed_sessions.remove(session_id);
    }

    pub(crate) fn remove_durable_tombstone(&mut self, delete: &SessionDatabaseTombstoneDelete) {
        self.durable_tombstone_keys.retain(|durable| {
            durable.session_id != delete.session_id
                || durable.owner_authority_fingerprint != delete.owner_authority_fingerprint
                || durable.incarnation_fingerprint != delete.incarnation_fingerprint
        });
    }

    fn ensure_session_dirty(
        &mut self,
        session_id: &str,
        committed_floor: Option<u64>,
    ) -> Option<u64> {
        if !self.seeded {
            return None;
        }
        if self.revision_exhausted {
            self.revision_allocation_failed_sessions
                .insert(session_id.to_string());
            return None;
        }
        self.session_dirty
            .entry(session_id.to_string())
            .or_insert_with(SessionDirtyState::default);
        let Some(revision) = self.allocate_revision(committed_floor) else {
            self.revision_allocation_failed_sessions
                .insert(session_id.to_string());
            return None;
        };
        self.revision_allocation_failed_sessions.remove(session_id);
        let entry = self
            .session_dirty
            .get_mut(session_id)
            .expect("entry exists after or_insert");
        // Fresh Store-monotonic revision per canonical mutation; in-flight markers,
        // committed watermarks, and pending event deltas are untouched on success.
        entry.dirty_revision = Some(revision);
        Some(revision)
    }

    fn allocate_revision(&mut self, floor: Option<u64>) -> Option<u64> {
        if self.revision_exhausted {
            return None;
        }
        if let Some(floor) = floor {
            if self.next_revision < floor {
                self.next_revision = floor;
            }
        }
        if self.next_revision >= SQLITE_REVISION_MAX {
            self.revision_exhausted = true;
            return None;
        }
        let revision = self.next_revision.checked_add(1)?;
        if revision > SQLITE_REVISION_MAX {
            self.revision_exhausted = true;
            return None;
        }
        self.next_revision = revision;
        Some(revision)
    }

    pub(crate) fn dirty_session_ids(&self) -> impl Iterator<Item = &str> {
        self.session_dirty
            .iter()
            .filter_map(|(id, state)| state.dirty_revision.map(|_| id.as_str()))
    }

    pub(crate) fn session_state(&self, session_id: &str) -> Option<&SessionDirtyState> {
        self.session_dirty.get(session_id)
    }

    pub(crate) fn has_pending_work(&self) -> bool {
        if !self.seeded {
            return false;
        }
        !self.revision_allocation_failed_sessions.is_empty()
            || self.dirty_session_ids().next().is_some()
            || self
                .session_dirty
                .values()
                .any(|state| state.in_flight_revision.is_some())
            || !self.pending_retirements.is_empty()
            || !self.pending_tombstone_upserts.is_empty()
            || !self.pending_tombstone_deletes.is_empty()
    }

    pub(crate) fn retirement_queue_effective_len(&self) -> usize {
        self.pending_retirements.len()
    }

    pub(crate) fn tombstone_delete_queue_effective_len(&self) -> usize {
        self.pending_tombstone_deletes.len() + self.tombstone_delete_flush_reserved
    }

    /// Reconcile-delete headroom before `mark_planned_writes_in_flight` reserves slots for
    /// standalone tombstone upserts and retirements with embedded `retention_tombstone`.
    ///
    /// Invariant: at plan capture time,
    /// `pending_tombstone_deletes.len() + tombstone_delete_flush_reserved + new_reservations`
    /// must not exceed `MAX_PENDING_TOMBSTONE_DELETES`. Planned tombstone deletes do not
    /// reduce this budget until SQL commit + ack removes them from `pending_tombstone_deletes`.
    pub(crate) fn available_tombstone_delete_reconcile_reservation_slots(&self) -> usize {
        MAX_PENDING_TOMBSTONE_DELETES.saturating_sub(self.tombstone_delete_queue_effective_len())
    }

    pub(crate) fn retention_queues_prevent_new_retirement_enqueue(&self) -> bool {
        self.retirement_queue_effective_len() >= MAX_PENDING_RETIREMENTS
    }

    pub(crate) fn retention_queues_prevent_new_tombstone_delete_enqueue(&self) -> bool {
        self.tombstone_delete_queue_effective_len() >= MAX_PENDING_TOMBSTONE_DELETES
    }

    /// Fail-closed admission for new business mutations when retention delete/retirement
    /// queues are at their hard caps (includes in-flight reconcile delete reservations).
    pub(crate) fn retention_queues_prevent_mutation_admission(&self) -> bool {
        self.retention_queues_prevent_new_retirement_enqueue()
            || self.retention_queues_prevent_new_tombstone_delete_enqueue()
    }

    pub(crate) fn try_push_pending_retirement(
        &mut self,
        retirement: SessionDatabaseRetirement,
    ) -> bool {
        if self.retention_queues_prevent_new_retirement_enqueue() {
            return false;
        }
        self.pending_retirements.push(retirement);
        true
    }

    pub(crate) fn try_push_pending_tombstone_delete(
        &mut self,
        delete: SessionDatabaseTombstoneDelete,
    ) -> bool {
        let duplicate = self.pending_tombstone_deletes.iter().any(|pending| {
            pending.session_id == delete.session_id
                && pending.owner_authority_fingerprint == delete.owner_authority_fingerprint
                && pending.incarnation_fingerprint == delete.incarnation_fingerprint
        });
        if duplicate {
            return true;
        }
        if self.retention_queues_prevent_new_tombstone_delete_enqueue() {
            return false;
        }
        self.pending_tombstone_deletes.push(delete);
        true
    }

    /// Capacity/duplicate preflight before mutating retirements or upserts for bound delete.
    fn preflight_tombstone_bound_delete(
        &self,
        tombstone: &SessionRetentionTombstone,
        delete: &SessionDatabaseTombstoneDelete,
    ) -> bool {
        let key = TombstoneDurableKey::from(tombstone);
        if !self.durable_tombstone_keys.contains(&key) {
            return true;
        }
        if self.pending_tombstone_deletes.iter().any(|pending| {
            pending.session_id == delete.session_id
                && pending.owner_authority_fingerprint == delete.owner_authority_fingerprint
                && pending.incarnation_fingerprint == delete.incarnation_fingerprint
        }) {
            return true;
        }
        !self.retention_queues_prevent_new_tombstone_delete_enqueue()
    }

    /// Stable lexicographic order for bounded session-write selection with O(`limit`) memory.
    /// Uses `dirty_session_pick_cursor` for fair rotation across high session ids.
    pub(crate) fn stable_dirty_session_ids_up_to(&self, limit: usize) -> Vec<String> {
        use std::collections::BinaryHeap;

        if limit == 0 {
            return Vec::new();
        }

        /// Max-heap of the `budget` lexicographically smallest ids seen so far; peek is the
        /// largest among that set (the eviction candidate when a smaller id arrives).
        fn push_candidate<'a>(
            heap: &mut BinaryHeap<&'a str>,
            budget: usize,
            candidate: &'a str,
        ) {
            if budget == 0 {
                return;
            }
            if heap.len() < budget {
                heap.push(candidate);
                return;
            }
            if let Some(worst) = heap.peek() {
                if candidate < *worst {
                    heap.pop();
                    heap.push(candidate);
                }
            }
        }

        fn drain_heap(heap: BinaryHeap<&str>) -> Vec<String> {
            let mut ids: Vec<String> = heap.into_iter().map(|id| id.to_string()).collect();
            ids.sort();
            ids
        }

        let dirty_iter = || {
            self.session_dirty.iter().filter_map(|(id, state)| {
                state.dirty_revision.map(|_| id.as_str())
            })
        };

        let mut heap: BinaryHeap<&str> = BinaryHeap::new();

        match self.dirty_session_pick_cursor.as_deref() {
            None => {
                for id in dirty_iter() {
                    push_candidate(&mut heap, limit, id);
                }
                drain_heap(heap)
            }
            Some(cursor) => {
                for id in dirty_iter().filter(|id| *id > cursor) {
                    push_candidate(&mut heap, limit, id);
                }
                let mut picked = drain_heap(heap);
                if picked.len() < limit {
                    let remaining = limit.saturating_sub(picked.len());
                    let mut wrap_heap: BinaryHeap<&str> = BinaryHeap::new();
                    for id in self.session_dirty.iter().filter_map(|(id, state)| {
                        if state.dirty_revision.is_some() && id.as_str() <= cursor {
                            Some(id.as_str())
                        } else {
                            None
                        }
                    }) {
                        push_candidate(&mut wrap_heap, remaining, id);
                    }
                    picked.extend(drain_heap(wrap_heap));
                    picked.sort();
                }
                picked
            }
        }
    }

    /// Advance rotation cursor after a successful chunk ack (abandon/failure leaves cursor).
    pub(crate) fn advance_dirty_session_pick_cursor_after_ack(
        &mut self,
        acked_session_ids: impl IntoIterator<Item = impl AsRef<str>>,
    ) {
        let mut max_id: Option<String> = None;
        for id in acked_session_ids {
            let id = id.as_ref();
            max_id = Some(match max_id {
                None => id.to_string(),
                Some(existing) if id > existing.as_str() => id.to_string(),
                Some(existing) => existing,
            });
        }
        if let Some(id) = max_id {
            self.dirty_session_pick_cursor = Some(id);
        }
    }

    pub(crate) fn reserve_tombstone_delete_flush_slot(&mut self) -> bool {
        if self.tombstone_delete_queue_effective_len() >= MAX_PENDING_TOMBSTONE_DELETES {
            return false;
        }
        self.tombstone_delete_flush_reserved = self.tombstone_delete_flush_reserved.saturating_add(1);
        true
    }

    pub(crate) fn release_tombstone_delete_flush_reservation(&mut self, count: usize) {
        self.tombstone_delete_flush_reserved =
            self.tombstone_delete_flush_reserved.saturating_sub(count);
    }

    pub(crate) fn mark_write_in_flight(&mut self, session_id: &str, revision: u64) {
        if let Some(state) = self.session_dirty.get_mut(session_id) {
            if state.dirty_revision == Some(revision) {
                state.in_flight_revision = Some(revision);
            }
        }
    }

    pub(crate) fn clear_write_in_flight(&mut self, session_id: &str, revision: u64) {
        if let Some(state) = self.session_dirty.get_mut(session_id) {
            if state.in_flight_revision == Some(revision) {
                state.in_flight_revision = None;
            }
        }
    }

    pub(crate) fn record_tombstone_committed(&mut self, tombstone: &SessionRetentionTombstone) {
        self.durable_tombstone_keys
            .insert(TombstoneDurableKey::from(tombstone));
    }

    /// After a durable Session write ack, advance queued retirement CAS targets so a
    /// retirement queued at eviction still matches the row revision produced by an
    /// in-flight write (`database.rs` retirement revision contract).
    pub(crate) fn advance_pending_retirement_expected_revision(
        &mut self,
        record: &PersistedSessionRecord,
        prior_expected_revision: Option<u64>,
        committed_revision: u64,
    ) {
        for retirement in &mut self.pending_retirements {
            if retirement.expected_revision != prior_expected_revision {
                continue;
            }
            if !retirement_identity_matches_record(retirement, record) {
                continue;
            }
            retirement.expected_revision = Some(committed_revision);
        }
    }

    fn upsert_pending_tombstone_by_session_id(&mut self, tombstone: SessionRetentionTombstone) {
        self.pending_tombstone_upserts
            .retain(|pending| pending.session_id != tombstone.session_id);
        self.pending_tombstone_upserts.push(tombstone);
    }

    fn cancel_pending_tombstone_upsert(&mut self, tombstone: &SessionRetentionTombstone) {
        let key = TombstoneDurableKey::from(tombstone);
        self.pending_tombstone_upserts
            .retain(|pending| TombstoneDurableKey::from(pending) != key);
    }

    fn cancel_pending_tombstone_delete(&mut self, tombstone: &SessionRetentionTombstone) {
        let key = TombstoneDurableKey::from(tombstone);
        self.pending_tombstone_deletes.retain(|pending| {
            pending.session_id != key.session_id
                || pending.owner_authority_fingerprint != key.owner_authority_fingerprint
                || pending.incarnation_fingerprint != key.incarnation_fingerprint
        });
    }

    fn clear_matching_retention_tombstone_from_retirements(
        &mut self,
        tombstone: &SessionRetentionTombstone,
    ) {
        for retirement in &mut self.pending_retirements {
            if retirement
                .retention_tombstone
                .as_ref()
                .is_some_and(|pending| tombstone_stable_identity_matches(pending, tombstone))
            {
                retirement.retention_tombstone = None;
            }
        }
    }

    fn sync_pending_retirement_tombstone(&mut self, tombstone: &SessionRetentionTombstone) {
        for retirement in &mut self.pending_retirements {
            if retirement.session_id == tombstone.session_id {
                retirement.retention_tombstone = Some(tombstone.clone());
            }
        }
    }
}

pub(crate) fn persisted_record_for_session(
    stored: &StoredSession,
    max_events: usize,
) -> Option<PersistedSessionRecord> {
    match stored {
        StoredSession::Hot(record) => {
            Some(PersistedSessionRecord::from_record(record, max_events))
        }
        StoredSession::Cold(cold) => {
            let record = materialize_cold_session(cold, max_events)?;
            Some(PersistedSessionRecord::from_record(&record, max_events))
        }
    }
}

pub(crate) fn retained_event_ordinals(record: &crate::model::SessionRecord) -> HashSet<u64> {
    let len = record.events.len() as u64;
    if len == 0 {
        return HashSet::new();
    }
    let base = record
        .events_observed
        .saturating_sub(len)
        .saturating_add(1);
    (0..len).map(|index| base.saturating_add(index)).collect()
}

pub(crate) fn retained_event_start(events_observed: u64, events_len: usize) -> u64 {
    if events_len == 0 {
        events_observed.saturating_add(1)
    } else {
        events_observed
            .saturating_sub(events_len as u64)
            .saturating_add(1)
    }
}

pub(crate) fn retained_event_start_for_record(record: &crate::model::SessionRecord) -> u64 {
    retained_event_start(record.events_observed, record.events.len())
}

pub(crate) fn event_ordinal_for_id(
    record: &crate::model::SessionRecord,
    event_id: &str,
) -> Option<u64> {
    record.events.iter().enumerate().find_map(|(index, event)| {
        if event.event_id == event_id {
            let len = record.events.len() as u64;
            let base = record
                .events_observed
                .saturating_sub(len)
                .saturating_add(1);
            Some(base.saturating_add(index as u64))
        } else {
            None
        }
    })
}

pub(crate) fn retirement_from_stored_session(
    stored: &StoredSession,
    max_events: usize,
    retention_tombstone: SessionRetentionTombstone,
    expected_revision: Option<u64>,
) -> Option<SessionDatabaseRetirement> {
    if stored.lifecycle() != SessionLifecycle::Closed {
        return None;
    }
    let record = match stored {
        StoredSession::Hot(record) => record.clone(),
        StoredSession::Cold(cold) => materialize_cold_session(cold, max_events)?,
    };
    Some(SessionDatabaseRetirement {
        session_id: record.session_id.clone(),
        created_at: record.created_at,
        project: record.project.clone(),
        owner_authority_fingerprint: record.owner_authority_fingerprint.clone(),
        project_identity: record.project_identity.clone(),
        expected_revision,
        retention_tombstone: Some(retention_tombstone),
    })
}

pub(crate) fn tombstone_delete_from_row(
    tombstone: &SessionRetentionTombstone,
) -> SessionDatabaseTombstoneDelete {
    SessionDatabaseTombstoneDelete {
        session_id: tombstone.session_id.clone(),
        owner_authority_fingerprint: tombstone.owner_authority_fingerprint.clone(),
        incarnation_fingerprint: tombstone.incarnation_fingerprint.clone(),
    }
}

pub(crate) fn tombstone_stable_identity_matches(
    left: &SessionRetentionTombstone,
    right: &SessionRetentionTombstone,
) -> bool {
    left.session_id == right.session_id
        && left.owner_authority_fingerprint == right.owner_authority_fingerprint
        && left.incarnation_fingerprint == right.incarnation_fingerprint
}

pub(crate) fn tombstones_match(
    left: &SessionRetentionTombstone,
    right: &SessionRetentionTombstone,
) -> bool {
    tombstone_stable_identity_matches(left, right) && left.expiry_ordinal == right.expiry_ordinal
}

pub(crate) fn retirement_matches(left: &SessionDatabaseRetirement, right: &SessionDatabaseRetirement) -> bool {
    left.session_id == right.session_id
        && left.created_at == right.created_at
        && left.project == right.project
        && left.owner_authority_fingerprint == right.owner_authority_fingerprint
        && left.project_identity == right.project_identity
        && left.expected_revision == right.expected_revision
}

pub(crate) fn retirement_identity_matches_record(
    retirement: &SessionDatabaseRetirement,
    record: &PersistedSessionRecord,
) -> bool {
    retirement.session_id == record.session_id
        && retirement.created_at == record.created_at
        && retirement.project == record.project
        && retirement.owner_authority_fingerprint == record.owner_authority_fingerprint
        && retirement.project_identity == record.project_identity
}

/// After SQLite acknowledges a tombstone upsert, always record durable identity,
/// then reconcile against current RAM authority. Eviction after plan capture may
/// cancel pending upserts; the row may still be durable and must be deleted when
/// RAM no longer admits the stable identity and no newer upsert is queued.
pub(crate) fn reconcile_tombstone_durable_ack(
    inner: &mut SessionStoreInner,
    committed: &SessionRetentionTombstone,
) -> bool {
    inner
        .database_persistence
        .record_tombstone_committed(committed);
    inner
        .database_persistence
        .pending_tombstone_upserts
        .retain(|pending| !tombstones_match(pending, committed));
    let ram_authoritative = inner
        .tombstones
        .values()
        .any(|live| tombstone_stable_identity_matches(live, committed));
    let pending_reupsert = inner
        .database_persistence
        .pending_tombstone_upserts
        .iter()
        .any(|pending| tombstone_stable_identity_matches(pending, committed));
    if ram_authoritative || pending_reupsert {
        return true;
    }
    let key = TombstoneDurableKey::from(committed);
    let delete_pending = inner
        .database_persistence
        .pending_tombstone_deletes
        .iter()
        .any(|delete| {
            delete.session_id == key.session_id
                && delete.owner_authority_fingerprint == key.owner_authority_fingerprint
                && delete.incarnation_fingerprint == key.incarnation_fingerprint
        });
    if delete_pending {
        return true;
    }
    let delete = tombstone_delete_from_row(committed);
    if inner
        .database_persistence
        .try_push_pending_tombstone_delete(delete)
    {
        return true;
    }
    inner.note_retention_queue_admission_failure("reconcile_tombstone_durable_ack delete enqueue");
    false
}
