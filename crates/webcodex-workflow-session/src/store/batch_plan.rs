//! Bounded SQLite flush units for Phase 2 Stage J.7.
//!
//! Live replan: each flush attempt plans at most one bounded unit from the *current*
//! `SessionStoreInner` snapshot, marks in-flight state, releases the store lock,
//! applies SQLite I/O, then acknowledges only that unit before replanning.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use super::dirty::{
    event_ordinal_for_id, persisted_record_for_session, reconcile_tombstone_durable_ack,
    retained_event_ordinals, retirement_matches, PendingEventUpsert, SessionDirtyState,
    SQLITE_REVISION_MAX,
};
use super::SessionStoreInner;
use crate::model::{PersistedSessionRecord, SessionEvent, SessionRecord, StoredSession};
use crate::persistence::database::{
    SessionDatabaseBatch, SessionDatabaseRetirement, SessionDatabaseTombstoneDelete,
    SessionDatabaseWrite,
};

/// Maximum work units per `SessionDatabaseBatch` (Stage J.7).
/// Session write + retained event upserts = 1 unit; retirement + embedded tombstone = 1;
/// each standalone tombstone upsert or delete = 1.
pub(crate) const SQLITE_BATCH_MAX_UNITS: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BatchPlanError {
    NotSeeded,
    RevisionExhausted,
    SessionRecordMaterializationFailed { session_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AcknowledgeBatchOutcome {
    Complete,
    RetentionQueueInvariantBroken,
}

#[derive(Debug, Clone)]
pub(crate) struct SessionWritePlan {
    pub revision: u64,
    pub expected_revision: Option<u64>,
    pub previous_message_revision: u64,
    pub record: PersistedSessionRecord,
    /// Retained-event upserts selected for this write (append and in-place updates).
    pub appended_events: Vec<Arc<SessionEvent>>,
    /// Snapshot generation used for stale-safe acknowledgement.
    pub dirty_revision: u64,
}

/// One bounded flush unit captured under the `SessionStoreInner` mutex.
#[derive(Debug, Clone)]
pub(crate) struct BatchPlan {
    pub session_writes: BTreeMap<String, SessionWritePlan>,
    pub retirements: Vec<SessionDatabaseRetirement>,
    pub tombstone_upserts: Vec<crate::model::SessionRetentionTombstone>,
    pub tombstone_deletes: Vec<SessionDatabaseTombstoneDelete>,
    pub delivery_replay_deletions: Vec<crate::persistence::database::SessionDatabaseDeliveryReplayDeletion>,
}

impl BatchPlan {
    pub(crate) fn plan_bounded_flush_unit_from_inner(
        inner: &SessionStoreInner,
        max_units: usize,
    ) -> Result<Option<Self>, BatchPlanError> {
        if !inner.database_persistence.is_seeded() {
            return Err(BatchPlanError::NotSeeded);
        }

        let all_pending_deletes = dedupe_tombstone_deletes(
            &inner.database_persistence.pending_tombstone_deletes,
        );
        let all_pending_retirements = &inner.database_persistence.pending_retirements;

        let mut units = 0usize;
        let mut tombstone_deletes = Vec::new();
        let mut tombstone_delete_session_ids = HashSet::new();
        let mut seen_delete_stable_keys = BTreeSet::new();
        for delete in &inner.database_persistence.pending_tombstone_deletes {
            if units >= max_units {
                break;
            }
            let stable_key = (
                delete.session_id.as_str(),
                delete.owner_authority_fingerprint.as_str(),
                delete.incarnation_fingerprint.as_str(),
            );
            if !seen_delete_stable_keys.insert(stable_key) {
                continue;
            }
            if !tombstone_delete_session_ids.insert(delete.session_id.as_str()) {
                continue;
            }
            tombstone_deletes.push(delete.clone());
            units += 1;
        }

        let mut reconcile_reservation_slots = inner
            .database_persistence
            .available_tombstone_delete_reconcile_reservation_slots();

        let mut retirements = Vec::new();
        let mut planned_retirement_session_ids = HashSet::new();
        for retirement in all_pending_retirements {
            if units >= max_units {
                break;
            }
            if tombstone_delete_session_ids.contains(retirement.session_id.as_str()) {
                continue;
            }
            if !retirement_ready_for_bounded_plan(inner, retirement) {
                continue;
            }
            if retirement.retention_tombstone.is_some() && reconcile_reservation_slots == 0 {
                continue;
            }
            retirements.push(retirement.clone());
            planned_retirement_session_ids.insert(retirement.session_id.as_str());
            if retirement.retention_tombstone.is_some() {
                reconcile_reservation_slots = reconcile_reservation_slots.saturating_sub(1);
            }
            units += 1;
        }

        let mut session_writes = BTreeMap::new();
        let session_write_budget = max_units.saturating_sub(units);
        for session_id in inner
            .database_persistence
            .stable_dirty_session_ids_up_to(session_write_budget)
        {
            if units >= max_units {
                break;
            }
            let session_key = session_id.as_str();
            if tombstone_delete_session_ids.contains(session_key)
                || planned_retirement_session_ids.contains(session_key)
            {
                continue;
            }
            let Some(stored) = inner.sessions.get(session_key) else {
                continue;
            };
            let plan = session_write_plan(
                session_key,
                stored,
                inner.database_persistence.session_state(session_key),
                inner.max_events_per_session,
            )?;
            session_writes.insert(session_id, plan);
            units += 1;
        }

        let upsert_unit_budget = max_units.saturating_sub(units);
        let upsert_reservation_budget = reconcile_reservation_slots;
        let upsert_budget = upsert_unit_budget.min(upsert_reservation_budget);
        let planned_tombstone_upserts = tombstone_upserts_for_plan_bounded(
            &inner.database_persistence.pending_tombstone_upserts,
            all_pending_retirements,
            &all_pending_deletes,
            &planned_retirement_session_ids,
            upsert_budget,
        );
        units = units.saturating_add(planned_tombstone_upserts.len());

        let plan = Self {
            session_writes,
            retirements,
            tombstone_upserts: planned_tombstone_upserts,
            tombstone_deletes,
            delivery_replay_deletions: Vec::new(),
        };
        if plan.is_empty() {
            Ok(None)
        } else {
            Ok(Some(plan))
        }
    }

    pub(crate) fn into_batch(self) -> SessionDatabaseBatch {
        SessionDatabaseBatch {
            writes: self
                .session_writes
                .into_values()
                .map(|plan| SessionDatabaseWrite {
                    revision: plan.revision,
                    expected_revision: plan.expected_revision,
                    previous_message_revision: plan.previous_message_revision,
                    record: plan.record,
                    appended_events: plan.appended_events,
                })
                .collect(),
            retirements: self.retirements,
            tombstone_upserts: self.tombstone_upserts,
            tombstone_deletes: self.tombstone_deletes,
            delivery_replay_deletions: self.delivery_replay_deletions,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.session_writes.is_empty()
            && self.retirements.is_empty()
            && self.tombstone_upserts.is_empty()
            && self.tombstone_deletes.is_empty()
            && self.delivery_replay_deletions.is_empty()
    }

    pub(crate) fn work_units(&self) -> usize {
        self.session_writes.len()
            + self.retirements.len()
            + self.tombstone_upserts.len()
            + self.tombstone_deletes.len()
            + self.delivery_replay_deletions.len()
    }
}

pub(crate) fn has_bounded_flush_unit(inner: &SessionStoreInner) -> bool {
    if !inner.database_persistence.is_seeded() {
        return false;
    }
    match BatchPlan::plan_bounded_flush_unit_from_inner(inner, 1) {
        Ok(Some(_)) => true,
        _ => false,
    }
}

pub(crate) fn has_deferred_retention_work(inner: &SessionStoreInner) -> bool {
    if !inner.database_persistence.is_seeded() {
        return false;
    }
    inner
        .database_persistence
        .pending_retirements
        .iter()
        .any(|retirement| retirement_writer_blocked_deferrable(inner, retirement))
}

/// Retirements waiting on a flushable live-session dirty write for the same session id.
fn retirement_writer_blocked_deferrable(
    inner: &SessionStoreInner,
    retirement: &SessionDatabaseRetirement,
) -> bool {
    if retirement_ready_for_bounded_plan(inner, retirement) {
        return false;
    }
    let session_id = retirement.session_id.as_str();
    let Some(stored) = inner.sessions.get(session_id) else {
        return false;
    };
    let Some(state) = inner.database_persistence.session_state(session_id) else {
        return false;
    };
    if state.committed_revision != retirement.expected_revision {
        return false;
    }
    let Some(dirty_revision) = state.dirty_revision else {
        return false;
    };
    if state.in_flight_revision.is_some_and(|in_flight| in_flight != dirty_revision) {
        return false;
    }
    persisted_record_for_session(stored, inner.max_events_per_session).is_some()
}

fn retirement_unrecoverable_stall_summary(
    inner: &SessionStoreInner,
    retirement: &SessionDatabaseRetirement,
) -> Option<String> {
    if retirement_ready_for_bounded_plan(inner, retirement) {
        return None;
    }
    if retirement_writer_blocked_deferrable(inner, retirement) {
        return None;
    }
    let session_id = retirement.session_id.as_str();
    let live = inner.sessions.get(session_id);
    if let Some(state) = inner.database_persistence.session_state(session_id) {
        if live.is_none() && (state.dirty_revision.is_some() || state.in_flight_revision.is_some()) {
            return Some(super::bound_summary_string(&format!(
                "persist_failed: sqlite_retirement_stall: dirty_missing_session: session={session_id}"
            )));
        }
        if let Some(in_flight) = state.in_flight_revision {
            if state.dirty_revision.is_none() {
                return Some(super::bound_summary_string(&format!(
                    "persist_failed: sqlite_retirement_stall: stale_in_flight: session={session_id}"
                )));
            }
            if state.dirty_revision != Some(in_flight) {
                return Some(super::bound_summary_string(&format!(
                    "persist_failed: sqlite_retirement_stall: in_flight_dirty_mismatch: session={session_id}"
                )));
            }
        }
        if state.committed_revision != retirement.expected_revision {
            return Some(super::bound_summary_string(&format!(
                "persist_failed: sqlite_retirement_revision_stall: session={session_id}"
            )));
        }
    }
    if let Some(stored) = live {
        if persisted_record_for_session(stored, inner.max_events_per_session).is_none() {
            return Some(super::bound_summary_string(&format!(
                "persist_failed: sqlite_retirement_stall: session_unmaterializable: session={session_id}"
            )));
        }
    }
    Some(super::bound_summary_string(&format!(
        "persist_failed: sqlite_retirement_stall: unflushable: session={session_id}"
    )))
}

/// CAS/revision mismatch, stale writer markers, or RAM retention debt that cannot enter
/// bounded flush — cannot make progress without intervention. Queue saturation with flushable
/// pending work is not treated as permanent.
pub(crate) fn unrecoverable_retention_stall_summary(inner: &SessionStoreInner) -> Option<String> {
    if !inner.database_persistence.is_seeded() {
        return None;
    }
    for retirement in &inner.database_persistence.pending_retirements {
        if let Some(summary) = retirement_unrecoverable_stall_summary(inner, retirement) {
            return Some(summary);
        }
    }
    if let Some(summary) = retention_ram_debt_unflushable_summary(inner) {
        return Some(summary);
    }
    None
}

/// True when closed/tombstone RAM counts are within configured historical bounds.
pub(crate) fn sqlite_retention_ram_within_bounds(inner: &SessionStoreInner) -> bool {
    inner.lru.closed_count() <= inner.historical_session_retention_limit
        && inner.tombstones.len() <= inner.historical_session_retention_limit
}

fn retention_ram_debt_unflushable_summary(inner: &SessionStoreInner) -> Option<String> {
    if has_bounded_flush_unit(inner) {
        return None;
    }
    let retirement_debt = inner.lru.closed_count() > inner.historical_session_retention_limit;
    let tombstone_debt = inner.tombstones.len() > inner.historical_session_retention_limit;
    if !retirement_debt && !tombstone_debt {
        return None;
    }

    if retirement_debt {
        let queue = &inner.database_persistence;
        let pending_flushable = queue.pending_retirements.iter().any(|retirement| {
            retirement_ready_for_bounded_plan(inner, retirement)
        });
        if pending_flushable {
            return None;
        }
        let cannot_admit = queue.retention_queues_prevent_new_retirement_enqueue();
        let pending_empty = queue.pending_retirements.is_empty();
        if pending_empty && cannot_admit {
            return Some(super::bound_summary_string(
                "persist_failed: sqlite_retention_debt_stall: historical_closed_sessions",
            ));
        }
        if !pending_empty && cannot_admit && !has_deferred_retention_work(inner) {
            return Some(super::bound_summary_string(
                "persist_failed: sqlite_retention_debt_stall: pending_retirements_unflushable",
            ));
        }
    }

    if tombstone_debt {
        let queue = &inner.database_persistence;
        let pending_flushable = !queue.pending_tombstone_deletes.is_empty();
        if pending_flushable {
            return None;
        }
        let cannot_admit = queue.retention_queues_prevent_new_tombstone_delete_enqueue();
        if cannot_admit {
            return Some(super::bound_summary_string(
                "persist_failed: sqlite_retention_debt_stall: tombstones",
            ));
        }
    }

    None
}

pub(crate) fn mark_planned_writes_in_flight(
    inner: &mut SessionStoreInner,
    plan: &BatchPlan,
) -> bool {
    if !inner.database_persistence.is_seeded() {
        return true;
    }
    for (session_id, write_plan) in &plan.session_writes {
        inner
            .database_persistence
            .mark_write_in_flight(session_id, write_plan.dirty_revision);
    }
    let reconcile_candidates = plan
        .tombstone_upserts
        .len()
        .saturating_add(
            plan.retirements
                .iter()
                .filter(|retirement| retirement.retention_tombstone.is_some())
                .count(),
        );
    let mut reserved = 0usize;
    for _ in 0..reconcile_candidates {
        if !inner
            .database_persistence
            .reserve_tombstone_delete_flush_slot()
        {
            inner
                .database_persistence
                .release_tombstone_delete_flush_reservation(reserved);
            for (session_id, write_plan) in &plan.session_writes {
                inner
                    .database_persistence
                    .clear_write_in_flight(session_id, write_plan.dirty_revision);
            }
            return false;
        }
        reserved = reserved.saturating_add(1);
    }
    true
}

pub(crate) fn abandon_in_flight_writes(inner: &mut SessionStoreInner, plan: &BatchPlan) {
    if !inner.database_persistence.is_seeded() {
        return;
    }
    for (session_id, write_plan) in &plan.session_writes {
        inner
            .database_persistence
            .clear_write_in_flight(session_id, write_plan.dirty_revision);
    }
    release_reconcile_delete_reservations(inner, plan);
}

fn release_reconcile_delete_reservations(inner: &mut SessionStoreInner, plan: &BatchPlan) {
    let reconcile_candidates = plan
        .tombstone_upserts
        .len()
        .saturating_add(
            plan.retirements
                .iter()
                .filter(|retirement| retirement.retention_tombstone.is_some())
                .count(),
        );
    inner
        .database_persistence
        .release_tombstone_delete_flush_reservation(reconcile_candidates);
}

pub(crate) fn acknowledge_batch(
    inner: &mut SessionStoreInner,
    plan: &BatchPlan,
) -> AcknowledgeBatchOutcome {
    if !inner.database_persistence.is_seeded() {
        return AcknowledgeBatchOutcome::Complete;
    }

    release_reconcile_delete_reservations(inner, plan);

    let mut acked_session_write_ids: Vec<&str> = Vec::new();
    for (session_id, write_plan) in &plan.session_writes {
        let write_acked = inner
            .database_persistence
            .session_dirty
            .get_mut(session_id)
            .is_some_and(|state| ack_session_write(state, write_plan));
        if !write_acked {
            continue;
        }
        acked_session_write_ids.push(session_id.as_str());
        inner.database_persistence.advance_pending_retirement_expected_revision(
            &write_plan.record,
            write_plan.expected_revision,
            write_plan.revision,
        );
        let remove_state = inner.database_persistence.session_dirty.get(session_id).is_some_and(
            |state| {
                state.dirty_revision.is_none() && state.in_flight_revision.is_none()
            },
        );
        if !inner.sessions.contains_key(session_id) && remove_state {
            inner.database_persistence.session_dirty.remove(session_id);
        }
    }
    if !acked_session_write_ids.is_empty() {
        let ids: Vec<String> = acked_session_write_ids.into_iter().map(str::to_string).collect();
        inner
            .database_persistence
            .advance_dirty_session_pick_cursor_after_ack(ids);
    }

    let planned_retirements_by_session: HashMap<&str, &SessionDatabaseRetirement> = plan
        .retirements
        .iter()
        .map(|planned| (planned.session_id.as_str(), planned))
        .collect();
    let mut acked_retirement_session_ids = HashSet::new();
    for pending in &inner.database_persistence.pending_retirements {
        let Some(planned) = planned_retirements_by_session.get(pending.session_id.as_str()) else {
            continue;
        };
        if retirement_matches(pending, planned) {
            acked_retirement_session_ids.insert(planned.session_id.as_str());
        }
    }
    inner
        .database_persistence
        .pending_retirements
        .retain(|pending| {
            let Some(planned) = planned_retirements_by_session.get(pending.session_id.as_str())
            else {
                return true;
            };
            !retirement_matches(pending, planned)
        });
    for planned in &plan.retirements {
        if !acked_retirement_session_ids.contains(planned.session_id.as_str()) {
            continue;
        }
        inner
            .database_persistence
            .clear_revision_allocation_failure(&planned.session_id);
        let remove_dirty = !inner.sessions.contains_key(&planned.session_id)
            && inner
                .database_persistence
                .session_dirty
                .get(&planned.session_id)
                .is_none_or(|state| {
                    state.dirty_revision.is_none() && state.in_flight_revision.is_none()
                });
        if remove_dirty {
            inner
                .database_persistence
                .session_dirty
                .remove(&planned.session_id);
        }
        if let Some(tombstone) = planned.retention_tombstone.as_ref() {
            if !reconcile_tombstone_durable_ack(inner, tombstone) {
                return AcknowledgeBatchOutcome::RetentionQueueInvariantBroken;
            }
        }
    }

    for planned in &plan.tombstone_upserts {
        if !reconcile_tombstone_durable_ack(inner, planned) {
            return AcknowledgeBatchOutcome::RetentionQueueInvariantBroken;
        }
    }

    let planned_tombstone_delete_keys: HashSet<(&str, &str, &str)> = plan
        .tombstone_deletes
        .iter()
        .map(|planned| {
            (
                planned.session_id.as_str(),
                planned.owner_authority_fingerprint.as_str(),
                planned.incarnation_fingerprint.as_str(),
            )
        })
        .collect();
    if !planned_tombstone_delete_keys.is_empty() {
        inner
            .database_persistence
            .pending_tombstone_deletes
            .retain(|pending| {
                !planned_tombstone_delete_keys.contains(&(
                    pending.session_id.as_str(),
                    pending.owner_authority_fingerprint.as_str(),
                    pending.incarnation_fingerprint.as_str(),
                ))
            });
    }
    for planned in &plan.tombstone_deletes {
        inner
            .database_persistence
            .remove_durable_tombstone(planned);
    }
    inner.reenforce_sqlite_retention_bounds_after_durable_ack();
    AcknowledgeBatchOutcome::Complete
}

fn retirement_ready_for_bounded_plan(
    inner: &SessionStoreInner,
    retirement: &SessionDatabaseRetirement,
) -> bool {
    let session_id = retirement.session_id.as_str();
    if let Some(state) = inner.database_persistence.session_state(session_id) {
        if state.dirty_revision.is_some() || state.in_flight_revision.is_some() {
            return false;
        }
        if state.committed_revision != retirement.expected_revision {
            return false;
        }
    }
    true
}

fn ack_session_write(state: &mut SessionDirtyState, write_plan: &SessionWritePlan) -> bool {
    if state.committed_revision != write_plan.expected_revision {
        return false;
    }
    if state.in_flight_revision != Some(write_plan.dirty_revision) {
        return false;
    }
    if write_plan.revision > SQLITE_REVISION_MAX {
        return false;
    }
    state.committed_revision = Some(write_plan.revision);
    state.committed_message_revision = write_plan.record.message_observation_revision;
    state.committed_event_ordinal = write_plan.record.events_observed;
    state
        .pending_event_upserts
        .retain(|upsert| upsert.dirty_revision > write_plan.dirty_revision);
    state.in_flight_revision = None;
    if state.dirty_revision == Some(write_plan.dirty_revision) {
        state.dirty_revision = None;
    }
    true
}

fn session_write_plan(
    session_id: &str,
    stored: &StoredSession,
    dirty: Option<&SessionDirtyState>,
    max_events: usize,
) -> Result<SessionWritePlan, BatchPlanError> {
    let dirty = dirty.ok_or_else(|| BatchPlanError::SessionRecordMaterializationFailed {
        session_id: session_id.to_string(),
    })?;
    let dirty_revision = dirty.dirty_revision.ok_or_else(|| {
        BatchPlanError::SessionRecordMaterializationFailed {
            session_id: session_id.to_string(),
        }
    })?;
    let (record, live_record) = materialized_record_pair(stored, max_events).ok_or_else(|| {
        BatchPlanError::SessionRecordMaterializationFailed {
            session_id: session_id.to_string(),
        }
    })?;
    let appended_events = event_upserts_for_plan(&live_record, dirty, dirty_revision);
    Ok(SessionWritePlan {
        revision: dirty_revision,
        expected_revision: dirty.committed_revision,
        previous_message_revision: dirty.committed_message_revision,
        record,
        appended_events,
        dirty_revision,
    })
}

fn materialized_record_pair(
    stored: &StoredSession,
    max_events: usize,
) -> Option<(PersistedSessionRecord, SessionRecord)> {
    match stored {
        StoredSession::Hot(record) => {
            let persisted = PersistedSessionRecord::from_record(record, max_events);
            Some((persisted, record.clone()))
        }
        StoredSession::Cold(cold) => {
            let record = crate::persistence::materialize_cold_session(cold, max_events)?;
            let persisted = PersistedSessionRecord::from_record(&record, max_events);
            Some((persisted, record))
        }
    }
}

fn event_upserts_for_plan(
    record: &SessionRecord,
    dirty: &SessionDirtyState,
    plan_dirty_revision: u64,
) -> Vec<Arc<SessionEvent>> {
    let retained = retained_event_ordinals(record);
    let mut by_ordinal: BTreeMap<u64, &PendingEventUpsert> = BTreeMap::new();
    for upsert in &dirty.pending_event_upserts {
        if upsert.dirty_revision > plan_dirty_revision {
            continue;
        }
        if !retained.contains(&upsert.ordinal) {
            continue;
        }
        if event_ordinal_for_id(record, upsert.event.event_id.as_str()) != Some(upsert.ordinal) {
            continue;
        }
        by_ordinal
            .entry(upsert.ordinal)
            .and_modify(|existing| {
                if upsert.dirty_revision >= existing.dirty_revision {
                    *existing = upsert;
                }
            })
            .or_insert(upsert);
    }
    by_ordinal
        .values()
        .map(|upsert| Arc::clone(&upsert.event))
        .collect()
}

/// Select at most `budget` standalone tombstone upserts in stable `session_id` order.
/// Filters against **all** pending retirements (embedded tombstones), **all** deduped pending
/// deletes (stable-key supersession), and chunk retirements (`validate_batch` cross-category).
/// One scan, `O(budget)` payload map; per `session_id` keeps the row with greatest
/// `expiry_ordinal`.
fn tombstone_upserts_for_plan_bounded(
    pending: &[crate::model::SessionRetentionTombstone],
    all_pending_retirements: &[SessionDatabaseRetirement],
    all_pending_deletes: &[SessionDatabaseTombstoneDelete],
    chunk_retirement_session_ids: &HashSet<&str>,
    budget: usize,
) -> Vec<crate::model::SessionRetentionTombstone> {
    if budget == 0 || pending.is_empty() {
        return Vec::new();
    }
    let covered_by_retirement: HashSet<String> = all_pending_retirements
        .iter()
        .filter_map(|retirement| {
            retirement
                .retention_tombstone
                .as_ref()
                .map(|tombstone| tombstone.session_id.clone())
        })
        .collect();
    let superseded_by_delete: HashSet<(&str, &str, &str)> = all_pending_deletes
        .iter()
        .map(|delete| {
            (
                delete.session_id.as_str(),
                delete.owner_authority_fingerprint.as_str(),
                delete.incarnation_fingerprint.as_str(),
            )
        })
        .collect();

    use std::collections::BinaryHeap;

    let mut pick_heap: BinaryHeap<String> = BinaryHeap::new();
    let mut selected_by_session: HashMap<String, crate::model::SessionRetentionTombstone> =
        HashMap::new();

    for tombstone in pending {
        let session_id = tombstone.session_id.as_str();
        if chunk_retirement_session_ids.contains(session_id) {
            continue;
        }
        if covered_by_retirement.contains(&tombstone.session_id) {
            continue;
        }
        let delete_key = (
            session_id,
            tombstone.owner_authority_fingerprint.as_str(),
            tombstone.incarnation_fingerprint.as_str(),
        );
        if superseded_by_delete.contains(&delete_key) {
            continue;
        }
        if let Some(existing) = selected_by_session.get(session_id) {
            if tombstone.expiry_ordinal > existing.expiry_ordinal {
                selected_by_session.insert(tombstone.session_id.clone(), tombstone.clone());
            }
            continue;
        }
        if pick_heap.len() < budget {
            pick_heap.push(tombstone.session_id.clone());
            selected_by_session.insert(tombstone.session_id.clone(), tombstone.clone());
            continue;
        }
        if let Some(worst) = pick_heap.peek() {
            if session_id < worst.as_str() {
                let worst = pick_heap.pop().expect("heap head was present");
                selected_by_session.remove(&worst);
                pick_heap.push(tombstone.session_id.clone());
                selected_by_session.insert(tombstone.session_id.clone(), tombstone.clone());
            }
        }
    }

    let mut selected: Vec<_> = selected_by_session.into_values().collect();
    selected.sort_by(|left, right| left.session_id.cmp(&right.session_id));
    selected
}

fn dedupe_tombstone_deletes(
    deletes: &[SessionDatabaseTombstoneDelete],
) -> Vec<SessionDatabaseTombstoneDelete> {
    let mut seen = BTreeSet::new();
    let mut unique = Vec::new();
    for delete in deletes {
        let key = (
            delete.session_id.clone(),
            delete.owner_authority_fingerprint.clone(),
            delete.incarnation_fingerprint.clone(),
        );
        if seen.insert(key) {
            unique.push(delete.clone());
        }
    }
    unique
}
