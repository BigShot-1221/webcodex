//! Non-destructive materialized working-set policy, under the Session store lock.
//! Cold bytes are owned in RAM, so dirty data and in-flight Job/Agent identities
//! are not discarded or delegated to an older disk image. Active is not a pin.
use std::sync::Arc;

use super::dirty::retained_event_start_for_record;
use super::*;
use crate::persistence::cold_session_from_record;
use crate::persistence::materialize_cold_session;

/// Outcome of resolving the mutation target while `sessions` may be mutably borrowed.
enum RetainedRecordBranch {
    Hot {
        updated: bool,
        pending_upsert: Option<(u64, Arc<SessionEvent>, u64)>,
        closed: bool,
    },
    Cold(ColdSessionRecord),
    Missing,
}

impl SessionStore {
    /// Update retained evidence without duplicating Hot/Cold ownership handling.
    /// Active Cold records hydrate; Closed records stay Cold. The callback owns
    /// event selection/timestamps and must be replay-safe: a Cold CAS conflict
    /// reapplies it to the newer record. Persistence remains the caller's duty.
    pub(super) fn update_retained_record<T: Clone>(
        &self,
        session_id: &str,
        value: T,
        update: impl Fn(&mut SessionRecord, T) -> Option<(u64, Arc<SessionEvent>)>,
    ) -> bool {
        let mut hot_value = Some(value);
        let cold = {
            let mut inner = self.lock_inner();
            if !inner.prepare_active_session(session_id) {
                return false;
            }
            let branch = match inner.sessions.get_mut(session_id) {
                Some(StoredSession::Hot(record)) => {
                    // Move, rather than clone, the evidence on the ordinary Hot path.
                    let (updated, pending_upsert, closed) = match update(record, hot_value.take().expect("hot update consumes value once")) {
                        Some((ordinal, event)) => {
                            let retained_event_start = retained_event_start_for_record(record);
                            (
                                true,
                                Some((ordinal, event, retained_event_start)),
                                !record.lifecycle.allows_mutation(),
                            )
                        }
                        None => (false, None, !record.lifecycle.allows_mutation()),
                    };
                    RetainedRecordBranch::Hot {
                        updated,
                        pending_upsert,
                        closed,
                    }
                }
                Some(StoredSession::Cold(record)) => RetainedRecordBranch::Cold(record.clone()),
                None => RetainedRecordBranch::Missing,
            };
            match branch {
                RetainedRecordBranch::Hot {
                    updated,
                    pending_upsert,
                    closed,
                } => {
                    if let Some((ordinal, event, retained_event_start)) = pending_upsert {
                        inner.mark_session_event_upsert(
                            session_id,
                            ordinal,
                            event,
                            retained_event_start,
                        );
                    }
                    drop(inner);
                    if updated && closed {
                        self.coldify_closed_session(session_id);
                    }
                    return updated;
                }
                RetainedRecordBranch::Cold(cold) => cold,
                RetainedRecordBranch::Missing => return false,
            }
        };
        let value = hot_value.expect("cold update retains value");
        self.rewrite_cold_record(session_id, cold, false, |record, _| {
            update(record, value.clone()).map(|(ordinal, event)| {
                let retained_event_start = retained_event_start_for_record(record);
                ColdRewriteOutcome::EventUpsert {
                    ordinal,
                    event,
                    retained_event_start,
                }
            })
        })
    }
}

impl SessionStoreInner {
    /// Prepare the Active mutation residency target before business fields change.
    ///
    /// Missing sessions return `true` so callers retain their own Unknown/Missing
    /// branches. When the session exists, SQLite seeded mode runs revision preflight
    /// admission via [`DatabasePersistenceQueue::ensure_revision_capacity`]; admission
    /// failure records `last_persist_error` when empty and returns `false` without
    /// touching session bytes, LRU, or hot working-set compaction. Already-Hot sessions
    /// return `true` after admission. Cold Active sessions materialize to Hot only after
    /// admission succeeds.
    pub(super) fn prepare_active_session(&mut self, session_id: &str) -> bool {
        if !self.sessions.contains_key(session_id) {
            return true;
        }
        if !self.database_persistence.ensure_revision_capacity(session_id) {
            self.note_revision_admission_failure(session_id);
            return false;
        }
        let Some(StoredSession::Cold(cold)) = self.sessions.get(session_id) else {
            return true;
        };
        if !cold.lifecycle.allows_mutation() {
            return true;
        }
        let Some(record) = materialize_cold_session(cold, self.max_events_per_session) else {
            tracing::warn!("canonical Active Session payload unavailable; mutation rejected");
            return false;
        };
        self.sessions
            .insert(session_id.to_owned(), StoredSession::Hot(record));
        self.touch(session_id);
        self.compact_hot_sessions(session_id);
        true
    }

    /// Reuse the existing access order rather than adding a CLOCK/SLRU index.
    /// This runs only on admission, not every event. No full-record borrow can
    /// race the transition because both are protected by the same store mutex.
    pub(super) fn compact_hot_sessions(&mut self, protected: &str) {
        let target = self.hot_session_capacity_target.max(1);
        let hot = self
            .sessions
            .values()
            .filter(|entry| entry.hot().is_some())
            .count();
        if hot <= target {
            return;
        }
        let candidates: Vec<_> = self
            .lru
            .iter()
            .filter(|id| id.as_str() != protected)
            .filter(|id| {
                self.sessions
                    .get(*id)
                    .is_some_and(|entry| entry.hot().is_some())
            })
            .cloned()
            .collect();
        let mut remaining = hot - target;
        for id in candidates {
            if remaining == 0 {
                break;
            }
            let Some(record) = self.sessions.get(&id).and_then(StoredSession::hot) else {
                continue;
            };
            match cold_session_from_record(record, self.max_events_per_session) {
                Ok(cold) => {
                    self.sessions.insert(id, StoredSession::Cold(cold));
                    remaining -= 1;
                }
                Err(_) => tracing::warn!("Session compaction failed; retaining Hot data"),
            }
        }
    }
}

#[cfg(test)]
mod tests;
