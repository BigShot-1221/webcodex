//! Incremental Runner hot inventory. Disk remains the authoritative replay fence.

use super::store::{DurableDispatchPhase, DurableRetentionListing, DurableRunRecord};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Mutex;
use webcodex_core::coding_agent::retention::{hot_retention_deadline, is_ordinary_terminal};
use webcodex_core::coding_agent::{archive_identity_fingerprint, CODING_AGENT_MAX_INVENTORY_RUNS};

pub(super) const ADMISSION_PRESSURE_RECEIPT_BUDGET: usize = 64;

#[derive(Debug, Default)]
pub(super) struct HotInventoryIndex {
    inner: Mutex<HotInventoryState>,
}

#[derive(Debug, Default)]
struct HotInventoryState {
    hot_dir_count: usize,
    tombstone_count: usize,
    active: HashSet<String>,
    records: HashMap<String, DurableRunRecord>,
    reserved_hot_dirs: HashSet<String>,
    receipt_runs: HashMap<String, String>,
    stale_hot_deadline: BTreeSet<(i64, String)>,
}

impl HotInventoryState {
    fn unindex_terminal(&mut self, record: &DurableRunRecord) {
        if record.dispatch_phase == DurableDispatchPhase::Terminal {
            let snapshot = record.snapshot(0);
            self.receipt_runs.remove(&archive_identity_fingerprint(&snapshot));
            if let Some(deadline) = hot_retention_deadline(&snapshot) {
                self.stale_hot_deadline.remove(&(deadline, record.run_id.clone()));
            }
        }
    }

    fn index_record(&mut self, record: &DurableRunRecord) {
        if let Some(prior) = self.records.remove(&record.run_id) {
            self.unindex_terminal(&prior);
        }
        if record.dispatch_phase == DurableDispatchPhase::Terminal {
            let snapshot = record.snapshot(0);
            self.receipt_runs.insert(archive_identity_fingerprint(&snapshot), record.run_id.clone());
            if let Some(deadline) = hot_retention_deadline(&snapshot) {
                self.stale_hot_deadline.insert((deadline, record.run_id.clone()));
            }
        }
        self.records.insert(record.run_id.clone(), record.clone());
    }
}

impl HotInventoryIndex {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn rebuild(&self, listing: &DurableRetentionListing) {
        let mut inner = self.inner.lock().unwrap();
        // A recovered terminal record is not an active execution.
        let active = std::mem::take(&mut inner.active);
        *inner = HotInventoryState {
            hot_dir_count: listing.entry_count,
            tombstone_count: listing.entry_count.saturating_sub(listing.records.len()),
            active,
            ..Default::default()
        };
        for record in &listing.records {
            inner.index_record(record);
        }
    }

    pub(super) fn hot_dir_count(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.hot_dir_count.saturating_add(inner.reserved_hot_dirs.len())
    }

    pub(super) fn reserve_admission_placeholder(&self, run_id: &str) {
        let mut inner = self.inner.lock().unwrap();
        if !inner.records.contains_key(run_id) {
            inner.reserved_hot_dirs.insert(run_id.to_owned());
        }
    }

    pub(super) fn release_admission_placeholder(&self, run_id: &str) {
        self.inner.lock().unwrap().reserved_hot_dirs.remove(run_id);
    }

    pub(super) fn active_live_count(&self) -> usize {
        self.inner.lock().unwrap().active.len()
    }

    pub(super) fn mark_active(&self, run_id: &str) {
        self.inner.lock().unwrap().active.insert(run_id.to_owned());
    }

    pub(super) fn mark_terminal(&self, run_id: &str) {
        self.inner.lock().unwrap().active.remove(run_id);
    }

    pub(super) fn on_record_written(&self, record: &DurableRunRecord, was_new_hot_dir: bool) {
        let mut inner = self.inner.lock().unwrap();
        inner.reserved_hot_dirs.remove(&record.run_id);
        if was_new_hot_dir && !inner.records.contains_key(&record.run_id) {
            inner.hot_dir_count += 1;
        }
        inner.index_record(record);
    }

    pub(super) fn on_hot_dir_removed(&self, run_id: &str) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(prior) = inner.records.remove(run_id) {
            inner.unindex_terminal(&prior);
            inner.hot_dir_count -= 1;
        }
        inner.reserved_hot_dirs.remove(run_id);
    }

    #[cfg(test)]
    pub(super) fn records_for_test_acknowledgements(&self) -> Vec<DurableRunRecord> {
        self.inner.lock().unwrap().records.values()
            .filter(|record| record.dispatch_phase == DurableDispatchPhase::Terminal)
            .cloned().collect()
    }

    /// Only the exact acknowledged identities can be considered. The cost is
    /// bounded by the receipt list, not by the number of retained Runs.
    pub(super) fn acknowledged_candidates(
        &self,
        protected_run_id: Option<&str>,
        acknowledgements: &[String],
    ) -> Vec<DurableRunRecord> {
        let inner = self.inner.lock().unwrap();
        let mut seen = HashSet::new();
        let mut candidates = acknowledgements.iter()
            .take(ADMISSION_PRESSURE_RECEIPT_BUDGET)
            .filter_map(|identity| inner.receipt_runs.get(identity))
            .filter(|id| protected_run_id != Some(id.as_str()) && seen.insert((*id).clone()))
            .filter_map(|id| inner.records.get(id).cloned())
            .collect::<Vec<_>>();
        candidates.sort_by_cached_key(|record| {
            (!is_ordinary_terminal(&record.snapshot(0)), record.updated_at, record.run_id.clone())
        });
        candidates
    }

    pub(super) fn stale_hot_candidates(&self, timestamp: i64) -> Vec<String> {
        self.inner.lock().unwrap().stale_hot_deadline.iter()
            .take_while(|(deadline, _)| *deadline <= timestamp)
            .take(ADMISSION_PRESSURE_RECEIPT_BUDGET)
            .map(|(_, id)| id.clone()).collect()
    }

    pub(super) fn admission_pressure(&self) -> bool {
        self.hot_dir_count() >= CODING_AGENT_MAX_INVENTORY_RUNS
    }

    pub(super) fn maintenance_counts(&self) -> (usize, usize, usize) {
        let inner = self.inner.lock().unwrap();
        (inner.hot_dir_count + inner.reserved_hot_dirs.len(), inner.tombstone_count, inner.active.len())
    }
}