//! Cancellation, restart recovery, and durable-before-live terminal transitions.

use super::protocol::bounded_text;
use super::store::{
    durable_record_from_snapshot, DurableDispatchPhase, DurableRunRecord, STORE_MAX_RUN_DIR_SCAN,
};
use super::*;
use webcodex_core::coding_agent::retention::{
    hot_inventory_expired, is_ordinary_terminal,
};

pub(super) fn coding_agent_run_retention_evictable(snapshot: &CodingAgentRunSnapshot) -> bool {
    is_ordinary_terminal(snapshot)
}

pub(super) fn select_bounded_inventory_runs(
    mut runs: Vec<CodingAgentRunSnapshot>,
) -> Vec<CodingAgentRunSnapshot> {
    runs.sort_by(|left, right| {
        let left_active = !left.state.terminal();
        let right_active = !right.state.terminal();
        left_active
            .cmp(&right_active)
            .reverse()
            .then_with(|| right.updated_at.cmp(&left.updated_at))
            .then_with(|| left.run_id.cmp(&right.run_id))
    });
    runs.truncate(CODING_AGENT_MAX_INVENTORY_RUNS);
    runs
}

fn durable_live_retention_aligned(
    record: &DurableRunRecord,
    snapshot: &CodingAgentRunSnapshot,
) -> bool {
    record.run_id == snapshot.run_id
        && record.intent_fingerprint == snapshot.intent_fingerprint
        && record.authority_fingerprint == snapshot.authority_fingerprint
        && record.runtime_project_id == snapshot.runtime_project_id
        && record.provider_id == snapshot.provider_id
        && record.provider_instance_id == snapshot.provider_instance_id
        && record.created_at == snapshot.created_at
        && record.replay_window == snapshot.replay_window
        && snapshot.state == record.state
        && snapshot.execution_state == record.execution_state
        && snapshot.updated_at == record.updated_at
        && snapshot.terminal == record.terminal
}

impl CodingAgentManager {
    pub(super) fn request_cancel(
        &self,
        run_id: &str,
        entry: &Arc<RunEntry>,
    ) -> CodingAgentRunSnapshot {
        let prompt_gate = entry.prompt_dispatch.lock().unwrap();
        let current = entry.snapshot();
        if current.state.terminal() {
            return current;
        }
        entry.cancel_requested.store(true, Ordering::Release);
        entry.changed.notify_all();
        entry.permission.wake_waiters();
        if *prompt_gate == PromptDispatchGateState::PrePrompt {
            self.finish_pre_prompt_cancelled(run_id, entry);
        }
        entry.snapshot()
    }

    pub(super) fn finish_pre_prompt_cancelled(&self, run_id: &str, entry: &Arc<RunEntry>) {
        let terminal = CodingAgentTerminal {
            stop_reason: None,
            error_code: None,
            message: Some("ACP prompt was not dispatched; CodingAgentRun was cancelled before prompt dispatch".to_string()),
            completed_at: now(),
        };
        let event = CodingAgentEvent {
            sequence: 0,
            kind: CodingAgentEventKind::Terminal,
            text: terminal.message.clone(),
            label: None,
            status: Some("cancelled".to_string()),
            usage: None,
        };
        self.commit_terminal_transition(
            run_id,
            entry,
            CodingAgentRunState::Cancelled,
            CodingAgentExecutionState::NotStarted,
            terminal,
            event,
        );
    }

    pub(super) fn pre_prompt_interrupted(&self, run_id: &str, entry: &Arc<RunEntry>) -> bool {
        if !self.accepting.load(Ordering::Acquire) && !entry.snapshot().state.terminal() {
            let _ = self.request_cancel(run_id, entry);
        }
        entry.snapshot().state.terminal()
    }

    pub(super) fn setup_timeout(&self, run_id: &str, entry: &Arc<RunEntry>, stage: &'static str) {
        tracing::warn!(run_id, stage, "ACP setup total deadline expired");
        self.setup_failure(
            run_id,
            entry,
            "coding_agent_setup_timeout",
            &format!(
                "CodingAgentRun total deadline expired during ACP {stage} before prompt dispatch"
            ),
        );
    }

    pub(super) fn recover(&self) -> Result<(), String> {
        let mut listing = self.store.list_records_for_retention()?;
        if listing.entry_count > STORE_MAX_RUN_DIR_SCAN {
            return Err("ACP Run durable state exceeds bounded record count".to_string());
        }
        let mut map = self.runs.lock().unwrap();
        for record in &mut listing.records {
            if record.dispatch_phase != DurableDispatchPhase::Terminal {
                let message = if record.dispatch_phase
                    == DurableDispatchPhase::PromptDispatchMayHaveOccurred
                {
                    "Runner restarted after prompt dispatch uncertainty barrier"
                } else {
                    "Runner restarted before prompt dispatch barrier"
                };
                let state = if record.dispatch_phase
                    == DurableDispatchPhase::PromptDispatchMayHaveOccurred
                {
                    CodingAgentRunState::Lost
                } else {
                    CodingAgentRunState::Failed
                };
                let execution = if state == CodingAgentRunState::Lost {
                    CodingAgentExecutionState::OutcomeUnknown
                } else {
                    CodingAgentExecutionState::NotStarted
                };
                record.state = state.clone();
                record.execution_state = execution;
                record.dispatch_phase = DurableDispatchPhase::Terminal;
                record.updated_at = now();
                record.terminal = Some(CodingAgentTerminal {
                    stop_reason: None,
                    error_code: Some(
                        if state == CodingAgentRunState::Lost {
                            "runner_restart_uncertain"
                        } else {
                            "runner_restart_not_started"
                        }
                        .to_string(),
                    ),
                    message: Some(message.to_string()),
                    completed_at: now(),
                });
                self.store.write(record)?;
            }
            map.insert(
                record.run_id.clone(),
                Arc::new(RunEntry::new(record.snapshot(0))),
            );
        }
        drop(map);
        self.hot_index.rebuild(&listing);

        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn sync_hot_index_from_store_for_test(&self) {
        if let Ok(listing) = self.store.list_records_for_retention() {
            self.hot_index.rebuild(&listing);
        }
    }

    pub(super) fn cleanup_expired_indexed(&self, archive_acknowledgements: &[String]) {
        let timestamp = now();
        let expired = self.hot_index.stale_hot_candidates(timestamp);
        self.cleanup_expired_stale_candidates(timestamp, expired, archive_acknowledgements);
    }

    pub(super) fn coordinate_admission_pressure(
        &self,
        protected_run_id: Option<&str>,
        archive_acknowledgements: &[String],
    ) -> Result<(), String> {
        self.enforce_durable_capacity_for_admission(protected_run_id, archive_acknowledgements)
    }

    pub(super) fn cleanup_expired(&self, archive_acknowledgements: &[String]) {
        self.cleanup_expired_indexed(archive_acknowledgements);
    }

    fn cleanup_expired_stale_candidates(&self, timestamp: i64, stale_candidate_run_ids: Vec<String>, archive_acknowledgements: &[String]) {
        for run_id in stale_candidate_run_ids {
            let entry = {
                let map = self.runs.lock().unwrap();
                map.get(&run_id).cloned()
            };
            let Some(entry) = entry else {
                continue;
            };
            let _terminal_transition = entry.terminal_transition.lock().unwrap();
            let snapshot = entry.snapshot();
            if !hot_inventory_expired(&snapshot, timestamp)
                || !archive_acknowledgements.contains(
                    &webcodex_core::coding_agent::archive_identity_fingerprint(&snapshot),
                )
            {
                continue;
            }
            let durable = match self.store.read(&run_id) {
                Ok(Some(record)) => record,
                Ok(None) | Err(_) => continue,
            };
            if !durable_live_retention_aligned(&durable, &snapshot) {
                continue;
            }
            match self.store.archive_terminal(&durable) {
                Ok(()) => {
                    self.hot_index.on_hot_dir_removed(&run_id);
                    self.runs.lock().unwrap().remove(&run_id);
                }
                Err(error) => tracing::error!(run_id = %run_id, error = %error,
                    "ACP terminal archival failed; retaining in-memory Run"),
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn cleanup_expired_for_test(&self) {
        self.cleanup_expired(&[]);
    }

    #[cfg(test)]
    pub(crate) fn cleanup_expired_stale_candidates_for_test(
        &self,
        stale_candidate_run_ids: Vec<String>,
        archive_acknowledgements: &[String],
    ) {
        self.cleanup_expired_stale_candidates(now(), stale_candidate_run_ids, archive_acknowledgements);
    }

    pub(super) fn enforce_durable_capacity_for_admission(
        &self,
        protected_run_id: Option<&str>,
        archive_acknowledgements: &[String],
    ) -> Result<(), String> {
        let deadline = Instant::now() + Duration::from_secs(2);
        let candidates = self.hot_index.acknowledged_candidates(
            protected_run_id, archive_acknowledgements,
        );
        for record in candidates {
            if !self.hot_index.admission_pressure() { return Ok(()); }
            if Instant::now() >= deadline { break; }
            // archive_terminal also reconciles an exact previous partial rename.
            self.try_evict_retained_durable_run(&record.run_id, &record)?;
        }
        if self.hot_index.admission_pressure() {
            return Err("ACP Run hot inventory cannot reclaim space without durable Server archive acknowledgement and a safe terminal candidate within the admission budget".into());
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn enforce_durable_capacity_for_admission_for_test(
        &self,
        protected_run_id: Option<&str>,
    ) -> Result<(), String> {
        self.sync_hot_index_from_store_for_test();
        let mut records = self.hot_index.records_for_test_acknowledgements();
        records.sort_by_cached_key(|record| (!is_ordinary_terminal(&record.snapshot(0)), record.updated_at, record.run_id.clone()));
        let acknowledgements = records.iter()
            .map(|record| webcodex_core::coding_agent::archive_identity_fingerprint(&record.snapshot(0)))
            .collect::<Vec<_>>();
        self.enforce_durable_capacity_for_admission(protected_run_id, &acknowledgements)
    }

    fn try_evict_retained_durable_run(
        &self,
        run_id: &str,
        record: &DurableRunRecord,
    ) -> Result<bool, String> {
        let entry = {
            let map = self.runs.lock().unwrap();
            map.get(run_id).cloned()
        };
        let Some(entry) = entry else {
            if record.dispatch_phase != DurableDispatchPhase::Terminal {
                return Ok(false);
            }
            self.retire_durable_terminal(record)?;
            return Ok(true);
        };
        let _terminal_transition = entry.terminal_transition.lock().unwrap();
        let snapshot = entry.snapshot();
        if !snapshot.state.terminal() || !durable_live_retention_aligned(record, &snapshot) {
            return Ok(false);
        }
        self.retire_durable_terminal(record)?;
        self.runs.lock().unwrap().remove(run_id);
        Ok(true)
    }

    fn retire_durable_terminal(&self, record: &DurableRunRecord) -> Result<(), String> {
        // The caller has already checked the immutable Server receipt. Hot
        // expiry or capacity pressure does not authorize replay-fence deletion.
        self.store.archive_terminal(record)?;
        self.hot_index.on_hot_dir_removed(&record.run_id);
        Ok(())
    }

    pub(super) fn setup_failure(
        &self,
        run_id: &str,
        entry: &Arc<RunEntry>,
        code: &str,
        message: &str,
    ) {
        let terminal = CodingAgentTerminal {
            stop_reason: None,
            error_code: Some(code.to_string()),
            message: Some(bounded_text(message)),
            completed_at: now(),
        };
        let event = CodingAgentEvent {
            sequence: 0,
            kind: CodingAgentEventKind::Terminal,
            text: terminal.message.clone(),
            label: terminal.error_code.clone(),
            status: Some("failed".to_string()),
            usage: None,
        };
        self.commit_terminal_transition(
            run_id,
            entry,
            CodingAgentRunState::Failed,
            CodingAgentExecutionState::NotStarted,
            terminal,
            event,
        );
    }

    pub(super) fn finish_failed(
        &self,
        run_id: &str,
        entry: &Arc<RunEntry>,
        code: &str,
        message: String,
    ) {
        let terminal = CodingAgentTerminal {
            stop_reason: None,
            error_code: Some(code.to_string()),
            message: Some(bounded_text(&message)),
            completed_at: now(),
        };
        let event = CodingAgentEvent {
            sequence: 0,
            kind: CodingAgentEventKind::Terminal,
            text: terminal.message.clone(),
            label: terminal.error_code.clone(),
            status: Some("failed".to_string()),
            usage: None,
        };
        self.commit_terminal_transition(
            run_id,
            entry,
            CodingAgentRunState::Failed,
            CodingAgentExecutionState::Completed,
            terminal,
            event,
        );
    }

    pub(super) fn finish_terminal(
        &self,
        run_id: &str,
        entry: &Arc<RunEntry>,
        state: CodingAgentRunState,
        stop_reason: &str,
        message: Option<&str>,
    ) {
        let terminal = CodingAgentTerminal {
            stop_reason: Some(stop_reason.to_string()),
            error_code: if state == CodingAgentRunState::Failed {
                Some(stop_reason.to_string())
            } else {
                None
            },
            message: message.map(bounded_text),
            completed_at: now(),
        };
        let event = CodingAgentEvent {
            sequence: 0,
            kind: CodingAgentEventKind::Terminal,
            text: message.map(bounded_text),
            label: Some(stop_reason.to_string()),
            status: Some(format!("{state:?}").to_ascii_lowercase()),
            usage: None,
        };
        self.commit_terminal_transition(
            run_id,
            entry,
            state,
            CodingAgentExecutionState::Completed,
            terminal,
            event,
        );
    }

    pub(super) fn mark_lost(&self, run_id: &str, entry: &Arc<RunEntry>, code: &str) {
        let terminal = CodingAgentTerminal {
            stop_reason: None,
            error_code: Some(code.to_string()),
            message: Some(
                "ACP prompt outcome is unknown; prompt must not be redispatched".to_string(),
            ),
            completed_at: now(),
        };
        let event = CodingAgentEvent {
            sequence: 0,
            kind: CodingAgentEventKind::Terminal,
            text: terminal.message.clone(),
            label: terminal.error_code.clone(),
            status: Some("lost".to_string()),
            usage: None,
        };
        self.commit_terminal_transition(
            run_id,
            entry,
            CodingAgentRunState::Lost,
            CodingAgentExecutionState::OutcomeUnknown,
            terminal,
            event,
        );
    }

    fn commit_terminal_transition(
        &self,
        run_id: &str,
        entry: &Arc<RunEntry>,
        desired_state: CodingAgentRunState,
        desired_execution_state: CodingAgentExecutionState,
        desired_terminal: CodingAgentTerminal,
        desired_event: CodingAgentEvent,
    ) {
        let Some(_terminal_transition) = entry.begin_terminal_transition() else {
            return;
        };
        let current = entry.snapshot();
        let mut candidate = current.clone();
        candidate.state = desired_state.clone();
        candidate.execution_state = desired_execution_state;
        candidate.updated_at = desired_terminal.completed_at;
        candidate.terminal = Some(desired_terminal);
        let durable = durable_record_from_snapshot(run_id, &candidate, DurableDispatchPhase::Terminal)
            .and_then(|record| self.store.write(&record).map(|()| record));
        match durable {
            Ok(record) => {
                self.hot_index.on_record_written(&record, false);
                entry.publish_terminal(candidate, desired_event);
                if !current.state.terminal() {
                    self.hot_index.mark_terminal(run_id);
                }
            }
            Err(error) => {
                let pre_prompt = desired_execution_state == CodingAgentExecutionState::NotStarted;
                let completed_at = now();
                let terminal = CodingAgentTerminal {
                    stop_reason: None,
                    error_code: Some(CODING_AGENT_TERMINAL_PERSISTENCE_UNCERTAIN.to_string()),
                    message: Some(TERMINAL_PERSISTENCE_UNCERTAIN_MESSAGE.to_string()),
                    completed_at,
                };
                let was_active = !current.state.terminal();
                let mut uncertain = current;
                uncertain.state = if pre_prompt {
                    CodingAgentRunState::Failed
                } else {
                    CodingAgentRunState::Lost
                };
                uncertain.execution_state = if pre_prompt {
                    CodingAgentExecutionState::NotStarted
                } else {
                    CodingAgentExecutionState::OutcomeUnknown
                };
                uncertain.updated_at = completed_at;
                uncertain.terminal = Some(terminal.clone());
                let status = if pre_prompt { "failed" } else { "lost" };
                tracing::error!(
                    run_id = %run_id,
                    desired_state = ?desired_state,
                    desired_execution_state = ?desired_execution_state,
                    error = %error,
                    "ACP terminal durable commit failed; publishing conservative persistence uncertainty"
                );
                entry.publish_terminal(
                    uncertain,
                    CodingAgentEvent {
                        sequence: 0,
                        kind: CodingAgentEventKind::Terminal,
                        text: terminal.message.clone(),
                        label: terminal.error_code.clone(),
                        status: Some(status.to_string()),
                        usage: None,
                    },
                );
                if was_active {
                    self.hot_index.mark_terminal(run_id);
                }
            }
        }
    }

    pub(super) fn persist_phase(
        &self,
        run_id: &str,
        entry: &Arc<RunEntry>,
        phase: DurableDispatchPhase,
        state: CodingAgentRunState,
        execution_state: CodingAgentExecutionState,
        terminal: Option<CodingAgentTerminal>,
    ) -> Result<(), String> {
        entry.update_snapshot(|snapshot| {
            snapshot.state = state.clone();
            snapshot.execution_state = execution_state;
            snapshot.terminal = terminal.clone();
        });
        self.persist_from_entry(run_id, entry, phase)
    }

    pub(super) fn persist_from_entry(
        &self,
        run_id: &str,
        entry: &Arc<RunEntry>,
        phase: DurableDispatchPhase,
    ) -> Result<(), String> {
        let snapshot = entry.snapshot();
        let record = durable_record_from_snapshot(run_id, &snapshot, phase)?;
        self.store.write(&record)?;
        self.hot_index.on_record_written(&record, false);
        Ok(())
    }
}
