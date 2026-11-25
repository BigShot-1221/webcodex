use super::*;

#[test]
#[cfg(unix)]
fn corrupt_durable_record_after_possible_dispatch_fails_closed_without_redispatch() {
    let temp = crate::tests::executable_tempdir();
    let (exe, args) = fake_agent(&temp, "end");
    let cfg = fake_config(exe, args);
    let projects = project_fixture(&temp);
    let root = temp.path().join("repo");
    let manager = CodingAgentManager::with_store(&cfg, temp.path().join("store")).unwrap();
    let run = "wc_agent_run_corruptdurable01";
    let request = start_request(&manager, &root, run, BTreeMap::new());
    assert!(manager.handle(request.clone(), &projects).error.is_none());
    wait_for_snapshot(&manager, run, |snapshot| snapshot.state.terminal());
    assert_eq!(
        received_methods(&wire_log(&temp))
            .iter()
            .filter(|method| method.as_str() == "session/prompt")
            .count(),
        1
    );

    manager.runs.lock().unwrap().remove(run);
    fs::write(manager.store.state_path(run), b"{corrupt-json").unwrap();
    let corrupt = manager.handle(request.clone(), &projects);
    assert_eq!(
        corrupt.dispatch_state,
        CodingAgentDispatchState::OutcomeUnknown
    );
    assert_eq!(
        corrupt.error.as_ref().map(|error| error.code.as_str()),
        Some("coding_agent_durable_state_unavailable")
    );

    fs::remove_file(manager.store.state_path(run)).unwrap();
    let missing = manager.handle(request.clone(), &projects);
    assert_eq!(
        missing.dispatch_state,
        CodingAgentDispatchState::OutcomeUnknown
    );
    assert_eq!(
        missing.error.as_ref().map(|error| error.code.as_str()),
        Some("coding_agent_durable_state_unavailable")
    );
    let store_root = manager.store.root.clone();
    drop(manager);
    let restarted = CodingAgentManager::with_store(&cfg, store_root).unwrap();
    let after_restart = restarted.handle(
        start_request(&restarted, &root, run, BTreeMap::new()),
        &projects,
    );
    assert_eq!(
        after_restart.dispatch_state,
        CodingAgentDispatchState::OutcomeUnknown
    );
    assert_eq!(
        after_restart
            .error
            .as_ref()
            .map(|error| error.code.as_str()),
        Some("coding_agent_durable_state_unavailable")
    );
    assert_eq!(
        received_methods(&wire_log(&temp))
            .iter()
            .filter(|method| method.as_str() == "session/prompt")
            .count(),
        1,
        "missing or corrupt durable state after a possible prompt must never become retry authority"
    );
}

#[test]
fn durable_store_replaces_existing_state_cross_platform() {
    let temp = TempDir::new().unwrap();
    let store = DurableRunStore::new(temp.path().join("store"));
    let timestamp = now();
    let mut record = DurableRunRecord {
        schema_version: STORE_SCHEMA_VERSION,
        run_id: "wc_agent_run_replace_state01".to_string(),
        intent_fingerprint: "fingerprint".to_string(),
        authority_fingerprint: "auth_replace".to_string(),
        runtime_project_id: "agent:test:demo".to_string(),
        provider_id: "codex".to_string(),
        provider_instance_id: "acp_replace".to_string(),
        state: CodingAgentRunState::Starting,
        execution_state: CodingAgentExecutionState::NotStarted,
        dispatch_phase: DurableDispatchPhase::BeforePromptBarrier,
        replay_window: None,
        created_at: timestamp,
        updated_at: timestamp,
        terminal: None,
    };
    store.write(&record).unwrap();
    record.state = CodingAgentRunState::Running;
    record.execution_state = CodingAgentExecutionState::OutcomeUnknown;
    record.dispatch_phase = DurableDispatchPhase::PromptDispatchMayHaveOccurred;
    record.updated_at = timestamp.saturating_add(1);
    store.write(&record).unwrap();
    let restored = store.read(&record.run_id).unwrap().unwrap();
    assert_eq!(
        restored.dispatch_phase,
        DurableDispatchPhase::PromptDispatchMayHaveOccurred
    );
    assert_eq!(
        restored.execution_state,
        CodingAgentExecutionState::OutcomeUnknown
    );
}
#[test]
#[cfg(unix)]
fn pre_barrier_restart_is_not_started_and_child_tree_is_reaped() {
    let temp = crate::tests::executable_tempdir();
    let (exe, args) = fake_agent(&temp, "end");
    let cfg = fake_config(exe, args);
    let store_root = temp.path().join("store");
    fs::create_dir_all(&store_root).unwrap();
    let initial = CodingAgentManager::with_store(&cfg, store_root.clone()).unwrap();
    let provider = initial.providers().remove(0);
    drop(initial);
    let timestamp = now();
    DurableRunStore::new(store_root.clone())
        .write(&DurableRunRecord {
            schema_version: STORE_SCHEMA_VERSION,
            run_id: "wc_agent_run_prebarrier01".to_string(),
            intent_fingerprint: "fingerprint".to_string(),
            authority_fingerprint: "auth_test".to_string(),
            runtime_project_id: "agent:test:demo".to_string(),
            provider_id: "codex".to_string(),
            provider_instance_id: provider.provider_instance_id,
            state: CodingAgentRunState::Starting,
            execution_state: CodingAgentExecutionState::NotStarted,
            dispatch_phase: DurableDispatchPhase::BeforePromptBarrier,
            replay_window: None,
            created_at: timestamp,
            updated_at: timestamp,
            terminal: None,
        })
        .unwrap();
    let restarted = CodingAgentManager::with_store(&cfg, store_root).unwrap();
    let recovered = restarted
        .runs
        .lock()
        .unwrap()
        .get("wc_agent_run_prebarrier01")
        .unwrap()
        .snapshot();
    assert_eq!(recovered.state, CodingAgentRunState::Failed);
    assert_eq!(
        recovered.execution_state,
        CodingAgentExecutionState::NotStarted
    );

    let temp = crate::tests::executable_tempdir();
    let (exe, args) = fake_agent(&temp, "spawn_descendant");
    let cfg = fake_config(exe, args);
    let projects = project_fixture(&temp);
    let root = temp.path().join("repo");
    let manager = CodingAgentManager::with_store(&cfg, temp.path().join("store")).unwrap();
    let run = "wc_agent_run_reaptree0001";
    assert!(manager
        .handle(
            start_request(&manager, &root, run, BTreeMap::new()),
            &projects
        )
        .error
        .is_none());
    wait_for_snapshot(&manager, run, |snapshot| snapshot.state.terminal());
    let descendant = wire_log(&temp)
        .iter()
        .find_map(|entry| entry.get("descendant_pid").and_then(Value::as_u64))
        .unwrap();
    let proc_path = PathBuf::from(format!("/proc/{descendant}"));
    let deadline = Instant::now() + Duration::from_secs(3);
    while proc_path.exists() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        !proc_path.exists(),
        "ACP descendant process survived ManagedChild cleanup"
    );
}

#[test]
#[cfg(unix)]
fn post_barrier_crash_is_lost_and_restart_never_redispatches() {
    let (manager, run, obs) = run_scenario("crash_after_prompt", BTreeMap::new());
    assert_eq!(obs.run.state, CodingAgentRunState::Lost);
    let cfg = AcpConfig {
        max_concurrent_runs: manager.max_concurrent_runs,
        permission_timeout_secs: 1,
        forced_config: manager.forced_config.clone(),
        agents: manager
            .providers
            .values()
            .map(|p| p.config.clone())
            .collect(),
    };
    let restarted = CodingAgentManager::with_store(&cfg, manager.store.root.clone()).unwrap();
    assert_eq!(
        restarted
            .runs
            .lock()
            .unwrap()
            .get(&run)
            .unwrap()
            .snapshot()
            .state,
        CodingAgentRunState::Lost
    );
}

#[test]
#[cfg(unix)]
fn durable_record_contains_no_prompt_or_event_bodies() {
    let (manager, run, _) = run_scenario("end", BTreeMap::new());
    let bytes = fs::read(manager.store.state_path(&run)).unwrap();
    let text = String::from_utf8(bytes).unwrap();
    assert!(!text.contains("inspect"));
    assert!(!text.contains("hello"));
    assert!(!text.contains("thinking"));
}

#[test]
fn inventory_selection_prefers_active_then_newest_terminal() {
    let snapshots = vec![
        terminal_snapshot("wc_agent_run_old_terminal", 1),
        terminal_snapshot("wc_agent_run_new_terminal", 100),
        active_snapshot("wc_agent_run_active", 2),
    ];
    let ranked = test_select_bounded_inventory_runs(snapshots);
    assert_eq!(ranked[0].run_id, "wc_agent_run_active");
    assert_eq!(ranked[1].run_id, "wc_agent_run_new_terminal");
    assert_eq!(ranked[2].run_id, "wc_agent_run_old_terminal");
}

#[test]
fn retention_evictable_skips_active_and_uncertain_terminals() {
    assert!(!test_coding_agent_run_retention_evictable(
        &active_snapshot("wc_agent_run_active", 1)
    ));
    let mut lost = terminal_snapshot("wc_agent_run_lost", 1);
    lost.state = CodingAgentRunState::Lost;
    lost.execution_state = CodingAgentExecutionState::OutcomeUnknown;
    assert!(!test_coding_agent_run_retention_evictable(&lost));
    assert!(test_coding_agent_run_retention_evictable(
        &terminal_snapshot("wc_agent_run_completed", 1)
    ));
}

#[test]
fn cleanup_expired_archives_acknowledged_expired_hot_terminal() {
    let temp = TempDir::new().unwrap();
    let store = DurableRunStore::new(temp.path().join("store"));
    let cfg = AcpConfig {
        max_concurrent_runs: 4,
        permission_timeout_secs: 1,
        forced_config: BTreeMap::new(),
        agents: Vec::new(),
    };
    let manager = CodingAgentManager::with_store(&cfg, store.root.clone()).unwrap();
    let recent = terminal_snapshot("wc_agent_run_recent_terminal", now() - (10 * 60));
    manager.runs.lock().unwrap().insert(
        recent.run_id.clone(),
        Arc::new(RunEntry::new(recent.clone())),
    );
    store.write(&durable_terminal_record(&recent)).unwrap();
    let expired = terminal_snapshot(
        "wc_agent_run_expired_terminal",
        now() - STORE_RETENTION_SECS - 1,
    );
    manager.runs.lock().unwrap().insert(
        expired.run_id.clone(),
        Arc::new(RunEntry::new(expired.clone())),
    );
    store.write(&durable_terminal_record(&expired)).unwrap();
    manager.inventory();
    assert!(manager.runs.lock().unwrap().contains_key(&expired.run_id));
    manager.sync_hot_index_from_store_for_test();
    manager.cleanup_expired(&[
        webcodex_core::coding_agent::archive_identity_fingerprint(&recent),
        webcodex_core::coding_agent::archive_identity_fingerprint(&expired),
    ]);
    assert!(store.read_archived(&expired.run_id).unwrap().is_some());
    assert!(manager
        .runs
        .lock()
        .unwrap()
        .contains_key("wc_agent_run_recent_terminal"));
    assert!(!manager
        .runs
        .lock()
        .unwrap()
        .contains_key("wc_agent_run_expired_terminal"));
}

#[test]
fn cleanup_expired_retains_protected_terminal_past_twenty_four_hours() {
    let temp = TempDir::new().unwrap();
    let store = DurableRunStore::new(temp.path().join("store"));
    let cfg = AcpConfig {
        max_concurrent_runs: 4,
        permission_timeout_secs: 1,
        forced_config: BTreeMap::new(),
        agents: Vec::new(),
    };
    let manager = CodingAgentManager::with_store(&cfg, store.root.clone()).unwrap();
    let old_ts = now() - STORE_RETENTION_SECS - 1;
    let mut lost = terminal_snapshot("wc_agent_run_old_lost", old_ts);
    lost.state = CodingAgentRunState::Lost;
    lost.execution_state = CodingAgentExecutionState::OutcomeUnknown;
    manager
        .runs
        .lock()
        .unwrap()
        .insert(lost.run_id.clone(), Arc::new(RunEntry::new(lost.clone())));
    store.write(&durable_terminal_record(&lost)).unwrap();
    let mut uncertain = terminal_snapshot("wc_agent_run_old_uncertain", old_ts);
    uncertain.state = CodingAgentRunState::Failed;
    uncertain.execution_state = CodingAgentExecutionState::NotStarted;
    uncertain.terminal = Some(CodingAgentTerminal {
        stop_reason: None,
        error_code: Some(CODING_AGENT_TERMINAL_PERSISTENCE_UNCERTAIN.to_string()),
        message: Some("uncertain".to_string()),
        completed_at: old_ts,
    });
    manager.runs.lock().unwrap().insert(
        uncertain.run_id.clone(),
        Arc::new(RunEntry::new(uncertain.clone())),
    );
    store.write(&durable_terminal_record(&uncertain)).unwrap();
    manager.inventory();
    assert!(manager
        .runs
        .lock()
        .unwrap()
        .contains_key("wc_agent_run_old_lost"));
    assert!(manager
        .runs
        .lock()
        .unwrap()
        .contains_key("wc_agent_run_old_uncertain"));
}

#[test]
fn corrupt_tombstone_counts_toward_bounded_capacity_and_scan_fail_closed() {
    let temp = TempDir::new().unwrap();
    let store = DurableRunStore::new(temp.path().join("store"));
    for index in 0..CODING_AGENT_MAX_INVENTORY_RUNS {
        let run_id = format!("wc_agent_run_valid_{index:03}");
        store
            .write(&durable_terminal_record(&terminal_snapshot(
                &run_id,
                index as i64 + 1,
            )))
            .unwrap();
    }
    let corrupt_run = "wc_agent_run_corrupt_tombstone";
    let corrupt_path = store.state_path(corrupt_run);
    fs::create_dir_all(corrupt_path.parent().unwrap()).unwrap();
    fs::write(&corrupt_path, b"{corrupt-json").unwrap();
    let listing = store.list_records_for_retention().unwrap();
    assert_eq!(listing.entry_count, CODING_AGENT_MAX_INVENTORY_RUNS + 1);
    assert_eq!(listing.records.len(), CODING_AGENT_MAX_INVENTORY_RUNS);
    assert!(store.scan().is_err());
}

#[test]
fn bounded_retention_scan_fail_closed_beyond_recoverable_overflow() {
    let temp = TempDir::new().unwrap();
    let store = DurableRunStore::new(temp.path().join("store"));
    for index in 0..CODING_AGENT_MAX_INVENTORY_RUNS + 2 {
        let run_id = format!("wc_agent_run_overflow_{index:03}");
        fs::create_dir_all(store.state_path(&run_id).parent().unwrap()).unwrap();
    }
    assert!(store.list_records_for_retention().is_err());
}

#[test]
fn enforce_durable_capacity_evicts_oldest_terminal_before_new_claim() {
    let temp = TempDir::new().unwrap();
    let store = DurableRunStore::new(temp.path().join("store"));
    let cfg = AcpConfig {
        max_concurrent_runs: 4,
        permission_timeout_secs: 1,
        forced_config: BTreeMap::new(),
        agents: Vec::new(),
    };
    let manager = CodingAgentManager::with_store(&cfg, store.root.clone()).unwrap();
    for index in 0..CODING_AGENT_MAX_INVENTORY_RUNS {
        let run_id = format!("wc_agent_run_capacity_{index:03}");
        let snapshot = terminal_snapshot(&run_id, index as i64 + 1);
        store.write(&durable_terminal_record(&snapshot)).unwrap();
        manager
            .runs
            .lock()
            .unwrap()
            .insert(run_id.clone(), Arc::new(RunEntry::new(snapshot)));
    }
    manager
        .enforce_durable_capacity_for_admission_for_test(Some("wc_agent_run_new_admission"))
        .unwrap();
    assert_eq!(
        store.list_records_for_retention().unwrap().entry_count,
        CODING_AGENT_MAX_INVENTORY_RUNS - 1
    );
    assert!(store.read_archived("wc_agent_run_capacity_000").unwrap().is_some());
    assert!(store.read("wc_agent_run_capacity_127").unwrap().is_some());
}

#[test]
fn enforce_durable_capacity_archives_uncertain_without_losing_replay_identity() {
    let temp = TempDir::new().unwrap();
    let store = DurableRunStore::new(temp.path().join("store"));
    let cfg = AcpConfig {
        max_concurrent_runs: 4,
        permission_timeout_secs: 1,
        forced_config: BTreeMap::new(),
        agents: Vec::new(),
    };
    let manager = CodingAgentManager::with_store(&cfg, store.root.clone()).unwrap();
    let active = active_snapshot("wc_agent_run_active_only", 1);
    store.write(&durable_active_record(&active)).unwrap();
    manager
        .runs
        .lock()
        .unwrap()
        .insert(active.run_id.clone(), Arc::new(RunEntry::new(active)));
    for index in 0..CODING_AGENT_MAX_INVENTORY_RUNS {
        let run_id = format!("wc_agent_run_uncertain_{index:03}");
        let mut lost = terminal_snapshot(&run_id, index as i64 + 1);
        lost.state = CodingAgentRunState::Lost;
        lost.execution_state = CodingAgentExecutionState::OutcomeUnknown;
        lost.terminal = Some(CodingAgentTerminal {
            stop_reason: None,
            error_code: Some("runner_restart_uncertain".to_string()),
            message: None,
            completed_at: lost.updated_at,
        });
        store.write(&durable_terminal_record(&lost)).unwrap();
        manager
            .runs
            .lock()
            .unwrap()
            .insert(run_id.clone(), Arc::new(RunEntry::new(lost)));
    }
    manager.sync_hot_index_from_store_for_test();
    // Without exact durable Server acknowledgements no uncertain identity may leave inventory.
    assert!(manager
        .enforce_durable_capacity_for_admission(None, &[])
        .is_err());
    let acknowledgements = (0..2)
        .map(|index| {
            let id = format!("wc_agent_run_uncertain_{index:03}");
            let run = store.read(&id).unwrap().unwrap().snapshot(0);
            webcodex_core::coding_agent::archive_identity_fingerprint(&run)
        })
        .collect::<Vec<_>>();
    manager.sync_hot_index_from_store_for_test();
    manager
        .enforce_durable_capacity_for_admission(None, &acknowledgements)
        .unwrap();
    assert!(store.archive_required().unwrap());
    assert_eq!(
        store.list_records_for_retention().unwrap().entry_count,
        CODING_AGENT_MAX_INVENTORY_RUNS - 1
    );
    assert!(store.read("wc_agent_run_active_only").unwrap().is_some());
    for index in 0..2 {
        let id = format!("wc_agent_run_uncertain_{index:03}");
        assert!(!manager.runs.lock().unwrap().contains_key(&id));
        let archived = store.read_archived(&id).unwrap().unwrap();
        assert_eq!(archived.state, CodingAgentRunState::Lost);
        assert_eq!(
            store.read(&id).unwrap().unwrap().snapshot(0),
            archived.snapshot(0)
        );
    }
    drop(manager);
    let restarted = CodingAgentManager::with_store(&cfg, store.root.clone()).unwrap();
    let id = "wc_agent_run_uncertain_000";
    assert!(!restarted.runs.lock().unwrap().contains_key(id));
    assert_eq!(
        restarted.store.read(id).unwrap().unwrap().state,
        CodingAgentRunState::Lost
    );
}

#[test]
fn enforce_durable_capacity_skips_when_durable_intent_fingerprint_mismatches_live() {
    let temp = TempDir::new().unwrap();
    let store = DurableRunStore::new(temp.path().join("store"));
    let cfg = AcpConfig {
        max_concurrent_runs: 4,
        permission_timeout_secs: 1,
        forced_config: BTreeMap::new(),
        agents: Vec::new(),
    };
    let manager = CodingAgentManager::with_store(&cfg, store.root.clone()).unwrap();
    let mismatch_id = "wc_agent_run_intent_mismatch01";
    let live = terminal_snapshot(mismatch_id, 1);
    let mut durable = durable_terminal_record(&live);
    durable.intent_fingerprint = "stale_intent_fingerprint".to_string();
    store.write(&durable).unwrap();
    manager
        .runs
        .lock()
        .unwrap()
        .insert(mismatch_id.to_string(), Arc::new(RunEntry::new(live)));
    for index in 1..CODING_AGENT_MAX_INVENTORY_RUNS {
        let run_id = format!("wc_agent_run_capacity_{index:03}");
        let snapshot = terminal_snapshot(&run_id, index as i64 + 1);
        store.write(&durable_terminal_record(&snapshot)).unwrap();
        manager
            .runs
            .lock()
            .unwrap()
            .insert(run_id.clone(), Arc::new(RunEntry::new(snapshot)));
    }
    manager
        .enforce_durable_capacity_for_admission_for_test(Some("wc_agent_run_new_admission"))
        .unwrap();
    assert!(store.read(mismatch_id).unwrap().is_some());
    assert!(manager.runs.lock().unwrap().contains_key(mismatch_id));
    assert_eq!(
        store.list_records_for_retention().unwrap().entry_count,
        CODING_AGENT_MAX_INVENTORY_RUNS - 1
    );
}

#[test]
fn enforce_durable_capacity_skips_when_durable_provider_instance_mismatches_live() {
    let temp = TempDir::new().unwrap();
    let store = DurableRunStore::new(temp.path().join("store"));
    let cfg = AcpConfig {
        max_concurrent_runs: 4,
        permission_timeout_secs: 1,
        forced_config: BTreeMap::new(),
        agents: Vec::new(),
    };
    let manager = CodingAgentManager::with_store(&cfg, store.root.clone()).unwrap();
    let mismatch_id = "wc_agent_run_provider_mismatch01";
    let live = terminal_snapshot(mismatch_id, 1);
    let mut durable = durable_terminal_record(&live);
    durable.provider_instance_id = "stale_acp_instance".to_string();
    store.write(&durable).unwrap();
    manager
        .runs
        .lock()
        .unwrap()
        .insert(mismatch_id.to_string(), Arc::new(RunEntry::new(live)));
    for index in 1..CODING_AGENT_MAX_INVENTORY_RUNS {
        let run_id = format!("wc_agent_run_capacity_{index:03}");
        let snapshot = terminal_snapshot(&run_id, index as i64 + 1);
        store.write(&durable_terminal_record(&snapshot)).unwrap();
        manager
            .runs
            .lock()
            .unwrap()
            .insert(run_id.clone(), Arc::new(RunEntry::new(snapshot)));
    }
    manager
        .enforce_durable_capacity_for_admission_for_test(Some("wc_agent_run_new_admission"))
        .unwrap();
    assert!(store.read(mismatch_id).unwrap().is_some());
    assert!(manager.runs.lock().unwrap().contains_key(mismatch_id));
    assert_eq!(
        store.list_records_for_retention().unwrap().entry_count,
        CODING_AGENT_MAX_INVENTORY_RUNS - 1
    );
}

#[test]
fn enforce_durable_capacity_skips_candidate_when_durable_terminal_precedes_live_publish() {
    let temp = TempDir::new().unwrap();
    let store = DurableRunStore::new(temp.path().join("store"));
    let cfg = AcpConfig {
        max_concurrent_runs: 4,
        permission_timeout_secs: 1,
        forced_config: BTreeMap::new(),
        agents: Vec::new(),
    };
    let manager = CodingAgentManager::with_store(&cfg, store.root.clone()).unwrap();
    let mismatch_id = "wc_agent_run_capacity_000";
    let durable_only = terminal_snapshot(mismatch_id, 1);
    store
        .write(&durable_terminal_record(&durable_only))
        .unwrap();
    manager.runs.lock().unwrap().insert(
        mismatch_id.to_string(),
        Arc::new(RunEntry::new(active_snapshot(mismatch_id, 1))),
    );
    for index in 1..CODING_AGENT_MAX_INVENTORY_RUNS {
        let run_id = format!("wc_agent_run_capacity_{index:03}");
        let snapshot = terminal_snapshot(&run_id, index as i64 + 1);
        store.write(&durable_terminal_record(&snapshot)).unwrap();
        manager
            .runs
            .lock()
            .unwrap()
            .insert(run_id.clone(), Arc::new(RunEntry::new(snapshot)));
    }
    manager
        .enforce_durable_capacity_for_admission_for_test(Some("wc_agent_run_new_admission"))
        .unwrap();
    assert!(store.read(mismatch_id).unwrap().is_some());
    assert!(manager.runs.lock().unwrap().contains_key(mismatch_id));
    assert_eq!(
        store.list_records_for_retention().unwrap().entry_count,
        CODING_AGENT_MAX_INVENTORY_RUNS - 1
    );
}

#[test]
fn cleanup_expired_rechecks_evictable_before_removal_under_terminal_transition() {
    let temp = TempDir::new().unwrap();
    let store = DurableRunStore::new(temp.path().join("store"));
    let cfg = AcpConfig {
        max_concurrent_runs: 4,
        permission_timeout_secs: 1,
        forced_config: BTreeMap::new(),
        agents: Vec::new(),
    };
    let manager = CodingAgentManager::with_store(&cfg, store.root.clone()).unwrap();
    let cutoff = now().saturating_sub(STORE_RETENTION_SECS);
    let old_ts = now() - STORE_RETENTION_SECS - 1;
    let run = "wc_agent_run_cleanup_recheck01";
    let completed = terminal_snapshot(run, old_ts);
    let entry = Arc::new(RunEntry::new(completed.clone()));
    manager
        .runs
        .lock()
        .unwrap()
        .insert(run.to_string(), Arc::clone(&entry));
    store.write(&durable_terminal_record(&completed)).unwrap();

    assert!(test_coding_agent_run_retention_evictable(&completed) && completed.updated_at < cutoff);
    let stale_candidates = vec![run.to_string()];
    let receipt = webcodex_core::coding_agent::archive_identity_fingerprint(&completed);

    let _guard = entry.terminal_transition.lock().unwrap();
    let mut lost = completed;
    lost.state = CodingAgentRunState::Lost;
    lost.execution_state = CodingAgentExecutionState::OutcomeUnknown;
    lost.terminal = Some(CodingAgentTerminal {
        stop_reason: None,
        error_code: Some("runner_restart_uncertain".to_string()),
        message: Some("protected after cleanup candidate selection".to_string()),
        completed_at: old_ts,
    });
    entry.publish_terminal(
        lost,
        CodingAgentEvent {
            sequence: 0,
            kind: CodingAgentEventKind::Terminal,
            text: Some("protected after cleanup candidate selection".to_string()),
            label: Some("runner_restart_uncertain".to_string()),
            status: Some("lost".to_string()),
            usage: None,
        },
    );
    drop(_guard);

    manager.cleanup_expired_stale_candidates_for_test(stale_candidates, &[receipt]);

    assert!(manager.runs.lock().unwrap().contains_key(run));
    assert!(store.read(run).unwrap().is_some());
}

#[test]
fn durable_store_remove_checked_not_found_is_success() {
    let temp = TempDir::new().unwrap();
    let store = DurableRunStore::new(temp.path().join("store"));
    assert!(store.remove_checked("wc_agent_run_missing_dir").is_ok());
}

#[test]
fn enforce_durable_capacity_fails_closed_without_durable_receipts() {
    let temp = TempDir::new().unwrap();
    let store = DurableRunStore::new(temp.path().join("store"));
    let cfg = AcpConfig {
        max_concurrent_runs: 4,
        permission_timeout_secs: 1,
        forced_config: BTreeMap::new(),
        agents: Vec::new(),
    };
    let manager = CodingAgentManager::with_store(&cfg, store.root.clone()).unwrap();
    for index in 0..CODING_AGENT_MAX_INVENTORY_RUNS {
        let run_id = format!("wc_agent_run_capacity_{index:03}");
        let snapshot = terminal_snapshot(&run_id, index as i64 + 1);
        store.write(&durable_terminal_record(&snapshot)).unwrap();
        manager
            .runs
            .lock()
            .unwrap()
            .insert(run_id.clone(), Arc::new(RunEntry::new(snapshot)));
    }
    manager.sync_hot_index_from_store_for_test();
    let error = manager
        .enforce_durable_capacity_for_admission(Some("wc_agent_run_new_admission"), &[])
        .unwrap_err();
    assert!(error.contains("without durable Server archive acknowledgement"));
    assert_eq!(
        store.list_records_for_retention().unwrap().entry_count,
        CODING_AGENT_MAX_INVENTORY_RUNS
    );
    assert!(store.read("wc_agent_run_capacity_000").unwrap().is_some());
}

#[test]
fn cleanup_expired_retains_run_without_durable_receipt() {
    let temp = TempDir::new().unwrap();
    let store = DurableRunStore::new(temp.path().join("store"));
    let cfg = AcpConfig {
        max_concurrent_runs: 4,
        permission_timeout_secs: 1,
        forced_config: BTreeMap::new(),
        agents: Vec::new(),
    };
    let manager = CodingAgentManager::with_store(&cfg, store.root.clone()).unwrap();
    let old_ts = now() - STORE_RETENTION_SECS - 1;
    let run = "wc_agent_run_cleanup_remove_fail";
    let completed = terminal_snapshot(run, old_ts);
    manager
        .runs
        .lock()
        .unwrap()
        .insert(run.to_string(), Arc::new(RunEntry::new(completed.clone())));
    store.write(&durable_terminal_record(&completed)).unwrap();
    manager.cleanup_expired_for_test();
    assert!(manager.runs.lock().unwrap().contains_key(run));
    assert!(store.read(run).unwrap().is_some());
}

fn terminal_snapshot(run_id: &str, updated_at: i64) -> CodingAgentRunSnapshot {
    CodingAgentRunSnapshot {
        run_id: run_id.to_string(),
        intent_fingerprint: "fingerprint".to_string(),
        authority_fingerprint: "auth_test".to_string(),
        runtime_project_id: "agent:test:demo".to_string(),
        provider_id: "codex".to_string(),
        provider_instance_id: "acp_test".to_string(),
        state: CodingAgentRunState::Completed,
        execution_state: CodingAgentExecutionState::Completed,
        observation_revision: 0,
        replay_window: None,
        created_at: updated_at,
        updated_at,
        terminal: Some(CodingAgentTerminal {
            stop_reason: Some(CODING_AGENT_STOP_REASON_END_TURN.to_string()),
            error_code: None,
            message: None,
            completed_at: updated_at,
        }),
        pending_permission: None,
        live_confirmed_effective_config: None,
    }
}

fn active_snapshot(run_id: &str, updated_at: i64) -> CodingAgentRunSnapshot {
    CodingAgentRunSnapshot {
        run_id: run_id.to_string(),
        intent_fingerprint: "fingerprint".to_string(),
        authority_fingerprint: "auth_test".to_string(),
        runtime_project_id: "agent:test:demo".to_string(),
        provider_id: "codex".to_string(),
        provider_instance_id: "acp_test".to_string(),
        state: CodingAgentRunState::Running,
        execution_state: CodingAgentExecutionState::OutcomeUnknown,
        observation_revision: 0,
        replay_window: None,
        created_at: updated_at,
        updated_at,
        terminal: None,
        pending_permission: None,
        live_confirmed_effective_config: None,
    }
}

fn durable_terminal_record(snapshot: &CodingAgentRunSnapshot) -> DurableRunRecord {
    DurableRunRecord {
        schema_version: STORE_SCHEMA_VERSION,
        run_id: snapshot.run_id.clone(),
        intent_fingerprint: snapshot.intent_fingerprint.clone(),
        authority_fingerprint: snapshot.authority_fingerprint.clone(),
        runtime_project_id: snapshot.runtime_project_id.clone(),
        provider_id: snapshot.provider_id.clone(),
        provider_instance_id: snapshot.provider_instance_id.clone(),
        state: snapshot.state.clone(),
        execution_state: snapshot.execution_state,
        dispatch_phase: DurableDispatchPhase::Terminal,
        replay_window: snapshot.replay_window,
        created_at: snapshot.created_at,
        updated_at: snapshot.updated_at,
        terminal: snapshot.terminal.clone(),
    }
}

fn durable_active_record(snapshot: &CodingAgentRunSnapshot) -> DurableRunRecord {
    DurableRunRecord {
        schema_version: STORE_SCHEMA_VERSION,
        run_id: snapshot.run_id.clone(),
        intent_fingerprint: snapshot.intent_fingerprint.clone(),
        authority_fingerprint: snapshot.authority_fingerprint.clone(),
        runtime_project_id: snapshot.runtime_project_id.clone(),
        provider_id: snapshot.provider_id.clone(),
        provider_instance_id: snapshot.provider_instance_id.clone(),
        state: snapshot.state.clone(),
        execution_state: snapshot.execution_state,
        dispatch_phase: DurableDispatchPhase::BeforePromptBarrier,
        replay_window: snapshot.replay_window,
        created_at: snapshot.created_at,
        updated_at: snapshot.updated_at,
        terminal: None,
    }
}
