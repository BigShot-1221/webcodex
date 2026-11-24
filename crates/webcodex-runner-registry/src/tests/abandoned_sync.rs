use super::*;

#[tokio::test]
async fn computer_poll_skips_observation_with_closed_waiter() {
    let registry = RunnerRegistry::default();
    let mut registration = runner_registration("computer", "computer-instance", Vec::new());
    registration.owner = Some("alice".into());
    registration.capabilities.computer_observe = true;
    let access = auth_context(Some("alice"), false);
    registry
        .register(current_runner_registration(registration))
        .await
        .unwrap();
    let (abandoned_id, abandoned_rx) = registry
        .enqueue_computer(
            "computer".into(),
            "computer_list_windows",
            r#"{"limit":1}"#.into(),
            "tester".into(),
            Some(&access),
            5,
        )
        .await
        .unwrap();
    let (live_id, live_rx) = registry
        .enqueue_computer(
            "computer".into(),
            "computer_list_windows",
            r#"{"limit":1}"#.into(),
            "tester".into(),
            Some(&access),
            5,
        )
        .await
        .unwrap();
    register_computer_test_runner(&registry, "other", "bob", true, false, false, false).await;
    let other_access = auth_context(Some("bob"), false);
    let (other_id, other_rx) = registry
        .enqueue_computer(
            "other".into(),
            "computer_list_windows",
            r#"{"limit":1}"#.into(),
            "bob".into(),
            Some(&other_access),
            5,
        )
        .await
        .unwrap();
    drop(abandoned_rx);
    let dispatched = registry
        .poll(RunnerPollRequest {
            client_id: "computer".into(),
            runner_instance_id: "computer-instance".into(),
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(dispatched.request_id, live_id);
    assert_eq!(
        registry.cancel_request_dispatch_state(&abandoned_id).await,
        None
    );
    assert_eq!(
        registry.cancel_request_dispatch_state(&live_id).await,
        Some(true)
    );
    assert_eq!(
        registry
            .poll(RunnerPollRequest {
                client_id: "other".into(),
                runner_instance_id: "computer-inst".into(),
            })
            .await
            .unwrap()
            .unwrap()
            .request_id,
        other_id
    );
    drop(live_rx);
    drop(other_rx);
}

#[tokio::test]
async fn computer_poll_never_dispatches_abandoned_control_requests() {
    let registry = RunnerRegistry::default();
    register_computer_test_runner(&registry, "computer", "alice", true, true, true, false).await;
    let access = auth_context(Some("alice"), false);
    for _ in 0..2 {
        let (_, rx) = registry
            .enqueue_computer(
                "computer".into(),
                "computer_control",
                r#"{"surface_id":"surface_test","element_id":"element_test","action":"press"}"#
                    .into(),
                "alice".into(),
                Some(&access),
                5,
            )
            .await
            .unwrap();
        drop(rx);
    }
    assert!(registry
        .poll(RunnerPollRequest {
            client_id: "computer".into(),
            runner_instance_id: "computer-inst".into(),
        })
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        registry
            .get_runner_view("computer")
            .await
            .unwrap()
            .pending_requests,
        0
    );
}

#[tokio::test]
async fn computer_closed_waiter_after_dispatch_preserves_dispatch_evidence() {
    let registry = RunnerRegistry::default();
    let mut registration = runner_registration("computer", "computer-instance", Vec::new());
    registration.owner = Some("alice".into());
    registration.capabilities.computer_observe = true;
    let access = auth_context(Some("alice"), false);
    registry
        .register(current_runner_registration(registration))
        .await
        .unwrap();
    let (id, rx) = registry
        .enqueue_computer(
            "computer".into(),
            "computer_list_windows",
            r#"{"limit":1}"#.into(),
            "tester".into(),
            Some(&access),
            5,
        )
        .await
        .unwrap();
    let poll = RunnerPollRequest {
        client_id: "computer".into(),
        runner_instance_id: "computer-instance".into(),
    };
    assert_eq!(
        registry
            .poll(poll.clone())
            .await
            .unwrap()
            .unwrap()
            .request_id,
        id
    );
    drop(rx);
    assert!(registry.poll(poll).await.unwrap().is_none());
    assert_eq!(
        registry.cancel_request_dispatch_state(&id).await,
        Some(true)
    );
}

#[tokio::test]
async fn abandoned_sync_cleanup_removes_only_closed_waiters() {
    let registry = RunnerRegistry::default();
    register_quic_v1_runner(&registry, "oe").await;
    let (abandoned_id, abandoned_rx) = registry
        .enqueue_file_op(file_request("read"), "tester".to_string())
        .await
        .unwrap();
    let (live_id, live_rx) = registry
        .enqueue_file_op(file_request("read"), "tester".to_string())
        .await
        .unwrap();
    drop(abandoned_rx);

    assert_eq!(registry.cancel_abandoned_sync_requests().await, 1);
    assert_eq!(
        registry
            .get_runner_view("oe")
            .await
            .unwrap()
            .pending_requests,
        1
    );
    assert!(
        !registry.cancel_request(&abandoned_id).await,
        "closed-waiter request should already be removed"
    );
    assert_eq!(
        registry.cancel_request_dispatch_state(&live_id).await,
        Some(false),
        "cleanup must preserve an undispatched synchronous request with a live receiver"
    );
    drop(live_rx);
}
