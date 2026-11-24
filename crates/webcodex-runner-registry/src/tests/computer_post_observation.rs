use super::*;

#[tokio::test]
async fn computer_post_observation_fence_rejects_wrong_instance_at_admission() {
    let registry = RunnerRegistry::default();
    register_computer_test_runner(&registry, "computer", "alice", true, true, false, false).await;
    let access = auth_context(Some("alice"), false);
    let error = registry
        .enqueue_computer_for_instance(
            "computer".into(),
            "computer_control",
            "{}".into(),
            "alice".into(),
            Some(&access),
            5,
            "old-instance",
        )
        .await
        .unwrap_err();
    assert!(error.starts_with("runner_replaced:"));
}

#[tokio::test]
async fn computer_post_observation_fence_rechecks_access_and_capability_at_dequeue() {
    for change_owner in [true, false] {
        let registry = RunnerRegistry::default();
        let mut registration = runner_registration("computer", "instance", Vec::new());
        registration.owner = Some("alice".into());
        registration.capabilities.computer_control = true;
        registration.capabilities.computer_accessibility_observe = true;
        registry
            .register(current_runner_registration(registration.clone()))
            .await
            .unwrap();
        let access = auth_context(Some("alice"), false);
        let (_, rx) = registry
            .enqueue_computer_for_instance(
                "computer".into(),
                "computer_control",
                "{}".into(),
                "alice".into(),
                Some(&access),
                5,
                "instance",
            )
            .await
            .unwrap();
        if change_owner {
            registration.owner = Some("bob".into());
        } else {
            registration.capabilities.computer_accessibility_observe = false;
        }
        registry
            .register(current_runner_registration(registration))
            .await
            .unwrap();
        assert!(registry
            .poll(RunnerPollRequest {
                client_id: "computer".into(),
                runner_instance_id: "instance".into()
            })
            .await
            .unwrap()
            .is_none());
        let result = rx.await.unwrap();
        assert_eq!(result.request_dispatched, Some(false));
        assert!(result.error.unwrap().starts_with(if change_owner {
            "client_access_denied:"
        } else {
            "capability_unavailable:"
        }));
    }
}
