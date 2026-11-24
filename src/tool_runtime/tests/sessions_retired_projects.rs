use super::support::*;
use crate::runner_protocol::{RunnerCapabilities, RunnerProjectSummary};
use crate::tool_runtime::sessions::{SessionMessageKind, SessionMessagePriority};
use crate::tool_runtime::{ToolCall, ToolRuntime};
use std::sync::Arc;
use webcodex_core::workflow_session_contract::SessionMode;
use webcodex_workflow_session::SessionLifecycle;

const RETIRED_PROJECT_INCARNATION: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
const INCARNATION_ONE: &str = "11111111-1111-1111-1111-111111111111";
const INCARNATION_TWO: &str = "22222222-2222-2222-2222-222222222222";

fn prefixed_root_fingerprint(hex_digest: &str) -> String {
    format!("wc_projroot_{hex_digest}")
}

fn project_summary(
    client: &str,
    id: &str,
    path: &str,
    incarnation: &str,
    root_fingerprint: &str,
    disabled: bool,
) -> RunnerProjectSummary {
    RunnerProjectSummary {
        id: id.to_string(),
        name: Some(id.to_string()),
        path: path.to_string(),
        allow_patch: true,
        kind: Some("repo".to_string()),
        registration_source: None,
        registration_incarnation: Some(incarnation.to_string()),
        description: None,
        hooks: Vec::new(),
        disabled,
        revision: None,
        root_fingerprint: Some(prefixed_root_fingerprint(root_fingerprint)),
        lineage: None,
        git_branch: None,
        git_head: None,
        git_dirty: None,
        updated_at: 1,
        shell_profile: None,
    }
}

#[tokio::test]
async fn retired_project_allows_owner_read_and_close_after_unregister() {
    let dir = tempfile::tempdir().unwrap();
    let ledger = dir.path().join("sessions.json");
    let runtime = ToolRuntime::new_for_tests_with_runner_registry(Arc::new(
        crate::runner_http::RunnerRegistry::with_shared_key_limits_for_test(4, 8, 1),
    ))
    .with_session_ledger(&ledger);
    let auth = shared_key_auth_context("retired-owner");
    let client = "retired-client";
    let project_id = "demo";
    let path = dir.path().to_string_lossy().to_string();
    let root_fp = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let project_summary = project_summary(
        client,
        project_id,
        &path,
        RETIRED_PROJECT_INCARNATION,
        root_fp,
        false,
    );
    register_agent_projects_for_auth(
        &runtime,
        client,
        &auth,
        RunnerCapabilities::default(),
        vec![project_summary],
    )
    .await;
    let project = format!("agent:{client}:{project_id}");
    let started = runtime
        .dispatch_with_auth(
            ToolCall::StartSession {
                project: Some(project.clone()),
                title: Some("retired session".into()),
                mode: SessionMode::Normal,
                deny_write_tools: false,
                deny_shell_tools: false,
                execution_context: None,
            },
            Some(&auth),
        )
        .await;
    assert!(started.success, "{:?}", started.error);
    let session_id = started.output["session_id"].as_str().unwrap().to_string();

    register_agent_projects_for_auth(
        &runtime,
        client,
        &auth,
        RunnerCapabilities::default(),
        vec![],
    )
    .await;

    let read = runtime
        .dispatch_with_auth(
            ToolCall::ReadSessionSummary {
                session_id: session_id.clone(),
                limit: None,
            },
            Some(&auth),
        )
        .await;
    assert!(read.success, "{:?}", read.output);

    let close = runtime
        .dispatch_with_auth(
            ToolCall::CloseSession {
                session_id: session_id.clone(),
            },
            Some(&auth),
        )
        .await;
    assert!(close.success, "{:?}", close.output);
    runtime.sessions.flush_persistence();
    let reopened = ToolRuntime::new_for_tests().with_session_ledger(&ledger);
    assert!(reopened.sessions.summary(&session_id, Some(20)).is_some());
}

#[tokio::test]
async fn disabled_project_still_registered_denies_retired_read_and_close() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = ToolRuntime::new_for_tests_with_runner_registry(Arc::new(
        crate::runner_http::RunnerRegistry::with_shared_key_limits_for_test(4, 8, 1),
    ));
    let auth = shared_key_auth_context("disabled-still-registered");
    let client = "disabled-retired-client";
    let project_id = "demo";
    let path = dir.path().to_string_lossy().to_string();
    let root_fp = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    let enabled = project_summary(
        client,
        project_id,
        &path,
        RETIRED_PROJECT_INCARNATION,
        root_fp,
        false,
    );
    register_agent_projects_for_auth(
        &runtime,
        client,
        &auth,
        RunnerCapabilities::default(),
        vec![enabled],
    )
    .await;
    let project = format!("agent:{client}:{project_id}");
    let started = runtime
        .dispatch_with_auth(
            ToolCall::StartSession {
                project: Some(project.clone()),
                title: Some("disabled fence".into()),
                mode: SessionMode::Normal,
                deny_write_tools: false,
                deny_shell_tools: false,
                execution_context: None,
            },
            Some(&auth),
        )
        .await;
    assert!(started.success, "{:?}", started.error);
    let session_id = started.output["session_id"].as_str().unwrap().to_string();
    assert_eq!(
        runtime.sessions.lifecycle_state(&session_id),
        Some(SessionLifecycle::Active)
    );

    let disabled = project_summary(
        client,
        project_id,
        &path,
        RETIRED_PROJECT_INCARNATION,
        root_fp,
        true,
    );
    register_agent_projects_for_auth(
        &runtime,
        client,
        &auth,
        RunnerCapabilities::default(),
        vec![disabled],
    )
    .await;

    let read = runtime
        .dispatch_with_auth(
            ToolCall::ReadSessionSummary {
                session_id: session_id.clone(),
                limit: None,
            },
            Some(&auth),
        )
        .await;
    assert!(!read.success);
    assert_eq!(read.output["error_kind"], "unknown_project");

    let close = runtime
        .dispatch_with_auth(
            ToolCall::CloseSession {
                session_id: session_id.clone(),
            },
            Some(&auth),
        )
        .await;
    assert!(!close.success);
    assert_eq!(close.output["error_kind"], "unknown_project");
    assert_eq!(
        runtime.sessions.lifecycle_state(&session_id),
        Some(SessionLifecycle::Active)
    );
}

#[tokio::test]
async fn same_project_id_new_incarnation_rejects_live_session_mutation() {
    let runtime = ToolRuntime::new_for_tests_with_runner_registry(Arc::new(
        crate::runner_http::RunnerRegistry::with_shared_key_limits_for_test(4, 8, 1),
    ));
    let auth = shared_key_auth_context("incarnation-fence");
    let client = "inc-client";
    let project_id = "demo";
    let path = "/tmp/inc-demo";
    let root_fp = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let project_summary = project_summary(client, project_id, path, INCARNATION_ONE, root_fp, false);
    register_agent_projects_for_auth(
        &runtime,
        client,
        &auth,
        RunnerCapabilities::default(),
        vec![project_summary],
    )
    .await;
    let project = format!("agent:{client}:{project_id}");
    let started = runtime
        .dispatch_with_auth(
            ToolCall::StartSession {
                project: Some(project.clone()),
                title: None,
                mode: SessionMode::Normal,
                deny_write_tools: false,
                deny_shell_tools: false,
                execution_context: None,
            },
            Some(&auth),
        )
        .await;
    assert!(started.success);
    let session_id = started.output["session_id"].as_str().unwrap().to_string();

    let recycled = project_summary(client, project_id, path, INCARNATION_TWO, root_fp, false);
    register_agent_projects_for_auth(
        &runtime,
        client,
        &auth,
        RunnerCapabilities::default(),
        vec![recycled],
    )
    .await;

    let denied = runtime
        .dispatch_with_auth(
            ToolCall::PostSessionMessage {
                session_id: session_id.clone(),
                kind: SessionMessageKind::Note,
                message: "must not post".into(),
                tags: Vec::new(),
                reply_to: None,
                priority: SessionMessagePriority::Normal,
                requires_ack: false,
                delivery_key: None,
            },
            Some(&auth),
        )
        .await;
    assert!(!denied.success);
    assert_eq!(
        denied.output["error_kind"],
        "session_project_identity_changed"
    );
}

#[tokio::test]
async fn project_session_with_incarnation_but_missing_root_fingerprint_fails_start() {
    let runtime = ToolRuntime::new_for_tests_with_runner_registry(Arc::new(
        crate::runner_http::RunnerRegistry::with_shared_key_limits_for_test(4, 8, 1),
    ));
    let auth = shared_key_auth_context("missing-root-fingerprint");
    let client = "no-root-client";
    let mut summary = named_registered_project(client, "demo", "demo", "/tmp/no-root", 1);
    summary.registration_incarnation = Some(INCARNATION_ONE.into());
    let runner_instance_id = format!("inst-{client}");
    runtime
        .runner_registry
        .register_with_auth(
            crate::test_support::runner_registration(
                client,
                &runner_instance_id,
                RunnerCapabilities::default(),
            ),
            Some(&crate::test_support::runner_access(&auth)),
        )
        .await
        .unwrap();
    crate::test_support::apply_project_inventory_snapshot_unmodified(
        &runtime.runner_registry,
        client,
        &runner_instance_id,
        vec![summary],
    )
    .await;
    let denied = runtime
        .dispatch_with_auth(
            ToolCall::StartSession {
                project: Some(format!("agent:{client}:demo")),
                title: None,
                mode: SessionMode::Normal,
                deny_write_tools: false,
                deny_shell_tools: false,
                execution_context: None,
            },
            Some(&auth),
        )
        .await;
    assert!(!denied.success);
    assert_eq!(
        denied.output["error_kind"],
        "project_root_fingerprint_unavailable"
    );
    assert_eq!(denied.output["failure_kind"], "project_identity_unavailable");
    assert_eq!(denied.output["state_changed"], false);
}

#[tokio::test]
async fn project_session_without_registration_incarnation_fails_closed_on_start() {
    let runtime = ToolRuntime::new_for_tests_with_runner_registry(Arc::new(
        crate::runner_http::RunnerRegistry::with_shared_key_limits_for_test(4, 8, 1),
    ));
    let auth = shared_key_auth_context("legacy-incarnation");
    let client = "legacy-client";
    let mut summary = named_registered_project(client, "demo", "demo", "/tmp/legacy", 1);
    summary.registration_incarnation = None;
    let runner_instance_id = format!("inst-{client}");
    runtime
        .runner_registry
        .register_with_auth(
            crate::test_support::runner_registration(client, &runner_instance_id, RunnerCapabilities::default()),
            Some(&crate::test_support::runner_access(&auth)),
        )
        .await
        .unwrap();
    crate::test_support::apply_project_inventory_snapshot_unmodified(
        &runtime.runner_registry,
        client,
        &runner_instance_id,
        vec![summary],
    )
    .await;
    let denied = runtime
        .dispatch_with_auth(
            ToolCall::StartSession {
                project: Some(format!("agent:{client}:demo")),
                title: None,
                mode: SessionMode::Normal,
                deny_write_tools: false,
                deny_shell_tools: false,
                execution_context: None,
            },
            Some(&auth),
        )
        .await;
    assert!(!denied.success);
    assert_eq!(
        denied.output["error_kind"],
        "project_registration_incarnation_unavailable"
    );
}
