use super::*;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;
use webcodex_environment::{
    current_account, embedded_tunnel_profiles, write_embedded_tunnel_observation, EmbeddedTunnelProfile,
    EnvironmentMode, EnvironmentRecord, EnvironmentStore, NativeEnvironment, RuntimeBinaries,
    Secret, ServiceScope, SetupRequest, TunnelCredentials, TunnelHostMode, TunnelState,
    ENVIRONMENT_SCHEMA,
};

fn profile(readiness_path: PathBuf) -> EmbeddedTunnelProfile {
    EmbeddedTunnelProfile {
        profile_id: "work".into(),
        autostart: true,
        runtime_revision: 3,
        credentials: TunnelCredentials {
            tunnel_id: Secret::new("tunnel-work".into()),
            api_key: Secret::new("api-key".into()),
        },
        local_token: Secret::new("local-token".into()),
        proxy: None,
        readiness_path,
    }
}

fn status_at(index: usize, state: ProfileStatus) -> (TunnelStatus, usize) {
    let status = TunnelStatus(Arc::new(Mutex::new(vec![state; index + 1])));
    (status, index)
}

fn running_status() -> ProfileStatus {
    ProfileStatus {
        profile_id: "work".into(),
        host_mode: "embedded",
        state: TunnelState::Running,
        tunnel_ready: true,
        local_mcp_ready: true,
        shutdown_outcome: None,
        diagnostic: None,
    }
}

fn configured_profile() -> (tempfile::TempDir, EmbeddedTunnelProfile) {
    let temp = tempfile::tempdir().unwrap();
    let store = EnvironmentStore::open(temp.path().join("environment")).unwrap();
    let binary = std::env::current_exe().unwrap();
    let record = EnvironmentRecord {
        schema_version: ENVIRONMENT_SCHEMA,
        environment_id: "fixture-environment".into(),
        request: SetupRequest {
            runner_display_name: None,
            service_scope: ServiceScope::User,
            mode: EnvironmentMode::Create {
                listen: "127.0.0.1:18080".into(),
            },
            server_url: "http://127.0.0.1:18080".into(),
            project: None,
            runner: None,
            account: current_account().unwrap(),
            binaries: RuntimeBinaries {
                cli: binary.clone(),
                server: binary.clone(),
                runner: binary,
            },
        },
        username: None,
        runner_client_id: None,
        projects: vec![],
        configured: true,
    };
    store.save_environment(&record).unwrap();
    let credentials = TunnelCredentials {
        tunnel_id: Secret::new("tunnel_work".into()),
        api_key: Secret::new("fixture-private".into()),
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(NativeEnvironment::new().configure_tunnel_profile(
            &store,
            "work",
            None,
            TunnelHostMode::Embedded,
            None,
            None,
            Some(&credentials),
            false,
        ))
        .unwrap();
    let profile = embedded_tunnel_profiles(store.root())
        .unwrap()
        .into_iter()
        .find(|profile| profile.profile_id == "work")
        .unwrap();
    (temp, profile)
}

#[tokio::test]
async fn initial_status_failure_blocks_launch() {
    let temp = tempfile::tempdir().unwrap();
    let readiness = temp.path().join("missing-readiness.json");
    let profile = profile(readiness);
    let initial = ProfileStatus {
        profile_id: "work".into(),
        host_mode: "embedded",
        state: TunnelState::Starting,
        tunnel_ready: false,
        local_mcp_ready: false,
        shutdown_outcome: None,
        diagnostic: None,
    };
    let (status, index) = status_at(0, initial);
    let (_, stop) = watch::channel(false);
    super::run_profile(
        index,
        profile,
        "http://127.0.0.1:18080/mcp".into(),
        status.clone(),
        stop,
    )
    .await;
    let snapshot = status.snapshot();
    assert_eq!(snapshot[index].state, TunnelState::Failed);
    assert_eq!(
        snapshot[index].diagnostic.as_deref(),
        Some("tunnel_health_unavailable")
    );
    assert!(!snapshot[index].tunnel_ready);
    assert!(!snapshot[index].local_mcp_ready);
    assert_eq!(snapshot[index].shutdown_outcome, None);
}

#[test]
fn missing_heartbeat_file_preserves_live_state_without_creating_it() {
    let temp = tempfile::tempdir().unwrap();
    let readiness = temp.path().join("readiness.json");
    assert!(!readiness.exists());
    let profile = profile(readiness.clone());
    let (status, index) = status_at(0, running_status());
    super::persist_heartbeat(&profile, &status, index);
    assert!(!readiness.exists());
    let snapshot = status.snapshot();
    assert_eq!(snapshot[index].state, TunnelState::Running);
    assert!(snapshot[index].tunnel_ready);
    assert!(snapshot[index].local_mcp_ready);
    assert_eq!(
        snapshot[index].diagnostic.as_deref(),
        Some("tunnel_health_unavailable")
    );
}

#[test]
fn reader_lock_contention_preserves_running_state_and_recovers_after_unlock() {
    let (_temp, profile) = configured_profile();
    let readiness = profile.readiness_path.clone();
    write_embedded_tunnel_observation(
        &readiness,
        profile.runtime_revision,
        TunnelState::Running,
        true,
        true,
        None,
    )
    .unwrap();
    let before = std::fs::read(&readiness).unwrap();
    let contender = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&readiness)
        .unwrap();
    fs2::FileExt::try_lock_shared(&contender).unwrap();

    let (status, index) = status_at(0, running_status());
    super::persist_heartbeat(&profile, &status, index);
    assert_eq!(std::fs::read(&readiness).unwrap(), before);
    let snapshot = status.snapshot();
    assert_eq!(snapshot[index].state, TunnelState::Running);
    assert!(snapshot[index].tunnel_ready);
    assert!(snapshot[index].local_mcp_ready);
    assert_eq!(
        snapshot[index].diagnostic.as_deref(),
        Some("tunnel_health_unavailable")
    );

    drop(contender);
    super::persist_heartbeat(&profile, &status, index);
    let snapshot = status.snapshot();
    assert_eq!(snapshot[index].diagnostic, None);
    let health: serde_json::Value = serde_json::from_slice(&std::fs::read(&readiness).unwrap()).unwrap();
    assert_eq!(health["state"], "running");
    assert_eq!(health["tunnel_ready"], true);
    assert_eq!(health["local_mcp_ready"], true);
    assert!(health.get("diagnostic").is_none() || health["diagnostic"].is_null());
}
