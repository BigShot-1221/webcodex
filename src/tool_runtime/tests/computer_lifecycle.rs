use super::support::*;
use crate::runner_protocol::{
    RunnerCapabilities, RunnerJobUpdateRequest, RunnerRequest, RunnerResultPayload,
};
use crate::tool_runtime::tool_call::ComputerControlToolCall;
use crate::tool_runtime::ToolCall;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;
use webcodex_core::runner_operation::RunnerOperation;
use webcodex_runner_registry::{RunnerRegistry, RunnerRegistryTelemetry};

#[derive(Debug)]
struct CleanupRaceTelemetry {
    armed: AtomicBool,
    entered: Mutex<Option<oneshot::Sender<()>>>,
    released: Mutex<bool>,
    release: Condvar,
}

impl CleanupRaceTelemetry {
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.release.notify_all();
    }
}

impl RunnerRegistryTelemetry for CleanupRaceTelemetry {
    fn request_enqueued(
        &self,
        _: &RunnerRequest,
        _: &RunnerOperation,
        _: &str,
        _: &str,
        _: Option<&str>,
        _: Option<&str>,
        _: Option<&str>,
        _: Option<&str>,
        _: Option<&str>,
    ) {
        if self.armed.swap(false, Ordering::SeqCst) {
            let _ = self.entered.lock().unwrap().take().unwrap().send(());
            let _ = self
                .release
                .wait_timeout_while(
                    self.released.lock().unwrap(),
                    Duration::from_secs(10),
                    |released| !*released,
                )
                .unwrap();
        }
    }
    fn runner_result_accepted(&self, _: &str, _: &RunnerResultPayload) {}
    fn runner_result_finalized(&self, _: &str) {}
    fn runner_job_update_accepted(&self, _: Option<&str>, _: &str, _: &RunnerJobUpdateRequest) {}
    fn runner_job_finalized(&self, _: Option<&str>, _: &str) {}
}

struct ReleaseOnDrop(Arc<CleanupRaceTelemetry>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[tokio::test(start_paused = true)]
async fn computer_effect_timeout_keeps_waiter_owned_until_dispatch_cleanup() {
    let (entered_tx, entered_rx) = oneshot::channel();
    let telemetry = Arc::new(CleanupRaceTelemetry {
        armed: AtomicBool::new(false),
        entered: Mutex::new(Some(entered_tx)),
        released: Mutex::new(false),
        release: Condvar::new(),
    });
    let _release_on_failure = ReleaseOnDrop(telemetry.clone());
    let mut runtime = runtime_with_agent_project("computer-timeout-race");
    runtime.runner_registry = Arc::new(RunnerRegistry::with_telemetry(telemetry.clone()));
    let auth = bootstrap_auth_context();
    register_agent_projects_for_auth(
        &runtime,
        "computer-timeout-race",
        &auth,
        RunnerCapabilities {
            computer_observe: true,
            computer_control: true,
            ..Default::default()
        },
        vec![registered_project("agent-proj", "/tmp/agent-proj")],
    )
    .await;
    let call = ToolCall::ComputerControl(
        serde_json::from_value::<ComputerControlToolCall>(
            serde_json::json!({"action":"press", "client_id":"computer-timeout-race",
            "surface_id":"surface_test", "element_id":"element_test"}),
        )
        .unwrap(),
    );
    let mut tool = Box::pin(runtime.dispatch_computer_tool(call, Some(&auth)));
    assert!(futures_util::poll!(tool.as_mut()).is_pending());

    // Hold the real Registry lock in an unrelated enqueue. Queue a sweep before
    // the timeout's cancellation, making their order deterministic via FIFO.
    telemetry.armed.store(true, Ordering::SeqCst);
    let registry = runtime.runner_registry.clone();
    let handle = tokio::runtime::Handle::current();
    let blocker = tokio::task::spawn_blocking(move || {
        handle.block_on(async move {
            let access = webcodex_runner_registry::RunnerAccess {
                global_visibility: true,
                owner_bypass: true,
                username: None,
                group: None,
            };
            registry
                .enqueue_computer(
                    "computer-timeout-race".into(),
                    "computer_list_windows",
                    "{\"limit\":1}".into(),
                    "test".into(),
                    Some(&access),
                    5,
                )
                .await
                .unwrap()
        })
    });
    entered_rx.await.unwrap();
    tokio::time::advance(Duration::from_secs(64)).await;
    let mut sweep = Box::pin(runtime.runner_registry.cancel_abandoned_sync_requests());
    assert!(futures_util::poll!(sweep.as_mut()).is_pending());
    assert!(futures_util::poll!(tool.as_mut()).is_pending());
    telemetry.release();
    let (_blocker_id, _blocker_rx) = blocker.await.unwrap();
    assert_eq!(
        sweep.await,
        0,
        "timeout cleanup still owns its response waiter"
    );
    let result = tool.await;
    assert!(!result.success);
    assert_eq!(result.output["execution_state"], "not_started");
}
