//! Registration acknowledgements and incoming envelope dispatch.

use super::*;
use crate::webcodex_runner::{CommandResult, ShellCommandResult};
use webcodex_core::runner_protocol::{RunnerResultPayload, RunnerResultRequest};

pub(in crate::webcodex_runner::transport) struct StreamPendingRequest {
    pub(in crate::webcodex_runner::transport) transport: StreamTransport,
    pub(in crate::webcodex_runner::transport) request: RunnerRequest,
    pub(in crate::webcodex_runner::transport) sink: RunnerSink,
    pub(in crate::webcodex_runner::transport) received_at: Instant,
}

pub(in crate::webcodex_runner::transport) fn spawn_admitted_stream_request(
    pending: StreamPendingRequest,
    permit: tokio::sync::OwnedSemaphorePermit,
    cfg: &RunnerConfig,
    runtime: &RunnerRuntimeState,
) -> Option<String> {
    let project_registry_dir = match project_registry_dir(cfg) {
        Ok(dir) => dir,
        Err(error) => return Some(error),
    };
    let StreamPendingRequest {
        transport,
        request,
        sink,
        received_at,
    } = pending;
    let config = Arc::clone(&runtime.config);
    let hot = config.snapshot();
    let jobs = runtime.jobs.clone();
    let persistent_shells = runtime.persistent_shells.clone();
    let lsp = runtime.lsp.clone();
    let browser = runtime.browser.clone();
    let dispatch_guard = runtime.dispatches.enter();
    let runtime = runtime.clone();
    let project_operation =
        webcodex_core::runner_operation::RunnerProjectOperationKind::from_wire(&request.kind)
            .is_some();
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let _dispatch_guard = dispatch_guard;
        observe_runner_stream_request_dispatch_wait(transport, received_at.elapsed());
        let dispatch_result = dispatch_request_with_outcome(
            &sink,
            &hot,
            &config,
            &jobs,
            &persistent_shells,
            &project_registry_dir,
            &lsp,
            &browser,
            request,
        );
        let refresh_inventory = match dispatch_result {
            Ok(outcome) => outcome.project_cache_invalidation_required,
            // The mutation can finish even when its result cannot reach
            // the old stream. Only reobserve the catalog; never replay it.
            Err(_) => project_operation,
        };
        if refresh_inventory {
            runtime.invalidate_project_inventory();
        }
    });
    None
}

/// Best-effort terminal signal for a request envelope already accepted by the
/// stream session but not yet admitted to an ordinary dispatch worker.
pub(in crate::webcodex_runner::transport) async fn reconcile_undispatched_stream_request(
    pending: StreamPendingRequest,
    out_tx: &tokio::sync::mpsc::Sender<RunnerEnvelope>,
) {
    let StreamPendingRequest {
        transport,
        request,
        sink,
        ..
    } = pending;
    let request_id = request.request_id;
    let shell_result = ShellCommandResult::outcome_unknown(CommandResult {
        exit_code: None,
        stdout: None,
        stderr: None,
        duration_ms: Some(0),
        error: Some(
            "outcome_unknown: runner transport closed before ordinary request dispatch began"
                .to_string(),
        ),
    });
    let ShellCommandResult {
        result,
        execution_state,
        stdout_truncated,
        stderr_truncated,
    } = shell_result;
    if let Some(duration_ms) = result.duration_ms {
        observe_runner_request_duration(transport.name(), duration_ms);
    }
    let payload = RunnerResultPayload {
        result: RunnerResultRequest {
            client_id: sink.client_id().to_string(),
            runner_instance_id: sink.runner_instance_id().to_string(),
            request_id,
            exit_code: result.exit_code,
            stdout: result.stdout,
            stderr: result.stderr,
            stdout_truncated,
            stderr_truncated,
            duration_ms: result.duration_ms,
            error: result.error,
        },
        command_execution_state: Some(execution_state),
        mcp_gateway: None,
        plugin_gateway: None,
        coding_agent: None,
    };
    let queue_started = Instant::now();
    let envelope = RunnerEnvelope::Result { payload };
    match tokio::time::timeout(TRANSPORT_CONTROL_SEND_TIMEOUT, out_tx.send(envelope)).await {
        Ok(Ok(())) => observe_runner_stream_outgoing_channel(
            transport,
            "result",
            Some(queue_started.elapsed()),
            false,
            RunnerStreamMetricOutcome::Success,
        ),
        Ok(Err(_)) => observe_runner_stream_outgoing_channel(
            transport,
            "result",
            None,
            false,
            RunnerStreamMetricOutcome::Closed,
        ),
        Err(_) => observe_runner_stream_outgoing_channel(
            transport,
            "result",
            None,
            false,
            RunnerStreamMetricOutcome::Timeout,
        ),
    }
}

pub(super) fn registered_ack(ack: RunnerEnvelope) -> Result<ShellProjectInventoryStatus, String> {
    match ack {
        RunnerEnvelope::Registered {
            success: true,
            client,
            ..
        } => {
            crate::webcodex_runner::computer_session::set_server_availability_contract(
                crate::webcodex_runner::computer_session::registration_echo_confirms_contract(
                    client
                        .as_ref()
                        .and_then(|view| view.computer_session_availability),
                ),
            );
            client
                .and_then(|client| client.project_inventory)
                .ok_or_else(|| "register acknowledgement missing canonical project_inventory status; Server is incompatible with this 0.4 Runner".to_string())
        }
        RunnerEnvelope::Registered { error, .. } => Err(format!(
            "register rejected by server: {}",
            error.unwrap_or_else(|| "no server error message".to_string())
        )),
        RunnerEnvelope::Error { code, message } => Err(format!(
            "server error during register {}: {}",
            code, message
        )),
        other => Err(format!("expected registered ack, got {}", other.kind())),
    }
}

pub(super) fn handle_stream_envelope(
    transport: StreamTransport,
    envelope: RunnerEnvelope,
    cfg: &RunnerConfig,
    sink: &RunnerSink,
    out_tx: &tokio::sync::mpsc::Sender<RunnerEnvelope>,
    project_inventory: &mut StreamingProjectInventoryCoordinator,
    runtime: &RunnerRuntimeState,
) -> Option<String> {
    let envelope_kind = envelope.kind();
    observe_runner_stream_incoming_envelope(transport, envelope_kind);
    match envelope {
        // `serve_registered_stream` admits Request envelopes through the pending
        // slot and owned-permit wait so ping/pong and inventory work can
        // continue under backpressure. Direct callers (tests) may still use the
        // immediate try-admit path below when a slot is already available.
        RunnerEnvelope::Request { request } => {
            let pending = StreamPendingRequest {
                transport,
                request,
                sink: sink.clone(),
                received_at: Instant::now(),
            };
            let permit = match runtime.request_dispatch_permits.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    return Some("request_dispatch_capacity_unavailable".to_string());
                }
            };
            spawn_admitted_stream_request(pending, permit, cfg, runtime)
        }
        RunnerEnvelope::Ping { ts } => {
            let _ = try_send_runner_stream_control(transport, out_tx, RunnerEnvelope::Pong { ts });
            None
        }
        RunnerEnvelope::Pong { .. } => None,
        RunnerEnvelope::ProjectInventoryStatus { status } => {
            project_inventory.handle_status(transport, status, cfg, runtime, out_tx);
            None
        }
        RunnerEnvelope::Registered { .. } if transport == StreamTransport::Quic => None,
        RunnerEnvelope::Error { code, message } => {
            Some(format!("server error {}: {}", code, message))
        }
        other => {
            eprintln!(
                "webcodex-runner {} ignoring unexpected envelope: {}",
                transport.name(),
                other.kind()
            );
            None
        }
    }
}
