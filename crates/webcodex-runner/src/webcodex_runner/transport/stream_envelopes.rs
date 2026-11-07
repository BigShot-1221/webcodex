//! Registration acknowledgements and incoming envelope dispatch.

use std::collections::VecDeque;

use super::*;
use crate::webcodex_runner::job_manager::decode_failure_prestart_lifecycle;
use crate::webcodex_runner::{CommandResult, ShellCommandResult};
use webcodex_core::runner_operation::RunnerFileOperation;
use webcodex_core::runner_protocol::{
    PersistentShellResult, RunnerJobUpdateRequest, RunnerPersistentShellResultRequest,
    RunnerResultPayload, RunnerResultRequest,
};

/// Pending overflow rejections awaiting a bounded async send on the stream loop.
/// Serializes result/job_update emission so a request burst cannot spawn
/// unbounded tasks while shared dispatch slots are saturated.
pub(in crate::webcodex_runner::transport) const STREAM_PENDING_SLOT_REJECTION_QUEUE_CAP: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::webcodex_runner::transport) enum StreamOutboundSendFailure {
    ChannelClosed,
    SendTimeout,
}

impl StreamOutboundSendFailure {
    pub(in crate::webcodex_runner::transport) fn session_error_message(self) -> &'static str {
        match self {
            Self::ChannelClosed => "stream_outbound_channel_closed",
            Self::SendTimeout => "stream_outbound_send_timeout",
        }
    }
}

const STREAM_PENDING_SLOT_OVERFLOW_REASON: &str = "not_started: runner stream already holds one undispatched request while shared dispatch capacity is saturated";
const STREAM_PENDING_SLOT_UNDISPATCHED_REASON: &str =
    "not_started: runner transport closed before ordinary request dispatch began";

pub(in crate::webcodex_runner::transport) struct StreamPendingRequest {
    pub(in crate::webcodex_runner::transport) transport: StreamTransport,
    pub(in crate::webcodex_runner::transport) request: RunnerRequest,
    pub(in crate::webcodex_runner::transport) sink: RunnerSink,
    pub(in crate::webcodex_runner::transport) received_at: Instant,
}

fn stream_pre_dispatch_not_started_command(reason: impl Into<String>) -> CommandResult {
    CommandResult {
        exit_code: None,
        stdout: None,
        stderr: None,
        duration_ms: Some(0),
        error: Some(reason.into()),
    }
}

fn stream_persistent_shell_pre_dispatch_rejection(
    request: &RunnerRequest,
    reason: String,
) -> Option<PersistentShellResult> {
    let operation = request.persistent_shell.as_ref()?;
    Some(PersistentShellResult {
        shell_id: operation.shell_id.clone(),
        workflow_session_id: operation.workflow_session_id.clone(),
        runtime_project_id: operation.runtime_project_id.clone(),
        shell_state: "unknown".to_string(),
        execution_state: "rejected".to_string(),
        command_started: false,
        command_completed: false,
        exit_code: None,
        stdout: String::new(),
        stderr: String::new(),
        stdout_truncated: false,
        stderr_truncated: false,
        duration_ms: 0,
        cwd: None,
        initial_cwd: None,
        shell: None,
        profile: None,
        created_at: None,
        last_activity_at: None,
        busy: false,
        already_closed: false,
        close_reason: None,
        error_code: Some("persistent_shell_rejected".to_string()),
        error: Some(reason),
    })
}

async fn send_stream_outbound_bounded(
    transport: StreamTransport,
    out_tx: &tokio::sync::mpsc::Sender<RunnerEnvelope>,
    envelope: RunnerEnvelope,
) -> Result<(), StreamOutboundSendFailure> {
    let envelope_kind = envelope.kind();
    let queue_started = Instant::now();
    match tokio::time::timeout(TRANSPORT_CONTROL_SEND_TIMEOUT, out_tx.send(envelope)).await {
        Ok(Ok(())) => {
            observe_runner_stream_outgoing_channel(
                transport,
                envelope_kind,
                Some(queue_started.elapsed()),
                false,
                RunnerStreamMetricOutcome::Success,
            );
            Ok(())
        }
        Ok(Err(_)) => {
            observe_runner_stream_outgoing_channel(
                transport,
                envelope_kind,
                None,
                false,
                RunnerStreamMetricOutcome::Closed,
            );
            Err(StreamOutboundSendFailure::ChannelClosed)
        }
        Err(_) => {
            observe_runner_stream_outgoing_channel(
                transport,
                envelope_kind,
                None,
                false,
                RunnerStreamMetricOutcome::Timeout,
            );
            Err(StreamOutboundSendFailure::SendTimeout)
        }
    }
}

async fn send_stream_result_payload(
    transport: StreamTransport,
    out_tx: &tokio::sync::mpsc::Sender<RunnerEnvelope>,
    payload: RunnerResultPayload,
) -> Result<(), StreamOutboundSendFailure> {
    if let Some(duration_ms) = payload.result.duration_ms {
        observe_runner_request_duration(transport.name(), duration_ms);
    }
    send_stream_outbound_bounded(
        transport,
        out_tx,
        RunnerEnvelope::Result { payload },
    )
    .await
}

async fn send_stream_terminal_shell_result(
    transport: StreamTransport,
    sink: &RunnerSink,
    request_id: String,
    shell_result: ShellCommandResult,
    out_tx: &tokio::sync::mpsc::Sender<RunnerEnvelope>,
) -> Result<(), StreamOutboundSendFailure> {
    let ShellCommandResult {
        result,
        execution_state,
        stdout_truncated,
        stderr_truncated,
    } = shell_result;
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
    send_stream_result_payload(transport, out_tx, payload).await
}

fn is_stream_job_start_kind(kind: &str) -> bool {
    matches!(
        kind,
        "start_job"
            | "start_validation_job"
            | "start_build_job"
            | "start_format_job"
            | "start_process_job"
            | "start_interactive_process_job"
            | "start_detached_process_job"
            | "start_script_job"
            | "start_skill_resource_job"
    )
}

/// Mirror `dispatch::submit_decode_failure` envelope routing for requests that
/// never reached ordinary dispatch.
async fn reject_stream_request_before_dispatch(
    transport: StreamTransport,
    sink: &RunnerSink,
    request: RunnerRequest,
    out_tx: &tokio::sync::mpsc::Sender<RunnerEnvelope>,
    reason: &str,
) -> Result<(), StreamOutboundSendFailure> {
    let request_id = request.request_id.clone();
    match request.kind.as_str() {
        kind if is_stream_job_start_kind(kind) => {
            if let Some(job_id) = request.job_id.clone() {
                let update = RunnerJobUpdateRequest {
                    client_id: sink.client_id().to_string(),
                    runner_instance_id: sink.runner_instance_id().to_string(),
                    job_id,
                    request_id: Some(request_id),
                    update_seq: Some(1),
                    status: "failed".to_string(),
                    stdout_chunk: None,
                    stderr_chunk: None,
                    log_snapshot: None,
                    exit_code: None,
                    duration_ms: Some(0),
                    error: Some(reason.to_string()),
                    command_execution_state: decode_failure_prestart_lifecycle(&request),
                    validation_progress: None,
                    test_count_evidence: None,
                    format_mutation: None,
                    build_artifacts: None,
                    activity: None,
                    finished: true,
                };
                return send_stream_outbound_bounded(
                    transport,
                    out_tx,
                    RunnerEnvelope::JobUpdate { payload: update },
                )
                .await;
            }
            let payload = RunnerResultPayload {
                result: RunnerResultRequest {
                    client_id: sink.client_id().to_string(),
                    runner_instance_id: sink.runner_instance_id().to_string(),
                    request_id,
                    exit_code: None,
                    stdout: None,
                    stderr: None,
                    stdout_truncated: false,
                    stderr_truncated: false,
                    duration_ms: Some(0),
                    error: Some(reason.to_string()),
                },
                command_execution_state: None,
                mcp_gateway: None,
                plugin_gateway: None,
                coding_agent: None,
            };
            return send_stream_result_payload(transport, out_tx, payload).await;
        }
        "coding_agent" => {
            let payload = RunnerResultPayload {
                result: RunnerResultRequest {
                    client_id: sink.client_id().to_string(),
                    runner_instance_id: sink.runner_instance_id().to_string(),
                    request_id,
                    exit_code: None,
                    stdout: None,
                    stderr: None,
                    stdout_truncated: false,
                    stderr_truncated: false,
                    duration_ms: None,
                    error: None,
                },
                command_execution_state: None,
                mcp_gateway: None,
                plugin_gateway: None,
                coding_agent: Some(webcodex_core::coding_agent::CodingAgentResponse::error(
                    webcodex_core::coding_agent::CodingAgentDispatchState::NotStarted,
                    "stream_dispatch_backpressure",
                    reason.to_string(),
                    Some("backpressure"),
                    Some("reobserve"),
                )),
            };
            return send_stream_result_payload(transport, out_tx, payload).await;
        }
        "mcp_gateway" => {
            let payload = RunnerResultPayload {
                result: RunnerResultRequest {
                    client_id: sink.client_id().to_string(),
                    runner_instance_id: sink.runner_instance_id().to_string(),
                    request_id,
                    exit_code: None,
                    stdout: None,
                    stderr: None,
                    stdout_truncated: false,
                    stderr_truncated: false,
                    duration_ms: None,
                    error: None,
                },
                command_execution_state: None,
                mcp_gateway: Some(webcodex_core::mcp_gateway::McpGatewayResponse::error(
                    webcodex_core::mcp_gateway::McpGatewayDispatchState::NotStarted,
                    "stream_dispatch_backpressure",
                    reason.to_string(),
                )),
                plugin_gateway: None,
                coding_agent: None,
            };
            return send_stream_result_payload(transport, out_tx, payload).await;
        }
        "plugin_gateway" => {
            let payload = RunnerResultPayload {
                result: RunnerResultRequest {
                    client_id: sink.client_id().to_string(),
                    runner_instance_id: sink.runner_instance_id().to_string(),
                    request_id,
                    exit_code: None,
                    stdout: None,
                    stderr: None,
                    stdout_truncated: false,
                    stderr_truncated: false,
                    duration_ms: None,
                    error: None,
                },
                command_execution_state: None,
                mcp_gateway: None,
                plugin_gateway: Some(webcodex_core::plugin::PluginGatewayResponse::error(
                    webcodex_core::plugin::PluginDispatchState::NotStarted,
                    "stream_dispatch_backpressure",
                    reason.to_string(),
                )),
                coding_agent: None,
            };
            return send_stream_result_payload(transport, out_tx, payload).await;
        }
        "persistent_shell" => {
            if let Some(result) =
                stream_persistent_shell_pre_dispatch_rejection(&request, reason.to_string())
            {
                let body = RunnerPersistentShellResultRequest {
                    client_id: sink.client_id().to_string(),
                    runner_instance_id: sink.runner_instance_id().to_string(),
                    request_id,
                    result,
                };
                return send_stream_outbound_bounded(
                    transport,
                    out_tx,
                    RunnerEnvelope::PersistentShellResult { payload: body },
                )
                .await;
            }
            let shell_result = ShellCommandResult::not_started(
                stream_pre_dispatch_not_started_command(reason.to_string()),
            );
            return send_stream_terminal_shell_result(
                transport,
                sink,
                request.request_id,
                shell_result,
                out_tx,
            )
            .await;
        }
        "run_shell" => {
            return send_stream_terminal_shell_result(
                transport,
                sink,
                request_id,
                ShellCommandResult::not_started(stream_pre_dispatch_not_started_command(reason)),
                out_tx,
            )
            .await;
        }
        "run_process" | "run_script" | "run_internal_posix_script" | "skill_resource_execution" => {
            return send_stream_terminal_shell_result(
                transport,
                sink,
                request_id,
                ShellCommandResult::not_started(stream_pre_dispatch_not_started_command(reason)),
                out_tx,
            )
            .await;
        }
        kind if kind.starts_with("file_") && !RunnerFileOperation::is_wire_kind(kind) => {
            let payload = RunnerResultPayload {
                result: RunnerResultRequest {
                    client_id: sink.client_id().to_string(),
                    runner_instance_id: sink.runner_instance_id().to_string(),
                    request_id,
                    exit_code: None,
                    stdout: None,
                    stderr: None,
                    stdout_truncated: false,
                    stderr_truncated: false,
                    duration_ms: Some(0),
                    error: Some(reason.to_string()),
                },
                command_execution_state: None,
                mcp_gateway: None,
                plugin_gateway: None,
                coding_agent: None,
            };
            return send_stream_result_payload(transport, out_tx, payload).await;
        }
        _ => {
            send_stream_terminal_shell_result(
                transport,
                sink,
                request_id,
                ShellCommandResult::not_started(stream_pre_dispatch_not_started_command(reason)),
                out_tx,
            )
            .await
        }
    }
}

pub(in crate::webcodex_runner::transport) async fn reject_stream_pending_slot_bounded(
    pending: StreamPendingRequest,
    out_tx: &tokio::sync::mpsc::Sender<RunnerEnvelope>,
    reason: &'static str,
) -> Result<(), StreamOutboundSendFailure> {
    let StreamPendingRequest {
        transport,
        request,
        sink,
        ..
    } = pending;
    reject_stream_request_before_dispatch(transport, &sink, request, out_tx, reason).await
}

/// Enqueue one overflow rejection and apply backpressure when the bounded queue
/// is full so every admitted request still receives a terminal reconciliation.
pub(in crate::webcodex_runner::transport) async fn enqueue_stream_pending_slot_rejection(
    queue: &mut VecDeque<StreamPendingRequest>,
    pending: StreamPendingRequest,
    out_tx: &tokio::sync::mpsc::Sender<RunnerEnvelope>,
) -> Result<(), StreamOutboundSendFailure> {
    while queue.len() >= STREAM_PENDING_SLOT_REJECTION_QUEUE_CAP {
        let front = queue
            .pop_front()
            .expect("guarded non-empty overflow rejection queue");
        reject_stream_pending_slot_bounded(front, out_tx, STREAM_PENDING_SLOT_OVERFLOW_REASON)
            .await?;
    }
    queue.push_back(pending);
    Ok(())
}

pub(in crate::webcodex_runner::transport) async fn drain_stream_pending_slot_rejection_queue(
    queue: &mut VecDeque<StreamPendingRequest>,
    out_tx: &tokio::sync::mpsc::Sender<RunnerEnvelope>,
) -> Result<(), StreamOutboundSendFailure> {
    let Some(pending) = queue.pop_front() else {
        return Ok(());
    };
    reject_stream_pending_slot_bounded(pending, out_tx, STREAM_PENDING_SLOT_OVERFLOW_REASON).await
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
) -> Result<(), StreamOutboundSendFailure> {
    let StreamPendingRequest {
        transport,
        request,
        sink,
        ..
    } = pending;
    reject_stream_request_before_dispatch(
        transport,
        &sink,
        request,
        out_tx,
        STREAM_PENDING_SLOT_UNDISPATCHED_REASON,
    )
    .await
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
