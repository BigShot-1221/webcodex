//! Scheduled ACP hot-inventory maintenance. Internal Server work, not a tool
//! and never a provider execution. One bounded Runner page per global round.
use super::ToolRuntime;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use webcodex_core::coding_agent::retention::{
    RETENTION_MAINTENANCE_COOPERATIVE_SECS, RETENTION_MAINTENANCE_INTERVAL_SECS,
};
use webcodex_core::coding_agent::{
    CodingAgentRequest, CodingAgentRetentionMaintenanceRequest, CodingAgentResponsePayload,
};

pub(crate) struct CodingAgentRetentionMaintenanceOwner {
    stop: tokio::sync::watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl CodingAgentRetentionMaintenanceOwner {
    pub(crate) fn request_shutdown(&self) {
        self.stop.send_replace(true);
    }

    pub(crate) async fn shutdown_and_wait_in_flight(self) {
        self.request_shutdown();
        // A blocking durable publication cannot safely be detached on timeout.
        // The cooperative deadline prevents starting additional record I/O.
        if let Err(error) = self.task.await {
            tracing::warn!(%error, "ACP retention maintenance task failed");
        }
    }
}

async fn maintain_runner(
    runtime: &ToolRuntime,
    client: &crate::runner_protocol::RunnerView,
    provider: &webcodex_core::coding_agent::CodingAgentProvider,
    deadline: Instant,
    stop: &tokio::sync::watch::Receiver<bool>,
    server_stop: &tokio::sync::watch::Receiver<Option<crate::server_shutdown::ShutdownReason>>,
) -> Result<(), String> {
    let acknowledgements = runtime.archive_acknowledgements_bounded_until(
        client, None, deadline, true,
    ).await.ok_or_else(|| "ACP retention receipt page unavailable".to_string())?;
    if acknowledgements.is_empty() || Instant::now() >= deadline
        || *stop.borrow() || server_stop.borrow().is_some()
    {
        return Ok(());
    }
    let work_budget_ms = deadline.saturating_duration_since(Instant::now()).as_millis() as u64;
    if work_budget_ms == 0 { return Ok(()); }
    let request = CodingAgentRequest::RetentionMaintenance(CodingAgentRetentionMaintenanceRequest {
        run_id: format!("wc_agent_run_maint_{}", uuid::Uuid::new_v4().simple()),
        provider_id: provider.provider_id.clone(),
        provider_instance_id: provider.provider_instance_id.clone(),
        archive_acknowledged_runs: acknowledgements,
        work_budget_ms,
    });
    let (request_id, receiver) = runtime.runner_registry.enqueue_coding_agent(
        &client.client_id, &client.runner_instance_id, &provider.provider_id,
        &provider.provider_instance_id, request, None,
        "auth_server_acp_retention".to_string(),
    ).await?;
    // Transport observation is separate from the cooperative record-work budget.
    match tokio::time::timeout(Duration::from_secs(7), receiver).await {
        Ok(Ok(response)) if matches!(response.payload, Some(CodingAgentResponsePayload::RetentionMaintenance { .. })) => Ok(()),
        _ => {
            runtime.runner_registry.cancel_request(&request_id).await;
            Err("ACP retention maintenance transport did not confirm completion".into())
        }
    }
}

pub(crate) fn spawn_coding_agent_retention_maintenance(
    runtime: Arc<ToolRuntime>,
    mut server_stop: tokio::sync::watch::Receiver<Option<crate::server_shutdown::ShutdownReason>>,
) -> CodingAgentRetentionMaintenanceOwner {
    let (stop, mut stop_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(async move {
        let period = Duration::from_secs(RETENTION_MAINTENANCE_INTERVAL_SECS);
        let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_client: Option<String> = None;
        loop {
            if *stop_rx.borrow() || server_stop.borrow().is_some() { break; }
            tokio::select! {
                biased;
                _ = stop_rx.changed() => break,
                _ = server_stop.changed() => break,
                _ = interval.tick() => {
                    let deadline = Instant::now() + Duration::from_secs(RETENTION_MAINTENANCE_COOPERATIVE_SECS);
                    if let Some((client, provider)) = runtime.runner_registry
                        .coding_agent_retention_maintenance_target(last_client.as_deref()).await
                    {
                        last_client = Some(client.client_id.clone());
                        if let Err(error) = maintain_runner(&runtime, &client, &provider, deadline, &stop_rx, &server_stop).await {
                            tracing::warn!(%error, "ACP retention maintenance round incomplete");
                        }
                    }
                }
            }
        }
    });
    CodingAgentRetentionMaintenanceOwner { stop, task }
}