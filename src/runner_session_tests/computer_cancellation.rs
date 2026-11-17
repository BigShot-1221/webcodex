use super::*;
use std::sync::Mutex;
use webcodex_core::runner_operation::RunnerOperation;
use webcodex_core::runner_protocol::{RunnerJobUpdateRequest, RunnerRequest, RunnerResultPayload};
use webcodex_runner_registry::{RunnerAccess, RunnerRegistryTelemetry};

#[derive(Debug, Default)]
struct DequeueTelemetry {
    dequeued: Mutex<Vec<(String, Instant)>>,
    notify: Notify,
}

impl RunnerRegistryTelemetry for DequeueTelemetry {
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
    }
    fn runner_request_dequeued(&self, id: &str, _: RunnerTransport, _: Duration) {
        self.dequeued
            .lock()
            .unwrap()
            .push((id.into(), Instant::now()));
        self.notify.notify_one();
    }
    fn runner_result_accepted(&self, _: &str, _: &RunnerResultPayload) {}
    fn runner_result_finalized(&self, _: &str) {}
    fn runner_job_update_accepted(&self, _: Option<&str>, _: &str, _: &RunnerJobUpdateRequest) {}
    fn runner_job_finalized(&self, _: Option<&str>, _: &str) {}
}

#[tokio::test]
async fn computer_streaming_pump_skips_abandoned_queue_but_preserves_backpressured_dispatch() {
    for transport in [RunnerTransport::WebSocket, RunnerTransport::Quic] {
        let telemetry = Arc::new(DequeueTelemetry::default());
        let registry = Arc::new(RunnerRegistry::with_telemetry(telemetry.clone()));
        let mut registration = streaming_registration("computer", "inst");
        registration.owner = Some("alice".into());
        registration.capabilities.computer_observe = true;
        let notify = Arc::new(Notify::new());
        let (_, cancel) = registry
            .register_streaming_session_with_cancel(
                registration,
                None,
                "conn",
                transport,
                notify.clone(),
            )
            .await
            .unwrap();
        let access = RunnerAccess {
            global_visibility: false,
            owner_bypass: false,
            username: Some("alice".into()),
            group: None,
        };
        let mut requests = Vec::new();
        for _ in 0..2 {
            requests.push(
                registry
                    .enqueue_computer(
                        "computer".into(),
                        "computer_list_windows",
                        "{\"limit\":1}".into(),
                        "alice".into(),
                        Some(&access),
                        5,
                    )
                    .await
                    .unwrap(),
            );
        }
        let (live_id, live_rx) = requests.pop().unwrap();
        let (abandoned_id, abandoned_rx) = requests.pop().unwrap();
        drop(abandoned_rx);
        let (out_tx, mut out_rx) = mpsc::channel(1);
        out_tx.send(RunnerEnvelope::Pong { ts: 1 }).await.unwrap();
        let pump = spawn_request_pump(
            &SessionContext {
                registry: &registry,
                client_id: "computer",
                runner_instance_id: "inst",
                connection_id: "conn",
                notify,
                cancel,
                transport,
            },
            out_tx,
        );
        tokio::time::timeout(Duration::from_secs(2), telemetry.notify.notified())
            .await
            .unwrap();
        tokio::task::yield_now().await;
        assert_eq!(
            out_rx.len(),
            1,
            "full channel still contains only the fixture Pong"
        );
        assert_eq!(telemetry.dequeued.lock().unwrap().len(), 1);
        let (dequeued_id, dequeued_at) = telemetry.dequeued.lock().unwrap()[0].clone();
        assert_eq!(dequeued_id, live_id);
        let waiter_closed_at = Instant::now();
        drop(live_rx);
        assert_eq!(
            registry.cancel_request_dispatch_state(&live_id).await,
            Some(true)
        );
        assert_eq!(
            registry.cancel_request_dispatch_state(&abandoned_id).await,
            None
        );
        assert!(matches!(
            out_rx.recv().await,
            Some(RunnerEnvelope::Pong { .. })
        ));
        let delivered = tokio::time::timeout(Duration::from_secs(2), out_rx.recv())
            .await
            .unwrap();
        let channel_received_at = Instant::now();
        let Some(RunnerEnvelope::Request { request }) = delivered else {
            panic!("expected live request")
        };
        assert_eq!(request.request_id, live_id);
        assert!(dequeued_at <= waiter_closed_at && waiter_closed_at <= channel_received_at);
        // Delivery here is to the writer channel, not native execution. Already
        // dispatched work is not retracted or made replayable by waiter loss.
        pump.abort();
        assert!(pump.await.unwrap_err().is_cancelled());
    }
}
