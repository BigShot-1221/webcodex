use salvo::conn::Acceptor;
use salvo::prelude::*;
use salvo::{Server, Service};
use std::future::Future;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShutdownReason {
    Sigint,
    #[cfg(any(unix, test))]
    Sigterm,
    ParentEof,
    ServiceStop,
    PersistenceFailure,
}

impl ShutdownReason {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Sigint => "SIGINT",
            #[cfg(any(unix, test))]
            Self::Sigterm => "SIGTERM",
            Self::ParentEof => "parent_stdin_eof",
            Self::ServiceStop => "SCM_STOP",
            Self::PersistenceFailure => "workflow_session_persistence_failure",
        }
    }
}

pub(crate) fn persistence_failure_exit_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::Other,
        "workflow session persistence failed; restart required",
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShutdownState {
    Running = 0,
    Draining = 1,
    Stopped = 2,
}

#[derive(Debug)]
pub(crate) struct ShutdownCoordinator {
    state: AtomicU8,
    /// Fatal or operator-requested shutdown may close root admission before the
    /// authoritative `Running -> Draining` transition in `serve_with_ingress`.
    close_admission: AtomicBool,
    shutdown_request_tx: tokio::sync::watch::Sender<Option<ShutdownReason>>,
    shutdown_request_rx: tokio::sync::watch::Receiver<Option<ShutdownReason>>,
    /// Background listeners (e.g. Runner QUIC) observe this without closing HTTP
    /// admission. Operator-driven shutdown uses this before the HTTP drain phase.
    background_services_stop_tx: tokio::sync::watch::Sender<Option<ShutdownReason>>,
    background_services_stop_rx: tokio::sync::watch::Receiver<Option<ShutdownReason>>,
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        let (shutdown_request_tx, shutdown_request_rx) = tokio::sync::watch::channel(None);
        let (background_services_stop_tx, background_services_stop_rx) =
            tokio::sync::watch::channel(None);
        Self {
            state: AtomicU8::new(ShutdownState::Running as u8),
            close_admission: AtomicBool::new(false),
            shutdown_request_tx,
            shutdown_request_rx,
            background_services_stop_tx,
            background_services_stop_rx,
        }
    }
}

impl ShutdownCoordinator {
    #[cfg(test)]
    fn state(&self) -> ShutdownState {
        match self.state.load(Ordering::Acquire) {
            0 => ShutdownState::Running,
            1 => ShutdownState::Draining,
            2 => ShutdownState::Stopped,
            value => panic!("invalid Server shutdown state {value}"),
        }
    }

    fn begin_draining(&self) -> bool {
        self.state
            .compare_exchange(
                ShutdownState::Running as u8,
                ShutdownState::Draining as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn is_running(&self) -> bool {
        self.state.load(Ordering::Acquire) == ShutdownState::Running as u8
            && !self.close_admission.load(Ordering::Acquire)
    }

    pub(crate) fn shutdown_request_receiver(
        &self,
    ) -> tokio::sync::watch::Receiver<Option<ShutdownReason>> {
        self.shutdown_request_rx.clone()
    }

    pub(crate) fn background_services_stop_receiver(
        &self,
    ) -> tokio::sync::watch::Receiver<Option<ShutdownReason>> {
        self.background_services_stop_rx.clone()
    }

    /// Notify background listeners without closing HTTP admission. Idempotent
    /// sends are safe; the last reason wins.
    pub(crate) fn notify_background_services_stop(&self, reason: ShutdownReason) {
        let _ = self.background_services_stop_tx.send(Some(reason));
    }

    /// Programmatic shutdown (e.g. workflow-session persistence fatal). Closes
    /// admission immediately while leaving lifecycle state `Running` until HTTP
    /// ingress observes the authoritative drain transition.
    pub(crate) fn request_shutdown(&self, reason: ShutdownReason) {
        self.close_admission.store(true, Ordering::Release);
        let _ = self.shutdown_request_tx.send(Some(reason));
        self.notify_background_services_stop(reason);
    }

    fn mark_stopped(&self) {
        self.state
            .store(ShutdownState::Stopped as u8, Ordering::Release);
    }
}

#[derive(Clone)]
pub(crate) struct DrainAdmission {
    coordinator: Arc<ShutdownCoordinator>,
}

impl DrainAdmission {
    pub(crate) fn new(coordinator: Arc<ShutdownCoordinator>) -> Self {
        Self { coordinator }
    }
}

#[async_trait]
impl Handler for DrainAdmission {
    async fn handle(
        &self,
        req: &mut Request,
        depot: &mut Depot,
        res: &mut Response,
        ctrl: &mut FlowCtrl,
    ) {
        if !self.coordinator.is_running() {
            res.status_code(StatusCode::SERVICE_UNAVAILABLE);
            res.headers_mut().insert(
                salvo::http::header::RETRY_AFTER,
                salvo::http::HeaderValue::from_static("1"),
            );
            res.render(Json(serde_json::json!({
                "error": "server_draining",
                "message": "Server is draining; retry the request against the replacement Server"
            })));
            ctrl.skip_rest();
            return;
        }
        ctrl.call_next(req, depot, res).await;
    }
}

#[cfg(unix)]
struct TerminationSignals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl TerminationSignals {
    fn new() -> io::Result<Self> {
        use tokio::signal::unix::{signal, SignalKind};

        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
        })
    }

    async fn recv(&mut self) -> ShutdownReason {
        tokio::select! {
            _ = self.interrupt.recv() => ShutdownReason::Sigint,
            _ = self.terminate.recv() => ShutdownReason::Sigterm,
        }
    }
}

#[cfg(not(unix))]
struct TerminationSignals;

#[cfg(not(unix))]
impl TerminationSignals {
    fn new() -> io::Result<Self> {
        Ok(Self)
    }

    async fn recv(&mut self) -> ShutdownReason {
        match tokio::signal::ctrl_c().await {
            Ok(()) => ShutdownReason::Sigint,
            Err(error) => {
                tracing::error!(
                    ?error,
                    "failed to listen for Server Ctrl-C; forcing shutdown"
                );
                ShutdownReason::Sigint
            }
        }
    }
}

async fn recv_coordinator_shutdown(
    rx: &mut tokio::sync::watch::Receiver<Option<ShutdownReason>>,
) -> ShutdownReason {
    loop {
        if rx.changed().await.is_err() {
            tracing::warn!(
                "shutdown coordinator request channel closed; treating as workflow persistence failure"
            );
            return ShutdownReason::PersistenceFailure;
        }
        if let Some(reason) = rx.borrow_and_update().clone() {
            return reason;
        }
    }
}

pub(crate) async fn serve_until_termination<A, S>(
    server: Server<A>,
    service: S,
    coordinator: Arc<ShutdownCoordinator>,
    graceful_timeout: Duration,
    stop_on_stdin_eof: bool,
    service_stop: impl Future<Output = ()> + Send,
    tunnels: crate::server_tunnels::TunnelSupervisor,
) -> io::Result<()>
where
    A: Acceptor + Send,
    S: Into<Service> + Send,
{
    let mut signals = TerminationSignals::new()?;
    let mut coordinator_shutdown = coordinator.shutdown_request_receiver();
    let pre_coordinator_reason = coordinator_shutdown.borrow().clone();
    let parent_eof = if stop_on_stdin_eof {
        Some(parent_eof_signal()?)
    } else {
        None
    };
    let signal = async move {
        if let Some(reason) = pre_coordinator_reason {
            return reason;
        }
        if let Some(parent_eof) = parent_eof {
            tokio::select! {
                reason = signals.recv() => reason,
                _ = parent_eof => ShutdownReason::ParentEof,
                _ = service_stop => ShutdownReason::ServiceStop,
                reason = recv_coordinator_shutdown(&mut coordinator_shutdown) => reason,
            }
        } else {
            tokio::select! {
                reason = signals.recv() => reason,
                _ = service_stop => ShutdownReason::ServiceStop,
                reason = recv_coordinator_shutdown(&mut coordinator_shutdown) => reason,
            }
        }
    };
    serve_with_ingress(
        server,
        service,
        coordinator,
        signal,
        graceful_timeout,
        |stop| tunnels.run(stop),
    )
    .await
}

fn parent_eof_signal() -> io::Result<tokio::sync::oneshot::Receiver<()>> {
    use std::io::Read;

    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("webcodex-server-parent-lease".to_string())
        .spawn(move || {
            let mut stdin = std::io::stdin();
            let mut buffer = [0_u8; 64];
            loop {
                match stdin.read(&mut buffer) {
                    Ok(0) | Err(_) => {
                        let _ = tx.send(());
                        return;
                    }
                    Ok(_) => {}
                }
            }
        })?;
    Ok(rx)
}

#[cfg(test)]
async fn serve_with_signal<A, S, F>(
    server: Server<A>,
    service: S,
    coordinator: Arc<ShutdownCoordinator>,
    signal: F,
    graceful_timeout: Duration,
) -> io::Result<()>
where
    A: Acceptor + Send,
    S: Into<Service> + Send,
    F: Future<Output = ShutdownReason> + Send,
{
    serve_with_ingress(
        server,
        service,
        coordinator,
        signal,
        graceful_timeout,
        |stop| crate::server_tunnels::TunnelSupervisor::default().run(stop),
    )
    .await
}

/// The ingress owner is polled alongside HTTP for its entire lifetime. Its stop
/// phase must complete before HTTP admission closes; abrupt loss drops the owned
/// future (and therefore every Tunnel owner) instead of detaching a poller.
async fn serve_with_ingress<A, S, F, I, T>(
    server: Server<A>,
    service: S,
    coordinator: Arc<ShutdownCoordinator>,
    signal: F,
    graceful_timeout: Duration,
    start_ingress: I,
) -> io::Result<()>
where
    A: Acceptor + Send,
    S: Into<Service> + Send,
    F: Future<Output = ShutdownReason> + Send,
    I: FnOnce(tokio::sync::watch::Receiver<bool>) -> T,
    T: Future<Output = ()> + Send,
{
    let handle = server.handle();
    let serve = server.try_serve(service);
    tokio::pin!(serve);
    tokio::pin!(signal);

    let (stop, stopped) = tokio::sync::watch::channel(false);
    let mut ingress = Box::pin(start_ingress(stopped));
    let mut ingress_finished = false;
    let reason = loop {
        tokio::select! {
            result = &mut serve => {
                let _ = stop.send(true);
                coordinator.notify_background_services_stop(ShutdownReason::ServiceStop);
                if !ingress_finished {
                    let _ = tokio::time::timeout(Duration::from_secs(15), &mut ingress).await;
                }
                coordinator.mark_stopped();
                return result;
            }
            reason = &mut signal => break reason,
            _ = &mut ingress, if !ingress_finished => ingress_finished = true,
        }
    };

    coordinator.notify_background_services_stop(reason);
    let _ = stop.send(true);
    let mut http_result = None;
    if !ingress_finished {
        tracing::info!(
            shutdown_signal = reason.as_str(),
            "Stopping Tunnel ingress before HTTP drain"
        );
        let settle = async {
            loop {
                tokio::select! {
                    _ = &mut ingress => break,
                    result = &mut serve, if http_result.is_none() => http_result = Some(result),
                }
            }
        };
        if tokio::time::timeout(Duration::from_secs(15), settle)
            .await
            .is_err()
        {
            tracing::warn!(
                "Tunnel owner shutdown deadline elapsed; retain unconfirmed restart fences"
            );
        }
    }
    drop(ingress);
    if let Some(result) = http_result {
        coordinator.mark_stopped();
        return result;
    }

    if coordinator.begin_draining() {
        tracing::info!(
            shutdown_signal = reason.as_str(),
            graceful_deadline_seconds = graceful_timeout.as_secs(),
            shutdown_state = "draining",
            "Server graceful shutdown requested"
        );
        handle.stop_graceful(Some(graceful_timeout));
    }

    let started = Instant::now();
    let deadline = tokio::time::sleep(graceful_timeout);
    tokio::pin!(deadline);
    let (result, deadline_elapsed) = tokio::select! {
        result = &mut serve => (result, false),
        _ = &mut deadline => {
            tracing::warn!(
                shutdown_signal = reason.as_str(),
                elapsed_ms = started.elapsed().as_millis() as u64,
                graceful_deadline_seconds = graceful_timeout.as_secs(),
                "Server graceful shutdown deadline reached; Salvo force-stop escalation is active"
            );
            // `stop_graceful(Some(timeout))` owns the force-stop timer. Keep
            // awaiting the same serve future instead of dropping/cancelling it
            // or duplicating Salvo's escalation lifecycle.
            (serve.await, true)
        }
    };
    coordinator.mark_stopped();
    let elapsed = started.elapsed();
    if deadline_elapsed {
        tracing::warn!(
            shutdown_signal = reason.as_str(),
            elapsed_ms = elapsed.as_millis() as u64,
            graceful_deadline_seconds = graceful_timeout.as_secs(),
            shutdown_state = "stopped",
            "Server shutdown completed after graceful deadline escalation"
        );
    } else {
        tracing::info!(
            shutdown_signal = reason.as_str(),
            elapsed_ms = elapsed.as_millis() as u64,
            graceful_deadline_seconds = graceful_timeout.as_secs(),
            shutdown_state = "stopped",
            "Server graceful shutdown completed"
        );
    }
    if reason == ShutdownReason::PersistenceFailure {
        return match result {
            Ok(()) => Err(persistence_failure_exit_error()),
            Err(error) => Err(error),
        };
    }
    result
}

#[cfg(test)]
mod tests;
