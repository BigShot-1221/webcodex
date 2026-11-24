//! Persistent Runner-owned MCP providers for the built-in MCP gateway.
//!
//! Each configured provider has one process-lifetime logical identity. Its stdio
//! or Streamable HTTP connection is initialized lazily and reused while healthy.
//! A fatal protocol/transport failure retires only that connection: the failed
//! request is never replayed, while a later explicit request may establish a
//! fresh connection under the same provider identity and revalidate tool schema
//! before dispatch.

use super::config::{
    validate_mcp_gateway_http_url, McpGatewayConfig, McpGatewayProviderConfig,
    McpGatewayProviderTransport, MCP_GATEWAY_HTTP_MAPPED_VALUE_MAX_BYTES,
    MCP_GATEWAY_MAX_CWD_BYTES,
};
#[cfg(windows)]
use super::shell::env_keys_equal;
use super::shell::is_sensitive_env_key;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::process::{ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{mpsc, Arc, Mutex, RwLock, TryLockError};
use std::time::{Duration, Instant};
use webcodex_core::mcp_gateway::{
    validate_json_value, validate_request, validate_tool_result,
    validate_tools_with_description_limit, McpGatewayContent, McpGatewayDispatchState,
    McpGatewayProvider, McpGatewayProviderState, McpGatewayRequest, McpGatewayResponse,
    McpGatewayResponsePayload, McpGatewayTool, McpGatewayToolResult, MCP_GATEWAY_MAX_MESSAGE_BYTES,
    MCP_GATEWAY_MAX_PROVIDER_MESSAGE_BYTES, MCP_GATEWAY_MAX_RESULT_BYTES,
    MCP_GATEWAY_MAX_TOOL_COUNT,
};
use webcodex_process::ManagedChild;

const MCP_PROTOCOL_VERSION: &str = "2025-06-18";
const SUPPORTED_MCP_PROTOCOL_VERSIONS: [&str; 4] = [
    MCP_PROTOCOL_VERSION,
    "2025-11-25",
    "2025-03-26",
    "2024-11-05",
];

fn protocol_version_requires_http_header(version: &str) -> bool {
    // The HTTP protocol-version header is required for negotiated versions
    // from 2025-06-18 onward; earlier handshake versions do not define it.
    matches!(version, "2025-06-18" | "2025-11-25")
}
const MCP_GATEWAY_MAX_IGNORED_NOTIFICATIONS: usize = 32;
// Limit bursts, not lifetime totals: an idle provider may ping indefinitely.
const MCP_GATEWAY_UNSOLICITED_WINDOW: Duration = Duration::from_secs(5);
const MCP_GATEWAY_MAX_PROVIDER_REQUEST_ID_BYTES: usize = 256;
const PROVIDER_NEVER_STARTED: u8 = 0;
const PROVIDER_HEALTHY: u8 = 1;
const PROVIDER_CONNECTION_RETIRED: u8 = 2;

pub(crate) struct McpGatewayManager {
    state: RwLock<McpGatewayState>,
    stopping: AtomicBool,
}

struct McpGatewayState {
    providers: BTreeMap<String, Arc<ProviderEntry>>,
    request_timeout: Duration,
    max_tool_description_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct McpGatewayReloadSummary {
    pub(crate) preserved: usize,
    pub(crate) replaced: usize,
    pub(crate) added: usize,
    pub(crate) removed: usize,
}

struct ProviderEntry {
    config: McpGatewayProviderConfig,
    instance_id: String,
    lifecycle: AtomicU8,
    accepting_requests: AtomicBool,
    session: Mutex<Option<ProviderConnection>>,
}

enum ProviderConnection {
    Stdio(StdioProviderConnection),
    Http(HttpProviderConnection),
}

struct StdioProviderConnection {
    child: ManagedChild,
    stdin: Arc<Mutex<ChildStdin>>,
    incoming: mpsc::Receiver<ReaderEvent>,
    next_id: u64,
}

enum ReaderEvent {
    Message(Result<Value, ReaderFault>),
    Eof,
}

#[derive(Debug, Clone, Copy)]
enum ReaderFault {
    Malformed,
    TooLarge,
    Io,
    Stdin,
    NotificationFlood,
}

struct ProviderFailure {
    code: &'static str,
    dispatch_state: McpGatewayDispatchState,
    fatal: bool,
    detail: Option<String>,
}

impl ProviderFailure {
    fn before_send(code: &'static str) -> Self {
        Self {
            code,
            dispatch_state: McpGatewayDispatchState::NotStarted,
            fatal: true,
            detail: None,
        }
    }

    fn after_send(code: &'static str) -> Self {
        Self {
            code,
            dispatch_state: McpGatewayDispatchState::OutcomeUnknown,
            fatal: true,
            detail: None,
        }
    }

    fn completed(code: &'static str) -> Self {
        Self {
            code,
            dispatch_state: McpGatewayDispatchState::Completed,
            // A correlated JSON-RPC response proves this request-response
            // exchange completed. Result-level V1 incompatibility must not
            // poison the whole provider instance; only transport/protocol
            // failures that lose correlation retire the session.
            fatal: false,
            detail: None,
        }
    }

    fn with_detail(mut self, detail: impl Into<String>) -> Self {
        let mut detail = detail.into();
        if detail.len() > 512 {
            let mut end = 512;
            while !detail.is_char_boundary(end) {
                end -= 1;
            }
            detail.truncate(end);
        }
        self.detail = Some(detail);
        self
    }

    fn not_started(code: &'static str) -> Self {
        Self {
            code,
            dispatch_state: McpGatewayDispatchState::NotStarted,
            fatal: false,
            detail: None,
        }
    }

    fn preflight(mut error: Self) -> Self {
        // A schema preflight may itself have been dispatched upstream, but the
        // model-requested effectful tools/call has definitely not started.
        error.dispatch_state = McpGatewayDispatchState::NotStarted;
        error
    }

    fn rpc_error() -> Self {
        Self {
            code: "provider_rpc_error",
            dispatch_state: McpGatewayDispatchState::Completed,
            fatal: false,
            detail: None,
        }
    }
}

impl McpGatewayManager {
    pub(crate) fn new(config: &McpGatewayConfig) -> Self {
        let providers = config
            .providers
            .iter()
            .cloned()
            .map(|provider| (provider.id.clone(), Arc::new(ProviderEntry::new(provider))))
            .collect();
        Self {
            state: RwLock::new(McpGatewayState {
                providers,
                request_timeout: Duration::from_secs(config.request_timeout_secs.clamp(1, 120)),
                max_tool_description_bytes: config.max_tool_description_bytes,
            }),
            stopping: AtomicBool::new(false),
        }
    }

    /// Exact process-lifetime provider inventory projected in normal Runner
    /// registration. Reading it does not start a provider process and does not
    /// expose executable, argv, environment, PID, stderr, or secret material.
    pub(crate) fn provider_inventory(&self) -> Vec<McpGatewayProvider> {
        self.state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .providers
            .values()
            .map(|provider| provider.advertisement())
            .collect()
    }

    /// Atomically replace the configured provider set without retargeting any
    /// exact provider identity. Providers whose complete local config is
    /// unchanged retain their instance id and live connection. Changed/new
    /// providers receive fresh instance ids; removed/replaced connections are
    /// retired after the new routing snapshot is committed.
    pub(crate) fn apply_config_candidate(
        &self,
        config: &McpGatewayConfig,
    ) -> Result<McpGatewayReloadSummary, &'static str> {
        if self.stopping.load(Ordering::SeqCst) {
            return Err("mcp_gateway_stopping");
        }
        let mut retired = Vec::new();
        let summary;
        {
            let mut state = self
                .state
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if self.stopping.load(Ordering::SeqCst) {
                return Err("mcp_gateway_stopping");
            }
            let mut current = std::mem::take(&mut state.providers);
            let mut next = BTreeMap::new();
            let mut preserved = 0usize;
            let mut replaced = 0usize;
            let mut added = 0usize;
            for provider_config in &config.providers {
                match current.remove(&provider_config.id) {
                    Some(existing) if existing.config == *provider_config => {
                        preserved += 1;
                        next.insert(provider_config.id.clone(), existing);
                    }
                    Some(existing) => {
                        replaced += 1;
                        existing.accepting_requests.store(false, Ordering::SeqCst);
                        retired.push(existing);
                        next.insert(
                            provider_config.id.clone(),
                            Arc::new(ProviderEntry::new(provider_config.clone())),
                        );
                    }
                    None => {
                        added += 1;
                        next.insert(
                            provider_config.id.clone(),
                            Arc::new(ProviderEntry::new(provider_config.clone())),
                        );
                    }
                }
            }
            let removed = current.len();
            for existing in current.into_values() {
                existing.accepting_requests.store(false, Ordering::SeqCst);
                retired.push(existing);
            }
            state.providers = next;
            state.request_timeout = Duration::from_secs(config.request_timeout_secs.clamp(1, 120));
            state.max_tool_description_bytes = config.max_tool_description_bytes;
            summary = McpGatewayReloadSummary {
                preserved,
                replaced,
                added,
                removed,
            };
        }
        for provider in retired {
            provider.retire_connection_nonblocking();
        }
        Ok(summary)
    }

    pub(crate) fn handle(&self, request: McpGatewayRequest) -> McpGatewayResponse {
        if self.stopping.load(Ordering::SeqCst) {
            return bridge_error(
                McpGatewayDispatchState::NotStarted,
                "runner_stopping",
                "Runner is stopping; provider request was not started",
            );
        }
        if validate_request(&request).is_err() {
            return bridge_error(
                McpGatewayDispatchState::NotStarted,
                "invalid_bridge_request",
                "Bridge request failed bounded validation",
            );
        }
        match request {
            McpGatewayRequest::ProviderStatus {
                provider_id,
                provider_instance_id,
            } => {
                let Some((provider, _, _)) =
                    self.exact_provider(&provider_id, &provider_instance_id)
                else {
                    return stale_provider();
                };
                McpGatewayResponse::success(McpGatewayResponsePayload::ProviderStatus {
                    state: provider.lifecycle_state(),
                })
            }
            McpGatewayRequest::ToolsList {
                provider_id,
                provider_instance_id,
            } => {
                let Some((provider, timeout, max_description_bytes)) =
                    self.exact_provider(&provider_id, &provider_instance_id)
                else {
                    return stale_provider();
                };
                match provider.with_connection(
                    timeout,
                    || self.check_provider_admission(&provider_id, &provider_instance_id),
                    |connection, timeout| connection.tools_list(timeout, max_description_bytes),
                ) {
                    Ok(tools) => {
                        McpGatewayResponse::success(McpGatewayResponsePayload::Tools { tools })
                    }
                    Err(error) => provider_failure_response(error),
                }
            }
            McpGatewayRequest::ToolsCall {
                provider_id,
                provider_instance_id,
                name,
                arguments,
                expected_schema,
                allow_resource_links,
            } => {
                let Some((provider, timeout, max_description_bytes)) =
                    self.exact_provider(&provider_id, &provider_instance_id)
                else {
                    return stale_provider();
                };
                match provider.with_connection(
                    timeout,
                    || self.check_provider_admission(&provider_id, &provider_instance_id),
                    |connection, timeout| {
                        let started = Instant::now();
                        let tools = connection
                            .tools_list(timeout, max_description_bytes)
                            .map_err(ProviderFailure::preflight)?;
                        let Some(current) = tools.iter().find(|tool| tool.name == name) else {
                            return Err(ProviderFailure::not_started("provider_tool_missing"));
                        };
                        if current.schema_observation() != expected_schema {
                            return Err(ProviderFailure::not_started("provider_schema_changed"));
                        }
                        let Some(remaining) = timeout.checked_sub(started.elapsed()) else {
                            return Err(ProviderFailure::not_started("provider_timeout"));
                        };
                        if remaining.is_zero() {
                            return Err(ProviderFailure::not_started("provider_timeout"));
                        }
                        self.commit_provider_dispatch(&provider)?;
                        connection.tools_call(&name, arguments, remaining, allow_resource_links)
                    },
                ) {
                    Ok(result) => {
                        McpGatewayResponse::success(McpGatewayResponsePayload::ToolResult {
                            result,
                        })
                    }
                    Err(error) => provider_failure_response(error),
                }
            }
        }
    }

    fn check_provider_admission(
        &self,
        provider_id: &str,
        provider_instance_id: &str,
    ) -> Result<(), ProviderFailure> {
        if self.stopping.load(Ordering::SeqCst) {
            return Err(ProviderFailure::not_started("runner_stopping"));
        }
        self.exact_provider(provider_id, provider_instance_id)
            .map(|_| ())
            .ok_or_else(|| ProviderFailure::not_started("stale_provider"))
    }

    fn commit_provider_dispatch(&self, provider: &ProviderEntry) -> Result<(), ProviderFailure> {
        if self.stopping.load(Ordering::SeqCst) {
            return Err(ProviderFailure::not_started("runner_stopping"));
        }
        // Config replacement flips this bit while still holding the routing
        // write lock. This SeqCst load is the effect-dispatch linearization
        // point: earlier retirement fences the call; later retirement is
        // ordered after this request was admitted for dispatch.
        if !provider.accepting_requests.load(Ordering::SeqCst) {
            return Err(ProviderFailure::not_started("stale_provider"));
        }
        Ok(())
    }

    fn exact_provider(
        &self,
        provider_id: &str,
        provider_instance_id: &str,
    ) -> Option<(Arc<ProviderEntry>, Duration, usize)> {
        let state = self
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let provider = state.providers.get(provider_id)?;
        if provider.instance_id != provider_instance_id {
            return None;
        }
        Some((
            Arc::clone(provider),
            provider.request_timeout(state.request_timeout),
            state.max_tool_description_bytes,
        ))
    }

    pub(crate) fn shutdown(&self) {
        if self.stopping.swap(true, Ordering::SeqCst) {
            return;
        }
        let providers = self
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .providers
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for provider in providers {
            provider.shutdown_connection();
        }
    }
}

impl Drop for McpGatewayManager {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl ProviderEntry {
    fn new(config: McpGatewayProviderConfig) -> Self {
        Self {
            config,
            instance_id: uuid::Uuid::new_v4().simple().to_string(),
            lifecycle: AtomicU8::new(PROVIDER_NEVER_STARTED),
            accepting_requests: AtomicBool::new(true),
            session: Mutex::new(None),
        }
    }

    fn retire_connection_nonblocking(&self) {
        self.accepting_requests.store(false, Ordering::SeqCst);
        self.lifecycle
            .store(PROVIDER_CONNECTION_RETIRED, Ordering::SeqCst);
        match self.session.try_lock() {
            Ok(mut session) => drop(session.take()),
            Err(TryLockError::WouldBlock) => {
                // An already-dispatched request owns the connection. Do not
                // interrupt it or block config reload; once that request drops
                // the last Arc<ProviderEntry>, the old connection is retired.
            }
            Err(TryLockError::Poisoned(poisoned)) => {
                let mut session = poisoned.into_inner();
                drop(session.take());
            }
        }
    }

    fn shutdown_connection(&self) {
        let mut session = self
            .session
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        drop(session.take());
        self.lifecycle
            .store(PROVIDER_CONNECTION_RETIRED, Ordering::SeqCst);
    }

    fn advertisement(&self) -> McpGatewayProvider {
        McpGatewayProvider {
            provider_id: self.config.id.clone(),
            provider_instance_id: self.instance_id.clone(),
            name: self.config.name.clone(),
        }
    }

    fn request_timeout(&self, default: Duration) -> Duration {
        self.config
            .timeout_secs
            .map(Duration::from_secs)
            .unwrap_or(default)
    }

    fn lifecycle_state(&self) -> McpGatewayProviderState {
        let mut session = match self.session.try_lock() {
            Ok(session) => session,
            Err(TryLockError::WouldBlock) => return McpGatewayProviderState::Busy,
            Err(TryLockError::Poisoned(_)) => return McpGatewayProviderState::ConnectionRetired,
        };
        if let Some(connection) = session.as_mut() {
            match connection {
                ProviderConnection::Stdio(stdio) => match stdio.child.try_wait() {
                    Ok(None) => {
                        self.lifecycle.store(PROVIDER_HEALTHY, Ordering::SeqCst);
                        return McpGatewayProviderState::Healthy;
                    }
                    Ok(Some(_)) | Err(_) => {
                        drop(session.take());
                        self.lifecycle
                            .store(PROVIDER_CONNECTION_RETIRED, Ordering::SeqCst);
                        return McpGatewayProviderState::ConnectionRetired;
                    }
                },
                ProviderConnection::Http(_) => {
                    self.lifecycle.store(PROVIDER_HEALTHY, Ordering::SeqCst);
                    return McpGatewayProviderState::Healthy;
                }
            }
        }
        match self.lifecycle.load(Ordering::SeqCst) {
            PROVIDER_NEVER_STARTED => McpGatewayProviderState::NeverStarted,
            PROVIDER_HEALTHY | PROVIDER_CONNECTION_RETIRED => {
                McpGatewayProviderState::ConnectionRetired
            }
            _ => McpGatewayProviderState::ConnectionRetired,
        }
    }

    fn with_connection<T>(
        &self,
        timeout: Duration,
        admission_check: impl Fn() -> Result<(), ProviderFailure>,
        operation: impl FnOnce(&mut ProviderConnection, Duration) -> Result<T, ProviderFailure>,
    ) -> Result<T, ProviderFailure> {
        let started = Instant::now();
        let admission_budget = timeout.min(Duration::from_secs(2));
        let mut session = loop {
            admission_check()?;
            match self.session.try_lock() {
                Ok(session) => break session,
                Err(TryLockError::WouldBlock) => {
                    let Some(remaining) = admission_budget
                        .checked_sub(started.elapsed())
                        .filter(|remaining| !remaining.is_zero())
                    else {
                        return Err(ProviderFailure::not_started("provider_busy"));
                    };
                    std::thread::sleep(remaining.min(Duration::from_millis(10)));
                }
                Err(TryLockError::Poisoned(_)) => {
                    return Err(ProviderFailure::before_send("provider_unavailable"));
                }
            }
        };
        admission_check()?;
        let remaining = timeout
            .checked_sub(started.elapsed())
            .filter(|remaining| !remaining.is_zero())
            .ok_or_else(|| ProviderFailure::not_started("provider_busy"))?;
        if session.is_none() {
            match ProviderConnection::connect(&self.config, remaining) {
                Ok(connection) => {
                    self.lifecycle.store(PROVIDER_HEALTHY, Ordering::SeqCst);
                    *session = Some(connection);
                }
                Err(mut error) => {
                    self.lifecycle
                        .store(PROVIDER_CONNECTION_RETIRED, Ordering::SeqCst);
                    // Initialization is provider lifecycle setup, not the
                    // requested tools/list or tools/call. Even if initialize
                    // reached the child, the caller's operation did not. A
                    // later explicit request may attempt a fresh connection.
                    error.dispatch_state = McpGatewayDispatchState::NotStarted;
                    return Err(error);
                }
            }
        }
        let Some(remaining) = timeout
            .checked_sub(started.elapsed())
            .filter(|remaining| !remaining.is_zero())
        else {
            self.lifecycle
                .store(PROVIDER_CONNECTION_RETIRED, Ordering::SeqCst);
            if let Some(mut connection) = session.take() {
                connection.retire();
            }
            return Err(ProviderFailure::before_send("provider_timeout"));
        };
        admission_check()?;
        let result = operation(
            session.as_mut().expect("provider connection initialized"),
            remaining,
        );
        if result.as_ref().is_err_and(|error| error.fatal) {
            self.lifecycle
                .store(PROVIDER_CONNECTION_RETIRED, Ordering::SeqCst);
            // Retire only the desynchronized connection. Never replay the
            // failed request here; a later explicit request may spawn a fresh
            // connection under the same logical provider identity.
            if let Some(mut connection) = session.take() {
                connection.retire();
            }
        }
        result
    }
}

impl ProviderConnection {
    fn connect(
        config: &McpGatewayProviderConfig,
        timeout: Duration,
    ) -> Result<Self, ProviderFailure> {
        match config.transport {
            McpGatewayProviderTransport::Stdio => {
                StdioProviderConnection::spawn(config, timeout).map(ProviderConnection::Stdio)
            }
            McpGatewayProviderTransport::StreamableHttp => {
                HttpProviderConnection::connect(config, timeout).map(ProviderConnection::Http)
            }
        }
    }

    fn tools_list(
        &mut self,
        timeout: Duration,
        max_description_bytes: usize,
    ) -> Result<Vec<McpGatewayTool>, ProviderFailure> {
        match self {
            Self::Stdio(connection) => connection.tools_list(timeout, max_description_bytes),
            Self::Http(connection) => connection.tools_list(timeout, max_description_bytes),
        }
    }

    fn tools_call(
        &mut self,
        name: &str,
        arguments: Value,
        timeout: Duration,
        allow_resource_links: bool,
    ) -> Result<McpGatewayToolResult, ProviderFailure> {
        match self {
            Self::Stdio(connection) => {
                connection.tools_call(name, arguments, timeout, allow_resource_links)
            }
            Self::Http(connection) => {
                connection.tools_call(name, arguments, timeout, allow_resource_links)
            }
        }
    }

    fn retire(&mut self) {
        match self {
            Self::Stdio(connection) => connection.terminate(),
            Self::Http(connection) => connection.retire(),
        }
    }
}

fn resolve_provider_environment(
    config: &McpGatewayProviderConfig,
) -> Result<Vec<(String, std::ffi::OsString)>, ProviderFailure> {
    let mut resolved = Vec::with_capacity(config.env_from_env.len() + usize::from(cfg!(windows)));
    #[cfg(windows)]
    if !config
        .env_from_env
        .keys()
        .any(|destination| env_keys_equal(destination, "SYSTEMROOT"))
    {
        // Keep Windows process bootstrap usable after env_clear() without
        // inheriting PATH, user profile data, proxy settings, or credentials.
        // Operators may still explicitly map SYSTEMROOT to another source.
        if let Some(system_root) = std::env::var_os("SYSTEMROOT") {
            resolved.push(("SYSTEMROOT".to_string(), system_root));
        }
    }
    for (destination, source) in &config.env_from_env {
        // Keep the Runner transport/account secret invariant authoritative even
        // if a caller constructs config without going through load_config.
        if is_sensitive_env_key(destination) || is_sensitive_env_key(source) {
            return Err(ProviderFailure::before_send("provider_env_forbidden"));
        }
        let Some(value) = std::env::var_os(source) else {
            return Err(ProviderFailure::before_send("provider_env_missing"));
        };
        resolved.push((destination.clone(), value));
    }
    Ok(resolved)
}

fn resolve_provider_cwd(
    config: &McpGatewayProviderConfig,
) -> Result<Option<&std::path::Path>, ProviderFailure> {
    let Some(cwd) = config.cwd.as_deref() else {
        return Ok(None);
    };
    let path = std::path::Path::new(cwd);
    if cwd.is_empty()
        || cwd.len() > MCP_GATEWAY_MAX_CWD_BYTES
        || cwd.contains('\0')
        || !path.is_absolute()
    {
        return Err(ProviderFailure::before_send("provider_cwd_invalid"));
    }
    match std::fs::metadata(path) {
        Ok(metadata) if metadata.is_dir() => Ok(Some(path)),
        _ => Err(ProviderFailure::before_send("provider_cwd_unavailable")),
    }
}

fn validate_provider_tool_result_structure(result: &Value) -> Result<(), ProviderFailure> {
    // Preserve the existing JSON depth/node/string and 512 KiB non-image
    // defenses while exempting only standard image.data from the generic
    // JSON-string ceiling. The typed image validator applies its own stricter
    // base64/decoded aggregate budget below.
    let mut structural = result.clone();
    if let Some(items) = structural.get_mut("content").and_then(Value::as_array_mut) {
        for item in items {
            let is_image = item.get("type").and_then(Value::as_str) == Some("image");
            if is_image {
                if let Some(data) = item
                    .as_object_mut()
                    .and_then(|object| object.get_mut("data"))
                {
                    if data.is_string() {
                        *data = Value::String(String::new());
                    }
                }
            }
        }
    }
    validate_json_value(
        &structural,
        MCP_GATEWAY_MAX_RESULT_BYTES,
        "provider tool result",
    )
    .map_err(|_| ProviderFailure::completed("invalid_provider_result"))
}

fn parse_tools_list_result(
    result: Value,
    max_description_bytes: usize,
) -> Result<Vec<McpGatewayTool>, ProviderFailure> {
    let encoded = serde_json::to_vec(&result)
        .map_err(|_| ProviderFailure::completed("invalid_provider_tools"))?;
    if encoded.len() > MCP_GATEWAY_MAX_MESSAGE_BYTES {
        return Err(ProviderFailure::completed("invalid_provider_tools"));
    }
    let object = result
        .as_object()
        .ok_or_else(|| ProviderFailure::completed("invalid_provider_tools"))?;
    if object
        .get("nextCursor")
        .is_some_and(|cursor| !cursor.is_null())
    {
        return Err(ProviderFailure::completed(
            "provider_pagination_unsupported",
        ));
    }
    let raw_tools = object
        .get("tools")
        .and_then(Value::as_array)
        .ok_or_else(|| ProviderFailure::completed("invalid_provider_tools"))?;
    if raw_tools.len() > MCP_GATEWAY_MAX_TOOL_COUNT {
        return Err(ProviderFailure::completed("invalid_provider_tools"));
    }
    let mut envelope = object.clone();
    envelope.insert("tools".to_string(), json!([]));
    validate_json_value(
        &Value::Object(envelope),
        MCP_GATEWAY_MAX_MESSAGE_BYTES,
        "provider tools response",
    )
    .map_err(|_| ProviderFailure::completed("invalid_provider_tools"))?;
    let mut tools = Vec::with_capacity(raw_tools.len());
    for raw in raw_tools {
        validate_json_value(
            &json!({"tools": [raw]}),
            MCP_GATEWAY_MAX_MESSAGE_BYTES,
            "provider tool",
        )
        .map_err(|_| ProviderFailure::completed("invalid_provider_tools"))?;
        let object = raw
            .as_object()
            .ok_or_else(|| ProviderFailure::completed("invalid_provider_tools"))?;
        let name = object
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| ProviderFailure::completed("invalid_provider_tools"))?;
        let title = object
            .get("title")
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_string)
                    .ok_or_else(|| ProviderFailure::completed("invalid_provider_tools"))
            })
            .transpose()?;
        let description = object
            .get("description")
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_string)
                    .ok_or_else(|| ProviderFailure::completed("invalid_provider_tools"))
            })
            .transpose()?;
        let input_schema = object
            .get("inputSchema")
            .cloned()
            .ok_or_else(|| ProviderFailure::completed("invalid_provider_tools"))?;
        let output_schema = object.get("outputSchema").cloned();
        let annotations = object.get("annotations").cloned();
        let meta = object.get("_meta").cloned();
        tools.push(McpGatewayTool {
            name: name.to_string(),
            title,
            description,
            input_schema,
            output_schema,
            annotations,
            meta,
        });
    }
    validate_tools_with_description_limit(&tools, max_description_bytes)
        .map_err(|error| ProviderFailure::completed("invalid_provider_tools").with_detail(error))?;
    Ok(tools)
}

fn parse_tools_call_result(
    result: Value,
    allow_resource_links: bool,
) -> Result<McpGatewayToolResult, ProviderFailure> {
    validate_provider_tool_result_structure(&result)?;
    let object = result
        .as_object()
        .ok_or_else(|| ProviderFailure::completed("invalid_provider_result"))?;
    let raw_content = object
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(|| ProviderFailure::completed("invalid_provider_result"))?;
    let mut content = Vec::with_capacity(raw_content.len());
    for item in raw_content {
        let item = item
            .as_object()
            .ok_or_else(|| ProviderFailure::completed("invalid_provider_result"))?;
        let content_type = item
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| ProviderFailure::completed("invalid_provider_result"))?;
        match content_type {
            "text" => {
                let text = item
                    .get("text")
                    .and_then(Value::as_str)
                    .ok_or_else(|| ProviderFailure::completed("invalid_provider_result"))?;
                content.push(McpGatewayContent::Text {
                    text: text.to_string(),
                });
            }
            "image" => {
                let data = item
                    .get("data")
                    .and_then(Value::as_str)
                    .ok_or_else(|| ProviderFailure::completed("invalid_provider_result"))?;
                let mime_type = item
                    .get("mimeType")
                    .and_then(Value::as_str)
                    .ok_or_else(|| ProviderFailure::completed("invalid_provider_result"))?;
                content.push(McpGatewayContent::Image {
                    data: data.to_string(),
                    mime_type: mime_type.to_string(),
                });
            }
            "resource_link" => {
                if !allow_resource_links {
                    return Err(ProviderFailure::completed("unsupported_provider_content"));
                }
                let link = serde_json::from_value(Value::Object(item.clone()))
                    .map_err(|_| ProviderFailure::completed("invalid_provider_result"))?;
                content.push(link);
            }
            _ => {
                return Err(ProviderFailure::completed("unsupported_provider_content"));
            }
        }
    }
    let result = McpGatewayToolResult {
        content,
        structured_content: object.get("structuredContent").cloned(),
        is_error: object
            .get("isError")
            .map(|value| {
                value
                    .as_bool()
                    .ok_or_else(|| ProviderFailure::completed("invalid_provider_result"))
            })
            .transpose()?
            .unwrap_or(false),
    };
    validate_tool_result(&result)
        .map_err(|_| ProviderFailure::completed("invalid_provider_result"))?;
    Ok(result)
}

impl StdioProviderConnection {
    fn spawn(
        config: &McpGatewayProviderConfig,
        timeout: Duration,
    ) -> Result<Self, ProviderFailure> {
        // Resolve the complete operator-declared execution context before
        // creating the child. Missing env sources or unavailable cwd therefore
        // cannot produce a partially initialized provider process.
        let environment = resolve_provider_environment(config)?;
        let cwd = resolve_provider_cwd(config)?;
        let mut command = Command::new(&config.executable);
        command
            .args(&config.args)
            // Never inherit the Runner process environment implicitly. The
            // resolved environment below is limited to explicit env_from_env
            // mappings plus the minimal non-secret Windows OS bootstrap.
            .env_clear();
        for (destination, value) in environment {
            command.env(destination, value);
        }
        if let Some(cwd) = cwd {
            command.current_dir(cwd);
        }
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = ManagedChild::spawn_background(&mut command)
            .map_err(|_| ProviderFailure::before_send("provider_spawn_failed"))?;
        let stdin = child
            .child_mut()
            .stdin
            .take()
            .ok_or_else(|| ProviderFailure::before_send("provider_stdio_unavailable"))?;
        let stdout = child
            .child_mut()
            .stdout
            .take()
            .ok_or_else(|| ProviderFailure::before_send("provider_stdio_unavailable"))?;
        let stderr = child
            .child_mut()
            .stderr
            .take()
            .ok_or_else(|| ProviderFailure::before_send("provider_stdio_unavailable"))?;
        let (sender, incoming) = mpsc::sync_channel(8);
        let stdin = Arc::new(Mutex::new(stdin));
        let reader_stdin = Arc::clone(&stdin);
        std::thread::Builder::new()
            .name(format!("wc-mcp-{}", config.id))
            .spawn(move || provider_stdout_reader(stdout, sender, reader_stdin))
            .map_err(|_| ProviderFailure::before_send("provider_reader_unavailable"))?;
        std::thread::Builder::new()
            .name(format!("wc-mcp-stderr-{}", config.id))
            .spawn(move || {
                // Stderr is diagnostic-only and intentionally discarded. It
                // never enters protocol parsing, HTTP results, or audit data.
                let _ = std::io::copy(&mut BufReader::new(stderr), &mut std::io::sink());
            })
            .map_err(|_| ProviderFailure::before_send("provider_reader_unavailable"))?;

        let mut connection = Self {
            child,
            stdin,
            incoming,
            next_id: 1,
        };
        let initialized = connection.request(
            "initialize",
            json!({
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {
                    "name": "webcodex-runner-mcp-gateway",
                    "version": env!("CARGO_PKG_VERSION")
                }
            }),
            timeout,
        )?;
        validate_initialize_result(&initialized)?;
        connection.send_notification("notifications/initialized", json!({}))?;
        Ok(connection)
    }

    fn tools_list(
        &mut self,
        timeout: Duration,
        max_description_bytes: usize,
    ) -> Result<Vec<McpGatewayTool>, ProviderFailure> {
        let result = self.request("tools/list", json!({}), timeout)?;
        parse_tools_list_result(result, max_description_bytes)
    }

    fn tools_call(
        &mut self,
        name: &str,
        arguments: Value,
        timeout: Duration,
        allow_resource_links: bool,
    ) -> Result<McpGatewayToolResult, ProviderFailure> {
        let params = json!({"name": name, "arguments": arguments});
        let result = self.request("tools/call", params, timeout)?;
        parse_tools_call_result(result, allow_resource_links)
    }

    fn request(
        &mut self,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> Result<Value, ProviderFailure> {
        let mut ignored_notifications = 0usize;
        loop {
            match self.incoming.try_recv() {
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) | Ok(ReaderEvent::Eof) => {
                    return Err(ProviderFailure::before_send("provider_eof"));
                }
                Ok(ReaderEvent::Message(Ok(message))) => {
                    if is_provider_notification(&message) {
                        ignored_notifications += 1;
                        if ignored_notifications > MCP_GATEWAY_MAX_IGNORED_NOTIFICATIONS {
                            return Err(ProviderFailure::before_send(
                                "provider_notification_flood",
                            ));
                        }
                        continue;
                    }
                    if message.get("method").is_some() {
                        return Err(ProviderFailure::before_send(
                            "provider_callbacks_unsupported",
                        ));
                    }
                    let response_id = message
                        .get("id")
                        .and_then(Value::as_u64)
                        .unwrap_or(u64::MAX);
                    return Err(ProviderFailure::before_send(
                        if response_id < self.next_id {
                            "provider_duplicate_response_id"
                        } else {
                            "provider_unknown_response_id"
                        },
                    ));
                }
                Ok(ReaderEvent::Message(Err(ReaderFault::Malformed))) => {
                    return Err(ProviderFailure::before_send("provider_malformed_json"));
                }
                Ok(ReaderEvent::Message(Err(ReaderFault::TooLarge))) => {
                    return Err(ProviderFailure::before_send("provider_message_too_large"));
                }
                Ok(ReaderEvent::Message(Err(ReaderFault::NotificationFlood))) => {
                    return Err(ProviderFailure::before_send("provider_notification_flood"));
                }
                Ok(ReaderEvent::Message(Err(ReaderFault::Io))) => {
                    return Err(ProviderFailure::before_send("provider_stdout_failed"));
                }
                Ok(ReaderEvent::Message(Err(ReaderFault::Stdin))) => {
                    return Err(ProviderFailure::before_send("provider_stdin_failed"));
                }
            }
        }

        let id = self.next_id;
        self.next_id = id.checked_add(1).ok_or_else(|| {
            // Reusing a saturated ID would lose request-response correlation.
            // Retire this connection before writing; only a later explicit
            // request may reconnect, never replay the exhausted request.
            ProviderFailure::before_send("provider_request_id_exhausted")
        })?;
        let message = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        let mut encoded = serde_json::to_vec(&message)
            .map_err(|_| ProviderFailure::before_send("provider_request_invalid"))?;
        if encoded.len() > MCP_GATEWAY_MAX_MESSAGE_BYTES {
            return Err(ProviderFailure::before_send("provider_request_too_large"));
        }
        encoded.push(b'\n');
        self.stdin
            .lock()
            .map_err(|_| ProviderFailure::after_send("provider_stdin_failed"))?
            .write_all(&encoded)
            .map_err(|_| ProviderFailure::after_send("provider_stdin_failed"))?;

        let deadline = Instant::now() + timeout;
        let mut ignored_notifications = 0usize;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(ProviderFailure::after_send("provider_timeout"));
            }
            let response = match self.incoming.recv_timeout(remaining) {
                Ok(ReaderEvent::Message(Ok(response))) => response,
                Ok(ReaderEvent::Message(Err(ReaderFault::Malformed))) => {
                    return Err(ProviderFailure::after_send("provider_malformed_json"));
                }
                Ok(ReaderEvent::Message(Err(ReaderFault::TooLarge))) => {
                    return Err(ProviderFailure::after_send("provider_message_too_large"));
                }
                Ok(ReaderEvent::Message(Err(ReaderFault::NotificationFlood))) => {
                    return Err(ProviderFailure::after_send("provider_notification_flood"));
                }
                Ok(ReaderEvent::Message(Err(ReaderFault::Io))) => {
                    return Err(ProviderFailure::after_send("provider_stdout_failed"));
                }
                Ok(ReaderEvent::Message(Err(ReaderFault::Stdin))) => {
                    return Err(ProviderFailure::after_send("provider_stdin_failed"));
                }
                Ok(ReaderEvent::Eof) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(ProviderFailure::after_send("provider_eof"));
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    return Err(ProviderFailure::after_send("provider_timeout"));
                }
            };
            if is_provider_notification(&response) {
                ignored_notifications += 1;
                if ignored_notifications > MCP_GATEWAY_MAX_IGNORED_NOTIFICATIONS {
                    return Err(ProviderFailure::after_send("provider_notification_flood"));
                }
                continue;
            }
            if response.get("method").is_some() {
                return Err(ProviderFailure::after_send(
                    "provider_callbacks_unsupported",
                ));
            }
            return validate_rpc_response(response, id);
        }
    }

    fn send_notification(&mut self, method: &str, params: Value) -> Result<(), ProviderFailure> {
        let message = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        });
        let mut encoded = serde_json::to_vec(&message)
            .map_err(|_| ProviderFailure::before_send("provider_request_invalid"))?;
        if encoded.len() > MCP_GATEWAY_MAX_MESSAGE_BYTES {
            return Err(ProviderFailure::before_send("provider_request_too_large"));
        }
        encoded.push(b'\n');
        self.stdin
            .lock()
            .map_err(|_| ProviderFailure::before_send("provider_stdin_failed"))?
            .write_all(&encoded)
            .map_err(|_| ProviderFailure::before_send("provider_stdin_failed"))
    }

    fn terminate(&mut self) {
        // Never let provider cleanup turn Runner shutdown into an unbounded
        // wait. ManagedChild owns the complete process tree; force termination
        // first, then spend one shared bounded deadline confirming tree exit
        // and reaping the direct child. Drop remains the final fail-safe.
        let deadline = Instant::now() + Duration::from_secs(1);
        let _ = self.child.terminate_tree();
        let _ = self
            .child
            .wait_tree_exit(deadline.saturating_duration_since(Instant::now()));
        loop {
            match self.child.try_wait() {
                Ok(Some(_)) | Err(_) => return,
                Ok(None) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(10).min(remaining));
                }
            }
        }
    }
}

impl Drop for StdioProviderConnection {
    fn drop(&mut self) {
        self.terminate();
    }
}

#[derive(Default)]
struct UnsolicitedWindow {
    start: Option<Instant>,
    count: usize,
}

impl UnsolicitedWindow {
    fn exceeded(&mut self, now: Instant) -> bool {
        if self
            .start
            .is_none_or(|start| now.duration_since(start) >= MCP_GATEWAY_UNSOLICITED_WINDOW)
        {
            self.start = Some(now);
            self.count = 0;
        }
        self.count += 1;
        self.count > MCP_GATEWAY_MAX_IGNORED_NOTIFICATIONS
    }

    fn reset(&mut self) {
        self.start = None;
        self.count = 0;
    }
}

fn provider_stdout_reader(
    stdout: std::process::ChildStdout,
    sender: mpsc::SyncSender<ReaderEvent>,
    stdin: Arc<Mutex<ChildStdin>>,
) {
    let mut reader = BufReader::new(stdout);
    let mut unsolicited_window = UnsolicitedWindow::default();
    loop {
        let event = match read_bounded_line(&mut reader) {
            Ok(Some(line)) => match serde_json::from_slice::<Value>(&line) {
                Ok(message) => {
                    if is_provider_notification(&message) {
                        if unsolicited_window.exceeded(Instant::now()) {
                            ReaderEvent::Message(Err(ReaderFault::NotificationFlood))
                        } else {
                            continue;
                        }
                    } else if let Some(request_id) = provider_request_id(&message) {
                        if unsolicited_window.exceeded(Instant::now()) {
                            return send_reader_fault(&sender, ReaderFault::NotificationFlood);
                        }
                        let reply = if message.get("method").and_then(Value::as_str) == Some("ping")
                        {
                            json!({"jsonrpc": "2.0", "id": request_id, "result": {}})
                        } else {
                            json!({"jsonrpc": "2.0", "id": request_id,
                                "error": {"code": -32601, "message": "Method not found"}})
                        };
                        let result = serde_json::to_vec(&reply)
                            .map_err(|_| ReaderFault::Stdin)
                            .and_then(|mut data| {
                                data.push(b'\n');
                                stdin
                                    .lock()
                                    .map_err(|_| ReaderFault::Stdin)?
                                    .write_all(&data)
                                    .map_err(|_| ReaderFault::Stdin)
                            });
                        match result {
                            Ok(()) => continue,
                            Err(fault) => ReaderEvent::Message(Err(fault)),
                        }
                    } else {
                        unsolicited_window.reset();
                        ReaderEvent::Message(Ok(message))
                    }
                }
                Err(_) => ReaderEvent::Message(Err(ReaderFault::Malformed)),
            },
            Ok(None) => ReaderEvent::Eof,
            Err(fault) => ReaderEvent::Message(Err(fault)),
        };
        let terminal = !matches!(&event, ReaderEvent::Message(Ok(_)));
        if sender.send(event).is_err() || terminal {
            return;
        }
    }
}

fn send_reader_fault(sender: &mpsc::SyncSender<ReaderEvent>, fault: ReaderFault) {
    let _ = sender.send(ReaderEvent::Message(Err(fault)));
}

fn provider_request_id(message: &Value) -> Option<Value> {
    let object = message.as_object()?;
    if object.get("jsonrpc")?.as_str()? != "2.0"
        || object.contains_key("result")
        || object.contains_key("error")
    {
        return None;
    }
    let method = object.get("method")?.as_str()?;
    if method.is_empty() || method.len() > 256 || method.chars().any(char::is_control) {
        return None;
    }
    if object
        .get("params")
        .is_some_and(|params| !params.is_object())
    {
        return None;
    }
    let id = object.get("id")?;
    match id {
        Value::String(value) if value.len() <= MCP_GATEWAY_MAX_PROVIDER_REQUEST_ID_BYTES => {
            Some(id.clone())
        }
        Value::Number(value) if value.is_i64() || value.is_u64() => Some(id.clone()),
        _ => None,
    }
}

fn read_bounded_line(reader: &mut impl BufRead) -> Result<Option<Vec<u8>>, ReaderFault> {
    let mut line = Vec::new();
    let read = (&mut *reader)
        .take((MCP_GATEWAY_MAX_PROVIDER_MESSAGE_BYTES + 2) as u64)
        .read_until(b'\n', &mut line)
        .map_err(|_| ReaderFault::Io)?;
    if read == 0 {
        return Ok(None);
    }
    if line.last() != Some(&b'\n') {
        return Err(if line.len() > MCP_GATEWAY_MAX_PROVIDER_MESSAGE_BYTES {
            ReaderFault::TooLarge
        } else {
            ReaderFault::Malformed
        });
    }
    line.pop();
    if line.last() == Some(&b'\r') {
        line.pop();
    }
    if line.is_empty() || line.len() > MCP_GATEWAY_MAX_PROVIDER_MESSAGE_BYTES {
        return Err(if line.len() > MCP_GATEWAY_MAX_PROVIDER_MESSAGE_BYTES {
            ReaderFault::TooLarge
        } else {
            ReaderFault::Malformed
        });
    }
    Ok(Some(line))
}

fn is_provider_notification(message: &Value) -> bool {
    let Some(object) = message.as_object() else {
        return false;
    };
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || object.contains_key("id")
        || object.contains_key("result")
        || object.contains_key("error")
    {
        return false;
    }
    let Some(method) = object.get("method").and_then(Value::as_str) else {
        return false;
    };
    if method.is_empty() || method.len() > 256 || method.chars().any(char::is_control) {
        return false;
    }
    match object.get("params") {
        None => true,
        Some(params) => params.is_object() || params.is_array(),
    }
}

fn validate_rpc_response(response: Value, expected_id: u64) -> Result<Value, ProviderFailure> {
    let object = response.as_object().ok_or_else(|| {
        ProviderFailure::after_send("provider_protocol_error")
            .with_detail("JSON-RPC response must be an object")
    })?;
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err(
            ProviderFailure::after_send("provider_protocol_error").with_detail(format!(
                "JSON-RPC response for request id {expected_id} did not declare version 2.0"
            )),
        );
    }
    if object.contains_key("method") {
        return Err(ProviderFailure::after_send(
            "provider_callbacks_unsupported",
        ));
    }
    let response_id = object.get("id").and_then(Value::as_u64).ok_or_else(|| {
        ProviderFailure::after_send("provider_response_id_invalid")
            .with_detail("JSON-RPC response omitted a valid integer id")
    })?;
    if response_id < expected_id {
        return Err(
            ProviderFailure::after_send("provider_duplicate_response_id")
                .with_detail("JSON-RPC response id was older than the outstanding request"),
        );
    }
    if response_id != expected_id {
        return Err(ProviderFailure::after_send("provider_unknown_response_id")
            .with_detail("JSON-RPC response id did not match the outstanding request"));
    }
    match (object.get("result"), object.get("error")) {
        (Some(result), None) => Ok(result.clone()),
        (None, Some(Value::Object(error)))
            if error.get("code").and_then(Value::as_i64).is_some()
                && error
                    .get("message")
                    .and_then(Value::as_str)
                    .is_some_and(|message| {
                        !message.is_empty()
                            && message.len() <= 512
                            && !message.chars().any(|character| {
                                character.is_control() && !matches!(character, '\n' | '\r' | '\t')
                            })
                    }) =>
        {
            Err(ProviderFailure::rpc_error())
        }
        _ => Err(ProviderFailure::after_send("provider_protocol_error")
            .with_detail("JSON-RPC response must contain exactly one of result or error")),
    }
}

fn validate_initialize_result(result: &Value) -> Result<String, ProviderFailure> {
    validate_json_value(
        result,
        MCP_GATEWAY_MAX_MESSAGE_BYTES,
        "provider initialize response",
    )
    .map_err(|_| {
        ProviderFailure::after_send("provider_initialize_invalid")
            .with_detail("MCP initialize response exceeded validation limits")
    })?;
    let object = result.as_object().ok_or_else(|| {
        ProviderFailure::after_send("provider_initialize_invalid")
            .with_detail("MCP initialize response must be a JSON object")
    })?;
    let protocol_version = object
        .get("protocolVersion")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            ProviderFailure::after_send("provider_initialize_invalid")
                .with_detail("MCP initialize response omitted protocolVersion")
        })?;
    if protocol_version.is_empty()
        || protocol_version.len() > 64
        || protocol_version.chars().any(char::is_control)
    {
        return Err(ProviderFailure::after_send("provider_initialize_invalid")
            .with_detail("MCP initialize response contains an invalid protocolVersion"));
    }
    if !SUPPORTED_MCP_PROTOCOL_VERSIONS.contains(&protocol_version) {
        return Err(ProviderFailure::after_send("provider_protocol_version_unsupported")
            .with_detail(format!(
                "MCP server negotiated unsupported protocol version '{protocol_version}'; supported versions are {}",
                SUPPORTED_MCP_PROTOCOL_VERSIONS.join(", ")
            )));
    }
    let server_info = object.get("serverInfo").and_then(Value::as_object);
    let implementation_field_valid = |field: &str| {
        server_info
            .and_then(|info| info.get(field))
            .and_then(Value::as_str)
            .is_some_and(|value| {
                !value.is_empty() && value.len() <= 128 && !value.chars().any(char::is_control)
            })
    };
    if !object.get("capabilities").is_some_and(Value::is_object)
        || !object
            .get("capabilities")
            .and_then(|capabilities| capabilities.get("tools"))
            .is_some_and(Value::is_object)
        || !implementation_field_valid("name")
        || !implementation_field_valid("version")
    {
        return Err(
            ProviderFailure::after_send("provider_initialize_invalid").with_detail(
                "MCP initialize response omitted required tools capability or valid serverInfo",
            ),
        );
    }
    Ok(protocol_version.to_string())
}

fn provider_failure_response(error: ProviderFailure) -> McpGatewayResponse {
    let state = error.dispatch_state;
    let message = error.detail.unwrap_or_else(|| {
        match state {
            McpGatewayDispatchState::NotStarted => {
                "Provider request was not started; no downstream effect was dispatched"
            }
            McpGatewayDispatchState::OutcomeUnknown => {
                "Provider request may have been dispatched; outcome is unknown and must not be retried automatically"
            }
            McpGatewayDispatchState::Completed => {
                "Provider completed the request-response exchange but returned a downstream error or unsupported/invalid bounded result"
            }
        }
        .to_string()
    });
    let message = format!("{message} (provider error code: {})", error.code);
    McpGatewayResponse::error(state, error.code, message)
}

fn bridge_error(
    state: McpGatewayDispatchState,
    code: &'static str,
    message: &'static str,
) -> McpGatewayResponse {
    McpGatewayResponse::error(state, code, message)
}

fn stale_provider() -> McpGatewayResponse {
    bridge_error(
        McpGatewayDispatchState::NotStarted,
        "stale_provider",
        "Exact provider instance is unavailable; request was not routed elsewhere",
    )
}

const MCP_SESSION_HEADER: &str = "mcp-session-id";
const MCP_PROTOCOL_VERSION_HEADER: &str = "mcp-protocol-version";
const MCP_HTTP_BEST_EFFORT_DELETE_TIMEOUT: Duration = Duration::from_secs(1);

fn remaining_connect_budget(
    started: Instant,
    budget: Duration,
) -> Result<Duration, ProviderFailure> {
    budget
        .checked_sub(started.elapsed())
        .filter(|remaining| !remaining.is_zero())
        .ok_or_else(|| ProviderFailure::before_send("provider_timeout"))
}

fn validate_http_mapped_value(value: &str) -> Result<(), ProviderFailure> {
    if value.is_empty()
        || value.len() > MCP_GATEWAY_HTTP_MAPPED_VALUE_MAX_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(ProviderFailure::before_send("provider_env_invalid"));
    }
    Ok(())
}

const MCP_GATEWAY_HTTP_ERROR_BODY_PREFIX_BYTES: u64 = 8 * 1024;
const MCP_GATEWAY_HTTP_CF_RAY_MAX_BYTES: usize = 64;

fn provider_http_response_failure(
    response: reqwest::blocking::Response,
    code: &'static str,
    after_send: bool,
) -> ProviderFailure {
    let status = response.status();
    let is_cloudflare = response
        .headers()
        .get("server")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("cloudflare"));
    let cf_ray = response
        .headers()
        .get("cf-ray")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| {
            !value.is_empty()
                && value.len() <= MCP_GATEWAY_HTTP_CF_RAY_MAX_BYTES
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
        .map(str::to_string);

    // Inspect only a small prefix for known edge-provider markers. Never
    // surface the response body: error pages may contain request-specific data.
    let mut body_prefix = Vec::new();
    let _ = response
        .take(MCP_GATEWAY_HTTP_ERROR_BODY_PREFIX_BYTES)
        .read_to_end(&mut body_prefix);
    let body_prefix = String::from_utf8_lossy(&body_prefix).to_ascii_lowercase();
    let cloudflare_error =
        if body_prefix.contains("error 1010") || body_prefix.contains("browser_signature_banned") {
            Some("Cloudflare Error 1010 (browser signature blocked)")
        } else if body_prefix.contains("error 1020") {
            Some("Cloudflare Error 1020 (access denied by firewall rule)")
        } else {
            None
        };

    let mut detail = format!("Upstream returned HTTP {}", status.as_u16());
    if is_cloudflare || cf_ray.is_some() {
        detail.push_str("; Cloudflare edge");
        if let Some(cf_ray) = cf_ray {
            detail.push_str(" (Ray ID: ");
            detail.push_str(&cf_ray);
            detail.push(')');
        }
    }
    if let Some(cloudflare_error) = cloudflare_error {
        detail.push_str("; ");
        detail.push_str(cloudflare_error);
    }

    let failure = if after_send {
        ProviderFailure::after_send(code)
    } else {
        ProviderFailure::before_send(code)
    };
    failure.with_detail(detail)
}

fn accept_http_notification_ack(
    response: reqwest::blocking::Response,
    after_send: bool,
) -> Result<(), ProviderFailure> {
    let fault = |code: &'static str| -> ProviderFailure {
        if after_send {
            ProviderFailure::after_send(code)
        } else {
            ProviderFailure::before_send(code)
        }
    };
    let status = response.status();
    match status.as_u16() {
        202 | 204 => Ok(()),
        code if (200..300).contains(&code) => {
            let body = read_bounded_http_body(
                response,
                MCP_GATEWAY_MAX_PROVIDER_MESSAGE_BYTES,
                after_send,
            )?;
            if body.is_empty() {
                Ok(())
            } else {
                Err(fault("provider_protocol_error"))
            }
        }
        _ => Err(provider_http_response_failure(
            response,
            "provider_http_failed",
            after_send,
        )),
    }
}

struct HttpProviderConnection {
    client: reqwest::blocking::Client,
    base_url: url::Url,
    env_from_env: BTreeMap<String, String>,
    header_mappings: Vec<(String, String)>,
    query_mappings: Vec<(String, String)>,
    session_id: Option<String>,
    next_id: u64,
    protocol_version: Option<String>,
}

impl HttpProviderConnection {
    fn connect(
        config: &McpGatewayProviderConfig,
        timeout: Duration,
    ) -> Result<Self, ProviderFailure> {
        let raw_url = config
            .url
            .as_deref()
            .map(str::trim)
            .filter(|url| !url.is_empty())
            .ok_or_else(|| ProviderFailure::before_send("provider_url_invalid"))?;
        let base_url = validate_mcp_gateway_http_url(raw_url)
            .map_err(|_| ProviderFailure::before_send("provider_url_invalid"))?;
        let client = reqwest::blocking::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(timeout)
            .build()
            .map_err(|_| ProviderFailure::before_send("provider_http_unavailable"))?;
        let header_mappings = config
            .http_headers_from_env
            .iter()
            .map(|(header, destination)| (header.clone(), destination.clone()))
            .collect();
        let query_mappings = config
            .http_query_from_env
            .iter()
            .map(|(key, destination)| (key.clone(), destination.clone()))
            .collect();
        let env_from_env = config.env_from_env.clone();
        let mut connection = Self {
            client,
            base_url,
            env_from_env,
            header_mappings,
            query_mappings,
            session_id: None,
            next_id: 1,
            protocol_version: None,
        };
        let connect_started = Instant::now();
        let init_timeout = remaining_connect_budget(connect_started, timeout)?;
        let initialized = connection.request(
            "initialize",
            json!({
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {
                    "name": "webcodex-runner-mcp-gateway",
                    "version": env!("CARGO_PKG_VERSION")
                }
            }),
            init_timeout,
        )?;
        connection.protocol_version = Some(validate_initialize_result(&initialized)?);
        let notify_timeout = remaining_connect_budget(connect_started, timeout)?;
        connection.send_notification("notifications/initialized", json!({}), notify_timeout)?;
        Ok(connection)
    }

    fn tools_list(
        &mut self,
        timeout: Duration,
        max_description_bytes: usize,
    ) -> Result<Vec<McpGatewayTool>, ProviderFailure> {
        let result = self.request("tools/list", json!({}), timeout)?;
        parse_tools_list_result(result, max_description_bytes)
    }

    fn tools_call(
        &mut self,
        name: &str,
        arguments: Value,
        timeout: Duration,
        allow_resource_links: bool,
    ) -> Result<McpGatewayToolResult, ProviderFailure> {
        let params = json!({"name": name, "arguments": arguments});
        let result = self.request("tools/call", params, timeout)?;
        parse_tools_call_result(result, allow_resource_links)
    }

    fn retire(&mut self) {
        self.best_effort_delete_session();
    }

    fn best_effort_delete_session(&mut self) {
        let Some(session_id) = self.session_id.take() else {
            return;
        };
        let Ok(url) = self.build_request_url() else {
            return;
        };
        let Ok(operator_headers) = self.build_operator_headers() else {
            return;
        };
        let client = self.client.clone();
        let protocol_version = self.protocol_version.clone();
        if tokio::runtime::Handle::try_current().is_ok() {
            let _ = std::thread::Builder::new()
                .name("wc-mcp-http-delete".into())
                .spawn(move || {
                    send_best_effort_http_session_delete(
                        client,
                        url,
                        session_id,
                        protocol_version,
                        operator_headers,
                    );
                });
            return;
        }
        send_best_effort_http_session_delete(
            client,
            url,
            session_id,
            protocol_version,
            operator_headers,
        );
    }

    fn resolve_env_value(&self, destination_key: &str) -> Result<String, ProviderFailure> {
        let source = self
            .env_from_env
            .get(destination_key)
            .ok_or_else(|| ProviderFailure::before_send("provider_env_missing"))?;
        if is_sensitive_env_key(destination_key) || is_sensitive_env_key(source) {
            return Err(ProviderFailure::before_send("provider_env_forbidden"));
        }
        let value = std::env::var_os(source)
            .ok_or_else(|| ProviderFailure::before_send("provider_env_missing"))?;
        let value = value
            .to_str()
            .map(str::to_string)
            .ok_or_else(|| ProviderFailure::before_send("provider_env_invalid"))?;
        validate_http_mapped_value(&value)?;
        Ok(value)
    }

    fn build_operator_headers(&self) -> Result<Vec<(String, String)>, ProviderFailure> {
        let mut headers = Vec::with_capacity(self.header_mappings.len());
        for (name, destination) in &self.header_mappings {
            headers.push((name.clone(), self.resolve_env_value(destination)?));
        }
        Ok(headers)
    }

    fn build_request_url(&self) -> Result<url::Url, ProviderFailure> {
        let mut url = self.base_url.clone();
        {
            let mut pairs = url.query_pairs_mut();
            for (key, destination) in &self.query_mappings {
                pairs.append_pair(key, &self.resolve_env_value(destination)?);
            }
        }
        Ok(url)
    }

    fn send_notification(
        &mut self,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> Result<(), ProviderFailure> {
        let message = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        });
        self.post_message(message, None, timeout).map(|_| ())
    }

    fn request(
        &mut self,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> Result<Value, ProviderFailure> {
        let id = self.next_id;
        self.next_id = id
            .checked_add(1)
            .ok_or_else(|| ProviderFailure::before_send("provider_request_id_exhausted"))?;
        let message = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        let value = self.post_message(message, Some(id), timeout)?;
        validate_rpc_response(value, id)
    }

    fn post_message(
        &mut self,
        message: Value,
        expected_id: Option<u64>,
        timeout: Duration,
    ) -> Result<Value, ProviderFailure> {
        let encoded = serde_json::to_vec(&message)
            .map_err(|_| ProviderFailure::before_send("provider_request_invalid"))?;
        if encoded.len() > MCP_GATEWAY_MAX_MESSAGE_BYTES {
            return Err(ProviderFailure::before_send("provider_request_too_large"));
        }
        let url = self.build_request_url()?;
        let operator_headers = self.build_operator_headers()?;
        let mut request = self
            .client
            .post(url)
            .timeout(timeout)
            .header("accept", "application/json, text/event-stream")
            .header("content-type", "application/json")
            .body(encoded);
        if let Some(protocol_version) = self
            .protocol_version
            .as_deref()
            .filter(|version| protocol_version_requires_http_header(version))
        {
            request = request.header(MCP_PROTOCOL_VERSION_HEADER, protocol_version);
        }
        if let Some(session_id) = self.session_id.as_deref() {
            request = request.header(MCP_SESSION_HEADER, session_id);
        }
        for (name, value) in operator_headers {
            request = request.header(name, value);
        }
        let after_send = expected_id.is_some();
        let fault = |code: &'static str| -> ProviderFailure {
            if after_send {
                ProviderFailure::after_send(code)
            } else {
                ProviderFailure::before_send(code)
            }
        };
        let response = request.send().map_err(|error| {
            let kind = if error.is_timeout() {
                "timeout"
            } else if error.is_connect() {
                "connect"
            } else {
                "other"
            };
            fault("provider_http_failed").with_detail(format!(
                "HTTP transport failed before receiving a response ({kind})"
            ))
        })?;
        if response.status().is_redirection() {
            return Err(provider_http_response_failure(
                response,
                "provider_http_redirect",
                after_send,
            ));
        }
        if let Some(value) = response.headers().get(MCP_SESSION_HEADER) {
            let session_id = value
                .to_str()
                .map_err(|_| fault("provider_protocol_error"))?
                .trim();
            if session_id.is_empty()
                || session_id.len() > MCP_GATEWAY_MAX_PROVIDER_REQUEST_ID_BYTES
                || session_id.chars().any(char::is_control)
            {
                return Err(fault("provider_protocol_error"));
            }
            self.session_id = Some(session_id.to_string());
        }
        let status = response.status();
        if expected_id.is_none() {
            accept_http_notification_ack(response, after_send)?;
            return Ok(Value::Null);
        }
        if !status.is_success() {
            return Err(provider_http_response_failure(
                response,
                "provider_http_failed",
                after_send,
            ));
        }
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();
        if content_type == "text/event-stream" {
            let id = expected_id.ok_or_else(|| fault("provider_protocol_error"))?;
            let json_id = Value::Number(id.into());
            return read_http_sse_response(response, &json_id, after_send);
        }
        if content_type != "application/json" {
            return Err(fault("provider_protocol_error"));
        }
        let body =
            read_bounded_http_body(response, MCP_GATEWAY_MAX_PROVIDER_MESSAGE_BYTES, after_send)?;
        if body.is_empty() {
            return Err(fault("provider_protocol_error"));
        }
        let value: Value =
            serde_json::from_slice(&body).map_err(|_| fault("provider_malformed_json"))?;
        Ok(value)
    }
}

impl Drop for HttpProviderConnection {
    fn drop(&mut self) {
        self.best_effort_delete_session();
    }
}

fn send_best_effort_http_session_delete(
    client: reqwest::blocking::Client,
    url: url::Url,
    session_id: String,
    protocol_version: Option<String>,
    operator_headers: Vec<(String, String)>,
) {
    let mut request = client
        .delete(url)
        .timeout(MCP_HTTP_BEST_EFFORT_DELETE_TIMEOUT)
        .header("accept", "application/json, text/event-stream")
        .header(MCP_SESSION_HEADER, session_id);
    if let Some(protocol_version) = protocol_version
        .as_deref()
        .filter(|version| protocol_version_requires_http_header(version))
    {
        request = request.header(MCP_PROTOCOL_VERSION_HEADER, protocol_version);
    }
    for (name, value) in operator_headers {
        request = request.header(name, value);
    }
    let _ = request.send();
}

fn read_bounded_http_body(
    response: reqwest::blocking::Response,
    limit: usize,
    after_send: bool,
) -> Result<Vec<u8>, ProviderFailure> {
    let fault = |code: &'static str| -> ProviderFailure {
        if after_send {
            ProviderFailure::after_send(code)
        } else {
            ProviderFailure::before_send(code)
        }
    };
    if response
        .content_length()
        .is_some_and(|length| length as usize > limit)
    {
        return Err(fault("provider_message_too_large"));
    }
    let mut body = Vec::new();
    let read = response
        .take((limit.saturating_add(1)) as u64)
        .read_to_end(&mut body)
        .map_err(|_| fault("provider_http_failed"))?;
    if read > limit || body.len() > limit {
        return Err(fault("provider_message_too_large"));
    }
    Ok(body)
}

fn read_http_sse_response(
    mut response: reqwest::blocking::Response,
    expected_id: &Value,
    after_send: bool,
) -> Result<Value, ProviderFailure> {
    let fault = |code: &'static str| -> ProviderFailure {
        if after_send {
            ProviderFailure::after_send(code)
        } else {
            ProviderFailure::before_send(code)
        }
    };
    let mut decoder = HttpSseDecoder::new(MCP_GATEWAY_MAX_PROVIDER_MESSAGE_BYTES);
    let mut buffer = [0u8; 4096];
    loop {
        let read = response
            .read(&mut buffer)
            .map_err(|_| fault("provider_http_failed"))?;
        if read == 0 {
            break;
        }
        for byte in &buffer[..read] {
            if let Some(data) = decoder.push(*byte)? {
                if data.is_empty() {
                    continue;
                }
                let value: Value = serde_json::from_slice(&data).map_err(|_| {
                    fault("provider_malformed_json")
                        .with_detail("Streamable HTTP SSE event data was not valid JSON")
                })?;
                if http_terminal_response(&value, expected_id) {
                    // Return the complete JSON-RPC envelope. The request layer
                    // validates its id/result exactly once, just as it does for
                    // application/json responses.
                    return Ok(value);
                }
                if !http_provider_notification(&value) {
                    return Err(fault("provider_protocol_error").with_detail(
                        "Streamable HTTP SSE event was neither the matching JSON-RPC response nor a valid notification",
                    ));
                }
            }
        }
    }
    Err(fault("provider_protocol_error").with_detail(
        "Streamable HTTP SSE stream ended before the matching JSON-RPC response arrived",
    ))
}

fn http_terminal_response(response: &Value, expected_id: &Value) -> bool {
    response.is_object()
        && response.get("jsonrpc").and_then(Value::as_str) == Some("2.0")
        && response.get("id") == Some(expected_id)
        && response.get("method").is_none()
        && (response.get("result").is_some() ^ response.get("error").is_some())
}

fn http_provider_notification(value: &Value) -> bool {
    is_provider_notification(value)
}

struct HttpSseDecoder {
    line: Vec<u8>,
    data: Vec<u8>,
    max: usize,
    stream_total: usize,
    cr: bool,
    first: bool,
}

impl HttpSseDecoder {
    fn new(max: usize) -> Self {
        Self {
            line: Vec::new(),
            data: Vec::new(),
            max,
            stream_total: 0,
            cr: false,
            first: true,
        }
    }

    fn push(&mut self, byte: u8) -> Result<Option<Vec<u8>>, ProviderFailure> {
        self.stream_total = self.stream_total.saturating_add(1);
        if self.stream_total > self.max {
            return Err(ProviderFailure::after_send("provider_message_too_large"));
        }
        if byte == b'\n' && self.cr {
            self.cr = false;
            return Ok(None);
        }
        self.cr = byte == b'\r';
        if byte != b'\n' && byte != b'\r' {
            if self.line.len().saturating_add(self.data.len()) >= self.max {
                return Err(ProviderFailure::after_send("provider_message_too_large"));
            }
            self.line.push(byte);
            return Ok(None);
        }
        let mut line = std::mem::take(&mut self.line);
        if self.first {
            self.first = false;
            if line.starts_with(&[0xef, 0xbb, 0xbf]) {
                line.drain(..3);
            }
        }
        if line.is_empty() {
            if self.data.is_empty() {
                return Ok(None);
            }
            self.data.pop();
            return Ok(Some(std::mem::take(&mut self.data)));
        }
        let colon = line.iter().position(|b| *b == b':').unwrap_or(line.len());
        if &line[..colon] == b"data" {
            let mut value = line.get(colon + 1..).unwrap_or_default();
            if value.first() == Some(&b' ') {
                value = &value[1..];
            }
            if self
                .data
                .len()
                .saturating_add(value.len())
                .saturating_add(1)
                > self.max
            {
                return Err(ProviderFailure::after_send("provider_message_too_large"));
            }
            self.data.extend_from_slice(value);
            self.data.push(b'\n');
        }
        Ok(None)
    }
}

#[cfg(test)]
#[path = "mcp_gateway_tests.rs"]
mod tests;
