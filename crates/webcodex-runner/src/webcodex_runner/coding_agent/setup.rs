//! Pre-prompt ACP negotiation and Runner-enforced configuration.

use super::protocol::{
    bounded_setup_wait, remaining_run_budget, request_frame, wait_outbound_write, wait_response,
    AcpFailure, AcpOutboundWriter, OutboundInterruption, OutboundWriteOutcome, ReaderEvent,
};
use super::*;
use webcodex_core::coding_agent::CodingAgentLiveConfirmedConfigEntry;

fn unique_config_option<'a>(
    options: &'a [SessionConfigOption],
    key: &str,
) -> Option<&'a SessionConfigOption> {
    let mut matches = options.iter().filter(|option| option.id.to_string() == key);
    let option = matches.next()?;
    matches.next().is_none().then_some(option)
}

fn config_override_is_valid(
    options: &[SessionConfigOption],
    key: &str,
    value: &CodingAgentConfigValue,
) -> bool {
    let Some(option) = unique_config_option(options, key) else {
        return false;
    };
    match (&option.kind, value) {
        (SessionConfigKind::Boolean(_), CodingAgentConfigValue::Bool(_)) => true,
        (SessionConfigKind::Select(select), CodingAgentConfigValue::String(requested)) => {
            match &select.options {
                SessionConfigSelectOptions::Ungrouped(options) => options
                    .iter()
                    .any(|option| option.value.to_string() == *requested),
                SessionConfigSelectOptions::Grouped(groups) => groups
                    .iter()
                    .flat_map(|group| group.options.iter())
                    .any(|option| option.value.to_string() == *requested),
                _ => false,
            }
        }
        _ => false,
    }
}

fn config_override_is_current(
    options: &[SessionConfigOption],
    key: &str,
    value: &CodingAgentConfigValue,
) -> bool {
    let Some(option) = unique_config_option(options, key) else {
        return false;
    };
    match (&option.kind, value) {
        (SessionConfigKind::Boolean(current), CodingAgentConfigValue::Bool(requested)) => {
            current.current_value == *requested
        }
        (SessionConfigKind::Select(current), CodingAgentConfigValue::String(requested)) => {
            current.current_value.to_string() == *requested
        }
        _ => false,
    }
}

pub(super) fn acp_initialize_client_capabilities(config: &AcpAgentConfig) -> Value {
    if config.parameterized_model_picker {
        json!({"_meta": {"parameterizedModelPicker": true}})
    } else {
        json!({})
    }
}

fn select_choice_count(option: &SessionConfigOption) -> usize {
    match &option.kind {
        SessionConfigKind::Select(select) => match &select.options {
            SessionConfigSelectOptions::Ungrouped(options) => options.len(),
            SessionConfigSelectOptions::Grouped(groups) => {
                groups.iter().map(|group| group.options.len()).sum()
            }
            _ => 0,
        },
        _ => 0,
    }
}

fn config_kind_rank(option: Option<&SessionConfigOption>) -> u8 {
    match option.map(|option| &option.kind) {
        Some(SessionConfigKind::Select(_)) => 0,
        Some(SessionConfigKind::Boolean(_)) => 1,
        _ => 2,
    }
}

/// Provider-agnostic apply order: high-choice `select` options before booleans
/// so model-style pickers settle before dependent toggles. When two `select`
/// options tie on choice count (for example Cursor `fast` before `model` under
/// lexicographic key order), a single bounded re-pass plus final fail-closed
/// current checks must converge or reject — never report success early.
fn caller_config_application_order(
    advertised: &[SessionConfigOption],
    keys: &[String],
) -> Vec<String> {
    let mut ordered = keys.to_vec();
    ordered.sort_by(|left, right| {
        let left_option = unique_config_option(advertised, left);
        let right_option = unique_config_option(advertised, right);
        config_kind_rank(left_option)
            .cmp(&config_kind_rank(right_option))
            .then_with(|| {
                let left_count = left_option.map(select_choice_count).unwrap_or(0);
                let right_count = right_option.map(select_choice_count).unwrap_or(0);
                right_count.cmp(&left_count)
            })
            .then_with(|| left.cmp(right))
    });
    ordered
}

/// Project only provider-reported currents that are legal advertised values,
/// never arbitrary secret-like free text outside the option enum.
fn config_current_advertised_identity(
    advertised: &[SessionConfigOption],
    key: &str,
) -> Option<String> {
    let option = unique_config_option(advertised, key)?;
    match &option.kind {
        SessionConfigKind::Boolean(boolean) => Some(boolean.current_value.to_string()),
        SessionConfigKind::Select(select) => {
            let current = select.current_value.to_string();
            let advertised_current = match &select.options {
                SessionConfigSelectOptions::Ungrouped(options) => options
                    .iter()
                    .any(|option| option.value.to_string() == current),
                SessionConfigSelectOptions::Grouped(groups) => groups
                    .iter()
                    .flat_map(|group| group.options.iter())
                    .any(|option| option.value.to_string() == current),
                _ => false,
            };
            advertised_current.then_some(current)
        }
        _ => None,
    }
}

fn build_live_confirmed_effective_config(
    advertised: &[SessionConfigOption],
    forced: &BTreeMap<String, CodingAgentConfigValue>,
    caller: &BTreeMap<String, CodingAgentConfigValue>,
) -> Vec<CodingAgentLiveConfirmedConfigEntry> {
    let mut out = Vec::new();
    for key in forced.keys() {
        if let Some(current) = config_current_advertised_identity(advertised, key) {
            out.push(CodingAgentLiveConfirmedConfigEntry {
                id: key.clone(),
                current,
                source: "runner_forced".into(),
            });
        }
    }
    for key in caller.keys() {
        if forced.contains_key(key) {
            continue;
        }
        if let Some(current) = config_current_advertised_identity(advertised, key) {
            out.push(CodingAgentLiveConfirmedConfigEntry {
                id: key.clone(),
                current,
                source: "caller_requested".into(),
            });
        }
    }
    out
}

fn caller_config_is_fully_current(
    advertised: &[SessionConfigOption],
    caller: &BTreeMap<String, CodingAgentConfigValue>,
    forced: &BTreeMap<String, CodingAgentConfigValue>,
) -> bool {
    caller.iter().all(|(key, value)| {
        forced.contains_key(key)
            || (config_override_is_valid(advertised, key, value)
                && config_override_is_current(advertised, key, value))
    })
}

fn config_params(session_id: &str, key: &str, value: &CodingAgentConfigValue) -> Option<Value> {
    match value {
        CodingAgentConfigValue::String(value) => {
            Some(json!({"sessionId":session_id,"configId":key,"value":value}))
        }
        CodingAgentConfigValue::Bool(value) => {
            Some(json!({"sessionId":session_id,"configId":key,"type":"boolean","value":value}))
        }
        CodingAgentConfigValue::Integer(_) => None,
    }
}
impl CodingAgentManager {
    pub(super) fn setup_transport_failure(
        &self,
        run_id: &str,
        entry: &Arc<RunEntry>,
        child: &mut ManagedChild,
        code: &str,
        failure: AcpFailure,
    ) {
        let status = child.try_wait().ok().flatten();
        let failure = failure.diagnose(run_id, code, status);
        self.setup_failure(run_id, entry, code, failure.safe_message());
    }

    pub(super) fn pre_prompt_should_stop(
        &self,
        run_id: &str,
        entry: &Arc<RunEntry>,
        run_deadline: Instant,
        stage: &'static str,
    ) -> bool {
        if self.pre_prompt_interrupted(run_id, entry) {
            return true;
        }
        if remaining_run_budget(run_deadline).is_none() {
            self.setup_timeout(run_id, entry, stage);
            return true;
        }
        false
    }

    fn write_pre_prompt_frame(
        &self,
        run_id: &str,
        entry: &Arc<RunEntry>,
        child: &mut ManagedChild,
        outbound: &mut AcpOutboundWriter,
        frame: std::io::Result<Vec<u8>>,
        run_deadline: Instant,
        failure_code: &str,
        stage: &'static str,
    ) -> bool {
        let frame = match frame {
            Ok(frame) => frame,
            Err(_) => {
                let failure = AcpFailure::InvalidFrame.diagnose(run_id, failure_code, None);
                self.setup_failure(
                    run_id,
                    entry,
                    failure_code,
                    &format!("ACP {stage}: {}", failure.safe_message()),
                );
                self.terminate_run_io(child, outbound);
                return false;
            }
        };
        let pending = match outbound.start_frame(frame) {
            Ok(pending) => pending,
            Err(error) => {
                if !self.pre_prompt_should_stop(run_id, entry, run_deadline, stage) {
                    self.setup_failure(
                        run_id,
                        entry,
                        failure_code,
                        &format!(
                            "ACP {stage}: {}",
                            error
                                .diagnose(run_id, failure_code, child.try_wait().ok().flatten())
                                .safe_message()
                        ),
                    );
                }
                self.terminate_run_io(child, outbound);
                return false;
            }
        };
        match wait_outbound_write(
            pending,
            run_deadline,
            Some(&entry.cancel_requested),
            Some(&self.accepting),
        ) {
            OutboundWriteOutcome::Written => true,
            OutboundWriteOutcome::Failed(error) => {
                if !self.pre_prompt_should_stop(run_id, entry, run_deadline, stage) {
                    self.setup_failure(
                        run_id,
                        entry,
                        failure_code,
                        &format!(
                            "ACP {stage}: {}",
                            error
                                .diagnose(run_id, failure_code, child.try_wait().ok().flatten())
                                .safe_message()
                        ),
                    );
                }
                self.terminate_run_io(child, outbound);
                false
            }
            OutboundWriteOutcome::Interrupted(OutboundInterruption::Deadline) => {
                self.setup_timeout(run_id, entry, stage);
                self.terminate_run_io(child, outbound);
                false
            }
            OutboundWriteOutcome::Interrupted(
                OutboundInterruption::Cancelled | OutboundInterruption::Shutdown,
            ) => {
                let _ = self.pre_prompt_interrupted(run_id, entry);
                self.terminate_run_io(child, outbound);
                false
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_pre_prompt_config_option(
        &self,
        run_id: &str,
        entry: &Arc<RunEntry>,
        child: &mut ManagedChild,
        outbound: &mut AcpOutboundWriter,
        rx: &Receiver<ReaderEvent>,
        run_deadline: Instant,
        session_id: &str,
        next_id: &mut u64,
        advertised: &mut Vec<SessionConfigOption>,
        key: &str,
        value: &CodingAgentConfigValue,
        forced: bool,
    ) -> bool {
        if self.pre_prompt_should_stop(run_id, entry, run_deadline, "session/set_config_option") {
            self.terminate_run_io(child, outbound);
            return false;
        }
        if !advertised.iter().any(|option| option.id.to_string() == key) {
            self.setup_failure(
                run_id,
                entry,
                if forced {
                    "coding_agent_forced_config_not_advertised"
                } else {
                    "coding_agent_config_invalid"
                },
                if forced {
                    "Runner-enforced ACP config option is not advertised by provider"
                } else {
                    "ACP config override is not currently advertised/legal"
                },
            );
            self.terminate_run_io(child, outbound);
            return false;
        }
        if !config_override_is_valid(advertised, key, value) {
            self.setup_failure(
                run_id,
                entry,
                if forced {
                    "coding_agent_forced_config_invalid"
                } else {
                    "coding_agent_config_invalid"
                },
                if forced {
                    "Runner-enforced ACP config value is not currently legal for provider"
                } else {
                    "ACP config override is not currently advertised/legal"
                },
            );
            self.terminate_run_io(child, outbound);
            return false;
        }

        let config_id = *next_id;
        *next_id += 1;
        let params = match config_params(session_id, key, value) {
            Some(params) => params,
            None => {
                self.setup_failure(
                    run_id,
                    entry,
                    if forced {
                        "coding_agent_forced_config_invalid"
                    } else {
                        "coding_agent_config_invalid"
                    },
                    if forced {
                        "Runner-enforced ACP config value type is unsupported by stable v1"
                    } else {
                        "ACP config value type is unsupported by stable v1"
                    },
                );
                self.terminate_run_io(child, outbound);
                return false;
            }
        };
        if !self.write_pre_prompt_frame(
            run_id,
            entry,
            child,
            outbound,
            request_frame(config_id, "session/set_config_option", params),
            run_deadline,
            if forced {
                "coding_agent_forced_config_write_failed"
            } else {
                "coding_agent_config_write_failed"
            },
            "session/set_config_option",
        ) {
            return false;
        }
        let Some(config_wait) = bounded_setup_wait(run_deadline) else {
            self.setup_timeout(run_id, entry, "session/set_config_option");
            self.terminate_run_io(child, outbound);
            return false;
        };
        let result = match wait_response(rx, config_id, config_wait, Some(&entry.cancel_requested))
        {
            Ok(result) => result,
            Err(error) => {
                if !self.pre_prompt_should_stop(
                    run_id,
                    entry,
                    run_deadline,
                    "session/set_config_option",
                ) {
                    self.setup_transport_failure(
                        run_id,
                        entry,
                        child,
                        if forced {
                            "coding_agent_forced_config_failed"
                        } else {
                            "coding_agent_config_failed"
                        },
                        error,
                    );
                }
                self.terminate_run_io(child, outbound);
                return false;
            }
        };
        if self.pre_prompt_should_stop(run_id, entry, run_deadline, "session/set_config_option") {
            self.terminate_run_io(child, outbound);
            return false;
        }
        let refreshed: SetSessionConfigOptionResponse = match serde_json::from_value(result) {
            Ok(result) => result,
            Err(_) => {
                self.setup_failure(
                    run_id,
                    entry,
                    if forced {
                        "coding_agent_forced_config_invalid_response"
                    } else {
                        "coding_agent_config_invalid_response"
                    },
                    "invalid refreshed ACP config options",
                );
                self.terminate_run_io(child, outbound);
                return false;
            }
        };
        *advertised = refreshed.config_options;
        if !config_override_is_current(advertised, key, value) {
            self.setup_failure(
                run_id,
                entry,
                if forced {
                    "coding_agent_forced_config_not_applied"
                } else {
                    "coding_agent_config_not_applied"
                },
                if forced {
                    "Runner-enforced ACP config was not reflected by provider"
                } else {
                    "ACP config override was not reflected by provider"
                },
            );
            self.terminate_run_io(child, outbound);
            return false;
        }
        true
    }
}

impl CodingAgentManager {
    /// Negotiate the ACP Session and enforce configuration before prompt dispatch.
    /// The caller owns process I/O and the one absolute run deadline.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn initialize_session(
        &self,
        request: &webcodex_core::coding_agent::CodingAgentStartRequest,
        provider: &ProviderEntry,
        entry: &Arc<RunEntry>,
        child: &mut ManagedChild,
        outbound: &mut AcpOutboundWriter,
        rx: &Receiver<ReaderEvent>,
        run_deadline: Instant,
    ) -> Option<(String, u64, Vec<CodingAgentLiveConfirmedConfigEntry>)> {
        let mut next_id = 1u64;
        if self.pre_prompt_should_stop(&request.run_id, entry, run_deadline, "initialize") {
            self.terminate_run_io(child, outbound);
            return None;
        }
        let initialize_id = next_id;
        next_id += 1;
        if !self.write_pre_prompt_frame(
            &request.run_id,
            entry,
            child,
            outbound,
            request_frame(
                initialize_id,
                "initialize",
                json!({
                    "protocolVersion": 1,
                    "clientCapabilities": acp_initialize_client_capabilities(&provider.config),
                    "clientInfo": {"name":"webcodex-runner","version":env!("CARGO_PKG_VERSION")}
                }),
            ),
            run_deadline,
            "coding_agent_initialize_write_failed",
            "initialize",
        ) {
            return None;
        }
        let Some(initialize_wait) = bounded_setup_wait(run_deadline) else {
            self.setup_timeout(&request.run_id, entry, "initialize");
            self.terminate_run_io(child, outbound);
            return None;
        };
        let initialize = match wait_response(
            rx,
            initialize_id,
            initialize_wait,
            Some(&entry.cancel_requested),
        ) {
            Ok(value) => value,
            Err(error) => {
                if !self.pre_prompt_should_stop(&request.run_id, entry, run_deadline, "initialize")
                {
                    self.setup_transport_failure(
                        &request.run_id,
                        entry,
                        child,
                        "coding_agent_initialize_failed",
                        error,
                    );
                }
                self.terminate_run_io(child, outbound);
                return None;
            }
        };
        if self.pre_prompt_should_stop(&request.run_id, entry, run_deadline, "initialize") {
            self.terminate_run_io(child, outbound);
            return None;
        }
        if initialize.get("protocolVersion").and_then(Value::as_u64) != Some(1) {
            self.setup_failure(
                &request.run_id,
                entry,
                "coding_agent_protocol_version_unsupported",
                "ACP v1 was not negotiated",
            );
            self.terminate_run_io(child, outbound);
            return None;
        }

        if self.pre_prompt_should_stop(&request.run_id, entry, run_deadline, "initialize") {
            self.terminate_run_io(child, outbound);
            return None;
        }

        let new_id = next_id;
        next_id += 1;
        if !self.write_pre_prompt_frame(
            &request.run_id,
            entry,
            child,
            outbound,
            request_frame(
                new_id,
                "session/new",
                json!({
                    "cwd": request.project_root,
                    "mcpServers": []
                }),
            ),
            run_deadline,
            "coding_agent_session_new_write_failed",
            "session/new",
        ) {
            return None;
        }
        let Some(session_new_wait) = bounded_setup_wait(run_deadline) else {
            self.setup_timeout(&request.run_id, entry, "session/new");
            self.terminate_run_io(child, outbound);
            return None;
        };
        let new_value =
            match wait_response(rx, new_id, session_new_wait, Some(&entry.cancel_requested)) {
                Ok(value) => value,
                Err(error) => {
                    if !self.pre_prompt_should_stop(
                        &request.run_id,
                        entry,
                        run_deadline,
                        "session/new",
                    ) {
                        self.setup_transport_failure(
                            &request.run_id,
                            entry,
                            child,
                            "coding_agent_session_new_failed",
                            error,
                        );
                    }
                    self.terminate_run_io(child, outbound);
                    return None;
                }
            };
        if self.pre_prompt_should_stop(&request.run_id, entry, run_deadline, "session/new") {
            self.terminate_run_io(child, outbound);
            return None;
        }
        let new_session: NewSessionResponse = match serde_json::from_value(new_value) {
            Ok(response) => response,
            Err(_) => {
                self.setup_failure(
                    &request.run_id,
                    entry,
                    "coding_agent_session_new_invalid",
                    "invalid ACP session/new result",
                );
                self.terminate_run_io(child, outbound);
                return None;
            }
        };
        let session_id = new_session.session_id.to_string();
        let mut advertised = new_session.config_options.unwrap_or_default();

        // Runner-owned global forced config is admission policy, not a
        // best-effort preference. Conflicting caller input fails before prompt.
        for (key, value) in &request.config {
            if let Some(forced_value) = self.forced_config.get(key) {
                if value != forced_value {
                    self.setup_failure(
                        &request.run_id,
                        entry,
                        "coding_agent_forced_config_conflict",
                        "caller ACP config conflicts with Runner-enforced policy",
                    );
                    self.terminate_run_io(child, outbound);
                    return None;
                }
            }
        }

        for (key, value) in &self.forced_config {
            if !self.apply_pre_prompt_config_option(
                &request.run_id,
                entry,
                child,
                outbound,
                rx,
                run_deadline,
                &session_id,
                &mut next_id,
                &mut advertised,
                key,
                value,
                true,
            ) {
                return None;
            }
        }

        let caller_keys: Vec<String> = request
            .config
            .keys()
            .filter(|key| !self.forced_config.contains_key(key.as_str()))
            .cloned()
            .collect();
        for key in &caller_keys {
            if !provider
                .config
                .allowed_config_options
                .iter()
                .any(|allowed| allowed == key)
            {
                self.setup_failure(
                    &request.run_id,
                    entry,
                    "coding_agent_config_not_allowed",
                    "ACP config override is not operator-allowed",
                );
                self.terminate_run_io(child, outbound);
                return None;
            }
        }
        let caller_apply_order =
            caller_config_application_order(&advertised, &caller_keys);
        for key in &caller_apply_order {
            let value = request.config.get(key).expect("caller key was enumerated");
            if !self.apply_pre_prompt_config_option(
                &request.run_id,
                entry,
                child,
                outbound,
                rx,
                run_deadline,
                &session_id,
                &mut next_id,
                &mut advertised,
                key,
                value,
                false,
            ) {
                return None;
            }
        }
        if !caller_config_is_fully_current(&advertised, &request.config, &self.forced_config) {
            for key in &caller_apply_order {
                let value = request.config.get(key).expect("caller key was enumerated");
                if self.forced_config.contains_key(key) {
                    continue;
                }
                if config_override_is_current(&advertised, key, value) {
                    continue;
                }
                if !self.apply_pre_prompt_config_option(
                    &request.run_id,
                    entry,
                    child,
                    outbound,
                    rx,
                    run_deadline,
                    &session_id,
                    &mut next_id,
                    &mut advertised,
                    key,
                    value,
                    false,
                ) {
                    return None;
                }
            }
        }

        // Caller-allowed changes may have provider-side effects on another
        // option. Re-assert any forced value that drifted.
        for (key, value) in &self.forced_config {
            if config_override_is_valid(&advertised, key, value)
                && config_override_is_current(&advertised, key, value)
            {
                continue;
            }
            if !self.apply_pre_prompt_config_option(
                &request.run_id,
                entry,
                child,
                outbound,
                rx,
                run_deadline,
                &session_id,
                &mut next_id,
                &mut advertised,
                key,
                value,
                true,
            ) {
                return None;
            }
        }

        // Final fail-closed check immediately before the prompt-dispatch path.
        if self.forced_config.iter().any(|(key, value)| {
            !config_override_is_valid(&advertised, key, value)
                || !config_override_is_current(&advertised, key, value)
        }) {
            self.setup_failure(
                &request.run_id,
                entry,
                "coding_agent_forced_config_not_applied",
                "Runner-enforced ACP config was not active before prompt dispatch",
            );
            self.terminate_run_io(child, outbound);
            return None;
        }
        if !caller_config_is_fully_current(&advertised, &request.config, &self.forced_config) {
            self.setup_failure(
                &request.run_id,
                entry,
                "coding_agent_config_not_applied",
                "caller-requested ACP config was not active before prompt dispatch",
            );
            self.terminate_run_io(child, outbound);
            return None;
        }

        let live_confirmed = build_live_confirmed_effective_config(
            &advertised,
            &self.forced_config,
            &request.config,
        );
        Some((session_id, next_id, live_confirmed))
    }
}
