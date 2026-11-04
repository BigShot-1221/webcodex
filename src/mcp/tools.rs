use super::app_registry;
use super::presentation;
use super::resources;
use super::response::{
    mcp_runtime_tool_result_fallback, mcp_stateless_result, rpc_error, rpc_error_with_data,
    rpc_result, McpToolResultPresentation,
};
use super::{require_mcp_scope, scope_forbidden, McpOutcome};
use crate::auth::AuthContext;
use crate::model_surface::AdaptiveRuntimeGatewayTargetRoute;
pub(super) use crate::model_surface::ADAPTIVE_RUNTIME_GATEWAY_TOOL_NAME;
use crate::model_surface::{
    project_suggested_tool_call_schema, project_tool_result_suggested_calls, SuggestedToolCallRoute,
};
use crate::tool_request_trace::ToolRequestLifecycle;
use crate::tool_runtime::kernel::{
    check_runtime_tool_scope, HostFileImportTrust, ToolCallContext, ToolCallErrorStatus,
    ToolCallRequest as KernelToolCallRequest, ToolInvocationMetadata, ToolProtocolCapabilities,
    ToolTransport,
};
use crate::tool_runtime::model_ergonomics_telemetry::{
    ModelErgonomicsRecord, ModelErgonomicsTimer,
};
use crate::tool_runtime::specialized::SpecializedGovernanceDenial;
use crate::tool_runtime::tool_definition::{
    is_adaptive_runtime_direct_tool, is_model_visible_tool_name,
    runtime_tool_operator_extension_family, ToolOperatorExtensionFamily,
};
use crate::tool_runtime::{SuggestedToolCall, ToolCall, ToolResult, ToolRuntime, ToolSpec};
use serde::Deserialize;
use serde_json::{json, Value};
use webcodex_tool_contracts::registry::{self as tool_registry, ToolDescriptor};

mod call;
pub(super) use call::handle_call;
mod invocation;
use invocation::{
    mcp_invocation_envelope_supported_fields, validate_mcp_invocation_envelope_for_target,
    MCP_INVOCATION_ENVELOPE_FIELD,
};
pub(super) use invocation::{parse_mcp_invocation_envelope, McpInvocationEnvelope};

pub(super) const WORK_RESULT_APP_RESULT_META_KEY: &str = "webcodex/workResult";
pub(super) const WORK_RESULT_INPUT_META_KEY: &str = "webcodex/workResultInput";
pub(super) const WORK_RESULT_THREAD_CONTEXT_META_KEY: &str = "webcodex/workResultThread";
const WORK_RESULT_THREAD_ENTRYPOINT_TOOL_NAME: &str = "work_result_thread_panel";

fn filter_descriptors_for_oauth(
    mut specs: Vec<ToolDescriptor>,
    auth: Option<&AuthContext>,
) -> Vec<ToolDescriptor> {
    specs.retain(|spec| {
        crate::tool_runtime::kernel::runtime_tool_scope_allows_discovery(auth, &spec.name)
    });
    specs
}

fn work_result_thread_entrypoint_tool_json(compact: bool) -> Value {
    let state = tool_registry::work_result_app_tool_descriptors()
        .into_iter()
        .find(|spec| spec.name == "get_work_result_state")
        .expect("Work Result state App tool must exist");
    // Only this Host launcher can succeed without a selected Work Result.
    // Canonical state reads retain their exact Project-bound output contract.
    let annotations = state.annotations();
    let output_schema = (!compact).then(|| {
        let mut schema = state.into_spec().output_schema;
        schema["properties"]["output"]["properties"]["work_result"]["type"] =
            json!(["object", "null"]);
        schema
    });
    mcp_tool_parts_json(
        WORK_RESULT_THREAD_ENTRYPOINT_TOOL_NAME.to_string(),
        "Host UI entrypoint only; models should use present_work_result, not call this tool. Opens the Work Result already presented in this exact Host Window, or an empty panel when none has been presented. The Host invokes this entrypoint with an empty object; WebCodex resolves only a prior successful present_work_result binding from the same authenticated Window and then reuses the normal work_result_state authorization and projection path. An empty panel selects no Project or Session and creates no binding.".to_string(),
        json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false
        }),
        output_schema,
        annotations,
        false,
    )
}

// Discovery projection only. Canonical operator-extension specs still own
// direct compatibility, capability-aware manifests, and gateway admission.
fn stateless_advertised_operator_extension_descriptors_for_auth(
    stateless_2026: bool,
    auth: Option<&AuthContext>,
) -> Vec<ToolDescriptor> {
    if !stateless_2026 {
        return Vec::new();
    }
    tool_registry::stateless_operator_extension_tool_descriptors()
        .into_iter()
        .filter(
            |spec| match runtime_tool_operator_extension_family(&spec.name) {
                Some(ToolOperatorExtensionFamily::SkillManagement) => {
                    auth.is_some_and(|auth| auth.has_scope(crate::auth::SCOPE_ADMIN))
                }
                Some(ToolOperatorExtensionFamily::TraceDiagnostics) => {
                    check_runtime_tool_scope(auth, &spec.name).is_ok()
                }
                Some(
                    ToolOperatorExtensionFamily::SkillRuntime
                    | ToolOperatorExtensionFamily::MemoryRuntime
                    | ToolOperatorExtensionFamily::MemoryManagement,
                )
                | None => false,
            },
        )
        .collect()
}

fn adaptive_runtime_gateway_tool_json(compact: bool) -> Value {
    mcp_tool_parts_json(
        ADAPTIVE_RUNTIME_GATEWAY_TOOL_NAME.to_string(),
        "Invoke one admitted runtime tool when its direct callable is unavailable. Use read_tool_manifest for exact contracts/routes; if it is not directly callable, invoke this gateway with tool='read_tool_manifest'. Follow route.primary, or route.fallback only when authorized. Never guess names/fields or bypass OAuth/permission gates. With MCP Apps enabled, call App presentation tools directly for Host metadata. Retired wrapper failures do not imply ACP is absent.".to_string(),
        json!({
            "type": "object",
            "properties": {
                "tool": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 128,
                    "description": "Exact runtime tool name already admitted by Adaptive Runtime. Discover the contract with read_tool_manifest before calling when it is not already known."
                },
                "arguments": {
                    "type": "object",
                    "description": "Canonical business arguments for the selected runtime tool after read_tool_manifest (or another authoritative contract source) confirms the admitted name and input schema.",
                    "additionalProperties": true
                }
            },
            "required": ["tool", "arguments"],
            "additionalProperties": false
        }),
        (!compact).then(|| json!({
            "type": "object",
            "description": "Normal MCP result for the selected runtime tool.",
            "additionalProperties": true
        })),
        json!({
            "readOnlyHint": false,
            "destructiveHint": true,
            "idempotentHint": false,
            // The gateway also admits open-world native execution targets.
            // Its descriptor must conservatively cover their possible effects.
            "openWorldHint": true
        }),
        false,
    )
}

#[cfg(test)]
pub(crate) fn adaptive_runtime_gateway_input_schema_for_test() -> Value {
    adaptive_runtime_gateway_tool_json(false)["inputSchema"].take()
}

fn mcp_adaptive_runtime_gateway_target_route(
    target: &str,
    stateless_2026: bool,
) -> crate::model_surface::AdaptiveRuntimeGatewayTargetRoute {
    use crate::model_surface::AdaptiveRuntimeGatewayTargetRoute;

    if stateless_2026 && runtime_tool_operator_extension_family(target).is_some() {
        let (availability, gateway_tool) =
            crate::model_surface::adaptive_runtime_tool_invocation_route_with_operator_extension(
                target, true,
            );
        return match (availability, gateway_tool) {
            (crate::model_surface::TOOL_SURFACE_AVAILABILITY_DIRECT, None) => {
                AdaptiveRuntimeGatewayTargetRoute::Direct
            }
            (
                crate::model_surface::TOOL_SURFACE_AVAILABILITY_GATEWAY,
                Some(ADAPTIVE_RUNTIME_GATEWAY_TOOL_NAME),
            ) => AdaptiveRuntimeGatewayTargetRoute::Gateway,
            _ => AdaptiveRuntimeGatewayTargetRoute::Unknown,
        };
    }
    let route = crate::model_surface::adaptive_runtime_gateway_target_route(target);
    if route != AdaptiveRuntimeGatewayTargetRoute::Unknown {
        return route;
    }
    // The MCP adapter's own gateway remains a specialized protocol route. It is
    // not part of the runtime ToolSpec extension universe.
    if target == crate::mcp_gateway::MCP_TOOL_NAME {
        return AdaptiveRuntimeGatewayTargetRoute::Gateway;
    }
    AdaptiveRuntimeGatewayTargetRoute::Unknown
}

#[cfg(test)]
pub(crate) fn adaptive_runtime_gateway_target_admitted_for_test(
    target: &str,
    stateless_2026: bool,
) -> bool {
    matches!(
        mcp_adaptive_runtime_gateway_target_route(target, stateless_2026),
        crate::model_surface::AdaptiveRuntimeGatewayTargetRoute::Gateway
            | crate::model_surface::AdaptiveRuntimeGatewayTargetRoute::Direct
    )
}

fn mcp_suggested_tool_call_route(target: &str, stateless_2026: bool) -> SuggestedToolCallRoute {
    if target == ADAPTIVE_RUNTIME_GATEWAY_TOOL_NAME {
        return SuggestedToolCallRoute::Unavailable;
    }
    if target == crate::mcp_gateway::MCP_TOOL_NAME {
        return SuggestedToolCallRoute::Gateway(ADAPTIVE_RUNTIME_GATEWAY_TOOL_NAME);
    }
    let operator_extension_admitted =
        stateless_2026 && runtime_tool_operator_extension_family(target).is_some();
    crate::model_surface::suggested_tool_call_route(target, operator_extension_admitted)
}

fn project_mcp_tool_spec_output_schema(mut spec: ToolSpec, stateless_2026: bool) -> ToolSpec {
    project_suggested_tool_call_schema(&mut spec.output_schema, &|target| {
        mcp_suggested_tool_call_route(target, stateless_2026)
    });
    spec
}

fn unwrap_adaptive_runtime_gateway_arguments(arguments: Value) -> Result<(String, Value), String> {
    let mut outer = arguments
        .as_object()
        .cloned()
        .ok_or_else(|| "adaptive runtime gateway arguments must be an object".to_string())?;
    let target = outer
        .remove("tool")
        .and_then(|value| value.as_str().map(str::trim).map(str::to_string))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            "adaptive runtime gateway field 'tool' must be a non-empty string".to_string()
        })?;
    let mut target_arguments = outer.remove("arguments").unwrap_or_else(|| json!({}));
    if target_arguments.is_null() {
        target_arguments = json!({});
    }
    let target_object = target_arguments.as_object_mut().ok_or_else(|| {
        "adaptive runtime gateway field 'arguments' must be an object".to_string()
    })?;
    if target_object.contains_key(MCP_INVOCATION_ENVELOPE_FIELD) {
        return Err(
            "adaptive runtime gateway target 'arguments' must contain canonical business arguments only; put invocation metadata in outer '_wc'"
                .to_string(),
        );
    }
    if let Some(key) = outer.keys().next() {
        return Err(format!(
            "unsupported adaptive runtime gateway field '{key}'"
        ));
    }
    Ok((target, target_arguments))
}
fn adaptive_runtime_read_tool_manifest_gateway_arguments(arguments: Value) -> Value {
    json!({
        "tool": "read_tool_manifest",
        "arguments": arguments,
    })
}

/// Legacy invocation metadata used to appear beside business arguments. Keep
/// rejecting it, but report the current `_wc` contract so stale Hosts can recover.
const LEGACY_MCP_INVOCATION_FIELDS: &[&str] = &[
    "_session_id",
    crate::tool_runtime::sessions::TOOL_CALL_RECORDING_SESSION_ID_FIELD,
    crate::tool_runtime::sessions::TOOL_CALL_ACK_SESSION_MESSAGE_IDS_FIELD,
    crate::tool_runtime::sessions::TOOL_CALL_ACK_REF_FIELD,
    crate::tool_runtime::sessions::TOOL_CALL_SESSION_MESSAGE_RESOLUTION_FIELD,
    crate::tool_runtime::window_collaboration::TOOL_CALL_WINDOW_REPLY_FIELD,
    crate::tool_runtime::context_projection::TOOL_CALL_CONTEXT_REQUEST_FIELD,
    crate::tool_runtime::control_sidecar::CONTROL_FIELD,
];

pub(crate) fn mcp_legacy_invocation_contract_fields(
    tool_name: &str,
    arguments: &Value,
) -> Vec<&'static str> {
    if !is_model_visible_tool_name(tool_name) {
        return Vec::new();
    }
    let Some(object) = arguments.as_object() else {
        return Vec::new();
    };
    LEGACY_MCP_INVOCATION_FIELDS
        .iter()
        .copied()
        .filter(|field| object.contains_key(*field))
        .collect()
}

/// Known retired fields are reported before ToolRuntime executes a call so a Host
/// with cached MCP metadata can recover from the current canonical contract. This
/// is diagnostic only: no aliases are accepted and authority is unchanged.
pub(crate) fn mcp_stale_tool_contract_fields(
    tool_name: &str,
    arguments: &Value,
) -> Vec<&'static str> {
    let mut fields = mcp_legacy_invocation_contract_fields(tool_name, arguments);
    if tool_name == "work_on_project" {
        if let Some(object) = arguments.as_object() {
            for field in ["include_project_instructions", "include_workflow_guidance"] {
                if object.contains_key(field) {
                    fields.push(field);
                }
            }
        }
    }
    fields
}

pub(crate) struct McpStaleToolContractDiagnostic {
    pub message: String,
    pub diagnostics: Value,
}

pub(crate) fn mcp_stale_tool_contract_diagnostic(
    tool_name: &str,
    fields: &[&str],
    auth: Option<&AuthContext>,
    stateless_2026: bool,
) -> McpStaleToolContractDiagnostic {
    let field_list = fields.join(", ");
    let message = format!(
        "tool '{tool_name}' received field(s) not accepted by the current contract: {field_list}. No execution occurred. Refresh tools/list or reconnect the MCP Host, then discover the canonical contract with read_tool_manifest and retry using only current fields. Legacy invocation metadata belongs in the outer '_wc' object (for example '_wc.context' for context requests), not in business arguments."
    );
    let discovery_admitted = is_model_visible_tool_name(tool_name)
        && matches!(
            mcp_adaptive_runtime_gateway_target_route("read_tool_manifest", stateless_2026),
            AdaptiveRuntimeGatewayTargetRoute::Gateway | AdaptiveRuntimeGatewayTargetRoute::Direct
        )
        && crate::tool_runtime::kernel::runtime_tool_scope_allows_discovery(
            auth,
            "read_tool_manifest",
        );
    let mut diagnostics = json!({
        "error_kind": "stale_tool_contract",
        "execution_state": "not_started",
        "state_changed": false,
        "target_tool": tool_name,
        "recovery_kind": "fix_input",
        "retired_fields": fields,
        "discovery_tool": "read_tool_manifest",
    });
    if discovery_admitted {
        diagnostics["suggested_call"] = SuggestedToolCall::fallback_recovery(
            ADAPTIVE_RUNTIME_GATEWAY_TOOL_NAME,
            adaptive_runtime_read_tool_manifest_gateway_arguments(json!({
                "tool_name": tool_name,
            })),
        )
        .to_value();
    }
    McpStaleToolContractDiagnostic {
        message,
        diagnostics,
    }
}

fn adaptive_runtime_gateway_unknown_target(target: &str) -> ToolResult {
    let discovery_arguments = json!({ "query": target, "limit": 10 });
    let suggested_call = SuggestedToolCall::fallback_recovery(
        ADAPTIVE_RUNTIME_GATEWAY_TOOL_NAME,
        adaptive_runtime_read_tool_manifest_gateway_arguments(discovery_arguments),
    )
    .to_value();
    ToolResult::err_with_output(
        format!(
            "unknown adaptive runtime tool '{target}'. No target execution occurred and WebCodex does not auto-correct or dispatch guessed tool names. Refresh tools/list when supported, then discover admitted tools with read_tool_manifest using query, category, or intent (do not assume tool_name='{target}' is valid). When read_tool_manifest is not directly callable on this MCP surface, invoke call_runtime_tool with tool='read_tool_manifest' and arguments such as intent='coding' or tool_name='start_coding_agent', then follow the returned route; stale retired direct-wrapper failures are not proof ACP is absent."
        ),
        json!({
            "error_kind": "unknown_tool",
            "execution_state": "not_started",
            "state_changed": false,
            "target_tool": target,
            "recovery_kind": "fix_input",
            "discovery_tool": "read_tool_manifest",
            "suggested_call": suggested_call,
        }),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum McpAdaptiveRuntimeDirectRouteDenialKind {
    NotOnSurface,
    GatewayRouteRequired,
    InsufficientScope,
}

pub(crate) struct McpAdaptiveRuntimeDirectRouteDenial {
    #[cfg(test)]
    pub kind: McpAdaptiveRuntimeDirectRouteDenialKind,
    pub message: String,
    pub diagnostics: Value,
}

pub(crate) fn mcp_adaptive_runtime_direct_route_denial(
    tool_name: &str,
    auth: Option<&AuthContext>,
    stateless_2026: bool,
) -> McpAdaptiveRuntimeDirectRouteDenial {
    let route = mcp_adaptive_runtime_gateway_target_route(tool_name, stateless_2026);
    let admitted = matches!(
        route,
        AdaptiveRuntimeGatewayTargetRoute::Gateway | AdaptiveRuntimeGatewayTargetRoute::Direct
    );
    let scope_allows =
        crate::tool_runtime::kernel::runtime_tool_scope_allows_discovery(auth, tool_name);

    let kind = if admitted && !scope_allows {
        McpAdaptiveRuntimeDirectRouteDenialKind::InsufficientScope
    } else if admitted && route == AdaptiveRuntimeGatewayTargetRoute::Gateway {
        McpAdaptiveRuntimeDirectRouteDenialKind::GatewayRouteRequired
    } else {
        McpAdaptiveRuntimeDirectRouteDenialKind::NotOnSurface
    };

    let message = match kind {
        McpAdaptiveRuntimeDirectRouteDenialKind::InsufficientScope => format!(
            "tool '{tool_name}' is not callable on this MCP surface under the current authorization; refresh tools/list for admitted callables and check granted OAuth scopes"
        ),
        McpAdaptiveRuntimeDirectRouteDenialKind::GatewayRouteRequired => format!(
            "tool '{tool_name}' is admitted on Adaptive Runtime but not as a direct MCP callable; invoke it with call_runtime_tool using its exact canonical arguments and normal authorization. Load the contract first by calling call_runtime_tool with tool='read_tool_manifest' and arguments containing tool_name='{tool_name}', then follow the returned route"
        ),
        McpAdaptiveRuntimeDirectRouteDenialKind::NotOnSurface => format!(
            "tool '{tool_name}' is not exposed as a direct MCP callable on this surface. Refresh tools/list when supported, then discover admitted canonical tool names with read_tool_manifest via query, category, or intent, or invoke call_runtime_tool with tool='read_tool_manifest' and arguments containing intent='coding' or tool_name='start_coding_agent', then follow the returned route. Do not retry stale retired direct wrappers such as tool_manifest; their not-on-surface failure is not proof ACP is absent. WebCodex does not auto-correct unknown tool names"
        ),
    };

    let error_kind = match kind {
        McpAdaptiveRuntimeDirectRouteDenialKind::InsufficientScope => "insufficient_scope",
        McpAdaptiveRuntimeDirectRouteDenialKind::GatewayRouteRequired => "gateway_route_required",
        McpAdaptiveRuntimeDirectRouteDenialKind::NotOnSurface => "direct_route_denied",
    };

    let mut diagnostics = json!({
        "error_kind": error_kind,
        "execution_state": "not_started",
        "state_changed": false,
        "target_tool": tool_name,
        "recovery_kind": "fix_input",
    });
    if kind == McpAdaptiveRuntimeDirectRouteDenialKind::GatewayRouteRequired {
        diagnostics["suggested_call"] = SuggestedToolCall::fallback_recovery(
            ADAPTIVE_RUNTIME_GATEWAY_TOOL_NAME,
            adaptive_runtime_read_tool_manifest_gateway_arguments(
                json!({ "tool_name": tool_name }),
            ),
        )
        .to_value();
    } else if kind == McpAdaptiveRuntimeDirectRouteDenialKind::NotOnSurface {
        diagnostics["suggested_call"] = SuggestedToolCall::fallback_recovery(
            ADAPTIVE_RUNTIME_GATEWAY_TOOL_NAME,
            adaptive_runtime_read_tool_manifest_gateway_arguments(json!({
                "query": tool_name,
                "limit": 10,
            })),
        )
        .to_value();
        diagnostics["discovery_tool"] = json!("read_tool_manifest");
    }

    McpAdaptiveRuntimeDirectRouteDenial {
        #[cfg(test)]
        kind,
        message,
        diagnostics,
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct McpToolCallParams {
    pub(super) name: String,
    #[serde(default)]
    pub(super) arguments: Value,
}

pub(super) fn tool_name_from_params(params: &Value) -> Option<String> {
    let name = params.get("name").and_then(Value::as_str)?;
    if name == ADAPTIVE_RUNTIME_GATEWAY_TOOL_NAME {
        return params["arguments"]["tool"]
            .as_str()
            .map(str::to_string)
            .or_else(|| Some(name.to_string()));
    }
    Some(name.to_string())
}

pub(super) fn project_from_tool_call_params(params: &Value) -> Option<String> {
    if params.get("name").and_then(Value::as_str) == Some(ADAPTIVE_RUNTIME_GATEWAY_TOOL_NAME) {
        return params["arguments"]["arguments"]["project"]
            .as_str()
            .map(str::to_string);
    }
    params["arguments"]["project"].as_str().map(str::to_string)
}

/// Test helper for legacy/non-stateless tools/list rendering. Production uses
/// the canonical auth/surface renderer below directly.
#[cfg(test)]
pub(super) fn mcp_tools_list_payload_with_compact(compact: bool) -> Value {
    mcp_tools_list_payload_with_features(compact, false)
}

#[cfg(test)]
pub(super) fn mcp_tools_list_payload_with_compact_and_app(
    compact: bool,
    app_enabled: bool,
) -> Value {
    mcp_tools_list_payload_with_features(compact, app_enabled)
}

#[cfg(test)]
fn mcp_tools_list_payload_with_features(compact: bool, app_enabled: bool) -> Value {
    mcp_tools_list_payload_with_features_for_auth(compact, app_enabled, false, None)
}

pub(super) fn mcp_tools_list_payload_with_features_for_auth(
    compact: bool,
    app_enabled: bool,
    stateless_2026: bool,
    auth: Option<&AuthContext>,
) -> Value {
    let mut specs = filter_descriptors_for_oauth(
        crate::model_surface::adaptive_runtime_direct_tool_descriptors(),
        auth,
    );
    specs
        .extend(stateless_advertised_operator_extension_descriptors_for_auth(stateless_2026, auth));

    let mut tools = specs
        .into_iter()
        .map(|spec| mcp_tool_descriptor_json(spec, compact, app_enabled, Some(stateless_2026)))
        .collect::<Vec<_>>();
    if app_enabled && stateless_2026 {
        for name in super::workbench::CANONICAL_TOOLS {
            if tools.iter().any(|tool| tool["name"] == *name)
                || !crate::tool_runtime::kernel::runtime_tool_scope_allows_discovery(auth, name)
            {
                continue;
            }
            if let Some(definition) =
                crate::tool_runtime::tool_definition::lookup_tool_definition(name)
            {
                if let Some(model) = definition.model_spec {
                    let spec = ToolDescriptor {
                        name: (*name).into(),
                        description: model.description.into(),
                        discovery_keywords: model.discovery_keywords,
                    };
                    let mut value = mcp_tool_descriptor_json(spec, compact, false, None);
                    attach_app_visibility(&mut value);
                    tools.push(value);
                }
            }
        }
        tools.extend(
            super::workbench::descriptors(compact)
                .into_iter()
                .filter(|tool| match tool["name"].as_str() {
                    Some("update_workbench_settings") => {
                        auth.is_some_and(|a| a.kind != crate::auth::AuthKind::OpenAnonymous)
                            && check_runtime_tool_scope(auth, "update_goal").is_ok()
                    }
                    Some("submit_workbench_work") => {
                        check_runtime_tool_scope(auth, "create_agent_task").is_ok()
                    }
                    _ => true,
                }),
        );
        // The View uses the existing authorized Session discovery implementation.
        if !tools.iter().any(|tool| tool["name"] == "list_sessions") {
            if let Some(definition) =
                crate::tool_runtime::tool_definition::lookup_tool_definition("list_sessions")
            {
                let description = if compact {
                    "List authorized Sessions for an explicitly selected Project. Page with offset/limit and select an exact session_id before browsing recorded outputs."
                } else {
                    definition
                        .model_spec
                        .expect("public Session descriptor")
                        .description
                };
                let spec = ToolDescriptor {
                    name: "list_sessions".into(),
                    description: description.into(),
                    discovery_keywords: definition.model_spec.unwrap().discovery_keywords,
                };
                for spec in filter_descriptors_for_oauth(vec![spec], auth) {
                    let mut value = mcp_tool_descriptor_json(spec, compact, false, None);
                    attach_app_visibility(&mut value);
                    tools.push(value);
                }
            }
        }
        if check_runtime_tool_scope(auth, "get_work_result_state").is_ok() {
            let mut thread_entrypoint = work_result_thread_entrypoint_tool_json(compact);
            // A user-opened launcher renders a View, so it must remain public.
            // ChatGPT rejects private/App-only tools with rendering resources.
            attach_app_metadata(
                &mut thread_entrypoint,
                app_registry::MCP_WORK_RESULT_UI_RESOURCE_URI,
            );
            attach_openai_thread_entrypoint(&mut thread_entrypoint);
            tools.push(thread_entrypoint);
        }
        let mut app_specs = filter_descriptors_for_oauth(
            tool_registry::goal_plan_app_tool_descriptors()
                .into_iter()
                .chain(tool_registry::work_result_app_tool_descriptors())
                .chain(tool_registry::artifact_app_tool_descriptors())
                .chain(tool_registry::agent_continuation_app_tool_descriptors())
                .chain(tool_registry::job_terminal_continuation_app_tool_descriptors())
                .collect(),
            auth,
        )
        .into_iter()
        .map(|spec| {
            let agent_continuation_tool = is_agent_continuation_app_tool_name(&spec.name);
            let job_terminal_continuation_tool =
                is_job_terminal_continuation_app_tool_name(&spec.name);
            let goal_sync_tool = spec.name == "sync_goal_plan";
            let mut value = mcp_tool_descriptor_json(spec, compact, false, None);
            attach_app_visibility(&mut value);
            if agent_continuation_tool || job_terminal_continuation_tool || goal_sync_tool {
                // Bridge helpers return data to an existing View; they never
                // render another widget. Private rendering tools are rejected
                // by ChatGPT during discovery/refresh.
                attach_agent_continuation_app_diagnostic_schema(&mut value);
            }
            value
        })
        .collect::<Vec<_>>();
        tools.append(&mut app_specs);
        if ["list_projects", "list_goals"]
            .iter()
            .any(|tool| crate::tool_runtime::kernel::check_runtime_tool_scope(auth, tool).is_ok())
        {
            let mut mention = json!({
                "name":"search_mentions","description":"Search authorized WebCodex Projects and owned Goals for resource references. Files and artifacts require explicit Workbench selection.",
                "inputSchema":{"type":"object","properties":{"query":{"type":"string","maxLength":200}},"required":["query"],"additionalProperties":false},
                "annotations":{"readOnlyHint":true},"_meta":{"openai/extensions":{"mentions/search":{}},"ui":{"visibility":["app"]}}
            });
            if compact {
                super::discovery::compact_tool(&mut mention);
            } else {
                mention["outputSchema"] = json!({"type":"object","properties":{"items":{"type":"array","maxItems":20,"items":{"type":"object","required":["type","uri","name"],"properties":{"type":{"const":"resource_link"},"uri":{"type":"string"},"name":{"type":"string"},"title":{"type":"string"},"description":{"type":"string"},"mimeType":{"type":"string"},"_meta":{"type":"object"}},"additionalProperties":false}}},"required":["items"],"additionalProperties":false});
            }

            tools.push(mention);
        }
    }
    if auth.is_some_and(AuthContext::is_oauth_token) {
        for tool in tools.iter_mut() {
            let Some(name) = tool["name"].as_str().map(str::to_string) else {
                continue;
            };
            if let Some(scope_options) = crate::auth::oauth_mcp_security_scheme_scopes(&name) {
                tool["securitySchemes"] = json!(scope_options
                    .into_iter()
                    .map(|scopes| json!({ "type": "oauth2", "scopes": scopes }))
                    .collect::<Vec<_>>());
            }
        }
    }
    json!({ "tools": tools })
}

fn adapt_native_image_output_schema_for_mcp(schema: &mut Value) {
    let properties = schema
        .pointer_mut("/properties/output/properties")
        .and_then(Value::as_object_mut)
        .expect("observe_computer output schema properties");
    properties.remove("content_base64");
    properties.insert(
        "content_delivery".to_string(),
        json!({
            "type": "string",
            "const": "mcp_image",
            "description": "MCP native-image delivery marker; binary image bytes are carried in the image ContentBlock rather than structuredContent."
        }),
    );
}

fn mcp_context_projection_output_schema() -> Value {
    json!({
        "type": "object",
        "description": "Optional bounded post-tool context sidecar. It describes material projected after the main effect/observation and never grants authority or retroactively governs that effect.",
        "properties": {
            "materials": {
                "type": "array",
                "maxItems": crate::tool_runtime::context_projection::MAX_CONTEXT_REQUEST_ITEMS,
                "items": {
                    "type": "object",
                    "properties": {
                        "key": {"type": "string", "maxLength": crate::tool_runtime::context_projection::MAX_CONTEXT_REQUEST_KEY_CHARS},
                        "status": {"type": "string", "enum": ["available", "unavailable", "unsupported"]},
                        "reason_code": {"type": "string"},
                        "projection": {}
                    },
                    "required": ["key", "status"],
                    "additionalProperties": false
                }
            },
            "truncated": {"type": "boolean"}
        },
        "required": ["materials", "truncated"],
        "additionalProperties": false
    })
}

fn add_wrapper_projection_to_output_shape(
    schema: &mut Value,
    field: &str,
    projection_schema: &Value,
) {
    if schema.get("type").and_then(Value::as_str) == Some("object") {
        if let Some(properties) = schema.get_mut("properties").and_then(Value::as_object_mut) {
            properties.insert(field.to_string(), projection_schema.clone());
        }
    }
    for keyword in ["anyOf", "oneOf", "allOf"] {
        if let Some(branches) = schema.get_mut(keyword).and_then(Value::as_array_mut) {
            for branch in branches {
                add_wrapper_projection_to_output_shape(branch, field, projection_schema);
            }
        }
    }
}

fn add_stateless_context_projection_output_schema(tool: &mut Value) {
    let Some(output_schema) = tool.get_mut("outputSchema") else {
        return;
    };
    let projection_schema = mcp_context_projection_output_schema();
    if let Some(output) = output_schema.pointer_mut("/properties/output") {
        add_wrapper_projection_to_output_shape(output, "context_projection", &projection_schema);
    }
    if let Some(conditions) = output_schema.get_mut("allOf").and_then(Value::as_array_mut) {
        for condition in conditions {
            for branch_name in ["then", "else"] {
                if let Some(output) =
                    condition.pointer_mut(&format!("/{branch_name}/properties/output"))
                {
                    add_wrapper_projection_to_output_shape(
                        output,
                        "context_projection",
                        &projection_schema,
                    );
                }
            }
        }
    }
}

fn stateless_collaboration_ack_schema() -> Value {
    json!({
        "type": "array",
        "maxItems": crate::tool_runtime::sessions::MAX_TOOL_CALL_ACK_MESSAGE_IDS,
        "items": {
            "type": "string",
            "pattern": "^wc_msg_([A-Za-z0-9_-]{16}|[0-9a-f]{32})$"
        },
        "description": "Proves the current model context still retains the listed ACK-required collaboration messages. Session ACK uses the explicit recorder when present, otherwise an authorized same-Window active Session affinity for the resolved Project; Peer and Operator ACK target the current principal-bound ClientWindow. Repeat while retained. The historical field name is shared across all three channels. ACK neither resolves messages nor grants authority or gates execution."
    })
}

fn stateless_session_ack_ref_schema() -> Value {
    json!({
        "type": "string",
        "maxLength": crate::tool_runtime::sessions::MAX_TOOL_CALL_ACK_REF_CHARS,
        "description": "Compact request-scoped ACK evidence returned as session_attention.ack_ref for the exact retained Session ACK set represented by that exchange. It acknowledges only that Session set, grants no authority, never resolves messages, and does not apply to Peer ACK."
    })
}

fn stateless_session_attention_output_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "description": "Bounded open ACK-required messages from one exact authorized Workflow Session. Window affinity may select this delivery scope only when no explicit recorder was supplied; it never records the main call or supplies business authority.",
        "properties": {
            "session_id": {
                "type": "string",
                "pattern": "^wc_sess_([A-Za-z0-9_-]{16}|[0-9a-f]{32})$"
            },
            "source": {
                "type": "string",
                "enum": ["recording_session", "business_session", "window_affinity"]
            },
            "requires_ack": {"type": "boolean"},
            "ack_ref": {
                "type": "string",
                "maxLength": crate::tool_runtime::sessions::MAX_TOOL_CALL_ACK_REF_CHARS,
                "description": "Exact compact Session ACK set retained across this exchange; echo on a later request while this context is still retained."
            },
            "messages": {
                "type": "array",
                "maxItems": crate::tool_runtime::SESSION_ATTENTION_MAX_MESSAGES,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "properties": {
                        "message_id": {"type": "string"},
                        "kind": {"type": "string"},
                        "priority": {"type": "string"},
                        "created_at": {"type": "integer"},
                        "message": {"type": "string"},
                        "message_truncated": {"type": "boolean"}
                    },
                    "required": ["message_id", "kind", "priority", "created_at", "message", "message_truncated"]
                }
            },
            "omitted_count": {"type": "integer", "minimum": 0},
            "truncated": {"type": "boolean"},
            "ack": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "accepted_count": {"type": "integer", "minimum": 0},
                    "ignored_count": {"type": "integer", "minimum": 0}
                },
                "required": ["accepted_count", "ignored_count"]
            }
        },
        "required": ["session_id", "source", "requires_ack", "messages", "omitted_count", "truncated", "ack"]
    })
}

fn add_stateless_session_attention_output_schema(tool: &mut Value) {
    let Some(output_schema) = tool.get_mut("outputSchema") else {
        return;
    };
    let projection = stateless_session_attention_output_schema();
    if let Some(output) = output_schema.pointer_mut("/properties/output") {
        add_wrapper_projection_to_output_shape(output, "session_attention", &projection);
    }
    if let Some(conditions) = output_schema.get_mut("allOf").and_then(Value::as_array_mut) {
        for condition in conditions {
            for branch_name in ["then", "else"] {
                if let Some(output) =
                    condition.pointer_mut(&format!("/{branch_name}/properties/output"))
                {
                    add_wrapper_projection_to_output_shape(
                        output,
                        "session_attention",
                        &projection,
                    );
                }
            }
        }
    }
}

fn stateless_window_reply_supported(tool_name: Option<&str>) -> bool {
    let Some(tool_name) = tool_name else {
        return false;
    };
    if matches!(
        tool_name,
        crate::mcp_gateway::MCP_TOOL_NAME
            | crate::plugin_gateway::PLUGIN_TOOL_NAME
            | crate::ssh_resource_gateway::SSH_RESOURCE_TOOL_NAME
    ) {
        return false;
    }
    tool_name == ADAPTIVE_RUNTIME_GATEWAY_TOOL_NAME || is_model_visible_tool_name(tool_name)
}

fn stateless_window_reply_input_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "description": "Reply to one Operator message retained in this exact Window; no Workflow Session required. The reply is persisted independently after the main tool result.",
        "properties": {
            "reply_to": {
                "type": "string",
                "pattern": "^wc_msg_([A-Za-z0-9_-]{16}|[0-9a-f]{32})$"
            },
            "message": {
                "type": "string",
                "minLength": 1,
                "maxLength": crate::tool_runtime::window_collaboration::MAX_WINDOW_REPLY_CHARS
            }
        },
        "required": ["reply_to", "message"]
    })
}

fn stateless_window_reply_output_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": true,
        "properties": {
            "success": {"type": "boolean"},
            "message_id": {"type": "string"},
            "reply_to": {"type": "string"},
            "replayed": {"type": "boolean"},
            "state_changed": {}
        },
        "required": ["success", "state_changed"]
    })
}

fn add_stateless_window_reply_output_schema(tool: &mut Value) {
    let Some(output_schema) = tool.get_mut("outputSchema") else {
        return;
    };
    let projection = stateless_window_reply_output_schema();
    if let Some(output) = output_schema.pointer_mut("/properties/output") {
        add_wrapper_projection_to_output_shape(output, "window_reply", &projection);
    }
    if let Some(conditions) = output_schema.get_mut("allOf").and_then(Value::as_array_mut) {
        for condition in conditions {
            for branch_name in ["then", "else"] {
                if let Some(output) =
                    condition.pointer_mut(&format!("/{branch_name}/properties/output"))
                {
                    add_wrapper_projection_to_output_shape(output, "window_reply", &projection);
                }
            }
        }
    }
}

pub(super) const RECORDING_SESSION_SELECTOR_SCHEMA_PATTERN: &str =
    "^(~s[1-9][0-9]{0,19}|wc_sess_([A-Za-z0-9_-]{16}|[0-9a-f]{32}))$";

fn stateless_invocation_envelope_schema(tool_name: &str) -> Value {
    let allowed = mcp_invocation_envelope_supported_fields(tool_name);
    let mut properties = serde_json::Map::new();
    if allowed.contains(&"compact_execution") {
        properties.insert("compact_execution".to_string(), json!({
            "type":"boolean", "default":false,
            "description":"For run_process, run_script, run_shell, project_build, run_skill_resource and observe_jobs only. Opt in to output.execution (or items[].execution) control facts and separate details. Only outcome=passed proves execution success; outer success may mean acceptance. Retains logs and collaboration obligations. Does not wait, retry or authorize dependent work."
        }));
    }
    if allowed.contains(&"record") {
        properties.insert(
            "record".to_string(),
            json!({
                "type": "string",
                "pattern": RECORDING_SESSION_SELECTOR_SCHEMA_PATTERN,
                "description": "Optional explicit recorder provenance for one exact Workflow Session; canonical wc_sess_* or issued principal-scoped ~sN. Never business authority."
            }),
        );
    }
    if allowed.contains(&"ack") {
        properties.insert("ack".to_string(), stateless_collaboration_ack_schema());
    }
    if allowed.contains(&"ack_ref") {
        properties.insert("ack_ref".to_string(), stateless_session_ack_ref_schema());
    }
    if allowed.contains(&"reply") {
        properties.insert("reply".to_string(), stateless_window_reply_input_schema());
    }
    if allowed.contains(&"resolve") {
        properties.insert(
            "resolve".to_string(),
            json!({
                "type": "object",
                "description": "Resolve one handled non-todo message in the exact recording Session. Requires _wc.record; grants no authority.",
                "properties": {
                    "message_id": {
                        "type": "string",
                        "pattern": "^wc_msg_([A-Za-z0-9_-]{16}|[0-9a-f]{32})$"
                    },
                    "resolution": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": crate::tool_runtime::sessions::MAX_MESSAGE_RESOLUTION_CHARS
                    }
                },
                "required": ["message_id", "resolution"],
                "additionalProperties": false
            }),
        );
    }
    if allowed.contains(&"context") {
        properties.insert(
            "context".to_string(),
            json!({
                "type": "array",
                "maxItems": crate::tool_runtime::context_projection::MAX_CONTEXT_REQUEST_ITEMS,
                "items": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": crate::tool_runtime::context_projection::MAX_CONTEXT_REQUEST_KEY_CHARS
                },
                "description": format!("Request bounded post-result context material; unsupported keys are nonfatal. Current keys include {}.", crate::tool_runtime::context_projection::context_material_keys_csv())
            }),
        );
    }
    if allowed.contains(&"control") {
        properties.insert(
            "control".to_string(),
            crate::tool_runtime::control_sidecar::input_schema(tool_name),
        );
    }
    json!({
        "type": "object",
        "additionalProperties": false,
        "description": "Optional untrusted model-supplied WebCodex invocation sidecars. Omit when unused. Never grants authority, changes the business target, or supplies Host/Window trust.",
        "properties": properties
    })
}

pub(super) fn add_stateless_workflow_recorder_metadata(payload: &mut Value) {
    let Some(tools) = payload.get_mut("tools").and_then(Value::as_array_mut) else {
        return;
    };
    for tool in tools {
        let tool_name_owned = tool.get("name").and_then(Value::as_str).map(str::to_string);
        let tool_name = tool_name_owned.as_deref();
        if matches!(
            tool_name,
            Some(
                "sync_goal_plan"
                    | "get_work_result_state"
                    | "read_changed_file_diff"
                    | "search_mentions"
            )
        ) || tool_name == Some(WORK_RESULT_THREAD_ENTRYPOINT_TOOL_NAME)
            || tool_name.is_some_and(is_host_continuation_app_tool_name)
            || (tool.pointer("/_meta/ui/visibility") == Some(&json!(["app"]))
                && tool_name.is_some_and(|name| {
                    super::workbench::is_adapter_tool(name)
                        || super::workbench::CANONICAL_TOOLS.contains(&name)
                }))
        {
            continue;
        }
        let Some(name) = tool_name else {
            continue;
        };
        let Some(properties) = tool
            .pointer_mut("/inputSchema/properties")
            .and_then(Value::as_object_mut)
        else {
            continue;
        };
        properties.insert(
            MCP_INVOCATION_ENVELOPE_FIELD.to_string(),
            stateless_invocation_envelope_schema(name),
        );

        if name == ADAPTIVE_RUNTIME_GATEWAY_TOOL_NAME
            || crate::tool_runtime::control_sidecar::supports_control_sidecars(name)
        {
            let projection = crate::tool_runtime::control_sidecar::output_schema();
            if let Some(output) = tool.pointer_mut("/outputSchema/properties/output") {
                add_wrapper_projection_to_output_shape(output, "control", &projection);
            }
            if let Some(conditions) = tool
                .pointer_mut("/outputSchema/allOf")
                .and_then(Value::as_array_mut)
            {
                for condition in conditions {
                    for branch in ["then", "else"] {
                        if let Some(output) =
                            condition.pointer_mut(&format!("/{branch}/properties/output"))
                        {
                            add_wrapper_projection_to_output_shape(output, "control", &projection);
                        }
                    }
                }
            }
        }
        add_stateless_context_projection_output_schema(tool);
        add_stateless_session_attention_output_schema(tool);
        if stateless_window_reply_supported(tool_name) {
            add_stateless_window_reply_output_schema(tool);
        }
        super::execution_control::add_output_schema(tool, name);
    }
}
fn tool_meta_object(value: &mut Value) -> Option<&mut serde_json::Map<String, Value>> {
    let object = value.as_object_mut()?;
    let meta = object
        .entry("_meta".to_string())
        .or_insert_with(|| json!({}));
    meta.as_object_mut()
}

pub(super) fn attach_app_metadata(value: &mut Value, resource_uri: &str) {
    let Some(meta) = tool_meta_object(value) else {
        return;
    };
    let ui = meta.entry("ui".to_string()).or_insert_with(|| json!({}));
    let Some(ui) = ui.as_object_mut() else {
        return;
    };
    ui.insert(
        "resourceUri".to_string(),
        Value::String(resource_uri.to_string()),
    );
}

fn attach_openai_thread_entrypoint(value: &mut Value) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    object.insert(
        "title".to_string(),
        Value::String("WebCodex Work Result".to_string()),
    );
    let Some(meta) = tool_meta_object(value) else {
        return;
    };
    meta.insert(
        "openai/ui".to_string(),
        json!({
            "entrypoints": [
                {"type": "thread"}
            ]
        }),
    );
}
#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkResultThreadBinding {
    project: String,
    session_id: Option<String>,
}

fn work_result_thread_binding(
    runtime: &ToolRuntime,
    auth: Option<&AuthContext>,
    window: Option<&crate::client_window::ClientWindow>,
) -> Result<Option<WorkResultThreadBinding>, String> {
    let window = window.ok_or_else(|| {
        "Work Result thread panel requires a stable Host Window identity".to_string()
    })?;
    let auth = auth
        .filter(|auth| !auth.is_open_anonymous())
        .ok_or_else(|| {
            "Work Result thread panel requires a stable authenticated principal".to_string()
        })?;
    let (principal_kind, principal_id) =
        crate::tool_runtime::runtime_observation_principal(Some(auth)).map_err(|_| {
            "Work Result thread panel principal identity is unavailable".to_string()
        })?;
    let db = runtime
        .window_activity_db
        .as_ref()
        .ok_or_else(|| "Work Result thread panel activity store is unavailable".to_string())?;
    let event = db
        .latest_successful_window_action(
            window.key(),
            (principal_kind.as_str(), principal_id.as_str()),
            "present_work_result",
        )
        .map_err(|_| "Work Result thread panel activity lookup failed".to_string())?;
    let Some(event) = event else {
        return Ok(None);
    };
    let project = event
        .project
        .ok_or_else(|| "Work Result thread panel presentation binding is invalid".to_string())?;
    Ok(Some(WorkResultThreadBinding {
        project,
        // This comes from the exact selected present_work_result event's
        // canonical ActionAudit correlation. Never substitute Window
        // affinity from another action or Session.
        session_id: event.business_session_id,
    }))
}

#[cfg(test)]
pub(super) fn work_result_thread_binding_for_test(
    runtime: &ToolRuntime,
    auth: Option<&AuthContext>,
    window: Option<&crate::client_window::ClientWindow>,
) -> Result<Option<(String, Option<String>)>, String> {
    work_result_thread_binding(runtime, auth, window)
        .map(|binding| binding.map(|binding| (binding.project, binding.session_id)))
}

fn attach_app_visibility(value: &mut Value) {
    let Some(meta) = tool_meta_object(value) else {
        return;
    };
    let ui = meta.entry("ui".to_string()).or_insert_with(|| json!({}));
    let Some(ui) = ui.as_object_mut() else {
        return;
    };
    ui.insert("visibility".to_string(), json!(["app"]));
}

fn is_agent_continuation_app_tool_name(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "bind_agent_continuation"
            | "recover_agent_continuation_endpoint"
            | "get_agent_continuation_state"
            | "acquire_agent_continuation_wake"
            | "prepare_agent_continuation_wake"
            | "finish_agent_continuation_wake"
            | "unbind_agent_continuation"
            | "get_agent_wait_state"
    )
}

fn is_job_terminal_continuation_app_tool_name(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "bind_job_terminal_continuation"
            | "get_job_terminal_continuation_state"
            | "prepare_job_terminal_continuation"
            | "finish_job_terminal_continuation"
            | "unbind_job_terminal_continuation"
    )
}

fn is_host_continuation_app_tool_name(tool_name: &str) -> bool {
    is_agent_continuation_app_tool_name(tool_name)
        || is_job_terminal_continuation_app_tool_name(tool_name)
}

fn is_diagnostic_app_tool_name(tool_name: &str) -> bool {
    is_host_continuation_app_tool_name(tool_name) || tool_name == "sync_goal_plan"
}

const AGENT_CONTINUATION_APP_CALL_ID_FIELD: &str = "app_call_id";
const AGENT_CONTINUATION_APP_CALL_ID_PATTERN: &str = "^wc_app_call_[0-9a-f]{16}_[1-9][0-9]{0,5}$";

fn valid_agent_continuation_app_call_id(value: &str) -> bool {
    let Some(rest) = value.strip_prefix("wc_app_call_") else {
        return false;
    };
    let Some((view_prefix, sequence)) = rest.split_once('_') else {
        return false;
    };
    view_prefix.len() == 16
        && view_prefix
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        && !sequence.is_empty()
        && sequence.len() <= 6
        && !sequence.starts_with('0')
        && sequence.bytes().all(|byte| byte.is_ascii_digit())
}

pub(super) fn agent_continuation_app_call_id_from_params(params: &Value) -> Option<String> {
    let name = params.get("name").and_then(Value::as_str)?;
    if !is_diagnostic_app_tool_name(name) {
        return None;
    }
    let value = params
        .get("arguments")?
        .get(AGENT_CONTINUATION_APP_CALL_ID_FIELD)?
        .as_str()?;
    valid_agent_continuation_app_call_id(value).then(|| value.to_string())
}

fn attach_agent_continuation_app_diagnostic_schema(value: &mut Value) {
    let Some(properties) = value
        .pointer_mut("/inputSchema/properties")
        .and_then(Value::as_object_mut)
    else {
        return;
    };
    properties.insert(
        AGENT_CONTINUATION_APP_CALL_ID_FIELD.to_string(),
        json!({
            "type": "string",
            "pattern": AGENT_CONTINUATION_APP_CALL_ID_PATTERN,
            "description": "Optional App-generated diagnostic correlation id. Grants no authority and is stripped by the MCP adapter before ToolRuntime parsing."
        }),
    );
}

fn strip_agent_continuation_app_call_id(arguments: &mut Value) -> Result<Option<String>, String> {
    let Some(object) = arguments.as_object_mut() else {
        return Ok(None);
    };
    let Some(value) = object.remove(AGENT_CONTINUATION_APP_CALL_ID_FIELD) else {
        return Ok(None);
    };
    let Value::String(value) = value else {
        return Err(format!(
            "field '{AGENT_CONTINUATION_APP_CALL_ID_FIELD}' must be a canonical App diagnostic id"
        ));
    };
    if !valid_agent_continuation_app_call_id(&value) {
        return Err(format!(
            "field '{AGENT_CONTINUATION_APP_CALL_ID_FIELD}' must match {AGENT_CONTINUATION_APP_CALL_ID_PATTERN}"
        ));
    }
    Ok(Some(value))
}

fn attach_app_tool_content_fallback(result: &mut Value) {
    let Some(structured) = result.get("structuredContent") else {
        return;
    };
    let Ok(text) = serde_json::to_string(structured) else {
        return;
    };
    result["content"] = json!([{ "type": "text", "text": text }]);
}

fn work_result_input_selectors(arguments: &Value) -> Option<Value> {
    use webcodex_core::model_reference::{parse_model_reference, ModelReferenceKind};
    let mut selectors = serde_json::Map::new();
    for (field, kind) in [
        ("project", ModelReferenceKind::Project),
        ("session_id", ModelReferenceKind::Session),
    ] {
        if let Some(value) = arguments.get(field).and_then(Value::as_str) {
            let value = value.trim();
            if matches!(parse_model_reference(value, kind), Some(Ok(_))) {
                selectors.insert(field.to_string(), json!(value));
            }
        }
    }
    (!selectors.is_empty()).then_some(Value::Object(selectors))
}

fn attach_work_result_app_private_result(result: &mut Value, input_selectors: Option<Value>) {
    let Some(structured) = result.get("structuredContent").cloned() else {
        return;
    };
    let meta = result.as_object_mut().and_then(|result| {
        result
            .entry("_meta")
            .or_insert_with(|| json!({}))
            .as_object_mut()
    });
    if let Some(meta) = meta {
        if structured.get("success") == Some(&Value::Bool(true)) {
            if let Some(selectors) = input_selectors {
                meta.insert(WORK_RESULT_INPUT_META_KEY.to_string(), selectors);
            }
        }
        meta.insert(WORK_RESULT_APP_RESULT_META_KEY.to_string(), structured);
    }
}

#[cfg(test)]
mod work_result_input_tests {
    use super::*;

    #[test]
    fn work_result_input_projection_contains_only_valid_typed_selectors() {
        assert_eq!(
            work_result_input_selectors(&json!({
                "project": " ~p1 ", "session_id": "~s18446744073709551615", "instruction": "private"
            })),
            Some(json!({"project": "~p1", "session_id": "~s18446744073709551615"}))
        );
        for invalid in [
            "~p0",
            "~p01",
            "~p18446744073709551616",
            "~p-1",
            "~s1",
            "private",
        ] {
            assert!(work_result_input_selectors(&json!({"project": invalid})).is_none());
        }
        assert!(work_result_input_selectors(
            &json!({"project": "agent:r:p", "session_id": "wc_sess_1111111111111111"})
        )
        .is_none());
    }

    #[test]
    fn work_result_selector_binding_is_private_and_requires_success() {
        for success in [false, true] {
            let structured =
                json!({"success": success, "output": {"work_result": {"project": "agent:r:p"}}});
            let mut result = json!({"structuredContent": structured, "content": [{"type": "text", "text": "compact"}]});
            let public = result.clone();
            attach_work_result_app_private_result(&mut result, Some(json!({"project": "~p1"})));
            assert_eq!(result["structuredContent"], public["structuredContent"]);
            assert_eq!(result["content"], public["content"]);
            assert_eq!(result["_meta"][WORK_RESULT_APP_RESULT_META_KEY], structured);
            assert_eq!(
                result["_meta"][WORK_RESULT_INPUT_META_KEY],
                if success {
                    json!({"project": "~p1"})
                } else {
                    Value::Null
                }
            );
        }
    }
}

fn safe_continuation_dispatch_observation(value: Option<&Value>) -> &'static str {
    match value.and_then(Value::as_str) {
        Some("dispatch_prepared") => "dispatch_prepared",
        Some("dispatch_accepted") => "dispatch_accepted",
        Some("dispatch_unknown") => "dispatch_unknown",
        Some("continuation_consumed") => "continuation_consumed",
        _ => "-",
    }
}

fn log_agent_continuation_app_result(
    lifecycle: Option<&ToolRequestLifecycle>,
    tool_name: &str,
    app_call_id: Option<&str>,
    result: &Value,
) {
    if !crate::tool_request_trace::tool_request_trace_enabled() {
        return;
    }
    let Some(lifecycle) = lifecycle else {
        return;
    };
    let structured = result.get("structuredContent").and_then(Value::as_object);
    let tool_success = structured
        .and_then(|structured| structured.get("success"))
        .and_then(Value::as_bool);
    let output = structured
        .and_then(|structured| structured.get("output"))
        .and_then(Value::as_object);
    if let Some(output) = result.pointer("/structuredContent/output") {
        lifecycle.capture_continuation_ids(output);
    }
    let projection = output
        .and_then(|output| output.get("agent_continuation"))
        .and_then(Value::as_object)
        .or(output);
    let wake_present = projection
        .and_then(|projection| projection.get("wake"))
        .is_some_and(|wake| !wake.is_null())
        || output
            .and_then(|output| output.get("wake_id"))
            .is_some_and(Value::is_string);
    let queued_delivery_count = projection
        .and_then(|projection| projection.get("queued_delivery_count"))
        .and_then(Value::as_i64)
        .or_else(|| {
            projection
                .and_then(|projection| projection.get("wake"))
                .and_then(Value::as_object)
                .and_then(|wake| wake.get("queued_delivery_count"))
                .and_then(Value::as_i64)
        });
    let host_bound = projection
        .and_then(|projection| projection.get("host_binding"))
        .and_then(Value::as_object)
        .and_then(|host_binding| host_binding.get("bound"))
        .and_then(Value::as_bool);
    let dispatch_observation = safe_continuation_dispatch_observation(
        projection
            .and_then(|projection| projection.get("dispatch_observation"))
            .or_else(|| output.and_then(|output| output.get("dispatch_observation"))),
    );
    let content_blocks = result
        .get("content")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or(0);
    tracing::info!(
        event = "mcp_agent_continuation_app_result",
        server_trace_id = %lifecycle.correlation_trace_id(),
        app_call_id = app_call_id.unwrap_or("-"),
        tool_name,
        tool_success = tool_success.map(|value| if value { 1_i32 } else { 0_i32 }).unwrap_or(-1),
        structured_content_present = if structured.is_some() { 1_i32 } else { 0_i32 },
        content_blocks = content_blocks as i64,
        host_bound = host_bound.map(|value| if value { 1_i32 } else { 0_i32 }).unwrap_or(-1),
        wake_present = if wake_present { 1_i32 } else { 0_i32 },
        queued_delivery_count = queued_delivery_count.unwrap_or(-1),
        dispatch_observation,
        "mcp_agent_continuation_app_result"
    );
}

fn attach_job_terminal_resume_suggested_call_schema(
    tool_name: &str,
    app_enabled: bool,
    value: &mut Value,
) {
    if !app_enabled
        || tool_name != "wait_for_job_terminal"
        || !is_adaptive_runtime_direct_tool("present_job_terminal_continuation")
    {
        return;
    }
    let Some(properties) = value
        .pointer_mut("/outputSchema/properties/output/properties")
        .and_then(Value::as_object_mut)
    else {
        return;
    };
    properties.insert(
        "suggested_call".to_string(),
        json!({
            "type": "object",
            "description": "Host-specific parser-ready advisory call for the current MCP App continuation carrier. Present only for a still-waiting Job when this Host can create that carrier; it grants no authority and should be used only when no independent work remains and the current model turn can yield immediately after presentation.",
            "additionalProperties": false,
            "properties": {
                "follow_up_kind": {"type": "string", "const": "fallback_recovery"},
                "tool": {"type": "string", "const": "present_job_terminal_continuation"},
                "arguments": {
                    "type": "object",
                    "additionalProperties": false,
                    "properties": {
                        "wait_id": {
                            "type": "string",
                            "pattern": "^wc_job_wait_[A-Za-z0-9_-]{16}$"
                        }
                    },
                    "required": ["wait_id"]
                }
            },
            "required": ["follow_up_kind", "tool", "arguments"]
        }),
    );
}

pub(super) fn project_job_terminal_resume_suggested_call(
    carrier_available: bool,
    result: &mut ToolResult,
) {
    // MCP-added edges are not part of the canonical domain output schema.
    // Derive their availability from the same static descriptor policy, not
    // workflow guidance or an App capability alone.
    if !carrier_available
        || !result.success
        || !is_adaptive_runtime_direct_tool("present_job_terminal_continuation")
    {
        return;
    }
    let Some(output) = result.output.as_object_mut() else {
        return;
    };
    if output
        .get("automatic_resume_available")
        .and_then(Value::as_bool)
        != Some(false)
        || output.get("state").and_then(Value::as_str) != Some("waiting")
        || output.get("delivery_state").and_then(Value::as_str) != Some("not_ready")
    {
        return;
    }
    let Some(wait_id) = output
        .get("wait_id")
        .and_then(Value::as_str)
        .map(str::to_string)
    else {
        return;
    };
    output.insert(
        "suggested_call".to_string(),
        crate::tool_runtime::SuggestedToolCall::fallback_recovery(
            "present_job_terminal_continuation",
            json!({"wait_id": wait_id}),
        )
        .to_value(),
    );
}

fn project_mcp_runtime_status_input_schema(input_schema: &mut Value) {
    let Some(compact) = input_schema
        .pointer_mut("/properties/compact")
        .and_then(Value::as_object_mut)
    else {
        return;
    };
    compact.insert("default".to_string(), json!(true));
    compact.insert(
        "description".to_string(),
        json!("MCP defaults to sparse status. Set false for full diagnostics; summary_only=true still selects sparse."),
    );
}

fn project_mcp_model_default_input_schema(tool_name: &str, input_schema: &mut Value) {
    match tool_name {
        "get_runtime_status" => project_mcp_runtime_status_input_schema(input_schema),
        "observe_jobs" => {
            if let Some(summary) = input_schema
                .pointer_mut("/properties/summary_only")
                .and_then(Value::as_object_mut)
            {
                summary.insert("default".to_string(), json!(true));
                summary.insert("description".to_string(), json!("MCP defaults to compact proven-success validation logs. Set false to expand retained logs from the original cursor. Failures, unknown results and ordinary commands keep full evidence."));
            }
        }
        _ => {}
    }
}

fn project_mcp_model_manifest_defaults(result: &mut ToolResult) {
    if !result.success {
        return;
    }
    let tool_name = result.output["name"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    if let Some(input_schema) = result.output.get_mut("input_schema") {
        project_mcp_model_default_input_schema(&tool_name, input_schema);
    }
}

/// MCP-only omission defaults, after direct/gateway admission resolves the same
/// canonical tool identity. Explicit values and canonical HTTP defaults survive.
pub(super) fn project_mcp_model_argument_defaults(tool_name: &str, arguments: &mut Value) {
    let field = match tool_name {
        "get_runtime_status" => "compact",
        "observe_jobs" => "summary_only",
        _ => return,
    };
    if arguments.is_null() {
        *arguments = json!({});
    }
    if let Some(arguments) = arguments.as_object_mut() {
        arguments.entry(field).or_insert(json!(true));
    }
}

fn mcp_tool_descriptor_json(
    descriptor: ToolDescriptor,
    compact: bool,
    app_enabled: bool,
    stateless_output_projection: Option<bool>,
) -> Value {
    if !compact {
        let mut spec = descriptor.into_spec();
        if let Some(stateless) = stateless_output_projection {
            spec = project_mcp_tool_spec_output_schema(spec, stateless);
        }
        return mcp_tool_spec_json(spec, false, app_enabled);
    }
    let input_schema = descriptor.input_schema();
    let annotations = descriptor.annotations();
    mcp_tool_parts_json(
        descriptor.name,
        descriptor.description,
        input_schema,
        None,
        annotations,
        app_enabled,
    )
}

fn mcp_tool_spec_json(spec: ToolSpec, compact: bool, app_enabled: bool) -> Value {
    mcp_tool_parts_json(
        spec.name,
        spec.description,
        spec.input_schema,
        (!compact).then_some(spec.output_schema),
        spec.annotations,
        app_enabled,
    )
}

fn mcp_tool_parts_json(
    name: String,
    mut description: String,
    mut input_schema: Value,
    mut output_schema: Option<Value>,
    annotations: Value,
    app_enabled: bool,
) -> Value {
    let compact = output_schema.is_none();
    let tool_name = name.clone();
    project_mcp_model_default_input_schema(&tool_name, &mut input_schema);
    if tool_name == "get_runtime_status" {
        description
            .push_str(" MCP defaults to sparse status; compact=false opts into full diagnostics.");
    }

    if tool_name == "observe_jobs" {
        description.push_str(" MCP defaults summary_only=true; set false for full retained logs.");
    }

    if matches!(tool_name.as_str(), "observe_computer" | "observe_browser") {
        if let Some(schema) = output_schema.as_mut() {
            adapt_native_image_output_schema_for_mcp(schema);
        }
    }
    if tool_name == "read_project_artifact_chunk" {
        if let Some(properties) = input_schema["properties"].as_object_mut() {
            properties.insert(
                "as_image".to_string(),
                json!({
                    "type": "boolean",
                    "description": "MCP-only. When true, read one complete PNG, JPEG, or WebP up to 1 MiB and return it as native image content. Cannot be combined with offset or length."
                }),
            );
        }
        description.push_str(
            " Over MCP, set as_image=true to return one complete PNG, JPEG, or WebP as native image content; ordinary calls keep the existing chunked base64 response.",
        );
    }
    let mut value = json!({
        "name": name,
        "description": description,
        "inputSchema": input_schema,
        "annotations": annotations,
    });
    if let Some(schema) = output_schema {
        value["outputSchema"] = schema;
    }
    if !compact && tool_name == crate::ssh_resource_gateway::SSH_RESOURCE_TOOL_NAME {
        if let Some(object) = value.as_object_mut() {
            object.insert(
                "outputSchema".to_string(),
                crate::ssh_resource_gateway::mcp_output_schema(),
            );
        }
    }
    if tool_name == "import_host_files" {
        if let Some(required) =
            value.pointer_mut("/inputSchema/properties/openaiFileIdRefs/items/required")
        {
            *required = json!(["download_url", "file_id"]);
        }
        if let Some(meta) = tool_meta_object(&mut value) {
            meta.insert("openai/fileParams".to_string(), json!(["openaiFileIdRefs"]));
        }
    }
    if app_enabled {
        if let Some(app) = app_registry::for_tool(&tool_name) {
            attach_app_metadata(&mut value, app.uri);
            app.add_tool_metadata(&mut value);
        }
    }
    attach_job_terminal_resume_suggested_call_schema(&tool_name, app_enabled, &mut value);
    if compact {
        super::discovery::compact_tool(&mut value);
    }
    value
}

#[cfg(test)]
pub(super) fn mcp_runtime_tool_result(
    tool_name: &str,
    as_image_requested: bool,
    result: ToolResult,
) -> Value {
    let result_presentation = McpToolResultPresentation::Standard;
    let artifact_presentation = if as_image_requested {
        resources::ProjectArtifactPresentationMode::Image
    } else {
        resources::ProjectArtifactPresentationMode::None
    };
    match resources::adapt_tool_result(
        tool_name,
        artifact_presentation,
        result,
        resources::McpResourceToolCallContext::default(),
        false,
        result_presentation,
    ) {
        resources::McpResourceToolResultAdaptation::Framed(value) => value,
        resources::McpResourceToolResultAdaptation::Unhandled(result) => {
            mcp_runtime_tool_result_fallback(result, false, result_presentation)
        }
    }
}

pub(super) async fn handle_list_with_runtime(
    runtime: &ToolRuntime,
    id: Option<Value>,
    auth: Option<&AuthContext>,
    stateless_2026: bool,
    compact_schemas: bool,
    app_enabled: bool,
) -> McpOutcome {
    handle_list_inner(
        Some(runtime),
        id,
        auth,
        stateless_2026,
        compact_schemas,
        app_enabled,
    )
    .await
}

#[cfg(test)]
pub(super) async fn handle_list(
    id: Option<Value>,
    auth: Option<&AuthContext>,
    stateless_2026: bool,
    compact_schemas: bool,
    app_enabled: bool,
) -> McpOutcome {
    handle_list_inner(None, id, auth, stateless_2026, compact_schemas, app_enabled).await
}

async fn handle_list_inner(
    runtime: Option<&ToolRuntime>,
    id: Option<Value>,
    auth: Option<&AuthContext>,
    stateless_2026: bool,
    compact_schemas: bool,
    app_enabled: bool,
) -> McpOutcome {
    let mut result = mcp_tools_list_payload_with_features_for_auth(
        compact_schemas,
        app_enabled,
        stateless_2026,
        auth,
    );
    if let Some(runtime) = runtime.filter(|_| app_enabled) {
        let settings = runtime.workbench_settings(auth);
        if let Some(title) = settings.output.get("display_name").and_then(Value::as_str) {
            if let Some(tools) = result.get_mut("tools").and_then(Value::as_array_mut) {
                if let Some(tool) = tools
                    .iter_mut()
                    .find(|tool| tool["name"] == "open_webcodex_workbench")
                {
                    tool["title"] = json!(title);
                }
            }
        }
    }
    if let Some(tools) = result.get_mut("tools").and_then(Value::as_array_mut) {
        tools.push(adaptive_runtime_gateway_tool_json(compact_schemas));
    }
    if stateless_2026 {
        add_stateless_workflow_recorder_metadata(&mut result);
    }
    if crate::mcp_gateway::authorized(auth) {
        if let Some(tools) = result.get_mut("tools").and_then(Value::as_array_mut) {
            let mut spec = crate::mcp_gateway::tool_spec();
            if stateless_2026 {
                if let Some(properties) = spec
                    .pointer_mut("/inputSchema/properties")
                    .and_then(Value::as_object_mut)
                {
                    properties.insert(
                        MCP_INVOCATION_ENVELOPE_FIELD.to_string(),
                        stateless_invocation_envelope_schema(crate::mcp_gateway::MCP_TOOL_NAME),
                    );
                }
            }
            tools.push(spec);
        }
    }
    if compact_schemas {
        // Include adapter-added gateway and Session/context wrapper descriptions.
        // Apply after overlays so none of their repeated full copy leaks into L1.
        if let Some(tools) = result.get_mut("tools").and_then(Value::as_array_mut) {
            for tool in tools {
                super::discovery::compact_tool(tool);
            }
        }
    }
    McpOutcome::Ok(rpc_result(
        id,
        if stateless_2026 {
            mcp_stateless_result(result, true)
        } else {
            result
        },
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HostFileImportTrustReason {
    Trusted,
    TrustedLoopbackApiToken,
    TrustedLoopbackBootstrap,
    LoopbackApiTokenTrustRequiresLoopback,
    LoopbackBootstrapTrustRequiresLoopback,
    MissingConfig,
    MissingDatabase,
    MissingAuth,
    NotOAuthToken,
    MissingAllowedClientId,
    OAuthDisabled,
    AuthenticatedOAuthOpenAiHostOnly,
    ClientRegistrationMissingOrRevoked,
    ClientRegistrationLookupFailed,
}

impl HostFileImportTrustReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::Trusted => "trusted",
            Self::TrustedLoopbackApiToken => "trusted_loopback_api_token",
            Self::TrustedLoopbackBootstrap => "trusted_loopback_bootstrap",
            Self::LoopbackApiTokenTrustRequiresLoopback => {
                "loopback_api_token_trust_requires_loopback"
            }
            Self::LoopbackBootstrapTrustRequiresLoopback => {
                "loopback_bootstrap_trust_requires_loopback"
            }
            Self::MissingConfig => "missing_config",
            Self::MissingDatabase => "missing_database",
            Self::MissingAuth => "missing_auth",
            Self::NotOAuthToken => "not_oauth_token",
            Self::MissingAllowedClientId => "missing_allowed_client_id",
            Self::OAuthDisabled => "oauth_disabled",
            Self::AuthenticatedOAuthOpenAiHostOnly => "authenticated_oauth_openai_host_only",
            Self::ClientRegistrationMissingOrRevoked => "client_registration_missing_or_revoked",
            Self::ClientRegistrationLookupFailed => "client_registration_lookup_failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct HostFileImportTrustDecision {
    pub(super) trust: HostFileImportTrust,
    pub(super) reason: HostFileImportTrustReason,
    config_present: bool,
    database_present: bool,
    oauth_enabled: bool,
    configured_trusted_client_count: usize,
    pub(super) client_id_configured: Option<bool>,
    pub(super) active_client_registration_found: Option<bool>,
}

#[cfg(test)]
static LAST_MCP_HOST_FILE_IMPORT_TRUST_DECISION: std::sync::OnceLock<
    std::sync::Mutex<Option<HostFileImportTrustDecision>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
pub(super) fn take_last_mcp_host_file_import_trust_decision() -> Option<HostFileImportTrustDecision>
{
    LAST_MCP_HOST_FILE_IMPORT_TRUST_DECISION
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .unwrap()
        .take()
}

impl HostFileImportTrustDecision {
    fn unavailable(reason: HostFileImportTrustReason) -> Self {
        Self {
            trust: HostFileImportTrust::Untrusted,
            reason,
            config_present: false,
            database_present: false,
            oauth_enabled: false,
            configured_trusted_client_count: 0,
            client_id_configured: None,
            active_client_registration_found: None,
        }
    }

    fn from_config(reason: HostFileImportTrustReason, config: &crate::Config) -> Self {
        Self {
            trust: HostFileImportTrust::Untrusted,
            reason,
            config_present: true,
            database_present: false,
            oauth_enabled: config.oauth2.enabled,
            configured_trusted_client_count: config.oauth2.trusted_mcp_file_client_ids.len(),
            client_id_configured: None,
            active_client_registration_found: None,
        }
    }
}

pub(super) fn mcp_host_file_import_trust_decision_from_state(
    config: &crate::Config,
    db: &crate::Database,
    auth: Option<&AuthContext>,
) -> HostFileImportTrustDecision {
    let base = HostFileImportTrustDecision {
        trust: HostFileImportTrust::Untrusted,
        reason: HostFileImportTrustReason::MissingAuth,
        config_present: true,
        database_present: true,
        oauth_enabled: config.oauth2.enabled,
        configured_trusted_client_count: config.oauth2.trusted_mcp_file_client_ids.len(),
        client_id_configured: None,
        active_client_registration_found: None,
    };
    let Some(auth) = auth else {
        return base;
    };
    if config.oauth2.trust_loopback_api_token_mcp_file_import {
        let loopback_reasons = if auth.kind == crate::auth::AuthKind::ApiToken
            && auth.token_kind.as_deref() == Some("user")
        {
            Some((
                HostFileImportTrustReason::TrustedLoopbackApiToken,
                HostFileImportTrustReason::LoopbackApiTokenTrustRequiresLoopback,
            ))
        } else if auth.kind == crate::auth::AuthKind::Bootstrap
            && auth.is_bootstrap()
            && config
                .token
                .as_deref()
                .is_some_and(|token| !token.trim().is_empty())
        {
            Some((
                HostFileImportTrustReason::TrustedLoopbackBootstrap,
                HostFileImportTrustReason::LoopbackBootstrapTrustRequiresLoopback,
            ))
        } else {
            None
        };
        if let Some((trusted_reason, non_loopback_reason)) = loopback_reasons {
            if config.is_loopback_bound() {
                return HostFileImportTrustDecision {
                    trust: HostFileImportTrust::LoopbackTrustedMcpHostFile,
                    reason: trusted_reason,
                    ..base
                };
            }
            return HostFileImportTrustDecision {
                reason: non_loopback_reason,
                ..base
            };
        }
    }
    if !auth.is_oauth_token() {
        return HostFileImportTrustDecision {
            reason: HostFileImportTrustReason::NotOAuthToken,
            ..base
        };
    }
    let Some(client_id) = auth
        .allowed_client_id
        .as_deref()
        .map(str::trim)
        .filter(|client_id| !client_id.is_empty())
    else {
        return HostFileImportTrustDecision {
            reason: HostFileImportTrustReason::MissingAllowedClientId,
            ..base
        };
    };
    if !config.oauth2.enabled {
        return HostFileImportTrustDecision {
            reason: HostFileImportTrustReason::OAuthDisabled,
            ..base
        };
    }
    let client_id_configured = config
        .oauth2
        .trusted_mcp_file_client_ids
        .iter()
        .any(|trusted_client_id| trusted_client_id == client_id);
    match db.get_oauth_client_by_client_id(client_id) {
        Ok(Some(client)) if client.client_id == client_id => {
            if client_id_configured {
                HostFileImportTrustDecision {
                    trust: HostFileImportTrust::TrustedMcpHostFile,
                    reason: HostFileImportTrustReason::Trusted,
                    client_id_configured: Some(true),
                    active_client_registration_found: Some(true),
                    ..base
                }
            } else {
                HostFileImportTrustDecision {
                    trust: HostFileImportTrust::AuthenticatedMcpOpenAiHostFile,
                    reason: HostFileImportTrustReason::AuthenticatedOAuthOpenAiHostOnly,
                    client_id_configured: Some(false),
                    active_client_registration_found: Some(true),
                    ..base
                }
            }
        }
        Ok(_) => HostFileImportTrustDecision {
            reason: HostFileImportTrustReason::ClientRegistrationMissingOrRevoked,
            client_id_configured: Some(client_id_configured),
            active_client_registration_found: Some(false),
            ..base
        },
        Err(_) => HostFileImportTrustDecision {
            reason: HostFileImportTrustReason::ClientRegistrationLookupFailed,
            client_id_configured: Some(client_id_configured),
            active_client_registration_found: None,
            ..base
        },
    }
}

#[cfg(test)]
pub(super) fn mcp_host_file_import_trust_from_state(
    config: &crate::Config,
    db: &crate::Database,
    auth: Option<&AuthContext>,
) -> HostFileImportTrust {
    mcp_host_file_import_trust_decision_from_state(config, db, auth).trust
}

pub(super) fn host_file_import_trust_for_call(
    tool_name: Option<&str>,
    auth: Option<&AuthContext>,
    config: Option<&crate::Config>,
    db: Option<&crate::Database>,
) -> HostFileImportTrust {
    if tool_name != Some("import_host_files") {
        return HostFileImportTrust::Untrusted;
    }
    let decision = match config {
        None => HostFileImportTrustDecision::unavailable(HostFileImportTrustReason::MissingConfig),
        Some(config) => match db {
            None => HostFileImportTrustDecision::from_config(
                HostFileImportTrustReason::MissingDatabase,
                config,
            ),
            Some(db) => mcp_host_file_import_trust_decision_from_state(config, db, auth),
        },
    };
    log_mcp_host_file_import_trust_decision(auth, &decision);
    decision.trust
}

fn mcp_auth_kind_classification(auth: Option<&AuthContext>) -> &'static str {
    match auth.map(|auth| auth.kind) {
        None => "none",
        Some(crate::auth::AuthKind::OAuth2Token) => "oauth2",
        Some(crate::auth::AuthKind::ApiToken) => "api_token",
        Some(crate::auth::AuthKind::Bootstrap) => "bootstrap",
        Some(crate::auth::AuthKind::AgentToken) => "agent_token",
        Some(crate::auth::AuthKind::AccountCredential) => "account_credential",
        Some(crate::auth::AuthKind::SharedKey) => "shared_key",
        Some(crate::auth::AuthKind::ProjectCredential) => "project_credential",
        Some(crate::auth::AuthKind::OpenAnonymous) => "open_anonymous",
    }
}

fn mcp_token_kind_classification(auth: Option<&AuthContext>) -> &'static str {
    match auth.and_then(|auth| auth.token_kind.as_deref()) {
        None => "none",
        Some("oauth2") => "oauth2",
        Some("oauth2_shared_key") => "oauth2_shared_key",
        Some("oauth2_project") => "oauth2_project",
        Some("user") => "user",
        Some("agent") => "agent",
        Some(_) => "other",
    }
}

fn log_mcp_host_file_import_trust_decision(
    auth: Option<&AuthContext>,
    decision: &HostFileImportTrustDecision,
) {
    #[cfg(test)]
    {
        *LAST_MCP_HOST_FILE_IMPORT_TRUST_DECISION
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .unwrap() = Some(*decision);
    }
    let allowed_client_id_present = auth
        .and_then(|auth| auth.allowed_client_id.as_deref())
        .is_some_and(|client_id| !client_id.trim().is_empty());
    tracing::info!(
        target: "webcodex::mcp",
        trust = decision.trust.is_trusted(),
        reason = decision.reason.as_str(),
        auth_kind = mcp_auth_kind_classification(auth),
        token_kind = mcp_token_kind_classification(auth),
        allowed_client_id_present,
        config_present = decision.config_present,
        database_present = decision.database_present,
        oauth_enabled = decision.oauth_enabled,
        configured_trusted_client_count = decision.configured_trusted_client_count,
        client_id_configured = ?decision.client_id_configured,
        active_client_registration_found = ?decision.active_client_registration_found,
        "mcp_host_file_import_trust_decision"
    );
}

fn canonicalize_recording_session_id(
    runtime: &crate::tool_runtime::ToolRuntime,
    raw: Option<String>,
    auth: Option<&crate::auth::AuthContext>,
) -> Result<Option<String>, String> {
    raw.map(|raw| {
        runtime
            .canonicalize_explicit_session_selector(&raw, auth)
            .map_err(|err| err.to_string())
    })
    .transpose()
}

#[cfg(test)]
mod adaptive_runtime_route_guidance_tests {
    use super::*;

    #[test]
    fn unknown_gateway_target_points_at_query_discovery_not_exact_name() {
        let result = adaptive_runtime_gateway_unknown_target("definitely_unknown_tool");
        assert!(!result.success);
        let output = &result.output;
        assert_eq!(output["error_kind"], "unknown_tool");
        assert_eq!(output["execution_state"], "not_started");
        assert_eq!(output["state_changed"], false);
        assert_eq!(output["discovery_tool"], "read_tool_manifest");
        let suggested = &output["suggested_call"];
        assert_eq!(suggested["tool"], ADAPTIVE_RUNTIME_GATEWAY_TOOL_NAME);
        assert_eq!(suggested["arguments"]["tool"], "read_tool_manifest");
        assert_eq!(
            suggested["arguments"]["arguments"]["query"],
            "definitely_unknown_tool"
        );
        assert!(suggested["arguments"]["arguments"]
            .get("tool_name")
            .is_none());
        assert!(result
            .error
            .as_deref()
            .unwrap()
            .contains("read_tool_manifest"));
        assert!(result.error.as_deref().unwrap().contains("query"));
    }

    #[test]
    fn direct_route_denial_classifies_gateway_scope_and_surface_cases() {
        let gateway = mcp_adaptive_runtime_direct_route_denial("apply_patch", None, true);
        assert_eq!(
            gateway.kind,
            McpAdaptiveRuntimeDirectRouteDenialKind::GatewayRouteRequired
        );
        assert_eq!(gateway.diagnostics["error_kind"], "gateway_route_required");
        assert_eq!(
            gateway.diagnostics["suggested_call"]["arguments"]["tool"],
            "read_tool_manifest"
        );
        assert_eq!(
            gateway.diagnostics["suggested_call"]["arguments"]["arguments"]["tool_name"],
            "apply_patch"
        );
        assert!(gateway.message.contains("call_runtime_tool"));
        assert!(gateway.message.contains("tool='read_tool_manifest'"));
        assert!(gateway.message.contains("tool_name='apply_patch'"));
        assert!(!gateway
            .message
            .contains("directly or via call_runtime_tool"));

        let mut restricted = AuthContext::new(crate::auth::AuthKind::OAuth2Token);
        restricted.scopes = vec!["runtime:read".to_string()];
        let scope =
            mcp_adaptive_runtime_direct_route_denial("list_projects", Some(&restricted), true);
        assert_eq!(
            scope.kind,
            McpAdaptiveRuntimeDirectRouteDenialKind::InsufficientScope
        );
        assert_eq!(scope.diagnostics["error_kind"], "insufficient_scope");
        assert!(scope.diagnostics.get("suggested_call").is_none());
        assert!(scope.message.contains("authorization"));

        let hidden = mcp_adaptive_runtime_direct_route_denial("list_skills", None, false);
        assert_eq!(
            hidden.kind,
            McpAdaptiveRuntimeDirectRouteDenialKind::NotOnSurface
        );
        assert_eq!(hidden.diagnostics["error_kind"], "direct_route_denied");
        assert_eq!(hidden.diagnostics["discovery_tool"], "read_tool_manifest");
        assert!(!hidden
            .message
            .contains("call_runtime_tool with tool='list_skills'"));
        let suggested = &hidden.diagnostics["suggested_call"];
        assert_eq!(suggested["tool"], ADAPTIVE_RUNTIME_GATEWAY_TOOL_NAME);
        assert_eq!(suggested["arguments"]["tool"], "read_tool_manifest");
        assert_eq!(suggested["arguments"]["arguments"]["query"], "list_skills");
        assert_ne!(suggested["arguments"]["tool"].as_str(), Some("list_skills"));
    }
}
