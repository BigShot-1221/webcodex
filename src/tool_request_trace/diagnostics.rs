//! Bounded operator diagnostics, deliberately separate from safe business audit.
//! Never clone/serialize a full payload to produce its preview. Selection is
//! diagnostic policy only: it does not admit tools or change their execution.
use serde_json::{json, Map, Value};

pub(super) const MAX_DIAGNOSTIC_BYTES: usize = 8 * 1024;
const MAX_TEXT_BYTES: usize = 768;
const MAX_ITEMS: usize = 8;
const MAX_NODES: usize = 128;

pub(super) fn selected(tool: &str) -> bool {
    matches!(
        tool,
        "work_on_project"
            | "read_files"
            | "search_project_texts"
            | "search_file_context"
            | "run_process"
            | "write_job_input"
            | "run_shell"
            | "run_script"
            | "run_detached_process"
            | "edit_project_files"
            | "apply_text_edits"
            | "apply_patch"
            | "write_project_file"
            | "cargo_check"
            | "cargo_test"
            | "cargo_fmt"
            | "project_validate"
            | "observe_jobs"
            | "wait_for_job_readiness"
            | "wait_for_job_terminal"
            | "commit_git_paths"
            | "restore_git_paths"
            | "review_changes"
            | "list_projects"
            | "list_project_files"
            | "list_project_tracked_files"
            | "start_coding_agent"
            | "observe_coding_agent"
            | "cancel_coding_agent"
            | "respond_coding_agent_permission"
    )
}

fn prefix(text: &str, limit: usize) -> &str {
    let mut end = text.len().min(limit);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

#[derive(Default)]
struct Budget {
    bytes: usize,
    nodes: usize,
    truncated: bool,
    omitted_bodies: usize,
}
impl Budget {
    fn copy(&mut self, value: &Value, depth: usize, field: &str) -> Value {
        self.nodes += 1;
        if depth > 6 || self.nodes > MAX_NODES || self.bytes >= 6000 {
            self.truncated = true;
            return Value::Null;
        }
        let key = prefix(field, 64).to_ascii_lowercase();
        if key.contains("token")
            || key.contains("password")
            || key.contains("secret")
            || matches!(
                key.as_str(),
                "authorization"
                    | "private_key"
                    | "headers"
                    | "env"
                    | "environment"
                    | "binding_id"
                    | "activation_key"
                    | "idempotency_key"
            )
        {
            self.bytes += 32;
            return json!({"omitted": "credential_field"});
        }
        if matches!(
            key.as_str(),
            "content"
                | "text"
                | "script"
                | "stdin"
                | "stdout"
                | "stderr"
                | "stdout_tail"
                | "stderr_tail"
                | "preview"
                | "old_text"
                | "new_text"
                | "patch"
                | "diff"
                | "image"
                | "base64"
                | "data"
                | "instruction"
                | "message"
                | "resolution"
        ) {
            self.omitted_bodies += 1;
            self.bytes += 64;
            return match value.as_str() {
                Some(body) => json!({"bytes": body.len(), "omitted": "body"}),
                None => json!({"omitted": "body"}),
            };
        }
        match value {
            Value::String(text) => {
                let text_prefix = prefix(
                    text,
                    MAX_TEXT_BYTES.min(6000_usize.saturating_sub(self.bytes) / 6),
                );
                self.bytes += serde_json::to_vec(text_prefix)
                    .expect("string serialization")
                    .len();
                if text_prefix.len() < text.len() {
                    self.truncated = true;
                    json!({"preview": text_prefix, "bytes": text.len(), "truncated": true})
                } else {
                    Value::String(text_prefix.to_owned())
                }
            }
            Value::Array(values) => {
                let mut items = Vec::new();
                for item in values.iter().take(MAX_ITEMS) {
                    if self.nodes >= MAX_NODES || self.bytes >= 6000 {
                        break;
                    }
                    items.push(self.copy(item, depth + 1, ""));
                }
                self.bytes += 2 + items.len();
                if items.len() != values.len() {
                    self.truncated = true;
                    json!({"items": items, "total_count": values.len(), "truncated": true})
                } else {
                    Value::Array(items)
                }
            }
            Value::Object(values) => {
                let mut object = Map::new();
                for (key, item) in values.iter().take(32) {
                    if depth == 0 && key == "_wc" {
                        continue;
                    } // Separately retained once as invocation metadata.
                    if self.nodes >= MAX_NODES || self.bytes >= 6000 {
                        break;
                    }
                    let name = prefix(key, 64);
                    self.bytes += name.len() * 2 + 4;
                    if name.len() != key.len() {
                        self.truncated = true;
                    }
                    object.insert(name.to_owned(), self.copy(item, depth + 1, key));
                }
                let expected = values.len() - usize::from(depth == 0 && values.contains_key("_wc"));
                if object.len() != expected {
                    self.truncated = true;
                }
                Value::Object(object)
            }
            _ => {
                self.bytes += 24;
                value.clone()
            }
        }
    }

    fn finish(self, value: Value) -> Value {
        let result = json!({"version": 1, "value": value, "truncated": self.truncated,
            "omitted_bodies": self.omitted_bodies, "max_bytes": MAX_DIAGNOSTIC_BYTES});
        // The traversal bound limits allocation/work before this final byte check.
        // No oversized original body is serialized merely to compute its size.
        if crate::json_measurement::serialized_json_len(&result).unwrap_or(usize::MAX)
            <= MAX_DIAGNOSTIC_BYTES
        {
            result
        } else {
            json!({"version": 1, "truncated": true, "reason": "diagnostic_budget_exceeded", "max_bytes": MAX_DIAGNOSTIC_BYTES})
        }
    }
}

pub(super) fn request(entry_tool: &str, arguments: &Value) -> Option<Value> {
    let (tool, args) = if entry_tool == "call_runtime_tool" {
        (
            arguments.get("tool").and_then(Value::as_str)?,
            arguments.get("arguments").unwrap_or(&Value::Null),
        )
    } else {
        (entry_tool, arguments)
    };
    if super::tool_suppresses_payload_capture(Some(tool)) {
        return None;
    }
    if !selected(tool) && arguments.get("_wc").is_none() {
        return None;
    }
    let mut budget = Budget::default();
    let invocation = arguments.get("_wc").map(|wc| budget.copy(wc, 0, ""));
    let nested_invocation = (entry_tool == "call_runtime_tool")
        .then(|| args.get("_wc"))
        .flatten()
        .map(|wc| budget.copy(wc, 0, ""));
    let args = if selected(tool) {
        budget.copy(args, 0, "")
    } else {
        json!({"omitted":"tool_not_selected"})
    };
    Some(budget.finish(json!({"entry_tool": prefix(entry_tool,128), "tool": prefix(tool,128), "arguments": args, "invocation": invocation, "nested_invocation":nested_invocation})))
}

pub(super) fn arguments(tool: &str, arguments: &Value) -> Option<Value> {
    if !selected(tool) {
        return None;
    }
    let mut budget = Budget::default();
    let args = budget.copy(arguments, 0, "");
    Some(budget.finish(json!({"tool": tool, "arguments": args})))
}

fn context_receipt(output: &Value) -> Value {
    let Some(projection) = output.get("context_projection") else {
        return Value::Null;
    };
    let materials = projection.get("materials").and_then(Value::as_array);
    let receipts = materials.into_iter().flatten().take(MAX_ITEMS).map(|material| {
        let view = &material["projection"];
        let sources = view.get("sources").and_then(Value::as_array);
        json!({
            "key": material.get("key"), "status": material.get("status"),
            "reason_code": material.get("reason_code"), "observation_status": view.get("status"),
            "content_included": view.get("content_included"), "truncated": view.get("truncated"),
            "total_chars": view.get("total_chars"),
            "sources": sources.into_iter().flatten().take(MAX_ITEMS).map(|source| json!({
                "path": source.get("path"), "source_scope": source.get("source_scope"),
                "fingerprint": source.get("fingerprint"), "truncated": source.get("truncated"),
                "returned_bytes": source.get("content").and_then(Value::as_str).map(str::len),
                "read_more": source.get("read_more"),
            })).collect::<Vec<_>>()
        })
    }).collect::<Vec<_>>();
    json!({"truncated": projection.get("truncated"), "materials": receipts,
        "evidence_boundary": "included_in_server_response_not_proof_of_client_receipt_or_model_reading"})
}

const CODING_AGENT_EVENT_KINDS: &[&str] = &[
    "agent_message",
    "reasoning",
    "plan",
    "tool_activity",
    "file_change",
    "terminal_activity",
    "usage",
    "permission_request",
    "terminal",
];

fn summarize_coding_agent_events(budget: &mut Budget, value: &Value) -> Value {
    let Some(events) = value.as_array() else {
        return Value::Null;
    };
    let count = events.len();
    let mut items = Vec::new();
    for event in events.iter().take(MAX_ITEMS) {
        budget.nodes += 1;
        if budget.nodes > MAX_NODES || budget.bytes >= 6000 {
            budget.truncated = true;
            break;
        }
        let sequence = event.get("sequence").cloned().unwrap_or(Value::Null);
        let kind = event
            .get("kind")
            .and_then(Value::as_str)
            .filter(|kind| CODING_AGENT_EVENT_KINDS.contains(kind))
            .map(|kind| Value::String(kind.to_owned()))
            .unwrap_or(Value::Null);
        budget.bytes += 48;
        items.push(json!({"sequence": sequence, "kind": kind}));
    }
    if items.len() != count {
        budget.truncated = true;
    }
    json!({
        "count": count,
        "items": items,
        "truncated": items.len() != count,
    })
}

pub(super) fn result(tool: &str, response: &Value) -> Option<Value> {
    if super::tool_suppresses_payload_capture(Some(tool)) {
        return None;
    }
    let protocol = response.get("result").unwrap_or(response);
    let result = protocol.get("structuredContent").unwrap_or(protocol);
    let output = result.get("output").unwrap_or(result);
    if !selected(tool) && output.get("context_projection").is_none() {
        return None;
    }
    let mut budget = Budget::default();
    let mut fields = Map::new();
    // Keep the actual final material receipts before optional result facts.
    let receipt = context_receipt(output);
    if !receipt.is_null() {
        fields.insert("context".into(), budget.copy(&receipt, 0, ""));
    }
    for name in [
        "resolved_project",
        "project",
        "project_resolution",
        "session_id",
        "workspace",
        "instructions",
        "semantic_navigation",
        "input_normalization",
        "run_id",
        "state",
        "provider_id",
        "execution_state",
        "command_execution_state",
        "command_started",
        "command_completed",
        "effective_timeout_secs",
        "sync_wait_secs",
        "cwd",
        "shell",
        "state_changed",
        "exit_code",
        "job_id",
        "failure_kind",
        "error_kind",
        "recovery_kind",
        "reason_code",
        "passed",
        "tests_run_count",
        "source_state",
        "count",
        "matched_count",
        "returned",
        "total_entries",
        "offset",
        "next_offset",
        "requested_count",
        "returned_count",
        "failed_count",
        "truncated",
        "output_truncated",
        "truncation_reason",
        "items",
        "terminal",
        "has_more",
        "history_lost",
        "first_retained_sequence",
        "events",
        "cancel_requested",
        "applied",
    ] {
        if let Some(value) = output.get(name) {
            let diagnostic = if name == "events" && omits_provider_error_message(tool) {
                summarize_coding_agent_events(&mut budget, value)
            } else {
                budget.copy(value, 0, name)
            };
            fields.insert(name.into(), diagnostic);
        }
    }
    Some(budget.finish(json!({"tool": prefix(tool,128), "success": result.get("success"),
        "is_error": protocol.get("isError"), "error": budgeted_error(tool, response, result), "output": fields})))
}

fn omits_provider_error_message(tool: &str) -> bool {
    matches!(
        tool,
        "start_coding_agent"
            | "observe_coding_agent"
            | "cancel_coding_agent"
            | "respond_coding_agent_permission"
    )
}

fn budgeted_error(tool: &str, response: &Value, result: &Value) -> Value {
    let error = result.get("error").or_else(|| response.get("error"));
    let omit_message = omits_provider_error_message(tool);
    match error {
        Some(Value::String(_)) if omit_message => json!({"omitted": "provider_error_message"}),
        Some(Value::String(text)) => Value::String(prefix(text, 256).to_owned()),
        Some(value) if value.is_object() => {
            if omit_message {
                let mut object = Map::new();
                if let Some(code) = value.get("code") {
                    object.insert("code".into(), code.clone());
                }
                if value.get("message").is_some() {
                    object.insert(
                        "message".into(),
                        json!({"omitted": "provider_error_message"}),
                    );
                }
                Value::Object(object)
            } else {
                json!({"code": value.get("code"),
                    "message": value.get("message").and_then(Value::as_str).map(|s| prefix(s, 256))})
            }
        }
        _ => Value::Null,
    }
}

#[cfg(test)]
pub(crate) mod test_api {
    use super::Value;

    pub(crate) fn request(entry_tool: &str, arguments: &Value) -> Option<Value> {
        super::request(entry_tool, arguments)
    }

    pub(crate) fn result(tool: &str, response: &Value) -> Option<Value> {
        super::result(tool, response)
    }
}

#[cfg(test)]
mod tests;
