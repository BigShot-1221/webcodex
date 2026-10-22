//! In-run ACP permission callback coordination (wait, respond, fail-closed).

use super::protocol::bounded_text;
use super::*;
use agent_client_protocol_schema::v1::RequestPermissionRequest;
use serde_json::Value;
use uuid::Uuid;
use webcodex_core::coding_agent::{
    validate_pending_permission, CodingAgentPendingPermission, CodingAgentPermissionOptionSummary,
    CODING_AGENT_MAX_PERMISSION_OPTIONS, CODING_AGENT_PERMISSION_REQUEST_TOKEN_PREFIX,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PermissionWaitOutcome {
    Selected(String),
    Cancelled,
}

#[derive(Debug)]
struct ActivePermission {
    pending: CodingAgentPendingPermission,
    rpc_id: u64,
    deadline: Instant,
    decision: Option<PermissionWaitOutcome>,
}

#[derive(Debug)]
pub(super) struct PermissionSlot {
    generation: std::sync::atomic::AtomicU64,
    active: Mutex<Option<ActivePermission>>,
    changed: Condvar,
}

impl Default for PermissionSlot {
    fn default() -> Self {
        Self {
            generation: std::sync::atomic::AtomicU64::new(0),
            active: Mutex::new(None),
            changed: Condvar::new(),
        }
    }
}

impl PermissionSlot {
    pub(super) fn open_from_acp(
        &self,
        params: &Value,
        rpc_id: u64,
        session_id: &str,
        deadline: Instant,
    ) -> Result<CodingAgentPendingPermission, String> {
        let _request: RequestPermissionRequest = serde_json::from_value(params.clone())
            .map_err(|_| "ACP permission params are invalid".to_string())?;
        let declared_session = params
            .get("sessionId")
            .and_then(Value::as_str)
            .ok_or_else(|| "ACP permission session identity is missing".to_string())?;
        if declared_session != session_id {
            return Err("ACP permission session identity mismatch".to_string());
        }
        let tool_call_id = params
            .pointer("/toolCall/toolCallId")
            .and_then(Value::as_str)
            .map(bounded_text)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| "ACP permission tool_call_id is invalid".to_string())?;
        let title = params
            .pointer("/toolCall/title")
            .and_then(Value::as_str)
            .map(bounded_text)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| "ACP permission title is invalid".to_string())?;
        let mut options = Vec::new();
        let option_values = params
            .get("options")
            .and_then(Value::as_array)
            .ok_or_else(|| "ACP permission options are invalid".to_string())?;
        for option in option_values.iter().take(CODING_AGENT_MAX_PERMISSION_OPTIONS + 1) {
            let option_id = option
                .get("optionId")
                .and_then(Value::as_str)
                .map(bounded_text)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| "ACP permission option is invalid".to_string())?;
            let name = option
                .get("name")
                .and_then(Value::as_str)
                .map(bounded_text)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| "ACP permission option is invalid".to_string())?;
            let kind = option
                .get("kind")
                .and_then(Value::as_str)
                .map(bounded_text)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| "ACP permission option is invalid".to_string())?;
            if option_id.is_empty() || name.is_empty() || kind.is_empty() {
                return Err("ACP permission option is invalid".to_string());
            }
            options.push(CodingAgentPermissionOptionSummary {
                option_id,
                name,
                kind,
            });
        }
        if options.is_empty() || options.len() > CODING_AGENT_MAX_PERMISSION_OPTIONS {
            return Err("ACP permission options are invalid".to_string());
        }
        {
            let slot = self.active.lock().unwrap();
            if slot.is_some() {
                return Err("ACP permission callback already active".to_string());
            }
        }
        let generation = self
            .generation
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        let request_token = format!(
            "{}{}",
            CODING_AGENT_PERMISSION_REQUEST_TOKEN_PREFIX,
            Uuid::new_v4().simple()
        );
        let pending = CodingAgentPendingPermission {
            request_token,
            generation,
            tool_call_id,
            title,
            options,
        };
        validate_pending_permission(&pending)?;
        let mut slot = self.active.lock().unwrap();
        *slot = Some(ActivePermission {
            pending: pending.clone(),
            rpc_id,
            deadline,
            decision: None,
        });
        self.changed.notify_all();
        Ok(pending)
    }

    pub(super) fn wake_waiters(&self) {
        self.changed.notify_all();
    }

    fn permission_interrupted(
        deadline: Instant,
        cancel_requested: &AtomicBool,
        accepting: &AtomicBool,
    ) -> bool {
        cancel_requested.load(Ordering::Acquire)
            || !accepting.load(Ordering::Acquire)
            || Instant::now() >= deadline
    }

    pub(super) fn wait_until(
        &self,
        deadline: Instant,
        cancel_requested: &AtomicBool,
        accepting: &AtomicBool,
    ) -> PermissionWaitOutcome {
        let mut slot = self.active.lock().unwrap();
        loop {
            if Self::permission_interrupted(deadline, cancel_requested, accepting) {
                return PermissionWaitOutcome::Cancelled;
            }
            if let Some(active) = slot.as_ref() {
                if let Some(decision) = active.decision.clone() {
                    if Self::permission_interrupted(deadline, cancel_requested, accepting) {
                        return PermissionWaitOutcome::Cancelled;
                    }
                    return decision;
                }
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return PermissionWaitOutcome::Cancelled;
            }
            let (next, _) = self.changed.wait_timeout(slot, remaining).unwrap();
            slot = next;
        }
    }

    pub(super) fn try_respond(
        &self,
        request_token: &str,
        option_id: &str,
        cancel_requested: &AtomicBool,
        accepting: &AtomicBool,
    ) -> Result<(), &'static str> {
        if cancel_requested.load(Ordering::Acquire) || !accepting.load(Ordering::Acquire) {
            return Err("coding_agent_permission_not_pending");
        }
        let mut slot = self.active.lock().unwrap();
        let Some(active) = slot.as_mut() else {
            return Err("coding_agent_permission_not_pending");
        };
        if Instant::now() >= active.deadline {
            return Err("coding_agent_permission_not_pending");
        }
        if Self::permission_interrupted(active.deadline, cancel_requested, accepting) {
            return Err("coding_agent_permission_not_pending");
        }
        if active.pending.request_token != request_token {
            return Err("coding_agent_permission_token_stale");
        }
        let expected_generation = self.generation.load(Ordering::Acquire);
        if active.pending.generation != expected_generation {
            return Err("coding_agent_permission_token_stale");
        }
        if active.decision.is_some() {
            return Err("coding_agent_permission_already_resolved");
        }
        if !active
            .pending
            .options
            .iter()
            .any(|option| option.option_id == option_id)
        {
            return Err("coding_agent_permission_option_invalid");
        }
        active.decision = Some(PermissionWaitOutcome::Selected(option_id.to_string()));
        self.changed.notify_all();
        Ok(())
    }

    pub(super) fn clear(&self) {
        let mut slot = self.active.lock().unwrap();
        *slot = None;
        self.changed.notify_all();
    }

    pub(super) fn active_rpc_id(&self) -> Option<u64> {
        self.active
            .lock()
            .unwrap()
            .as_ref()
            .map(|active| active.rpc_id)
    }
}

pub(super) fn permission_outcome_for_acp(
    outcome: PermissionWaitOutcome,
    lifecycle_interrupted: bool,
    permission_deadline: Instant,
) -> PermissionWaitOutcome {
    if lifecycle_interrupted || Instant::now() >= permission_deadline {
        PermissionWaitOutcome::Cancelled
    } else {
        outcome
    }
}

pub(super) fn permission_result_json(outcome: &PermissionWaitOutcome) -> Value {
    match outcome {
        PermissionWaitOutcome::Selected(option_id) => {
            json!({"outcome":{"outcome":"selected","optionId":option_id}})
        }
        PermissionWaitOutcome::Cancelled => json!({"outcome":{"outcome":"cancelled"}}),
    }
}
