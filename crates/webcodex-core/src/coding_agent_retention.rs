//! ACP replay and retention policy shared by the Server and Runner.
//! This is deliberately separate from the provider protocol and Run state machine.

use super::{CodingAgentExecutionState, CodingAgentRunSnapshot, CodingAgentRunState};
use serde::{Deserialize, Serialize};

pub const REPLAY_KEY_PREFIX: &str = "wc-acp-v2:";
pub const REPLAY_WINDOW_SECS: i64 = 24 * 60 * 60;
pub const SUCCESS_HOT_RETENTION_SECS: i64 = 15 * 60;
pub const FAILURE_HOT_RETENTION_SECS: i64 = 24 * 60 * 60;
pub const ORDINARY_REPLAY_RETENTION_SECS: i64 = 24 * 60 * 60;

/// A single immutable, versioned key timestamp. The original key remains part
/// of the deterministic Run identity, not a separately mutable expiry input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodingAgentReplayWindow {
    pub issued_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayWindowError {
    Invalid,
    Future,
    Expired,
}

impl CodingAgentReplayWindow {
    pub fn expires_at(self) -> Result<i64, ReplayWindowError> {
        if self.issued_at <= 0 {
            return Err(ReplayWindowError::Invalid);
        }
        self.issued_at
            .checked_add(REPLAY_WINDOW_SECS)
            .ok_or(ReplayWindowError::Invalid)
    }

    /// For new execution only. Observing existing identities remains possible
    /// after expiry; missing evidence is never a proof of non-execution.
    pub fn check_at(self, now: i64) -> Result<(), ReplayWindowError> {
        let expiry = self.expires_at()?;
        if now < self.issued_at {
            return Err(ReplayWindowError::Future);
        }
        if now >= expiry {
            return Err(ReplayWindowError::Expired);
        }
        Ok(())
    }
}

/// Strict canonical v2 syntax; non-versioned keys remain legacy identities.
/// Unknown/malformed versioned keys must not fall back to legacy admission.
pub fn parse_replay_key(key: &str) -> Result<Option<CodingAgentReplayWindow>, ReplayWindowError> {
    let Some(rest) = key.strip_prefix(REPLAY_KEY_PREFIX) else {
        return if key.starts_with("wc-acp-v") {
            Err(ReplayWindowError::Invalid)
        } else {
            Ok(None)
        };
    };
    let (timestamp, nonce) = rest.split_once(':').ok_or(ReplayWindowError::Invalid)?;
    if timestamp.is_empty()
        || timestamp.starts_with('0')
        || !timestamp.bytes().all(|byte| byte.is_ascii_digit())
        || nonce.len() != 32
        || !nonce.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ReplayWindowError::Invalid);
    }
    let window = CodingAgentReplayWindow {
        issued_at: timestamp.parse().map_err(|_| ReplayWindowError::Invalid)?,
    };
    window.expires_at()?;
    Ok(Some(window))
}

/// Uncertain outcomes and uncertain terminal persistence are never ordinary
/// retention candidates, even if the visible state is terminal.
pub fn is_ordinary_terminal(run: &CodingAgentRunSnapshot) -> bool {
    run.state.terminal()
        && run.state != CodingAgentRunState::Lost
        && run.execution_state != CodingAgentExecutionState::OutcomeUnknown
        && run.terminal.as_ref().is_some_and(|terminal| {
            terminal.error_code.as_deref() != Some("coding_agent_terminal_persistence_uncertain")
        })
}

pub fn hot_retention_deadline(run: &CodingAgentRunSnapshot) -> Option<i64> {
    if !is_ordinary_terminal(run) {
        return None;
    }
    let duration = match run.state {
        CodingAgentRunState::Completed | CodingAgentRunState::Cancelled => SUCCESS_HOT_RETENTION_SECS,
        CodingAgentRunState::Failed => FAILURE_HOT_RETENTION_SECS,
        _ => return None,
    };
    run.terminal.as_ref()?.completed_at.checked_add(duration)
}

/// Lower bound on durable ordinary replay retention, independent of hot-cache
/// residency and of the key's admission window. Not deletion authorization.
pub fn ordinary_replay_deadline(run: &CodingAgentRunSnapshot) -> Option<i64> {
    if !is_ordinary_terminal(run) {
        return None;
    }
    run.terminal.as_ref()?.completed_at.checked_add(ORDINARY_REPLAY_RETENTION_SECS)
}

/// Whether a Run may leave the bounded hot inventory (Server or Runner memory).
pub fn hot_inventory_expired(run: &CodingAgentRunSnapshot, now: i64) -> bool {
    hot_retention_deadline(run).is_some_and(|deadline| now >= deadline)
}

/// Whether ordinary replay evidence has reached the durable retention floor.
pub fn durable_replay_expired(run: &CodingAgentRunSnapshot, now: i64) -> bool {
    ordinary_replay_deadline(run).is_some_and(|deadline| now >= deadline)
}

// Neither the ordinary retention floor nor a wall-clock expiry is sufficient
// authority to erase a replay fence. A later bounded maintenance operation
// must prove durable expiry and downgrade safety; legacy keys have no expiry.
