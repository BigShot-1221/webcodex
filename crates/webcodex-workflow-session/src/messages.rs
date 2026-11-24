//! Session message board: post / list / resolve and discussion aggregates.
//!
//! All message-map mutations go through `SessionStoreInner` helpers.

use super::assignment::{open_assignment_state, snapshot_from_state};
use super::model::{
    CompleteSessionMessageInput, CompleteSessionMessageOutcome, ListSessionMessagesFilter,
    PostSessionMessageInput, ReplaceSessionMessageInput, ReplaceSessionMessageOutcome,
    SessionAckObservation, SessionAssignmentSnapshot, SessionAttentionSnapshot,
    SessionDiscussionSummary, SessionInboxHint, SessionMessage, SessionMessageDelivery,
    SessionMessageDeliveryOutcome, SessionMessageError, SessionMessageObservationError,
    SessionMessageObservationOutcome, SessionMessageStatus, WithdrawSessionMessageOutcome,
    DEFAULT_MAX_MESSAGES_PER_SESSION, DEFAULT_MESSAGE_LIST_LIMIT, MAX_MESSAGE_LIST_LIMIT,
    MAX_SESSION_MESSAGE_OBSERVATION_TOKEN_LEN, MAX_TOOL_CALL_ACK_REF_CHARS,
};
use super::query::{build_discussion_summary, build_inbox_hint};
use super::store::SessionStore;
use base64::{engine::general_purpose, Engine as _};
use sha2::{Digest, Sha256};

impl SessionStore {
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn post_message(
        &self,
        input: PostSessionMessageInput,
    ) -> Result<SessionMessage, SessionMessageError> {
        self.post_message_with_ack(input, false)
    }

    pub fn post_message_with_ack(
        &self,
        input: PostSessionMessageInput,
        requires_ack: bool,
    ) -> Result<SessionMessage, SessionMessageError> {
        let work_token = self
            .ensure_mutation_persistence_admitted()
            .map_err(|()| SessionMessageError::PersistenceUnavailable)?;
        let outcome = {
            let mut inner = self.lock_inner();
            inner.post_message(input, requires_ack, None)?
        };
        if outcome.state_changed {
            self.persist_after_mutation(&work_token)
                .map_err(|()| SessionMessageError::PersistenceUncertain)?;
            self.notify_message_observation();
        }
        Ok(outcome.message)
    }

    pub async fn post_message_with_ack_and_delivery(
        &self,
        input: PostSessionMessageInput,
        requires_ack: bool,
        delivery: Option<SessionMessageDelivery>,
    ) -> Result<SessionMessageDeliveryOutcome, SessionMessageError> {
        let durable = delivery.is_some();
        let admission = if durable {
            Some(
                self.prepare_durable_mutation()
                    .await
                    .map_err(|()| SessionMessageError::PersistenceUnavailable)?,
            )
        } else {
            None
        };
        let sync_work_token = if !durable {
            Some(
                self.ensure_mutation_persistence_admitted()
                    .map_err(|()| SessionMessageError::PersistenceUnavailable)?,
            )
        } else {
            None
        };
        let outcome = {
            let mut inner = self.lock_inner();
            inner.post_message(input, requires_ack, delivery)?
        };
        if durable {
            // Wake local observers before the cancellable durability wait so
            // subscribers can poll; the subsequent barrier still fences the
            // observation token against unpersisted revision state.
            if outcome.state_changed {
                self.notify_message_observation();
            }
            if self
                .complete_durable_mutation(admission.expect("durable admission"))
                .await
                .is_err()
            {
                return Err(SessionMessageError::DeliveryPersistenceUncertain);
            }
        } else if outcome.state_changed {
            self.persist_after_mutation(sync_work_token.as_ref().expect("sync admission"))
                .map_err(|()| SessionMessageError::PersistenceUncertain)?;
            self.notify_message_observation();
        }
        Ok(outcome)
    }

    pub fn list_messages(
        &self,
        session_id: &str,
        filter: ListSessionMessagesFilter,
    ) -> Result<Vec<SessionMessage>, SessionMessageError> {
        self.with_record_for_query(session_id, |record, _| {
            let limit = filter
                .limit
                .unwrap_or(DEFAULT_MESSAGE_LIST_LIMIT)
                .clamp(0, MAX_MESSAGE_LIST_LIMIT);
            record
                .messages
                .iter()
                .filter(|message| filter.kind.is_none_or(|kind| message.kind == kind))
                .filter(|message| filter.status.is_none_or(|status| message.status == status))
                .filter(|message| {
                    filter
                        .message_id
                        .as_deref()
                        .is_none_or(|message_id| message.message_id == message_id)
                })
                .filter(|message| {
                    filter
                        .reply_to
                        .as_deref()
                        .is_none_or(|reply_to| message.reply_to.as_deref() == Some(reply_to))
                })
                .rev()
                .take(limit)
                .map(|message| message.as_ref().clone())
                .collect()
        })
        .ok_or(SessionMessageError::UnknownSession)
    }

    /// Read one exact open todo and every retained direct reply under one
    /// Session-store snapshot. The durable persistence barrier is completed
    /// before the semantic snapshot fence is returned so a post-restart retry
    /// can compare against the same durable assignment state.
    pub async fn get_assignment(
        &self,
        session_id: &str,
        todo_id: &str,
    ) -> Result<SessionAssignmentSnapshot, SessionMessageError> {
        let admission = self
            .prepare_durable_observation_barrier()
            .await
            .map_err(|()| SessionMessageError::PersistenceUnavailable)?;
        let state = self
            .with_record_for_query(session_id, |record, _| {
                open_assignment_state(record, todo_id)
            })
            .ok_or(SessionMessageError::UnknownSession)??;
        if self
            .complete_durable_observation_barrier(admission)
            .await
            .is_err()
        {
            return Err(SessionMessageError::InvalidObservationState);
        }
        Ok(snapshot_from_state(session_id, todo_id, state))
    }

    pub fn observe_message_acks(
        &self,
        session_id: &str,
        message_ids: &[String],
    ) -> SessionAckObservation {
        if message_ids.is_empty() {
            return SessionAckObservation::default();
        }
        let work_token = match self.ensure_mutation_persistence_admitted() {
            Ok(token) => token,
            Err(()) => return SessionAckObservation::default(),
        };
        let outcome = {
            let mut inner = self.lock_inner();
            inner.observe_message_acks(session_id, message_ids)
        };
        if outcome.first_observed_count > 0 {
            if self.persist_after_mutation(&work_token).is_err() {
                return SessionAckObservation::default();
            }
            self.notify_message_observation();
        }
        outcome
    }

    pub fn issue_ack_ref(&self, session_id: &str, retained_ids: &[String]) -> Option<String> {
        if retained_ids.is_empty() {
            return None;
        }
        self.with_record_for_query(session_id, |record, _| {
            let open_ids = sorted_open_ack_message_ids(record);
            let retained = retained_ids
                .iter()
                .map(String::as_str)
                .collect::<std::collections::HashSet<_>>();
            if retained.is_empty() {
                return None;
            }

            let mut membership = vec![0_u8; open_ids.len().div_ceil(8)];
            let mut matched = 0_usize;
            for (index, message_id) in open_ids.iter().enumerate() {
                if retained.contains(message_id.as_str()) {
                    membership[index / 8] |= 1_u8 << (index % 8);
                    matched += 1;
                }
            }
            if matched != retained.len() {
                return None;
            }
            while membership.last().is_some_and(|byte| *byte == 0) {
                membership.pop();
            }
            if membership.is_empty() {
                return None;
            }

            let tag = ack_set_digest(
                session_id,
                &record.owner_authority_fingerprint,
                open_ids.iter().map(String::as_str),
                &membership,
            );
            let mut payload = Vec::with_capacity(ACK_REF_TAG_BYTES + membership.len());
            payload.extend_from_slice(&tag[..ACK_REF_TAG_BYTES]);
            payload.extend_from_slice(&membership);
            let token = format!(
                "{ACK_REF_PREFIX}{}",
                general_purpose::URL_SAFE_NO_PAD.encode(payload)
            );
            (token.len() <= MAX_TOOL_CALL_ACK_REF_CHARS).then_some(token)
        })
        .flatten()
    }

    pub fn resolve_ack_ref(&self, session_id: &str, ack_ref: &str) -> Option<Vec<String>> {
        if ack_ref.len() > MAX_TOOL_CALL_ACK_REF_CHARS {
            return None;
        }
        if !ack_ref.starts_with(ACK_REF_PREFIX) {
            return None;
        }
        let payload = general_purpose::URL_SAFE_NO_PAD
            .decode(ack_ref.strip_prefix(ACK_REF_PREFIX)?)
            .ok()?;
        if payload.len() < ACK_REF_TAG_BYTES {
            return None;
        }
        let (tag, membership) = payload.split_at(ACK_REF_TAG_BYTES);
        self.with_record_for_query(session_id, |record, _| {
            let open_ids = sorted_open_ack_message_ids(record);
            if membership.len() != open_ids.len().div_ceil(8) {
                return None;
            }
            let expected =
                ack_set_digest(
                    session_id,
                    &record.owner_authority_fingerprint,
                    open_ids.iter().map(String::as_str),
                    membership,
                );
            if tag != &expected[..ACK_REF_TAG_BYTES] {
                return None;
            }
            let mut resolved = Vec::new();
            for (index, message_id) in open_ids.iter().enumerate() {
                let byte = membership.get(index / 8).copied().unwrap_or(0);
                if byte & (1_u8 << (index % 8)) != 0 {
                    resolved.push(message_id.clone());
                }
            }
            (!resolved.is_empty()).then_some(resolved)
        })
        .flatten()
    }

    pub fn ack_required_messages(
        &self,
        session_id: &str,
        suppressed_ids: &[String],
    ) -> SessionAttentionSnapshot {
        let suppressed = suppressed_ids
            .iter()
            .map(String::as_str)
            .collect::<std::collections::HashSet<_>>();
        self.with_record_for_query(session_id, |record, _| {
            let mut open = record
                .messages
                .iter()
                .filter(|message| {
                    message.status == SessionMessageStatus::Open && message.requires_ack
                })
                .map(|message| message.as_ref().clone())
                .collect::<Vec<_>>();
            open.sort_by(|left, right| {
                left.created_at
                    .cmp(&right.created_at)
                    .then_with(|| left.message_id.cmp(&right.message_id))
            });
            let total_open_requires_ack = open.len();
            open.retain(|message| !suppressed.contains(message.message_id.as_str()));
            SessionAttentionSnapshot {
                messages: open,
                total_open_requires_ack,
            }
        })
        .unwrap_or_default()
    }

    pub async fn withdraw_message(
        &self,
        session_id: &str,
        message_id: &str,
    ) -> Result<WithdrawSessionMessageOutcome, SessionMessageError> {
        let admission = self
            .prepare_durable_mutation()
            .await
            .map_err(|()| SessionMessageError::PersistenceUnavailable)?;
        let outcome = {
            let mut inner = self.lock_inner();
            inner.withdraw_message(session_id, message_id)?
        };
        if !outcome.replayed {
            self.notify_message_observation();
        }
        if self.complete_durable_mutation(admission).await.is_err() {
            return Err(SessionMessageError::PersistenceUncertain);
        }
        Ok(outcome)
    }

    pub async fn replace_message(
        &self,
        input: ReplaceSessionMessageInput,
    ) -> Result<ReplaceSessionMessageOutcome, SessionMessageError> {
        let admission = self
            .prepare_durable_mutation()
            .await
            .map_err(|()| SessionMessageError::PersistenceUnavailable)?;
        let outcome = {
            let mut inner = self.lock_inner();
            inner.replace_message(input)?
        };
        if !outcome.replayed {
            self.notify_message_observation();
        }
        if self.complete_durable_mutation(admission).await.is_err() {
            return Err(SessionMessageError::PersistenceUncertain);
        }
        Ok(outcome)
    }

    pub fn resolve_message(
        &self,
        session_id: &str,
        message_id: &str,
        resolution: Option<String>,
    ) -> Result<SessionMessage, SessionMessageError> {
        let work_token = self
            .ensure_mutation_persistence_admitted()
            .map_err(|()| SessionMessageError::PersistenceUnavailable)?;
        let (message, changed) = {
            let mut inner = self.lock_inner();
            inner.resolve_message(session_id, message_id, resolution)?
        };
        if changed {
            self.persist_after_mutation(&work_token)
                .map_err(|()| SessionMessageError::PersistenceUncertain)?;
            self.notify_message_observation();
        }
        Ok(message)
    }

    pub fn resolve_message_from_wrapper(
        &self,
        session_id: &str,
        message_id: &str,
        resolution: String,
        current_request_acknowledged: bool,
    ) -> Result<SessionMessage, SessionMessageError> {
        let work_token = self
            .ensure_mutation_persistence_admitted()
            .map_err(|()| SessionMessageError::PersistenceUnavailable)?;
        let (message, changed) = {
            let mut inner = self.lock_inner();
            inner.resolve_message_from_wrapper(
                session_id,
                message_id,
                resolution,
                current_request_acknowledged,
            )?
        };
        if changed {
            self.persist_after_mutation(&work_token)
                .map_err(|()| SessionMessageError::PersistenceUncertain)?;
            self.notify_message_observation();
        }
        Ok(message)
    }

    pub async fn complete_message(
        &self,
        input: CompleteSessionMessageInput,
    ) -> Result<CompleteSessionMessageOutcome, SessionMessageError> {
        let admission = self
            .prepare_durable_mutation()
            .await
            .map_err(|()| SessionMessageError::PersistenceUnavailable)?;
        let result = {
            let mut inner = self.lock_inner();
            inner.complete_message(input)
        };
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(SessionMessageError::AssignmentStale {
                current,
                fresh_assignment_fence,
            }) => {
                if fresh_assignment_fence.is_some() {
                    if self.complete_durable_mutation(admission).await.is_err() {
                        return Err(SessionMessageError::AssignmentStale {
                            current,
                            fresh_assignment_fence: None,
                        });
                    }
                } else {
                    drop(admission);
                }
                return Err(SessionMessageError::AssignmentStale {
                    current,
                    fresh_assignment_fence,
                });
            }
            Err(error) => {
                drop(admission);
                return Err(error);
            }
        };
        if !outcome.replayed {
            self.notify_message_observation();
        }
        if self.complete_durable_mutation(admission).await.is_err() {
            return Err(SessionMessageError::PersistenceUncertain);
        }
        Ok(outcome)
    }

    pub async fn observe_messages(
        &self,
        session_id: &str,
        after_observation_token: Option<&str>,
        wait_secs: Option<u64>,
        limit: Option<usize>,
    ) -> Result<SessionMessageObservationOutcome, SessionMessageObservationError> {
        let limit = limit
            .unwrap_or(DEFAULT_MESSAGE_LIST_LIMIT)
            .clamp(1, MAX_MESSAGE_LIST_LIMIT);
        if after_observation_token.is_none() {
            let current_revision = self.message_observation_current_revision(session_id)?;
            self.fence_message_observation_token().await?;
            return Ok(SessionMessageObservationOutcome {
                messages: Vec::new(),
                observation_token: encode_observation_token(session_id, current_revision)?,
                changed: false,
                wait_outcome: "immediate",
                waited_ms: 0,
                history_lost: false,
                has_more: false,
            });
        }

        let after_revision =
            parse_observation_token(after_observation_token.expect("checked above"), session_id)?;
        let mut receiver = self.message_observation_notify.subscribe();
        let initial = self.message_observation_snapshot(session_id, after_revision, limit)?;
        if initial.2 || wait_secs.is_none() {
            self.fence_message_observation_token().await?;
            return observation_outcome(session_id, initial, "immediate", 0);
        }

        let wait_secs = wait_secs.expect("checked above");
        let started = std::time::Instant::now();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(wait_secs);
        loop {
            match tokio::time::timeout_at(deadline, receiver.changed()).await {
                Ok(Ok(())) => {
                    let snapshot =
                        self.message_observation_snapshot(session_id, after_revision, limit)?;
                    if snapshot.2 {
                        self.fence_message_observation_token().await?;
                        return observation_outcome(
                            session_id,
                            snapshot,
                            "updated",
                            elapsed_millis(started),
                        );
                    }
                }
                Ok(Err(_)) | Err(_) => {
                    // Re-snapshot after the deadline so a mutation racing the
                    // timeout cannot be lost between the last check and return.
                    let snapshot =
                        self.message_observation_snapshot(session_id, after_revision, limit)?;
                    if snapshot.2 {
                        self.fence_message_observation_token().await?;
                        return observation_outcome(
                            session_id,
                            snapshot,
                            "updated",
                            elapsed_millis(started),
                        );
                    }
                    self.fence_message_observation_token().await?;
                    return observation_outcome(
                        session_id,
                        snapshot,
                        "timeout",
                        elapsed_millis(started),
                    );
                }
            }
        }
    }

    fn message_observation_current_revision(
        &self,
        session_id: &str,
    ) -> Result<u64, SessionMessageObservationError> {
        self.retained_revisions(session_id)
            .map(|(_, revision)| revision)
            .ok_or(SessionMessageObservationError::UnknownSession)
    }

    fn message_observation_snapshot(
        &self,
        session_id: &str,
        after_revision: u64,
        limit: usize,
    ) -> Result<(Vec<SessionMessage>, u64, bool, bool, bool), SessionMessageObservationError> {
        self.with_record_for_query(session_id, |record, _| {
            if after_revision > record.message_observation_revision {
                return Err(SessionMessageObservationError::FutureRevision);
            }
            let history_lost = after_revision < record.message_observation_floor;
            let changed = record.message_observation_revision > after_revision;
            let mut candidates = record
                .messages
                .iter()
                .filter_map(|message| {
                    let revision = *record
                        .message_observation_revisions
                        .get(&message.message_id)
                        .unwrap_or(&0);
                    (revision > after_revision).then_some((revision, message.as_ref().clone()))
                })
                .collect::<Vec<_>>();
            candidates.sort_by(|left, right| {
                left.0
                    .cmp(&right.0)
                    .then_with(|| left.1.message_id.cmp(&right.1.message_id))
            });
            let has_more = candidates.len() > limit;
            candidates.truncate(limit);
            let next_revision = if has_more {
                candidates
                    .last()
                    .map(|(revision, _)| *revision)
                    .unwrap_or(after_revision)
            } else {
                record.message_observation_revision
            };
            Ok((
                candidates.into_iter().map(|(_, message)| message).collect(),
                next_revision,
                changed,
                history_lost,
                has_more,
            ))
        })
        .ok_or(SessionMessageObservationError::UnknownSession)?
    }

    async fn fence_message_observation_token(&self) -> Result<(), SessionMessageObservationError> {
        self.persist_after_mutation_durable()
            .await
            .map_err(|_| SessionMessageObservationError::InvalidObservationState)
    }

    fn notify_message_observation(&self) {
        self.message_observation_notify
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }

    pub fn discussion_summary(
        &self,
        session_id: &str,
        limit: Option<usize>,
    ) -> Result<SessionDiscussionSummary, SessionMessageError> {
        self.with_record_for_query(session_id, |record, _| {
            let limit = limit
                .unwrap_or(DEFAULT_MESSAGE_LIST_LIMIT)
                .clamp(0, MAX_MESSAGE_LIST_LIMIT);
            build_discussion_summary(record, limit)
        })
        .ok_or(SessionMessageError::UnknownSession)
    }

    pub fn inbox_hint(&self, session_id: &str) -> Option<SessionInboxHint> {
        self.with_record_for_query(session_id, |record, _| build_inbox_hint(record))
            .flatten()
    }
}

#[cfg(test)]
impl SessionStore {
    pub(crate) fn get_assignment_for_test(
        &self,
        session_id: &str,
        todo_id: &str,
    ) -> Result<super::model::SessionAssignmentSnapshot, SessionMessageError> {
        test_block_on(self.get_assignment(session_id, todo_id))
    }

    pub(crate) fn post_message_with_ack_and_delivery_for_test(
        &self,
        input: PostSessionMessageInput,
        requires_ack: bool,
        delivery: Option<SessionMessageDelivery>,
    ) -> Result<SessionMessageDeliveryOutcome, SessionMessageError> {
        test_block_on(self.post_message_with_ack_and_delivery(input, requires_ack, delivery))
    }

    pub(crate) fn withdraw_message_for_test(
        &self,
        session_id: &str,
        message_id: &str,
    ) -> Result<WithdrawSessionMessageOutcome, SessionMessageError> {
        test_block_on(self.withdraw_message(session_id, message_id))
    }

    pub(crate) fn replace_message_for_test(
        &self,
        input: ReplaceSessionMessageInput,
    ) -> Result<ReplaceSessionMessageOutcome, SessionMessageError> {
        test_block_on(self.replace_message(input))
    }

    pub(crate) fn complete_message_for_test(
        &self,
        input: CompleteSessionMessageInput,
    ) -> Result<CompleteSessionMessageOutcome, SessionMessageError> {
        test_block_on(self.complete_message(input))
    }
}

#[cfg(test)]
pub(crate) fn test_block_on<T>(future: impl std::future::Future<Output = T>) -> T {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        return handle.block_on(future);
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("session test runtime")
        .block_on(future)
}

fn elapsed_millis(started: std::time::Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn observation_outcome(
    session_id: &str,
    snapshot: (Vec<SessionMessage>, u64, bool, bool, bool),
    wait_outcome: &'static str,
    waited_ms: u64,
) -> Result<SessionMessageObservationOutcome, SessionMessageObservationError> {
    let (messages, next_revision, changed, history_lost, has_more) = snapshot;
    Ok(SessionMessageObservationOutcome {
        messages,
        observation_token: encode_observation_token(session_id, next_revision)?,
        changed,
        wait_outcome,
        waited_ms,
        history_lost,
        has_more,
    })
}

const OBSERVATION_TOKEN_PREFIX: &str = "wsm2_";
const OBSERVATION_TOKEN_BYTES: usize = 24;
const ACK_REF_PREFIX: &str = "wc_ack1_";
const ACK_REF_TAG_BYTES: usize = 12;

pub fn encode_observation_token(
    session_id: &str,
    revision: u64,
) -> Result<String, SessionMessageObservationError> {
    let mut payload = [0_u8; OBSERVATION_TOKEN_BYTES];
    payload[..8].copy_from_slice(&revision.to_be_bytes());
    payload[8..].copy_from_slice(&observation_revision_tag(session_id, revision)[..16]);
    let token = format!(
        "{OBSERVATION_TOKEN_PREFIX}{}",
        general_purpose::URL_SAFE_NO_PAD.encode(payload)
    );
    if token.len() > MAX_SESSION_MESSAGE_OBSERVATION_TOKEN_LEN {
        return Err(SessionMessageObservationError::OversizedToken);
    }
    Ok(token)
}

fn observation_revision_tag(session_id: &str, revision: u64) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"webcodex/session-message-observation/v2\0");
    hasher.update(session_id.as_bytes());
    hasher.update(revision.to_be_bytes());
    hasher.finalize().into()
}

fn parse_observation_token(
    token: &str,
    session_id: &str,
) -> Result<u64, SessionMessageObservationError> {
    if token.chars().count() > MAX_SESSION_MESSAGE_OBSERVATION_TOKEN_LEN {
        return Err(SessionMessageObservationError::OversizedToken);
    }
    let encoded = token
        .strip_prefix(OBSERVATION_TOKEN_PREFIX)
        .ok_or(SessionMessageObservationError::MalformedToken)?;
    let payload = general_purpose::URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| SessionMessageObservationError::MalformedToken)?;
    if payload.len() != OBSERVATION_TOKEN_BYTES {
        return Err(SessionMessageObservationError::MalformedToken);
    }
    let revision =
        u64::from_be_bytes(payload[..8].try_into().expect("observation revision bytes"));
    let expected = observation_revision_tag(session_id, revision);
    if payload[8..] != expected[..16] {
        return Err(SessionMessageObservationError::MalformedToken);
    }
    Ok(revision)
}

fn sorted_open_ack_message_ids(record: &super::model::SessionRecord) -> Vec<String> {
    let mut open_ids = record
        .messages
        .iter()
        .filter(|message| message.status == SessionMessageStatus::Open && message.requires_ack)
        .map(|message| message.message_id.clone())
        .collect::<Vec<_>>();
    open_ids.sort();
    open_ids
}

fn ack_set_digest<'a>(
    session_id: &str,
    owner_authority_fingerprint: &str,
    open_ids: impl IntoIterator<Item = &'a str>,
    membership: &[u8],
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"webcodex/session-ack-ref/v1\0");
    hasher.update(session_id.as_bytes());
    hasher.update(owner_authority_fingerprint.as_bytes());
    for message_id in open_ids {
        hasher.update(message_id.as_bytes());
        hasher.update([0]);
    }
    hasher.update(membership);
    hasher.finalize().into()
}
