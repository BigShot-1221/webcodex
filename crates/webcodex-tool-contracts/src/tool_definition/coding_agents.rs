use super::RunnerCapabilityRequirement::CodingAgentRuns;
use super::ToolVisibility::ModelVisible;
use super::{
    model_spec, permission_risk, require_all_scopes, ToolDeclaration, ToolDefinition,
    PERMISSION_RISK_JOB, PERMISSION_RISK_WRITE, TOOL_CATEGORY_CODING_AGENT,
};
use crate::metadata::{
    ToolPathHint::None as NoPath,
    ToolRisk::{JobRun, Read},
    CODING_AGENT_RUN, TOOL_PROVIDER_RUNNER,
};

pub(super) const DEFINITIONS: &[ToolDefinition] = &[
    permission_risk(
        model_spec(
            require_all_scopes(
                ToolDeclaration {
                    name: "start_coding_agent",
                    audit: super::ToolAuditPolicy::typed_fields(&[
                        super::ToolAuditResultField::value("run_id"),
                        super::ToolAuditResultField::value("project"),
                        super::ToolAuditResultField::value("provider_id"),
                        super::ToolAuditResultField::value("state"),
                        super::ToolAuditResultField::value("execution_state"),
                        super::ToolAuditResultField::value("cancel_requested"),
                        super::ToolAuditResultField::pointer("terminal_stop_reason", "/terminal/stop_reason"),
                        super::ToolAuditResultField::pointer("terminal_error_code", "/terminal/error_code"),
                        super::ToolAuditResultField::pointer("terminal_completed_at", "/terminal/completed_at"),
                        super::ToolAuditResultField::value("error_kind"),
                        super::ToolAuditResultField::value("recovery_kind"),
                    ]),
                    visibility: ModelVisible,
                    category: TOOL_CATEGORY_CODING_AGENT,
                    runner_capability: Some(CodingAgentRuns),
                    provider_id: TOOL_PROVIDER_RUNNER,
                    semantic: super::ToolSemanticContract {
                        effect: super::ToolEffect::Execute,
                        risk: JobRun,
                        approval: super::ToolApprovalPolicy::Standard,
                        idempotency: super::ToolIdempotency::Keyed,
                    },
                    required_scope: Some(CODING_AGENT_RUN),
                    requires_project: true,
                    path_hint: NoPath,
                    destructive: true,
                    shell_like: false,
                    session_evidence: super::ToolSessionEvidencePolicy::NONE,
                }.define(),
                &[CODING_AGENT_RUN, webcodex_core::authority::SCOPE_PROJECT_WRITE],
            ),
            "Start one delegated ACP Run on a Project/provider. Optional context_session_id adds an authorized handoff/Goal snapshot, not source resume or authority transfer. Intent binds project, provider_id, instruction, config, timeout, and context; changing any intent under the same idempotency_key conflicts. run_id is the opaque Run identity derived from authenticated principal + idempotency_key; never use JSON-RPC ids, app_call_id, or Job ids. With run_id, use observe_coding_agent(run_id); for lost/outcome_unknown follow reconciliation; do not start a replacement Run. If a just-issued start response was lost and no run_id arrived, an immediate retry with the exact same idempotency_key and exact same intent (unchanged start arguments) can recover the existing Run without redispatching session/prompt, while its record remains retained; this is idempotent recovery, not a new prompt. Ordinary terminal records are kept for up to 24h, with a 128-record store bound; capacity pressure can evict them earlier. Protected Lost/OutcomeUnknown may persist longer; if they fill capacity, new starts fail closed. If the window may have expired or capacity eviction may have occurred, do not retry start (even with the same key): a missing record does not prove the prompt never ran; stop for operator reconciliation. Do not mint a new idempotency_key or alter intent after uncertainty. Autonomous execution may outlive this request. Choose providers only from the exact Runner inventory; no automatic quota/model switching.",
        ),
        PERMISSION_RISK_JOB,
    ),
    model_spec(
        ToolDeclaration {
            name: "observe_coding_agent",
            audit: super::ToolAuditPolicy::typed_semantic(
                super::ToolAuditSemanticResultPolicy::CodingAgentObservation,
            ),
            visibility: ModelVisible,
            category: TOOL_CATEGORY_CODING_AGENT,
            runner_capability: None,
            provider_id: TOOL_PROVIDER_RUNNER,
            semantic: super::ToolSemanticContract {
                effect: super::ToolEffect::Observe,
                risk: Read,
                approval: super::ToolApprovalPolicy::None,
                idempotency: super::ToolIdempotency::PureRead,
            },
            required_scope: Some(CODING_AGENT_RUN),
            requires_project: false,
            path_hint: NoPath,
            destructive: false,
            shell_like: false,
            session_evidence: super::ToolSessionEvidencePolicy::NONE,
        }.define(),
        "Observe bounded normalized events and lifecycle for one existing CodingAgentRun. Return the opaque token for only-new follow-ups; history loss/reset is explicit. Observation never starts, retries, or resumes ACP work.",
    ),
    permission_risk(
        model_spec(
            ToolDeclaration {
                name: "cancel_coding_agent",
                audit: super::ToolAuditPolicy::typed_fields(&[
                    super::ToolAuditResultField::value("run_id"),
                    super::ToolAuditResultField::value("project"),
                    super::ToolAuditResultField::value("provider_id"),
                    super::ToolAuditResultField::value("state"),
                    super::ToolAuditResultField::value("execution_state"),
                    super::ToolAuditResultField::value("cancel_requested"),
                    super::ToolAuditResultField::pointer("terminal_stop_reason", "/terminal/stop_reason"),
                    super::ToolAuditResultField::pointer("terminal_error_code", "/terminal/error_code"),
                    super::ToolAuditResultField::pointer("terminal_completed_at", "/terminal/completed_at"),
                    super::ToolAuditResultField::value("error_kind"),
                    super::ToolAuditResultField::value("recovery_kind"),
                ]),
                visibility: ModelVisible,
                category: TOOL_CATEGORY_CODING_AGENT,
                runner_capability: None,
                provider_id: TOOL_PROVIDER_RUNNER,
                // Cancel is Run lifecycle control but deliberately not a second
                // WebCodex PermissionEvaluator decision after start admission.
                semantic: super::ToolSemanticContract {
                    effect: super::ToolEffect::Mutate,
                    risk: super::ToolRisk::RunControl,
                    approval: super::ToolApprovalPolicy::InheritFromStart,
                    idempotency: super::ToolIdempotency::DesiredState,
                },
                required_scope: Some(CODING_AGENT_RUN),
                requires_project: false,
                path_hint: NoPath,
                destructive: true,
                shell_like: false,
                session_evidence: super::ToolSessionEvidencePolicy::NONE,
            }.define(),
            "Request cancellation of one existing CodingAgentRun. This does not grant permission, retry a prompt, or create a replacement Run; observe the same run_id for authoritative terminal state.",
        ),
        PERMISSION_RISK_WRITE,
    ),
    permission_risk(
        model_spec(
            require_all_scopes(
                ToolDeclaration {
                    name: "respond_coding_agent_permission",
                    audit: super::ToolAuditPolicy::typed_fields(&[
                        super::ToolAuditResultField::value("run_id"),
                        super::ToolAuditResultField::value("project"),
                        super::ToolAuditResultField::value("provider_id"),
                        super::ToolAuditResultField::value("state"),
                        super::ToolAuditResultField::value("execution_state"),
                        super::ToolAuditResultField::value("applied"),
                        super::ToolAuditResultField::pointer("terminal_stop_reason", "/terminal/stop_reason"),
                        super::ToolAuditResultField::pointer("terminal_error_code", "/terminal/error_code"),
                        super::ToolAuditResultField::pointer("terminal_completed_at", "/terminal/completed_at"),
                        super::ToolAuditResultField::value("error_kind"),
                        super::ToolAuditResultField::value("recovery_kind"),
                    ]),
                    visibility: ModelVisible,
                    category: TOOL_CATEGORY_CODING_AGENT,
                    runner_capability: None,
                    provider_id: TOOL_PROVIDER_RUNNER,
                    semantic: super::ToolSemanticContract {
                        effect: super::ToolEffect::Mutate,
                        risk: super::ToolRisk::RunControl,
                        approval: super::ToolApprovalPolicy::Standard,
                        idempotency: super::ToolIdempotency::NonIdempotent,
                    },
                    required_scope: Some(CODING_AGENT_RUN),
                    requires_project: false,
                    path_hint: NoPath,
                    destructive: true,
                    shell_like: false,
                    session_evidence: super::ToolSessionEvidencePolicy::NONE,
                }.define(),
                &[CODING_AGENT_RUN, webcodex_core::authority::SCOPE_PROJECT_WRITE],
            ),
            "Submit one explicit ACP permission choice for an authorized Run in waiting_permission. Requires the exact pending request token and an option_id advertised by the live provider callback; never auto-approves and never accepts JSON-RPC correlation ids. Observe the same run_id for follow-up execution.",
        ),
        PERMISSION_RISK_WRITE,
    ),
    model_spec(
        ToolDeclaration {
            name: "diagnose_coding_agent",
            audit: super::ToolAuditPolicy::typed_fields(&[
                super::ToolAuditResultField::value("project"),
                super::ToolAuditResultField::value("provider_id"),
                super::ToolAuditResultField::pointer("live_acp_outcome", "/live_acp/outcome"),
            ]),
            visibility: ModelVisible,
            category: TOOL_CATEGORY_CODING_AGENT,
            runner_capability: Some(CodingAgentRuns),
            provider_id: TOOL_PROVIDER_RUNNER,
            // Presence-only is pure observation. The optional live ACP probe
            // launches one ephemeral provider process, so the runtime gates
            // include_live_acp=true on the same explicit authority as
            // start_coding_agent; this static contract never grants it.
            semantic: super::ToolSemanticContract {
                effect: super::ToolEffect::Observe,
                risk: Read,
                approval: super::ToolApprovalPolicy::None,
                idempotency: super::ToolIdempotency::PureRead,
            },
            required_scope: Some(CODING_AGENT_RUN),
            requires_project: true,
            path_hint: NoPath,
            destructive: false,
            shell_like: false,
            session_evidence: super::ToolSessionEvidencePolicy::NONE,
        }.define(),
        "Diagnose one explicitly selected ACP provider for one exact authorized Project through its configured Runner. Default (include_live_acp=false): report bounded operator-allowed and forced config option IDs (never values), and the provider's configured env_from_env destination keys as present/absent booleans for their mapped source variables in the running Runner process - not the source names or values, unrelated names, paths, or process launches. include_live_acp=true additionally requires the same scope authority as start_coding_agent (including OAuth coding_agent:run and project:write) and a fresh per-Project permission decision, then launches one bounded ephemeral provider process exactly like a Run start, performing only initialize then session/new to project the live config-option identities (bounded ids, labels, kinds, and allowlisted choices); it never sends session/prompt, runs provider tools, or writes configuration, and the whole process tree is terminated on every exit path. One probe runs at a time per Runner within a fixed budget; a concurrent probe returns a bounded busy outcome. Results come from the live Runner, never from static configuration; stale Runner, provider-generation, or Project identities fail closed with bounded reason codes. No automatic retry or fallback: re-observe the Runner inventory and fix inputs instead.",
    ),
];
