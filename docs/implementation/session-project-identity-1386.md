# Session project identity (#1386) — Server / Workflow Session

This Fork adapts upstream **8553da96** Session identity fencing without porting
SQLite incremental persistence (#1322). The JSON ledger/full-snapshot writer
remains authoritative.

## Durable identity invariant

Every **new** Project-bound Workflow Session persists `project_identity` with:

1. `root_fingerprint` — canonical `PROJECT_ROOT_FINGERPRINT_PREFIX` (`wc_projroot_`)
   plus exactly 64 hexadecimal digits (ASCII hex case per shared Runner validation).
2. `registration_incarnation` — non-empty UUID string, at most 36 characters.

`SessionProjectIdentity::from_runner_inventory` never mints path-only identity.
The `registered_path` field is a legacy compatibility field; it may round-trip
from old JSON rows but is never minted for new identity or used for
authorization. Live authorization compares **strong** identities and rejects
path-only rows fail-closed (`session_project_identity_changed`).

Runner inventory validation may still admit projects with incomplete metadata
for discovery compatibility. Missing or invalid identity fields fail closed at
durable Session **creation**, **resume**, and **authorization** only.

## Error taxonomy (ToolRuntime)

`start_session` and coding-workflow startup distinguish:

| Condition | `error_kind` | `failure_kind` | `state_changed` |
|-----------|--------------|----------------|-----------------|
| Missing/invalid root fingerprint | `project_root_fingerprint_unavailable` | `project_identity_unavailable` | `false` |
| Missing/invalid registration incarnation | `project_registration_incarnation_unavailable` | `project_identity_unavailable` | `false` |

Both use `FixInput` recovery. A missing root fingerprint is never reported as a
missing incarnation.

## Authorization

- `authorize_session_target` compares stored identity to the identity derived
  from the **same** resolved Project snapshot used for credential checks.
- Both sides must pass `SessionProjectIdentity::is_strong_identity()` and match
  exactly; otherwise `session_project_identity_changed` before side effects.
- Only `read_session_summary` and `close_session` may use the retired-project
  fallback (complete inventory, connected online Runner, exact Project id absent
  from the snapshot—including disabled rows still present in inventory).

## Close durability

After `close_session` and `close_persistent_shells_for_session`, **every**
successful close (including `already_closed` replay) awaits
`SessionStore::await_persistence_barrier()` so lifecycle and shell evidence share
one durability boundary. Barrier failure → `session_close_persistence_uncertain`
with `state_changed=true` / `RetrySame`. Writer-side barrier integration for
#1322 remains separate.

## Creation / resume

- `start_session` and coding-task bootstrap require strong identity from Runner
  inventory and persist `project_identity` on create.
- Resume requires exact Project id, owner authority, and matching strong
  `project_identity`.
- `SessionStore` resume additionally requires stored and request
  `project_identity` to match exactly (`ResumeProjectIdentityMismatch`). That
  check lives on the coding/resume commit path inside the store, not on
  `authorize_session_target`. Production ToolRuntime always derives identity via
  `session_project_identity_failure_from_resolved` before calling the store, and
  every subsequent Session-scoped tool mutation still fences through
  `authorize_session_target` with `is_strong_identity()` on both sides. This is
  layered enforcement, not a production authorization bypass.

## Tests (focused)

- `src/tool_runtime/tests/sessions_retired_projects.rs` — retired access, incarnation
  fence, missing root fingerprint, missing registration incarnation.

## Deferred / risks

- Many integration tests still seed Sessions without `project_identity` via direct
  `SessionStore` helpers; live tool paths against those rows will fence.
- Workflow Session crate tests that resume without matching identity need explicit
  `project_identity` on `CodingSessionRequest` (store tests use `None` only where
  resume identity checks are not exercised).
