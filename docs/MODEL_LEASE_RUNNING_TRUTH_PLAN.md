# Model Lease Running Truth Plan

## Purpose
Fix model-node runtime truth so UI never shows `waiting` while a node is actively executing with an LLM lease, and ensure star/badge behavior remains correct under concurrency, partial runs, and lease handoffs.

## Problem Statement
Observed mismatch:
- Upstream work edge appears running.
- Model node (`ResumeBuilder`) is labeled `waiting`.
- Node has ready upstream artifacts.
- Concurrent LLM capacity is 2.

Likely failure mode:
- Frontend lease projection removes active holders incorrectly on `llm_lease: released`, especially when `holderNodeId` refers to another currently running holder.
- Running/star truth is currently tied to `meta.llmAllocated`; once cleared, lifecycle is demoted to `waiting`.

## Architectural Goals
1. Single source of truth for lease-running state in frontend: `queueRuntime.llmLease.activeNodeIds`.
2. No lease-state mutation from non-authoritative events (`control_signal` should remain informational only).
3. Deterministic handling of lease release/handoff under concurrency > 1.
4. Env-var contract is explicit and testable.
5. Running visuals obey rules in `docs/RULES_STORE.md`.

## Canonical Invariants
1. If model node id is in `activeNodeIds` during active run, node lifecycle must render as `running` and star must be visible.
2. If model node id is not in `activeNodeIds` during active run, star must be hidden.
3. A `released` event for node A must never clear star/running for node B unless B is explicitly absent from `activeNodeIds` after reconciliation.
4. Work-edge running visualization must not be used as sole proof of node running; node running is lease + scheduler truth.
5. Out-of-run lease events must not mutate active-run lease state.

## Env Var Contract (Normalization and Precedence)

### Canonical Variables
- `RUNNER_MAX_MODEL_PROVIDER_<PROVIDER>` (highest priority per provider)
- `RUNNER_MAX_MODEL_PROVIDER` (provider default)
- `RUNNER_MAX_MODEL` (global scheduler model cap)

### Compatibility Variables
- `RUNNER_MAX_LLM` (legacy alias for `RUNNER_MAX_MODEL`)
- `RUN_MAX_LLM` (legacy external alias; map to `RUNNER_MAX_LLM` with warning)
- `RUNNER_MAX_MODEL` ( map to `RUNNER_MAX_MODEL` with warning)

### Precedence Rules
1. Provider cap: `RUNNER_MAX_MODEL_PROVIDER_<PROVIDER>`
2. Provider fallback: `RUNNER_MAX_MODEL_PROVIDER`
3. Global model cap: `RUNNER_MAX_MODEL`
4. Legacy global alias: `RUNNER_MAX_LLM`
5. External compatibility aliases: `RUN_MAX_LLM`, `RUNNER_MAX_MODEL`

### Policy
- Compatibility aliases should be accepted for one deprecation window and emit structured startup warnings.
- Values must be parsed as positive ints; invalid/non-positive values are ignored with warning and fallback to next source.

## Phase Plan

## Phase 0: Freeze Current Behavior with Repro Tests
Goal: add failing tests that represent the bug before logic changes.

Frontend tests:
1. `acquired(A)`, `acquired(B)`, `released(A, holder=B)` keeps `B` in `activeNodeIds` and star visible for `B`.
2. During active run, node B with lease remains `running` even if A releases.
3. `released(A)` does not demote unrelated active model bindings.
4. Out-of-run `llm_lease` event is ignored (runId mismatch / stale run).

Backend tests:
1. Provider cap=2 emits lease sequence where release of A can coexist with active B.
2. `holderNodeId` semantics are documented and asserted in tests.

Acceptance:
- New tests fail on current implementation, proving the issue.

## Phase 1: Lease Projection Fix in Frontend Reducer
Goal: make lease set updates concurrency-safe and run-scoped.

Changes:
1. In `llm_lease` reducer:
- On `acquired`: add `nodeId`.
- On `released`: remove only `nodeId` from active set.
- Never remove `holderNodeId` as part of release removal logic.
2. Apply strict run scoping for lease events (same guard semantics as other node events).
3. Keep `activeNodeIds` as canonical truth used by `reconcileModelLeaseRunningInvariant`.

Acceptance:
- Phase 0 frontend tests pass.
- Existing lease/running invariant tests remain green.

## Phase 2: Lifecycle and Visual Consistency Hardening
Goal: remove transient UI contradictions.

Changes:
1. Ensure `reconcileModelLeaseLifecycle` only demotes running when lease set confirms absence.
2. Ensure node badge/star and lifecycle label derive from same reconciled state.
3. Preserve rule: non-work edges do not imply running execution.

Acceptance:
- Star + status + lease set are always coherent in snapshot tests.

## Phase 3: Env Var Compatibility and Diagnostics
Goal: make capacity semantics explicit and robust across environments.

Changes:
1. Add env normalization helper (backend) that resolves aliases and precedence.
2. Add warnings when `RUN_MAX_LLM` or `RUNNER_MAX_MODEL` are used.
3. Add diagnostics output in run logs/startup summary showing effective caps:
- global model cap
- provider default cap
- provider-specific cap

Acceptance:
- Env matrix tests pass (below).
- Runtime logs clearly show resolved cap sources.

## Phase 4: End-to-End Regression Harness
Goal: prevent reintroduction.

E2E scenarios:
1. `RUNNER_MAX_MODEL_PROVIDER_OLLAMA=2`: two models run concurrently, both show stars/running while leased.
2. Release one model while second remains active: second stays running/starred.
3. `RUNNER_MAX_MODEL_PROVIDER_OLLAMA=1`: queued waiting behavior, then handoff preserves correctness.
4. `run from selected` scope with sibling model already leased does not cross-mutate status.

Acceptance:
- All E2E scenarios pass with deterministic event ordering checks.

## Test Matrix

### Frontend Unit/Reducer
- `graphStore.llmAllocation.test.ts`
- `graphStore.modelRunningLeaseInvariant.test.ts`
- New: `graphStore.llmLease.concurrentHandoff.test.ts`
- New: `graphStore.llmLease.runScopeGuard.test.ts`

Assertions:
- `activeNodeIds` correctness
- node binding status (`running`/`waiting`)
- `meta.llmAllocated` star flag correctness
- no unrelated-node regressions

### Frontend Component
- `ModelNode.svelte` render tests:
- star visible iff leased and active run
- lifecycle text `running` when leased
- never `waiting` while leased

### Backend Unit
- `test_model_concurrency_caps.py`
- `test_llm_lease_events.py`
- New: alias/preference tests for `RUN_MAX_LLM` and `RUNNER_MAX_MODEL`

Assertions:
- cap resolution precedence
- valid fallback behavior on invalid values
- lease event shape and handoff semantics

### Backend Integration/E2E
- `test_model_e2e_guardrails.py`
- New: concurrent release-handoff truth test with cap=2

Assertions:
- lease event stream matches expected sequence
- holder and active set semantics are stable

## Rollout / Risk Controls
1. Ship in small commits by phase.
2. Keep compatibility aliases behind warning-only behavior first (no hard break).
3. Add debug trace toggle to print lease set transitions per event in dev/test.
4. Verify no regression in checkpoint/cache behavior after each phase.

## Definition of Done
1. Repro bug no longer occurs.
2. Star + running label are accurate under concurrent model execution.
3. Lease release for one node never clears another active node.
4. Env variable behavior is documented, implemented, and fully tested.
5. Full frontend + backend relevant suites pass.

## Suggested Commit Sequence
1. `test: add failing concurrent llm lease handoff regressions`
2. `fix: make llm lease active set update concurrency-safe and run-scoped`
3. `test: add model node visual truth assertions for running star/status`
4. `feat: add env cap alias normalization with precedence diagnostics`
5. `test: add env alias precedence and e2e lease handoff coverage`
