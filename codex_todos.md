# Node/Edge State Consistency TODOs

## Why this is worth doing now
This lays down a clear contract between backend execution truth and frontend status projection so we stop debugging "visual contradictions" (node waiting while edge running). It reduces operator confusion, improves trust in Run Monitor, and gives us deterministic tests before more control-plane work lands.

## Ticket NSC-001: Define authoritative running contract
Goal: Establish one canonical rule for when a node is `running` vs `waiting` in active runs.

Scope:
- Document contract in `docs/RULES_STORE.md` and status model comments.
- Running is true if any of: runtime status `running|active`, scheduler inflight > 0, incoming work edge exec=`active`.
- Waiting is true only when not running and node is eligible but no work is currently executing.

Acceptance Criteria:
- No UI state where incoming work edge is active and node lifecycle is waiting.
- Contract wording is explicit and testable.

Tests:
- Add `src/lib/flow/store/statusModel.consistency.test.ts`
- Case: incoming active work edge + inflight=0 => node lifecycle resolves to `running`.
- Case: no active edges + no inflight + ready signal present => `waiting`.

## Ticket NSC-002: Remove unconditional streaming downgrade
Goal: Remove/replace `single_item|batch completed -> waiting` unconditional downgrade in active runs.

Scope:
- Refactor `reconcileLifecycleForActiveRun` in `src/lib/flow/store/statusModel.ts`.
- Keep streaming nodes `completed` when terminal signals are satisfied.
- Use explicit execution signals (inflight/edge active/runtime status), not consume mode alone.

Acceptance Criteria:
- Streaming nodes do not flip to waiting solely because consume mode is `single_item` or `batch`.
- Completed nodes remain completed when terminalized.

Tests:
- Update/add `src/lib/flow/store/statusModel.test.ts`
- Assert completed streaming node remains completed with no inflight + no active edges.
- Assert streaming node goes running when edge active.

## Ticket NSC-003: Edge/node projection consistency guard in UI
Goal: Enforce consistency at projection boundary so canvas cannot render contradictory states.

Scope:
- In projection layer (`FlowCanvas.svelte` + monitor model), add consistency guard:
  - if incoming work edge visual class is running/active, node lifecycle must not be waiting.
- Guard is non-destructive and deterministic (no random time-based fallback).

Acceptance Criteria:
- Repro screenshot scenario (dashed blue edge + waiting node) no longer possible.

Tests:
- Add `src/lib/flow/store/graphStore.edgeNodeConsistency.test.ts`
- Event sequence replay: edge_exec active + node_blocked waiting => final projection must show node running.

## Ticket NSC-004: Symmetric backend edge terminalization
Goal: Guarantee `edge_exec=active` is always terminalized (`done` or `idle`) on all node execution exits.

Scope:
- In `backend/app/runner/run.py`, ensure edge terminal emit happens in a finally-style path for:
  - success
  - fail
  - canceled
  - early-return/cache branches
- Success => `done`; fail/cancel => `idle` (or policy-defined value, but consistent).

Acceptance Criteria:
- No stale `edge_exec=active` after node terminal event.

Tests:
- Add `backend/tests/runner/test_edge_exec_terminalization_symmetry.py`
- Parametrize exit paths (success/fail/cancel/cache-hit).
- Assert last edge state is never `active` after node terminal event.

## Ticket NSC-005: Monitor semantics deconfliction
Goal: Prevent label collision between node lifecycle `waiting` and monitor blocked reason `-`.

Scope:
- Rename/clarify monitor field label (e.g., `blocked reason` or `last blocked`) so `-` is clearly "no blocked reason".
- Ensure node lifecycle label is independent and explicitly documented.

Acceptance Criteria:
- UI text makes it impossible to infer `monitor '-' == node waiting`.

Tests:
- Add `src/lib/flow/components/runMonitorModel.semantics.test.ts`
- Verify node lifecycle and blocked reason fields are independently populated.
- Verify no blocked reason maps to `null`/`-` display without changing lifecycle.

## Ticket NSC-006: End-to-end regression for the reported contradiction
Goal: Lock the exact user-observed contradiction as a permanent regression test.

Scope:
- Build event replay fixture from real sequence:
  - upstream once node completed
  - downstream streaming node processed subset
  - edge active
  - node incorrectly waiting (current behavior)
- Expected final projection after fixes: node running while edge active, then completed after edge terminalization.

Acceptance Criteria:
- Test fails on current buggy projection and passes after fixes.

Tests:
- Add `src/lib/flow/store/graphStore.regression.waiting-vs-running.test.ts`
- Optional backend counterpart in `backend/tests/integration/test_edge_node_state_alignment.py`.

## Ticket NSC-007: Observability assertion hooks
Goal: Make mismatch diagnosable in logs without deep manual forensics.

Scope:
- Emit warning when projection sees `edge active + node waiting` for same target node.
- Include node id, edge id, run id, inflight, pending counts.

Acceptance Criteria:
- One structured warning event per mismatch signature (deduped).

Tests:
- Add `src/lib/flow/store/graphStore.telemetryConsistency.test.ts`
- Verify warning emitted and deduped for repeated identical mismatch.

## Suggested implementation order (when you’re ready)
1. NSC-001, NSC-002 (contract + lifecycle fix)
2. NSC-004 (backend edge terminal symmetry)
3. NSC-003 (projection guard)
4. NSC-005 (semantics clarity)
5. NSC-006, NSC-007 (regression + telemetry hardening)
