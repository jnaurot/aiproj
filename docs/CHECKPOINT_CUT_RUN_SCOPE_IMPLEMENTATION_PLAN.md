# CHECKPOINT_CUT_RUN_SCOPE_IMPLEMENTATION_PLAN.md

## Objective
Implement checkpoint-cut run planning so `run from selected` executes only the minimal required subgraph:

- Valid checkpointed nodes become execution boundaries.
- Upstream traversal stops at those boundaries.
- Sibling upstream paths are still included when required by downstream dependency closure.

This preserves deterministic debugging and reduces unnecessary recompute cost.

---

## Current vs Target

### Current
- `computePlannedNodeSet` includes ancestors for partial runs unconditionally.
- Checkpoint boundaries are applied for stale propagation, but not for initial planned set construction.

### Target
- Planner computes required closure from run targets (`selected` and descendants for `from_selected_onward`).
- Reverse traversal includes all required upstream nodes except upstream of checkpoint boundaries.
- If a downstream target depends on a checkpoint sibling branch, that sibling branch remains included.

---

## Non-Goals
- No change to checkpoint eligibility or staleness policy in this work.
- No change to run monitor styling or edge color semantics.
- No backend API contract changes.

---

## Phase 0 - Plan Authoring
**Commit message:** `docs: add checkpoint-cut run-scope implementation plan`

### Tasks
1. Add this document under `docs/`.
2. Define acceptance criteria and test matrix.

### Exit Criteria
- Plan committed with concrete phases, tests, and commit messages.

---

## Phase 1 - Planner Primitive: Dependency Closure With Checkpoint Cuts
**Commit message:** `phase1: add checkpoint-cut dependency-closure planner for partial runs`

### Tasks
1. Extend planner utility in `runScope.ts` to support checkpoint boundary cuts for partial runs.
2. Keep default behavior unchanged when no checkpoint boundary set is provided.
3. Ensure `from_start` remains full-graph planning.
4. Ensure `selected_only` and `from_selected_onward` use closure-with-cuts logic.

### Integration Tests
1. `from_selected_onward` with checkpoint on selected node:
- Graph `src -> xfm -> a`.
- Selected `a`, checkpoint boundary at `xfm`.
- Planned set should include `a` + `xfm`; exclude `src`.

2. Downstream node depends on checkpointed branch and sibling branch:
- Graph: `src -> xfm -> a`, and `src -> sib -> a`.
- Checkpoint boundary at `xfm`.
- Planned set for selected `a` should include `a`, `xfm`, `sib`, `src`.

### Regression Tests
1. Existing no-checkpoint planner tests still pass unchanged.
2. `from_start` still includes all nodes.

### Exit Criteria
- Planner supports optional checkpoint-cut behavior with deterministic outputs.

---

## Phase 2 - Wire Planner Into Run State Construction
**Commit message:** `phase2: use checkpoint-cut planner for run scope and schema guard`

### Tasks
1. Apply checkpoint-aware planner where planned node sets are derived:
- `run_started` fallback when event does not provide `plannedNodeIds`.
- `runRemote` pre-plan set and subgraph planning-related scope derivations.
- schema guard path (`assessSchemaGuard`) so validation scope matches run scope.
2. Derive checkpoint boundary set from `checkpointRegistry` entries that have executable lineage (`artifactId` and `execKey`).

### Integration Tests
1. Reducer fallback planning (`run_started` without planned ids) uses checkpoint cuts.
2. Schema guard includes only in-scope schema errors under checkpoint-cut scope.

### Regression Tests
1. Explicit `plannedNodeIds` from event remain authoritative.
2. Existing partial-run sibling exclusion tests with no checkpoint remain valid.

### Exit Criteria
- Active run scope, stale pre-marking scope, and schema guard scope all use the same planning semantics.

---

## Phase 3 - Scope Consistency for Stale/Update Guards
**Commit message:** `phase3: align stale/update guards with checkpoint-cut planned scope semantics`

### Tasks
1. Verify/adjust stale marking and binding update gates to honor new planned scope boundaries consistently.
2. Ensure out-of-scope siblings retain prior binding/memo/checkpointability state.
3. Preserve race protections (completed snapshot before `run_started`, old-run event rejection).

### Integration Tests
1. Partial run from downstream of checkpoint does not stale non-required upstream nodes.
2. Sibling-required upstream nodes are still stale/updated when in planned scope.

### Regression Tests
1. Existing race/run-scope tests continue passing.
2. No regression in checkpoint save visibility for cached artifacts in out-of-scope siblings.

### Exit Criteria
- Scope-sensitive state transitions are consistent and deterministic under checkpoint-cut planning.

---

## Phase 4 - Observability, Documentation, and Final Verification
**Commit message:** `phase4: add planner diagnostics and finalize checkpoint-cut run-scope docs/tests`

### Tasks
1. Add concise trace log for planner outcomes (targets, boundaries, planned set size) in dev trace path.
2. Document behavior in comments and update this plan with final status.
3. Run targeted suites and full related suites.

### Integration Tests
1. Planner trace details are stable and include boundary-aware summary.

### Regression Tests
1. `runScope.test.ts`
2. `graphStore.runScope.test.ts`
3. `graphStore.race.test.ts`
4. Checkpoint action/eligibility suites touched by run scope if impacted.

### Exit Criteria
- All related tests pass and planning behavior matches objective.

---

## Acceptance Criteria
1. `run from selected` downstream of checkpoint skips unnecessary upstream recompute.
2. Required sibling upstream paths are still included for correctness.
3. No behavioral change for runs without checkpoints.
4. `plannedNodeIds` (when provided by backend) continue to take precedence.
5. Existing run-scope and race protections remain green.

---

## File Touch Map
- `docs/CHECKPOINT_CUT_RUN_SCOPE_IMPLEMENTATION_PLAN.md`
- `src/lib/flow/store/runScope.ts`
- `src/lib/flow/store/runScope.test.ts`
- `src/lib/flow/store/graphStore.run.ts`
- `src/lib/flow/store/graphStore.runScope.test.ts`
- Additional touched tests as required by failures.
