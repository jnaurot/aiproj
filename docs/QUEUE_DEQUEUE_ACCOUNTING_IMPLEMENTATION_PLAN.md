# Queue Dequeue Accounting and Depth Reduction Implementation Plan

## Step 0 - `docs: add dequeue accounting implementation and test plan`

### Goal
Establish a single, explicit implementation contract for queue depth truth so edge lifecycle/rendering cannot drift from backend execution semantics.

### Deliverables
- This plan committed in `docs/`.
- Scope confirmation note in PR description: no behavior change in this step.

### Best Practices
- No mixed refactor + behavior changes in one commit.
- Freeze baseline before touching runtime logic.
- Make invariants explicit before adding code.

### Tests
- None added in this step.

---

## Step 1 - `test(runner): lock current queue enqueue/dequeue invariants with failing reproductions`

### Goal
Create backend tests that reproduce the current mismatch (`enq > deq`, persistent depth) and prove expected behavior.

### Implementation
- Add targeted runner tests under `backend/tests/runner/`:
  - `test_queue_once_mode_depth_reduction.py`
  - `test_queue_dequeue_signal_ordering.py`
  - `test_queue_metrics_enq_deq_balance.py`
- Use small deterministic graphs:
  - `once` downstream consumer case (currently problematic).
  - `single_item` case (must continue to pass).
  - `batch` case (must continue to pass).
- Assert on emitted events and final `queue_metrics` payload:
  - Final `metrics.globalDepth == 0` for completed runs.
  - Per-edge `depth == 0` for consumed work edges.
  - `runtimeItemMetrics.itemsDequeued` tracks actual dequeue operations.

### Integration Tests
- Extend or add integration test in `backend/tests/integration/` covering full run lifecycle and run-finished snapshot consistency.

### Regression Tests
- Add explicit regression test for prior observed symptom: all nodes succeeded but edge remains logically waiting due to stale depth.

### Best Practices
- Tests first, fail first.
- One assertion per behavior axis (depth, counters, order).
- Deterministic fixtures only; no timing-sensitive sleeps.

### Exit Criteria
- New tests fail on current code for the known mismatch.
- No unrelated test edits.

---

## Step 2 - `feat(runner): emit explicit dequeue control signal and decrement control-plane depth`

### Goal
Make dequeue a first-class state transition in backend control signals.

### Implementation
- Backend changes in:
  - `backend/app/runner/run.py`
  - `backend/app/runner/control_plane.py`
- Add `item_dequeued` control signal emission whenever `queue_registry.dequeue(...)` succeeds.
- Update control-plane reducer logic:
  - `ITEM_ENQUEUED` -> `depth += 1`
  - `ITEM_DEQUEUED` -> `depth -= 1` (floored at 0)
  - `INPUT_DRAINED` remains terminal reset/sanity signal.
- Preserve sequence monotonicity and payload versioning (`control_signal.v1`).

### Integration Tests
- Validate event order for streaming edges:
  - `item_enqueued` -> `item_dequeued` -> `input_drained` -> `node_terminal`.

### Regression Tests
- Ensure no regressions for existing `input_drained`-based tests.

### Best Practices
- Backward compatible event schema evolution.
- Avoid overloading `input_drained` as both decrement and terminal.
- Keep event semantics orthogonal and auditable.

### Exit Criteria
- Step 1 tests pass except `once`-mode behavior still pending (if separate).
- Existing control signal tests continue to pass.

---

## Step 3 - `fix(runner): align once-mode queue behavior with zero-residual-depth policy`

### Goal
Prevent `once` nodes from leaving persistent queue depth artifacts.

### Implementation Options (choose one, document in PR)
1. Preferred: do not enqueue work-plane items for downstream `once` consumers; use dependency release and direct input contract only.
2. Alternate: keep enqueue but immediately consume logical token during `once` execution admission so depth is reduced to zero.

### Required Behavior
- Completed `once` runs cannot end with residual work-edge depth.
- Queue metrics and node completion are consistent.

### Integration Tests
- End-to-end run with mixed `once + single_item + batch` nodes where all complete successfully:
  - no stranded queue depth.
  - no false waiting on edges.

### Regression Tests
- Recreate user scenario: sibling branches where one executes and the other remains idle; ensure no phantom queue wait state leaks.

### Best Practices
- Minimize branching complexity in scheduler path.
- Keep consume-mode policy centralized.
- Add inline comments describing why `once` semantics differ from streaming.

### Exit Criteria
- All Step 1 failing reproductions now pass.
- No scheduler stall regressions.

---

## Step 4 - `feat(frontend): support item_dequeued signal and reconcile edge waiting projection`

### Goal
Ensure frontend runtime projection converges to backend queue truth.

### Implementation
- Frontend changes in:
  - `src/lib/flow/types/run.ts`
  - `src/lib/flow/store/graphStore.run.ts`
  - `src/lib/flow/components/runMonitorModel.ts`
  - `src/lib/flow/FlowCanvas.svelte` (only if needed)
- Add `item_dequeued` to allowed control signals.
- Update control-plane edge state reducer:
  - decrement depth on `item_dequeued`.
- Preserve completed-edge override behavior so settled edges remain green once source/target are completed.

### Integration Tests
- UI/store integration test with event stream including enqueue/dequeue/drain.
- Assert edge lifecycle transitions: waiting -> running (if active) -> settled/inactive with no amber residue.

### Regression Tests
- Add frontend regression test reproducing “all nodes succeeded but edge stayed waiting/amber.”

### Best Practices
- Keep projection logic deterministic and monotonic under out-of-order events.
- Never infer dequeue from unrelated signals.
- Prefer explicit backend truth over heuristic UI correction.

### Exit Criteria
- Edge color/state converges correctly after run completion.
- No regressions in run monitor filtering/sorting.

---

## Step 5 - `test(e2e): add full-stack queue truth scenarios for mixed consume modes`

### Goal
Prove end-to-end behavior across backend + frontend state model.

### Implementation
- Add E2E test coverage for:
  - forced-off cache full run,
  - partial run (`run from selected`),
  - checkpoint + cache-hit scenarios,
  - mixed consume modes and parallel model limits.
- Validate:
  - final queue depth correctness,
  - node lifecycle correctness,
  - edge visual state correctness,
  - no stranded queue warnings in successful runs.

### Integration Tests
- Use existing run event stream hooks and monitor projections.

### Regression Tests
- Preserve prior known-good scenarios for control signals and runtime metrics.

### Best Practices
- Favor black-box assertions from emitted events/state snapshots.
- Keep fixtures readable and small.
- Stabilize test IDs and timestamps.

### Exit Criteria
- End-to-end suite green for queue truth scenarios.

---

## Step 6 - `chore(observability): add queue truth diagnostics and run-end invariant checks`

### Goal
Increase operational confidence and ease future debugging.

### Implementation
- Add run-end diagnostics/log fields:
  - per-edge `enqueued`, `dequeued`, `depth` summary.
  - flag edges with `enqueued - dequeued != depth`.
- Add non-fatal warning log when invariant is violated.

### Integration Tests
- Assert diagnostic payload exists in run summary/logs.

### Regression Tests
- Ensure diagnostics do not alter runtime scheduling or status transitions.

### Best Practices
- Diagnostics must be side-effect free.
- Keep warning volume bounded.
- Use clear reason codes for grepability.

### Exit Criteria
- Invariants visible in logs and test assertions.

---

## Step 7 - `docs: finalize queue semantics and operational troubleshooting guide`

### Goal
Document the final contract so future changes cannot silently drift.

### Implementation
- Update relevant docs:
  - queue semantics,
  - control signal semantics (`item_enqueued`, `item_dequeued`, `input_drained`),
  - consume-mode behavior differences (`once` vs streaming),
  - expected edge lifecycle mapping.

### Integration Tests
- None.

### Regression Tests
- None.

### Best Practices
- Docs reflect code and tests exactly.
- Include “known bad smells” checklist for triage.

### Exit Criteria
- Documentation merged with links to core tests.

---

## Suggested Commit Sequence
1. `docs: add dequeue accounting implementation and test plan`
2. `test(runner): lock current queue enqueue/dequeue invariants with failing reproductions`
3. `feat(runner): emit explicit dequeue control signal and decrement control-plane depth`
4. `fix(runner): align once-mode queue behavior with zero-residual-depth policy`
5. `feat(frontend): support item_dequeued signal and reconcile edge waiting projection`
6. `test(e2e): add full-stack queue truth scenarios for mixed consume modes`
7. `chore(observability): add queue truth diagnostics and run-end invariant checks`
8. `docs: finalize queue semantics and troubleshooting`

## Execution Guardrails
- Do not combine frontend and backend behavioral changes in one commit.
- Every behavior change must have a regression test in the same commit.
- Run full affected suites after each step and record exact command/output in PR notes.
- If any step causes broad unrelated failures, stop and isolate before continuing.
