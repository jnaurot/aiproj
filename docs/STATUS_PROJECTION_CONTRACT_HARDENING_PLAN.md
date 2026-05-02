# Status Projection Contract Hardening Plan

## Goal

Reduce node-status drift risk quickly without introducing a full backend->frontend codegen pipeline.

Scope (this pass):
1. Add explicit contract tests that verify backend **node runtime status strings** are handled by frontend projection.
2. Remove duplicate lifecycle remapping paths in monitor projection.

Explicitly out of scope (this pass):
- Run-status contract alignment (`pending`/`deleted` vs frontend `idle` model).
- Full shared enum/codegen pipeline.

---

## Why this plan first

Recent bugs were primarily event-ordering and state-source bugs, not missing enum constants. This plan adds fast, mechanical guardrails against silent projection drift while preserving existing runtime semantics.

---

## Implementation Steps

## Step 0: One-time frontend status reachability audit (directional gap closure)

Before implementing the parity gate, run a one-time classification audit for frontend lifecycle/runtime statuses.

Scope for this audit must explicitly cover:
- `NodeLifecycleStatus`
- `RuntimeNodeStatus`
- (and any directly related projection status unions used by `normalizeRuntimeStatus()` / `projectNodeStatus()` call paths)

Use this taxonomy:

- `reachable` — backend emits this state through a known path.
- `legacy-compat` — no longer emitted but retained for compatibility with older data/events.
- `reserved` — intentionally defined in frontend ahead of planned backend feature; not yet reachable.
- `dead` — not emitted, not planned, no compatibility requirement; candidate for removal.

Notes:
- `reserved` entries are required to include a tracking anchor (issue/feature reference) so future audits can distinguish planned placeholders from stale scaffolding.
- `skipped` is currently classified as `reserved` (future development; intentionally not emitted by current backend `NODE_STATES`).
- Reachability search must include both:
	- state-machine declarations (`NODE_STATES` / transition helpers), and
	- direct binding status writes in backend event-application paths (e.g., `runtime.py` / `run.py`), which may bypass state-machine helper functions.

## Step 1: Add canonical backend node-status fixture in frontend tests

Create a JSON fixture plus a frontend contract test file:

- `src/lib/flow/components/backend-node-states.fixture.json` (new)
- `src/lib/flow/components/statusProjectionContract.test.ts`

Define an explicit fixture list mirroring backend `NODE_STATES` from:

- `backend/src/runtime/execution_state.py` (or current path where `NODE_STATES` is defined)

Fixture values:
- `idle`
- `running`
- `active`
- `blocked`
- `paused`
- `succeeded_up_to_date`
- `failed`
- `canceled`
- `stale`

Test file should import the JSON fixture and assert that list is the frontend contract source of truth.

## Step 2: Add projection contract tests (both input paths)

For **each backend status** in fixture:

1. `normalizeRuntimeStatus(status)` returns non-null.
2. `projectNodeStatus(...)` returns valid fields:
	- lifecycle in `idle|waiting|running|blocked|completed|failed|canceled|skipped`
	- execution in `inactive|waiting|running|blocked|finished`
	- freshness in `fresh|stale|unknown`

Exercise **both supported input forms** to avoid path-specific regressions:
- binding path (`{ status: <value> }`)
- explicit runtime-status argument path (where applicable)

Add explicit compressed-state mapping assertions:
- `active -> lifecycle=running`
- `succeeded_up_to_date -> lifecycle=completed`
- `paused -> lifecycle=waiting` **and** `execution=waiting`
- `stale -> lifecycle=completed` **and** `freshness=stale` **and** `execution=inactive` (triple assertion, all required)

Add precedence assertion for dual-input invocation:
- when both binding status and explicit runtime-status argument are provided, explicit runtime-status wins.

## Step 3: Unknown-status safety regression

Add a deterministic unknown-status case with explicit expected values:
- `normalizeRuntimeStatus('new_backend_state')` must not throw.
- `normalizeRuntimeStatus('new_backend_state')` must return `null`.
- `projectNodeStatus()` fallback must be:
	- `lifecycle='idle'`
	- `execution='inactive'`
	- `freshness='unknown'`

## Step 4: Remove duplicate lifecycle remapping only (precise scope)

Audit `runMonitorModel.ts` and remove secondary status->lifecycle remappers (e.g. `lifecycleFromDisplayStatus()` style logic) that re-derive lifecycle from display strings.

Rules:
- Authoritative lifecycle projection remains: runtime/binding status -> `projectNodeStatus()`.
- `displayReason` stays presentation-only and must not affect lifecycle.
- **Do not remove** `reconcileLifecycleForActiveRun()` or `reconcileModelLeaseLifecycle()` in this pass; those are runtime-signal reconciliations, not duplicate mappings.

## Step 5: Monitor regression tests for single-source lifecycle

Extend `src/lib/flow/components/runMonitorModel.test.ts` with:

- `succeeded_up_to_date` projects to monitor lifecycle `completed`.
- Post-refactor behavior parity check: monitor lifecycle/output remains correct after removing display-string remapping path(s).
- Reconciliation behavior still works with runtime signals (inflight/pending/lease), confirming Step 4 did not break active-run semantics.

## Step 6: Replace advisor drift step with explicit non-goal + gap note

Do **not** add redundant advisor-vs-display tests for this pass (advisor already consumes monitor rows, not display labels).

Instead, record a known deferred risk:
- Advisor rules that parse backend log text depend on log-string shape (protocol-by-convention).
- This coupling is out of scope for this pass and should be handled in a dedicated advisor/log-contract hardening plan.

---

## CI Gate (Specific, Lightweight, Mechanical)

Add a CI check script that compares backend `NODE_STATES` set with frontend JSON fixture set.

Recommended shape:

1. Backend extraction script (Python import mode):
	- Import backend module and serialize `NODE_STATES` into a sorted JSON array.
	- Example (this repo path, with backend import context available):
		- `python -c "from app.runner.execution_state import NODE_STATES; import json; print(json.dumps(sorted(NODE_STATES)))"`
	- Prerequisite: this CI job runs where Python is available (same pipeline that runs backend tests).
	- If Python is unavailable in the frontend-only CI job, run this gate in a separate backend-capable CI stage and publish comparison result.
2. Frontend fixture extraction:
	- Read `src/lib/flow/components/backend-node-states.fixture.json` directly and sort.
	- No TypeScript parsing, no grep/regex extraction.
3. Compare exact set equality (not count-only):
	- Fails on additions, removals, and renames.

Acceptance for gate:
- CI fails if sets differ in either direction.
- CI output prints missing-on-frontend and extra-on-frontend entries for quick fix.

This is intentionally not codegen; it is a low-cost drift gate.

---

## Test Plan

## A) New contract suite

- `src/lib/flow/components/statusProjectionContract.test.ts`
	- Exhaustive backend node-status coverage
	- Unknown-status fallback behavior
	- Compressed-state paired assertions
	- Dual input-path coverage

## B) Existing suite extensions

- `src/lib/flow/components/runMonitorModel.test.ts`
	- Single-source lifecycle projection regression (behavior preserved after remapping-path removal)
	- Reconciliation safety regression

## C) Focused execution command

```bash
npm run test -- \
  src/lib/flow/components/statusProjectionContract.test.ts \
  src/lib/flow/components/runMonitorModel.test.ts
```

## D) Backend transition-validity gap (separate but explicitly tracked)

Add follow-up backend tests (Python layer, not frontend):
- Transition table assertion tests in backend for illegal transitions (example: `blocked -> succeeded_up_to_date` invalid).
- Event-handler guard tests ensuring runtime event application respects transition constraints.

These are tracked as a follow-up item because they validate behavioral semantics, not projection coverage.

---

## Acceptance Criteria

0. One-time status reachability audit completed using taxonomy (`reachable|legacy-compat|reserved|dead`), with `reserved` entries carrying a tracking anchor.
1. Every backend node status in `NODE_STATES` is explicitly covered by frontend projection tests.
2. Contract tests cover both `projectNodeStatus()` input paths.
3. `paused` mapping explicitly asserts lifecycle/execution compression.
4. `stale` mapping explicitly asserts lifecycle + freshness + execution triple.
5. Dual-input precedence is tested (`runtimeStatus` overrides binding status when both supplied).
6. `runMonitorModel` no longer has display-string lifecycle remapping paths (verified by code review: deletion of remapper + call sites).
7. Reconciliation functions remain intact and behaviorally covered.
8. CI drift gate fails on backend/frontend status-set mismatch (add/remove/rename).
9. Targeted tests pass.

---

## Suggested Commit Sequence

1. `test(status): add backend node-status projection contract coverage (dual-path + stale pairing)`
2. `refactor(monitor): remove display-string lifecycle remapping and keep runtime reconciliation`
3. `test(monitor): add single-source lifecycle and reconciliation regressions`
4. `ci(status): add backend/frontend NODE_STATES set-equality drift gate`

If done in one pass:
- `fix(status-projection): enforce node-status contract, remove duplicate remapping, and add CI drift gate`

---

## Follow-up (separate sprint)

1. Run-status contract alignment (backend vs frontend run-state model).
2. Backend transition-validity hardening tests (if not completed in immediate follow-up).
3. Long-term shared schema/codegen pipeline:
	- canonical lifecycle/status schema
	- generated TS types from backend contract
	- CI generated-artifact drift gate
4. Advisor log-format contract hardening (reduce string-format coupling).
5. Module-boundary cleanup: move run-context lifecycle reconciliation helpers from `statusModel.ts` to a run-monitor-oriented module (e.g., `runMonitorModel.ts`) to restore projection-layer purity.
6. Revisit `reserved` states during feature implementation checkpoints; promote to `reachable` or reclassify with rationale when plans change.
