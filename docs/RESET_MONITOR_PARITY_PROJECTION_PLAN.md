# Reset Monitor Parity Projection Plan

## Objective

When user triggers **Project -> Reset**, node cards and Run Monitor must converge immediately from the same runtime truth:

- Cards: `idle`
- Monitor buckets: no stale `Active` / `Waiting` rows from prior run

No reset-only exception logic. Fix by strengthening the projection contract and canonical runtime state flow.

---

## Design Principle

Use a **single canonical runtime snapshot** and derive both surfaces from it:

1. Canvas node status projection
2. Run Monitor row projection + group buckets

Reset should only mutate canonical runtime state. All UI projections should naturally reflect idle/empty runtime.

---

## Implementation Steps

## Step 1: Locate canonical runtime state and projection entrypoints

Files to inspect first:

- `src/lib/flow/store/graphStore.ts`
- `src/lib/flow/components/runMonitorModel.ts`
- `src/lib/flow/FlowCanvas.svelte`

Identify:

- runtime state fields used to compute monitor rows (`lifecycle`, `execution`, `phase`, blockers, inflight, pending)
- reset path(s): `hard reset`, `run reset`, `ui reset`
- where monitor rows are recomputed

Expected outcome:

- explicit mapping doc comments in code for which fields are canonical vs derived.

## Step 2: Normalize reset to canonical runtime-state transition only

In graphStore reset path:

- clear/terminalize prior run runtime state in one state transaction
- set node runtime lifecycle/execution/phase into idle baseline
- clear runtime blocker/lease/inflight metadata that can mark nodes active/waiting

Do **not** patch monitor groups directly.

Expected outcome:

- no stale runtime markers remain after reset mutation.

## Step 3: Ensure monitor projection recomputes from post-reset canonical state

In monitor model projection:

- guarantee rows are rebuilt from current runtime state after reset
- prevent retained row artifacts from prior run snapshot if run is reset

Add invariant guard:

- if canonical state has no active runtime evidence, row cannot project as `active`/`waiting` due only to stale phase fields.

## Step 4: Align card status and monitor status to same projection epoch

In FlowCanvas/store subscriptions:

- ensure both card projection and monitor projection consume same updated snapshot/version
- avoid split updates where cards update first but monitor rows lag on old object reference

If needed, add a monotonic runtime revision counter to force synchronized recompute.

## Step 5: Add structured diagnostics (dev-only/log-level controlled)

Add a concise one-line log on reset projection:

- `[reset-projection] run_id=... active_rows=... waiting_rows=... pending_rows=... done_rows=...`

Use this only to validate parity and keep it removable/guarded.

---

## Tests

## A) Store/monitor semantics tests

Add/extend tests in:

- `src/lib/flow/store/graphStore.monitorSemantics.test.ts`
- `src/lib/flow/components/runMonitorModel.test.ts`

Cases:

1. **Reset clears active rows**
	- start with active/waiting runtime rows
	- call reset
	- expect monitor `active=0`, `waiting=0` (unless explicit idle grouping behavior includes waiting placeholders)

2. **Reset parity with card status**
	- after reset, node projection status = `idle`
	- monitor row status/lifecycle not `running`/`waiting` from prior run residue

3. **No reset special-casing in monitor buckets**
	- assert groups derive from row projection only
	- no branch like `if reset then force empty` in bucketing layer

4. **Regression: prior-run terminal rows not reused as active**
	- simulate completed run then reset
	- ensure stale phase/blocker from prior run cannot repopulate Active bucket.

## B) Integration/UI-level test

Add/extend FlowCanvas-oriented test:

- run -> pause/resume optional -> reset
- assert rendered node footer = `idle`
- assert monitor “Active” section count = 0

Suggested location:

- new test adjacent to existing monitor parity/status projection tests

## C) Optional invariant test

Add runtime invariant test:

- `runStatus=idle` + no inflight + no leases + zero pending => projected active rows must be zero.

---

## Acceptance Criteria

- After reset, cards and monitor agree immediately (no stale Active rows).
- No reset-specific monitor UI patch branches added.
- Projection source remains canonical runtime state only.
- Existing monitor/status tests remain green.

---

## Suggested Commit Sequence

1. `refactor(runtime): normalize reset to canonical runtime idle transition`
2. `fix(monitor): derive post-reset rows from current runtime projection epoch`
3. `test(monitor): add reset parity regressions for active/waiting bucket cleanup`

If done in one pass:

- `fix(reset-monitor-parity): unify reset projection source and clear stale active rows`

