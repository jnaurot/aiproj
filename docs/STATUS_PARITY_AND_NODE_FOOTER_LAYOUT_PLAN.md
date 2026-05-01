# Status Parity + Node Footer Layout Plan

## Problem Statement

Two issues are visible at the same time:

1. **Node card footer layout bug**: lifecycle/status chips (e.g. `waiting`, `computed`) can collide with/right-shift port handle anchors.
2. **Status projection drift**: the Run Monitor can show `Model_ScoreJob` as `Done/TERMINAL` while the node card still shows `waiting`.

These indicate that node-card status projection and monitor status projection are not fully unified.

---

## Goals

1. Ensure node footer status occupies a dedicated row and never overlaps port handles.
2. Enforce a **single terminal truth contract** across UI surfaces:
	- if control-plane terminal truth is present, node card and monitor both render terminal status.
3. Prevent lifecycle/status/monitor drift with regression + integration coverage.

---

## Scope

Primary files likely involved:

- `src/lib/flow/components/nodes/**` (node card/footer UI)
- `src/lib/flow/components/runMonitorModel.ts`
- `src/lib/flow/store/statusModel.ts`
- `src/lib/flow/store/graphStore.ts` (if card status comes from a different projection path)
- Existing tests under:
	- `src/lib/flow/components/**.test.ts`
	- `src/lib/flow/store/**.test.ts`

---

## Implementation Steps

### Step 1 — Trace current projection paths (node card vs monitor)

- Identify exactly where node card lifecycle/status text is derived.
- Compare with monitor row derivation (`buildRunMonitorNodeRows`).
- Document any divergence points:
	- stale coercion path
	- old binding-to-display mapper
	- separate run-status reconciliation logic

**Exit criteria**: one explicit map of both call paths and divergence points.

---

### Step 2 — Create/route through canonical projection for node cards

- Reuse the same terminal-aware lifecycle reconciliation contract used by monitor.
- Ensure node-card path receives terminal signal (`terminalReasonCode`/`node_terminal` equivalent).
- Apply terminal precedence:
	- terminalized streaming nodes must not be re-coerced to `waiting` during active run.

**Exit criteria**: same runtime snapshot produces same lifecycle class for monitor row and node card.

---

### Step 3 — Normalize status mapping layer

- Ensure lifecycle -> display status mapping is centralized (single mapper).
- Remove or bypass any legacy local mapper in node card path that can disagree with monitor.
- Keep monitor grouping semantics unchanged, but ensure node-card status text and color derive from same final lifecycle/freshness projection.

**Exit criteria**: no duplicate business logic for terminal vs waiting projection.

---

### Step 4 — Fix node footer layout

- Refactor node footer to dedicated rows:
	- row A: lifecycle/freshness chips
	- row B: metrics (e.g. `ok:6/6`, consume mode)
- Guarantee right-side handle column has reserved space independent of footer text width.
- Add CSS constraints:
	- no overlap with handles
	- chip wrapping/truncation rules
	- consistent card min-height for variants

**Exit criteria**: status chips never push/overlap handle anchors at any common zoom/label lengths.

---

### Step 5 — Add regression tests for status parity

Add tests that assert parity across surfaces for same snapshot:

1. **Terminalized streaming node parity**
	- setup: `consume_mode=single_item`, no pending/inflight, terminal signal present.
	- assert:
		- monitor lifecycle = `completed` in Done group
		- node card status = `completed`
		- node card is not `waiting`

2. **Non-terminal streaming node compatibility**
	- setup: active run, streaming node between items, no terminal signal.
	- assert existing waiting semantics still hold.

3. **Blocker suppression under terminal truth**
	- setup: stale waiting blocker + terminal signal.
	- assert current blocker cleared (history may remain).

---

### Step 6 — Add UI integration test for footer layout

Add one focused UI-level test (or component render test) that:

- renders a node with long status/freshness text + right-side handles,
- verifies footer rows are rendered separately,
- verifies status row container exists and does not share the handle anchor row,
- verifies expected class hooks (`data-testid`/class) for dedicated status row.

If visual overlap assertion is hard in jsdom, enforce structural/layout invariants via DOM hierarchy + class contract.

---

### Step 7 — Add run-monitor + node-card integration test

Simulate runtime sequence:

1. streaming node processes final item,
2. terminal signal arrives (`node_terminal` / terminal reason),
3. run still active globally.

Assert:

- monitor row = `completed`, `phase=TERMINAL`, in Done bucket,
- node card status also `completed`,
- no `waiting` label on the node card.

---

### Step 8 — Verify with targeted test suite

Run at minimum:

- `npm run test -- src/lib/flow/store/statusModel.test.ts`
- `npm run test -- src/lib/flow/components/runMonitorModel.test.ts`
- node-card/component tests touched by footer/status changes
- new parity + integration tests added in this pass

Then run broader guard slice:

- `npm run test -- src/lib/flow/store`
- `npm run test -- src/lib/flow/components`

---

## Regression Matrix

- Terminal streaming node: card=completed, monitor=done
- Non-terminal streaming node: card/monitor waiting behavior preserved
- Failed terminal node: card/monitor both failed
- Stale completed (not terminalized this run): remains stale semantics, not forced terminal
- Footer layout: no chip/handle overlap

---

## Acceptance Criteria

1. A node cannot be `Done` in monitor and `waiting` on card for the same runtime snapshot.
2. Terminalized streaming nodes render terminal lifecycle on both surfaces.
3. Footer status line never collides with handle anchors.
4. Existing non-terminal streaming behavior remains intact.
5. New regression + integration tests pass and prevent re-drift.

---

## Suggested Commit Message

```text
fix(ui+monitor): unify terminal streaming status projection and stabilize node footer layout

- route node card status through terminal-aware lifecycle projection used by monitor
- enforce terminal precedence for streaming nodes during active runs
- centralize lifecycle->display mapping to prevent surface drift
- refactor node footer into dedicated status/metrics rows to avoid handle overlap
- add parity regressions and UI integration coverage for terminal streaming nodes
```

---

## Suggested PR Description

```text
This change fixes two linked UX issues:
1) node footer status chips could overlap/push handle anchors, and
2) node cards could show `waiting` while Run Monitor correctly showed `Done`.

We now use a unified terminal-aware projection contract across monitor and node cards,
so streaming nodes that have terminalized remain terminal on all surfaces.

Also includes node footer layout hardening and focused regression/integration tests
for parity and non-regression.
```
