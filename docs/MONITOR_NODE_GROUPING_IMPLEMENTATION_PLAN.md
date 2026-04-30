# MONITOR_NODE_GROUPING_IMPLEMENTATION_PLAN.md

## Goal
Replace the flat, depth-sorted node table in the Monitor with four semantically meaningful
groups so operators can immediately distinguish what is active, what is waiting, what has
not started, and what has finished — without scanning every row.

## Problem Summary
The current flat list sorted by `depth_desc` puts completed nodes at the top and buries
active nodes in the middle. In a 20-node pipeline where 15 nodes are done, the 15 rows
you don't need to read appear before the 5 you do. Signal-to-noise degrades linearly with
pipeline size.

Additionally, `running` and `waiting` nodes are visually identical in a flat list but
require completely different operator responses: running nodes warrant latency observation,
waiting nodes warrant dependency investigation.

## Desired Outcome
1. Four groups rendered in priority order: **Active → Waiting → Pending → Done**.
2. Each group header carries an aggregate count summary (e.g. `Active (3)  2 running · 1 throttled`).
3. Active and Waiting are expanded by default; Pending and Done are collapsed by default.
4. Failed nodes live in Done but are visually prominent; their count is surfaced in the
   Done header even when collapsed (`Done (8)  7 completed · 1 failed ⚠`).
5. Nodes animate between groups in real time as lifecycle state changes.
6. Existing filter/sort controls operate within each group (not across groups).
7. A fifth **Needs Attention** group is reserved as a future option for complex failure
   triage; it is not implemented in this rollout.

## Explicit Design Decisions (Locked Before Implementation)
1. **Group boundaries are model-layer concerns**, not UI concerns.  
   `classifyNodeToGroup` is a pure function in `runMonitorModel.ts`; the UI consumes
   `MonitorGroupedNodes` and does not re-classify rows.
2. **Failed nodes stay in Done** in this rollout. They are visually distinguished within
   Done via a `failedCount` on the group and per-row `isFailed` flag.
3. **Paused nodes go in Waiting**, not Active. A paused node is not making progress and
   may need operator attention; it is not executing.
4. **Canceled nodes go in Done** alongside completed. They are terminal and no longer
   actionable.
5. **`filterAndSortRunMonitorNodes` is preserved unchanged.** Grouping wraps it — the
   existing sort/filter logic is applied per-group after classification.
6. **Collapsibility is UI-only state** — it does not affect the model output.
7. **Group order is fixed**: Active → Waiting → Pending → Done. No user reordering in
   this rollout.
8. **Stale nodes** (`lifecycle === 'stale'`) go in Pending. They have been seen before
   but are not currently executing and have no active inflight work.

## Canonical Group Taxonomy

| Group    | Lifecycle values included                                    | Phase hints                                                       | Notes                              |
|----------|--------------------------------------------------------------|-------------------------------------------------------------------|------------------------------------|
| active   | `running`, `active`                                          | `AWAITING_DISPATCH`, `AWAITING_LEASE`, `AWAITING_PROVIDER_RESPONSE`, `POSTPROCESSING`, `WRITING_OUTPUT` | Also: `inflight > 0`, `isLlmHolder`, `isLlmWaiting` regardless of lifecycle |
| waiting  | `waiting`, `blocked`, `paused`                               | `AWAITING_INPUT`                                                  | Also: `isBlocked`, `isWaiting` when not already in active |
| pending  | `idle`, `stale`                                              | `null` (executor has not touched this node)                       |                                    |
| done     | `completed`, `failed`, `canceled`, `skipped`                 | `TERMINAL`                                                        | Failed rows flagged within group   |

**Classification priority** (first match wins):
1. Terminal lifecycle (`completed`, `failed`, `canceled`, `skipped`) → **done**
2. `isLlmHolder` OR `isLlmWaiting` OR `inflight > 0` → **active**
3. Lifecycle `running` or `active` → **active**
4. Phase in `{AWAITING_DISPATCH, AWAITING_LEASE, AWAITING_PROVIDER_RESPONSE, POSTPROCESSING, WRITING_OUTPUT}` → **active**
5. Lifecycle `waiting`, `blocked`, or `paused` → **waiting**
6. `isBlocked` OR `isWaiting` → **waiting**
7. Phase `AWAITING_INPUT` → **waiting**
8. Fallthrough → **pending**

---

## New Types (added to `runMonitorModel.ts`)

```typescript
export type MonitorGroupKey = 'active' | 'waiting' | 'pending' | 'done';

export type MonitorNodeGroup = {
  key: MonitorGroupKey;
  label: string;             // Human label: 'Active', 'Waiting', 'Pending', 'Done'
  rows: RunMonitorNodeRow[];
  totalCount: number;
  // Active group breakdown
  runningCount: number;      // inflight > 0 or isLlmHolder
  throttledCount: number;    // phase === AWAITING_DISPATCH with active blocker
  // Done group breakdown
  completedCount: number;
  failedCount: number;
  canceledCount: number;
};

export type MonitorGroupedNodes = {
  groups: MonitorNodeGroup[];        // always length 4, in fixed order
  totalNodeCount: number;
  hasFailures: boolean;              // any group has failedCount > 0
  activeGroupIndex: number;          // index of 'active' group (0)
  waitingGroupIndex: number;         // index of 'waiting' group (1)
  pendingGroupIndex: number;         // index of 'pending' group (2)
  doneGroupIndex: number;            // index of 'done' group (3)
};
```

---

## Phase 0 — Baseline + Contracts
**Commit message:** `test(monitor-groups): lock flat-list baseline and define grouping contracts`

### Implementation
1. Write contract tests that assert current flat-list behaviour before any model change.
2. Add expected-fail tests (`.todo` / gated) for the target grouped behaviour so they go
   red now and green after Phase 1.
3. Verify `filterAndSortRunMonitorNodes` passes all existing tests with no changes.

### Integration tests
- `INT-GRP-BASE-01`: flat `filterAndSortRunMonitorNodes` returns all nodes when filter is
  `'all'`; no grouping is applied.
- `INT-GRP-BASE-02`: a mix of running/waiting/idle/completed nodes returns a flat list in
  depth order.

### Regression tests
- `REG-GRP-BASE-01`: existing filter values (`blocked`, `waiting`, `stalled`) still
  produce correct subsets with no model changes.
- `REG-GRP-BASE-02`: existing sort values (`depth_desc`, `label_asc`, etc.) preserve
  stable output.

### Exit criteria
- All existing monitor model tests pass.
- Expected-fail grouping target tests are present and documented.

---

## Phase 1 — Classification Model
**Commit message:** `feat(monitor-groups): add classifyNodeToGroup and groupMonitorNodeRows`

### Implementation
1. Add `classifyNodeToGroup(row: RunMonitorNodeRow): MonitorGroupKey` to
   `runMonitorModel.ts` using the priority rules from the taxonomy above.
2. Add `groupMonitorNodeRows(rows: RunMonitorNodeRow[], filter: RunMonitorFilter, sort: RunMonitorSort, globalStalled: boolean): MonitorGroupedNodes`.
   - Classifies each row.
   - Calls the existing `filterAndSortRunMonitorNodes` per group (not across groups).
   - Builds `MonitorNodeGroup` aggregates from classified rows.
   - Returns `MonitorGroupedNodes` with all four groups present (empty groups have
     `totalCount: 0` and `rows: []`).
3. Every node is assigned to exactly one group (invariant enforced in tests).

### Integration tests
- `INT-GRP-MODEL-01`: node with `lifecycle='running'` classifies to **active**.
- `INT-GRP-MODEL-02`: node with `lifecycle='waiting'` classifies to **waiting**.
- `INT-GRP-MODEL-03`: node with `lifecycle='idle'` classifies to **pending**.
- `INT-GRP-MODEL-04`: node with `lifecycle='completed'` classifies to **done**.
- `INT-GRP-MODEL-05`: node with `lifecycle='failed'` classifies to **done** with
  `failedCount` incremented on that group.
- `INT-GRP-MODEL-06`: node with `lifecycle='canceled'` classifies to **done**.
- `INT-GRP-MODEL-07`: node with `isLlmHolder=true` and `lifecycle='waiting'` classifies
  to **active** (priority rule 2 fires before rule 5).
- `INT-GRP-MODEL-08`: node with `lifecycle='paused'` classifies to **waiting**, not
  **active**.
- `INT-GRP-MODEL-09`: node with `lifecycle='stale'` classifies to **pending**.
- `INT-GRP-MODEL-10`: `groupMonitorNodeRows` returns exactly four groups in the order
  active → waiting → pending → done regardless of input composition.
- `INT-GRP-MODEL-11`: sum of `totalCount` across all groups equals total input row count
  (no duplicates, no orphans).
- `INT-GRP-MODEL-12`: `hasFailures` is `true` when any node has `lifecycle='failed'`,
  `false` otherwise.
- `INT-GRP-MODEL-13`: `completedCount + failedCount + canceledCount` equals Done
  group `totalCount`.
- `INT-GRP-MODEL-14`: `runningCount` in Active group counts nodes where
  `inflight > 0 || isLlmHolder`.
- `INT-GRP-MODEL-15`: `throttledCount` in Active group counts nodes where
  `phase === 'AWAITING_DISPATCH'` and `blocker !== null`.

### Regression tests
- `REG-GRP-MODEL-01`: classification is pure and deterministic — same input row always
  produces same group key.
- `REG-GRP-MODEL-02`: `filterAndSortRunMonitorNodes` called per-group produces the same
  result as calling it on a pre-filtered slice of the same rows.
- `REG-GRP-MODEL-03`: empty node list returns four groups all with `totalCount: 0` and
  `hasFailures: false`.

### Exit criteria
- `classifyNodeToGroup` and `groupMonitorNodeRows` are exported and fully tested.
- No existing monitor tests broken.

---

## Phase 2 — Group Aggregates + Header Summaries
**Commit message:** `feat(monitor-groups): add per-group aggregate counts for header display`

### Implementation
1. Verify `MonitorNodeGroup` fields `runningCount`, `throttledCount`, `completedCount`,
   `failedCount`, `canceledCount` are correctly populated in `groupMonitorNodeRows`.
2. Add `headerSummary(group: MonitorNodeGroup): string` helper that produces the compact
   label string shown in each group header:
   - Active: `"2 running · 1 throttled"` (omit zero counts)
   - Waiting: `"3 waiting"` or `"2 waiting · 1 paused"`
   - Pending: `"4 not yet started"`
   - Done: `"7 completed · 1 failed ⚠"` (⚠ only when failedCount > 0)
3. Failed count surfaces in Done header even when group is collapsed.

### Integration tests
- `INT-GRP-AGG-01`: `headerSummary` for Active with 2 running, 1 throttled returns
  `"2 running · 1 throttled"`.
- `INT-GRP-AGG-02`: `headerSummary` for Active with 0 throttled omits the throttled
  segment entirely.
- `INT-GRP-AGG-03`: `headerSummary` for Done with 0 failures omits the `⚠` marker.
- `INT-GRP-AGG-04`: `headerSummary` for Done with 1 failure includes `⚠` marker.
- `INT-GRP-AGG-05`: `headerSummary` for empty group returns `"none"` or `"0"` (not
  blank — blank would be ambiguous).

### Regression tests
- `REG-GRP-AGG-01`: `headerSummary` never returns the literal string `"-"` or an empty
  string for any non-empty group.

### Exit criteria
- Header summary strings are deterministic and test-covered.
- Done group always surfaces failed count regardless of collapsed state.

---

## Phase 3 — Monitor UI Rendering
**Commit message:** `feat(monitor-ui): render nodes in four collapsible groups`

### Implementation
1. Replace the flat `<table>` / `<tbody>` node list in the Monitor component with four
   `<section>` elements, one per group, in fixed order.
2. Each section has a `<header>` row showing: group label, total count badge,
   `headerSummary` string, collapse toggle.
3. Default collapsed state:
   - Active: **expanded**
   - Waiting: **expanded**
   - Pending: **collapsed**
   - Done: **collapsed**
4. Collapsed state is local UI state (not persisted across page reloads in this phase).
5. When a group is collapsed, only the header row is rendered; the tbody is not mounted
   (not just hidden) to avoid rendering large done lists unnecessarily.
6. Failed rows in Done group: rendered with a red left border / background tint
   and a `⚠` prefix on the node label. Not moved to a separate sub-table.
7. Within each group the existing `NodeFilter` and `NodeSort` controls remain unchanged
   and apply to that group's rows only.
8. When all four groups are empty (no nodes), render a single "No nodes" placeholder.
9. Backward compatibility: if `groupMonitorNodeRows` is unavailable (e.g. older data
   snapshot), fall back to existing flat `filterAndSortRunMonitorNodes` output.

### Integration tests
- `INT-GRP-UI-01`: Active group section is visible and expanded on initial render.
- `INT-GRP-UI-02`: Done group section header is visible but tbody is not mounted when
  collapsed.
- `INT-GRP-UI-03`: clicking a collapsed group header expands that group.
- `INT-GRP-UI-04`: Done group header shows `⚠` and failed count when `failedCount > 0`,
  even when collapsed.
- `INT-GRP-UI-05`: failed node row in Done group has distinct visual treatment (red
  marker class present in DOM).
- `INT-GRP-UI-06`: empty groups render their header with count `0` and no rows.
- `INT-GRP-UI-07`: filter `'blocked'` applied within Active group filters only Active
  rows, does not remove rows from other groups.
- `INT-GRP-UI-08`: sort `'depth_desc'` applied within Waiting group sorts only
  Waiting rows.

### Regression tests
- `REG-GRP-UI-01`: existing per-row column values (status, phase, blocker, processed,
  pending, depth) are unchanged by grouping.
- `REG-GRP-UI-02`: existing edge table below node table is unaffected.
- `REG-GRP-UI-03`: no UI crash when a node transitions group mid-render (reactive
  re-classification must not throw).
- `REG-GRP-UI-04`: no UI crash when older snapshots lack `phase`/`blocker` fields
  (backward compat fallback renders flat list cleanly).

### Exit criteria
- Four groups render with correct default collapsed states.
- Failed node visual treatment present and tested.
- Backward compat fallback confirmed working.

---

## Phase 4 — Real-Time Group Transitions
**Commit message:** `feat(monitor-groups): live group reclassification on node lifecycle change`

### Implementation
1. Group classification is reactive: when a `RunMonitorNodeRow`'s `lifecycle`, `phase`,
   `inflight`, `isLlmHolder`, or `isLlmWaiting` fields change, the row is reclassified
   and moves to the correct group.
2. Because `groupMonitorNodeRows` is a pure function called on the full `rows` array,
   reactivity is free — any store update that rebuilds `rows` will rebuild the groups.
   Verify this is the case; no manual group membership cache should exist.
3. Add a CSS transition on group count badges so they animate (fade/count-up) when a
   node enters or leaves a group, making the movement visible.
4. When the last node leaves Active, Active group auto-collapses if Done has nodes
   (i.e. run has finished). This avoids an empty Active header dominating the view
   post-completion.
5. When a run completes, Done group auto-expands if it was collapsed.

### Integration tests
- `INT-GRP-TRANS-01`: row reclassified from **active** to **done** when `lifecycle`
  changes from `running` to `completed`.
- `INT-GRP-TRANS-02`: row reclassified from **waiting** to **active** when `inflight`
  changes from `0` to `1`.
- `INT-GRP-TRANS-03`: row reclassified from **active** to **done** when `lifecycle`
  changes from `running` to `failed`; `failedCount` increments in Done group.
- `INT-GRP-TRANS-04`: node that hits LLM cap (`isLlmWaiting=true`) while in waiting
  moves to active; on dispatch, stays in active with phase `AWAITING_PROVIDER_RESPONSE`.
- `INT-GRP-TRANS-05`: `groupMonitorNodeRows` called twice with identical input produces
  identical output (idempotent, no side effects from transition logic).

### Regression tests
- `REG-GRP-TRANS-01`: concurrent model nodes with `max_inflight=2` both appear in
  Active simultaneously with correct counts.
- `REG-GRP-TRANS-02`: a node that transiently hit `MAX_INFLIGHT_REACHED` and then
  dispatched is in Active (not Waiting), matching the phase/blocker semantics from
  `MONITOR_STATE_PHASE_BLOCKER_SEMANTICS_IMPLEMENTATION_PLAN.md`.

### Exit criteria
- Group membership is always consistent with the current row state.
- Auto-collapse / auto-expand on run completion works correctly.

---

## Phase 5 — Needs Attention Group (Future, Not Implemented Now)
**Reserved design notes only — no implementation in this plan.**

When pipeline failure modes become complex enough to warrant independent triage:
- A fifth group `'needs_attention'` is inserted at position 0 (above Active).
- Membership: `lifecycle === 'failed'` rows are promoted from Done to this group.
- The group is always expanded.
- `MonitorGroupKey` gains `'needs_attention'` as a valid value.
- `classifyNodeToGroup` checks `lifecycle === 'failed'` before terminal check and
  returns `'needs_attention'`.
- The Done group `failedCount` drops to 0; the Done header `⚠` marker is removed.

This is a non-breaking additive change once Phase 1–4 are in place — the type extension
and re-classification rule are the only model changes required.

---

## Suggested File Touches

| File | Change |
|------|--------|
| `src/lib/flow/components/runMonitorModel.ts` | Add `MonitorGroupKey`, `MonitorNodeGroup`, `MonitorGroupedNodes`, `classifyNodeToGroup`, `groupMonitorNodeRows`, `headerSummary` |
| `src/lib/flow/components/runMonitorModel.test.ts` | New `describe` blocks for Phase 1–2 tests |
| Monitor UI component(s) under `src/lib/flow/components/` | Replace flat node table with grouped sections |
| Monitor UI test file(s) | Phase 3–4 UI tests |
| `src/lib/flow/components/runMonitorModel.transitions.test.ts` | Phase 4 transition tests |

No changes to:
- `graphStore.run.ts` — grouping is a projection concern, not a store concern
- `graphStore.types.ts` — no new store fields required
- `statusModel.ts` — lifecycle values are consumed as-is

---

## Verification Command Set

```bash
npx vitest run \
  src/lib/flow/components/runMonitorModel.test.ts \
  src/lib/flow/components/runMonitorModel.transitions.test.ts \
  src/lib/flow/components/runMonitorModel.phaseBlocker.contract.test.ts \
  src/lib/flow/components/runMonitorModel.displayReason.test.ts \
  src/lib/flow/store/graphStore.runMonitorHistory.test.ts \
  src/lib/flow/store/graphStore.llmLease.concurrentHandoff.test.ts \
  src/lib/flow/store/graphStore.modelRunningLeaseInvariant.test.ts
```

Plus any new monitor-grouping suites introduced in phases above.

---

## Definition of Done
1. Four groups render in fixed order: Active → Waiting → Pending → Done.
2. Every node appears in exactly one group; no node is duplicated or orphaned.
3. `classifyNodeToGroup` priority rules are exhaustive and covered by tests.
4. Done group header surfaces failed count even when collapsed.
5. Failed node rows have distinct visual treatment within Done group.
6. Active and Waiting are expanded by default; Pending and Done are collapsed.
7. Existing filter/sort controls operate correctly within each group.
8. Backward compat fallback confirmed for snapshots without `phase`/`blocker` fields.
9. `REG-GRP-TRANS-02` passes: LLM-cap-delayed node that dispatched appears in Active,
   not Waiting, consistent with phase/blocker semantics plan.
