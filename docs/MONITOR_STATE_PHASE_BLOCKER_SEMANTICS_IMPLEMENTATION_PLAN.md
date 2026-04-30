# MONITOR_STATE_PHASE_BLOCKER_SEMANTICS_IMPLEMENTATION_PLAN.md

## Goal
Make Monitor node status diagnostics truthful and unambiguous by separating lifecycle status from execution phase and active blockers.

## Problem Summary
Current `reason` mixes multiple meanings (active blocker, historical blocker, terminal no-work reasons). This can show contradictory combinations like `status=running` with stale `MAX_INFLIGHT_REACHED`.

## Desired Outcome
1. Monitor displays `status`, `phase`, and `blocker` with clear semantics.
2. `blocker` reflects current blocker only (nullable).
3. `last_blocker` reflects most recent cleared blocker (updated on non-null -> null transition only).
4. Optional bounded blocker history supports debugging without full event-store complexity.
5. Existing node execution behavior remains unchanged; this is telemetry/modeling and UI truthfulness work.

## Explicit Design Decisions (Locked Before Implementation)
1. `queued` is display-derived in this rollout (not a scheduler enum change).
2. `LLM_HOLD` is represented as a phase, not a blocker code.
3. `phaseSince` is required for dwell-time observability.
4. Bounded blocker history uses named constant `BLOCKER_HISTORY_MAX = 10`.
5. `AWAITING_DISPATCH` may exist with `blocker=null` only as a transient handoff phase.
- If sustained beyond a short threshold, a blocker must be present (`LEASE_UNAVAILABLE` or `MAX_INFLIGHT_REACHED:*`), otherwise it is a telemetry bug.
6. Distinction rules are explicit:
- `MAX_INFLIGHT_REACHED:*` = capacity/semaphore cap gate.
- `LEASE_UNAVAILABLE` = fairness/ownership lock gate.
- `WAITING_REQUIRED_INPUT` = required handle input is not satisfied.
- `DEPENDENCY_NOT_READY` = non-handle prerequisite/dependency is not satisfied.

## Canonical Values
### Phase values
- `AWAITING_INPUT`
- `AWAITING_DISPATCH`
- `AWAITING_LEASE`
- `AWAITING_PROVIDER_RESPONSE`
- `POSTPROCESSING`
- `WRITING_OUTPUT`
- `TERMINAL`

### Blocker values (current-only)
- `MAX_INFLIGHT_REACHED:global`
- `MAX_INFLIGHT_REACHED:provider`
- `MAX_INFLIGHT_REACHED:model`
- `MAX_INFLIGHT_REACHED:node`
- `WAITING_REQUIRED_INPUT`
- `DEPENDENCY_NOT_READY`
- `LEASE_UNAVAILABLE`

---

## Phase 0 - Baseline + Contracts
**Commit message:** `test(monitor): lock baseline reason/blocked semantics before refactor`

### Implementation
1. Add/extend tests that capture current contradictions and define desired contract for monitor display model.
2. Introduce type-level contract tests (or compile-time assertions) for new monitor fields:
- `status` (existing lifecycle)
- `phase` (new)
- `phaseSince` (new)
- `blocker` (new current blocker)
- `lastBlocker` (new historical single snapshot)
3. Test intent in this phase is explicit:
- baseline invariants that are already correct should pass now.
- target contradiction-fix tests should be added as expected-fail (`.fails`/`todo`/gated) and made green in later phases.

### Integration tests
1. `INT-MON-BASE-01`: running node with active lease displays running + phase; no stale blocker in current blocker field.
2. `INT-MON-BASE-02`: waiting node displays blocker reason in blocker field.

### Regression tests
1. `REG-MON-BASE-01`: completed node never shows transient throttling blocker as current blocker.

### Exit criteria
- Baseline truthfulness contract captured in tests.
- expected-fail target tests are present and documented.

---

## Phase 1 - Runtime Telemetry Model Split (State vs Phase vs Blocker)
**Commit message:** `feat(monitor): split lifecycle status from phase and active blocker`

### Implementation
0. Add canonical compile-time types immediately (not deferred):
- `type PhaseCode = ...` (from canonical phase values)
- `type BlockerCode = ...` (from canonical blocker values)
1. Add monitor runtime fields in store/runtime model:
- `phase: PhaseCode | null`
- `phaseSince: string | null`
- `blocker: { code: BlockerCode; detail?: BlockerDetail; since?: string } | null`
- `lastBlocker: { code: BlockerCode; detail?: BlockerDetail; clearedAt?: string } | null`
- optional `blockerHistory: Array<{ code: string; at: string; action: 'set' | 'cleared' }>` (bounded ring with `BLOCKER_HISTORY_MAX`)
2. Populate fields from run events/state transitions without changing scheduling behavior.
3. Keep legacy `reason` temporarily as derived compatibility field (from blocker or terminal reason) to avoid breaking older UI paths.
4. Define `BlockerDetail` shape (structured diagnostic payload):
- `source?: 'global' | 'provider' | 'model' | 'node'`
- `limit?: number`
- `inflight?: number`
- `holderNodeId?: string`
- `provider?: string`
- `model?: string`
- `queueDepth?: number`

### Integration tests
1. `INT-MON-MODEL-01`: node transitions `waiting -> running` clear current blocker and set `lastBlocker`.
2. `INT-MON-MODEL-02`: node transitions through multiple blockers before dispatch; `lastBlocker` updates only on non-null -> null.
3. `INT-MON-MODEL-03`: optional blocker history ring caps size and preserves order.

### Regression tests
1. `REG-MON-MODEL-01`: model nodes with `max_inflight=2` can concurrently run without stale blocker shown as current.
2. `REG-MON-MODEL-02`: historical blocker remains visible in `lastBlocker` after completion.

### Exit criteria
- Store/runtime model has explicit fields with tested transition semantics.

---

## Phase 2 - Transition Hygiene Rules
**Commit message:** `fix(monitor): enforce blocker transition hygiene and last-blocker update semantics`

### Implementation
1. Enforce rule: update `lastBlocker` only when `blocker` transitions from non-null to null.
2. Prevent `lastBlocker` churn on repeated writes of same blocker.
3. On entering active execution phase, clear stale current blocker.
4. Preserve terminal reason separately for completed/failed diagnostics (do not overload blocker).
5. Update `phaseSince` only when `phase` value changes.

### Integration tests
1. `INT-MON-HYGIENE-01`: repeated `MAX_INFLIGHT_REACHED` writes do not overwrite `lastBlocker` until cleared.
2. `INT-MON-HYGIENE-02`: `AWAITING_LEASE` -> `AWAITING_PROVIDER_RESPONSE` clears/updates blocker semantics and snapshots `lastBlocker` exactly once when blocker clears.
3. `INT-MON-HYGIENE-03`: completed node carries terminal summary without current blocker.

### Regression tests
1. `REG-MON-HYGIENE-01`: running nodes never render stale current blocker after dispatch.
2. `REG-MON-HYGIENE-02`: mixed model/provider caps preserve accurate blocker source and clearing behavior.

### Exit criteria
- Blocker lifecycle semantics are deterministic and race-resistant.

---

## Phase 3 - Monitor UI Rendering + Backward Compatibility
**Commit message:** `feat(monitor-ui): render status phase blocker columns with compatibility fallback`

### Implementation
1. Update monitor table columns:
- `status` (existing)
- `phase` (new)
- `blocker` (current blocker only)
- `last blocker` (optional compact column/tooltip)
2. Keep legacy `reason` hidden or compatibility-only; remove ambiguity in visible labels.
3. Add tooltip text clarifying semantics:
- blocker = what's preventing progress right now
- last blocker = most recent blocker that cleared
4. Keep Node cards/status badges unchanged unless explicitly needed.
5. Add display-derived `queued` rendering for running-like nodes with active blocker.

### Integration tests
1. `INT-MON-UI-01`: running nodes show phase and empty blocker when healthy.
2. `INT-MON-UI-02`: waiting/queued nodes show blocker code.
3. `INT-MON-UI-03`: completed nodes show no current blocker and optional last-blocker tooltip.

### Regression tests
1. `REG-MON-UI-01`: existing monitor filters/sorts still work with new columns.
2. `REG-MON-UI-02`: no UI crash when older snapshots provide only legacy `reason`.

### Exit criteria
- Monitor is semantically clear and backward compatible.

---

## Phase 4 - Cap Source Attribution + Diagnostics Quality
**Commit message:** `feat(monitor): annotate blocker source for inflight/lease caps`

### Implementation
1. Normalize blocker code taxonomy:
- `MAX_INFLIGHT_REACHED:global`
- `MAX_INFLIGHT_REACHED:provider`
- `MAX_INFLIGHT_REACHED:model`
- `MAX_INFLIGHT_REACHED:node`
- `WAITING_REQUIRED_INPUT` etc.
2. Normalize LLM wait semantics into phase values (`AWAITING_LEASE`, `AWAITING_PROVIDER_RESPONSE`).
3. Ensure blocker source is propagated into monitor details.
4. Add minimal run-log correlation marker for blocker set/clear events and phase transitions.

### Integration tests
1. `INT-MON-CAP-01`: global cap hit displays `MAX_INFLIGHT_REACHED:global`.
2. `INT-MON-CAP-02`: tighter provider cap displays provider source while global has headroom.
3. `INT-MON-CAP-03`: run-log correlation marker is emitted on blocker set/clear and on phase transition.

### Regression tests
1. `REG-MON-CAP-01`: cap source attribution does not affect scheduler decisions.
2. `REG-MON-CAP-02`: existing log parsing remains stable with new blocker tags.

### Exit criteria
- Operator can identify which throttle knob is binding.
- Correlation markers are test-verified (blocker set, blocker clear, phase transition).

---

## Suggested File Touches
- `src/lib/flow/store/graphStore.run.ts`
- `src/lib/flow/store/graphStore.types.ts`
- `src/lib/flow/store/graphStore.ts` (derived/compat fields as needed)
- Monitor UI component(s) under `src/lib/flow/components/` (run monitor table rendering)
- Tests likely under:
- `src/lib/flow/store/graphStore.*.test.ts`
- monitor UI test file(s)
- preferred new suite: `src/lib/flow/store/graphStore.monitorSemantics.test.ts`

---

## Verification Command Set
Run at minimum:

```bash
npx vitest run \
  src/lib/flow/store/graphStore.schemaRunGuard.test.ts \
  src/lib/flow/store/graphStore.modelRunningLeaseInvariant.test.ts \
  src/lib/flow/store/graphStore.llmLease.concurrentHandoff.test.ts \
  src/lib/flow/store/graphStore.llmLeaseRunningInvariant.regression.test.ts \
  src/lib/flow/store/graphStore.runScope.test.ts
```

Plus any new monitor-specific suites introduced in phases above.

---

## Definition of Done
1. No monitor row shows contradictory state/blocker semantics.
2. `blocker` is current-only; `lastBlocker` updates only on non-null -> null transitions.
3. Running nodes show active phase truthfully even under high concurrency.
4. Completed nodes do not retain transient blocker as current reason.
5. Cap source attribution is visible and test-covered.
