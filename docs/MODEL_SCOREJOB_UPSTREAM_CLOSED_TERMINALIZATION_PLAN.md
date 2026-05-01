# Model_ScoreJob Upstream-Closed Terminalization Plan

## Problem Summary

Observed run diagnostics show nodes (for example `Model_ScoreJob`) entering:

- `WAITING_REQUIRED_INPUT`
- `queued_any=0`
- `inflight=0`
- `pending_input=0`
- `all_upstream_closed=0` with `upstream_total=1`

This indicates the node is not waiting for work; it is waiting for the required `upstream_closed` control-plane state to be set on its incoming required work edge.

---

## Goal

Ensure streaming nodes (`consume_mode=single_item` / `batch`) reliably transition to terminal (`Done`) once:

1. No queued work remains
2. No inflight work remains
3. No active lease remains
4. Required upstream work edges are closed

And prevent permanent waiting when closure is logically complete but closure signal/state is missing.

---

## Scope

Primary files:

- `backend/app/runner/run.py`
- `backend/app/runner/control_plane.py` (if invariant helper updates are needed)
- `backend/tests/runner/*` (targeted regression tests)
- `backend/tests/integration/*` (end-to-end behavior coverage)

Non-goal:

- No frontend projection behavior changes in this pass (unless a backend truth fix requires monitor expectation updates).

---

## Implementation Steps

## Step 1: Audit and lock closure emission points

Verify every successful upstream completion path emits `upstream_closed` for outbound required work edges exactly once.

Checklist:

- Success path after item processing loop
- Soft-fail skip path (`on_error=skip_failed`)
- End-of-run closure sweep path
- Component-expanded graph path (if edge IDs are remapped)

Add temporary assertion/log guard (debug-only) if needed:

- If producer is terminal and edge depth is 0 but edge not closed, emit a trace diagnostic marker.

---

## Step 2: Add deterministic closure reconciliation for waiting nodes

When a node is blocked with `WAITING_REQUIRED_INPUT`, reconcile closure state for its required incoming work edges.

Safe reconciliation policy:

- If incoming required work edge has:
  - producer terminal
  - queue depth 0
  - no further producer inflight
- then synthesize/apply control-plane closure state for that edge (`UPSTREAM_CLOSED` equivalent state update).

Immediately re-run terminalization check after reconciliation:

- call `_maybe_emit_node_terminal(node_id, "completed")` (or existing reason path)

Guardrails:

- Only apply to required work edges for streaming consumers.
- Do not synthesize closure when producer is not terminal.
- Keep idempotent behavior (repeat reconciliation should be no-op).

---

## Step 3: Ensure blocked state cannot outlive terminal truth

After terminalization is emitted:

- clear blocked state for node (`_clear_node_blocked`)
- ensure waiting transition marker state does not re-open without new work/transition.

This prevents stale `WAITING_REQUIRED_INPUT` from persisting after terminal truth.

---

## Step 4: Preserve observability and add focused trace keys

Keep the new one-line wait diagnostic and extend only if required:

- existing: `[wait-check] ...`
- add optional closure reconciliation line (single-line searchable), e.g.:
  - `[wait-reconcile] node=... edge=... action=closed_synthesized producer_terminal=1 depth=0`

Make it easy to remove:

- one helper formatter
- one call site cluster

---

## Regression and Integration Test Plan

## A) Runner regression: waiting with empty queue + missing closure resolves to terminal

Create/extend test in `backend/tests/runner`:

Scenario:

- streaming consumer (`single_item`)
- all items consumed
- incoming depth 0
- producer terminal
- simulate missing closure state before reconciliation point

Assert:

- reconciliation path marks edge closed
- `node_terminal` emitted
- node not left in `WAITING_REQUIRED_INPUT`

---

## B) Runner regression: do not synthesize closure while producer still active

Scenario:

- consumer waiting
- depth 0 temporarily
- producer not terminal / may emit more

Assert:

- no synthesized closure
- node remains waiting

---

## C) Soft-fail skip path regression

Scenario:

- per-item model failures with `on_error=skip_failed`
- final item processed/failed/accepted mix

Assert:

- `upstream_closed` eventually true for required incoming edge
- terminalization still occurs
- no permanent waiting with `queued_any=0 inflight=0 pending_input=0`

---

## D) Integration test: end-to-end “all items processed then done”

Create/extend integration test with realistic graph:

- upstream producer -> streaming model node (`single_item`)
- include retries/timeouts and possible mixed accept/reject

Assert from emitted events:

- final control-plane state shows required incoming edge closed
- `node_terminal` for target node
- no terminal snapshot with active blocker `WAITING_REQUIRED_INPUT` for that node

---

## E) Diagnostics regression

Ensure wait diagnostics remain stable/searchable:

- `[wait-check]` appears on waiting transition
- if reconciliation added: `[wait-reconcile]` appears when closure synthesized

---

## Acceptance Criteria

1. A streaming node with no queued work, no inflight, and terminal upstream does not remain permanently waiting.
2. Required incoming work edge closure is guaranteed (native emit or safe reconciliation).
3. `node_terminal` is emitted for the downstream node in the above condition.
4. No regression for active-producer waiting behavior.
5. New/updated runner + integration tests pass.

---

## Suggested Commit Message

```text
fix(runner): reconcile missing upstream closure to prevent terminal waiting stalls

- ensure required incoming work edges close deterministically for streaming consumers
- reconcile closure state for terminal upstream + drained edge conditions
- re-run terminalization after reconciliation and clear stale blocked state
- add runner/integration regressions for WAITING_REQUIRED_INPUT terminal-stall cases
```

---

## Suggested PR Notes

```text
This fixes a control-plane terminalization stall where streaming nodes could remain
in WAITING_REQUIRED_INPUT despite queue drained and no inflight work.

Root cause: required incoming edge closure could be missing on certain paths, leaving
can_node_terminalize false.

Fix: deterministic closure reconciliation + immediate terminalization recheck, with
regression coverage for success and soft-fail skip flows.
```

