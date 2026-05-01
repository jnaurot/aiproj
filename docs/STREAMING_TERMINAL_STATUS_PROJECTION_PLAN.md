# Streaming Terminal Status Projection Plan

## Problem Summary

Backend now correctly emits terminal truth for streaming nodes (example `Model_ScoreJob`):

- `Node finished (succeeded)`
- `[wait-reconcile] ... action=closed_synthesized`
- `[control] node_terminal ...`

But monitor/UI can still show `waiting` because frontend status reconciliation currently remaps streaming `completed -> waiting` while run is active.

This creates a truth mismatch:

- Control-plane says terminal
- Monitor/node badge says waiting

---

## Goal

When a streaming node (`consume_mode = single_item|batch`) has terminalized, its lifecycle/status must remain terminal (`completed`/`failed`/`canceled`/`skipped`) and must not be coerced back to `waiting` during active run.

---

## Root Cause

In `src/lib/flow/store/statusModel.ts`, `reconcileLifecycleForActiveRun(...)` has logic that intentionally maps:

- `completed -> waiting` for `single_item`/`batch`

without checking terminal control-plane truth (e.g., `node_terminal`, `terminalReasonCode`).

---

## Implementation Scope

Primary files:

- `src/lib/flow/store/statusModel.ts`
- `src/lib/flow/components/runMonitorModel.ts`
- `src/lib/flow/store/statusModel.test.ts`
- `src/lib/flow/components/runMonitorModel.test.ts`
- `src/lib/flow/components/runMonitorModel.displayReason.test.ts` (if needed)

Optional integration test file:

- `src/lib/flow/store/graphStore.*.test.ts` or existing monitor integration harness

---

## Implementation Steps

## Step 1: Add terminal-awareness input to lifecycle reconciliation

Extend `reconcileLifecycleForActiveRun(...)` input with an explicit terminal signal, e.g.:

- `isTerminalized?: boolean`
- or `terminalReasonCode?: string | null`

Rule:

- If node is terminalized, return the projected terminal lifecycle unchanged.
- Do not apply streaming `completed -> waiting` coercion.

Keep existing behavior unchanged for non-terminal streaming nodes.

---

## Step 2: Wire terminal signal from monitor projection

In `runMonitorModel.ts`, pass terminal indicator into reconciliation from control-plane node state:

- use `terminalReasonCode` (already read there)
- derive `isTerminalized = terminalReasonCode.length > 0`

Use this in `reconcileLifecycleForActiveRun(...)` call.

---

## Step 3: Block stale waiting blocker projection for terminalized streaming nodes

If lifecycle is terminalized:

- do not emit current `WAITING_REQUIRED_INPUT` as active blocker
- allow `lastBlocker`/history for diagnostics

This keeps monitor rows semantically consistent.

---

## Step 4: Keep compatibility for active streaming cycles

For streaming nodes that are **not terminalized**, preserve current behavior:

- may remain `waiting` between items while run active
- blocker/phase still reflect queue/gate state

---

## Regression Test Plan

## A) statusModel unit test: terminalized streaming node remains completed

Add test in `statusModel.test.ts`:

Scenario:

- lifecycle initially `completed`
- consume mode `single_item`
- run status `running`
- `isTerminalized=true`
- pending/inflight/readyWork all false/zero

Assert:

- result lifecycle stays `completed` (not `waiting`).

---

## B) statusModel unit test: non-terminal streaming node may still reconcile to waiting

Scenario:

- same as above but `isTerminalized=false`

Assert:

- current behavior preserved (`completed -> waiting`) where intended.

---

## C) runMonitorModel regression: node_terminal + streaming mode renders done

Add test in `runMonitorModel.test.ts`:

Scenario:

- node with `consumeMode=single_item`
- binding projects `completed`
- scheduler per-node shows no inflight/pending
- control-plane node state includes terminal reason

Assert:

- monitor row lifecycle/status is terminal (`completed`)
- phase is `TERMINAL`
- not listed in waiting group.

---

## D) runMonitor blocker regression: terminalized row does not show active waiting blocker

Scenario:

- terminalized streaming node with stale blocked reason present

Assert:

- active blocker is null/empty
- last blocker/history can remain populated.

---

## Integration Test Plan

## E2E projection test: wait-reconcile + node_terminal + active run

Add/extend a store/monitor integration test to simulate:

1. streaming node processes last item
2. wait-reconcile occurs
3. `node_terminal` emitted while run still active globally

Assert final UI projection:

- node displays `completed` (not `waiting`)
- monitor row appears under done/terminal section
- waiting count excludes this node.

---

## Acceptance Criteria

1. Streaming nodes with terminal control-plane truth are displayed as terminal, not waiting.
2. Non-terminal streaming behavior remains unchanged.
3. Active blocker text does not contradict terminal lifecycle.
4. New unit + monitor regression tests pass.
5. Integration projection test passes.

---

## Suggested Commit Message

```text
fix(monitor): honor terminal control-plane truth for streaming node lifecycle

- prevent streaming completed nodes from being coerced back to waiting once terminalized
- wire terminal signal into active-run lifecycle reconciliation
- suppress active waiting blocker on terminalized rows
- add statusModel + runMonitor regressions and projection integration coverage
```

---

## Suggested PR Description

```text
Fixes a monitor projection mismatch where streaming nodes could show `waiting`
after backend had already emitted terminal truth (`node_terminal`).

The lifecycle reconciler now respects terminal control-plane state and keeps
terminal rows terminal during active runs.

Includes focused regression and integration tests to lock the behavior.
```

