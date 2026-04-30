# Edge Freshness Rendering Plan

## Problem Statement

Current edge coloring can show **green (completed)** for edges whose downstream node is actually **`completed (stale)`**.  
This creates conflicting UI truth:

- Node footer says stale (needs recompute)
- Edge says completed (looks current)

That is incorrect for execution/freshness semantics.

---

## Desired Behavior (Contract)

Edge visuals must represent **current run/freshness truth**, not historical success.

### Edge State Rules

1. **Running edge (blue dashed)**  
   - Show only when work-plane edge is actively executing in current run.

2. **Completed edge (green solid)**  
   - Show only when downstream consumption is **current/up-to-date**.
   - Do **not** show green for stale downstream state.

3. **Neutral edge (grey)**  
   - Show for idle, stale, not-yet-run, or otherwise not-current states.
   - Specifically: if downstream node display is `completed (stale)`, edge must be grey.

### Non-goal

Do not alter scheduler/runtime behavior. This is strictly a projection/rendering correctness fix.

---

## Implementation Scope

Focus on edge-state derivation and rendering inputs in frontend flow graph code.

Likely touch points:

- `src/lib/flow/FlowCanvas.svelte`
- `src/lib/flow/store/graphStore.run.ts` (only if edge runtime flags are currently over-latched)
- `src/lib/flow/store/graphStore.types.ts` (if a clearer edge view-state enum is needed)
- Any edge style helper/selectors used by canvas rendering

If edge color is currently derived from `computed/cached/last success` fields, switch to freshness-aware node/edge view state.

---

## Suggested Technical Approach

1. **Define a single edge-display-state selector**
   - Centralize decision into one pure function (or one store selector path).
   - Inputs should include:
     - edge runtime exec state (`active`/`done`/`idle`)
     - source/target node display/freshness state
     - run status context

2. **Prioritize active over historical**
   - If active now -> blue dashed
   - Else if target current-up-to-date complete -> green
   - Else -> grey

3. **Block stale from green**
   - Explicit guard:
     - target display status `completed (stale)` => never green
   - Equivalent guard for internal status forms (e.g. stale reason/upToDate=false)

4. **Keep plane separation intact**
   - Work-plane running styles only for work edges.
   - Param/control must never appear as work-running.

---

## Tests to Add/Update

Add focused tests that lock this contract.

## 1) Unit/selector tests (primary)

Create/extend tests around edge display-state derivation:

- **Case A:** target `completed` (current) => green
- **Case B:** target `completed (stale)` => grey
- **Case C:** target running => blue dashed
- **Case D:** previously completed but stale while sibling edges run => stale branch stays grey

Recommended location:

- `src/lib/flow/store/graphStore.*.test.ts` (where edge projection logic already tested)
  or
- dedicated edge display test near canvas logic.

## 2) Store integration test

Simulate fan-out from one source/component node to 4 downstream nodes:

- two running
- two stale-completed

Assert:

- running branches blue dashed
- stale branches grey (not green)

## 3) UI-level integration test (if infra exists)

Render canvas and assert class/style tokens:

- stale edges do not include completed/green class
- running edges include running class

---

## Regression Risks

1. Accidentally removing green for truly up-to-date completed nodes.
2. Using node historical status instead of freshness status.
3. Reintroducing dual truth by keeping multiple edge-style pathways.

Mitigation:

- Single selector source of truth.
- Snapshot/assertions for both completed and completed(stale).

---

## Acceptance Criteria

1. A downstream node with display `completed (stale)` never causes incoming edge to render green.
2. Running edges still render blue dashed correctly.
3. Fully current completed edges still render green.
4. Fan-out mixed-state scenario renders each branch correctly and independently.
5. Added regression tests pass.

---

## Suggested Commit Message

```text
fix(flow-ui): render stale downstream edges as neutral instead of completed

- make edge color derive from current freshness state (not historical success)
- prevent `completed (stale)` downstream nodes from rendering green edges
- keep running work edges dashed blue
- add regression tests for mixed fan-out (running + stale branches)
```

---

## Suggested PR Description (optional)

```text
This fixes a UI truth mismatch where edges could render as completed (green)
even when the downstream node was `completed (stale)`.

Edge rendering now uses freshness-aware status:
- running => blue dashed
- completed current => green
- stale/idle/not-current => grey

Includes regression tests to lock mixed fan-out behavior.
```
