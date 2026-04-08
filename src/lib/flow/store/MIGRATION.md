# Migration guide — graphStore.types.ts & graphStore.history.ts

## What was produced

| File | What it contains |
|---|---|
| `graphStore.types.ts` | Every type, interface, and shared constant from the god file |
| `graphStore.history.ts` | The full undo/redo stack as a self-contained factory |

---

## Step 1 — import from the types file

In `graphStore.ts`, replace the inline type declarations (lines 176–328 and
725–926) with a single barrel import:

```ts
// graphStore.ts  ← BEFORE (many inline type declarations)
type NodeOutputInfo = { … };
export type NodeExecutionError = { … };
export type GraphState = { … };
// …hundreds of lines…

// graphStore.ts  ← AFTER
export type {
  NodeOutputInfo,
  NodeExecutionError,
  NodeBindingInfo,
  NormalizedNodeBinding,
  RunSnapshotLike,
  AuditContext,
  RunLog,
  RunStatus,
  GraphLastRunStatus,
  EdgeExec,
  LogLevel,
  ApiEditorUiState,
  InspectorState,
  InspectorDraftAcceptValidation,
  InspectorDraftPatchIntent,
  SavePreflightSeverity,
  SavePreflightDiagnostic,
  SavePreflightResult,
  SaveConsistencyEntity,
  SaveConsistencyMismatch,
  EditorContext,
  ComponentEditSessionSnapshot,
  ComponentEditSession,
  SchemaCompatibility,
  EdgeCheck,
  EdgeInvalidReason,
  AdapterTransformKind,
  EdgeSchemaConstraint,
  EdgeSchemaDiagnostic,
  NodeSchemaContractEdge,
  NodeSchemaContractSnapshot,
  InputResolution,
  GraphState,
  QueueRuntime,
} from './graphStore.types';

export {
  RUN_IDLE,
  NODE_STATUS_IDLE,
  NODE_STATUS_SUCCEEDED,
  INITIAL_INSPECTOR,
} from './graphStore.types';
```

Then delete the two constants that referenced `NodeStatus` directly:

```ts
// DELETE these lines from graphStore.ts:
const IDLE: NodeStatus = 'idle';
const SUCCEEDED: NodeStatus = 'succeeded';
const initialInspector: InspectorState = { … };
const RUN_IDLE = "idle";

// REPLACE usages:
//   IDLE          → NODE_STATUS_IDLE  (or just the string 'idle')
//   SUCCEEDED     → NODE_STATUS_SUCCEEDED
//   initialInspector → INITIAL_INSPECTOR
```

`QueueRuntime` was previously an anonymous inline type inside `GraphState`.
It is now a named export so other modules can reference it without going
through `GraphState['queueRuntime']`.

---

## Step 2 — wire the history manager

The history manager is a factory — it holds no module-level state, so it is
safe to create once inside the store IIFE.

### 2a. Construction

```ts
// graphStore.ts — inside the IIFE, right after creating the writable store

import { createHistoryManager, runInHistoryTransaction } from './graphStore.history';

export const graphStore = (() => {
  const { subscribe, set, update: rawUpdate } = writable<GraphState>(initialState);

  // ── history ──────────────────────────────────────────────────────────
  const history = createHistoryManager({
    getState:          () => get({ subscribe } as any) as GraphState,
    applyDocument:     (graph, graphId) => {
                         // applyGraphDocument is defined later in this file;
                         // forward-reference is fine because this callback is
                         // only ever called at undo/redo time, not at init.
                         return applyGraphDocument(graph, graphId).ok;
                       },
    snapshotFromState: (s) => stripToDTO(s.nodes as any, s.edges as any, s.graphId),
  });

  // ── audited update ───────────────────────────────────────────────────
  const update = history.wrapUpdate(
    rawUpdate,
    auditStateTransition,           // unchanged — still defined in graphStore.ts
    (s) => stripToDTO(s.nodes as any, s.edges as any, s.graphId),
  );

  // … rest of the IIFE …
```

### 2b. Replace the hand-rolled history variables

**DELETE** these from inside the IIFE:

```ts
// DELETE:
const HISTORY_LIMIT_DEFAULT = 100;
let historyLimit = HISTORY_LIMIT_DEFAULT;
let historyPast: PipelineGraphDTO[] = [];
let historyFuture: PipelineGraphDTO[] = [];
let historyPresent: PipelineGraphDTO = stripToDTO(…);
let historyApplying = false;
let historyTransactionDepth = 0;
let historyTransactionStartKey: string | null = null;
function snapshotFromState(…) { … }
function snapshotKey(…) { … }
function resetHistoryToSnapshot(…) { … }
function pushHistorySnapshot(…) { … }
function beginHistoryTxn() { … }
function endHistoryTxn() { … }
function runInHistoryTransaction(…) { … }
function applyHistorySnapshot(…) { … }
```

### 2c. Update call sites inside the IIFE

| Old call | New call |
|---|---|
| `resetHistoryToSnapshot(snap)` | `history.resetToSnapshot(snap)` |
| `pushHistorySnapshot(snap)` | now called automatically by `wrapUpdate` — **remove manual calls** |
| `historyApplying` | `history.isApplying()` |
| `runInHistoryTransaction(() => { … })` | `runInHistoryTransaction(history, () => { … })` |

Specific locations:

**`applyGraphDocument`** (line ~6991):
```ts
// BEFORE:
if (!historyApplying) {
  resetHistoryToSnapshot(snapshotFromState(get(…) as GraphState));
}

// AFTER:
if (!history.isApplying()) {
  history.resetToSnapshot(stripToDTO(s.nodes as any, s.edges as any, s.graphId));
  // (use the state returned from the update callback so you don't need get())
}
```

**`hardResetGraph`** (line ~8743):
```ts
// BEFORE:
resetHistoryToSnapshot(snapshotFromState(next));

// AFTER:
history.resetToSnapshot(stripToDTO(next.nodes as any, next.edges as any, next.graphId));
```

**`setSourceKind`, `setLlmKind`, `setTransformKind`, `setToolProvider`** (lines ~7315, 7368, 7424, 7477):
```ts
// BEFORE:
return runInHistoryTransaction(() => { … });

// AFTER:
return runInHistoryTransaction(history, () => { … });
```

### 2d. Expose history actions in the returned object

```ts
  return {
    subscribe,
    // history
    ...history.actions,   // canUndo, canRedo, undo, redo, setHistoryLimit,
                          // clearHistory, beginHistoryTransaction, endHistoryTransaction
    // … all other actions …
  };
})();
```

This replaces the eight hand-written methods that were previously inlined in
the `return { … }` block (lines 7074–7118):

```ts
// DELETE from the return block:
canUndo() { … },
canRedo() { … },
setHistoryLimit(…) { … },
clearHistory() { … },
beginHistoryTransaction() { … },
endHistoryTransaction() { … },
undo() { … },
redo() { … },
```

---

## Step 3 — update other files that import from graphStore.ts

Because `graphStore.types.ts` re-exports everything with the same names, no
consumer outside the store directory needs to change their import paths.
`graphStore.ts` re-exports everything with `export type { … } from './graphStore.types'`,
so the public API surface is identical.

The only files that will need a touch are any that currently do:
```ts
import type { GraphState } from '$lib/flow/store/graphStore';
```
— those continue to work without change because `graphStore.ts` re-exports the
type.  No action needed.

---

## What is NOT in these two files (next steps)

After this PR lands:

| Next module | Key contents |
|---|---|
| `graphStore.audit.ts` | `auditStateTransition`, `auditSucceededRegressions`, `assertHydrationBindingInvariants` |
| `graphStore.node-schema.ts` | `canonicalizeNodeSchemas`, edge constraint / diagnostic computation |
| `graphStore.inspector.ts` | `patchInspectorDraft`, `applyInspectorDraft`, `updateNodeConfigImpl` |
| `graphStore.run.ts` | `attachActiveRunEventStream`, `applyRunEventState`, `hydrateFromRunSnapshot` |
| `graphStore.graph-edit.ts` | `addNode`, `deleteNode`, `addEdge`, `preflightConnection`, stale invalidation |
| `graphStore.persistence.ts` | `saveGraph`, `loadGraphRevision`, component session actions |
