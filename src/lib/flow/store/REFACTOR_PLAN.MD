# graphStore Refactor Plan
# Breaking up the god file into focused modules

## Context

`src/lib/flow/store/graphStore.ts` is ~10,500 lines.  Some extraction has
already happened before this refactor began:

| Already extracted | Contents |
|---|---|
| `graphStore.utils.ts` | IO derivation helpers (`deriveNodeIoForData`, etc.) |
| `graphStore.schema.utils.ts` | Schema normalisation utilities |
| `graphStore.bindings.ts` | Binding pair logic (`normalizeBinding`, `withPair`, etc.) |
| `graphStore.components.ts` | Component-specific helpers |

Everything below is what remains in the god file and needs to move.

---

## End-state module map

```
src/lib/flow/store/
│
├── graphStore.types.ts          ← [DONE] All types, interfaces, shared constants
├── graphStore.history.ts        ← [DONE] Undo/redo stack (factory)
│
├── graphStore.audit.ts          ← [NEXT] State transition auditing
├── graphStore.node-schema.ts    ← [NEXT] Node schema canonicalisation + edge constraints
│
├── graphStore.inspector.ts      ← Inspector / node-config editing
├── graphStore.run.ts            ← Run lifecycle (SSE, hydration, event handling)
├── graphStore.graph-edit.ts     ← Canvas mutations (add/delete/sync nodes & edges)
├── graphStore.persistence.ts    ← Save/load API calls, component session actions
│
├── graphStore.ts                ← Orchestrator (target: ~200–300 lines)
│
│   --- already extracted, do not touch ---
├── graphStore.utils.ts
├── graphStore.schema.utils.ts
├── graphStore.bindings.ts
└── graphStore.components.ts
```

---

## Dependency direction (no circular imports allowed)

```
graphStore.types
       ↑
       │  (imports types only)
       │
graphStore.bindings  graphStore.schema.utils  graphStore.utils  graphStore.components
       ↑                      ↑                      ↑                  ↑
       └──────────────────────┴──────────────────────┴──────────────────┘
                                      ↑
                    graphStore.audit  graphStore.node-schema
                                      ↑
                    graphStore.inspector   graphStore.history
                                      ↑
                         graphStore.run   graphStore.graph-edit
                                      ↑
                              graphStore.persistence
                                      ↑
                               graphStore.ts  (orchestrator)
```

Rule: arrows point upward only.  A module may only import from modules
above it in the diagram.  `graphStore.ts` is the only file that imports
from everything.

---

## Module-by-module specification

### ✅ 1. `graphStore.types.ts`  — DONE

**What it contains**
- Every type and interface that was previously scattered through the god file
- 42 named exports (types + constants)
- `QueueRuntime` broken out as a named type (was anonymous inline in `GraphState`)
- `NODE_STATUS_IDLE`, `NODE_STATUS_SUCCEEDED`, `INITIAL_INSPECTOR`, `RUN_IDLE`

**What it does NOT contain**
- Any runtime logic
- Any Svelte store imports

**Key exports**
`GraphState`, `AuditContext`, `RunStatus`, `RunLog`, `NodeBindingInfo`,
`NormalizedNodeBinding`, `NodeOutputInfo`, `NodeExecutionError`,
`InspectorState`, `ApiEditorUiState`, `ComponentEditSession`,
`EdgeSchemaConstraint`, `EdgeSchemaDiagnostic`, `NodeSchemaContractEdge`,
`NodeSchemaContractSnapshot`, `InputResolution`, `SavePreflightResult`,
`QueueRuntime`, and ~20 more.

**Integration into `graphStore.ts`**
See `MIGRATION.md` Step 1.

---

### ✅ 2. `graphStore.history.ts`  — DONE

**What it contains**
- `createHistoryManager(deps)` factory — all stack state lives in the closure
- `HistoryManager` and `HistoryActions` types
- `runInHistoryTransaction(manager, fn)` free function

**Dependencies injected at construction (not imported)**
- `getState()` — read current store state
- `applyDocument(graph, graphId)` — owned by graph-edit, injected here
- `snapshotFromState(state)` — converts state to a DTO

**Key design decisions**
- No module-level mutable state (safe to construct multiple times in tests)
- `wrapUpdate()` produces the store's audited `update` function rather than
  monkey-patching Svelte's rawUpdate
- `isApplying()` guard lets `applyGraphDocument` suppress its own stack push
  during undo/redo restoration

**Integration into `graphStore.ts`**
See `MIGRATION.md` Step 2.

---

### ⬜ 3. `graphStore.audit.ts`  — TODO

**What it contains**
Pure functions that verify state transitions are legal.  No side effects
beyond `console.error` / `console.debug` in dev mode.

**Functions to extract from `graphStore.ts`**
| Function | Approx. line |
|---|---|
| `auditStateTransition(prev, next, ctx)` | 1335 |
| `auditSucceededRegressions(prev, next, ctx)` | 1277 |
| `assertHydrationBindingInvariants(prev, next, ctx)` | 1306 |
| `isAllowedSucceededRegression(nodeId, ctx)` | 1201 |
| `logSucceededRegression(…)` | 1226 |
| `captureStack(label)` | 1193 |
| `stableJson(value)` | 1562 |

**Module-level state to move**
```ts
const statusRegressionLogThrottle = new Map<string, number>();
const debugLastStatusChange = new Map<string, {…}>();
const DEV_MODE = …;
```

**Dependencies**
- `graphStore.types.ts` (types only)
- `runScope.ts` (`displayStatusFromBinding`)

**Integration into `graphStore.ts`**
- Replace the `auditStateTransition` argument passed to `history.wrapUpdate()`
  with the imported version
- Delete the 7 functions and 3 module-level variables listed above

---

### ⬜ 4. `graphStore.node-schema.ts`  — TODO

**What it contains**
Pure functions that canonicalise node schemas and compute edge schema
constraints / diagnostics.  No store dependency.

**Functions to extract from `graphStore.ts`**
| Function | Approx. line |
|---|---|
| `canonicalizeNodeSchemas(nodes)` | 348 |
| `deriveInferredSchemaObservationForNode(node)` | (called within 348) |
| `computeEdgeSchemaConstraintsInternal(nodes, edges)` | ~5000 |
| `computeEdgeSchemaDiagnosticsInternal(constraints)` | ~5100 |
| `buildNodeSchemaContractSnapshotInternal(state, nodeId)` | 10405 |
| `isSchemaCompatible(provided, required, mode)` | 4788 |
| `adapterKindForTypes(provided, required)` | 4762 |
| `adapterSuggestionForTypes(provided, required)` | (near 4762) |
| `inferredTransformOutputHint(node)` | 4910 |
| `buildProvidedSchema(node, sourceHandle)` | 4938 |
| `expectedInputTypedSchemaForHandle(node, handle)` | 4949 |
| `inferPortAffinityFromHandle(handle, direction)` | 5258 |
| `normalizeHintType(raw)` | (near 5258) |
| `normalizeHintPayloadType(raw)` | 5253 |

**Types to move** (currently private to `graphStore.ts`)
`AdapterTransformKind`, `SchemaCompatibility`, `EdgeCheck`, `EdgeInvalidReason`
→ these are already in `graphStore.types.ts`; the functions just need to
import them from there.

**Test hook to keep working**
```ts
// graphStore.ts re-exports these for test compatibility:
export { __buildNodeSchemaContractSnapshotForTest } from './graphStore.node-schema';
```

**Dependencies**
- `graphStore.types.ts`
- `graphStore.schema.utils.ts`
- `graphStore.bindings.ts`
- `$lib/flow/schema/coercionPolicy`
- `$lib/flow/schema/diagnosticsContract`

---

### ⬜ 5. `graphStore.inspector.ts`  — TODO

**What it contains**
Everything that reads or writes `state.inspector`, plus the node-config commit
pipeline.

**Functions to extract from `graphStore.ts`**
| Function | Approx. line |
|---|---|
| `validateInspectorDraftForAccept(state)` | 645 |
| `updateNodeConfigImpl(nodeId, config, opts)` | 6225 |
| `setNodeSchemaObservationImpl(nodeId, channel, typedSchema, handle)` | 6428 |
| `patchInspectorDraft(nodeId, patch, intent)` | ~6640 |
| `commitInspectorImmediate(nodeId)` | ~6670 |
| `applyInspectorDraft()` | ~6688 |
| `revertInspectorDraft()` | ~6720 |
| `commitSnapshotSelection(nodeId, revisionId)` | ~6757 |
| `pendingInspectorDraftSaveDiagnostic(state)` | ~6932 |
| `getInspectorUi(nodeId, paramsHint?)` | 7021 |
| `setInspectorUi(nodeId, patch)` | 7030 |

**Pattern**
Factory function `createInspectorManager(deps)` receiving:
- `update` (the audited update function)
- `getState()`

Returns `{ actions }` spread into the public store object.

**Dependencies**
- `graphStore.types.ts`
- `graphStore.bindings.ts`
- `graphStore.components.ts`
- `graphStore.schema.utils.ts`
- `graphStore.node-schema.ts`
- `./graph` (`updateNodeParamsValidated`)

---

### ⬜ 6. `graphStore.run.ts`  — TODO

**What it contains**
Everything related to run lifecycle: creating runs, streaming events,
hydrating from snapshots, the SSE/poll reconciliation loop, and the reset
helpers.

**Functions to extract from `graphStore.ts`**
| Function | Approx. line |
|---|---|
| `attachActiveRunEventStream(runId)` | 6038 |
| `applyRunEventState(state, evt, runId)` | ~2000 |
| `hydrateFromRunSnapshotState(state, snap)` | 3332 |
| `resetEdgesExec(edges)` | 5553 |
| `resetRunUiState(state)` | 5557 |
| `applyLlmHolderToNodes(nodes, holderNodeId)` | 1359 |
| `clearNodeCacheUi(state, nodeId)` | ~1660 |
| `isNodeStateFromActiveRunAndFresh(state, binding)` | ~1700 |
| `collectPinnedArtifacts(state)` | ~1720 |
| `clearPerRunPinsIfAny()` | ~1740 |
| `waitForTerminalViaSse(runId, opts)` | ~9900 |
| `runSingleWithStream(payload, plannedNodeSet)` | ~10100 |
| `runAllWithStream(…)` | ~10200 |
| Public: `startRun(opts?)` | ~9850 |
| Public: `pauseRun()`, `resumeRun()`, `cancelRun()` | ~9880 |
| Public: `resetRunUi()` | 8729 |
| Public: `acceptNodeParams(nodeId)` | ~8800 |

**Module-level state to move**
```ts
let activeRunStreamHandle: { runId: string; close: () => void } | null = null;
let resumeFallbackPollTimer: ReturnType<typeof setTimeout> | null = null;
const sseRuntimeStats = { … };
```

**Test hooks to re-export**
```ts
export { __applyRunEventForTest, __hydrateFromRunSnapshotForTest,
         __resetRunUiStateForTest } from './graphStore.run';
```

**Dependencies**
- `graphStore.types.ts`
- `graphStore.bindings.ts`
- `graphStore.audit.ts` (`withGraphMeta`, or move `withGraphMeta` here)
- `runScope.ts`
- `$lib/flow/client/runs`

---

### ⬜ 7. `graphStore.graph-edit.ts`  — TODO

**What it contains**
All canvas mutations: adding/deleting/syncing nodes and edges, edge preflight,
stale invalidation, kind-switching.

**Functions to extract from `graphStore.ts`**
| Function | Approx. line |
|---|---|
| `syncFromCanvas(nodes, edges)` | 7508 |
| `addNode(kind, position, opts?)` | 7551 |
| `deleteNode(nodeId)` | 7590 |
| `addEdge(edge)` | 7886 |
| `deleteEdge(edgeId)` | 7611 |
| `updateEdgeConfig(edgeId, config)` | 7620 |
| `preflightConnection(input)` | 7701 |
| `insertSchemaAdapterForEdgeConnection(input)` | 8123 |
| `updateNodeTitle(nodeId, label)` | 8215 |
| `validateNodeName(name, opts?)` | 8244 |
| `setNodeFreezeMode(nodeId, mode)` | 8265 |
| `setSelectedNodeFreezeMode(mode)` | 8324 |
| `updateNodeProcessingPolicy(nodeId, policy)` | 8331 |
| `updateNodeInputHandleProcessingPolicy(nodeId, handle, policy)` | 8380 |
| `updateNodePortDeclaration(nodeId, direction, handle, decl)` | 8447 |
| `removeNodePortDeclaration(nodeId, direction, handle)` | 8553 |
| `setNodeMeta(nodeId, patch)` | 8649 |
| `setSourceKind(nodeId, nextKind)` | 7314 |
| `setLlmKind(nodeId, nextKind)` | 7367 |
| `setTransformKind(nodeId, nextKind)` | 7423 |
| `setToolProvider(nodeId, nextProvider)` | 7476 |
| `setToolKind(nodeId, nextProvider)` | 7503 |
| `applyLocalStaleInvalidation(nodeId, reason)` | ~6630 |
| `applySemanticSubtypeReset(nodeId, payload)` | 7049 |
| `dedupeEdgesBySignature(edges)` | 662 |
| `pruneAndRecontractEdgesStrict(nodes, edges)` | ~5300 |
| `recomputeEdgeContractsBestEffort(nodes, edges)` | ~5400 |
| `normalizeGraphForComponentMigration(nodes, edges)` | ~3550 |
| `canonicalizeComponentEdgeSourceHandles(nodes, edges, mode)` | ~3600 |
| `applyGraphDocument(graph, graphIdOverride?)` | 6954 |
| `hardResetGraph()` | 8738 |
| `loadGraphDocument(graph, graphIdOverride?)` | 8750 |

**Pattern**
Factory function `createGraphEditManager(deps)` receiving:
- `update`, `set`, `getState()`
- `history` (the HistoryManager — needed for `runInHistoryTransaction` and
  `resetToSnapshot` after `applyGraphDocument`)
- `persist(state)` — local storage helper

**Test hooks to re-export**
```ts
export { __stripToDTOForTest, __hardResetGraphForTest } from './graphStore.graph-edit';
```

**Dependencies**
- `graphStore.types.ts`
- `graphStore.history.ts` (`runInHistoryTransaction`)
- `graphStore.audit.ts` (`withGraphMeta`)
- `graphStore.node-schema.ts`
- `graphStore.bindings.ts`
- `graphStore.components.ts`
- `./nodeNameUniqueness`
- `./graph`
- `./persist`

---

### ⬜ 8. `graphStore.persistence.ts`  — TODO

**What it contains**
All async actions that call the backend API: saving/loading graph revisions,
component session lifecycle, graph management.

**Functions to extract from `graphStore.ts`**
| Function | Approx. line |
|---|---|
| `saveGraph(message?, opts?)` | 8756 |
| `loadGraphRevision(graphId, revisionId?)` | ~8850 |
| `listGraphRevisions(graphId)` | ~8920 |
| `deleteGraphRevision(graphId, revisionId)` | ~8950 |
| `listGraphs()` | ~8960 |
| `deleteGraph(graphId)` | ~8970 |
| `enterComponentEditSession(componentId, revisionId?)` | ~9100 |
| `returnFromComponentEditSession()` | 9253 |
| `updateComponentEditSessionRevision(revisionId)` | 9299 |
| `patchComponentEditContractDraft(patch)` | 9339 |
| `applySavedComponentRevisionToReturnGraph(…)` | 9383 |
| `selectNode(nodeId)` | 7526 |
| `setNodeDocExplanationMode(mode)` | 7146 |
| `setNodeDocTrainingMode(mode)` | 7157 |
| `setNodeDocRuntimeConfig(config)` | 7163 |
| `setNodeDocGeneratedExplanation(nodeId, generated)` | 7225 |
| `clearNodeDocGeneratedExplanation(nodeId)` | 7267 |
| `setNodeExpectedSchema(…)` | 7300 |
| `setNodeExpectedInputSchema(…)` | 7303 |
| `setNodeExpectedInputSchemaForHandle(…)` | 7306 |

**Pattern**
Factory function `createPersistenceManager(deps)` receiving:
- `update`, `getState()`
- `history.resetToSnapshot`
- `persist(state)`
- `applyGraphDocument` (from graph-edit)

**Dependencies**
- `graphStore.types.ts`
- `graphStore.audit.ts` (`withGraphMeta`, `logPush`)
- `$lib/flow/client/graphs`
- `$lib/flow/client/components`
- `./persist`

---

### ⬜ 9. `graphStore.ts`  (orchestrator)  — TODO (final step)

**Target size:** ~200–300 lines

**What stays**
- The `writable` store declaration
- `initialState` construction
- Wiring of all factory managers
- The `return { subscribe, …actions }` block
- Re-exports of test hooks and public utility functions

**Shape after all extractions**

```ts
import { writable, get, derived } from 'svelte/store';
import { createHistoryManager, runInHistoryTransaction } from './graphStore.history';
import { auditStateTransition, withGraphMeta, logPush } from './graphStore.audit';
import { createInspectorManager } from './graphStore.inspector';
import { createRunManager } from './graphStore.run';
import { createGraphEditManager, stripToDTO } from './graphStore.graph-edit';
import { createPersistenceManager } from './graphStore.persistence';
// … type imports from graphStore.types …

const initialState: GraphState = { … };

export const graphStore = (() => {
  const { subscribe, set, update: rawUpdate } = writable<GraphState>(initialState);
  const getState = () => get({ subscribe } as any) as GraphState;

  const history = createHistoryManager({
    getState,
    applyDocument: (graph, graphId) => graphEdit.applyDocument(graph, graphId),
    snapshotFromState: (s) => stripToDTO(s.nodes as any, s.edges as any, s.graphId),
  });

  const update = history.wrapUpdate(
    rawUpdate,
    auditStateTransition,
    (s) => stripToDTO(s.nodes as any, s.edges as any, s.graphId),
  );

  const graphEdit   = createGraphEditManager({ update, set, getState, history, persist });
  const inspector   = createInspectorManager({ update, getState });
  const run         = createRunManager({ update, getState });
  const persistence = createPersistenceManager({ update, getState, history, persist });

  return {
    subscribe,
    ...history.actions,
    ...inspector.actions,
    ...run.actions,
    ...graphEdit.actions,
    ...persistence.actions,
  };
})();

// derived stores (unchanged)
export const selectedNode = derived(graphStore, …);
export const edgeSchemaConstraints = derived(graphStore, …);
export const edgeSchemaDiagnostics = derived(edgeSchemaConstraints, …);

// re-exported test hooks
export { __hardResetGraphForTest, __stripToDTOForTest } from './graphStore.graph-edit';
export { __applyRunEventForTest, __hydrateFromRunSnapshotForTest } from './graphStore.run';
export { __buildNodeSchemaContractSnapshotForTest } from './graphStore.node-schema';
```

---

## Extraction order (safest sequence)

| Step | Module | Risk | Why this order |
|---|---|---|---|
| 1 | `graphStore.types.ts` | ✅ Done | Zero runtime change; pure types |
| 2 | `graphStore.history.ts` | ✅ Done | Self-contained; no store circular dep |
| 3 | `graphStore.audit.ts` | Low | Pure functions; no Svelte imports |
| 4 | `graphStore.node-schema.ts` | Low | Pure functions; no store dependency |
| 5 | `graphStore.inspector.ts` | Medium | Touches store state but well-bounded |
| 6 | `graphStore.run.ts` | Medium-High | Largest chunk; async + SSE + timers |
| 7 | `graphStore.graph-edit.ts` | Medium | Many call sites; use history txns |
| 8 | `graphStore.persistence.ts` | Low | Mostly async API calls |
| 9 | `graphStore.ts` slim-down | Low | Just wiring at this point |

**Rule for each step:** do one module per PR/commit, run `tsc --noEmit` and
your test suite before merging.  The god file shrinks but stays valid TypeScript
at every intermediate stage — nothing breaks mid-refactor.

---

## Progress checklist

- [x] `graphStore.types.ts` — extracted and on disk
- [x] `graphStore.history.ts` — extracted and on disk
- [x] `graphStore.types.ts` wired into `graphStore.ts` (replace inline types)
- [x] `graphStore.history.ts` wired into `graphStore.ts` (replace IIFE history vars)
- [ ] `graphStore.audit.ts` — extracted
- [ ] `graphStore.audit.ts` wired into `graphStore.ts`
- [ ] `graphStore.node-schema.ts` — extracted
- [ ] `graphStore.node-schema.ts` wired into `graphStore.ts`
- [ ] `graphStore.inspector.ts` — extracted
- [ ] `graphStore.inspector.ts` wired into `graphStore.ts`
- [ ] `graphStore.run.ts` — extracted
- [ ] `graphStore.run.ts` wired into `graphStore.ts`
- [ ] `graphStore.graph-edit.ts` — extracted
- [ ] `graphStore.graph-edit.ts` wired into `graphStore.ts`
- [ ] `graphStore.persistence.ts` — extracted
- [ ] `graphStore.persistence.ts` wired into `graphStore.ts`
- [ ] `graphStore.ts` slimmed to orchestrator (~200–300 lines)
- [ ] Full `tsc --noEmit` passes
- [ ] All tests pass
- [ ] `MIGRATION.md` and `REFACTOR_PLAN.md` removed or moved to docs