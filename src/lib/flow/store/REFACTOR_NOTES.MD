Let me read more of the file to understand its full structure before planning the refactor.I now have a thorough understanding of the file. Let me produce the refactor plan as a document before touching any code.Now I have everything I need. Here's my full refactor plan:

---

## Refactor Plan for `graphStore.ts`

The file is ~10,500 lines. Some extraction has already started (`.utils`, `.schema.utils`, `.bindings`, `.components`), but the core is still a single god object. The refactor splits it into **6 focused modules** plus a thin orchestrator.

### Current structure (what's already extracted)
- `graphStore.utils.ts` — IO derivation helpers
- `graphStore.schema.utils.ts` — schema normalization
- `graphStore.bindings.ts` — binding pair logic
- `graphStore.components.ts` — component-specific logic

### What remains in the god file (and where it should go)

---

**`graphStore.types.ts`** *(new)*
Extract all type/interface declarations that are currently scattered through the file. Nothing else.
- `GraphState`, `NodeOutputInfo`, `NodeExecutionError`, `NodeBindingInfo`, `NormalizedNodeBinding`, `InspectorState`, `RunLog`, `RunStatus`, `ComponentEditSession`, `InputResolution`, `SavePreflightResult`, `ApiEditorUiState`, etc.
- Also: the `IDLE`, `SUCCEEDED` constants and `initialInspector`.

---

**`graphStore.node-schema.ts`** *(new)*
Pure functions that deal with canonicalizing and repairing node schemas and processing policies. These are stateless transforms on `Node<PipelineNodeData>[]`.
- `canonicalizeNodeSchemas()`
- `deriveInferredSchemaObservationForNode()`
- `computeEdgeSchemaConstraintsInternal()` / `computeEdgeSchemaDiagnosticsInternal()`
- `buildNodeSchemaContractSnapshotInternal()`
- `setNodeSchemaObservationImpl()` (the pure state-transform part)

---

**`graphStore.run.ts`** *(new)*
Everything related to run lifecycle — creating runs, streaming events, hydrating from snapshots, the SSE/poll reconciliation loop.
- `attachActiveRunEventStream()`
- `hydrateFromRunSnapshot()`
- `applyRunEventState()` and all the `evt.type` branches inside it
- `resetEdgesExec()` / `resetRunUiState()`
- `applyLlmHolderToNodes()`
- The `runSingleWithStream` / `runAllWithStream` functions
- `waitForTerminalViaSse()`

---

**`graphStore.inspector.ts`** *(new)*
Inspector and node-config editing — the only things that touch `state.inspector` and commit/revert flows.
- `patchInspectorDraft()`
- `commitInspectorImmediate()`
- `applyInspectorDraft()`
- `revertInspectorDraft()`
- `commitSnapshotSelection()`
- `validateInspectorDraftForAccept()`
- `updateNodeConfigImpl()`
- `getInspectorUi()` / `setInspectorUi()`
- `pendingInspectorDraftSaveDiagnostic()`

---

**`graphStore.history.ts`** *(new)*
The undo/redo stack — completely self-contained.
- `snapshotFromState()`, `snapshotKey()`
- `resetHistoryToSnapshot()`, `pushHistorySnapshot()`
- `beginHistoryTxn()` / `endHistoryTxn()` / `runInHistoryTransaction()`
- `applyHistorySnapshot()`
- Public: `undo()`, `redo()`, `canUndo()`, `canRedo()`, `setHistoryLimit()`, `clearHistory()`, `beginHistoryTransaction()`, `endHistoryTransaction()`

---

**`graphStore.graph-edit.ts`** *(new)*
Canvas mutations — add/delete/sync of nodes and edges, edge preflight, stale invalidation.
- `syncFromCanvas()`
- `addNode()`, `deleteNode()`
- `addEdge()`, `deleteEdge()`, `updateEdgeConfig()`
- `preflightConnection()`, `insertSchemaAdapterForEdgeConnection()`
- `updateNodeTitle()`, `validateNodeName()`
- `setNodeFreezeMode()`, `setNodeMeta()`
- `updateNodeProcessingPolicy()`, `updateNodeInputHandleProcessingPolicy()`, `updateNodePortDeclaration()`, `removeNodePortDeclaration()`
- `setSourceKind()`, `setLlmKind()`, `setTransformKind()`, `setToolProvider()`, `setToolKind()`
- `applyLocalStaleInvalidation()`
- `dedupeEdgesBySignature()`
- `pruneAndRecontractEdgesStrict()` / `recomputeEdgeContractsBestEffort()`

---

**`graphStore.persistence.ts`** *(new — thin)*
Save/load actions that call the client APIs. Currently they're large `async` methods sitting inside the store IIFE.
- `saveGraph()`, `loadGraphRevision()`, `listGraphRevisions()`, `deleteGraphRevision()`
- `enterComponentEditSession()`, `returnFromComponentEditSession()`, `updateComponentEditSessionRevision()`
- `applySavedComponentRevisionToReturnGraph()`
- `loadGraphFromRemote()`, `listGraphs()`, `deleteGraph()`

---

**`graphStore.ts`** *(orchestrator — stays, but slim)*
Becomes ~200–300 lines:
```
import { writable, get, derived } from 'svelte/store';
import { createHistoryManager } from './graphStore.history';
import { createRunManager } from './graphStore.run';
import { createInspectorManager } from './graphStore.inspector';
// ... etc.

export const graphStore = (() => {
  const { subscribe, set, update: rawUpdate } = writable<GraphState>(initialState);
  const update = wrapWithAudit(rawUpdate);

  const history = createHistoryManager(update, subscribe);
  const run = createRunManager(update, subscribe);
  const inspector = createInspectorManager(update, subscribe);
  const graphEdit = createGraphEditManager(update, subscribe);
  const persistence = createPersistenceManager(update, subscribe);

  return {
    subscribe,
    ...history.actions,
    ...inspector.actions,
    ...run.actions,
    ...graphEdit.actions,
    ...persistence.actions,
  };
})();
```

---

### Key principles for the extraction

**Dependency direction** (no circular imports):
```
types  ←  node-schema, bindings, schema.utils, utils
          ↓
        inspector, run, history, graph-edit
          ↓
        persistence
          ↓
        graphStore (orchestrator)
```

**Each module gets**: its own `update`/`subscribe` reference passed in at construction time (factory pattern), so nothing needs to reach back into the store singleton.

**`auditStateTransition`** stays in a shared `graphStore.audit.ts` — it's used by `update` itself, not by any single domain.

**`withGraphMeta`** / `logPush` / `ensureNormalizedBindingsForNodes` are utilities consumed by multiple modules → move to `graphStore.utils.ts` (already exists, just expand it).

---

### Suggested migration order (to keep the app working throughout)

1. Extract `graphStore.types.ts` first — zero behavior change, just moves types.
2. Extract `graphStore.history.ts` — fully self-contained, easy to test.
3. Extract `graphStore.node-schema.ts` — pure functions, no side effects.
4. Extract `graphStore.inspector.ts` — well-bounded, testable in isolation.
5. Extract `graphStore.run.ts` — largest chunk, do last among pure extractions.
6. Extract `graphStore.graph-edit.ts` — canvas mutations.
7. Extract `graphStore.persistence.ts` — async API calls.
8. Slim down `graphStore.ts` to orchestrator.

Each step should be its own PR/commit so you can verify nothing broke before moving on.

---

###  the module extraction order and the dependency direction rule:

types ← node-schema, bindings, schema.utils, utils
        ↓
      inspector, run, history, graph-edit
        ↓
      persistence
        ↓
      graphStore (orchestrator)