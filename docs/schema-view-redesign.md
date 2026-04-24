# Schema View Redesign — Implementation Plan

## Overview

The current Schema View toggle produces no meaningful visual change and the error badge
appears in the wrong mode. This plan delivers three things:

1. **Execution View** carries a passive signal on the toggle button when schema issues exist.
2. **Schema View** becomes a distinct visual mode: all edge colours are contract-only, node
   handles carry per-port error/warning badges.
3. **Edge click in Schema View** opens a focused two-sided panel showing the source handle
   schema and the target handle schema, with inline editing and live re-validation.

---

## Phase 1 — Signal on the toggle button (Execution View)

### Goal
A user in Execution View can see at a glance that schema issues exist without leaving the mode.

### Changes

#### `FlowCanvas.svelte`
- The `SchemaPlaneOverlay` component currently receives `enabled={viewMode === 'schema'}`.
  When `enabled=false` it already renders a collapsed pill. That pill must show a non-zero
  `errorCount` badge even in Execution View.
- Change: always render `SchemaPlaneOverlay` (remove the `enabled` gate on rendering).
  The component already has two visual states — expanded overlay (schema mode) and collapsed
  pill (execution mode). The collapsed pill already conditionally shows an error badge if
  `errorCount > 0` (`.schema-err-pill`). This is already correct; it just needs to not be
  hidden.
- Remove the separate `schemaErrorCount` display in the toolbar header (line ~4746) to avoid
  duplication.

#### `SchemaPlaneOverlay.svelte`
- Verify the collapsed pill state shows a clear affordance: "Schema View · ⚠ 1 error" with a
  click target that navigates to Schema View. The `onToggle` prop already supports this.
- The pill should be visually unobtrusive in Execution View (small, bottom-left) but present.

### Tests

**Integration — `SchemaPlaneOverlay.svelte`**
- Collapsed pill renders with error badge when `enabled=false` and `errorCount > 0`.
- Collapsed pill renders without badge when `enabled=false` and `errorCount === 0`.
- Clicking collapsed pill calls `onToggle`.

**Regression**
- When `enabled=true` (Schema View), the expanded overlay still renders correctly.
- No duplicate error count appears in the toolbar header.

---

## Phase 2 — Schema View as a distinct visual mode

### Goal
In Schema View, every edge colour is derived from contract validation only. Node handles
display per-port error/warning badges. Execution colours (queue depth amber, done green,
active blue) are completely suppressed.

### 2a — Edge colours in Schema View

#### `FlowCanvas.svelte` — `displayEdges` reactive block
The `visualClass` logic currently computes execution state first and schema class is appended
separately. In Schema View the visual class must be computed differently.

Current class string (simplified):
```
edge edge-{exec} {visualClass} {schemaClass} {linkKindClass}
```

New behaviour:
- When `viewMode === 'schema'`:
  - `visualClass` is derived solely from `schemaClass`:
    - `edge-schema-error` → use `edge-state-blocked` (red)
    - `edge-schema-warning` → use `edge-state-waiting` (amber)
    - no error/warning → use `edge-state-inactive` (grey/muted)
  - Execution-state signals (`edgeExec`, `monitorFlags`, `sourceLifecycle`,
    `targetLifecycle`) are ignored entirely.
- When `viewMode === 'execution'`: existing logic unchanged.

The `schemaClass` is already computed per edge via `getEdgeSchemaValidationState()` and
`getEdgeDiagnosticSnapshot()` / `resolveSchemaClassFromSnapshot()`. No new data pipeline
is needed; only the branching logic inside `displayEdges` changes.

#### CSS
The existing `edge-schema-error` and `edge-schema-warning` stroke classes remain as-is.
No new CSS needed for edges.

### Tests

**Integration — `displayEdges` logic (unit-testable pure function)**

Extract the `visualClass` computation into a pure function
`computeEdgeVisualClass(input: EdgeVisualInput): string` where `EdgeVisualInput` includes
`viewMode`, `edgeExec`, `monitorFlags`, `sourceLifecycle`, `targetLifecycle`, `schemaClass`.
This makes the branching independently testable without a full Svelte mount.

Tests:
- In `'schema'` mode, edge with `schemaClass === 'edge-schema-error'` → `edge-state-blocked`.
- In `'schema'` mode, edge with `schemaClass === 'edge-schema-warning'` → `edge-state-waiting`.
- In `'schema'` mode, edge with no schema class and `monitorFlags.waiting = true` → `edge-state-inactive` (execution flags suppressed).
- In `'schema'` mode, edge with no schema class and both nodes completed → `edge-state-inactive` (not settled green).
- In `'execution'` mode, all existing `displayEdges` behaviour unchanged (regression).

**Regression — existing `displayEdges` tests**
All tests in the existing `graphStore.cachePill.test.ts`, `graphStore.completedStaleSemantics.test.ts`
and any edge-class snapshot tests must continue to pass unmodified, verifying the execution
path is not affected.

### 2b — Per-handle error/warning badges on nodes in Schema View

#### Goal
In Schema View, each node handle that participates in a schema-invalid edge shows a small
⚠ (warning) or ✕ (error) badge on the port dot. The badge sits on the **target handle**
(receiving end) of an error edge, since that is where the type incompatibility manifests.

#### Data pipeline
- `state.schemaPlane.edgeSchemas[edgeId]` and `getEdgeDiagnosticSnapshot(edgeId)` provide
  per-edge severity.
- A derived map `handleErrorsByNodeId: Map<nodeId, Map<handleId, 'error'|'warning'>>` can be
  computed in FlowCanvas from the edge diagnostic snapshots when `viewMode === 'schema'`.
  For each edge with severity > none: add an entry keyed by `(targetNodeId, targetHandle)`.
  Source handles are not badged (they are outputting what they always output).
- Pass this map down to the node renderer via node `data` or a Svelte context.

#### Node render component
- The handle port dot component receives the badge signal for its specific handle ID.
- In Schema View: overlay a small coloured dot or icon (`⚠` amber for warning, `✕` red for
  error) at the port position.
- In Execution View: badge is not rendered.
- Badge is purely visual — no interaction on the badge itself; clicking the *edge* is the
  interaction (Phase 3).

#### New component: `HandleSchemaBadge.svelte`
Props: `severity: 'error' | 'warning' | null`, `viewMode: GraphViewMode`.
Renders nothing when `severity === null` or `viewMode !== 'schema'`.

### Tests

**Unit — `HandleSchemaBadge.svelte`**
- Renders `⚠` with warning colour when `severity='warning'` and `viewMode='schema'`.
- Renders `✕` with error colour when `severity='error'` and `viewMode='schema'`.
- Renders nothing when `viewMode='execution'` regardless of severity.
- Renders nothing when `severity=null`.

**Integration — handle badge derivation**
- Given one edge with `severity='error'`, target handle `'in'` on node `'n1'`:
  `handleErrorsByNodeId.get('n1').get('in') === 'error'`.
- Source node handle is NOT badged.
- Given two edges into the same target handle, the higher severity wins.

**Regression**
- Node rendering in Execution View: no badges, no visual change from current.
- Existing node component snapshot/render tests pass unmodified.

---

## Phase 3 — Edge click panel: two-sided contract view

### Goal
In Schema View, clicking an edge that has a schema error or warning opens a focused panel
showing the source handle schema and target handle schema side by side. Either or both sides
can be edited. Changes trigger immediate re-validation. Applying persists the change to the
graph store.

### 3a — Edge click wiring

#### `FlowCanvas.svelte`
- SvelteFlow emits an `on:edgeclick` event. Currently this event is either unused or used
  only for selection.
- When `viewMode === 'schema'` and the clicked edge has `schemaClass !== ''`:
  - Set a new store field `schemaEdgeInspectorEdgeId: string | null` to the clicked edge ID.
  - This field drives the panel visibility.
- When `viewMode !== 'schema'` or the edge has no schema issue, the panel does not open
  (normal edge selection behaviour preserved).

### 3b — `SchemaEdgeInspectorPanel.svelte` (new component)

#### Props / bindings
```typescript
edgeId: string;
graphStore: GraphStore;
onClose: () => void;
```

#### Layout
Two-column layout within a side panel or modal. Columns are labelled with node name and
handle name, not just "source" / "target".

```
┌─────────────────────────────────────────────────┐
│  Edge: Transform_Sel [out] → Model_ScoreJob [in] │
│  ⚠ Schema mismatch: expected json, got text      │
├────────────────────┬────────────────────────────┤
│  SOURCE            │  TARGET                    │
│  Transform_Sel     │  Model_ScoreJob             │
│  Handle: out       │  Handle: in                │
│                    │                            │
│  Output schema     │  Expected input schema     │
│  [schema editor]   │  [schema editor]           │
│                    │                            │
│  [read-only if     │  [editable if declared     │
│   inferred]        │   schema; read-only if     │
│                    │   fixed by node kind]       │
│         [Apply source]       [Apply target]     │
└─────────────────────────────────────────────────┘
```

#### Data sourcing
- **Edge metadata:** `edge.source`, `edge.target`, `edge.sourceHandle`, `edge.targetHandle`
  from the edge object.
- **Source schema:** `state.schemaPlane.edgeSchemas[edgeId]` provides the propagated schema
  on the edge (what the source is actually outputting on this handle).
- **Target expected schema:** `state.nodes[targetNodeId].data.schema.expectedInputSchemas[targetHandle]`
  — the declared expected schema for that input handle.
- **Mismatch message:** `getEdgeDiagnosticSnapshot(edgeId).contractMessage` or
  `EdgeSchemaDiagnostic.message` for the human-readable description.
- **Editability:**
  - Source schema: editable only if the source node kind supports output schema declaration
    (e.g., transform nodes with declared output). Fixed for source/model nodes where schema
    is inferred from data or model output. Show a "Schema inferred from data — not editable"
    label when read-only.
  - Target expected schema: editable if `expectedInputSchemas[handle].source === 'declared'`.
    Read-only if `source === 'inferred'` or `source === 'default'`.

#### Live re-validation
- As the user edits either schema, call `graphStore.recomputeSchemaPlane()` (or the
  equivalent incremental update) and re-derive the `EdgeSchemaDiagnostic` for this edge.
- The mismatch banner at the top of the panel updates in real time: changes from ⚠/✕ to ✓
  as soon as the schemas are compatible.
- The edge colour on the canvas also updates in real time because `displayEdges` is reactive
  on schema state.

#### Apply
- "Apply source" and "Apply target" are separate buttons, each scoped to their column.
- Applying calls `graphStore.updateNodeSchema(nodeId, handleId, newSchema)` (new action,
  see Phase 3c).
- On apply, the panel remains open, re-validation runs, and the banner updates.
- The panel is closed by an explicit ✕ button or by clicking outside it.

#### Asymmetric consequence labelling
Because changing a source schema has different implications from changing a target schema,
each column header carries a short explanatory label:
- Source column: *"What this node outputs"*
- Target column: *"What this node expects to receive"*

### 3c — New graph store action: `updateNodeSchema`

```typescript
graphStore.updateNodeSchema(
  nodeId: string,
  handleId: string,
  direction: 'input' | 'output',
  schema: TypedSchema
): void
```

- For `direction === 'input'`: updates `node.data.schema.expectedInputSchemas[handleId]`.
- For `direction === 'output'`: updates the relevant output schema declaration on the node.
- After updating, triggers `recomputeSchemaPlane()` for reactive re-validation.
- The change is treated as a graph edit: it is undoable via the existing undo/redo stack
  (`graphStore.history`).

### Tests

**Unit — `SchemaEdgeInspectorPanel.svelte`**
- Renders source node name, handle name, and schema.
- Renders target node name, handle name, and expected schema.
- Renders mismatch message from `contractMessage`.
- "Apply target" button calls `updateNodeSchema` with correct args.
- "Apply source" button calls `updateNodeSchema` with correct args.
- When source schema is inferred, source column shows read-only label; "Apply source" is
  absent or disabled.
- When target schema is declared, target column is editable.
- Live re-validation: editing target schema to match source schema changes the mismatch
  banner to ✓ before applying.

**Unit — `graphStore.updateNodeSchema`**
- Updates `expectedInputSchemas[handleId]` on the correct node.
- Triggers `recomputeSchemaPlane()`.
- Is recorded in undo history and can be reversed by `graphStore.undo()`.
- Does not mutate other nodes or handles.

**Integration — edge click → panel open**
- In Schema View, clicking an edge with a schema error sets `schemaEdgeInspectorEdgeId`.
- In Execution View, clicking an edge does not set `schemaEdgeInspectorEdgeId`.
- In Schema View, clicking an edge with no schema issue does not open the panel.

**Integration — panel → canvas re-validation loop**
- Given a graph with one schema-error edge:
  - Open panel by simulating edge click.
  - Edit target schema in panel to match source.
  - Assert `getEdgeDiagnosticSnapshot(edgeId).effectiveSeverity` becomes `'none'`.
  - Assert the edge `schemaClass` in `displayEdges` becomes `''` (no error class).
  - Assert the mismatch banner in the panel shows ✓.
- Applying the change:
  - Assert `node.data.schema.expectedInputSchemas[handleId]` contains the new schema.
  - Assert `graphStore.undo()` restores the original schema.
  - Assert the edge error reappears after undo.

**Regression**
- All existing `graphStore.schemaPlane.test.ts` tests pass unmodified.
- All existing `graphStore.edgeContractDrift.test.ts` tests pass unmodified.
- All existing `graphStore.schemaContractSnapshot.test.ts` tests pass unmodified.
- Edge click in Execution View: no panel, no `schemaEdgeInspectorEdgeId` side effect.
- Undo/redo stack: non-schema edits (node move, param change) are unaffected.

---

## Phase ordering and dependencies

```
Phase 1  ──►  Phase 2a  ──►  Phase 2b  ──►  Phase 3
(badge)       (edge      (handle       (click panel)
              colours)    badges)
```

Each phase is independently shippable. Phase 1 can go out immediately.
Phase 2a and 2b can be developed in parallel once Phase 1 is merged.
Phase 3 depends on Phase 2a being complete (edge click only opens the panel when schema
colours are active, which requires Phase 2a).

---

## Files created or modified

| File | Change |
|------|--------|
| `FlowCanvas.svelte` | Phase 1: remove overlay render gate; Phase 2a: `displayEdges` branching; Phase 2b: `handleErrorsByNodeId` derivation + pass to nodes; Phase 3: `on:edgeclick` handler + `schemaEdgeInspectorEdgeId` state |
| `components/ui/SchemaPlaneOverlay.svelte` | Phase 1: verify collapsed pill always visible |
| `store/graphStore.ts` | Phase 3c: `updateNodeSchema` action; `schemaEdgeInspectorEdgeId` state field |
| `store/graphStore.types.ts` | Phase 3c: add `schemaEdgeInspectorEdgeId: string \| null` to `GraphState` |
| `store/graphStore.graph-edit.ts` | Phase 3c: implement `updateNodeSchema` mutation |
| `components/ui/HandleSchemaBadge.svelte` | Phase 2b: new component |
| `components/ui/SchemaEdgeInspectorPanel.svelte` | Phase 3b: new component |
| `store/edgeSchemaAuthority.ts` | Phase 2a: expose helper used by `computeEdgeVisualClass` |

**New test files:**
| File | Phase |
|------|-------|
| `components/ui/HandleSchemaBadge.test.ts` | 2b |
| `components/ui/SchemaEdgeInspectorPanel.test.ts` | 3b |
| `store/graphStore.updateNodeSchema.test.ts` | 3c |
| `store/graphStore.schemaEdgeInspector.integration.test.ts` | 3 integration |
| `store/computeEdgeVisualClass.test.ts` | 2a |
