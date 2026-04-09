
---

## Root Cause Analysis

### Bug 1 — Pinned component-internal nodes recalculate instead of using their artifact

**Location**: `graphStore.run.ts:531–535` (`collectPinnedArtifactsByNode`)

When a user pins a node inside a component (via `setNodeFreezeMode`, `graphStore.graph-edit.ts:1655`), two things are written to the node:
- `node.data.meta.freeze = { enabled: true, mode }` — the pin flag
- `node.data.meta.freezeLineage = { artifactId, execKey }` — the artifact reference (taken from `nodeBindings` at pin time)

When `collectPinnedArtifactsByNode` runs at the start of each run (line 2787–2791), it finds the artifact **only from `nodeBindings`**:

```typescript
const binding = _normalizeBinding(nodeBindings?.[nodeId], nodeId);
const lineage = binding.last?.artifactId … ? binding.last : binding.current;
const artifactId = String(lineage?.artifactId ?? '').trim();
if (!artifactId || !execKey) continue;   // ← silently skips the pin
```

The problem: `applyGraphDocument` (`graphStore.graph-edit.ts:695`) always resets `nodeBindings` to empty normalized values:

```typescript
nodeBindings: ensureNormalizedBindingsForNodes(normalized.nodes as any, {}),
```

This is called both when first entering a component edit session (`openComponentRevisionForEditing:1343`) and when the cached draft is loaded on re-entry. So every time the user opens a component to edit, `nodeBindings` starts blank. `collectPinnedArtifactsByNode` finds no artifact, skips every pinned node, and sends no pin hints to the backend. The backend has no reason to skip those nodes, so they recompute.

`meta.freezeLineage` on the node itself has the correct artifact reference, but `collectPinnedArtifactsByNode` never looks there.

---

### Bug 2 — Pins not persisted after save

There are three compounding sub-bugs:

**2a — `saveGraph`/`saveGraphVersion`/`saveGraphAs` silently corrupt the parent graph while in component edit mode**

When `editingContext === 'component'`:
- `getState().nodes` = component's internal nodes
- `getState().graphId` = **parent**'s graphId (because `applyGraphDocument` was called with `null` as graphId override, so line 681 keeps `s.graphId`)

`saveGraph` (line 898) does:
```typescript
buildPersistableGraphStrict(current.nodes, current.edges, graphId)
// then:
createGraphRevision({ graphId, graph })
```

This saves the component's internal nodes as a new revision of the **parent graph**. The call succeeds, the user thinks they saved; but the parent graph on the backend now contains component-internal nodes. This is the most dangerous failure: it looks like a successful save but is data corruption.

**2b — `persist()` in `graphStore.ts` also overwrites the parent graph in localStorage while in component edit**

Every state mutation while editing a component calls `persist(next)` → `saveGraphToLocalStorage(stripToDTO(state.nodes, state.edges, state.graphId))`. At that point `state.nodes` are the component's internal nodes, so localStorage is overwritten with component nodes under the parent's graphId. After `returnFromComponentEditSession` calls `persist` again with the parent's nodes, localStorage is corrected — but any crash or reload between those two points loads the corrupted state.

**2c — `componentContractDraftCache` is never written to localStorage**

`returnFromComponentEditSession` (line 1408–1411) saves the component's current nodes (with `meta.freeze`/`meta.freezeLineage`) to `state.componentContractDraftCache[cacheKey].__graphDraft`. This is the only mechanism that lets re-entering the same component see the pinned state.

But `persist.ts` has no awareness of the draft cache. `saveGraphToLocalStorage` only serializes `PipelineGraphDTO` (nodes + edges + meta). The `componentContractDraftCache` lives in memory only. On page reload, it is gone. The next `openComponentRevisionForEditing` call finds no draft, falls back to the backend revision (which has no pins), and the user sees all pins lost.

---

## Step-by-Step Fix Plan

### Step 1 — Fix `collectPinnedArtifactsByNode` to fall back to `meta.freezeLineage`

**File**: `src/lib/flow/store/graphStore.run.ts`  
**Function**: `collectPinnedArtifactsByNode` (~line 519)  
**Change**: After the current logic that reads from `nodeBindings`, if `artifactId`/`execKey` are still empty, read from `node.data.meta.freezeLineage`.

```typescript
// current (lines 531–535):
const binding = _normalizeBinding(nodeBindings?.[nodeId], nodeId);
const lineage = binding.last?.artifactId || binding.last?.execKey ? binding.last : binding.current;
const artifactId = String(lineage?.artifactId ?? '').trim();
const execKey = String(lineage?.execKey ?? '').trim();
if (!artifactId || !execKey) continue;

// replacement:
const binding = _normalizeBinding(nodeBindings?.[nodeId], nodeId);
const lineage = binding.last?.artifactId || binding.last?.execKey ? binding.last : binding.current;
let artifactId = String(lineage?.artifactId ?? '').trim();
let execKey = String(lineage?.execKey ?? '').trim();
if (!artifactId || !execKey) {
    // nodeBindings are empty after entering a component edit session — fall back
    // to the lineage snapshotted onto the node at pin time
    const fl = (node.data as any)?.meta?.freezeLineage;
    if (fl && typeof fl === 'object') {
        artifactId = String(fl.artifactId ?? '').trim();
        execKey = String(fl.execKey ?? '').trim();
    }
}
if (!artifactId || !execKey) continue;
```

For component nodes (the `if (node.data.kind === 'component')` branch at line 540), the `outputs` should also fall back to `meta.freezeLineage.outputs` when `binding.outputLineage` is absent.

**Verification**: After this fix, pins set while `nodeBindings` is populated (at the time of pinning) survive the `nodeBindings` reset that happens when re-entering the component edit session.

---

### Step 2 — Guard `saveGraph`, `saveGraphVersion`, `saveGraphAs` against component edit mode

**File**: `src/lib/flow/store/graphStore.persistence.ts`  
**Functions**: `saveGraph` (~line 898), `saveGraphVersion` (~line 969), `saveGraphAs` (~line ~1050)  
**Change**: Add an early return at the top of each function, before any save logic:

```typescript
async function saveGraph(message?: string, opts?: { graphName?: string }) {
    const current = getState();
    if (current.editingContext === 'component') {
        return {
            ok: false,
            reason: 'in_component_edit' as const,
            error: 'Cannot save graph while in component edit mode. Exit the component editor first, or use "Save Component Revision".'
        };
    }
    // ... rest of existing logic
}
```

Apply the same guard to `saveGraphVersion` and `saveGraphAs`. This eliminates the silent data corruption path.

---

### Step 3 — Stop `persist()` from overwriting the parent graph in localStorage during component edit

**File**: `src/lib/flow/store/graphStore.ts`  
**Function**: `persist` (the local function inside the IIFE, ~line 591)  
**Change**: Skip `saveGraphToLocalStorage` when `editingContext === 'component'`:

```typescript
function persist(state: GraphState) {
    if (state.editingContext === 'component') return;
    saveGraphToLocalStorage(stripToDTO(state.nodes, state.edges, state.graphId));
}
```

The component's in-session state is managed by the draft cache (Step 4 below), not by the main graph localStorage key.

---

### Step 4 — Add draft cache persistence to localStorage

**File**: `src/lib/flow/store/persist.ts`  
**Change**: Add two functions with their own localStorage key:

```typescript
const DRAFT_CACHE_KEY = 'flow:componentDraftCache:v1';

export function saveComponentDraftCache(cache: Record<string, unknown>): void {
    if (!hasLocalStorage()) return;
    try {
        window.localStorage.setItem(DRAFT_CACHE_KEY, JSON.stringify(cache));
    } catch (e) {
        console.warn('Failed to save component draft cache to localStorage', e);
    }
}

export function loadComponentDraftCache(): Record<string, unknown> {
    if (!hasLocalStorage()) return {};
    try {
        const raw = window.localStorage.getItem(DRAFT_CACHE_KEY);
        if (!raw) return {};
        const parsed = JSON.parse(raw);
        return parsed && typeof parsed === 'object' ? parsed : {};
    } catch {
        return {};
    }
}
```

**File**: `src/lib/flow/store/graphStore.ts`  
**Change 1** — update the `persist` function (from Step 3) to also save the draft cache:

```typescript
function persist(state: GraphState) {
    if (state.editingContext !== 'component') {
        saveGraphToLocalStorage(stripToDTO(state.nodes, state.edges, state.graphId));
    }
    const cache = state.componentContractDraftCache;
    if (cache && Object.keys(cache).length > 0) {
        saveComponentDraftCache(cache);
    }
}
```

**Change 2** — restore the draft cache in `initialState`:

```typescript
// near the top with the other load calls:
import { saveComponentDraftCache, loadComponentDraftCache } from './persist';

const initialState: GraphState = {
    // ... existing fields ...
    componentContractDraftCache: loadComponentDraftCache(),
    // ...
};
```

With this, pins survive page reload: on the next `openComponentRevisionForEditing`, `readComponentDraftGraph(cachedDraftRaw)` finds the saved draft (with `meta.freeze`/`meta.freezeLineage`) and loads it instead of the bare backend revision.

---

### Step 5 — Add `saveComponentRevision` store action for backend-persistent pins

**File**: `src/lib/flow/store/graphStore.persistence.ts`  
**What**: A new action inside `createPersistenceManager` that saves the component's current internal state (including sticky pins) as a new component revision on the backend.

```typescript
async function saveComponentRevision(opts?: { message?: string }) {
    const s = getState();
    const session = s.componentEditSession;
    if (!session || s.editingContext !== 'component') {
        return { ok: false, reason: 'not_in_component_edit' as const };
    }
    const cid = String(session.componentId ?? '').trim();
    const rid = String(session.revisionId ?? '').trim();
    if (!cid || !rid) return { ok: false, reason: 'missing_component_ref' as const };

    // Strip per-run pins (they will have been cleared already by clearPerRunPinsOnNodes,
    // but guard explicitly in case of edge cases)
    const cleanNodes = (s.nodes as any[]).map((node) => {
        const freeze = (node?.data as any)?.meta?.freeze;
        if (freeze?.mode === 'per_run') {
            const nextMeta = { ...((node?.data as any)?.meta ?? {}) };
            delete (nextMeta as any).freeze;
            delete (nextMeta as any).freezeLineage;
            return { ...node, data: { ...(node.data ?? {}), meta: nextMeta } };
        }
        return node;
    });

    const strictGraph = buildPersistableGraphStrict(cleanNodes as any, s.edges as any);
    if (!strictGraph.ok) return { ok: false, reason: 'invalid_graph' as const, error: strictGraph.error };

    try {
        const existingDetail = await getComponentRevision(cid, rid);
        const created = await createComponentRevision({
            componentId: cid,
            parentRevisionId: rid,
            message: String(opts?.message ?? '').trim() || 'save_component',
            schemaVersion: Number((existingDetail as any)?.schemaVersion ?? 1) || 1,
            graph: { nodes: strictGraph.graph.nodes, edges: strictGraph.graph.edges },
            api: ((existingDetail?.definition?.api ?? { inputs: [], outputs: [] }) as ComponentApiContract),
            configSchema: structuredClone((existingDetail?.definition?.configSchema ?? {}) as Record<string, unknown>),
            exposureRegistry: structuredClone(
                Array.isArray((existingDetail?.definition as any)?.exposureRegistry)
                    ? (existingDetail.definition as any).exposureRegistry
                    : []
            )
        });
        updateComponentEditSessionRevision(String(created.revisionId ?? ''));
        return { ok: true, revisionId: String(created.revisionId ?? '') };
    } catch (error) {
        return { ok: false, reason: 'save_failed' as const, error: String(error) };
    }
}
```

Add `saveComponentRevision` to the return block of `createPersistenceManager`.

This makes sticky pins truly persistent — they survive browser storage clears, different machines, and collaboration scenarios where other users load the same component.

---

### Step 6 — Clear the draft cache entry when a component revision is explicitly saved to backend

When `saveComponentRevision` succeeds (Step 5), the backend now has the pins. The draft cache entry for the old revision (`cid@rid`) becomes stale (the new revision has a different `rid`). Clear it:

```typescript
// After updateComponentEditSessionRevision(newRid):
const oldCacheKey = `${cid}@${rid}`;
update((s) => ({
    ...s,
    componentContractDraftCache: Object.fromEntries(
        Object.entries(s.componentContractDraftCache ?? {}).filter(([k]) => k !== oldCacheKey)
    )
}));
// Re-persist the now-smaller draft cache
saveComponentDraftCache(getState().componentContractDraftCache);
```

---

### Summary of Changes

| Step | File | Change |
|------|------|--------|
| 1 | `graphStore.run.ts:531` | `collectPinnedArtifactsByNode` falls back to `meta.freezeLineage` |
| 2 | `graphStore.persistence.ts:898,969,~1050` | `saveGraph`/`saveGraphVersion`/`saveGraphAs` return error when `editingContext === 'component'` |
| 3 | `graphStore.ts` (`persist` fn) | Skip `saveGraphToLocalStorage` when in component edit |
| 4a | `persist.ts` | Add `saveComponentDraftCache`/`loadComponentDraftCache` |
| 4b | `graphStore.ts` (`persist` fn + `initialState`) | Call draft cache save/load |
| 5 | `graphStore.persistence.ts` | Add `saveComponentRevision` action |
| 6 | `graphStore.persistence.ts` | Clear stale draft cache key after backend save |

**Order to implement**: Steps 1 → 3 → 2 → 4a → 4b → 5 → 6. Steps 1 and 3 are independent. Step 2 must come after Step 5 exists (so users have an alternative save path). Steps 4a and 4b are a unit. Steps 5 and 6 are a unit.