Now I have everything I need. Let me write the comprehensive plan.

---

# Migration Plan: Pinning → Memoization + Checkpointing

## Architecture Summary

The migration introduces two separate systems that replace the current `meta.freeze` / `meta.freezeLineage` pin system:

**Memoization** — automatic, content-addressed caching keyed by a Merkle fingerprint of `(nodeKind + params + inputArtifactIds)`. The backend computes this fingerprint before executing any node. A cache hit skips execution entirely. No user action required. Invalidated automatically when any input changes.

**Checkpointing** — explicit, named, user-created snapshots of a node's output. A checkpoint stores the artifact reference, a copy of the fingerprint at creation time, and human-readable metadata. On each run, the backend compares the current fingerprint against the stored one and reports whether the checkpoint is valid, stale, or has a missing artifact. User acknowledges stale checkpoints before running.

The legacy pin system is kept in read-only compatibility mode until Phase 8, then removed.

---

## Phase 1 — Audit Baseline and Test Inventory

### Step 1 — Capture Baseline Test Results and Establish Regression Gate

**Goal:** Record what currently passes before any migration code lands. Every subsequent step must leave this set passing.

**Actions:**

Run the full frontend test suite and record the passing count:
```
npx vitest run --reporter=json > baseline_test_results.json
```
Run `tsc --noEmit` and record the error count (current baseline: ≤313).

Run the backend integration suite:
```
pytest backend/tests/integration/test_run_from_selected_incremental.py -v
```

Create `MIGRATION_BASELINE.md` at repo root documenting:
- Frontend test count and names of all pin/freeze-related tests
- `tsc --noEmit` error baseline
- Backend integration test names that exercise pin behavior
- The six test files that are pin-relevant: `graphStore.freeze.test.ts`, `graphStore.runRemotePinnedHintsAfterReset.test.ts`, `graphStore.resetRunUi.test.ts`, `graphStore.component.test.ts`, `graphStore.runScope.test.ts`, `runScope.test.ts`

**Tests to write:** None yet — this step is measurement only.

**Regression gate:** MIGRATION_BASELINE.md exists and is committed. All documented tests pass at step start.

---

### Step 2 — Annotate All Legacy Pin Code With Deprecation Markers

**Goal:** Make every piece of the old system explicitly labeled so nothing is accidentally missed during removal. Zero behavioral change.

**Files to change:**
- `src/lib/flow/types/base.ts`
- `src/lib/flow/schema/base.ts`
- `src/lib/flow/store/graphStore.run.ts`
- `src/lib/flow/store/graphStore.graph-edit.ts`
- `src/lib/flow/store/graphStore.inspector.ts`
- `backend/app/runner/run.py`
- `backend/app/runner/compile.py`

**Actions:**

In `src/lib/flow/types/base.ts`, add `@deprecated` JSDoc to the `freeze` field and add the missing `freezeLineage` type with deprecation marker:
```typescript
/** @deprecated Legacy pin system. Use CheckpointRecord instead. Removed in Phase 8. */
freeze?: {
  enabled?: boolean;
  mode?: 'per_run' | 'sticky';
};
/** @deprecated Legacy pin system. Use CheckpointRecord instead. Removed in Phase 8. */
freezeLineage?: {
  artifactId: string;
  execKey: string;
  outputs?: Record<string, { artifactId: string; execKey?: string }>;
};
```

In `src/lib/flow/schema/base.ts`, add `// @deprecated` comment above the `freeze` field.

In `src/lib/flow/store/graphStore.run.ts`, add `/** @deprecated use memoization/checkpoint system */` above `collectPinnedNodeIds`, `collectPinnedArtifactsByNode`, `clearPerRunPinsOnNodes`, `validatePinEligibility`, and all `tracePinXxx` functions.

In `src/lib/flow/store/graphStore.graph-edit.ts`, add `/** @deprecated */` above `setNodeFreezeMode` and `setSelectedNodeFreezeMode`.

In `src/lib/flow/store/graphStore.inspector.ts`, add `/** @deprecated */` above `nodeFreezeMode`.

In `backend/app/runner/run.py`, add `# DEPRECATED: legacy pin system` comment above `_node_freeze_mode` and `_node_freeze_lineage`.

In `backend/app/runner/compile.py`, add `# DEPRECATED: legacy pin boundary logic` above the `pinned` assignment block.

**Tests to write:** None — zero behavioral change. Verify `tsc --noEmit` error count is unchanged.

**Regression gate:** All baseline tests pass. `tsc --noEmit` ≤ baseline error count.

---

## Phase 2 — New Type System Foundation

### Step 3 — Define MemoKey, CheckpointRecord, and CheckpointRegistry Types

**Goal:** Establish the complete type surface for both new systems before any implementation code touches them. No runtime code changes yet.

**Files to create:**
- `src/lib/flow/types/checkpoint.ts` (new file)
- `src/lib/flow/schema/checkpoint.ts` (new file)

**Contents of `src/lib/flow/types/checkpoint.ts`:**
```typescript
/**
 * A Merkle-style fingerprint encoding (nodeKind + serialized params + sorted inputArtifactIds).
 * Produced by the backend before each node execution. Identical inputs always produce identical keys.
 */
export type MemoKey = string; // sha256 hex, 64 chars

/**
 * The result of a memoization cache lookup as reported in run trace events.
 */
export type MemoLookupResult =
  | { hit: true;  artifactId: string; execKey: string; memoKey: MemoKey }
  | { hit: false; memoKey: MemoKey };

/**
 * Staleness status of a checkpoint relative to the current run context.
 * Computed by comparing the checkpoint's fingerprintAtCreation against the
 * current MemoKey for that node.
 */
export type CheckpointStaleness =
  | 'valid'           // current fingerprint matches fingerprintAtCreation
  | 'stale'           // fingerprint has changed since checkpoint was created
  | 'artifact_missing' // fingerprint matches but artifact no longer exists in store
  | 'unknown';        // checkpoint has never been validated against a run

/**
 * A named, explicit, user-created snapshot of a node's execution output.
 * Stored in CheckpointRegistry keyed by nodeId.
 */
export type CheckpointRecord = {
  id: string;                        // uuid v4
  name: string;                      // user-supplied, required, non-empty
  description?: string;              // optional user notes
  nodeId: string;                    // node this checkpoint belongs to
  graphId: string;                   // graph (or component revision graphId) where created
  runId: string;                     // run that produced the artifact
  artifactId: string;                // artifact reference
  execKey: string;                   // execution key reference
  fingerprintAtCreation: MemoKey;    // MemoKey at time of checkpoint creation
  createdAt: string;                 // ISO 8601
  staleness: CheckpointStaleness;    // updated after each run
  outputs?: Record<string, {         // per-output-handle artifact refs (for component nodes)
    artifactId: string;
    execKey?: string;
  }>;
};

/**
 * Per-graph registry mapping nodeId → the single active CheckpointRecord for that node.
 * Only one checkpoint per node is active at a time. Historical checkpoints are not retained
 * in the registry (they may be stored separately in a future audit log feature).
 */
export type CheckpointRegistry = Record<string, CheckpointRecord>;

/**
 * Payload shape sent in the run request replacing legacy pinnedNodeIds + pinnedArtifacts.
 */
export type CheckpointExecutionHints = {
  checkpoints: Record<string, {       // nodeId → checkpoint artifact ref
    artifactId: string;
    execKey: string;
    fingerprintAtCreation: MemoKey;
    outputs?: Record<string, { artifactId: string; execKey?: string }>;
  }>;
};
```

**Contents of `src/lib/flow/schema/checkpoint.ts`:**
```typescript
import { z } from 'zod';

export const MemoKeySchema = z.string().regex(/^[0-9a-f]{64}$/, 'MemoKey must be 64-char hex');

export const CheckpointStalenessSchema = z.enum(['valid', 'stale', 'artifact_missing', 'unknown']);

export const CheckpointRecordSchema = z.object({
  id:                   z.string().uuid(),
  name:                 z.string().min(1),
  description:          z.string().optional(),
  nodeId:               z.string().min(1),
  graphId:              z.string().min(1),
  runId:                z.string().min(1),
  artifactId:           z.string().min(1),
  execKey:              z.string().min(1),
  fingerprintAtCreation: MemoKeySchema,
  createdAt:            z.string().datetime(),
  staleness:            CheckpointStalenessSchema,
  outputs: z.record(z.object({
    artifactId: z.string().min(1),
    execKey:    z.string().optional(),
  })).optional(),
});

export const CheckpointRegistrySchema = z.record(CheckpointRecordSchema);

export const CheckpointExecutionHintsSchema = z.object({
  checkpoints: z.record(z.object({
    artifactId:            z.string().min(1),
    execKey:               z.string().min(1),
    fingerprintAtCreation: MemoKeySchema,
    outputs: z.record(z.object({
      artifactId: z.string().min(1),
      execKey:    z.string().optional(),
    })).optional(),
  })),
});
```

**Update `src/lib/flow/types/base.ts`:** Add `checkpointRegistry?: CheckpointRegistry` to `GraphState` (or wherever the store state is typed), imported from `./checkpoint`.

**Tests to write** in `src/lib/flow/schema/checkpoint.test.ts`:
```
- CheckpointRecordSchema.parse() accepts a fully valid record
- CheckpointRecordSchema.parse() rejects missing name (empty string)
- CheckpointRecordSchema.parse() rejects non-uuid id
- MemoKeySchema.parse() rejects a 63-char hex string
- MemoKeySchema.parse() rejects a non-hex string
- CheckpointRegistrySchema.parse() accepts empty object {}
- CheckpointExecutionHintsSchema.parse() roundtrips a record with outputs
```

**Regression gate:** All baseline tests pass. `tsc --noEmit` error count does not increase (new schemas must be type-correct from the start).

---

### Step 4 — Add memoizable Flag to Node Type System

**Goal:** Allow nodes to declare themselves non-memoizable so the backend never caches their results. Needed before fingerprinting so the backend knows which nodes to skip.

**Files to change:**
- `src/lib/flow/types/base.ts`
- `src/lib/flow/schema/base.ts`

**In `src/lib/flow/types/base.ts`**, add to `NodeMeta`:
```typescript
/**
 * When false, the backend memoization cache will never store or reuse this node's output.
 * Defaults to true for all node kinds except those that are non-deterministic by nature
 * (consume_once sources, external API sources without idempotency guarantees).
 * Set explicitly via node schema defaults or per-node user override.
 */
memoizable?: boolean;
```

**In `src/lib/flow/schema/base.ts`**, add to the meta object schema:
```typescript
memoizable: z.boolean().optional(),
```

**In `src/lib/flow/schema/sourceDefaults.ts`**, update the defaults for `api` and `stream` source kinds to include `meta: { memoizable: false }`. File sources (`csv`, `json`, `parquet`, `excel`, `text`) default to `memoizable: true` (omit the field, let it default).

**In `backend/app/runner/run.py`**, add a helper:
```python
def _node_is_memoizable(node: Dict[str, Any] | None) -> bool:
    """Returns True (default) unless node.data.meta.memoizable is explicitly False."""
    if not isinstance(node, dict):
        return True
    data = node.get("data") if isinstance(node.get("data"), dict) else {}
    meta = data.get("meta") if isinstance(meta := data.get("meta"), dict) else {}
    val = meta.get("memoizable")
    if val is False:
        return False
    return True
```

**Tests to write** in `src/lib/flow/schema/base.test.ts` (or extend existing):
```
- NodeMetaSchema.parse() accepts { memoizable: false }
- NodeMetaSchema.parse() accepts { memoizable: true }
- NodeMetaSchema.parse() accepts {} (memoizable absent, defaults to undefined)
- NodeMetaSchema.parse() rejects { memoizable: 'yes' } (must be boolean)
```

Add a Python unit test in `backend/tests/unit/test_node_helpers.py`:
```
- _node_is_memoizable returns True for node with no meta
- _node_is_memoizable returns True for node with meta.memoizable = True
- _node_is_memoizable returns False for node with meta.memoizable = False
- _node_is_memoizable returns True for node with meta.memoizable = null
```

**Regression gate:** All baseline tests pass. `tsc --noEmit` unchanged.

---

## Phase 3 — Backend Memoization Infrastructure

### Step 5 — Backend: Node Fingerprint Computation

**Goal:** Implement the deterministic Merkle fingerprint that keys the memoization cache. This is a pure utility function — nothing in the run path calls it yet.

**File to create:** `backend/app/runner/memo.py`

**Contents:**
```python
"""
Memoization fingerprint computation for DAG nodes.

A node's MemoKey is a SHA-256 hash of:
  - node kind (str)
  - canonical JSON of node params (keys sorted, floats rounded to 8 sig figs)
  - sorted list of input artifact IDs from upstream nodes

The hash is a Merkle structure: changing any upstream artifact ID changes every
downstream node's MemoKey, so cache invalidation propagates automatically.

Non-memoizable nodes (meta.memoizable = False) return None and are never cached.
"""

import hashlib
import json
from typing import Any, Dict, List, Optional, Sequence


def _canonical_params(params: Any) -> str:
    """Produce a deterministic JSON string from params, sorting keys, normalizing floats."""
    def _normalize(v: Any) -> Any:
        if isinstance(v, float):
            # Round to 8 significant figures to avoid float representation noise
            return float(f'{v:.8g}')
        if isinstance(v, dict):
            return {k: _normalize(val) for k, val in sorted(v.items())}
        if isinstance(v, (list, tuple)):
            return [_normalize(i) for i in v]
        return v
    return json.dumps(_normalize(params), sort_keys=True, separators=(',', ':'))


def compute_memo_key(
    node_kind: str,
    params: Any,
    input_artifact_ids: Sequence[str],
) -> str:
    """
    Compute a 64-char hex SHA-256 MemoKey for a node.

    Args:
        node_kind: The node's kind string (e.g. 'llm', 'transform', 'source').
        params: The node's params dict (from node.data.params).
        input_artifact_ids: Sorted list of artifactId strings from all upstream
            edges in topological order. Empty list for source nodes.

    Returns:
        64-character lowercase hex SHA-256 digest.
    """
    parts = [
        f'kind:{node_kind}',
        f'params:{_canonical_params(params)}',
        f'inputs:{json.dumps(sorted(str(a) for a in input_artifact_ids), separators=(",", ":"))}',
    ]
    payload = '\n'.join(parts).encode('utf-8')
    return hashlib.sha256(payload).hexdigest()


def compute_memo_key_for_node(
    node: Dict[str, Any],
    input_artifact_ids: Sequence[str],
) -> Optional[str]:
    """
    Compute a MemoKey for a pipeline node dict.
    Returns None if node is non-memoizable (meta.memoizable = False).
    """
    from backend.app.runner.run import _node_is_memoizable  # avoid circular at module level
    if not _node_is_memoizable(node):
        return None
    data = node.get('data') if isinstance(node.get('data'), dict) else {}
    kind = str(data.get('kind') or '').strip()
    params = data.get('params') or {}
    return compute_memo_key(kind, params, input_artifact_ids)
```

**Tests to write** in `backend/tests/unit/test_memo.py`:
```
test_canonical_params_sorts_keys:
    params_a = {'b': 1, 'a': 2}
    params_b = {'a': 2, 'b': 1}
    assert _canonical_params(params_a) == _canonical_params(params_b)

test_canonical_params_normalizes_floats:
    # 0.1 + 0.2 in float vs 0.3 should produce the same string after rounding
    assert _canonical_params({'x': 0.30000000000000004}) == _canonical_params({'x': 0.3})

test_compute_memo_key_deterministic:
    k1 = compute_memo_key('llm', {'temperature': 0.7, 'model': 'gpt-4'}, ['art1', 'art2'])
    k2 = compute_memo_key('llm', {'model': 'gpt-4', 'temperature': 0.7}, ['art1', 'art2'])
    assert k1 == k2

test_compute_memo_key_changes_with_param:
    k1 = compute_memo_key('llm', {'temperature': 0.7}, ['art1'])
    k2 = compute_memo_key('llm', {'temperature': 0.8}, ['art1'])
    assert k1 != k2

test_compute_memo_key_changes_with_input_artifact:
    k1 = compute_memo_key('transform', {}, ['art1'])
    k2 = compute_memo_key('transform', {}, ['art2'])
    assert k1 != k2

test_compute_memo_key_input_order_invariant:
    # inputs are sorted internally
    k1 = compute_memo_key('join', {}, ['art1', 'art2'])
    k2 = compute_memo_key('join', {}, ['art2', 'art1'])
    assert k1 == k2

test_compute_memo_key_returns_64_char_hex:
    k = compute_memo_key('source', {}, [])
    assert len(k) == 64
    assert all(c in '0123456789abcdef' for c in k)

test_compute_memo_key_for_node_returns_none_when_not_memoizable:
    node = {'data': {'kind': 'source', 'params': {}, 'meta': {'memoizable': False}}}
    assert compute_memo_key_for_node(node, []) is None

test_compute_memo_key_for_node_returns_key_when_memoizable:
    node = {'data': {'kind': 'llm', 'params': {'temperature': 0}, 'meta': {}}}
    key = compute_memo_key_for_node(node, ['art1'])
    assert isinstance(key, str) and len(key) == 64
```

**Regression gate:** All baseline tests pass. New unit tests all pass. `tsc --noEmit` unchanged (Python only in this step).

---

### Step 6 — Backend: Memoization Cache Storage

**Goal:** Create the database model (or in-process store) that maps MemoKey → artifact reference. Implement lookup and store operations. Nothing in the run path calls these yet.

**File to create:** `backend/app/runner/memo_cache.py`

**Contents:**
```python
"""
Memoization cache: maps MemoKey → (artifactId, execKey).

Storage backend is configurable. Default is a simple SQLite table (using the
existing DB session). For development, an in-process LRU dict is used if the
DB session is unavailable.

Cache entries have a configurable TTL (default: 7 days).
Maximum entries per graph: 10,000 (LRU eviction).
"""

import time
from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    pass  # DB session type hints here when integrated

MEMO_CACHE_TTL_SECONDS = 7 * 24 * 3600
MEMO_CACHE_MAX_ENTRIES = 10_000


@dataclass
class MemoCacheEntry:
    memo_key: str
    artifact_id: str
    exec_key: str
    graph_id: str
    node_id: str
    created_at: float  # unix timestamp


class InProcessMemoCache:
    """
    Thread-safe in-process LRU memo cache for development and testing.
    Production deployments should replace with a DB-backed implementation.
    """

    def __init__(self, max_entries: int = MEMO_CACHE_MAX_ENTRIES, ttl: float = MEMO_CACHE_TTL_SECONDS):
        self._store: dict[str, MemoCacheEntry] = {}
        self._max = max_entries
        self._ttl = ttl

    def get(self, memo_key: str) -> Optional[MemoCacheEntry]:
        entry = self._store.get(memo_key)
        if entry is None:
            return None
        if time.time() - entry.created_at > self._ttl:
            del self._store[memo_key]
            return None
        # Move to end (LRU)
        self._store.pop(memo_key)
        self._store[memo_key] = entry
        return entry

    def put(self, entry: MemoCacheEntry) -> None:
        if memo_key := entry.memo_key:
            if len(self._store) >= self._max:
                # Evict oldest entry
                oldest = next(iter(self._store))
                del self._store[oldest]
            self._store[entry.memo_key] = entry

    def invalidate(self, graph_id: str) -> int:
        """Remove all entries for a graph. Returns count removed."""
        keys = [k for k, v in self._store.items() if v.graph_id == graph_id]
        for k in keys:
            del self._store[k]
        return len(keys)

    def clear(self) -> None:
        self._store.clear()

    def __len__(self) -> int:
        return len(self._store)


# Module-level singleton for use in tests and dev. Production code injects via DI.
_dev_cache = InProcessMemoCache()


def get_dev_cache() -> InProcessMemoCache:
    return _dev_cache
```

**Tests to write** in `backend/tests/unit/test_memo_cache.py`:
```
test_put_and_get_returns_entry:
    cache = InProcessMemoCache()
    entry = MemoCacheEntry('abc123...', 'art1', 'exec1', 'graph1', 'node1', time.time())
    cache.put(entry)
    result = cache.get('abc123...')
    assert result is not None
    assert result.artifact_id == 'art1'

test_get_returns_none_for_missing_key:
    cache = InProcessMemoCache()
    assert cache.get('nonexistent') is None

test_expired_entry_returns_none:
    cache = InProcessMemoCache(ttl=0.001)
    entry = MemoCacheEntry('k1', 'art1', 'exec1', 'g1', 'n1', time.time())
    cache.put(entry)
    time.sleep(0.01)
    assert cache.get('k1') is None

test_lru_eviction_removes_oldest:
    cache = InProcessMemoCache(max_entries=2)
    cache.put(MemoCacheEntry('k1', 'a1', 'e1', 'g', 'n', time.time()))
    cache.put(MemoCacheEntry('k2', 'a2', 'e2', 'g', 'n', time.time()))
    cache.put(MemoCacheEntry('k3', 'a3', 'e3', 'g', 'n', time.time()))
    assert cache.get('k1') is None   # evicted
    assert cache.get('k2') is not None
    assert cache.get('k3') is not None

test_invalidate_removes_graph_entries:
    cache = InProcessMemoCache()
    cache.put(MemoCacheEntry('k1', 'a1', 'e1', 'graph-A', 'n1', time.time()))
    cache.put(MemoCacheEntry('k2', 'a2', 'e2', 'graph-B', 'n2', time.time()))
    removed = cache.invalidate('graph-A')
    assert removed == 1
    assert cache.get('k1') is None
    assert cache.get('k2') is not None
```

**Regression gate:** All baseline tests pass. New unit tests pass. `tsc --noEmit` unchanged.

---

### Step 7 — Backend: Memoization in Run Execution

**Goal:** Integrate fingerprint computation and cache lookup into the run execution path. Before executing any node, check the memo cache. On hit, emit `memo.execute_decision: reuse` trace and skip execution. On miss, execute and store result. This step makes memoization live.

**Files to change:**
- `backend/app/runner/run.py`
- `backend/app/runner/compile.py`

**In `backend/app/runner/compile.py`**, update `compile_plan` to accept a `memo_cache` argument and `resolved_input_artifact_ids_by_node` dict. For each node in topological order, compute its MemoKey and check the cache. If hit, move the node from `execute_nodes` to `memo_hit_nodes` (a new set analogous to `cache_only_nodes`). Add `memo_hit_nodes` to the returned plan.

**In `backend/app/runner/run.py`**, in the main run dispatch loop:
1. After the existing artifact resolution step (where `input_artifact_ids` are available per node), call `compute_memo_key_for_node(node, input_artifact_ids)` from `memo.py`.
2. If `memo_key` is not None, call `memo_cache.get(memo_key)`.
3. If cache hit: emit trace event `memo.execute_decision` with `{ decision: 'reuse', memo_key, artifact_id, node_id }`. Skip execution. Bind the cached artifact to the node.
4. If cache miss: execute normally. After successful execution, call `memo_cache.put(MemoCacheEntry(...))`. Emit trace event `memo.execute_decision` with `{ decision: 'compute', memo_key, node_id }`.
5. If `memo_key` is None (non-memoizable node): execute normally. No trace event emitted for memo.

**New trace event schema** (add to run trace constants):
```python
TRACE_MEMO_EXECUTE_DECISION = 'memo.execute_decision'
# payload fields: decision ('reuse'|'compute'|'skip_non_memoizable'), memo_key, node_id, artifact_id (on reuse)
```

**Tests to write** in `backend/tests/integration/test_memoization.py`:

```
test_second_run_hits_memo_cache_for_all_nodes:
    # Run a 3-node graph twice with identical inputs and params.
    # First run: all nodes compute (0 memo hits).
    # Second run: all nodes reuse from cache (3 memo hits).
    # Verify via memo.execute_decision trace events.

test_changed_param_invalidates_node_and_dependents:
    # Run graph [A → B → C]. Change B's param. Run again.
    # A: memo hit (params unchanged).
    # B: memo miss (param changed).
    # C: memo miss (B's output artifact changed → C's input artifact IDs changed).

test_non_memoizable_source_always_computes:
    # Source node with meta.memoizable = False.
    # Run twice with identical inputs.
    # Source always emits memo.execute_decision: skip_non_memoizable.
    # Never a memo hit.

test_memo_cache_invalidated_on_graph_clear:
    # After calling memo_cache.invalidate(graph_id), subsequent run recomputes all nodes.

test_upstream_pin_and_memo_coexist:
    # Legacy: node A has meta.freeze sticky. Node B depends on A.
    # A uses pin (legacy path). B checks memo cache using A's artifact as input.
    # B gets memo hit on second run.
    # This test ensures old pins and new memo don't conflict during transition period.
```

**Regression gate:** All existing pin-related integration tests pass (legacy path still active). New memoization tests pass. `tsc --noEmit` unchanged. Run the `MIGRATION_BASELINE.md` test list explicitly.

---

## Phase 4 — Frontend Memoization Display

### Step 8 — Frontend: Expose MemoKey and MemoState in Node Run Status

**Goal:** Surface memo cache decisions in the frontend so the UI can show whether a node was reused from cache or freshly computed. No new UI components yet — just the data plumbing.

**Files to change:**
- `src/lib/flow/types/base.ts`
- `src/lib/flow/store/graphStore.run.ts` (run trace consumption)

**In `src/lib/flow/types/base.ts`**, add to `NodeBindingInfo` or wherever per-node run state is stored:
```typescript
memoState?: {
  decision: 'reuse' | 'compute' | 'skip_non_memoizable';
  memoKey?: string;
  resolvedAt?: string; // ISO — timestamp from the run trace
};
```

**In `src/lib/flow/store/graphStore.run.ts`**, in the run trace event consumer (the function that processes incoming trace messages from the backend WebSocket), handle `memo.execute_decision` events:
```typescript
case 'memo.execute_decision': {
  const { node_id, decision, memo_key } = tracePayload;
  // Write memoState into nodeBindings[node_id]
  update(s => ({
    ...s,
    nodeBindings: {
      ...s.nodeBindings,
      [node_id]: {
        ...s.nodeBindings[node_id],
        memoState: {
          decision,
          memoKey: memo_key ?? undefined,
          resolvedAt: new Date().toISOString(),
        }
      }
    }
  }));
  break;
}
```

**Tests to write** in `src/lib/flow/store/graphStore.memoState.test.ts`:
```
test_memo_state_written_on_reuse_trace_event:
    // Simulate receiving a memo.execute_decision trace with decision='reuse'
    // Verify nodeBindings[nodeId].memoState.decision === 'reuse'

test_memo_state_written_on_compute_trace_event:
    // decision='compute' → memoState.decision === 'compute'

test_memo_state_cleared_on_run_start:
    // When a new run starts, memoState for all nodes should be reset to undefined
    // (so stale memo indicators from previous runs don't persist)

test_memo_state_preserved_across_node_param_updates:
    // Updating a node's params (which marks it dirty) should NOT clear memoState
    // until the next run starts — the last-run state should remain visible
```

**Regression gate:** All baseline tests pass. New tests pass. `tsc --noEmit` unchanged.

---

### Step 9 — Frontend: Remove Manual Pin Toggle, Add Memo Cache Indicator

**Goal:** Remove the "Pin (per-run)" and "Pin (sticky)" controls from the node toolbar/inspector. Add a non-interactive memo cache indicator showing the last run's memo decision. This is the first visible UI change.

**Files to change:**
- The node toolbar Svelte component (find via `Grep 'per_run\|sticky\|freeze\|setSelectedNodeFreezeMode' src --include='*.svelte'`)
- The node inspector Svelte component (same search)
- Node status display component

**Actions:**

Remove all UI controls that call `setSelectedNodeFreezeMode` or `setNodeFreezeMode`. Do not remove the underlying functions yet (they're still called by legacy code and tests).

Add a `MemoIndicator` component:
```svelte
<!-- src/lib/flow/components/nodes/MemoIndicator.svelte -->
<script lang="ts">
  export let memoState: { decision: string } | undefined;
</script>

{#if memoState?.decision === 'reuse'}
  <span class="memo-badge memo-hit" title="Result reused from memo cache">⚡ cached</span>
{:else if memoState?.decision === 'compute'}
  <span class="memo-badge memo-miss" title="Freshly computed">↻ computed</span>
{/if}
```

Display `MemoIndicator` in the node status area (below run time or next to the status badge).

**Tests to write** (Svelte component tests or visual regression):
```
test_memo_indicator_shows_cached_when_decision_is_reuse
test_memo_indicator_shows_computed_when_decision_is_compute
test_memo_indicator_hidden_when_memoState_undefined
test_pin_toggle_controls_not_present_in_node_toolbar
test_pin_toggle_controls_not_present_in_inspector
```

**Regression gate:** All baseline tests pass (the underlying pin functions still exist — only UI controls removed). `tsc --noEmit` unchanged.

---

## Phase 5 — Checkpoint Data Model and Storage

### Step 10 — Add CheckpointRegistry to GraphState and Persistence

**Goal:** Integrate `CheckpointRegistry` into the graph store state and persist it alongside the graph DTO.

**Files to change:**
- `src/lib/flow/types/base.ts` — add `checkpointRegistry: CheckpointRegistry` to `GraphState`
- `src/lib/flow/store/persist.ts` — extend `saveGraphToLocalStorage` / `loadGraphFromLocalStorage` to include checkpoint registry
- `src/lib/flow/store/graphStore.persistence.ts` — include `checkpointRegistry` in `buildPersistableGraphStrict` output and restore it in `applyGraphDocument`

**In `src/lib/flow/types/base.ts`**, add to `GraphState`:
```typescript
import type { CheckpointRegistry } from './checkpoint';
// ...
checkpointRegistry: CheckpointRegistry; // always present, may be empty {}
```

**In `src/lib/flow/store/persist.ts`**, extend `PipelineGraphDTO` (or wherever it is typed) to include:
```typescript
checkpointRegistry?: CheckpointRegistry;
```

Update `saveGraphToLocalStorage` to write `checkpointRegistry` from state.

Update `loadGraphFromLocalStorage` to restore `checkpointRegistry`, defaulting to `{}` if absent (handles old persisted graphs without checkpoints).

**In `src/lib/flow/store/graphStore.persistence.ts`**, in `buildPersistableGraphStrict`, include `checkpointRegistry: state.checkpointRegistry ?? {}`.

In `applyGraphDocument`, restore `checkpointRegistry: dto.checkpointRegistry ?? {}`.

Initialize `checkpointRegistry: {}` in `emptyGraph` and `hardResetGraph`.

**Tests to write** in `src/lib/flow/store/graphStore.checkpoint.test.ts`:
```
test_checkpoint_registry_initializes_empty:
    graphStore.hardResetGraph();
    const state = get(graphStore);
    expect(state.checkpointRegistry).toEqual({});

test_checkpoint_registry_survives_persist_and_reload:
    // Insert a CheckpointRecord into state.checkpointRegistry via direct update
    // Call persist()
    // Load from localStorage
    // Verify the record is present with all fields intact

test_checkpoint_registry_defaults_to_empty_when_loading_legacy_graph:
    // Load a graph DTO that has no checkpointRegistry field
    // Verify state.checkpointRegistry === {}

test_checkpoint_registry_included_in_buildPersistableGraphStrict_output:
    // Call buildPersistableGraphStrict with a state that has a registry entry
    // Verify the output DTO includes checkpointRegistry with that entry
```

**Regression gate:** All baseline tests pass. `tsc --noEmit` error count does not increase. The `checkpointRegistry` field must typecheck correctly on `GraphState`.

---

### Step 11 — Persist Component Revision Checkpoints

**Goal:** When a component has internal nodes with checkpoints, those checkpoints must survive into the component revision and be restored when the component is opened for editing. This closes the component inner-node persistence gap.

**Files to change:**
- `src/lib/flow/store/graphStore.persistence.ts`

**In `returnFromComponentEditSession`** (line ~1392): when building the draft cache entry for `__graphDraft`, include the current `checkpointRegistry` filtered to the internal nodes:
```typescript
const internalNodeIds = new Set(internalNodes.map(n => n.id));
const internalCheckpoints = Object.fromEntries(
  Object.entries(state.checkpointRegistry ?? {})
    .filter(([nodeId]) => internalNodeIds.has(nodeId))
);
draftCacheEntry[COMPONENT_DRAFT_GRAPH_KEY] = {
  nodes: internalNodes,
  edges: internalEdges,
  checkpointRegistry: internalCheckpoints,  // ADD THIS
};
```

**In `openComponentRevisionForEditing`** (line ~1283): when restoring internal nodes from the draft cache or revision, also restore `checkpointRegistry` from the draft's `checkpointRegistry` field, merged with any checkpoints already in the revision:
```typescript
const draftCheckpoints = draftEntry?.checkpointRegistry ?? {};
const revisionCheckpoints = revisionGraph?.checkpointRegistry ?? {};
// Draft checkpoints take precedence over revision (draft is more recent)
const mergedCheckpoints = { ...revisionCheckpoints, ...draftCheckpoints };
```

**In `saveComponentRevision`**: include the filtered `checkpointRegistry` for internal nodes in the payload sent to the backend.

**Tests to write** in `graphStore.component.test.ts` (extend existing):
```
test_internal_checkpoint_survives_returnFromComponentEditSession:
    // Enter component edit, add a CheckpointRecord for an internal node,
    // return from edit session, re-enter edit session,
    // verify the checkpoint is still in state.checkpointRegistry

test_internal_checkpoint_included_in_draft_cache:
    // After returnFromComponentEditSession, inspect componentContractDraftCache
    // Verify the draft cache entry contains checkpointRegistry

test_internal_checkpoint_not_visible_in_parent_graph_registry:
    // After returning from component edit session, the parent graph's
    // checkpointRegistry should not contain the internal node IDs
    // (internal checkpoints are scoped to the component revision)

test_legacy_component_draft_without_checkpoints_loads_cleanly:
    // Load a draft cache entry that has no checkpointRegistry field
    // Verify no errors and checkpointRegistry defaults to {}
```

**Regression gate:** All existing component tests pass. All new tests pass. `tsc --noEmit` unchanged.

---

## Phase 6 — Backend Checkpoint Integration

### Step 12 — Backend: Parse Checkpoint Execution Hints

**Goal:** Teach the backend run path to read `__executionHints.checkpoints` (the new format) in addition to the legacy `pinnedNodeIds` + `pinnedArtifacts`. Both formats must work simultaneously during the transition period.

**Files to change:**
- `backend/app/runner/run.py`

In the execution hints parsing section (around line 4018), add a new block after the existing legacy hint parsing:
```python
# New checkpoint hints format (Phase 6+)
checkpoint_hints: Dict[str, Dict[str, Any]] = {}
raw_checkpoints = (execution_hints or {}).get('checkpoints')
if isinstance(raw_checkpoints, dict):
    for node_id, cp in raw_checkpoints.items():
        nid = str(node_id or '').strip()
        artifact_id = str((cp or {}).get('artifactId') or '').strip()
        exec_key = str((cp or {}).get('execKey') or '').strip()
        fingerprint = str((cp or {}).get('fingerprintAtCreation') or '').strip()
        if nid and artifact_id and exec_key and len(fingerprint) == 64:
            checkpoint_hints[nid] = {
                'artifactId': artifact_id,
                'execKey': exec_key,
                'fingerprintAtCreation': fingerprint,
                'outputs': (cp or {}).get('outputs') or {},
            }
```

Add a staleness check: for each `node_id` in `checkpoint_hints`, compute the current `MemoKey` using `compute_memo_key_for_node`. Compare against `fingerprintAtCreation`:
```python
for node_id, cp_hint in checkpoint_hints.items():
    node = nodes_by_id.get(node_id)
    current_key = compute_memo_key_for_node(node, resolved_input_ids.get(node_id, []))
    stored_key = cp_hint['fingerprintAtCreation']
    if current_key == stored_key:
        cp_hint['_staleness'] = 'valid'
    elif current_key is None:
        cp_hint['_staleness'] = 'skip_non_memoizable'
    else:
        cp_hint['_staleness'] = 'stale'
```

Emit a `checkpoint.backend_validate` trace event per checkpoint with: `{ node_id, staleness, current_key, stored_key }`.

For `valid` checkpoints: add node to `cache_only_nodes` (same as legacy pin behavior).
For `stale` checkpoints: do NOT add to cache_only_nodes. Node executes normally.
For missing artifacts: emit `artifact_missing` staleness, do NOT add to cache_only_nodes.

**Tests to write** in `backend/tests/integration/test_checkpoints.py`:

```
test_valid_checkpoint_reuses_artifact:
    # Build a run request with checkpoints hint where fingerprintAtCreation
    # matches what the backend would compute for that node.
    # Verify checkpoint.backend_validate trace shows staleness='valid'.
    # Verify node gets decision 'reuse' (checkpoint honored).

test_stale_checkpoint_triggers_recompute:
    # Build a run request with checkpoints hint where fingerprintAtCreation
    # does NOT match current fingerprint (param was changed).
    # Verify checkpoint.backend_validate trace shows staleness='stale'.
    # Verify node executes normally (decision 'compute').

test_missing_artifact_checkpoint_triggers_recompute_gracefully:
    # Build a run request with a checkpoint whose artifactId doesn't exist.
    # Verify staleness='artifact_missing'.
    # Verify node executes normally. No error raised.

test_legacy_pin_hints_still_honored_during_transition:
    # Build a run request using the OLD pinnedNodeIds + pinnedArtifacts format.
    # Verify backend still honors them (no regression).
    # Both checkpoint hints and pin hints can coexist in same request.
```

**Regression gate:** ALL existing pin integration tests pass unchanged. New checkpoint tests pass. Backend test suite passes. `tsc --noEmit` unchanged.

---

### Step 13 — Backend: Emit Checkpoint Staleness in Run Result

**Goal:** Make the backend's staleness decisions available to the frontend so the UI can update each checkpoint's `staleness` field after a run completes.

**Files to change:**
- `backend/app/runner/run.py`

At run completion, include a `checkpoint_outcomes` field in the run result payload:
```python
checkpoint_outcomes: Dict[str, str] = {}  # nodeId → staleness string
for node_id, cp_hint in checkpoint_hints.items():
    checkpoint_outcomes[node_id] = cp_hint.get('_staleness', 'unknown')
# Include in run_result response
result['checkpoint_outcomes'] = checkpoint_outcomes
```

**In `src/lib/flow/store/graphStore.run.ts`**, in the run completion handler, consume `checkpoint_outcomes` from the result and update `state.checkpointRegistry`:
```typescript
if (runResult.checkpoint_outcomes) {
  update(s => {
    const updatedRegistry = { ...s.checkpointRegistry };
    for (const [nodeId, staleness] of Object.entries(runResult.checkpoint_outcomes)) {
      if (updatedRegistry[nodeId]) {
        updatedRegistry[nodeId] = {
          ...updatedRegistry[nodeId],
          staleness: staleness as CheckpointStaleness,
        };
      }
    }
    return { ...s, checkpointRegistry: updatedRegistry };
  });
}
```

**Tests to write:**
```
test_checkpoint_staleness_updated_after_run_completes_valid:
    // After a run where a valid checkpoint was honored,
    // checkpointRegistry[nodeId].staleness === 'valid'

test_checkpoint_staleness_updated_after_run_completes_stale:
    // After a run where a stale checkpoint was bypassed,
    // checkpointRegistry[nodeId].staleness === 'stale'

test_checkpoint_staleness_not_changed_when_node_not_in_run:
    // A checkpoint for a node that wasn't in this run's scope
    // should keep its previous staleness value unchanged
```

**Regression gate:** All baseline and new tests pass. `tsc --noEmit` unchanged.

---

## Phase 7 — Checkpoint Creation and Management UI

### Step 14 — Frontend: Create Checkpoint Action

**Goal:** Allow the user to create a checkpoint from a successfully-run node. This is the replacement for the "Pin (sticky)" action.

**Files to create/change:**
- `src/lib/flow/store/graphStore.graph-edit.ts` — add `createCheckpoint(nodeId, name, description?)` action
- `src/lib/flow/store/graphStore.ts` — expose `createCheckpoint` and `removeCheckpoint` from the store

**In `graphStore.graph-edit.ts`**, add `createCheckpoint`:
```typescript
function createCheckpoint(
  nodeId: string,
  name: string,
  description?: string
): { ok: true; checkpoint: CheckpointRecord } | { ok: false; error: string } {
  const state = getState();
  const node = state.nodes.find(n => n.id === nodeId);
  if (!node) return { ok: false, error: 'Node not found.' };

  // Reuse existing pin eligibility check (node must be succeeded with current artifact)
  const binding = _normalizeBinding(state.nodeBindings?.[nodeId], nodeId);
  const eligibility = validatePinEligibility(node as any, binding);
  if (!eligibility.ok) return { ok: false, error: eligibility.error };

  const lineage = binding.last?.artifactId ? binding.last : binding.current;
  const artifactId = String(lineage?.artifactId ?? '').trim();
  const execKey = String(lineage?.execKey ?? '').trim();
  if (!artifactId || !execKey) return { ok: false, error: 'No artifact available.' };

  // MemoKey: frontend can't compute it — we use the one stored in memoState from last run
  const memoKey = state.nodeBindings?.[nodeId]?.memoState?.memoKey;
  if (!memoKey) return { ok: false, error: 'No fingerprint available. Run the node first.' };

  const checkpoint: CheckpointRecord = {
    id: crypto.randomUUID(),
    name: name.trim(),
    description,
    nodeId,
    graphId: state.graphId,
    runId: state.lastRunId ?? '',
    artifactId,
    execKey,
    fingerprintAtCreation: memoKey,
    createdAt: new Date().toISOString(),
    staleness: 'valid',  // just created from current run
  };

  update(s => {
    const next = {
      ...s,
      checkpointRegistry: {
        ...s.checkpointRegistry,
        [nodeId]: checkpoint,
      }
    };
    persist(next);
    return next;
  });

  return { ok: true, checkpoint };
}

function removeCheckpoint(nodeId: string): { ok: boolean } {
  update(s => {
    const registry = { ...s.checkpointRegistry };
    delete registry[nodeId];
    const next = { ...s, checkpointRegistry: registry };
    persist(next);
    return next;
  });
  return { ok: true };
}
```

**Update the node toolbar/inspector** to replace "Pin (sticky)" with "Create Checkpoint…" (opens a dialog). The dialog has a name field (required) and description field (optional). On submit, calls `createCheckpoint`.

**Tests to write** in `graphStore.checkpoint.test.ts`:
```
test_createCheckpoint_requires_succeeded_node:
    // Node that has never run → createCheckpoint returns ok:false

test_createCheckpoint_requires_memoState_key:
    // Succeeded node but memoState is undefined (ran before Phase 7 code) → ok:false

test_createCheckpoint_creates_registry_entry:
    // Succeeded node with memoState → createCheckpoint returns ok:true
    // checkpointRegistry[nodeId] has correct fields

test_createCheckpoint_persists_to_localStorage:
    // After createCheckpoint, loadGraphFromLocalStorage includes the record

test_removeCheckpoint_deletes_registry_entry:
    // After removeCheckpoint(nodeId), checkpointRegistry[nodeId] is undefined

test_createCheckpoint_replaces_existing_checkpoint:
    // Creating a second checkpoint for the same nodeId replaces the first
    // (only one active checkpoint per node)
```

**Regression gate:** All baseline tests pass. `tsc --noEmit` unchanged. The old `setSelectedNodeFreezeMode('sticky')` tests in `graphStore.freeze.test.ts` still pass (underlying function still exists, just not exposed in UI).

---

### Step 15 — Frontend: "Create Checkpoint" Replaces per_run Pin

**Goal:** Replace the "Pin (per-run)" behavior with an automatic first-run checkpoint. When a node produces its first successful result, offer a one-click "Save as checkpoint" inline action. No blocking UX — the action is optional.

**Files to change:**
- Node status display component
- Node run completion handler in `graphStore.run.ts`

**In the run completion handler**, after a node succeeds, if it does not already have a checkpoint:
```typescript
// Emit a 'checkpointable' flag into node binding so UI can show the button
update(s => ({
  ...s,
  nodeBindings: {
    ...s.nodeBindings,
    [nodeId]: {
      ...s.nodeBindings[nodeId],
      checkpointable: !s.checkpointRegistry[nodeId],
    }
  }
}));
```

The node status component shows an inline "📌 Save checkpoint" micro-button when `binding.checkpointable === true`. Clicking it opens the checkpoint dialog pre-filled with a generated name (e.g., `"Run #42 — 2026-04-09"`).

This replaces per-run pins entirely: per-run pins auto-cleared after each run; the checkpoint system gives the user the explicit "I want to save this" action instead.

**Tests to write:**
```
test_checkpointable_flag_set_after_node_succeeds_without_checkpoint
test_checkpointable_flag_false_when_checkpoint_already_exists
test_checkpointable_flag_cleared_on_run_start
```

**Regression gate:** All baseline tests pass. `tsc --noEmit` unchanged.

---

### Step 16 — Frontend: Checkpoint Registry Panel

**Goal:** Provide a global panel listing all active checkpoints across the graph, with staleness status and management actions.

**Files to create:**
- `src/lib/flow/components/panels/CheckpointRegistryPanel.svelte`

**Panel features:**
- Table with columns: Node Name, Checkpoint Name, Created, Staleness, Actions
- Staleness column shows color-coded badge: `valid` (green), `stale` (amber), `artifact_missing` (red), `unknown` (grey)
- Actions per row: "Remove", "Rename" (inline edit of name field)
- Global actions: "Remove all stale", "Remove all"
- Empty state: "No checkpoints. Run a node and save its output as a checkpoint."

**Integrate** the panel into the existing sidebar/panel system. Add a "Checkpoints" tab or icon button alongside the existing inspector/history panels.

**Tests to write** (Svelte component test or E2E):
```
test_panel_renders_empty_state_when_no_checkpoints
test_panel_renders_checkpoint_row_with_correct_name_and_staleness
test_panel_remove_button_calls_removeCheckpoint
test_panel_remove_all_stale_removes_only_stale_entries
test_panel_staleness_badge_color_matches_staleness_value
```

**Regression gate:** All baseline tests pass. `tsc --noEmit` unchanged.

---

### Step 17 — Frontend: Checkpoint Hints in Run Payload

**Goal:** Replace the legacy `pinnedNodeIds` + `pinnedArtifacts` execution hints with `checkpoints` hints built from `state.checkpointRegistry`. The build happens in `runScope.ts`.

**Files to change:**
- `src/lib/flow/store/runScope.ts` — extend `buildRunCreateRequest` to accept and emit `CheckpointExecutionHints`
- `src/lib/flow/store/graphStore.run.ts` — pass `checkpointRegistry` to run payload builder

**In `buildRunCreateRequest`**, add a `checkpoints` parameter of type `CheckpointRegistry | undefined`. Build the `executionHints.checkpoints` object:
```typescript
const sanitizedCheckpoints: CheckpointExecutionHints['checkpoints'] = {};
if (checkpoints) {
  for (const [nodeId, cp] of Object.entries(checkpoints)) {
    const nid = String(nodeId ?? '').trim();
    const aid = String(cp.artifactId ?? '').trim();
    const ek = String(cp.execKey ?? '').trim();
    const fp = String(cp.fingerprintAtCreation ?? '').trim();
    if (!nid || !aid || !ek || fp.length !== 64) continue;
    sanitizedCheckpoints[nid] = {
      artifactId: aid,
      execKey: ek,
      fingerprintAtCreation: fp,
      ...(cp.outputs ? { outputs: cp.outputs } : {}),
    };
  }
}
if (Object.keys(sanitizedCheckpoints).length > 0) {
  executionHints.checkpoints = sanitizedCheckpoints;
}
```

**In `graphStore.run.ts`**, in the run dispatch section, pass `state.checkpointRegistry` as the `checkpoints` argument alongside the legacy `pinnedNodeIds` and `pinnedArtifacts` (both still present during transition). The backend handles both formats simultaneously.

**Tests to write** in `runScope.test.ts` (extend existing):
```
test_buildRunCreateRequest_includes_checkpoints_hints_from_registry:
    // Pass a checkpointRegistry with one valid entry
    // Verify output payload has __executionHints.checkpoints[nodeId] with correct fields

test_buildRunCreateRequest_omits_checkpoint_with_invalid_fingerprint:
    // CheckpointRecord with fingerprintAtCreation = 'not64chars'
    // Verify it is dropped from the hints

test_buildRunCreateRequest_includes_both_legacy_pins_and_checkpoints_during_transition:
    // Pass both pinnedNodeIds and checkpointRegistry
    // Verify both appear in the payload

test_buildRunCreateRequest_sanitizes_empty_registry_to_no_hints:
    // Empty checkpointRegistry → executionHints has no 'checkpoints' key
```

**Regression gate:** All baseline tests pass. All `runScope.test.ts` tests pass. `tsc --noEmit` unchanged.

---

## Phase 8 — Component Revision Checkpoint Visibility

### Step 18 — Component Node Badge: Checkpoint Count

**Goal:** In the parent graph, component instance nodes display a badge showing how many active checkpoints (and how many stale) exist in their internal revision.

**Files to change:**
- `src/lib/flow/store/graphStore.persistence.ts` — surface component checkpoint summary in component node data
- Component node Svelte component

**When returning from a component edit session** (`returnFromComponentEditSession`), compute a checkpoint summary and store it on the component node's data:
```typescript
const internalCheckpoints = Object.values(internalCheckpointRegistry);
const staleCount = internalCheckpoints.filter(c => c.staleness === 'stale' || c.staleness === 'artifact_missing').length;
const validCount = internalCheckpoints.filter(c => c.staleness === 'valid').length;

// Write to the component instance node in the parent graph
nodes = nodes.map(n => {
  if (n.id !== componentNodeId) return n;
  return {
    ...n,
    data: {
      ...n.data,
      meta: {
        ...(n.data as any).meta,
        checkpointSummary: {
          total: internalCheckpoints.length,
          valid: validCount,
          stale: staleCount,
        }
      }
    }
  };
});
```

**Add `checkpointSummary` to `NodeMeta`** in `src/lib/flow/types/base.ts`:
```typescript
checkpointSummary?: {
  total: number;
  valid: number;
  stale: number;
};
```

**In the component node Svelte component**, render the badge when `data.meta.checkpointSummary.total > 0`. Color it amber if `stale > 0`, green otherwise.

**Tests to write:**
```
test_component_node_badge_shows_checkpoint_count_after_save:
    // Pin two internal nodes, return from component edit, verify
    // component node data.meta.checkpointSummary.total === 2

test_component_node_badge_shows_stale_count:
    // One valid, one stale → badge shows stale:1

test_component_node_badge_absent_when_no_checkpoints:
    // No internal checkpoints → checkpointSummary is absent or total === 0
```

**Regression gate:** All baseline tests pass. `tsc --noEmit` unchanged.

---

### Step 19 — Pre-Run Guard: Unsaved Component Checkpoint Changes

**Goal:** Before dispatching a run, detect if any component instance has a draft cache entry whose checkpoint state differs from what was last committed to the revision. If so, block run and prompt "Apply revision" or "Run without saving checkpoint changes."

**Files to change:**
- `src/lib/flow/store/graphStore.run.ts` — add pre-run validation step
- `src/lib/flow/store/graphStore.persistence.ts` — expose a `hasUnsavedCheckpointChanges(componentNodeId)` query

**In `graphStore.persistence.ts`**, add:
```typescript
function hasUnsavedCheckpointChanges(componentNodeId: string): boolean {
  const state = getState();
  const cacheKey = buildDraftCacheKey(componentNodeId, state.graphId);
  const draft = state.componentContractDraftCache?.[cacheKey];
  if (!draft) return false;
  const draftCheckpoints = draft[COMPONENT_DRAFT_GRAPH_KEY]?.checkpointRegistry ?? {};
  const revisionCheckpoints = draft.lastCommittedCheckpointRegistry ?? {};
  return JSON.stringify(draftCheckpoints) !== JSON.stringify(revisionCheckpoints);
}
```

**In `graphStore.run.ts`**, in the pre-run validation block (before building the run payload):
```typescript
const componentNodesWithUnsavedCheckpoints = state.nodes
  .filter(n => (n.data as any).kind === 'component')
  .filter(n => hasUnsavedCheckpointChanges(n.id));

if (componentNodesWithUnsavedCheckpoints.length > 0) {
  update(s => ({
    ...s,
    runBlockedReason: {
      type: 'unsaved_checkpoint_changes',
      componentNodeIds: componentNodesWithUnsavedCheckpoints.map(n => n.id),
      message: `Component${componentNodesWithUnsavedCheckpoints.length > 1 ? 's' : ''} have unsaved checkpoint changes. Apply revision to honor them.`,
    }
  }));
  return; // abort run
}
```

**In the UI**, when `runBlockedReason.type === 'unsaved_checkpoint_changes'` is set, show an inline warning with two actions: "Apply revision and run" (triggers save then run) and "Run without checkpoint changes" (clears the blocked reason and runs without the unsaved checkpoints).

**Tests to write:**
```
test_run_blocked_when_component_has_unsaved_checkpoint_changes:
    // Enter component edit, create checkpoint, do NOT save, attempt run
    // Verify runBlockedReason.type === 'unsaved_checkpoint_changes'

test_run_not_blocked_when_component_checkpoints_are_saved:
    // Enter component edit, create checkpoint, save, return, attempt run
    // Verify runBlockedReason is null

test_run_not_blocked_when_component_has_no_checkpoints:
    // Component with no internal checkpoints → no block

test_run_proceeds_after_run_without_checkpoint_changes_action:
    // Block raised → user chooses "Run without checkpoint changes" → run dispatches
```

**Regression gate:** All baseline tests pass. Existing component run tests pass. `tsc --noEmit` unchanged.

---

## Phase 9 — Legacy Migration

### Step 20 — Migrate Existing Graphs: Legacy Pins → Checkpoints

**Goal:** When loading a graph that contains legacy `meta.freeze` / `meta.freezeLineage` fields, automatically convert them to `CheckpointRecord` entries and store in `checkpointRegistry`. One-way migration runs once per graph on load.

**Files to change:**
- `src/lib/flow/store/graphStore.persistence.ts` — add migration function called from `applyGraphDocument`
- `src/lib/flow/store/persist.ts` — version-stamp the migrated graph

**Add migration function:**
```typescript
function migrateFreezePinsToCheckpoints(
  nodes: Node<PipelineNodeData & Record<string, unknown>>[],
  existingRegistry: CheckpointRegistry,
  graphId: string,
): { nodes: Node<any>[]; checkpointRegistry: CheckpointRegistry } {
  const registry = { ...existingRegistry };
  const migratedNodes = nodes.map(node => {
    const meta = (node.data as any)?.meta ?? {};
    const freeze = meta.freeze;
    const freezeLineage = meta.freezeLineage;

    if (!freeze?.enabled || !freezeLineage?.artifactId || !freezeLineage?.execKey) {
      return node;
    }

    // Only migrate if no checkpoint already exists for this node
    if (!registry[node.id]) {
      const checkpoint: CheckpointRecord = {
        id: crypto.randomUUID(),
        name: `Migrated pin (${freeze.mode ?? 'sticky'})`,
        description: 'Automatically migrated from legacy pin system.',
        nodeId: node.id,
        graphId,
        runId: '',                   // unknown — legacy pins didn't record runId
        artifactId: freezeLineage.artifactId,
        execKey: freezeLineage.execKey,
        fingerprintAtCreation: '0'.repeat(64), // unknown — will show as 'unknown' staleness
        createdAt: meta.updatedAt ?? new Date().toISOString(),
        staleness: 'unknown',        // will be resolved on next run
        outputs: freezeLineage.outputs,
      };
      registry[node.id] = checkpoint;
    }

    // Strip legacy fields from node meta
    const { freeze: _f, freezeLineage: _fl, ...cleanMeta } = meta;
    return {
      ...node,
      data: { ...node.data, meta: cleanMeta }
    };
  });

  return { nodes: migratedNodes, checkpointRegistry: registry };
}
```

**Call `migrateFreezePinsToCheckpoints`** inside `applyGraphDocument` before returning the new state. Pass the result's `checkpointRegistry` to the state. Pass the migrated nodes (with legacy fields stripped) as the new `nodes`.

**Tests to write** in `graphStore.checkpoint.test.ts`:
```
test_legacy_pin_migrated_to_checkpoint_on_load:
    // Load graph with node that has meta.freeze.enabled=true and meta.freezeLineage
    // Verify checkpointRegistry has a CheckpointRecord for that nodeId
    // Verify node.data.meta.freeze is undefined after migration
    // Verify node.data.meta.freezeLineage is undefined after migration

test_migrated_checkpoint_has_staleness_unknown:
    // Migrated checkpoint has unknown fingerprintAtCreation → staleness = 'unknown'

test_migration_skips_node_if_checkpoint_already_exists:
    // Node has legacy pin AND already has an entry in checkpointRegistry
    // Migration leaves the existing checkpoint intact, still strips legacy fields

test_migration_handles_node_with_freeze_but_no_lineage_gracefully:
    // Node has meta.freeze.enabled=true but no freezeLineage
    // No checkpoint created (no artifact to reference), legacy fields stripped

test_legacy_graph_without_any_pins_loads_with_empty_registry:
    // Graph with no meta.freeze on any node → checkpointRegistry stays {}
```

**Regression gate:** All baseline tests pass. All `graphStore.freeze.test.ts` tests still pass (they test legacy functions that still exist). `tsc --noEmit` unchanged.

---

## Phase 10 — Legacy Code Removal

### Step 21 — Remove Legacy Pin Functions From Frontend

**Goal:** Delete all deprecated pin functions, types, and schema fields. This is the final cleanup step. Do not proceed until all prior steps are confirmed passing.

**Pre-condition check:** Run the full test suite. Every test that previously exercised legacy pin behavior must either:
(a) Have been updated in a prior step to use the checkpoint API, OR
(b) Have been deleted because the behavior it tested is no longer valid.

No test should be testing `setNodeFreezeMode`, `collectPinnedNodeIds`, `collectPinnedArtifactsByNode`, `clearPerRunPinsOnNodes`, or `nodeFreezeMode` at this point.

**Files to change:**

`src/lib/flow/types/base.ts`:
- Remove `freeze?: { enabled?: boolean; mode?: 'per_run' | 'sticky'; }` from `NodeMeta`
- Remove `freezeLineage?: { ... }` from `NodeMeta`

`src/lib/flow/schema/base.ts`:
- Remove `freeze` from the `meta` Zod schema

`src/lib/flow/store/graphStore.run.ts`:
- Delete `collectPinnedNodeIds`
- Delete `collectPinnedArtifactsByNode`
- Delete `clearPerRunPinsOnNodes`
- Delete `validatePinEligibility` (replaced by the same validation inside `createCheckpoint`)
- Delete all `tracePinXxx` functions
- Delete `__setPinHintTraceEnabledForTest`, `getPinHintTraceEnabled`
- Delete `__collectPinnedArtifactsByNodeForTest`, `__validatePinEligibilityForTest`
- Remove legacy `pinnedNodeIds` and `pinnedArtifacts` from run dispatch call sites (checkpoint hints only)

`src/lib/flow/store/graphStore.graph-edit.ts`:
- Delete `setNodeFreezeMode`
- Delete `setSelectedNodeFreezeMode`

`src/lib/flow/store/graphStore.inspector.ts`:
- Delete `nodeFreezeMode`

`src/lib/flow/store/runScope.ts`:
- Remove `pinnedNodeIds` and `pinnedArtifacts` parameters from `buildRunCreateRequest`
- Update to accept only `checkpoints: CheckpointRegistry | undefined`

**Delete test files** that tested only legacy behavior:
- `src/lib/flow/store/graphStore.freeze.test.ts` — replace with `graphStore.checkpoint.test.ts` (already written)
- Any tests in `graphStore.runRemotePinnedHintsAfterReset.test.ts` that tested `collectPinnedArtifactsByNode` directly (update to test checkpoint hint building instead)

**Tests to write** (replacement tests for deleted tests):
```
test_checkpoint_hints_assembled_in_run_payload_from_registry:
    // Replaces the former collectPinnedArtifactsByNode tests
    // State has checkpointRegistry with one entry
    // buildRunCreateRequest produces __executionHints.checkpoints with that entry

test_stale_propagation_stops_at_checkpointed_node:
    // Replaces the former stale-propagation-stops-at-pinned-node test
    // A node with a checkpoint in the registry should still block upstream stale propagation
    // (same behavioral guarantee, different mechanism)
    // Note: stale propagation blocking is now driven by checkpointRegistry membership,
    // not meta.freeze. Update markStaleFromNode to check checkpointRegistry.

test_param_change_does_not_auto_clear_checkpoint:
    // Replaces the former "params change auto-clears pin" test
    // Changing params should NOT auto-remove a checkpoint — that's the user's choice
    // Instead, checkpoint staleness will be updated to 'stale' after the next run
    // Verify no auto-removal happens
```

**Regression gate:** `tsc --noEmit` must report zero new errors after removal. The total error count should be equal to or less than the baseline from Step 1. Every test file compiles cleanly. The only passing test suite for pin-related behavior is the new checkpoint suite.

---

### Step 22 — Remove Legacy Pin Code From Backend

**Goal:** Remove the legacy Python pin handling code. Both `_node_freeze_mode` and `_node_freeze_lineage` are deleted. The backend only accepts `checkpoints` hints from this point forward.

**Files to change:**
- `backend/app/runner/run.py`
- `backend/app/runner/compile.py`

**In `run.py`:**
- Delete `_node_freeze_mode` function
- Delete `_node_freeze_lineage` function
- Delete the `pinnedNodeIds` / `pinnedArtifacts` execution hints parsing block
- Delete the `graphPinnedNodeIds` / `graphPinnedDerivedLineageNodeIds` variable derivation block
- Delete the `pin.backend_parse` trace emission
- Delete `tracePinCollect`, `tracePinRequestBuild`, `tracePinSubmit` equivalent logic

**In `compile.py`:**
- Delete the legacy `pinned_node_ids` parameter from `compile_plan`
- Delete the `pinned = { nid for nid in requested_pins ... }` block
- Delete the `cache_only_nodes |= pinned` line
- The new checkpoint-based `cache_only_nodes` population (from Step 12) remains

**Delete backend test files** that tested legacy pin behavior exclusively. Update integration tests that used legacy pin hints to use checkpoint hints.

**Tests to write** in `backend/tests/integration/test_checkpoints.py` (extend existing):
```
test_legacy_pinnedNodeIds_in_request_produces_error_or_is_ignored:
    # Sending the old pinnedNodeIds format should either be gracefully ignored
    # or produce a 400 with a helpful migration message.
    # This test documents the new contract for API consumers.

test_full_checkpoint_lifecycle_integration:
    # Full end-to-end: build graph, run, collect memoKey from trace,
    # create checkpoint hint with that memoKey, run again,
    # verify checkpoint honored (staleness=valid, reuse decision)
```

**Regression gate:** All backend checkpoint tests pass. All backend memoization tests pass. No references to `pinnedNodeIds`, `pinnedArtifacts`, `_node_freeze_mode`, or `_node_freeze_lineage` exist in `run.py` or `compile.py`. `pytest backend/tests/ -v` passes in full.

---

### Step 23 — Final Audit and Regression Sweep

**Goal:** Confirm that no references to the legacy pin system survive anywhere in the codebase, and the full test suite passes against the new system.

**Actions:**

Run exhaustive search for legacy terms:
```bash
grep -r "meta\.freeze\|freezeLineage\|pinnedNodeIds\|pinnedArtifacts\|per_run\|nodeFreezeMode\|setNodeFreezeMode\|collectPinnedNode\|clearPerRunPin\|_node_freeze" src/ backend/ --include="*.ts" --include="*.svelte" --include="*.py" -l
```

The only permissible matches are:
- `MIGRATION_BASELINE.md` (documentation)
- `graphStore.checkpoint.test.ts` strings in test descriptions that mention "formerly known as pin" for historical context
- Any migration function (`migrateFreezePinsToCheckpoints`) which explicitly references the old field names it reads and strips

All other matches must be resolved before this step is considered complete.

Run the complete test suite:
```bash
npx vitest run --reporter=verbose
tsc --noEmit
pytest backend/tests/ -v
```

Confirm:
- Frontend test count equals or exceeds baseline
- `tsc --noEmit` error count ≤ baseline from Step 1
- All new checkpoint and memoization tests pass
- Zero legacy pin tests remain (they were either deleted or updated)

Update `MIGRATION_BASELINE.md` with final test counts and a confirmation line: `Migration complete. All legacy pin code removed as of Step 23.`

**Regression gate:** This step is the gate. No code merges after Step 23 unless all three test suites pass in full.

---

## Summary Table

| Step | Phase | What Changes | New Tests | Runtime Impact |
|------|-------|-------------|-----------|----------------|
| 1 | Baseline | Measurement only | None | None |
| 2 | Deprecation | JSDoc markers | None | None |
| 3 | Types | New type files | Schema tests | None |
| 4 | Types | `memoizable` flag | Unit tests | None |
| 5 | Backend Memo | Fingerprint utility | Unit tests | None |
| 6 | Backend Memo | Cache storage | Unit tests | None |
| 7 | Backend Memo | **Live memo in run path** | Integration tests | Memo cache active |
| 8 | Frontend | Memo state in bindings | Unit tests | UI reads memo traces |
| 9 | Frontend | Remove pin UI, add memo indicator | Component tests | Pin controls gone |
| 10 | Checkpoint Types | `CheckpointRecord` in state | Schema + store tests | None |
| 11 | Checkpoint | Component revision checkpoints | Component tests | Checkpoints persist |
| 12 | Backend Checkpoint | Parse checkpoint hints | Integration tests | Checkpoints honored |
| 13 | Backend Checkpoint | Staleness in run result | Store + backend tests | Staleness updates |
| 14 | UI | Create Checkpoint action | UI + store tests | Checkpoint creation |
| 15 | UI | Replace per-run pin | Store tests | per-run pin gone |
| 16 | UI | Registry panel | Component tests | Panel available |
| 17 | Run Payload | Checkpoint hints in payload | runScope tests | New hint format |
| 18 | Component | Checkpoint badge on component node | Store + UI tests | Badge visible |
| 19 | Run Guard | Block run on unsaved changes | Store tests | Guard active |
| 20 | Migration | Legacy pins → Checkpoints on load | Store tests | One-time migration |
| 21 | Removal | Delete frontend legacy pin code | Replacement tests | Legacy code gone |
| 22 | Removal | Delete backend legacy pin code | Replacement tests | Legacy code gone |
| 23 | Audit | Full regression sweep | None | Migration complete |