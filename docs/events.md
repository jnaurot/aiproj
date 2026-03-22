# Run Event Contract

This document defines the backend run-event payload contract used by SSE and replay APIs.

## Base Fields

All events include:

- `type` (string)
- `runId` (string)
- `at` (ISO timestamp string)
- `seq` (integer, monotonic per run)

Persisted replay rows also include:

- `id` (integer, global row id in `run_events`)
- `ts` (stored timestamp)
- `payload` (original event payload)

## Versioned Event Types

These event types include `schema_version: 1`:

- `cache_decision`
- `cache_summary`

### `cache_decision` (schema_version=1)

Required fields:

- `nodeId`
- `nodeKind`
- `decision` (`cache_hit | cache_miss | cache_hit_contract_mismatch`)
- `reason` (`CACHE_HIT | CACHE_ENTRY_MISSING | INPUTS_UNRESOLVED | PARAMS_CHANGED | INPUT_CHANGED | ENV_CHANGED | BUILD_CHANGED | UNCACHEABLE_EFFECTFUL_TOOL | CONTRACT_MISMATCH`)
- `execKey`

Optional fields:

- `artifactId`
- `producerExecKey`

### `cache_summary` (schema_version=1)

Required fields:

- `cache_hit` (int)
- `cache_miss` (int)
- `cache_hit_contract_mismatch` (int)

## Core Runtime Event Types

- `run_started`
- `run_finished`
- `run_cancel_requested`
- `run_cancelled`
- `scheduler_cancelled`
- `node_started`
- `node_output`
- `node_finished`
- `node_cancelled`
- `edge_exec`
- `control_signal`
- `queue_metrics`
- `node_decision`
- `log`

### `control_signal`

Runtime control-state transitions.

Fields:

- `signal` (`ready | busy | drain | pause | blocked | resume`)
- optional `nodeId`

### `queue_metrics`

Per-run queue observability snapshot.

Fields:

- `metrics.globalDepth`
- `metrics.globalMax`
- `metrics.perEdgeMax`
- `metrics.edges` (per-edge queue depth/rates/age/full/blocked)
- `nodeMetrics` (per-node input wait, run time, retry count, backpressure status)
- `runtimeItemMetrics` (itemsEnqueued/itemsDequeued/itemsAccepted/itemsRejected/nodeCounters)

### `node_decision`

Structured node decision signal (non-error accept/reject flow).

Fields:

- `nodeId`
- `decision` (`accept | reject`)
- optional `count`
- optional `reasonCode`

## Mode-Specific Contract Diagnostics

Schema/contract diagnostics are mode-aware:

- `work` edges emit `Work payload mismatch ...` diagnostics for payload type/schema issues.
- `param` edges emit `Param shape mismatch ...` diagnostics when required param keys/shapes are missing.
- `control` affinity errors emit `Control contract mismatch ...` diagnostics when control handles/mode contracts are incompatible.
- mismatch diagnostics include per-handle context: `edgeId`, `sourceHandle`, `targetHandle`, `sourceNodeId`, `targetNodeId`, `sourceLabel`, `targetLabel`, `mode`, `sourceAffinity`, `targetAffinity`.

Suggested auto-fixes are also mode-specific:

- `work`: adapter suggestions (for example `text_to_table`, `json_to_table`, `table_to_json`) when available.
- `param`: align `requiredKeys`/param shape with provided keys or enrich source param payload.
- `control`: reconnect using control-affinity handles (`control_*`/`ctl_*`) and `mode=control`.

## Ordering Invariants

For cache path nodes:

1. `node_started`
2. `cache_decision`
3. `node_output` (when artifact identity is available)
4. `node_finished`

For run-level summary:

- `cache_summary` is emitted once per run (success, failure, or cancellation).
