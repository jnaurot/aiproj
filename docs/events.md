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
- `expectedPortType`
- `actualPortType`
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
- `level_started`
- `level_finished`
- `scheduler_cancelled`
- `node_started`
- `node_output`
- `node_finished`
- `node_cancelled`
- `edge_exec`
- `log`

## Ordering Invariants

For cache path nodes:

1. `node_started`
2. `cache_decision`
3. `node_output` (when artifact identity is available)
4. `node_finished`

For run-level summary:

- `cache_summary` is emitted once per run (success, failure, or cancellation).
