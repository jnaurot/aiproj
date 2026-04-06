# Execution State Machine (Canonical)

This document is the authoritative lifecycle contract for runtime execution.

## Ownership
- Backend scheduler/runtime is authoritative for run and node execution state.
- UI state is derived from backend events/snapshots and must not invent transitions.

## Run Lifecycle States
- `pending`
- `running`
- `cancel_requested`
- `pausing`
- `paused`
- `resuming`
- `succeeded` (terminal)
- `failed` (terminal)
- `canceled` (terminal)
- `deleted` (terminal)

## Run Transition Matrix
- `pending` -> `running|paused|cancel_requested|pausing|failed|canceled|deleted`
- `running` -> `cancel_requested|pausing|succeeded|failed|canceled|deleted`
- `cancel_requested` -> `canceled|failed|pausing|deleted`
- `pausing` -> `paused|canceled|failed|deleted`
- `paused` -> `resuming|canceled|failed|deleted`
- `resuming` -> `running|paused|canceled|failed|deleted`
- terminal states are self-only

## Node Runtime States
- `idle`
- `running`
- `active`
- `blocked`
- `paused`
- `succeeded_up_to_date`
- `failed`
- `canceled`
- `stale`

## Node Transition Matrix
- `idle` -> `idle|running|blocked|paused|active|stale`
- `running` -> `running|succeeded_up_to_date|failed|canceled|blocked|paused|active|stale`
- `active` -> `active|running|blocked|paused|succeeded_up_to_date|failed|canceled|stale`
- `blocked` -> `blocked|running|active|paused|failed|canceled|stale`
- `paused` -> `paused|active|running|failed|canceled|stale`
- `succeeded_up_to_date` -> `succeeded_up_to_date|running|blocked|paused|stale`
- `failed` -> `failed|running|blocked|paused|stale`
- `canceled` -> `canceled|running|blocked|paused|stale`
- `stale` -> `stale|running|blocked|paused|active|failed|canceled|succeeded_up_to_date`

## Illegal Transition Policy
- Illegal transitions are runtime violations.
- Runtime raises deterministic errors:
  - `RUN_STATE_TRANSITION_VIOLATION`
  - `NODE_STATE_TRANSITION_VIOLATION`

## Event Mapping (selected)
- `run_started` -> run `running`
- `run_cancel_requested` -> run `cancel_requested`
- `run_pause_requested|run_pausing` -> run `pausing`
- `run_paused` -> run `paused`
- `run_resume_requested|run_resuming` -> run `resuming`
- `run_resumed` -> run `running`
- `run_canceled` -> run `canceled`
- `run_finished(status=...)` -> terminal run status
- `node_started` -> node `running`
- `node_finished(status=succeeded)` -> node `succeeded_up_to_date`
- `node_finished(status=failed)` -> node `failed`
- `node_finished(status=canceled)` -> node `canceled`
- `node_canceled` -> node `canceled`
- `node_blocked` -> node `blocked`
- `node_paused` -> node `paused`
- `node_resumed` -> node `active`
