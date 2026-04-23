# Queue Semantics and Troubleshooting

## Contract Summary

### Queue authority by consume mode
- `single_item` and `batch` consumers use queue depth as authoritative runtime truth.
- `once` consumers are node-boundary executions and do not retain work-queue depth.

### Control signal semantics
- `item_enqueued`: queue depth increased for a work edge.
- `item_dequeued`: queue depth decreased for a work edge.
- `input_drained`: queue depth is confirmed empty for that edge/handle.

### Queue invariant
For each edge/handle metric bucket:
- `enqueued - dequeued == depth`

The runner now emits run-end diagnostics for this invariant.

## Expected Behavior

### Successful run
- Final `queue_metrics.metrics.globalDepth` should be `0`.
- Work edges should finish with `depth=0`.
- No `QUEUE_DEPTH_INVARIANT_VIOLATION` warning log should be present.

### Completed node/edge visuals
- Frontend supports `item_dequeued` and decrements control-plane edge depth.
- Completed source+target lifecycle can settle edge visuals even if waiting flags were stale.

## Troubleshooting Checklist

1. Verify backend queue metrics at run end.
- Check final `queue_metrics` event for `globalDepth`, per-edge `depth`, `enqueued`, `dequeued`.

2. Verify control signal sequence.
- For streaming edges, expected order includes:
`item_enqueued -> item_dequeued -> input_drained -> node_terminal`.

3. Check invariant diagnostics log.
- Look for `[queue-invariant]` summary.
- If warning reason code `QUEUE_DEPTH_INVARIANT_VIOLATION` appears, inspect sample edge buckets.

4. Confirm consume mode.
- `once` nodes should not accumulate queue depth.
- `single_item`/`batch` nodes must dequeue as they consume.

5. Validate frontend projection.
- Ensure client receives `item_dequeued` signals.
- Ensure run monitor edge rows clear from `waiting` when depth reaches zero.

## Related Tests

### Backend
- `backend/tests/runner/test_queue_once_mode_depth_reduction.py`
- `backend/tests/runner/test_queue_dequeue_signal_ordering.py`
- `backend/tests/runner/test_queue_metrics_enq_deq_balance.py`
- `backend/tests/runner/test_queue_invariant_diagnostics.py`
- `backend/tests/integration/test_queue_depth_run_finished_snapshot.py`
- `backend/tests/e2e/test_queue_truth_mixed_consume_modes.py`

### Frontend
- `src/lib/flow/store/graphStore.controlPlaneEdgeState.test.ts`
- `src/lib/flow/store/graphStore.edgeDequeueProjection.regression.test.ts`
- `src/lib/flow/edgeVisualState.test.ts`
