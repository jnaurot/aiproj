import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { __applyRunEventForTest, graphStore } from './graphStore';
import { buildRunMonitorEdgeRows } from '$lib/flow/components/runMonitorModel';

describe('graphStore dequeue control + edge projection integration', () => {
	it('applies enqueue/dequeue/drain stream and clears waiting edge lifecycle when depth reaches zero', () => {
		graphStore.hardResetGraph();
		const src = graphStore.addNode('tool', { x: 0, y: 0 });
		const dst = graphStore.addNode('tool', { x: 220, y: 0 });
		const add = graphStore.addEdge({
			id: 'e_runtime',
			source: src,
			target: dst,
			sourceHandle: 'out',
			targetHandle: 'in',
			data: { exec: 'idle', mode: 'work' }
		} as any);
		expect(add.ok).toBe(true);

		let state = get(graphStore as any);
		state = __applyRunEventForTest(
			state as any,
			{ type: 'control_signal', runId: 'run-edge', at: '2026-04-22T10:00:00Z', signal: 'item_enqueued', edgeId: 'e_runtime', seq: 1 } as any,
			'run-edge'
		);
		state = __applyRunEventForTest(
			state as any,
			{ type: 'control_signal', runId: 'run-edge', at: '2026-04-22T10:00:01Z', signal: 'item_dequeued', edgeId: 'e_runtime', seq: 2 } as any,
			'run-edge'
		);
		state = __applyRunEventForTest(
			state as any,
			{ type: 'control_signal', runId: 'run-edge', at: '2026-04-22T10:00:02Z', signal: 'input_drained', edgeId: 'e_runtime', seq: 3 } as any,
			'run-edge'
		);
		expect(Number((state as any)?.queueRuntime?.controlPlaneEdgeState?.e_runtime?.depth ?? -1)).toBe(0);

		state = __applyRunEventForTest(
			state as any,
			{
				type: 'queue_metrics',
				runId: 'run-edge',
				at: '2026-04-22T10:00:03Z',
				scope: 'run',
				metrics: {
					globalDepth: 1,
					globalMax: 50000,
					perEdgeMax: 1000,
					edges: {
						'e_runtime:in': {
							edgeId: 'e_runtime',
							inputHandle: 'in',
							depth: 1,
							enqueued: 1,
							dequeued: 0,
							blocked: false,
							full: false
						}
					}
				},
				nodeMetrics: {},
				runtimeItemMetrics: { itemsEnqueued: 1, itemsDequeued: 0, itemsAccepted: 0, itemsRejected: 0 }
			} as any,
			'run-edge'
		);
		let rows = buildRunMonitorEdgeRows({
			nodes: (state.nodes ?? []) as any,
			edges: (state.edges ?? []) as any,
			queueRuntime: (state.queueRuntime ?? {}) as any
		});
		expect(rows.find((row) => row.edgeId === 'e_runtime')?.lifecycle).toBe('waiting');

		state = __applyRunEventForTest(
			state as any,
			{
				type: 'queue_metrics',
				runId: 'run-edge',
				at: '2026-04-22T10:00:04Z',
				scope: 'run',
				metrics: {
					globalDepth: 0,
					globalMax: 50000,
					perEdgeMax: 1000,
					edges: {
						'e_runtime:in': {
							edgeId: 'e_runtime',
							inputHandle: 'in',
							depth: 0,
							enqueued: 1,
							dequeued: 1,
							blocked: false,
							full: false
						}
					}
				},
				nodeMetrics: {},
				runtimeItemMetrics: { itemsEnqueued: 1, itemsDequeued: 1, itemsAccepted: 1, itemsRejected: 0 }
			} as any,
			'run-edge'
		);
		rows = buildRunMonitorEdgeRows({
			nodes: (state.nodes ?? []) as any,
			edges: (state.edges ?? []) as any,
			queueRuntime: (state.queueRuntime ?? {}) as any
		});
		expect(rows.find((row) => row.edgeId === 'e_runtime')?.lifecycle).toBe('inactive');
	});
});
