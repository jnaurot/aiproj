import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { __applyRunEventForTest, graphStore } from './graphStore';

describe('graphStore plane metrics', () => {
	it('stores plane-isolated queue runtime metrics from queue_metrics events', () => {
		graphStore.hardResetGraph();
		const src = graphStore.addNode('source', { x: 0, y: 0 });
		const dst = graphStore.addNode('transform', { x: 240, y: 0 });
		graphStore.addEdge({
			id: 'e_plane',
			source: src,
			target: dst,
			sourceHandle: 'out',
			targetHandle: 'in',
			data: { exec: 'idle', mode: 'work' }
		} as any);
		const state = get(graphStore as any);
		const next = __applyRunEventForTest(
			state as any,
			{
				type: 'queue_metrics',
				runId: 'run_plane',
				at: new Date().toISOString(),
				metrics: { globalDepth: 1, globalMax: 100, perEdgeMax: 10, edges: {} },
				nodeMetrics: {},
				runtimeItemMetrics: {
					itemsEnqueued: 2,
					itemsDequeued: 1,
					itemsRejected: 1,
					itemsAccepted: 0,
					byPlane: {
						work: { itemsEnqueued: 2, itemsDequeued: 1, itemsAccepted: 0, itemsRejected: 1 },
						param: { itemsEnqueued: 0, itemsDequeued: 0, itemsAccepted: 0, itemsRejected: 0 },
						control: { itemsEnqueued: 0, itemsDequeued: 0, itemsAccepted: 0, itemsRejected: 0 }
					},
					byHandle: {
						[`${dst}:in`]: {
							nodeId: dst,
							handle: 'in',
							plane: 'work',
							itemsEnqueued: 2,
							itemsDequeued: 1,
							itemsAccepted: 0,
							itemsRejected: 1
						}
					}
				}
			} as any,
			'run_plane'
		);

		const metrics = (next as any)?.queueRuntime?.runtimeItemMetrics ?? {};
		expect(metrics?.byPlane?.work?.itemsEnqueued).toBe(2);
		expect(metrics?.byPlane?.work?.itemsRejected).toBe(1);
		expect(metrics?.byHandle?.[`${dst}:in`]?.plane).toBe('work');
	});
});

