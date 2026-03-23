import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { __applyRunEventForTest, graphStore } from './graphStore';

describe('graphStore port observability', () => {
	it('captures queue_metrics payload for per-port inspector rendering', () => {
		graphStore.hardResetGraph();
		const src = graphStore.addNode('source', { x: 0, y: 0 });
		const dst = graphStore.addNode('transform', { x: 200, y: 0 });
		graphStore.addEdge({
			id: 'e_obs',
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
				runId: 'run_obs',
				at: new Date().toISOString(),
				metrics: {
					globalDepth: 1,
					globalMax: 50000,
					perEdgeMax: 1000,
					edges: {
						'e_obs:in': {
							edgeId: 'e_obs',
							inputHandle: 'in',
							depth: 1,
							enqueued: 1,
							dequeued: 0,
							oldestAgeSec: 0.5,
							blocked: false,
							full: false
						}
					}
				},
				nodeMetrics: {
					[dst]: { inputWaitMs: 10, runTimeMs: 50, retryCount: 0, backpressureStatus: 'clear' }
				},
				runtimeItemMetrics: { itemsEnqueued: 1, itemsDequeued: 0, itemsRejected: 0, itemsAccepted: 0 }
			} as any,
			'run_obs'
		);

		expect((next as any)?.queueRuntime?.metrics?.edges?.['e_obs:in']?.depth).toBe(1);
		expect((next as any)?.queueRuntime?.nodeMetrics?.[dst]?.runTimeMs).toBe(50);
	});
});
