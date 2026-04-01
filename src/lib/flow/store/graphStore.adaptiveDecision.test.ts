import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { __applyRunEventForTest, graphStore } from './graphStore';

describe('graphStore adaptive decision projection', () => {
	it('stores scheduler_adaptive_decision rows in queue runtime', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('transform', { x: 0, y: 0 });
		let next = get(graphStore as any);
		next = __applyRunEventForTest(
			next as any,
			{
				type: 'run_started',
				runId: 'run_adapt_1',
				at: '2026-03-31T04:00:00.000Z',
				runFrom: null,
				runMode: 'from_start',
				plannedNodeIds: [nodeId]
			} as any,
			'run_adapt_1'
		);
		next = __applyRunEventForTest(
			next as any,
			{
				type: 'scheduler_adaptive_decision',
				runId: 'run_adapt_1',
				at: '2026-03-31T04:00:01.000Z',
				mode: 'enforce',
				enforced: true,
				reasons: ['queue_depth_high'],
				hardCaps: { global: 4, model: 1 },
				minCaps: { global: 1, model: 1 },
				proposedCaps: { global: 3, model: 1 },
				effectiveCaps: { global: 3, model: 1 },
				changedCaps: { global: { from: 4, to: 3 } }
			} as any,
			'run_adapt_1'
		);
		const rows = ((next as any)?.queueRuntime?.adaptiveDecisions ?? []) as any[];
		expect(rows).toHaveLength(1);
		expect(rows[0]).toMatchObject({
			runId: 'run_adapt_1',
			mode: 'enforce',
			enforced: true
		});
		expect(rows[0]?.changedCaps?.global).toEqual({ from: 4, to: 3 });
	});
});

