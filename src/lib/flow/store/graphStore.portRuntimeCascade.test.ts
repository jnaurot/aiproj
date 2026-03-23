import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { __applyRunEventForTest, graphStore } from './graphStore';

describe('graphStore PORT-RUNTIME cascade projection', () => {
	it('stores branch cascade events and keeps them across queue metrics snapshots', () => {
		graphStore.hardResetGraph();
		const originId = graphStore.addNode('tool', { x: 0, y: 0 });
		const blockedA = graphStore.addNode('tool', { x: 180, y: 0 });
		const blockedB = graphStore.addNode('tool', { x: 360, y: 0 });
		const base = get(graphStore as any);
		let next = __applyRunEventForTest(
			base as any,
			{
				type: 'branch_cascade',
				runId: 'run_port_runtime_cascade',
				at: '2026-03-23T16:00:00.000Z',
				originNodeId: originId,
				blockedNodeIds: [blockedA, blockedB],
				reasonCode: 'HANDLE_INPUT_NONE_PROVIDED'
			} as any,
			'run_port_runtime_cascade'
		);
		next = __applyRunEventForTest(
			next as any,
			{
				type: 'queue_metrics',
				runId: 'run_port_runtime_cascade',
				at: '2026-03-23T16:00:01.000Z',
				scope: 'run',
				metrics: { globalDepth: 0, perEdgeMax: 1000 },
				runtimeItemMetrics: { itemsEnqueued: 0, itemsDequeued: 0, itemsRejected: 0, byPlane: {} }
			} as any,
			'run_port_runtime_cascade'
		);
		const cascades = (((next as any)?.queueRuntime?.branchCascade ?? []) as any[]).slice();
		expect(cascades.length).toBe(1);
		expect(String(cascades[0]?.originNodeId ?? '')).toBe(originId);
		expect(Array.isArray(cascades[0]?.blockedNodeIds)).toBe(true);
		expect((cascades[0]?.blockedNodeIds ?? []).length).toBe(2);
		expect(String(cascades[0]?.reasonCode ?? '')).toBe('HANDLE_INPUT_NONE_PROVIDED');
		const logs = ((next as any)?.logs ?? []) as Array<{ message?: string }>;
		expect(logs.some((row) => String(row?.message ?? '').includes('[cascade] origin='))).toBe(true);
	});
});
