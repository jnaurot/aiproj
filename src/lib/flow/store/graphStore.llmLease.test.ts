import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { __applyRunEventForTest, graphStore } from './graphStore';

describe('graphStore llm_lease projection', () => {
	it('stores llm lease state and clears holder on released', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('model', { x: 0, y: 0 });
		const base = get(graphStore as any);
		let next = __applyRunEventForTest(
			base as any,
			{
				type: 'llm_lease',
				runId: 'run_llm_lease_1',
				at: '2026-03-29T04:00:00.000Z',
				state: 'acquired',
				nodeId,
				holderNodeId: nodeId,
				waitQueueLength: 0,
				waitingNodeIds: []
			} as any,
			'run_llm_lease_1'
		);
		expect((next as any)?.queueRuntime?.llmLease?.state).toBe('acquired');
		expect((next as any)?.queueRuntime?.llmLease?.holderNodeId).toBe(nodeId);
		next = __applyRunEventForTest(
			next as any,
			{
				type: 'llm_lease',
				runId: 'run_llm_lease_1',
				at: '2026-03-29T04:00:01.000Z',
				state: 'released',
				nodeId,
				holderNodeId: null,
				waitQueueLength: 1,
				waitingNodeIds: ['n_waiting']
			} as any,
			'run_llm_lease_1'
		);
		expect((next as any)?.queueRuntime?.llmLease?.state).toBe('released');
		expect((next as any)?.queueRuntime?.llmLease?.holderNodeId).toBeNull();
		expect((next as any)?.queueRuntime?.llmLease?.waitQueueLength).toBe(1);
		const logLines = ((next as any)?.logs ?? []).map((entry: any) => String(entry?.message ?? ''));
		expect(logLines.some((line: string) => line.includes('[monitor-phase] phase=AWAITING_PROVIDER_RESPONSE'))).toBe(true);
		expect(logLines.some((line: string) => line.includes('[monitor-phase] phase=AWAITING_DISPATCH'))).toBe(true);
	});
});
