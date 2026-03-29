import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { __applyRunEventForTest, graphStore } from './graphStore';

describe('graphStore run monitor scope reset', () => {
	it('resets blocked/scheduler/lease telemetry on run_started', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('model', { x: 0, y: 0 });
		const base = get(graphStore as any);
		let next = __applyRunEventForTest(
			base as any,
			{
				type: 'node_blocked',
				runId: 'run_monitor_prev',
				at: '2026-03-29T05:00:00.000Z',
				nodeId,
				reasonCode: 'WAITING_REQUIRED_INPUT'
			} as any,
			'run_monitor_prev'
		);
		next = __applyRunEventForTest(
			next as any,
			{
				type: 'scheduler_snapshot',
				runId: 'run_monitor_prev',
				at: '2026-03-29T05:00:01.000Z',
				readyCount: 0,
				inflightCount: 0,
				pendingQueueDepth: 1,
				runnableNodeCount: 0,
				stalled: true,
				perNode: []
			} as any,
			'run_monitor_prev'
		);
		next = __applyRunEventForTest(
			next as any,
			{
				type: 'llm_lease',
				runId: 'run_monitor_prev',
				at: '2026-03-29T05:00:02.000Z',
				state: 'acquired',
				nodeId,
				holderNodeId: nodeId,
				waitQueueLength: 0
			} as any,
			'run_monitor_prev'
		);
		expect(Boolean((next as any)?.queueRuntime?.blockedByNode?.[nodeId])).toBe(true);
		expect(Boolean((next as any)?.queueRuntime?.schedulerSnapshot)).toBe(true);
		expect(Boolean((next as any)?.queueRuntime?.llmLease)).toBe(true);
		next = __applyRunEventForTest(
			next as any,
			{
				type: 'run_started',
				runId: 'run_monitor_next',
				at: '2026-03-29T05:00:10.000Z',
				runFrom: null,
				runMode: 'from_start',
				plannedNodeIds: [nodeId]
			} as any,
			'run_monitor_next'
		);
		expect(Object.keys((next as any)?.queueRuntime?.blockedByNode ?? {})).toHaveLength(0);
		expect((next as any)?.queueRuntime?.schedulerSnapshot).toBeUndefined();
		expect((next as any)?.queueRuntime?.llmLease).toBeUndefined();
	});
});

