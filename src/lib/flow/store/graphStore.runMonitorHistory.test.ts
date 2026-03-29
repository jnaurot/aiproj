import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { __applyRunEventForTest, graphStore } from './graphStore';

describe('graphStore run monitor history projection', () => {
	it('captures blocked/stalled/depth and telemetry into run history on run_finished', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('model', { x: 0, y: 0 });
		let next = get(graphStore as any);
		next = __applyRunEventForTest(
			next as any,
			{
				type: 'run_started',
				runId: 'run_hist_1',
				at: '2026-03-29T23:10:00.000Z',
				runFrom: null,
				runMode: 'from_start',
				plannedNodeIds: [nodeId]
			} as any,
			'run_hist_1'
		);
		next = __applyRunEventForTest(
			next as any,
			{
				type: 'node_blocked',
				runId: 'run_hist_1',
				at: '2026-03-29T23:10:02.000Z',
				nodeId,
				reasonCode: 'WAITING_REQUIRED_INPUT'
			} as any,
			'run_hist_1'
		);
		next = __applyRunEventForTest(
			next as any,
			{
				type: 'scheduler_snapshot',
				runId: 'run_hist_1',
				at: '2026-03-29T23:10:04.000Z',
				readyCount: 0,
				inflightCount: 0,
				pendingQueueDepth: 7,
				runnableNodeCount: 0,
				stalled: true,
				perNode: []
			} as any,
			'run_hist_1'
		);
		next = __applyRunEventForTest(
			next as any,
			{
				type: 'run_telemetry',
				runId: 'run_hist_1',
				at: '2026-03-29T23:10:10.000Z',
				runtime_ms: 42000,
				peak_concurrency: 3,
				executed: 1,
				cached: 0,
				failed: 0,
				cache_hit: 0,
				cache_miss: 1,
				cache_hit_contract_mismatch: 0
			} as any,
			'run_hist_1'
		);
		next = __applyRunEventForTest(
			next as any,
			{
				type: 'run_finished',
				runId: 'run_hist_1',
				at: '2026-03-29T23:10:11.000Z',
				status: 'succeeded'
			} as any,
			'run_hist_1'
		);
		const history = ((next as any)?.queueRuntime?.runHistory ?? []) as any[];
		expect(history).toHaveLength(1);
		expect(history[0]).toMatchObject({
			runId: 'run_hist_1',
			status: 'succeeded',
			runtimeMs: 42000,
			peakConcurrency: 3,
			maxPendingQueueDepth: 7,
			hadStalledSnapshot: true,
			blockedEvents: 1
		});
	});

	it('caps run history to last 20 runs', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('model', { x: 0, y: 0 });
		let next = get(graphStore as any);
		for (let i = 1; i <= 22; i += 1) {
			const runId = `run_hist_cap_${i}`;
			next = __applyRunEventForTest(
				next as any,
				{
					type: 'run_started',
					runId,
					at: `2026-03-30T00:${String(i).padStart(2, '0')}:00.000Z`,
					runFrom: null,
					runMode: 'from_start',
					plannedNodeIds: [nodeId]
				} as any,
				runId
			);
			next = __applyRunEventForTest(
				next as any,
				{
					type: 'run_finished',
					runId,
					at: `2026-03-30T00:${String(i).padStart(2, '0')}:05.000Z`,
					status: 'succeeded'
				} as any,
				runId
			);
		}
		const history = ((next as any)?.queueRuntime?.runHistory ?? []) as any[];
		expect(history).toHaveLength(20);
		expect(history[0]?.runId).toBe('run_hist_cap_3');
		expect(history[19]?.runId).toBe('run_hist_cap_22');
	});
});
