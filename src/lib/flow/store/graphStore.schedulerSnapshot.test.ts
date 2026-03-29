import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { __applyRunEventForTest, graphStore } from './graphStore';

describe('graphStore scheduler_snapshot projection', () => {
	it('stores latest scheduler snapshot payload in queueRuntime', () => {
		graphStore.hardResetGraph();
		const base = get(graphStore as any);
		const next = __applyRunEventForTest(
			base as any,
			{
				type: 'scheduler_snapshot',
				runId: 'run_sched_1',
				at: '2026-03-29T03:30:00.000Z',
				readyCount: 0,
				inflightCount: 0,
				pendingQueueDepth: 5,
				runnableNodeCount: 0,
				stalled: true,
				perNode: [
					{
						nodeId: 'n_blocked',
						readyWork: false,
						inflight: 0,
						pendingInputCount: 5,
						lastBlockedReasonCode: 'WAITING_REQUIRED_INPUT'
					}
				]
			} as any,
			'run_sched_1'
		);
		const snapshot = (next as any)?.queueRuntime?.schedulerSnapshot as any;
		expect(snapshot?.stalled).toBe(true);
		expect(snapshot?.pendingQueueDepth).toBe(5);
		expect(Array.isArray(snapshot?.perNode)).toBe(true);
		expect(snapshot?.perNode?.[0]?.nodeId).toBe('n_blocked');
	});
});

