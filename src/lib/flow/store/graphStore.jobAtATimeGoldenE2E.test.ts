import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { __applyRunEventForTest, graphStore } from './graphStore';

describe('graphStore job-at-a-time golden e2e projection', () => {
	it('projects deterministic accept/reject flow and queue metrics', () => {
		graphStore.hardResetGraph();
		const selectId = graphStore.addNode('tool', { x: 0, y: 0 });
		const base = get(graphStore as any);
		let next = __applyRunEventForTest(
			base as any,
			{
				type: 'queue_metrics',
				runId: 'run_job_golden',
				at: '2026-03-23T13:00:00.000Z',
				scope: 'run',
				metrics: { globalDepth: 1, globalMax: 100, perEdgeMax: 10, edges: {} },
				nodeMetrics: {},
				runtimeItemMetrics: {
					itemsEnqueued: 3,
					itemsDequeued: 3,
					itemsAccepted: 2,
					itemsRejected: 1,
					byHandle: {
						[`${selectId}:in`]: {
							nodeId: selectId,
							handle: 'in',
							plane: 'work',
							itemsEnqueued: 3,
							itemsDequeued: 3,
							itemsAccepted: 2,
							itemsRejected: 1
						}
					}
				}
			} as any,
			'run_job_golden'
		);
		next = __applyRunEventForTest(
			next as any,
			{
				type: 'node_decision',
				runId: 'run_job_golden',
				at: '2026-03-23T13:00:01.000Z',
				nodeId: selectId,
				decision: 'reject',
				count: 1,
				reasonCode: 'NODE_REJECTED_NON_ERROR'
			} as any,
			'run_job_golden'
		);
		next = __applyRunEventForTest(
			next as any,
			{
				type: 'node_reject',
				runId: 'run_job_golden',
				at: '2026-03-23T13:00:01.100Z',
				nodeId: selectId,
				plane: 'work',
				reasonCode: 'NODE_REJECTED_NON_ERROR',
				count: 1
			} as any,
			'run_job_golden'
		);
		expect((next as any)?.queueRuntime?.runScoped?.runtimeItemMetrics?.itemsRejected).toBe(1);
		expect((next as any)?.queueRuntime?.runtimeItemMetrics?.byHandle?.[`${selectId}:in`]?.itemsAccepted).toBe(2);
		const lastLog = (next as any)?.logs?.[(next as any)?.logs?.length - 1];
		expect(String(lastLog?.message ?? '')).toContain('[reject]');
	});

	it('keeps run-scoped queue limits and per-item counters deterministic', () => {
		graphStore.hardResetGraph();
		const state = get(graphStore as any);
		const edgeId = 'e_jobs_to_select';
		const next = __applyRunEventForTest(
			state as any,
			{
				type: 'queue_metrics',
				runId: 'run_job_golden_limits',
				at: '2026-03-23T13:15:00.000Z',
				scope: 'run',
				metrics: {
					globalDepth: 1,
					globalMax: 100,
					perEdgeMax: 2,
					edges: {
						[edgeId]: { depth: 1, enqueueRate: 1, dequeueRate: 0, oldestAgeMs: 25, blocked: false, full: false }
					}
				},
				nodeMetrics: {},
				runtimeItemMetrics: {
					itemsEnqueued: 2,
					itemsDequeued: 1,
					itemsAccepted: 1,
					itemsRejected: 0,
					byPlane: {
						work: { itemsEnqueued: 2, itemsDequeued: 1, itemsAccepted: 1, itemsRejected: 0 }
					}
				}
			} as any,
			'run_job_golden_limits'
		);
		expect((next as any)?.queueRuntime?.runScoped?.metrics?.perEdgeMax).toBe(2);
		expect((next as any)?.queueRuntime?.runScoped?.metrics?.edges?.[edgeId]?.depth).toBe(1);
		expect((next as any)?.queueRuntime?.runScoped?.runtimeItemMetrics?.itemsEnqueued).toBe(2);
		expect((next as any)?.queueRuntime?.runScoped?.runtimeItemMetrics?.byPlane?.work?.itemsEnqueued).toBe(2);
	});
});
