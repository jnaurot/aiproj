import { describe, expect, it } from 'vitest';

import { __applyRunEventForTest, graphStore } from './graphStore';
import { get } from 'svelte/store';

describe('graphStore metrics scope separation', () => {
	it('keeps run-scoped queue metrics separate from aggregate diagnostics', () => {
		graphStore.hardResetGraph();
		const base = get(graphStore as any);
		const evt1 = {
			type: 'queue_metrics',
			runId: 'run_scope_1',
			at: '2026-03-23T12:00:00.000Z',
			scope: 'run',
			metrics: { globalDepth: 1, globalMax: 10, perEdgeMax: 5, edges: {} },
			nodeMetrics: {},
			runtimeItemMetrics: { itemsEnqueued: 2, itemsDequeued: 1, itemsAccepted: 1, itemsRejected: 1 }
		} as any;
		const evt2 = {
			type: 'queue_metrics',
			runId: 'run_scope_2',
			at: '2026-03-23T12:00:01.000Z',
			scope: 'run',
			metrics: { globalDepth: 0, globalMax: 10, perEdgeMax: 5, edges: {} },
			nodeMetrics: {},
			runtimeItemMetrics: { itemsEnqueued: 3, itemsDequeued: 3, itemsAccepted: 3, itemsRejected: 0 }
		} as any;
		let next = __applyRunEventForTest(base as any, evt1, 'run_scope_1');
		next = __applyRunEventForTest(next as any, evt2, 'run_scope_2');
		expect((next as any)?.queueRuntime?.runScoped?.runId).toBe('run_scope_2');
		expect((next as any)?.queueRuntime?.runScoped?.scope).toBe('run');
		expect((next as any)?.queueRuntime?.runScoped?.runtimeItemMetrics?.itemsEnqueued).toBe(3);
		expect((next as any)?.queueRuntime?.aggregateDiagnostics?.queueMetricEvents).toBe(2);
		expect((next as any)?.queueRuntime?.aggregateDiagnostics?.itemsEnqueued).toBe(5);
		expect((next as any)?.queueRuntime?.aggregateDiagnostics?.itemsRejected).toBe(1);
	});
});
