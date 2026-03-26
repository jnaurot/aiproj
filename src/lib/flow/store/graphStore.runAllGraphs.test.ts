import { describe, expect, it } from 'vitest';

import {
	__applyRunEventForTest,
	__aggregateRunAllSummaryForTest,
	__hardResetGraphForTest,
	__sortGraphCatalogForRunAllForTest
} from './graphStore';

describe('run all graphs helpers', () => {
	it('sorts graph catalog deterministically by graphId', () => {
		const sorted = __sortGraphCatalogForRunAllForTest([
			{ graphId: 'graph_c', graphName: 'C' },
			{ graphId: 'graph_a', graphName: 'A' },
			{ graphId: 'graph_b', graphName: 'B' }
		]);
		expect(sorted.map((g) => g.graphId)).toEqual(['graph_a', 'graph_b', 'graph_c']);
	});

	it('aggregates mixed run outcomes', () => {
		const summary = __aggregateRunAllSummaryForTest([
			'succeeded',
			'failed',
			'cancelled',
			'canceled',
			'unknown'
		]);
		expect(summary.totalGraphs).toBe(5);
		expect(summary.succeeded).toBe(1);
		expect(summary.failed).toBe(2);
		expect(summary.cancelled).toBe(2);
	});

	it('includes graph identity in run lifecycle log messages', () => {
		const base = __hardResetGraphForTest({} as any, 'graph_abc123');
		const started = __applyRunEventForTest(
			base,
			{
				type: 'run_started',
				runId: 'run_1',
				graphId: 'graph_abc123',
				runFrom: null,
				runMode: 'from_start',
				plannedNodeIds: []
			} as any,
			'run_1'
		);
		const startMsg = String(started.logs[started.logs.length - 1]?.message ?? '');
		expect(startMsg).toContain('[graph graph_abc123]');
		expect(startMsg).toContain('Run started');

		const finished = __applyRunEventForTest(
			started,
			{
				type: 'run_finished',
				runId: 'run_1',
				graphId: 'graph_abc123',
				status: 'succeeded'
			} as any,
			'run_1'
		);
		const finishMsg = String(finished.logs[finished.logs.length - 1]?.message ?? '');
		expect(finishMsg).toContain('[graph graph_abc123]');
		expect(finishMsg).toContain('Run finished');
	});
});
