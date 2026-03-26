import { describe, expect, it } from 'vitest';

import {
	__aggregateRunAllSummaryForTest,
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
});
