import { describe, expect, it } from 'vitest';

import {
	getExperimentFailureTaxonomy,
	getExperimentNodeTrends,
	getExperimentSlaBreaches
} from './runs';

describe('runs client analytics endpoints', () => {
	it('queries node trends endpoint', async () => {
		const originalFetch = globalThis.fetch;
		(globalThis as any).fetch = async (input: RequestInfo | URL) => {
			const url = String(input);
			expect(url.startsWith('/api/experiments/trends/nodes?')).toBe(true);
			expect(url.includes('graphId=graph_1')).toBe(true);
			expect(url.includes('nodeId=n1')).toBe(true);
			return new Response(
				JSON.stringify({
					schemaVersion: 1,
					metric: 'p95Ms',
					points: [{ runId: 'r1', createdAt: '2026-03-31T00:00:00Z', nodeId: 'n1', metric: 'p95Ms', value: 1200 }]
				}),
				{ status: 200, headers: { 'content-type': 'application/json' } }
			);
		};
		try {
			const res = await getExperimentNodeTrends({ graphId: 'graph_1', nodeId: 'n1', metric: 'p95Ms' });
			expect(res.points[0]?.nodeId).toBe('n1');
		} finally {
			(globalThis as any).fetch = originalFetch;
		}
	});

	it('queries SLA breaches endpoint', async () => {
		const originalFetch = globalThis.fetch;
		(globalThis as any).fetch = async (input: RequestInfo | URL) => {
			const url = String(input);
			expect(url.startsWith('/api/experiments/sla/breaches?')).toBe(true);
			return new Response(
				JSON.stringify({
					schemaVersion: 1,
					thresholdMs: 2000,
					breaches: [{ runId: 'r2', createdAt: '2026-03-31T00:10:00Z', nodeId: 'n2', p95Ms: 3400, thresholdMs: 2000 }]
				}),
				{ status: 200, headers: { 'content-type': 'application/json' } }
			);
		};
		try {
			const res = await getExperimentSlaBreaches({ graphId: 'graph_1', p95Ms: 2000 });
			expect(res.breaches[0]?.nodeId).toBe('n2');
		} finally {
			(globalThis as any).fetch = originalFetch;
		}
	});

	it('queries failure taxonomy endpoint', async () => {
		const originalFetch = globalThis.fetch;
		(globalThis as any).fetch = async (input: RequestInfo | URL) => {
			const url = String(input);
			expect(url.startsWith('/api/experiments/failures/taxonomy?')).toBe(true);
			return new Response(
				JSON.stringify({
					schemaVersion: 1,
					taxonomy: [{ errorCode: 'MODEL_EXECUTION_FAILED', count: 4 }]
				}),
				{ status: 200, headers: { 'content-type': 'application/json' } }
			);
		};
		try {
			const res = await getExperimentFailureTaxonomy({ graphId: 'graph_1' });
			expect(res.taxonomy[0]?.errorCode).toBe('MODEL_EXECUTION_FAILED');
		} finally {
			(globalThis as any).fetch = originalFetch;
		}
	});
});

