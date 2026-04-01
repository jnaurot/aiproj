import { describe, expect, it } from 'vitest';

import {
	getExperimentAdaptiveDecisions,
	getExperimentBottlenecks,
	getExperimentFailureTaxonomy,
	getExperimentNodeTrends,
	getExperimentRunSummary,
	getExperimentRunTrends,
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
			expect(url.includes('startAt=2026-03-31T00%3A00%3A00Z')).toBe(true);
			expect(url.includes('endAt=2026-03-31T01%3A00%3A00Z')).toBe(true);
			expect(url.includes('sort=value_desc')).toBe(true);
			expect(url.includes('limit=50')).toBe(true);
			expect(url.includes('offset=5')).toBe(true);
			return new Response(
				JSON.stringify({
					schemaVersion: 1,
					metric: 'p95Ms',
					sort: 'value_desc',
					total: 1,
					points: [{ runId: 'r1', createdAt: '2026-03-31T00:00:00Z', nodeId: 'n1', metric: 'p95Ms', value: 1200 }]
				}),
				{ status: 200, headers: { 'content-type': 'application/json' } }
			);
		};
		try {
			const res = await getExperimentNodeTrends({
				graphId: 'graph_1',
				nodeId: 'n1',
				metric: 'p95Ms',
				startAt: '2026-03-31T00:00:00Z',
				endAt: '2026-03-31T01:00:00Z',
				sort: 'value_desc',
				limit: 50,
				offset: 5
			});
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
			expect(url.includes('startAt=2026-03-31T00%3A00%3A00Z')).toBe(true);
			expect(url.includes('endAt=2026-03-31T01%3A00%3A00Z')).toBe(true);
			expect(url.includes('limit=25')).toBe(true);
			expect(url.includes('offset=2')).toBe(true);
			return new Response(
				JSON.stringify({
					schemaVersion: 1,
					thresholdMs: 2000,
					total: 1,
					breaches: [{ runId: 'r2', createdAt: '2026-03-31T00:10:00Z', nodeId: 'n2', p95Ms: 3400, thresholdMs: 2000 }]
				}),
				{ status: 200, headers: { 'content-type': 'application/json' } }
			);
		};
		try {
			const res = await getExperimentSlaBreaches({
				graphId: 'graph_1',
				p95Ms: 2000,
				startAt: '2026-03-31T00:00:00Z',
				endAt: '2026-03-31T01:00:00Z',
				limit: 25,
				offset: 2
			});
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
			expect(url.includes('startAt=2026-03-31T00%3A00%3A00Z')).toBe(true);
			expect(url.includes('endAt=2026-03-31T01%3A00%3A00Z')).toBe(true);
			expect(url.includes('limit=10')).toBe(true);
			expect(url.includes('offset=3')).toBe(true);
			return new Response(
				JSON.stringify({
					schemaVersion: 1,
					total: 1,
					taxonomy: [{ errorCode: 'MODEL_EXECUTION_FAILED', count: 4 }]
				}),
				{ status: 200, headers: { 'content-type': 'application/json' } }
			);
		};
		try {
			const res = await getExperimentFailureTaxonomy({
				graphId: 'graph_1',
				startAt: '2026-03-31T00:00:00Z',
				endAt: '2026-03-31T01:00:00Z',
				limit: 10,
				offset: 3
			});
			expect(res.taxonomy[0]?.errorCode).toBe('MODEL_EXECUTION_FAILED');
		} finally {
			(globalThis as any).fetch = originalFetch;
		}
	});

	it('queries run trends endpoint', async () => {
		const originalFetch = globalThis.fetch;
		(globalThis as any).fetch = async (input: RequestInfo | URL) => {
			const url = String(input);
			expect(url.startsWith('/api/experiments/trends/runs?')).toBe(true);
			expect(url.includes('graphId=graph_1')).toBe(true);
			expect(url.includes('startAt=2026-03-31T00%3A00%3A00Z')).toBe(true);
			expect(url.includes('endAt=2026-03-31T01%3A00%3A00Z')).toBe(true);
			expect(url.includes('limit=5')).toBe(true);
			expect(url.includes('offset=1')).toBe(true);
			return new Response(
				JSON.stringify({
					schemaVersion: 1,
					total: 1,
					points: [
						{
							runId: 'r3',
							createdAt: '2026-03-31T00:20:00Z',
							status: 'succeeded',
							runtimeMs: 987,
							peakConcurrency: 4
						}
					]
				}),
				{ status: 200, headers: { 'content-type': 'application/json' } }
			);
		};
		try {
			const res = await getExperimentRunTrends({
				graphId: 'graph_1',
				startAt: '2026-03-31T00:00:00Z',
				endAt: '2026-03-31T01:00:00Z',
				sort: 'runtime_desc',
				limit: 5,
				offset: 1
			});
			expect(res.points[0]?.runId).toBe('r3');
			expect(res.points[0]?.peakConcurrency).toBe(4);
		} finally {
			(globalThis as any).fetch = originalFetch;
		}
	});

	it('queries adaptive decisions endpoint', async () => {
		const originalFetch = globalThis.fetch;
		(globalThis as any).fetch = async (input: RequestInfo | URL) => {
			const url = String(input);
			expect(url.startsWith('/api/experiments/adaptive/decisions?')).toBe(true);
			expect(url.includes('graphId=graph_1')).toBe(true);
			expect(url.includes('startAt=2026-03-31T00%3A00%3A00Z')).toBe(true);
			expect(url.includes('endAt=2026-03-31T01%3A00%3A00Z')).toBe(true);
			expect(url.includes('mode=enforce')).toBe(true);
			expect(url.includes('modeSource=run_override')).toBe(true);
			expect(url.includes('severity=high')).toBe(true);
			expect(url.includes('sort=impact_desc')).toBe(true);
			expect(url.includes('limit=20')).toBe(true);
			expect(url.includes('offset=2')).toBe(true);
			return new Response(
				JSON.stringify({
					schemaVersion: 1,
					total: 1,
					decisions: [
						{
							runId: 'r4',
							at: '2026-03-31T00:40:00Z',
							mode: 'enforce',
							enforced: true,
							explanation: { score: 82, severity: 'high' }
						}
					]
				}),
				{ status: 200, headers: { 'content-type': 'application/json' } }
			);
		};
		try {
			const res = await getExperimentAdaptiveDecisions({
				graphId: 'graph_1',
				startAt: '2026-03-31T00:00:00Z',
				endAt: '2026-03-31T01:00:00Z',
				mode: 'enforce',
				modeSource: 'run_override',
				severity: 'high',
				sort: 'impact_desc',
				limit: 20,
				offset: 2
			});
			expect(res.decisions[0]?.mode).toBe('enforce');
			expect(res.decisions[0]?.enforced).toBe(true);
		} finally {
			(globalThis as any).fetch = originalFetch;
		}
	});

	it('queries run summary endpoint', async () => {
		const originalFetch = globalThis.fetch;
		(globalThis as any).fetch = async (input: RequestInfo | URL) => {
			const url = String(input);
			expect(url).toBe('/api/experiments/runs/run_42');
			return new Response(
				JSON.stringify({
					schemaVersion: 1,
					experiment: { runId: 'run_42', status: 'succeeded', analytics: {} }
				}),
				{ status: 200, headers: { 'content-type': 'application/json' } }
			);
		};
		try {
			const res = await getExperimentRunSummary('run_42');
			expect(String(res.experiment?.runId ?? '')).toBe('run_42');
		} finally {
			(globalThis as any).fetch = originalFetch;
		}
	});

	it('queries bottlenecks endpoint', async () => {
		const originalFetch = globalThis.fetch;
		(globalThis as any).fetch = async (input: RequestInfo | URL) => {
			const url = String(input);
			expect(url.startsWith('/api/experiments/bottlenecks?')).toBe(true);
			expect(url.includes('graphId=graph_1')).toBe(true);
			expect(url.includes('startAt=2026-03-31T00%3A00%3A00Z')).toBe(true);
			expect(url.includes('endAt=2026-03-31T01%3A00%3A00Z')).toBe(true);
			expect(url.includes('sort=p95_desc')).toBe(true);
			expect(url.includes('limit=15')).toBe(true);
			expect(url.includes('offset=3')).toBe(true);
			return new Response(
				JSON.stringify({
					schemaVersion: 1,
					total: 1,
					nodes: [
						{
							nodeId: 'node_a',
							runsSeen: 4,
							p95AvgMs: 1200,
							p95MaxMs: 2100,
							avgMsAvg: 900,
							maxMsMax: 3200,
							countSum: 99,
							bottleneckScore: 2115
						}
					]
				}),
				{ status: 200, headers: { 'content-type': 'application/json' } }
			);
		};
		try {
			const res = await getExperimentBottlenecks({
				graphId: 'graph_1',
				startAt: '2026-03-31T00:00:00Z',
				endAt: '2026-03-31T01:00:00Z',
				sort: 'p95_desc',
				limit: 15,
				offset: 3
			});
			expect(res.nodes[0]?.nodeId).toBe('node_a');
		} finally {
			(globalThis as any).fetch = originalFetch;
		}
	});
});
