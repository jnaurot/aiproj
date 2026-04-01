import { describe, expect, it } from 'vitest';

import { getExperimentRegressions } from './runs';

describe('runs client regressions endpoint', () => {
	it('queries experiments regression endpoint with run + baseline params', async () => {
		const originalFetch = globalThis.fetch;
		(globalThis as any).fetch = async (input: RequestInfo | URL) => {
			const url = String(input);
			expect(url.startsWith('/api/experiments/regressions?')).toBe(true);
			expect(url.includes('runId=run_new')).toBe(true);
			expect(url.includes('baselineRunId=run_old')).toBe(true);
			expect(url.includes('alertType=latency')).toBe(true);
			return new Response(
				JSON.stringify({
					schemaVersion: 1,
					runId: 'run_new',
					baselineRunId: 'run_old',
					alertType: 'latency',
					alerts: [{ type: 'latency_regression', reasonCode: 'LATENCY_DRIFT', nodeId: 'n1' }]
				}),
				{ status: 200, headers: { 'content-type': 'application/json' } }
			);
		};
		try {
			const res = await getExperimentRegressions({
				runId: 'run_new',
				baselineRunId: 'run_old',
				alertType: 'latency',
				latencyDriftPct: 20,
				failureDriftAbs: 1
			});
			expect(res.alerts[0]?.reasonCode).toBe('LATENCY_DRIFT');
		} finally {
			(globalThis as any).fetch = originalFetch;
		}
	});
});
