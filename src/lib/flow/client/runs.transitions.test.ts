import { describe, expect, it } from 'vitest';

import { getRunTransitions } from './runs';

describe('runs client transitions endpoint', () => {
	it('queries run transitions endpoint with pagination and filter params', async () => {
		const originalFetch = globalThis.fetch;
		(globalThis as any).fetch = async (input: RequestInfo | URL) => {
			const url = String(input);
			expect(url).toContain('/api/runs/run_123/transitions?');
			expect(url).toContain('after_id=5');
			expect(url).toContain('limit=75');
			expect(url).toContain('entity=node');
			expect(url).toContain('include_violations=false');
			expect(url).toContain('violations_only=false');
			return new Response(
				JSON.stringify({
					runId: 'run_123',
					afterId: 5,
					limit: 75,
					entity: 'node',
					includeViolations: false,
					violationsOnly: false,
					nextAfterId: 8,
					scannedAfterId: 9,
					events: [
						{
							id: 8,
							runId: 'run_123',
							type: 'state_transition',
							at: '2026-03-31T00:00:00Z',
							payload: {
								entity: 'run',
								entityId: 'run_123',
								source: 'running',
								target: 'pausing',
								reason: 'request_pause'
							}
						}
					]
				}),
				{ status: 200, headers: { 'content-type': 'application/json' } }
			);
		};
		try {
			const res = await getRunTransitions({
				runId: 'run_123',
				afterId: 5,
				limit: 75,
				entity: 'node',
				includeViolations: false,
				violationsOnly: false
			});
			expect(res.events[0]?.type).toBe('state_transition');
			expect(res.events[0]?.payload?.target).toBe('pausing');
			expect(res.scannedAfterId).toBe(9);
		} finally {
			(globalThis as any).fetch = originalFetch;
		}
	});
});
