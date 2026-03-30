import { describe, expect, it } from 'vitest';

import { listRuntimeEnvVars, updateRuntimeEnvVars } from './envVars';

describe('envVars client', () => {
	it('lists runtime env vars', async () => {
		const originalFetch = globalThis.fetch;
		(globalThis as any).fetch = async (input: RequestInfo | URL) => {
			expect(String(input)).toBe('/api/env/vars');
			return new Response(
				JSON.stringify({
					schemaVersion: 1,
					vars: [{ name: 'RUNNER_MAX_CONCURRENCY', value: '4', source: 'default' }]
				}),
				{ status: 200, headers: { 'content-type': 'application/json' } }
			);
		};
		try {
			const res = await listRuntimeEnvVars();
			expect(Array.isArray(res.vars)).toBe(true);
			expect(res.vars[0]?.name).toBe('RUNNER_MAX_CONCURRENCY');
		} finally {
			(globalThis as any).fetch = originalFetch;
		}
	});

	it('updates runtime env vars', async () => {
		const originalFetch = globalThis.fetch;
		(globalThis as any).fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
			expect(String(input)).toBe('/api/env/vars');
			expect(String(init?.method ?? '')).toBe('POST');
			const body = JSON.parse(String(init?.body ?? '{}'));
			expect(body?.updates?.[0]?.name).toBe('RUNNER_MAX_CONCURRENCY');
			expect(body?.updates?.[0]?.value).toBe('3');
			return new Response(
				JSON.stringify({
					schemaVersion: 1,
					updated: ['RUNNER_MAX_CONCURRENCY'],
					restartRequired: false,
					vars: [{ name: 'RUNNER_MAX_CONCURRENCY', value: '3', source: 'override' }]
				}),
				{ status: 200, headers: { 'content-type': 'application/json' } }
			);
		};
		try {
			const res = await updateRuntimeEnvVars([{ name: 'RUNNER_MAX_CONCURRENCY', value: '3' }]);
			expect(res.updated).toContain('RUNNER_MAX_CONCURRENCY');
			expect(res.vars[0]?.value).toBe('3');
		} finally {
			(globalThis as any).fetch = originalFetch;
		}
	});
});

