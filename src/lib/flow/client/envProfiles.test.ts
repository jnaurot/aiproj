import { describe, expect, it } from 'vitest';

import { installEnvProfile, listEnvProfiles } from './envProfiles';

describe('envProfiles client', () => {
	it('lists workspace env profiles', async () => {
		const originalFetch = globalThis.fetch;
		(globalThis as any).fetch = async (input: RequestInfo | URL) => {
			expect(String(input)).toBe('/api/env/profiles');
			return new Response(
				JSON.stringify({
					schemaVersion: 1,
					profiles: [{ profileId: 'core', packages: ['numpy'], installed: true, missingPackages: [], health: 'ok', platformNotes: [] }]
				}),
				{ status: 200, headers: { 'content-type': 'application/json' } }
			);
		};
		try {
			const res = await listEnvProfiles();
			expect(Array.isArray(res.profiles)).toBe(true);
			expect(res.profiles[0]?.profileId).toBe('core');
		} finally {
			(globalThis as any).fetch = originalFetch;
		}
	});

	it('installs a profile through the API route', async () => {
		const originalFetch = globalThis.fetch;
		(globalThis as any).fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
			expect(String(input)).toBe('/api/env/profiles/install');
			expect(String(init?.method ?? '')).toBe('POST');
			const parsed = JSON.parse(String(init?.body ?? '{}'));
			expect(parsed.profileId).toBe('core');
			return new Response(
				JSON.stringify({
					schemaVersion: 1,
					profileId: 'core',
					packages: ['numpy'],
					status: 'already_installed',
					installed: true,
					missingPackages: []
				}),
				{ status: 200, headers: { 'content-type': 'application/json' } }
			);
		};
		try {
			const res = await installEnvProfile('core');
			expect(res.profileId).toBe('core');
			expect(res.installed).toBe(true);
		} finally {
			(globalThis as any).fetch = originalFetch;
		}
	});
});
