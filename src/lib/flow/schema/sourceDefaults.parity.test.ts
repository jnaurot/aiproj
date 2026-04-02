import { describe, expect, test } from 'vitest';

import { defaultSourceParamsByKind } from '$lib/flow/schema/sourceDefaults';

describe('source frontend default mode/contract parity fixture', () => {
	test('test_source_default_mode_contract_parity_fixture_frontend', () => {
		const fixture = {
			file: 'text',
			database: 'table',
			api: 'json',
			object_store: 'text',
			warehouse: 'table'
		} as const;
		for (const [kind, expectedMode] of Object.entries(fixture)) {
			const params = (defaultSourceParamsByKind as Record<string, any>)[kind];
			expect(String(params?.output?.mode ?? '')).toBe(expectedMode);
		}
	});
});

