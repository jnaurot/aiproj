import { describe, expect, it } from 'vitest';

import { normalizeExposureRegistry } from '$lib/flow/components/exposureProfiles';

describe('component boundary validation', () => {
	it('normalizes duplicate exposure handle ids deterministically', () => {
		const records = normalizeExposureRegistry(
			[
				{
					handle_id: 'dup',
					alias: 'dup',
					internal_source_path: 'out:dup',
					kind: 'data_output',
					native_contract: { type: 'json', fields: [] },
					exposed: true,
					published: true,
					debug_visible: false
				},
				{
					handle_id: 'dup',
					alias: 'dup2',
					internal_source_path: 'out:dup2',
					kind: 'data_output',
					native_contract: { type: 'json', fields: [] },
					exposed: true,
					published: false,
					debug_visible: true
				}
			] as any,
			{ inputs: [], outputs: [] } as any
		);
		expect(records[0].handle_id).toBe('dup');
		expect(records[1].handle_id).toBe('dup__2');
	});
});

