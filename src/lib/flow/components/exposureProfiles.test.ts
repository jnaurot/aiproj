import { describe, expect, it } from 'vitest';

import {
	comparePublishedProfiles,
	materializeExposureProfiles,
	normalizeExposureRegistry
} from '$lib/flow/components/exposureProfiles';

describe('component exposure profiles', () => {
	it('derives default exposure registry from api when none provided', () => {
		const normalized = normalizeExposureRegistry(
			[],
			{
				inputs: [{ name: 'in_data', typedSchema: { type: 'table', fields: [] } }],
				outputs: [{ name: 'out_data', typedSchema: { type: 'json', fields: [] } }]
			} as any
		);
		expect(normalized.some((rec) => rec.handle_id === 'work_in::in_data')).toBe(true);
		expect(normalized.some((rec) => rec.handle_id === 'data_out::out_data')).toBe(true);
	});

	it('materializes published and debug profiles separately', () => {
		const { published_profile, debug_profile } = materializeExposureProfiles([
			{
				handle_id: 'h1',
				alias: 'h1',
				internal_source_path: 'out:h1',
				kind: 'data_output',
				native_contract: { type: 'json', fields: [] },
				exposed: true,
				published: true,
				debug_visible: false
			},
			{
				handle_id: 'h2',
				alias: 'h2',
				internal_source_path: 'out:h2',
				kind: 'data_output',
				native_contract: { type: 'json', fields: [] },
				exposed: true,
				published: false,
				debug_visible: true
			}
		] as any);
		expect(published_profile.map((item) => item.handle_id)).toEqual(['h1']);
		expect(debug_profile.map((item) => item.handle_id)).toEqual(['h1', 'h2']);
	});

	it('detects breaking diff on removed/retyped published handles', () => {
		const diff = comparePublishedProfiles(
			[
				{
					handle_id: 'h1',
					alias: 'h1',
					internal_source_path: 'out:h1',
					kind: 'data_output',
					native_contract: { type: 'json', fields: [] },
					exposed: true,
					published: true,
					debug_visible: false
				},
				{
					handle_id: 'h2',
					alias: 'h2',
					internal_source_path: 'out:h2',
					kind: 'data_output',
					native_contract: { type: 'text', fields: [] },
					exposed: true,
					published: true,
					debug_visible: false
				}
			] as any,
			[
				{
					handle_id: 'h2',
					alias: 'h2',
					internal_source_path: 'out:h2',
					kind: 'data_output',
					native_contract: { type: 'json', fields: [] },
					exposed: true,
					published: true,
					debug_visible: false
				}
			] as any
		);
		expect(diff.breaking).toBe(true);
		expect(diff.removed).toEqual(['h1']);
		expect(diff.retyped.length).toBe(1);
	});
});

