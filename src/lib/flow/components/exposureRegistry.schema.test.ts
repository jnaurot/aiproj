import { describe, expect, it } from 'vitest';

import { ComponentParamsSchema } from '$lib/flow/schema/component';

describe('component exposure registry schema', () => {
	it('accepts exposure registry lifecycle flags and profiles', () => {
		const parsed = ComponentParamsSchema.safeParse({
			componentRef: { componentId: 'cmp_a', revisionId: 'crev_1', apiVersion: 'v1' },
			bindings: { inputs: {}, config: {}, outputs: {} },
			config: {},
			api: { inputs: [], outputs: [] },
			exposureRegistry: [
				{
					handle_id: 'data_out::out_data',
					alias: 'out_data',
					internal_source_path: 'out:out_data',
					kind: 'data_output',
					native_contract: { type: 'json', fields: [] },
					exposed: true,
					published: true,
					debug_visible: false
				}
			],
			published_profile: [],
			debug_profile: []
		});
		expect(parsed.success).toBe(true);
	});
});

