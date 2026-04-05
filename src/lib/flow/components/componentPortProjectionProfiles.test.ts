import { describe, expect, it } from 'vitest';

import { projectConnectableHandles } from '$lib/flow/components/exposurePortProjection';

describe('component port projection profiles', () => {
	it('returns only published handles as connectable ports', () => {
		const handles = projectConnectableHandles([
			{
				handle_id: 'h_pub',
				alias: 'h_pub',
				internal_source_path: 'out:h_pub',
				kind: 'data_output',
				native_contract: { type: 'json', fields: [] },
				exposed: true,
				published: true,
				debug_visible: false
			},
			{
				handle_id: 'h_dbg',
				alias: 'h_dbg',
				internal_source_path: 'out:h_dbg',
				kind: 'data_output',
				native_contract: { type: 'json', fields: [] },
				exposed: true,
				published: false,
				debug_visible: true
			}
		] as any);
		expect(handles.map((h) => h.handle_id)).toEqual(['h_pub']);
	});
});

