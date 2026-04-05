import { describe, expect, it } from 'vitest';

import { buildMigrationDiagnostics } from '$lib/flow/components/componentMigrationDiagnostics';

describe('component migration diagnostics', () => {
	it('builds diagnostics for removed and retyped published handles', () => {
		const diagnostics = buildMigrationDiagnostics({
			breaking: true,
			removed: ['h_removed'],
			added: [],
			retyped: [
				{
					handle_id: 'h_retyped',
					before_kind: 'data_output',
					after_kind: 'data_output',
					before_type: 'text',
					after_type: 'json'
				}
			]
		});
		expect(diagnostics.map((d) => d.code)).toEqual(['HANDLE_REMOVED', 'HANDLE_RETYPED']);
	});
});

