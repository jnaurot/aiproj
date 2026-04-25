import { describe, expect, it } from 'vitest';
import { displayNodeNameForJoinClause, resolveJoinNodeColumns } from './joinModel';
import type { InputSchemaView } from './inputSchema';

function input(overrides: Partial<InputSchemaView>): InputSchemaView {
	return {
		artifactId: 'a1',
		label: 'NodeA.in',
		sourceNodeId: 'n_a',
		sourceDisplayName: 'NodeA',
		inputHandle: 'in',
		columns: [{ name: 'id', type: 'number' }],
		rowCount: null,
		provenance: null,
		coercion: null,
		schemaSource: 'unknown',
		schemaState: 'unknown',
		...overrides
	};
}

describe('resolveJoinNodeColumns', () => {
	it('uses sourceDisplayName relation labels from input schemas', () => {
		const rows = resolveJoinNodeColumns([
			input({ sourceNodeId: 'n_comp_a', sourceDisplayName: 'Comp1.Transform_A' }),
			input({ sourceNodeId: 'n_top_a', sourceDisplayName: 'Transform_A' }),
		]);
		expect(rows.map((row) => row.displayName)).toEqual(['Comp1.Transform_A', 'Transform_A']);
	});

	it('disambiguates duplicate display names by short node id', () => {
		const rows = resolveJoinNodeColumns([
			input({ sourceNodeId: 'n_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee', sourceDisplayName: 'Transform_A' }),
			input({ sourceNodeId: 'n_ffffffff-1111-2222-3333-444444444444', sourceDisplayName: 'Transform_A' }),
		]);
		expect(rows[0].displayName).toContain('Transform_A');
		expect(rows[1].displayName).toContain('Transform_A');
		expect(rows[0].displayName).not.toBe(rows[1].displayName);
	});

	it('falls back to node display-name map when relation metadata is temporarily unavailable', () => {
		const rows = resolveJoinNodeColumns([]);
		const display = displayNodeNameForJoinClause(
			'n_join_upstream_1',
			rows,
			{ n_join_upstream_1: 'Txt->Tbl (2)' }
		);
		expect(display).toBe('Txt->Tbl (2)');
	});
});
