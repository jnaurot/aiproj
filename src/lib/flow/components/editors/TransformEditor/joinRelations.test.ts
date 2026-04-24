import { describe, expect, it } from 'vitest';
import { buildJoinInputRelations } from './joinRelations';
import type { InputSchemaView } from './inputSchema';

function schema(overrides: Partial<InputSchemaView>): InputSchemaView {
	return {
		artifactId: 'a1',
		label: 'NodeA.in',
		sourceNodeId: 'n_a',
		sourceDisplayName: 'NodeA',
		inputHandle: 'in',
		columns: [],
		rowCount: null,
		provenance: null,
		coercion: null,
		schemaSource: 'unknown',
		schemaState: 'unknown',
		...overrides
	};
}

describe('buildJoinInputRelations', () => {
	it('builds deterministic nodeId-keyed relations from multi-in schemas', () => {
		const relations = buildJoinInputRelations([
			schema({
				sourceNodeId: 'n_b',
				sourceDisplayName: 'BNode',
				columns: [{ name: 'id', type: 'number' }]
			}),
			schema({
				sourceNodeId: 'n_a',
				sourceDisplayName: 'ANode',
				columns: [{ name: 'id', type: 'number' }]
			})
		]);
		expect(relations.map((relation) => relation.sourceNodeId)).toEqual(['n_a', 'n_b']);
		expect(relations.map((relation) => relation.relationDisplayName)).toEqual(['ANode', 'BNode']);
	});

	it('merges duplicate source schemas and prefers typed over unknown columns', () => {
		const relations = buildJoinInputRelations([
			schema({
				sourceNodeId: 'n_a',
				sourceDisplayName: 'ANode',
				columns: [{ name: 'id', type: 'unknown' }]
			}),
			schema({
				sourceNodeId: 'n_a',
				sourceDisplayName: 'ANode',
				columns: [
					{ name: 'id', type: 'number' },
					{ name: 'name', type: 'string' }
				]
			})
		]);
		expect(relations).toHaveLength(1);
		expect(relations[0].columns).toEqual([
			{ name: 'id', type: 'number' },
			{ name: 'name', type: 'string' }
		]);
	});

	it('keeps deterministic relation ordering for three incoming join inputs', () => {
		const relations = buildJoinInputRelations([
			schema({
				sourceNodeId: 'n_z',
				sourceDisplayName: 'ZNode',
				columns: [{ name: 'id', type: 'number' }]
			}),
			schema({
				sourceNodeId: 'n_m',
				sourceDisplayName: 'MNode',
				columns: [{ name: 'id', type: 'number' }]
			}),
			schema({
				sourceNodeId: 'n_a',
				sourceDisplayName: 'ANode',
				columns: [{ name: 'id', type: 'number' }]
			})
		]);
		expect(relations.map((relation) => relation.relationDisplayName)).toEqual([
			'ANode',
			'MNode',
			'ZNode'
		]);
		expect(relations.map((relation) => relation.sourceNodeId)).toEqual(['n_a', 'n_m', 'n_z']);
	});

	it('keeps same local names from different scopes distinct via canonical display names', () => {
		const relations = buildJoinInputRelations([
			schema({
				sourceNodeId: 'n_top_a',
				sourceDisplayName: 'Transform_A',
				columns: [{ name: 'id', type: 'number' }]
			}),
			schema({
				sourceNodeId: 'cmp:comp_1:n_inner_a',
				sourceDisplayName: 'Comp1.Transform_A',
				columns: [{ name: 'id', type: 'number' }]
			})
		]);
		expect(relations).toHaveLength(2);
		expect(relations.map((relation) => relation.relationDisplayName)).toEqual([
			'Comp1.Transform_A',
			'Transform_A'
		]);
		expect(new Set(relations.map((relation) => relation.sourceNodeId)).size).toBe(2);
	});
});
