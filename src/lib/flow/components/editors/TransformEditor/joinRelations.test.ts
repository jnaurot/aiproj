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
});

