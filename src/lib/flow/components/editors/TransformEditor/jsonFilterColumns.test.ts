import { describe, expect, it } from 'vitest';
import { buildJsonFilterColumns } from './jsonFilterColumns';

describe('json filter key suggestion sources', () => {
	it('includes expected input schema keys pre-run', () => {
		const selectedNode: any = {
			id: 'n1',
			data: {
				schema: {
					expectedInputSchemas: {
						in: {
							typedSchema: {
								type: 'json',
								fields: ['thing1', 'thing2'],
							},
						},
					},
				},
			},
		};
		const columns = buildJsonFilterColumns({ selectedNode, inputColumns: [], inputSchemaColumns: [], inputSchemas: [] });
		expect(columns.map((column) => column.name)).toEqual(['thing1', 'thing2']);
	});

	it('merges inferred and expected columns without clobbering expected keys', () => {
		const selectedNode: any = {
			id: 'n1',
			data: {
				schema: {
					expectedInputSchemas: {
						in: {
							typedSchema: {
								type: 'json',
								fields: [{ name: 'thing1', type: 'text' }, { name: 'thing2', type: 'text' }],
							},
						},
					},
				},
			},
		};
		const columns = buildJsonFilterColumns({
			selectedNode,
			inputColumns: ['thing3'],
			inputSchemaColumns: [{ name: 'thing3', type: 'integer' }],
			inputSchemas: [],
		});
		expect(columns.map((column) => column.name)).toEqual(['thing1', 'thing2', 'thing3']);
		expect(columns.find((column) => column.name === 'thing3')?.type).toBe('integer');
	});

	it('ignores param/control expected input handles for key candidates', () => {
		const selectedNode: any = {
			id: 'n1',
			data: {
				schema: {
					expectedInputSchemas: {
						param_context: { typedSchema: { type: 'text', fields: ['ignored'] } },
						control_in: { typedSchema: { type: 'none', fields: ['ignored2'] } },
						in: { typedSchema: { type: 'json', fields: ['keep_me'] } },
					},
				},
			},
		};
		const columns = buildJsonFilterColumns({ selectedNode, inputColumns: [], inputSchemaColumns: [], inputSchemas: [] });
		expect(columns.map((column) => column.name)).toEqual(['keep_me']);
	});

	it('returns empty list when no schema candidates exist', () => {
		const columns = buildJsonFilterColumns({
			selectedNode: undefined,
			inputColumns: [],
			inputSchemaColumns: [],
			inputSchemas: [],
		});
		expect(columns).toEqual([]);
	});
});
