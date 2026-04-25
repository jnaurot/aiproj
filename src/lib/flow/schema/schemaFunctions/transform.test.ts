import { describe, expect, it } from 'vitest';
import { schemaFn_transform } from './transform';

const baseInput = {
	mode: 'table' as const,
	columns: [
		{ name: 'id', type: 'number' as const, nullable: false, properties: {} },
		{ name: 'text', type: 'string' as const, nullable: true, properties: {} }
	]
};

describe('schemaFn_transform', () => {
	it('passthrough operation returns input schema', () => {
		const result = schemaFn_transform([baseInput], { op: 'filter' });
		expect(result.ok).toBe(true);
		if (result.ok) expect(result.output.columns.map((c) => c.name)).toEqual(['id', 'text']);
	});

	it('select errors when column missing', () => {
		const result = schemaFn_transform([baseInput], { op: 'select', select: { columns: ['missing'] } });
		expect(result.ok).toBe(false);
		if (!result.ok) expect(result.error.code).toBe('SHAPE_MISMATCH');
	});

	it('join validates key type mismatch', () => {
		const right = {
			mode: 'table' as const,
			columns: [{ name: 'id', type: 'string' as const, nullable: false, properties: {} }]
		};
		const result = schemaFn_transform([baseInput, right], {
			op: 'join',
			join: { clauses: [{ leftCol: 'id', rightCol: 'id' }] }
		});
		expect(result.ok).toBe(false);
		if (!result.ok) expect(result.error.code).toBe('TYPE_MISMATCH');
	});

	it('join validates clauses against nodeId-qualified multi-in inputs', () => {
		const left = {
			mode: 'table' as const,
			columns: [{ name: 'id', type: 'number' as const, nullable: false, properties: {} }]
		};
		const right = {
			mode: 'table' as const,
			columns: [{ name: 'id', type: 'number' as const, nullable: false, properties: {} }]
		};
		const result = schemaFn_transform([left, right], {
			op: 'join',
			join: {
				clauses: [{ leftNodeId: 'n_left', leftCol: 'id', rightNodeId: 'n_right', rightCol: 'id', how: 'inner' }]
			},
			__schemaInputRefs: [
				{ sourceNodeId: 'n_left', targetHandle: 'in' },
				{ sourceNodeId: 'n_right', targetHandle: 'in' }
			]
		} as any);
		expect(result.ok).toBe(true);
	});

	it('aggregate builds grouped metric schema', () => {
		const result = schemaFn_transform([baseInput], {
			op: 'aggregate',
			aggregate: { groupBy: ['id'], metrics: [{ name: 'sum_id', op: 'sum', column: 'id' }] }
		});
		expect(result.ok).toBe(true);
		if (result.ok) expect(result.output.columns.map((c) => c.name)).toEqual(['id', 'sum_id']);
	});

	it('derive infers string output types for string formula ops', () => {
		const result = schemaFn_transform([baseInput], {
			op: 'derive',
			derive: {
				mode: 'rules',
				rules: [
					{ name: 'joined_text', formula: { op: 'concat', args: [{ column: 'text' }, 'x'] } },
					{ name: 'lower_text', formula: { op: 'lower', args: [{ column: 'text' }] } }
				]
			}
		} as any);
		expect(result.ok).toBe(true);
		if (!result.ok) return;
		const joined = result.output.columns.find((column) => column.name === 'joined_text');
		const lowered = result.output.columns.find((column) => column.name === 'lower_text');
		expect(joined?.type).toBe('string');
		expect(lowered?.type).toBe('string');
	});

	it('derive keeps numeric output types for numeric formula ops', () => {
		const result = schemaFn_transform([baseInput], {
			op: 'derive',
			derive: {
				mode: 'rules',
				rules: [{ name: 'id_plus_one', formula: { op: 'add', args: [{ column: 'id' }, 1] } }]
			}
		} as any);
		expect(result.ok).toBe(true);
		if (!result.ok) return;
		const derived = result.output.columns.find((column) => column.name === 'id_plus_one');
		expect(derived?.type).toBe('number');
	});
});

