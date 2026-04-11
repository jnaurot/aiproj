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

	it('aggregate builds grouped metric schema', () => {
		const result = schemaFn_transform([baseInput], {
			op: 'aggregate',
			aggregate: { groupBy: ['id'], metrics: [{ name: 'sum_id', op: 'sum', column: 'id' }] }
		});
		expect(result.ok).toBe(true);
		if (result.ok) expect(result.output.columns.map((c) => c.name)).toEqual(['id', 'sum_id']);
	});
});

