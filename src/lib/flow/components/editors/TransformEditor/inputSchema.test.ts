import { describe, expect, it } from 'vitest';
import { parseInputSchemaView } from './inputSchema';

describe('parseInputSchemaView', () => {
	it('reads TABLE_V1 schema envelope and preserves columns/rowCount/provenance', () => {
		const payloadSchema = {
			type: 'table',
			schema: {
				contract: 'TABLE_V1',
				version: 1,
				table: {
					columns: [
						{ name: 'id', type: 'int64' },
						{ name: 'name' }
					],
					coercion: {
						mode: 'text_1row',
						lossy: false
					}
				},
				stats: { rowCount: 42 },
				provenance: { sourceKind: 'db', tableName: 'users' }
			}
		};
		const parsed = parseInputSchemaView('a1', 'Source.in', payloadSchema);
		expect(parsed.artifactId).toBe('a1');
		expect(parsed.label).toBe('Source.in');
		expect(parsed.rowCount).toBe(42);
		expect(parsed.provenance?.sourceKind).toBe('db');
		expect(parsed.provenance?.tableName).toBe('users');
		expect(parsed.coercion).toEqual({ mode: 'text_1row', lossy: false });
		expect(parsed.columns).toEqual([
			{ name: 'id', type: 'int64' },
			{ name: 'name', type: 'unknown' }
		]);
	});

	it('falls back to top-level payload columns when schema envelope missing', () => {
		const payloadSchema = {
			type: 'table',
			columns: [{ name: 'col_a', dtype: 'string' }, 'col_b']
		};
		const parsed = parseInputSchemaView('a2', 'Input.in', payloadSchema);
		expect(parsed.columns).toEqual([
			{ name: 'col_a', type: 'string' },
			{ name: 'col_b', type: 'unknown' }
		]);
		expect(parsed.rowCount).toBeNull();
		expect(parsed.provenance).toBeNull();
		expect(parsed.coercion).toBeNull();
	});

	it('defaults text payloads to a text column when schema columns are absent', () => {
		const parsed = parseInputSchemaView('a3', 'Source.in', {
			type: 'text'
		});
		expect(parsed.columns).toEqual([{ name: 'text', type: 'string' }]);
	});
});
