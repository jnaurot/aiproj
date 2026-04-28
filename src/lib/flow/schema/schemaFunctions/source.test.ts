import { describe, expect, it } from 'vitest';
import { schemaFn_source } from './source';

describe('schemaFn_source', () => {
	it('derives table schema from priming sample', () => {
		const result = schemaFn_source([], {
			sourceKind: 'file',
			priming: {
				sample_schema: {
					fields: [
						{ name: 'a', type: 'number' },
						{ name: 'b', type: 'string' }
					]
				}
			}
		});
		expect(result.ok).toBe(true);
		if (result.ok) expect(result.output.columns.map((c) => c.name)).toEqual(['a', 'b']);
	});

	it('derives table schema from priming.sampleSchema variant', () => {
		const result = schemaFn_source([], {
			sourceKind: 'file',
			priming: {
				sampleSchema: {
					fields: [
						{ name: 'id', type: 'integer' },
						{ name: 'title', type: 'text' }
					]
				}
			}
		});
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.output.mode).toBe('table');
			expect(result.output.columns.map((c) => `${c.name}:${c.type}`)).toEqual(['id:number', 'title:string']);
		}
	});

	it('uses declared database schema when provided', () => {
		const result = schemaFn_source([], {
			sourceKind: 'database',
			declared_schema: {
				fields: [
					{ name: 'user_id', type: 'integer', nullable: false },
					{ name: 'category', type: 'string', nullable: true }
				]
			}
		});
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.output.mode).toBe('table');
			expect(result.output.columns.map((c) => `${c.name}:${c.type}:${c.nullable}`)).toEqual([
				'user_id:number:false',
				'category:string:true'
			]);
		}
	});

	it('returns opaque output when no priming available', () => {
		const result = schemaFn_source([], { sourceKind: 'file' });
		expect(result.ok).toBe(true);
		if (result.ok) expect(result.output.mode).toBe('opaque');
	});

	it('keeps database source opaque when declared schema is missing', () => {
		const result = schemaFn_source([], { sourceKind: 'database' });
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.output.mode).toBe('opaque');
			expect(String(result.output.note ?? '')).toContain('No declared schema');
		}
	});

	it('stream source carries stream cardinality + consume_once', () => {
		const result = schemaFn_source([], { sourceKind: 'stream' });
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.output.properties?.cardinality).toBe('stream');
			expect(result.output.properties?.consume_once).toBe(true);
		}
	});
});

