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
		if (result.ok) {
			expect(result.output.columns.map((c) => c.name)).toEqual(['a', 'b']);
			expect(result.output.properties?.sourceProvenance).toBe('sample');
		}
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
			expect(result.output.properties?.sourceProvenance).toBe('sample');
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
			expect(result.output.properties?.sourceProvenance).toBe('declared');
		}
	});

	it('maps declared JSON Schema properties to table columns', () => {
		const result = schemaFn_source([], {
			sourceKind: 'api',
			declared_json_schema: {
				type: 'object',
				properties: {
					user_id: { type: 'integer' },
					category: { type: 'string' },
					active: { type: 'boolean' }
				},
				required: ['user_id']
			}
		});
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.output.mode).toBe('table');
			expect(result.output.columns.map((c) => `${c.name}:${c.type}:${c.nullable}`)).toEqual([
				'user_id:number:false',
				'category:string:true',
				'active:boolean:true'
			]);
			expect(result.output.properties?.sourceProvenance).toBe('declared');
		}
	});

	it('uses artifact schema when declared schema is missing', () => {
		const result = schemaFn_source([], {
			sourceKind: 'database',
			introspected_schema: {
				fields: [
					{ name: 'id', type: 'integer', nullable: false },
					{ name: 'text', type: 'string', nullable: true }
				]
			}
		});
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.output.mode).toBe('table');
			expect(result.output.columns.map((c) => c.name)).toEqual(['id', 'text']);
			expect(result.output.properties?.sourceProvenance).toBe('artifact');
		}
	});

	it('applies precedence declared > artifact > sample', () => {
		const result = schemaFn_source([], {
			sourceKind: 'database',
			declared_schema: {
				fields: [{ name: 'declared_col', type: 'string', nullable: true }]
			},
			introspected_schema: {
				fields: [{ name: 'artifact_col', type: 'string', nullable: true }]
			},
			priming: {
				sample_schema: {
					fields: [{ name: 'sample_col', type: 'string', nullable: true }]
				}
			}
		});
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.output.mode).toBe('table');
			expect(result.output.columns.map((c) => c.name)).toEqual(['declared_col']);
			expect(result.output.properties?.sourceProvenance).toBe('declared');
		}
	});

	it('returns opaque output when no priming available', () => {
		const result = schemaFn_source([], { sourceKind: 'file' });
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.output.mode).toBe('opaque');
			expect(result.output.properties?.sourceProvenance).toBe('opaque');
		}
	});

	it('keeps database source opaque when no declared/artifact/sample schema is available', () => {
		const result = schemaFn_source([], { sourceKind: 'database' });
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.output.mode).toBe('opaque');
			expect(result.output.properties?.sourceProvenance).toBe('opaque');
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

