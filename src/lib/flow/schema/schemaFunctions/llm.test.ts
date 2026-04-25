import { describe, expect, it } from 'vitest';
import { schemaFn_llm } from './llm';

describe('schemaFn_llm', () => {
	it('returns opaque for non-json output mode', () => {
		const result = schemaFn_llm([], { output: { mode: 'text' } } as any);
		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.output.mode).toBe('opaque');
	});

	it('maps json schema primitives to schema-plane table columns', () => {
		const result = schemaFn_llm(
			[],
			{
				output: {
					mode: 'json',
					jsonSchema: {
						type: 'object',
						properties: {
							user_id: { type: 'integer' },
							category: { type: 'string' },
							score: { type: 'number' },
							is_active: { type: 'boolean' },
							nested: { type: 'object' },
							items: { type: 'array' }
						},
						required: ['user_id', 'category'],
						additionalProperties: false
					}
				}
			} as any
		);
		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.output.mode).toBe('table');
		expect(result.output.columns.map((column) => `${column.name}:${column.type}:${column.nullable}`)).toEqual([
			'user_id:number:false',
			'category:string:false',
			'score:number:true',
			'is_active:boolean:true',
			'nested:unknown:true',
			'items:unknown:true'
		]);
		expect((result.output.properties as any)?.additional_properties).toBe(false);
	});

	it('defaults additional_properties to true when omitted', () => {
		const result = schemaFn_llm(
			[],
			{
				output: {
					mode: 'json',
					jsonSchema: {
						type: 'object',
						properties: {
							name: { type: 'string' }
						}
					}
				}
			} as any
		);
		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect((result.output.properties as any)?.additional_properties).toBe(true);
	});

	it('is resilient when json schema properties are absent', () => {
		const result = schemaFn_llm([], { output: { mode: 'json', jsonSchema: { type: 'object' } } } as any);
		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.output.mode).toBe('table');
		expect(result.output.columns).toEqual([]);
		expect((result.output.properties as any)?.source).toBe('llm_json_schema');
	});
});
