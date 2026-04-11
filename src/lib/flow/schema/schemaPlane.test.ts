import { describe, expect, it } from 'vitest';
import {
	AbstractPropertiesSchema,
	SchemaErrorSchema,
	SchemaPlaneOutputSchema
} from './schemaPlane';
import {
	OPAQUE_SCHEMA,
	UNKNOWN_SCHEMA,
	clearSchemaFunctionRegistryForTest,
	getSchemaFunction,
	registerSchemaFunction
} from './schemaRegistry';

describe('schemaPlane schemas', () => {
	it('accepts minimal table output', () => {
		const parsed = SchemaPlaneOutputSchema.parse({ mode: 'table', columns: [] });
		expect(parsed.mode).toBe('table');
	});

	it('accepts tensor output with symbolic shape', () => {
		const parsed = SchemaPlaneOutputSchema.parse({
			mode: 'tensor',
			columns: [],
			shape: [32, 'T', 128]
		});
		expect(parsed.shape).toEqual([32, 'T', 128]);
	});

	it('accepts opaque output with no columns', () => {
		const parsed = SchemaPlaneOutputSchema.parse({ mode: 'opaque', columns: [] });
		expect(parsed.mode).toBe('opaque');
	});

	it('abstract properties schema preserves unknown keys', () => {
		const parsed = AbstractPropertiesSchema.parse({ custom: 'value', normalized: true });
		expect((parsed as any).custom).toBe('value');
		expect(parsed.normalized).toBe(true);
	});

	it('schema error requires handles', () => {
		expect(() =>
			SchemaErrorSchema.parse({
				code: 'SHAPE_MISMATCH',
				message: 'missing'
			} as any)
		).toThrow();
	});

	it('opaque schema sentinel is valid', () => {
		expect(() => SchemaPlaneOutputSchema.parse(OPAQUE_SCHEMA)).not.toThrow();
	});

	it('unknown schema sentinel is valid', () => {
		expect(() => SchemaPlaneOutputSchema.parse(UNKNOWN_SCHEMA)).not.toThrow();
	});
});

describe('schema registry', () => {
	it('register + get returns same function ref', () => {
		clearSchemaFunctionRegistryForTest();
		const fn = () => ({ ok: true, output: OPAQUE_SCHEMA } as const);
		registerSchemaFunction('test-kind', fn);
		expect(getSchemaFunction('test-kind')).toBe(fn);
	});
});

