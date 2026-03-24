import { describe, expect, it } from 'vitest';

import { defaultTransformDeriveParams } from './transformDefaults';
import { TransformDeriveParamsSchema } from './transform';

describe('TransformDeriveParamsSchema dual mode', () => {
	it('defaults new derive params to rules mode', () => {
		expect(defaultTransformDeriveParams.mode).toBe('rules');
		const parsed = TransformDeriveParamsSchema.parse(defaultTransformDeriveParams);
		expect(parsed.mode).toBe('rules');
		expect(parsed.rules.length).toBeGreaterThan(0);
	});

	it('accepts valid formula rules with literals, column refs, and valueFrom args', () => {
		const parsed = TransformDeriveParamsSchema.safeParse({
			mode: 'rules',
			rules: [
				{
					name: 'salary_plus_target',
					formula: {
						op: 'add',
						args: [
							{ column: 'salary' },
							{
								valueFrom: {
									handle: 'param_config',
									path: 'preferences.salary_target'
								}
							}
						]
					}
				},
				{
					name: 'title_clean',
					formula: {
						op: 'trim',
						args: [{ column: 'title' }]
					}
				}
			],
			columns: []
		});
		expect(parsed.success).toBe(true);
	});

	it('rejects invalid formula op', () => {
		const parsed = TransformDeriveParamsSchema.safeParse({
			mode: 'rules',
			rules: [
				{
					name: 'x',
					formula: {
						op: 'pow',
						args: [1, 2]
					}
				}
			],
			columns: []
		});
		expect(parsed.success).toBe(false);
	});

	it('rejects invalid arity', () => {
		const parsed = TransformDeriveParamsSchema.safeParse({
			mode: 'rules',
			rules: [
				{
					name: 'x',
					formula: {
						op: 'add',
						args: [1]
					}
				}
			],
			columns: []
		});
		expect(parsed.success).toBe(false);
	});

	it('rejects malformed valueFrom paths', () => {
		const parsed = TransformDeriveParamsSchema.safeParse({
			mode: 'rules',
			rules: [
				{
					name: 'x',
					formula: {
						op: 'coalesce',
						args: [
							{
								valueFrom: {
									handle: 'param_config',
									path: '$.bad.path'
								}
							}
						]
					}
				}
			],
			columns: []
		});
		expect(parsed.success).toBe(false);
	});

	it('treats legacy sql derive payloads as sql mode', () => {
		const parsed = TransformDeriveParamsSchema.parse({
			columns: [{ name: 'x', expr: 'length(text)' }]
		});
		expect(parsed.mode).toBe('sql');
		expect(parsed.columns.length).toBe(1);
	});
});
