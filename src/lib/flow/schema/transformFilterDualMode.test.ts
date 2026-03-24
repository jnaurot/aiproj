import { describe, expect, it } from 'vitest';

import { defaultTransformFilterParams } from './transformDefaults';
import { TransformFilterParamsSchema } from './transform';

describe('TransformFilterParamsSchema dual mode', () => {
	it('defaults new filter params to rules mode', () => {
		expect(defaultTransformFilterParams.mode).toBe('rules');
		const parsed = TransformFilterParamsSchema.parse(defaultTransformFilterParams);
		expect(parsed.mode).toBe('rules');
	});

	it('accepts nested rules with literal and valueFrom values', () => {
		const parsed = TransformFilterParamsSchema.safeParse({
			mode: 'rules',
			expr: '',
			rules: {
				kind: 'group',
				op: 'all',
				conditions: [
					{
						kind: 'condition',
						column: 'salary',
						op: 'gte',
						value: 80000
					},
					{
						kind: 'group',
						op: 'any',
						conditions: [
							{
								kind: 'condition',
								column: 'job_type',
								op: 'eq',
								value: {
									valueFrom: {
										handle: 'param_config',
										path: 'preferences.job_type'
									}
								}
							}
						]
					}
				]
			}
		});
		expect(parsed.success).toBe(true);
	});

	it('rejects invalid operators', () => {
		const parsed = TransformFilterParamsSchema.safeParse({
			mode: 'rules',
			expr: '',
			rules: {
				kind: 'group',
				op: 'all',
				conditions: [
					{
						kind: 'condition',
						column: 'salary',
						op: 'between',
						value: 1000
					}
				]
			}
		});
		expect(parsed.success).toBe(false);
	});

	it('rejects missing values for value-required operators', () => {
		const parsed = TransformFilterParamsSchema.safeParse({
			mode: 'rules',
			expr: '',
			rules: {
				kind: 'group',
				op: 'all',
				conditions: [
					{
						kind: 'condition',
						column: 'salary',
						op: 'gte'
					}
				]
			}
		});
		expect(parsed.success).toBe(false);
	});

	it('rejects malformed valueFrom paths', () => {
		const parsed = TransformFilterParamsSchema.safeParse({
			mode: 'rules',
			expr: '',
			rules: {
				kind: 'group',
				op: 'all',
				conditions: [
					{
						kind: 'condition',
						column: 'location',
						op: 'eq',
						value: {
							valueFrom: {
								handle: 'param_config',
								path: '$.bad.path'
							}
						}
					}
				]
			}
		});
		expect(parsed.success).toBe(false);
	});

	it('treats legacy expr-only filter params as sql mode', () => {
		const parsed = TransformFilterParamsSchema.parse({
			expr: '"salary" > 50000'
		});
		expect(parsed.mode).toBe('sql');
		expect(parsed.expr).toBe('"salary" > 50000');
	});
});
