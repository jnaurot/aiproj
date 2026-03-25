import { describe, expect, it } from 'vitest';

import { TransformJsonFilterParamsSchema } from './transform';
import { defaultTransformJsonFilterParams } from './transformDefaults';

describe('TransformJsonFilterParamsSchema', () => {
	it('accepts default params', () => {
		const parsed = TransformJsonFilterParamsSchema.safeParse(defaultTransformJsonFilterParams);
		expect(parsed.success).toBe(true);
	});

	it('supports nested rule groups', () => {
		const parsed = TransformJsonFilterParamsSchema.safeParse({
			mode: 'rules',
			rules: {
				kind: 'group',
				op: 'all',
				conditions: [
					{
						kind: 'condition',
						path: 'pass',
						op: 'eq',
						value: true
					},
					{
						kind: 'group',
						op: 'any',
						conditions: [
							{
								kind: 'condition',
								path: 'score',
								op: 'gte',
								value: 70
							}
						]
					}
				]
			}
		});
		expect(parsed.success).toBe(true);
	});

	it('rejects conditions without a path', () => {
		const parsed = TransformJsonFilterParamsSchema.safeParse({
			mode: 'rules',
			rules: {
				kind: 'group',
				op: 'all',
				conditions: [
					{
						kind: 'condition',
						op: 'eq',
						value: true
					}
				]
			}
		});
		expect(parsed.success).toBe(false);
	});
});
