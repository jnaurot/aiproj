import { describe, expect, it } from 'vitest';

import { defaultFilterRules, normalizeFilterParams } from './filterRulesModel';

describe('filterRulesModel', () => {
	it('defaults to rules mode for empty params', () => {
		const normalized = normalizeFilterParams({});
		expect(normalized.mode).toBe('rules');
		expect(normalized.rules).toEqual(defaultFilterRules());
	});

	it('uses sql mode for legacy expr-only payload', () => {
		const normalized = normalizeFilterParams({ expr: '"value" > 10' });
		expect(normalized.mode).toBe('sql');
		expect(normalized.expr).toBe('"value" > 10');
	});

	it('keeps nested groups and conditions', () => {
		const normalized = normalizeFilterParams({
			mode: 'rules',
			rules: {
				kind: 'group',
				op: 'all',
				conditions: [
					{
						kind: 'condition',
						column: 'salary',
						op: 'gte',
						valueSource: 'literal',
						literalValue: '50000'
					},
					{
						kind: 'group',
						op: 'any',
						conditions: [
							{
								kind: 'condition',
								column: 'job_type',
								op: 'eq',
								valueSource: 'param_config',
								paramPath: 'preferences.job_type'
							}
						]
					}
				]
			}
		});
		expect(normalized.mode).toBe('rules');
		expect(normalized.rules.conditions.length).toBe(2);
		expect(normalized.rules.conditions[1].kind).toBe('group');
	});
});
