import { describe, expect, it } from 'vitest';
import { defaultDeriveRules, normalizeDeriveParams, validateRule } from './deriveRulesModel';

describe('deriveRulesModel', () => {
	it('defaults to rules mode for empty params', () => {
		const normalized = normalizeDeriveParams({});
		expect(normalized.mode).toBe('rules');
		expect(normalized.rules).toEqual(defaultDeriveRules());
	});

	it('uses sql mode for legacy sql columns payload', () => {
		const normalized = normalizeDeriveParams({
			columns: [{ name: 'score', expr: '"salary" * 2' }]
		});
		expect(normalized.mode).toBe('sql');
		expect(normalized.columns).toEqual([{ name: 'score', expr: '"salary" * 2' }]);
	});

	it('normalizes rules args for column and param_config sources', () => {
		const normalized = normalizeDeriveParams({
			mode: 'rules',
			rules: [
				{
					name: 'score',
					formula: {
						op: 'add',
						args: [{ column: 'salary' }, { valueFrom: { handle: 'param_config', path: 'prefs.bonus' } }]
					}
				}
			]
		});
		expect(normalized.rules[0]).toEqual({
			name: 'score',
			op: 'add',
			args: [
				{ source: 'column', column: 'salary' },
				{ source: 'param_config', paramPath: 'prefs.bonus' }
			]
		});
	});

	it('returns validation hints for arity and missing values', () => {
		const warnings = validateRule({
			name: '',
			op: 'length',
			args: [{ source: 'column', column: '' }]
		});
		expect(warnings.some((entry) => entry.includes('Output column name is required'))).toBe(true);
		expect(warnings.some((entry) => entry.includes('arg 1: choose a column'))).toBe(true);
	});
});

