import { describe, expect, it } from 'vitest';
import { TransformParamsSchemaByKind } from '$lib/flow/schema/transform';

describe('quality gate form validation', () => {
	it('accepts a valid mixed quality gate config', () => {
		const parsed = TransformParamsSchemaByKind.quality_gate.safeParse({
			stopOnFail: true,
			checks: [
				{ kind: 'null_pct', column: 'text', maxNullPct: 0.05, severity: 'fail' },
				{ kind: 'range', column: 'score', min: 0, max: 1, maxOutOfRangePct: 0.02, severity: 'warn' },
				{ kind: 'leakage', featureColumn: 'feature_a', targetColumn: 'target', maxAbsCorrelation: 0.9, severity: 'warn' },
			],
		});
		expect(parsed.success).toBe(true);
	});

	it('rejects range checks that define neither min nor max', () => {
		const parsed = TransformParamsSchemaByKind.quality_gate.safeParse({
			stopOnFail: true,
			checks: [{ kind: 'range', column: 'score', severity: 'fail' }],
		});
		expect(parsed.success).toBe(false);
	});

	it('rejects invalid ratio values', () => {
		const parsed = TransformParamsSchemaByKind.quality_gate.safeParse({
			stopOnFail: true,
			checks: [{ kind: 'uniqueness', column: 'id', minUniqueRatio: 1.5, severity: 'fail' }],
		});
		expect(parsed.success).toBe(false);
	});
});
