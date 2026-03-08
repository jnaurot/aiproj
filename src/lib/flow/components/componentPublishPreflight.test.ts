import { describe, expect, it } from 'vitest';

import { summarizeComponentPreflight, summarizeComponentPublishFailure } from './componentPublishPreflight';

describe('component publish preflight summary', () => {
	it('blocks publish when errors are present', () => {
		const summary = summarizeComponentPreflight(
			false,
			[
				{
					code: 'INVALID_API',
					path: 'api.outputs',
					message: 'api.outputs must be an array',
					severity: 'error'
				}
			],
			'Reader',
			'crev_1'
		);
		expect(summary.ok).toBe(false);
		expect(summary.errorCount).toBe(1);
		expect(summary.headline).toContain('blocked');
		expect(summary.detail).toContain('INVALID_API');
	});

	it('returns warning summary without blocking', () => {
		const summary = summarizeComponentPreflight(
			true,
			[
				{
					code: 'WARN_TYPED_SCHEMA',
					path: 'api.outputs[0].typedSchema',
					message: 'typedSchema is permissive',
					severity: 'warning'
				}
			],
			'Reader',
			'crev_2'
		);
		expect(summary.ok).toBe(true);
		expect(summary.errorCount).toBe(0);
		expect(summary.warningCount).toBe(1);
		expect(summary.headline).toContain('warnings');
	});

	it('extracts backend diagnostics from publish failure payload', () => {
		const err = new Error(
			'createComponentRevision failed: 422 {"detail":{"code":"COMPONENT_VALIDATION_FAILED","message":"Component definition failed preflight validation","diagnostics":[{"code":"INVALID_API","path":"api.outputs","message":"api.outputs must be an array","severity":"error"}]}}'
		);
		const summary = summarizeComponentPublishFailure(err, 'Reader', 'crev_3');
		expect(summary.ok).toBe(false);
		expect(summary.headline).toContain('blocked');
		expect(summary.detail).toContain('INVALID_API');
	});
});
