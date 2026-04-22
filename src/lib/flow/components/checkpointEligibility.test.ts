import { describe, expect, it } from 'vitest';

import { computeCheckpointEligibility } from './checkpointEligibility';

describe('checkpoint eligibility', () => {
	it('allows save only when succeeded + artifact + valid memo key', () => {
		const result = computeCheckpointEligibility({
			status: 'succeeded_up_to_date',
			current: {
				artifactId: 'art-1',
				execKey: 'exec-1'
			},
			memoState: {
				decision: 'reuse',
				memoKey: 'a'.repeat(64)
			}
		} as any);
		expect(result.canSave).toBe(true);
		expect(result.reason).toBeNull();
	});

	it('rejects when memo key is missing even if lineage execKey looks valid', () => {
		const result = computeCheckpointEligibility({
			status: 'succeeded_up_to_date',
			current: {
				artifactId: 'art-1',
				execKey: 'b'.repeat(64)
			},
			memoState: undefined
		} as any);
		expect(result.canSave).toBe(false);
		expect(String(result.reason ?? '')).toContain('memo');
	});

	it('rejects when node is not succeeded', () => {
		const result = computeCheckpointEligibility({
			status: 'running',
			current: {
				artifactId: 'art-1',
				execKey: 'exec-1'
			},
			memoState: {
				decision: 'compute',
				memoKey: 'c'.repeat(64)
			}
		} as any);
		expect(result.canSave).toBe(false);
		expect(String(result.reason ?? '')).toContain('succeeds');
	});

	it('rejects when artifact lineage is absent', () => {
		const result = computeCheckpointEligibility({
			status: 'succeeded_up_to_date',
			current: {
				artifactId: '',
				execKey: ''
			},
			memoState: {
				decision: 'compute',
				memoKey: 'd'.repeat(64)
			}
		} as any);
		expect(result.canSave).toBe(false);
		expect(String(result.reason ?? '')).toContain('artifact');
	});
});
