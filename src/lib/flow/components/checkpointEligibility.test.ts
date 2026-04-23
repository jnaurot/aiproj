import { describe, expect, it } from 'vitest';

import { computeCheckpointEligibility } from './checkpointEligibility';

describe('checkpoint eligibility', () => {
	it('allows save when lineage + memo fingerprint are valid', () => {
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

	it('allows save when node is idle but has valid artifact lineage + memo key', () => {
		const result = computeCheckpointEligibility({
			status: 'idle',
			isUpToDate: true,
			current: {
				artifactId: 'art-idle',
				execKey: 'exec-idle'
			},
			memoState: {
				decision: 'reuse',
				memoKey: 'e'.repeat(64)
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

	it('rejects while node is actively executing', () => {
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
		expect(String(result.reason ?? '')).toContain('execut');
	});

	it('rejects stale lineage even with artifact + memo key present', () => {
		// Under the three-state model staleness is driven by runtime === 'stale'
		// or exec-key drift, not by isUpToDate: false.
		const result = computeCheckpointEligibility({
			status: 'stale',
			current: {
				artifactId: 'art-1',
				execKey: 'exec-1'
			},
			memoState: {
				decision: 'compute',
				memoKey: 'f'.repeat(64)
			}
		} as any);
		expect(result.canSave).toBe(false);
		expect(String(result.reason ?? '')).toContain('non-stale');
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
