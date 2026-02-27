import { describe, expect, it } from 'vitest';

import { __assertBindingPairForTest, __normalizeBindingForTest } from './graphStore';

describe('graphStore binding normalization', () => {
	it('normalized binding always has required fields', () => {
		const b = __normalizeBindingForTest(undefined, 'n_test');
		expect(b.status).toBeDefined();
		expect(b.current).toEqual({ execKey: null, artifactId: null });
		expect(b.last).toEqual({ execKey: null, artifactId: null });
		expect(b.isUpToDate).toBe(false);
		expect(b.cacheValid).toBe(false);
		expect(b.currentRunId).toBeNull();
		expect(b.staleReason).toBeNull();
	});

	it('binding pair invariant rejects partial pairs', () => {
		expect(() =>
			__assertBindingPairForTest({ current: { execKey: 'ek', artifactId: null } }, 'n_bad', 'test')
		).toThrow(/INVALID_BINDING_PAIR/);
	});
});
