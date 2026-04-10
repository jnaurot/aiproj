import { describe, expect, it } from 'vitest';
import { render } from 'svelte/server';

import MemoIndicator from './MemoIndicator.svelte';

describe('MemoIndicator', () => {
	it('shows cached for reuse', () => {
		const out = render(MemoIndicator as any, {
			props: { memoState: { decision: 'reuse' } }
		});
		expect(out.body).toContain('cached');
		expect(out.body).not.toContain('computed');
	});

	it('shows computed for compute', () => {
		const out = render(MemoIndicator as any, {
			props: { memoState: { decision: 'compute' } }
		});
		expect(out.body).toContain('computed');
		expect(out.body).not.toContain('cached');
	});

	it('is hidden when memoState is undefined', () => {
		const out = render(MemoIndicator as any, {
			props: { memoState: undefined }
		});
		expect(out.body).not.toContain('cached');
		expect(out.body).not.toContain('computed');
		expect(out.body).not.toContain('memoBadge');
	});
});
