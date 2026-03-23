import { describe, expect, it } from 'vitest';

import { graphStore } from './graphStore';

describe('graphStore processing policy', () => {
	it('updates node processing policy', () => {
		graphStore.hardResetGraph();
		const nid = graphStore.addNode('tool', { x: 0, y: 0 });
		const res = graphStore.updateNodeProcessingPolicy(nid, {
			consume_mode: 'batch',
			batch_size: 4,
			max_inflight: 2
		});
		expect(res.ok).toBe(true);
	});
});
