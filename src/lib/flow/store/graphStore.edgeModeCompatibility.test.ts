import { describe, expect, it } from 'vitest';

import { graphStore } from './graphStore';

describe('graphStore edge mode compatibility', () => {
	it('blocks work mode edge targeting a param handle', () => {
		graphStore.hardResetGraph();
		const src = graphStore.addNode('source', { x: 0, y: 0 });
		const dst = graphStore.addNode('model', { x: 200, y: 0 });

		const result = graphStore.addEdge({
			id: 'e_mode_block',
			source: src,
			target: dst,
			targetHandle: 'param_filters',
			data: { exec: 'idle', mode: 'work' }
		} as any);
		expect(result.ok).toBe(false);
		expect(String(result.error ?? '')).toContain('Edge mode');
	});

	it('allows param mode edge targeting a param handle', () => {
		graphStore.hardResetGraph();
		const src = graphStore.addNode('source', { x: 0, y: 0 });
		const dst = graphStore.addNode('model', { x: 200, y: 0 });

		const result = graphStore.addEdge({
			id: 'e_mode_ok',
			source: src,
			target: dst,
			targetHandle: 'param_filters',
			data: { exec: 'idle', mode: 'param' }
		} as any);
		expect(result.ok).toBe(true);
	});
});
