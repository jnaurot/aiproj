import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { graphStore } from './graphStore';

describe('graphStore per-handle processing policy', () => {
	it('stores input handle overrides under processingPolicy.input_handles', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('transform', { x: 0, y: 0 });
		const updated = graphStore.updateNodeInputHandleProcessingPolicy(nodeId, 'in', {
			consume_mode: 'batch',
			batch_size: 3,
			max_inflight: 2
		});
		expect(updated.ok).toBe(true);
		const state = get(graphStore as any);
		const node = state.nodes.find((n: any) => n.id === nodeId);
		expect(node?.data?.processingPolicy?.input_handles?.in?.consume_mode).toBe('batch');
		expect(node?.data?.processingPolicy?.input_handles?.in?.batch_size).toBe(3);
		expect(node?.data?.processingPolicy?.input_handles?.in?.max_inflight).toBe(2);
	});
});
