import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { graphStore } from './graphStore';

describe('graphStore handle policy persistence', () => {
	it('persists read_once at node and handle scope', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('tool', { x: 0, y: 0 });
		const baseResult = graphStore.updateNodeProcessingPolicy(nodeId, {
			consume_mode: 'batch',
			batch_size: 4,
			max_inflight: 2,
			read_once: true
		} as any);
		expect(baseResult.ok).toBe(true);
		const handleResult = graphStore.updateNodeInputHandleProcessingPolicy(nodeId, 'in', {
			consume_mode: 'single_item',
			batch_size: 1,
			max_inflight: 1,
			read_once: false
		} as any);
		expect(handleResult.ok).toBe(true);
		const state = get(graphStore as any);
		const node = state.nodes.find((candidate: any) => candidate.id === nodeId);
		const policy = (node?.data as any)?.processingPolicy ?? {};
		expect(Boolean(policy.read_once)).toBe(true);
		expect(Boolean(policy?.input_handles?.in?.read_once)).toBe(false);
		expect(String(policy?.input_handles?.in?.consume_mode ?? '')).toBe('single_item');
	});
});

