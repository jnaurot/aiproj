import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { graphStore } from './graphStore';

describe('graphStore handle policy persistence', () => {
	it('derives read_once from consume mode at node and handle scope', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('tool', { x: 0, y: 0 });
		const baseResult = graphStore.updateNodeProcessingPolicy(nodeId, {
			consume_mode: 'once',
			batch_size: 4,
			max_inflight: 2
		} as any);
		expect(baseResult.ok).toBe(true);
		const handleResult = graphStore.updateNodeInputHandleProcessingPolicy(nodeId, 'in', {
			consume_mode: 'single_item',
			batch_size: 1,
			max_inflight: 1
		} as any);
		expect(handleResult.ok).toBe(true);
		const state = get(graphStore as any);
		const node = state.nodes.find((candidate: any) => candidate.id === nodeId);
		const policy = (node?.data as any)?.processingPolicy ?? {};
		expect(Boolean(policy.read_once)).toBe(true);
		expect(Boolean(policy?.input_handles?.in?.read_once)).toBe(false);
		expect(String(policy?.input_handles?.in?.consume_mode ?? '')).toBe('single_item');

		const nodeBatch = graphStore.updateNodeProcessingPolicy(nodeId, { consume_mode: 'batch' } as any);
		expect(nodeBatch.ok).toBe(true);
		const stateAfterBatch = get(graphStore as any);
		const nodeAfterBatch = stateAfterBatch.nodes.find((candidate: any) => candidate.id === nodeId);
		const policyAfterBatch = (nodeAfterBatch?.data as any)?.processingPolicy ?? {};
		expect(Boolean(policyAfterBatch.read_once)).toBe(false);
	});
});

