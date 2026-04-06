import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';
import { graphStore } from '$lib/flow/store/graphStore';
import { buildNodeDocDependencySignature, createMemoizedNodeDocResolver } from './nodeDocsViewModel';

describe('node docs view model memoization', () => {
	it('invalidates dependency signature when scheduler handle state changes', () => {
		graphStore.hardResetGraph();
		const modelId = graphStore.addNode('model', { x: 0, y: 0 });
		const state1 = get(graphStore as any);
		const signature1 = buildNodeDocDependencySignature(state1 as any, modelId);
		const state2 = structuredClone(state1);
		(state2 as any).queueRuntime = {
			...((state2 as any).queueRuntime ?? {}),
			schedulerSnapshot: {
				perNode: [
					{
						nodeId: modelId,
						readyWork: false,
						inflight: 0,
						pendingInputCount: 2,
						lastBlockedReasonCode: 'WAITING_REQUIRED_INPUT'
					}
				]
			}
		};
		const signature2 = buildNodeDocDependencySignature(state2 as any, modelId);
		expect(signature2).not.toBe(signature1);
	});

	it('returns memoized object identity when dependencies are unchanged', () => {
		graphStore.hardResetGraph();
		const modelId = graphStore.addNode('model', { x: 0, y: 0 });
		const state = get(graphStore as any);
		const resolver = createMemoizedNodeDocResolver();
		const first = resolver(state as any, modelId);
		const second = resolver(state as any, modelId);
		expect(second).toBe(first);
	});
});
