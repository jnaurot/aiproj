import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { __applyRunEventForTest, graphStore } from './graphStore';

describe('graphStore node_reject event', () => {
	it('logs structured reject signal with plane and reason', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('tool', { x: 0, y: 0 });
		const state = get(graphStore as any);
		const next = __applyRunEventForTest(
			state as any,
			{
				type: 'node_reject',
				runId: 'run_reject_evt',
				at: new Date().toISOString(),
				nodeId,
				plane: 'work',
				reasonCode: 'FILTERED_OUT',
				count: 1
			} as any,
			'run_reject_evt'
		);
		const last = (next as any)?.logs?.[(next as any)?.logs?.length - 1];
		expect(String(last?.message ?? '')).toContain('[reject]');
		expect(String(last?.message ?? '')).toContain('FILTERED_OUT');
	});
});

