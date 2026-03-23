import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { __applyRunEventForTest, graphStore } from './graphStore';

describe('graphStore handle state timeline projection', () => {
	it('tracks per-handle control states from control_signal events', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('tool', { x: 0, y: 0 });
		const base = get(graphStore as any);
		let next = __applyRunEventForTest(
			base as any,
			{
				type: 'control_signal',
				runId: 'run_handle_state',
				at: '2026-03-23T10:00:00.000Z',
				nodeId,
				handle: 'in',
				signal: 'busy'
			} as any,
			'run_handle_state'
		);
		next = __applyRunEventForTest(
			next as any,
			{
				type: 'control_signal',
				runId: 'run_handle_state',
				at: '2026-03-23T10:00:01.000Z',
				nodeId,
				handle: 'in',
				signal: 'ready'
			} as any,
			'run_handle_state'
		);
		const key = `${nodeId}:in`;
		expect((next as any)?.queueRuntime?.handleStates?.[key]?.state).toBe('ready');
		const timeline = ((next as any)?.queueRuntime?.handleTimeline ?? []) as any[];
		expect(timeline.length).toBeGreaterThanOrEqual(2);
		expect(timeline[timeline.length - 1]?.handle).toBe('in');
		expect(timeline[timeline.length - 1]?.signal).toBe('ready');
	});
});
