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

	it('does not toggle llmAllocated meta from llm control signals (lease events own star projection)', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('model', { x: 0, y: 0 });
		const base = get(graphStore as any);
		let next = __applyRunEventForTest(
			base as any,
			{
				type: 'control_signal',
				runId: 'run_llm_alloc',
				at: '2026-03-29T02:00:00.000Z',
				nodeId,
				signal: 'llm_acquired'
			} as any,
			'run_llm_alloc'
		);
		const acquiredNode = ((next as any)?.nodes ?? []).find((n: any) => String(n?.id) === String(nodeId));
		expect(Boolean(acquiredNode?.data?.meta?.llmAllocated)).toBe(false);
		next = __applyRunEventForTest(
			next as any,
			{
				type: 'control_signal',
				runId: 'run_llm_alloc',
				at: '2026-03-29T02:00:01.000Z',
				nodeId,
				signal: 'llm_released'
			} as any,
			'run_llm_alloc'
		);
		const releasedNode = ((next as any)?.nodes ?? []).find((n: any) => String(n?.id) === String(nodeId));
		expect(Boolean(releasedNode?.data?.meta?.llmAllocated)).toBe(false);
	});

	it('clears llmAllocated marker on run_finished', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('model', { x: 0, y: 0 });
		const base = get(graphStore as any);
		let next = __applyRunEventForTest(
			base as any,
			{
				type: 'control_signal',
				runId: 'run_llm_finish',
				at: '2026-03-29T02:10:00.000Z',
				nodeId,
				signal: 'llm_acquired'
			} as any,
			'run_llm_finish'
		);
		next = __applyRunEventForTest(
			next as any,
			{
				type: 'run_finished',
				runId: 'run_llm_finish',
				at: '2026-03-29T02:10:03.000Z',
				status: 'succeeded'
			} as any,
			'run_llm_finish'
		);
		const finalNode = ((next as any)?.nodes ?? []).find((n: any) => String(n?.id) === String(nodeId));
		expect(Boolean(finalNode?.data?.meta?.llmAllocated)).toBe(false);
	});
});
