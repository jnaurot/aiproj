import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { __applyRunEventForTest, graphStore } from './graphStore';
import { displayStatusFromBinding } from './runScope';

describe('graphStore soft-fail skip UI state projection', () => {
	it('maps node_finished(stale) to succeeded when preceded by soft-fail skip log', () => {
		graphStore.hardResetGraph();
		const modelId = graphStore.addNode('model', { x: 0, y: 0 });
		let next = get(graphStore as any);

		next = __applyRunEventForTest(
			next as any,
			{
				type: 'run_started',
				runId: 'run_soft_skip',
				at: '2026-03-24T12:00:00.000Z',
				runFrom: null,
				runMode: 'from_start',
				plannedNodeIds: [modelId]
			} as any,
			'run_soft_skip'
		);
		next = __applyRunEventForTest(
			next as any,
			{
				type: 'node_started',
				runId: 'run_soft_skip',
				at: '2026-03-24T12:00:01.000Z',
				nodeId: modelId
			} as any,
			'run_soft_skip'
		);
		next = __applyRunEventForTest(
			next as any,
			{
				type: 'log',
				runId: 'run_soft_skip',
				at: '2026-03-24T12:00:02.000Z',
				level: 'warn',
				nodeId: modelId,
				message:
					`[scheduler] soft-fail skip node=${modelId} on_error=skip_failed errorCode=UNKNOWN items=1`
			} as any,
			'run_soft_skip'
		);
		next = __applyRunEventForTest(
			next as any,
			{
				type: 'node_finished',
				runId: 'run_soft_skip',
				at: '2026-03-24T12:00:03.000Z',
				nodeId: modelId,
				status: 'stale',
				error: 'one item failed'
			} as any,
			'run_soft_skip'
		);

		const binding = (next as any)?.nodeBindings?.[modelId];
		expect(String(binding?.status ?? '')).toBe('succeeded_up_to_date');
		expect(Boolean(binding?.isUpToDate)).toBe(true);
		expect(displayStatusFromBinding(binding as any)).toBe('succeeded');
		expect((next as any)?.nodeOutputs?.[modelId]?.lastError ?? null).toBeNull();
	});

	it('forces active edges to terminal state on run_finished', () => {
		graphStore.hardResetGraph();
		const srcId = graphStore.addNode('source', { x: 0, y: 0 });
		const dstId = graphStore.addNode('model', { x: 220, y: 0 });
		const addRes = graphStore.addEdge({
			id: 'e_active_terminal',
			source: srcId,
			target: dstId,
			sourceHandle: 'out',
			targetHandle: 'in',
			data: { exec: 'idle', mode: 'work' }
		} as any);
		expect(addRes.ok).toBe(true);

		let next = get(graphStore as any);
		next = __applyRunEventForTest(
			next as any,
			{
				type: 'edge_exec',
				runId: 'run_edge_terminal',
				at: '2026-03-24T12:10:00.000Z',
				edgeId: 'e_active_terminal',
				exec: 'active'
			} as any,
			'run_edge_terminal'
		);
		next = __applyRunEventForTest(
			next as any,
			{
				type: 'run_finished',
				runId: 'run_edge_terminal',
				at: '2026-03-24T12:10:01.000Z',
				status: 'succeeded'
			} as any,
			'run_edge_terminal'
		);
		const doneEdge = ((next as any)?.edges ?? []).find(
			(edge: any) => String(edge?.id ?? '') === 'e_active_terminal'
		);
		expect(String(doneEdge?.data?.exec ?? '')).toBe('done');

		next = __applyRunEventForTest(
			next as any,
			{
				type: 'edge_exec',
				runId: 'run_edge_terminal_2',
				at: '2026-03-24T12:11:00.000Z',
				edgeId: 'e_active_terminal',
				exec: 'active'
			} as any,
			'run_edge_terminal_2'
		);
		next = __applyRunEventForTest(
			next as any,
			{
				type: 'run_finished',
				runId: 'run_edge_terminal_2',
				at: '2026-03-24T12:11:01.000Z',
				status: 'failed'
			} as any,
			'run_edge_terminal_2'
		);
		const idleEdge = ((next as any)?.edges ?? []).find(
			(edge: any) => String(edge?.id ?? '') === 'e_active_terminal'
		);
		expect(String(idleEdge?.data?.exec ?? '')).toBe('idle');
	});
});

