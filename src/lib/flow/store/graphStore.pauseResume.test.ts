import { describe, expect, it } from 'vitest';

import type { GraphState } from './graphStore';
import { __applyRunEventForTest } from './graphStore';
import type { KnownRunEvent } from '$lib/flow/types/run';

function makeState(runStatus: GraphState['runStatus'] = 'running'): GraphState {
	return {
		graphId: 'graph-pause',
		nodes: [{ id: 'n1', data: { kind: 'model', meta: { llmAllocated: true } } }] as any,
		edges: [
			{
				id: 'e_work',
				source: 'src',
				sourceHandle: 'out',
				target: 'n1',
				targetHandle: 'in',
				data: { mode: 'work', exec: 'active' }
			},
			{
				id: 'e_param',
				source: 'cfg',
				sourceHandle: 'out',
				target: 'n1',
				targetHandle: 'param_config',
				data: { mode: 'param', exec: 'idle' }
			}
		] as any,
		selectedNodeId: null,
		inspector: { nodeId: null, draftParams: {}, dirty: false } as any,
		logs: [],
		runStatus,
		lastRunStatus: 'succeeded',
		freshness: 'up_to_date',
		staleNodeCount: 0,
		activeRunMode: 'from_start',
		activeRunFrom: null,
		activeRunNodeSet: new Set<string>(['n1']),
		nodeOutputs: {},
		nodeBindings: {},
		activeRunId: 'run-pause'
	};
}

describe('graphStore pause/resume lifecycle', () => {
	it('handles pausing -> paused -> resuming -> running transitions', () => {
		let state = makeState('running');
		state = __applyRunEventForTest(
			state,
			{ type: 'run_pausing', runId: 'run-pause', at: '2026-03-30T00:00:00Z' } as any,
			'run-pause'
		);
		expect(state.runStatus).toBe('pausing');
		state = __applyRunEventForTest(
			state,
			{ type: 'run_paused', runId: 'run-pause', at: '2026-03-30T00:00:01Z' } as any,
			'run-pause'
		);
		expect(state.runStatus).toBe('paused');
		state = __applyRunEventForTest(
			state,
			{ type: 'run_resuming', runId: 'run-pause', at: '2026-03-30T00:00:02Z' } as any,
			'run-pause'
		);
		expect(state.runStatus).toBe('resuming');
		state = __applyRunEventForTest(
			state,
			{
				type: 'run_resumed',
				runId: 'run-pause',
				at: '2026-03-30T00:00:03Z',
				runMode: 'from_selected_onward',
				runFrom: 'n1',
				plannedNodeIds: ['n1']
			} as any,
			'run-pause'
		);
		expect(state.runStatus).toBe('running');
	});

	it('paused state clears running visuals and llm ownership', () => {
		const state = makeState('running');
		const next = __applyRunEventForTest(
			state,
			{ type: 'run_paused', runId: 'run-pause', at: '2026-03-30T00:00:01Z' } as any,
			'run-pause'
		);
		const workEdge = (next.edges as any[]).find((e) => String(e.id) === 'e_work');
		const paramEdge = (next.edges as any[]).find((e) => String(e.id) === 'e_param');
		expect(next.runStatus).toBe('paused');
		expect(String(workEdge?.data?.exec ?? '')).toBe('idle');
		expect(String(paramEdge?.data?.exec ?? '')).toBe('idle');
		expect(Boolean((((next.nodes as any[])[0]?.data ?? {})?.meta ?? {}).llmAllocated)).toBe(false);
	});

	it('run_resume_failed leaves run in paused state', () => {
		const state = makeState('resuming');
		const next = __applyRunEventForTest(
			state,
			{
				type: 'run_resume_failed',
				runId: 'run-pause',
				at: '2026-03-30T00:00:04Z',
				errorCode: 'RESUME_FRONTIER_VALIDATION_FAILED'
			} as any,
			'run-pause'
		);
		expect(next.runStatus).toBe('paused');
	});

	it('non-work edge_exec active remains ignored during pause lifecycle', () => {
		let state = makeState('running');
		state = __applyRunEventForTest(
			state,
			{ type: 'run_pausing', runId: 'run-pause', at: '2026-03-30T00:00:05Z' } as any,
			'run-pause'
		);
		const next = __applyRunEventForTest(
			state,
			{ type: 'edge_exec', runId: 'run-pause', at: '2026-03-30T00:00:06Z', edgeId: 'e_param', exec: 'active' } as KnownRunEvent,
			'run-pause'
		);
		const paramEdge = (next.edges as any[]).find((e) => String(e.id) === 'e_param');
		expect(String(paramEdge?.data?.exec ?? '')).toBe('idle');
	});

	it('test_paused_has_no_running_nodes', () => {
		const state = makeState('running');
		const next = __applyRunEventForTest(
			state,
			{ type: 'run_paused', runId: 'run-pause', at: '2026-03-30T00:00:01Z' } as any,
			'run-pause'
		);
		expect(next.runStatus).toBe('paused');
		expect(Boolean((((next.nodes as any[])[0]?.data ?? {})?.meta ?? {}).llmAllocated)).toBe(false);
	});

	it('test_paused_has_no_active_edges', () => {
		const state = makeState('running');
		const next = __applyRunEventForTest(
			state,
			{ type: 'run_paused', runId: 'run-pause', at: '2026-03-30T00:00:01Z' } as any,
			'run-pause'
		);
		for (const edge of next.edges as any[]) {
			expect(String((edge?.data ?? {}).exec ?? 'idle')).not.toBe('active');
		}
	});

	it('test_paused_has_no_running_star', () => {
		const state = makeState('running');
		const next = __applyRunEventForTest(
			state,
			{ type: 'run_paused', runId: 'run-pause', at: '2026-03-30T00:00:01Z' } as any,
			'run-pause'
		);
		expect(Boolean((((next.nodes as any[])[0]?.data ?? {})?.meta ?? {}).llmAllocated)).toBe(false);
	});

	it('test_pausing_allows_only_existing_work', () => {
		let state = makeState('running');
		state = __applyRunEventForTest(
			state,
			{ type: 'run_pausing', runId: 'run-pause', at: '2026-03-30T00:00:05Z' } as any,
			'run-pause'
		);
		const next = __applyRunEventForTest(
			state,
			{ type: 'edge_exec', runId: 'run-pause', at: '2026-03-30T00:00:06Z', edgeId: 'e_work', exec: 'active' } as any,
			'run-pause'
		);
		expect(next.runStatus).toBe('pausing');
		const workEdge = (next.edges as any[]).find((e) => String(e.id) === 'e_work');
		expect(String(workEdge?.data?.exec ?? '')).toBe('active');
	});

	it('test_resuming_has_no_speculative_running', () => {
		let state = makeState('running');
		state = __applyRunEventForTest(
			state,
			{ type: 'run_paused', runId: 'run-pause', at: '2026-03-30T00:00:01Z' } as any,
			'run-pause'
		);
		const next = __applyRunEventForTest(
			state,
			{ type: 'run_resuming', runId: 'run-pause', at: '2026-03-30T00:00:02Z' } as any,
			'run-pause'
		);
		expect(next.runStatus).toBe('resuming');
		for (const edge of next.edges as any[]) {
			expect(String((edge?.data ?? {}).exec ?? 'idle')).not.toBe('active');
		}
		expect(Boolean((((next.nodes as any[])[0]?.data ?? {})?.meta ?? {}).llmAllocated)).toBe(false);
	});
});
