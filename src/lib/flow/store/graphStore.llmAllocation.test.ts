import { describe, expect, it } from 'vitest';

import type { GraphState } from './graphStore';
import { __applyRunEventForTest } from './graphStore';
import type { KnownRunEvent } from '$lib/flow/types/run';

function makeState(runStatus: GraphState['runStatus'] = 'running'): GraphState {
	return {
		graphId: 'graph-llm',
		nodes: [
			{ id: 'a', data: { kind: 'model', meta: {} } },
			{ id: 'b', data: { kind: 'model', meta: { llmAllocated: true } } }
		] as any,
		edges: [] as any,
		selectedNodeId: null,
		inspector: { nodeId: null, draftParams: {}, dirty: false } as any,
		logs: [],
		runStatus,
		lastRunStatus: 'succeeded',
		freshness: 'up_to_date',
		staleNodeCount: 0,
		activeRunMode: 'from_start',
		activeRunFrom: null,
		activeRunNodeSet: new Set<string>(),
		nodeOutputs: {},
		nodeBindings: {},
		activeRunId: 'run-llm'
	};
}

function allocated(state: GraphState): string[] {
	return (state.nodes ?? [])
		.filter((n) => Boolean(((n.data as any)?.meta ?? {}).llmAllocated))
		.map((n) => String(n.id));
}

describe('graphStore llm allocation UI state', () => {
	it('does not mutate stars from control llm signals (lease is source of truth)', () => {
		const state = makeState('running');
		const evt: KnownRunEvent = {
			type: 'control_signal',
			runId: 'run-llm',
			at: '2026-03-29T00:00:00Z',
			nodeId: 'a',
			signal: 'llm_acquired'
		} as any;
		const next = __applyRunEventForTest(state, evt, 'run-llm');
		expect(allocated(next)).toEqual(['b']);
	});

	it('tracks exactly one star from llm_lease acquired holder', () => {
		const state = makeState('running');
		const evt: KnownRunEvent = {
			type: 'llm_lease',
			runId: 'run-llm',
			at: '2026-03-29T00:00:01Z',
			state: 'acquired',
			nodeId: 'a',
			holderNodeId: 'a',
			waitQueueLength: 0
		} as any;
		const next = __applyRunEventForTest(state, evt, 'run-llm');
		expect(allocated(next)).toEqual(['a']);
	});

	it('clears stars on llm_lease released', () => {
		const state = makeState('running');
		const evt: KnownRunEvent = {
			type: 'llm_lease',
			runId: 'run-llm',
			at: '2026-03-29T00:00:02Z',
			state: 'released',
			nodeId: 'a',
			holderNodeId: null,
			waitQueueLength: 0
		} as any;
		const next = __applyRunEventForTest(state, evt, 'run-llm');
		expect(allocated(next)).toEqual([]);
	});

	it('does not show star when run is not running even if late acquired lease arrives', () => {
		const state = makeState('stale');
		const evt: KnownRunEvent = {
			type: 'llm_lease',
			runId: 'run-llm',
			at: '2026-03-29T00:00:03Z',
			state: 'acquired',
			nodeId: 'a',
			holderNodeId: 'a',
			waitQueueLength: 0
		} as any;
		const next = __applyRunEventForTest(state, evt, 'run-llm');
		expect(allocated(next)).toEqual([]);
	});

	it('clears stale stars on run_started', () => {
		const state = makeState('running');
		const evt: KnownRunEvent = {
			type: 'run_started',
			runId: 'run-next',
			at: '2026-03-29T00:00:04Z',
			runFrom: null,
			runMode: 'from_start',
			plannedNodeIds: ['a', 'b']
		} as any;
		const next = __applyRunEventForTest(state, evt, 'run-next');
		expect(allocated(next)).toEqual([]);
	});

	it('marks node busy and clears active work-edge visuals on llm_released', () => {
		const state: GraphState = {
			...makeState('running'),
			edges: [
				{ id: 'e_work', source: 'src', sourceHandle: 'out', target: 'a', targetHandle: 'in', data: { mode: 'work', exec: 'active' } },
				{
					id: 'e_param',
					source: 'cfg',
					sourceHandle: 'out',
					target: 'a',
					targetHandle: 'param_config',
					data: { mode: 'param', exec: 'active' }
				}
			] as any,
			nodeBindings: {
				a: {
					status: 'running',
					isUpToDate: false,
					cacheValid: false,
					currentRunId: 'run-llm',
					current: { execKey: null, artifactId: null },
					last: { execKey: null, artifactId: null },
					staleReason: null
				}
			} as any
		};
		const evt: KnownRunEvent = {
			type: 'control_signal',
			runId: 'run-llm',
			at: '2026-03-29T00:00:06Z',
			nodeId: 'a',
			signal: 'llm_released'
		} as any;
		const next = __applyRunEventForTest(state, evt, 'run-llm');
		expect(String((next as any)?.nodeBindings?.a?.status ?? '')).toBe('busy');
		const workEdge = (next.edges as any[]).find((e) => String(e?.id) === 'e_work');
		const paramEdge = (next.edges as any[]).find((e) => String(e?.id) === 'e_param');
		expect(String(workEdge?.data?.exec ?? '')).toBe('done');
		expect(String(paramEdge?.data?.exec ?? '')).toBe('active');
	});

	it('llm_released edge cleanup is scoped to the released node only', () => {
		const state: GraphState = {
			...makeState('running'),
			edges: [
				{ id: 'e_a_work', source: 'src_a', sourceHandle: 'out', target: 'a', targetHandle: 'in', data: { mode: 'work', exec: 'active' } },
				{ id: 'e_b_work', source: 'src_b', sourceHandle: 'out', target: 'b', targetHandle: 'in', data: { mode: 'work', exec: 'active' } }
			] as any,
			nodeBindings: {
				a: {
					status: 'running',
					isUpToDate: false,
					cacheValid: false,
					currentRunId: 'run-llm',
					current: { execKey: null, artifactId: null },
					last: { execKey: null, artifactId: null },
					staleReason: null
				},
				b: {
					status: 'running',
					isUpToDate: false,
					cacheValid: false,
					currentRunId: 'run-llm',
					current: { execKey: null, artifactId: null },
					last: { execKey: null, artifactId: null },
					staleReason: null
				}
			} as any
		};
		const next = __applyRunEventForTest(
			state,
			{ type: 'control_signal', runId: 'run-llm', at: '2026-03-29T00:00:06Z', nodeId: 'a', signal: 'llm_released' } as any,
			'run-llm'
		);
		const edgeA = (next.edges as any[]).find((e) => String(e?.id) === 'e_a_work');
		const edgeB = (next.edges as any[]).find((e) => String(e?.id) === 'e_b_work');
		expect(String(edgeA?.data?.exec ?? '')).toBe('done');
		expect(String(edgeB?.data?.exec ?? '')).toBe('active');
	});

	it('ignores edge_exec active for non-work edges', () => {
		const state: GraphState = {
			...makeState('running'),
			edges: [
				{ id: 'e_param', source: 'cfg', target: 'a', data: { mode: 'param', exec: 'idle' } },
				{ id: 'e_ctrl', source: 'ctl', target: 'a', data: { mode: 'control', exec: 'idle' } }
			] as any
		};
		const activeParam = __applyRunEventForTest(
			state,
			{ type: 'edge_exec', runId: 'run-llm', at: '2026-03-29T00:00:07Z', edgeId: 'e_param', exec: 'active' } as any,
			'run-llm'
		);
		const activeCtrl = __applyRunEventForTest(
			activeParam as any,
			{ type: 'edge_exec', runId: 'run-llm', at: '2026-03-29T00:00:08Z', edgeId: 'e_ctrl', exec: 'active' } as any,
			'run-llm'
		);
		expect(String(((activeCtrl.edges as any[]).find((e) => e.id === 'e_param') as any)?.data?.exec ?? '')).toBe('idle');
		expect(String(((activeCtrl.edges as any[]).find((e) => e.id === 'e_ctrl') as any)?.data?.exec ?? '')).toBe('idle');
	});

	it('release/finish race never regresses succeeded node back to busy', () => {
		const state: GraphState = {
			...makeState('running'),
			nodeBindings: {
				a: {
					status: 'running',
					isUpToDate: false,
					cacheValid: false,
					currentRunId: 'run-llm',
					current: { execKey: 'k1', artifactId: 'a1' },
					last: { execKey: null, artifactId: null },
					staleReason: null
				}
			} as any
		};
		const finishedFirst = __applyRunEventForTest(
			state,
			{ type: 'node_finished', runId: 'run-llm', at: '2026-03-29T00:00:10Z', nodeId: 'a', status: 'succeeded' } as any,
			'run-llm'
		);
		const afterLateRelease = __applyRunEventForTest(
			finishedFirst as any,
			{ type: 'control_signal', runId: 'run-llm', at: '2026-03-29T00:00:11Z', nodeId: 'a', signal: 'llm_released' } as any,
			'run-llm'
		);
		expect(String((afterLateRelease as any)?.nodeBindings?.a?.status ?? '')).toBe('succeeded_up_to_date');

		const releaseFirst = __applyRunEventForTest(
			state,
			{ type: 'control_signal', runId: 'run-llm', at: '2026-03-29T00:00:12Z', nodeId: 'a', signal: 'llm_released' } as any,
			'run-llm'
		);
		expect(String((releaseFirst as any)?.nodeBindings?.a?.status ?? '')).toBe('busy');
		const afterFinish = __applyRunEventForTest(
			releaseFirst as any,
			{ type: 'node_finished', runId: 'run-llm', at: '2026-03-29T00:00:13Z', nodeId: 'a', status: 'succeeded' } as any,
			'run-llm'
		);
		expect(String((afterFinish as any)?.nodeBindings?.a?.status ?? '')).toBe('succeeded_up_to_date');
	});
});
