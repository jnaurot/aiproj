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
});
