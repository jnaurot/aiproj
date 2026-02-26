import { describe, expect, it } from 'vitest';

import type { GraphState } from './graphStore';
import { __applyRunEventForTest, __hardResetGraphForTest, __hydrateFromRunSnapshotForTest } from './graphStore';
import type { KnownRunEvent } from '$lib/flow/types/run';
import { displayStatusFromBinding } from './runScope';

function makeState(): GraphState {
	return {
		graphId: 'graph-test',
		nodes: [
			{ id: 'src', data: { status: 'succeeded' } },
			{ id: 'xfm', data: { status: 'succeeded' } },
			{ id: 'llm_a', data: { status: 'succeeded' } },
			{ id: 'llm_b', data: { status: 'succeeded' } }
		] as any,
		edges: [
			{ id: 'e1', source: 'src', target: 'xfm', data: { exec: 'idle' } },
			{ id: 'e2', source: 'xfm', target: 'llm_a', data: { exec: 'idle' } },
			{ id: 'e3', source: 'xfm', target: 'llm_b', data: { exec: 'idle' } }
		] as any,
		selectedNodeId: null,
		inspector: { nodeId: null, draftParams: {}, dirty: false },
		logs: [],
		runStatus: 'running',
		lastRunStatus: 'succeeded',
		freshness: 'up_to_date',
		staleNodeCount: 0,
		activeRunMode: 'from_start',
		activeRunFrom: null,
		activeRunNodeSet: new Set<string>(),
		nodeOutputs: {},
		nodeBindings: {
			src: { status: 'succeeded_up_to_date', isUpToDate: true, lastArtifactId: 'art-src' },
			xfm: { status: 'succeeded_up_to_date', isUpToDate: true, lastArtifactId: 'art-xfm' },
			llm_a: { status: 'succeeded_up_to_date', isUpToDate: true, lastArtifactId: 'art-a' },
			llm_b: { status: 'succeeded_up_to_date', isUpToDate: true, lastArtifactId: 'art-b' }
		},
		activeRunId: 'run-1'
	};
}

describe('graphStore partial run scope events', () => {
	it('run_started + scoped cache hits do not stale sibling branch', () => {
		const runId = 'run-1';
		let state = makeState();
		const beforeSibling = { ...state.nodeBindings.llm_a };
		const beforeBindings = JSON.parse(JSON.stringify(state.nodeBindings));

		const runStarted: KnownRunEvent = {
			type: 'run_started',
			runId,
			at: '2026-02-25T00:00:00Z',
			runFrom: 'llm_b',
			runMode: 'from_selected_onward',
			plannedNodeIds: ['src', 'xfm', 'llm_b']
		};
		state = __applyRunEventForTest(state, runStarted, runId);
		expect(state.nodeBindings).toEqual(beforeBindings);
		expect(state.nodeBindings.llm_a).toEqual(beforeSibling);

		const cacheEvents: KnownRunEvent[] = [
			{
				type: 'cache_decision',
				runId,
				at: '2026-02-25T00:00:01Z',
				nodeId: 'src',
				nodeKind: 'source',
				decision: 'cache_hit',
				execKey: 'src-key',
				artifactId: 'art-src-2'
			},
			{
				type: 'cache_decision',
				runId,
				at: '2026-02-25T00:00:02Z',
				nodeId: 'xfm',
				nodeKind: 'transform',
				decision: 'cache_hit',
				execKey: 'xfm-key',
				artifactId: 'art-xfm-2'
			},
			{
				type: 'cache_decision',
				runId,
				at: '2026-02-25T00:00:03Z',
				nodeId: 'llm_b',
				nodeKind: 'llm',
				decision: 'cache_hit',
				execKey: 'llm-b-key',
				artifactId: 'art-b-2'
			}
		];
		for (const evt of cacheEvents) {
			state = __applyRunEventForTest(state, evt, runId);
		}
		state = __applyRunEventForTest(
			state,
			{ type: 'run_finished', runId, at: '2026-02-25T00:00:04Z', status: 'succeeded' },
			runId
		);

		expect(state.nodeBindings.llm_a).toEqual(beforeSibling);
		expect(state.nodeBindings.llm_a.isUpToDate).toBe(true);
		expect(state.nodeBindings.llm_a.status).toBe('succeeded_up_to_date');
	});

	it('partial run hydration and planned-node events keep sibling output/status unchanged', () => {
		const runId = 'run-1';
		let state = makeState();
		const siblingBefore = { ...state.nodeBindings.llm_a };

		state = __applyRunEventForTest(
			state,
			{
				type: 'run_started',
				runId,
				at: '2026-02-25T00:00:00Z',
				runFrom: 'llm_b',
				runMode: 'from_selected_onward',
				plannedNodeIds: ['src', 'xfm', 'llm_b']
			},
			runId
		);
		state = __hydrateFromRunSnapshotForTest(state, {
			status: 'running',
			runMode: 'from_selected_onward',
			plannedNodeIds: ['src', 'xfm', 'llm_b'],
			nodeBindings: {
				src: { status: 'running' },
				xfm: { status: 'running' },
				llm_b: { status: 'running' }
			}
		});
		state = __applyRunEventForTest(
			state,
			{
				type: 'cache_decision',
				runId,
				at: '2026-02-25T00:00:01Z',
				nodeId: 'llm_b',
				nodeKind: 'llm',
				decision: 'cache_hit',
				execKey: 'llm-b-key',
				artifactId: 'art-b-2'
			},
			runId
		);

		expect(state.nodeBindings.llm_a).toEqual(siblingBefore);
		expect(displayStatusFromBinding(state.nodeBindings.llm_a as any)).toBe('succeeded');
		expect(state.nodeBindings.llm_a.lastArtifactId).toBe('art-a');
	});

	it('hydrate snapshot does not touch node card status for nodes absent from snapshot bindings', () => {
		const state = makeState();
		const beforeNodeStatuses = state.nodes.map((n) => ({ id: n.id, status: n.data.status }));
		const beforeBindingCount = Object.keys(state.nodeBindings).length;
		const next = __hydrateFromRunSnapshotForTest(state, {
			status: 'running',
			nodeBindings: {
				llm_b: { status: 'running' }
			}
		});
		const afterMap = new Map(next.nodes.map((n) => [n.id, n.data.status]));
		expect(afterMap.get('llm_a')).toBe(
			beforeNodeStatuses.find((n) => n.id === 'llm_a')?.status
		);
		expect(Object.keys(next.nodeBindings).length).toBeGreaterThanOrEqual(beforeBindingCount);
		expect(next.nodeBindings.llm_a).toEqual(state.nodeBindings.llm_a);
	});

	it('run_started does not pre-stale bindings or regress succeeded displays', () => {
		const runId = 'run-1';
		const state = makeState();
		const beforeBindings = JSON.parse(JSON.stringify(state.nodeBindings));
		const beforeDisplay = new Map(
			Object.entries(state.nodeBindings).map(([id, b]) => [id, displayStatusFromBinding(b as any)])
		);

		const next = __applyRunEventForTest(
			state,
			{
				type: 'run_started',
				runId,
				at: '2026-02-25T00:00:00Z',
				runFrom: 'llm_b',
				runMode: 'from_selected_onward',
				plannedNodeIds: ['src', 'xfm', 'llm_b']
			},
			runId
		);

		expect(next.nodeBindings).toEqual(beforeBindings);
		for (const [id, status] of beforeDisplay.entries()) {
			expect(displayStatusFromBinding(next.nodeBindings[id] as any)).toBe(status);
		}
	});

	it('hard reset rotates graphId, clears bindings, and rejects old-graph updates', () => {
		const prev = makeState();
		const reset = __hardResetGraphForTest(prev, 'graph-B');

		expect(reset.graphId).toBe('graph-B');
		expect(reset.graphId).not.toBe(prev.graphId);
		expect(reset.nodes).toEqual([]);
		expect(reset.edges).toEqual([]);
		expect(reset.nodeBindings).toEqual({});
		expect(reset.nodeOutputs).toEqual({});
		expect(reset.activeRunId).toBeNull();
		expect(reset.freshness).toBe('never_run');
		expect(reset.lastRunStatus).toBe('never_run');

		const foreignEvt: KnownRunEvent = {
			type: 'run_started',
			runId: 'run-old',
			at: '2026-02-26T00:00:00Z',
			runFrom: null,
			runMode: 'from_start',
			plannedNodeIds: ['src']
		} as any;
		(foreignEvt as any).graphId = 'graph-A';
		const afterForeignEvent = __applyRunEventForTest(reset, foreignEvt, 'run-old');
		expect(afterForeignEvent).toEqual(reset);

		const afterForeignSnapshot = __hydrateFromRunSnapshotForTest(reset, {
			graphId: 'graph-A',
			status: 'running',
			nodeBindings: {
				src: { status: 'running' }
			}
		});
		expect(afterForeignSnapshot).toEqual(reset);
	});
});
