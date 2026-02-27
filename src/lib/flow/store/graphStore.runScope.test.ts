import { describe, expect, it } from 'vitest';

import type { GraphState } from './graphStore';
import {
	__applyRunEventForTest,
	__assertBindingPairForTest,
	__hardResetGraphForTest,
	__hydrateFromRunSnapshotForTest,
	__markStaleFromNodeForTest
} from './graphStore';
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

function makeArtistBranchState(): GraphState {
	return {
		graphId: 'graph-test',
		nodes: [{ id: 'src' }, { id: 'llm_artist' }, { id: 'llm' }] as any,
		edges: [
			{ id: 'e1', source: 'src', target: 'llm_artist', data: { exec: 'idle' } },
			{ id: 'e2', source: 'src', target: 'llm', data: { exec: 'idle' } }
		] as any,
		selectedNodeId: null,
		inspector: { nodeId: null, draftParams: {}, dirty: false },
		logs: [],
		runStatus: 'running',
		lastRunStatus: 'succeeded',
		freshness: 'stale',
		staleNodeCount: 3,
		activeRunMode: 'from_selected_onward',
		activeRunFrom: 'llm',
		activeRunNodeSet: new Set<string>(['src', 'llm_artist', 'llm']),
		nodeOutputs: {},
		nodeBindings: {
			src: { status: 'stale', isUpToDate: false, lastArtifactId: 'art-src' },
			llm_artist: { status: 'stale', isUpToDate: false, lastArtifactId: 'art-artist' },
			llm: { status: 'stale', isUpToDate: false, lastArtifactId: 'art-llm' }
		},
		activeRunId: 'run-artist'
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

	it('hydrate snapshot preserves derived status for nodes absent from snapshot bindings', () => {
		const state = makeState();
		const beforeDerived = new Map(
			Object.entries(state.nodeBindings).map(([id, b]) => [id, displayStatusFromBinding(b as any)])
		);
		const beforeBindingCount = Object.keys(state.nodeBindings).length;
		const next = __hydrateFromRunSnapshotForTest(state, {
			status: 'running',
			nodeBindings: {
				llm_b: { status: 'running' }
			}
		});
		expect(displayStatusFromBinding(next.nodeBindings.llm_a as any)).toBe(beforeDerived.get('llm_a'));
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

	it('snapshot switch marks downstream stale while keeping stale previews visible', () => {
		const state = makeState();
		const withPreviews: GraphState = {
			...state,
			nodeOutputs: {
				xfm: { preview: 'xfm old preview', cacheDecision: 'cache_hit', cached: true },
				llm_a: { preview: 'llm_a old preview', cacheDecision: 'cache_hit', cached: true },
				llm_b: { preview: 'llm_b old preview', cacheDecision: 'cache_hit', cached: true }
			}
		};
		const next = __markStaleFromNodeForTest(withPreviews, 'src');

		expect(next.nodeBindings.src.status).toBe('stale');
		expect(next.nodeBindings.xfm.status).toBe('stale');
		expect(next.nodeBindings.llm_a.status).toBe('stale');
		expect(next.nodeBindings.llm_b.status).toBe('stale');
		expect(next.nodeBindings.xfm.staleReason).toBe('UPSTREAM_CHANGED');
		expect(next.nodeBindings.src.staleReason).toBe('PARAMS_CHANGED');

		expect(next.nodeBindings.xfm.lastArtifactId).toBe('art-xfm');
		expect(next.nodeBindings.llm_a.lastArtifactId).toBe('art-a');
		expect(next.nodeBindings.llm_b.lastArtifactId).toBe('art-b');

		expect(next.nodeOutputs.xfm.preview).toBe('xfm old preview');
		expect(next.nodeOutputs.llm_a.preview).toBe('llm_a old preview');
		expect(next.nodeOutputs.llm_b.preview).toBe('llm_b old preview');
	});

	it('stale downstream can fast-reuse cached artifacts on selected-onward run', () => {
		const runId = 'run-2';
		let state = __markStaleFromNodeForTest(makeState(), 'src');
		expect(state.nodeBindings.xfm.status).toBe('stale');
		expect(state.nodeBindings.llm_b.status).toBe('stale');

		state = __applyRunEventForTest(
			state,
			{
				type: 'run_started',
				runId,
				at: '2026-03-01T00:00:00Z',
				runFrom: 'llm_b',
				runMode: 'from_selected_onward',
				plannedNodeIds: ['src', 'xfm', 'llm_b']
			},
			runId
		);

		state = __applyRunEventForTest(
			state,
			{
				type: 'cache_decision',
				runId,
				at: '2026-03-01T00:00:01Z',
				nodeId: 'xfm',
				nodeKind: 'transform',
				decision: 'cache_hit',
				execKey: 'xfm-key-new',
				artifactId: 'art-xfm-cached'
			},
			runId
		);
		state = __applyRunEventForTest(
			state,
			{ type: 'node_finished', runId, at: '2026-03-01T00:00:01Z', nodeId: 'xfm', status: 'succeeded' },
			runId
		);
		state = __applyRunEventForTest(
			state,
			{
				type: 'cache_decision',
				runId,
				at: '2026-03-01T00:00:02Z',
				nodeId: 'llm_b',
				nodeKind: 'llm',
				decision: 'cache_hit',
				execKey: 'llm-b-key-new',
				artifactId: 'art-b-cached'
			},
			runId
		);
		state = __applyRunEventForTest(
			state,
			{ type: 'node_finished', runId, at: '2026-03-01T00:00:02Z', nodeId: 'llm_b', status: 'succeeded' },
			runId
		);
		state = __applyRunEventForTest(
			state,
			{ type: 'run_finished', runId, at: '2026-03-01T00:00:03Z', status: 'succeeded' },
			runId
		);

		expect(state.nodeBindings.xfm.current?.artifactId).toBe('art-xfm-cached');
		expect(state.nodeBindings.llm_b.current?.artifactId).toBe('art-b-cached');
		expect(state.nodeBindings.xfm.cacheValid).toBe(true);
		expect(state.nodeBindings.llm_b.cacheValid).toBe(true);
		expect(state.nodeBindings.xfm.isUpToDate).toBe(true);
		expect(state.nodeBindings.llm_b.isUpToDate).toBe(true);
		expect(displayStatusFromBinding(state.nodeBindings.xfm as any)).toBe('succeeded');
		expect(displayStatusFromBinding(state.nodeBindings.llm_b as any)).toBe('succeeded');
	});

	it('source stays succeeded after finish while downstream continues running', () => {
		const runId = 'run-3';
		let state = __markStaleFromNodeForTest(makeState(), 'src');
		expect(state.nodeBindings.src.status).toBe('stale');
		expect(state.nodeBindings.xfm.status).toBe('stale');

		state = __applyRunEventForTest(
			state,
			{
				type: 'run_started',
				runId,
				at: '2026-03-01T01:00:00Z',
				runFrom: 'llm_b',
				runMode: 'from_selected_onward',
				plannedNodeIds: ['xfm', 'llm_b']
			},
			runId
		);

		state = __applyRunEventForTest(
			state,
			{
				type: 'node_started',
				runId,
				at: '2026-03-01T01:00:01Z',
				nodeId: 'src',
				nodeKind: 'source'
			} as any,
			runId
		);
		expect(displayStatusFromBinding(state.nodeBindings.src as any)).toBe('running');

		state = __applyRunEventForTest(
			state,
			{
				type: 'node_finished',
				runId,
				at: '2026-03-01T01:00:02Z',
				nodeId: 'src',
				status: 'succeeded'
			},
			runId
		);
		expect(displayStatusFromBinding(state.nodeBindings.src as any)).toBe('succeeded');

		state = __applyRunEventForTest(
			state,
			{
				type: 'node_started',
				runId,
				at: '2026-03-01T01:00:03Z',
				nodeId: 'llm_b',
				nodeKind: 'llm'
			} as any,
			runId
		);
		expect(displayStatusFromBinding(state.nodeBindings.llm_b as any)).toBe('running');
		expect(displayStatusFromBinding(state.nodeBindings.src as any)).toBe('succeeded');
		expect(state.nodeBindings.src.status).toBe('succeeded_up_to_date');
	});

	it('LLM_artist stays succeeded after finish even if stale marking runs while sibling LLM is running', () => {
		const runId = 'run-artist';
		let state = makeArtistBranchState();

		state = __applyRunEventForTest(
			state,
			{
				type: 'node_started',
				runId,
				at: '2026-03-01T02:00:00Z',
				nodeId: 'llm_artist',
				nodeKind: 'llm'
			} as any,
			runId
		);
		expect(displayStatusFromBinding(state.nodeBindings.llm_artist as any)).toBe('running');

		state = __applyRunEventForTest(
			state,
			{
				type: 'node_finished',
				runId,
				at: '2026-03-01T02:00:01Z',
				nodeId: 'llm_artist',
				status: 'succeeded'
			},
			runId
		);
		expect(displayStatusFromBinding(state.nodeBindings.llm_artist as any)).toBe('succeeded');

		state = __applyRunEventForTest(
			state,
			{
				type: 'node_started',
				runId,
				at: '2026-03-01T02:00:02Z',
				nodeId: 'llm',
				nodeKind: 'llm'
			} as any,
			runId
		);
		expect(displayStatusFromBinding(state.nodeBindings.llm as any)).toBe('running');

		state = __markStaleFromNodeForTest(state, 'src');

		expect(displayStatusFromBinding(state.nodeBindings.llm_artist as any)).toBe('succeeded');
		expect(state.nodeBindings.llm_artist.status).toBe('succeeded_up_to_date');
		expect(displayStatusFromBinding(state.nodeBindings.llm as any)).toBe('running');
		expect(state.nodeBindings.llm.status).toBe('running');
	});

	it('node_finished from another run_id is ignored during active run', () => {
		const runId = 'run-1';
		const state = makeState();
		const next = __applyRunEventForTest(
			state,
			{
				type: 'node_finished',
				runId: 'run-old',
				at: '2026-03-01T03:00:00Z',
				nodeId: 'llm_b',
				status: 'failed'
			},
			runId
		);
		expect(next.nodeBindings.llm_b.status).toBe(state.nodeBindings.llm_b.status);
	});

	it('cache_hit from another run_id cannot flip active run node state', () => {
		const runId = 'run-1';
		const state = makeState();
		const next = __applyRunEventForTest(
			state,
			{
				type: 'cache_decision',
				runId: 'run-old',
				at: '2026-03-01T03:00:01Z',
				nodeId: 'src',
				nodeKind: 'source',
				decision: 'cache_hit',
				execKey: 'old-key',
				artifactId: 'old-art'
			},
			runId
		);
		expect(next.nodeBindings.src.current).toBeUndefined();
		expect(next.nodeBindings.src.isUpToDate).toBe(true);
	});

	it('binding pair invariant rejects partial pairs', () => {
		expect(() =>
			__assertBindingPairForTest({
				current: { execKey: 'only-exec', artifactId: null },
				last: { execKey: null, artifactId: null }
			} as any)
		).toThrow(/INVALID_BINDING_PAIR/);
	});

	it('node_finished keeps current/last pairs atomic on cache hit path', () => {
		const runId = 'run-1';
		let state = makeState();
		state = __applyRunEventForTest(
			state,
			{
				type: 'cache_decision',
				runId,
				at: '2026-03-01T04:00:00Z',
				nodeId: 'src',
				nodeKind: 'source',
				decision: 'cache_hit',
				execKey: 'src-key-4',
				artifactId: 'art-src-4'
			},
			runId
		);
		state = __applyRunEventForTest(
			state,
			{ type: 'node_finished', runId, at: '2026-03-01T04:00:01Z', nodeId: 'src', status: 'succeeded' },
			runId
		);
		expect(state.nodeBindings.src.current).toEqual({ execKey: 'src-key-4', artifactId: 'art-src-4' });
		expect(state.nodeBindings.src.last).toEqual({ execKey: 'src-key-4', artifactId: 'art-src-4' });
	});

	it('stale propagation never mutates last pair', () => {
		const state: GraphState = {
			...makeState(),
			nodeBindings: {
				...makeState().nodeBindings,
				src: {
					status: 'succeeded_up_to_date',
					isUpToDate: true,
					current: { execKey: 'k1', artifactId: 'a1' },
					last: { execKey: 'k1', artifactId: 'a1' }
				}
			}
		};
		const next = __markStaleFromNodeForTest(state, 'src');
		expect(next.nodeBindings.src.current).toEqual({ execKey: null, artifactId: null });
		expect(next.nodeBindings.src.last).toEqual({ execKey: 'k1', artifactId: 'a1' });
	});

	it('contract mismatch marks stale without wiping preview or last pair', () => {
		const runId = 'run-1';
		let state: GraphState = {
			...makeState(),
			nodeBindings: {
				...makeState().nodeBindings,
				src: {
					status: 'succeeded_up_to_date',
					isUpToDate: true,
					cacheValid: true,
					current: { execKey: 'k1', artifactId: 'a1' },
					last: { execKey: 'k1', artifactId: 'a1' }
				}
			},
			nodeOutputs: {
				src: { preview: 'old', cached: true, cacheDecision: 'cache_hit' }
			}
		};
		state = __applyRunEventForTest(
			state,
			{
				type: 'cache_decision',
				runId,
				at: '2026-03-01T05:00:00Z',
				nodeId: 'src',
				nodeKind: 'source',
				decision: 'cache_hit_contract_mismatch',
				execKey: 'k1',
				artifactId: 'a1',
				expectedContractFingerprint: 'expfp',
				actualContractFingerprint: 'actfp',
				mismatchKind: 'port_type'
			},
			runId
		);
		expect(state.nodeBindings.src.status).toBe('stale');
		expect(state.nodeBindings.src.staleReason).toBe('CONTRACT_MISMATCH');
		expect(state.nodeBindings.src.cacheValid).toBe(false);
		expect(state.nodeBindings.src.isUpToDate).toBe(false);
		expect(state.nodeBindings.src.last).toEqual({ execKey: 'k1', artifactId: 'a1' });
		expect(state.nodeOutputs.src.preview).toBe('old');
		expect(state.nodeOutputs.src.expectedContractFingerprint).toBe('expfp');
		expect(state.nodeOutputs.src.actualContractFingerprint).toBe('actfp');
		expect(state.nodeOutputs.src.mismatchKind).toBe('port_type');
	});

	it('contract mismatch cannot override running/succeeded node in active run', () => {
		const runId = 'run-1';
		let state: GraphState = {
			...makeState(),
			nodeBindings: {
				...makeState().nodeBindings,
				src: {
					status: 'running',
					currentRunId: runId,
					isUpToDate: true,
					cacheValid: true,
					current: { execKey: 'k2', artifactId: 'a2' },
					last: { execKey: 'k2', artifactId: 'a2' }
				}
			}
		};
		state = __applyRunEventForTest(
			state,
			{
				type: 'cache_decision',
				runId,
				at: '2026-03-01T05:00:01Z',
				nodeId: 'src',
				nodeKind: 'source',
				decision: 'cache_hit_contract_mismatch',
				execKey: 'k2',
				artifactId: 'a2'
			},
			runId
		);
		expect(state.nodeBindings.src.status).toBe('running');
		expect(state.nodeBindings.src.cacheValid).toBe(false);
	});
});
