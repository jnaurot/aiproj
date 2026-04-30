import { describe, expect, it } from 'vitest';

import type { GraphState } from './graphStore';
import {
	__applyRunEventForTest,
	__assertBindingPairForTest,
	__markStaleFromNodeForTest,
	__normalizeBindingForTest
} from './graphStore';
import type { KnownRunEvent } from '$lib/flow/types/run';
import { displayStatusFromBinding } from './runScope';

function makeState(runId = 'run-race'): GraphState {
	return {
		graphId: 'graph-race',
		nodes: [
			{
				id: 'n_source',
				data: { kind: 'source', sourceKind: 'file', status: 'idle', label: 'Source', params: {} }
			},
			{
				id: 'n_transform',
				data: { kind: 'transform', transformKind: 'filter', status: 'idle', label: 'Transform', params: {} }
			},
			{
				id: 'n_sink',
				data: { kind: 'model', llmKind: 'chat', status: 'idle', label: 'Sink', params: {} }
			}
		] as any,
		edges: [
			{ id: 'e1', source: 'n_source', target: 'n_transform', data: { exec: 'idle' } },
			{ id: 'e2', source: 'n_transform', target: 'n_sink', data: { exec: 'idle' } }
		] as any,
		selectedNodeId: null,
		inspector: { nodeId: null, draftParams: {}, dirty: false },
		logs: [],
		runStatus: 'running',
		lastRunStatus: 'succeeded',
		freshness: 'up_to_date',
		staleNodeCount: 0,
		activeRunMode: 'from_selected_onward',
		activeRunFrom: 'n_transform',
		activeRunNodeSet: new Set<string>(['n_transform', 'n_sink']),
		nodeOutputs: {
			n_source: { preview: 'source-preview', mimeType: 'text/plain', payloadType: 'text' },
			n_transform: { preview: 'transform-preview', mimeType: 'text/plain', payloadType: 'text' }
		},
		nodeBindings: {
			n_source: __normalizeBindingForTest(
				{ status: 'succeeded_up_to_date', isUpToDate: true, lastArtifactId: 'art-source' },
				'n_source'
			),
			n_transform: __normalizeBindingForTest(
				{ status: 'succeeded_up_to_date', isUpToDate: true, lastArtifactId: 'art-transform' },
				'n_transform'
			),
			n_sink: __normalizeBindingForTest(
				{ status: 'succeeded_up_to_date', isUpToDate: true, lastArtifactId: 'art-sink' },
				'n_sink'
			)
		},
		activeRunId: runId
	};
}

function makeStateWith(overrides: Partial<GraphState>, runId = String(overrides.activeRunId ?? 'run-race')): GraphState {
	return {
		...makeState(runId),
		...overrides
	};
}

function b(state: GraphState, nodeId: string) {
	const binding = state.nodeBindings[nodeId];
	expect(binding).toBeTruthy();
	__assertBindingPairForTest(binding, nodeId, 'race-test');
	return binding;
}

function apply(state: GraphState, evt: KnownRunEvent, runId = 'run-race') {
	return __applyRunEventForTest(state, evt, runId);
}

describe('graphStore races', () => {
	it('stale propagation cannot overwrite running node in active run', () => {
		const runId = 'run-1';
		let state = makeState(runId);
		state = apply(
			state,
			{ type: 'node_started', runId, at: '2026-03-01T00:00:00Z', nodeId: 'n_transform' },
			runId
		);
		expect(displayStatusFromBinding(b(state, 'n_transform'))).toBe('running');

		state = __markStaleFromNodeForTest(state, 'n_source');
		expect(displayStatusFromBinding(b(state, 'n_transform'))).toBe('running');
	});

	it('completed run with retained activeRunId does not block local stale invalidation', () => {
		const runId = 'run-completed';
		const state0 = makeStateWith(
			{
				runStatus: 'succeeded',
				activeRunId: runId,
				nodeBindings: {
					n_source: __normalizeBindingForTest(
						{
							status: 'succeeded_up_to_date',
							isUpToDate: true,
							currentRunId: runId,
							current: { execKey: 'ek_source', artifactId: 'art_source' },
							last: { execKey: 'ek_source', artifactId: 'art_source' }
						},
						'n_source'
					),
					n_transform: __normalizeBindingForTest(
						{
							status: 'succeeded_up_to_date',
							isUpToDate: true,
							currentRunId: runId,
							current: { execKey: 'ek_transform', artifactId: 'art_transform' },
							last: { execKey: 'ek_transform', artifactId: 'art_transform' }
						},
						'n_transform'
					),
					n_sink: __normalizeBindingForTest(
						{
							status: 'succeeded_up_to_date',
							isUpToDate: true,
							currentRunId: runId,
							current: { execKey: 'ek_sink', artifactId: 'art_sink' },
							last: { execKey: 'ek_sink', artifactId: 'art_sink' }
						},
						'n_sink'
					)
				}
			},
			runId
		);
		const state = __markStaleFromNodeForTest(state0, 'n_source');
		expect(displayStatusFromBinding(b(state, 'n_source'))).toBe('stale');
		expect(displayStatusFromBinding(b(state, 'n_transform'))).toBe('stale');
		expect(displayStatusFromBinding(b(state, 'n_sink'))).toBe('stale');
	});

	it('old-run node_finished does not mutate active run node state', () => {
		const runId = 'run-1';
		let state = makeState(runId);
		state = apply(
			state,
			{ type: 'node_started', runId, at: '2026-03-01T00:00:01Z', nodeId: 'n_transform' },
			runId
		);

		const next = apply(
			state,
			{
				type: 'node_finished',
				runId: 'run-old',
				at: '2026-03-01T00:00:02Z',
				nodeId: 'n_transform',
				status: 'failed'
			},
			runId
		);
		expect(displayStatusFromBinding(b(next, 'n_transform'))).toBe('running');
		expect(next.nodeBindings.n_transform.currentRunId).toBe(runId);
	});

	it('late cache_hit does not force running node to succeeded', () => {
		const runId = 'run-1';
		let state = makeState(runId);
		const beforeLast = state.nodeBindings.n_transform.last;
		const beforePreview = state.nodeOutputs.n_transform.preview;
		state = apply(
			state,
			{ type: 'node_started', runId, at: '2026-03-01T00:00:03Z', nodeId: 'n_transform' },
			runId
		);
		state = apply(
			state,
			{
				type: 'cache_decision',
				runId,
				at: '2026-03-01T00:00:04Z',
				nodeId: 'n_transform',
				nodeKind: 'transform',
				decision: 'cache_hit',
				execKey: 'ek-transform',
				artifactId: 'art-transform-v2'
			},
			runId
		);
		expect(displayStatusFromBinding(b(state, 'n_transform'))).toBe('running');
		expect(state.nodeBindings.n_transform.current.execKey).toBe('ek-transform');
		expect(state.nodeBindings.n_transform.current.artifactId).toBe('art-transform-v2');
		expect(state.nodeBindings.n_transform.last).toEqual(beforeLast);
		expect(state.nodeOutputs.n_transform.preview).toBe(beforePreview);
	});

	it('contract mismatch does not regress fresh active-run success', () => {
		const runId = 'run-1';
		let state = makeState(runId);
		state = apply(
			state,
			{
				type: 'cache_decision',
				runId,
				at: '2026-03-01T00:00:05Z',
				nodeId: 'n_transform',
				nodeKind: 'transform',
				decision: 'cache_hit',
				execKey: 'ek-transform',
				artifactId: 'art-transform-v3'
			},
			runId
		);
		state = apply(
			state,
			{
				type: 'node_output',
				runId,
				at: '2026-03-01T00:00:06Z',
				nodeId: 'n_transform',
				artifactId: 'art-transform-v3',
				preview: 'new-preview',
				mimeType: 'text/plain',
				payloadType: 'text'
			},
			runId
		);
		state = apply(
			state,
			{
				type: 'node_finished',
				runId,
				at: '2026-03-01T00:00:07Z',
				nodeId: 'n_transform',
				status: 'succeeded'
			},
			runId
		);
		const afterFinished = b(state, 'n_transform');
		expect(displayStatusFromBinding(b(state, 'n_transform'))).toBe('succeeded');
		expect(afterFinished.isUpToDate).toBe(true);
		expect(afterFinished.cacheValid).toBe(true);
		expect(afterFinished.staleReason).toBeNull();
		expect(afterFinished.currentRunId).toBe(runId);
		expect(afterFinished.current.execKey).toBe('ek-transform');
		expect(afterFinished.current.artifactId).toBe('art-transform-v3');
		expect(afterFinished.last.execKey).toBe('ek-transform');
		expect(afterFinished.last.artifactId).toBe('art-transform-v3');

		const next = apply(
			state,
			{
				type: 'cache_decision',
				runId,
				at: '2026-03-01T00:00:08Z',
				nodeId: 'n_transform',
				nodeKind: 'transform',
				decision: 'cache_hit_contract_mismatch',
				execKey: 'ek-transform',
				expectedContractFingerprint: 'exp',
				actualContractFingerprint: 'act'
			} as KnownRunEvent,
			runId
		);
		expect(displayStatusFromBinding(b(next, 'n_transform'))).toBe('succeeded');
		expect(next.nodeOutputs.n_transform.preview).toBe('new-preview');
	});

	it('node_finished before node_started does not regress succeeded node to running', () => {
		// Models the out-of-order event race: the backend emitted node_started at T=09
		// and node_finished at T=12 (cache hit, fast path), but the frontend received
		// them out of order — node_finished arrived first, then the delayed node_started.
		// The delayed start's at timestamp (T=09) is BEFORE the finish (T=12),
		// so the timestamp guard correctly identifies it as stale and drops it.
		const runId = 'run-1';
		let state = makeState(runId);
		state = apply(
			state,
			{
				type: 'cache_decision',
				runId,
				at: '2026-03-01T00:00:10Z',
				nodeId: 'n_transform',
				nodeKind: 'transform',
				decision: 'cache_hit',
				execKey: 'ek-finish-first',
				artifactId: 'art-finish-first'
			},
			runId
		);
		state = apply(
			state,
			{
				type: 'node_output',
				runId,
				at: '2026-03-01T00:00:11Z',
				nodeId: 'n_transform',
				artifactId: 'art-finish-first',
				preview: 'finish-first-preview',
				mimeType: 'text/plain',
				payloadType: 'text'
			},
			runId
		);
		state = apply(
			state,
			{
				type: 'node_finished',
				runId,
				at: '2026-03-01T00:00:12Z',
				nodeId: 'n_transform',
				status: 'succeeded'
			},
			runId
		);
		const beforeStart = b(state, 'n_transform');
		expect(beforeStart.isUpToDate).toBe(true);
		expect(beforeStart.cacheValid).toBe(true);
		expect(beforeStart.staleReason).toBeNull();
		expect(state.nodeOutputs.n_transform.preview).toBe('finish-first-preview');

		// Delayed node_started: at=T09 (before the finish at T12) → stale → dropped
		const next = apply(
			state,
			{ type: 'node_started', runId, at: '2026-03-01T00:00:09Z', nodeId: 'n_transform' },
			runId
		);
		const afterStart = b(next, 'n_transform');
		expect(displayStatusFromBinding(afterStart)).toBe('succeeded');
		expect(afterStart.isUpToDate).toBe(true);
		expect(afterStart.cacheValid).toBe(true);
		expect(afterStart.staleReason).toBeNull();
		expect(next.nodeOutputs.n_transform.preview).toBe('finish-first-preview');
	});

	it('duplicate node_finished is idempotent', () => {
		const runId = 'run-1';
		const event: KnownRunEvent = {
			type: 'node_finished',
			runId,
			at: '2026-03-01T00:00:14Z',
			nodeId: 'n_transform',
			status: 'succeeded'
		};
		let state = makeState(runId);
		state = apply(
			state,
			{
				type: 'cache_decision',
				runId,
				at: '2026-03-01T00:00:14Z',
				nodeId: 'n_transform',
				nodeKind: 'transform',
				decision: 'cache_hit',
				execKey: 'ek-idem',
				artifactId: 'art-idem'
			},
			runId
		);
		state = apply(
			state,
			{
				type: 'node_output',
				runId,
				at: '2026-03-01T00:00:14Z',
				nodeId: 'n_transform',
				artifactId: 'art-idem',
				preview: 'idem-preview'
			},
			runId
		);
		const once = apply(state, event, runId);
		const twice = apply(once, event, runId);
		expect(twice.nodeBindings.n_transform).toEqual(once.nodeBindings.n_transform);
		expect(twice.nodeOutputs.n_transform).toEqual(once.nodeOutputs.n_transform);
	});

	it('finish then late start then duplicate finish remains succeeded and pair-stable', () => {
		// Delayed node_started arrives after node_finished (same item, out-of-order).
		// lateStartAt is BEFORE firstFinishAt: represents when the backend emitted the
		// start event, which arrived at the frontend after node_finished due to ordering.
		const runId = 'run-1';
		const firstFinishAt = '2026-03-01T00:00:18Z';
		const lateStartAt = '2026-03-01T00:00:17Z';   // emitted before finish — arrives late
		const secondFinishAt = '2026-03-01T00:00:20Z';

		let state = makeState(runId);
		state = apply(
			state,
			{
				type: 'cache_decision',
				runId,
				at: firstFinishAt,
				nodeId: 'n_transform',
				nodeKind: 'transform',
				decision: 'cache_hit',
				execKey: 'ek-seq',
				artifactId: 'art-seq'
			},
			runId
		);
		state = apply(
			state,
			{
				type: 'node_output',
				runId,
				at: firstFinishAt,
				nodeId: 'n_transform',
				artifactId: 'art-seq',
				preview: 'seq-preview'
			},
			runId
		);
		state = apply(
			state,
			{
				type: 'node_finished',
				runId,
				at: firstFinishAt,
				nodeId: 'n_transform',
				status: 'succeeded'
			},
			runId
		);
		const afterFirstFinish = b(state, 'n_transform');
		const afterFirstFinishCurrent = { ...afterFirstFinish.current };
		const afterFirstFinishLast = { ...afterFirstFinish.last };
		expect(displayStatusFromBinding(afterFirstFinish)).toBe('succeeded');

		state = apply(
			state,
			{ type: 'node_started', runId, at: lateStartAt, nodeId: 'n_transform' },
			runId
		);
		const afterLateStart = b(state, 'n_transform');
		expect(displayStatusFromBinding(afterLateStart)).toBe('succeeded');
		expect(afterLateStart.current).toEqual(afterFirstFinishCurrent);
		expect(afterLateStart.last).toEqual(afterFirstFinishLast);

		state = apply(
			state,
			{
				type: 'node_finished',
				runId,
				at: secondFinishAt,
				nodeId: 'n_transform',
				status: 'succeeded'
			},
			runId
		);
		const afterSecondFinish = b(state, 'n_transform');
		expect(displayStatusFromBinding(afterSecondFinish)).toBe('succeeded');
		expect(afterSecondFinish.current).toEqual(afterFirstFinishCurrent);
		expect(afterSecondFinish.last).toEqual(afterFirstFinishLast);
		expect(state.nodeOutputs.n_transform.preview).toBe('seq-preview');
	});

	it('duplicate old-run node_finished events are ignored', () => {
		const runId = 'run-1';
		let state = makeState(runId);
		state = apply(
			state,
			{ type: 'node_started', runId, at: '2026-03-01T00:00:15Z', nodeId: 'n_transform' },
			runId
		);
		const before = b(state, 'n_transform');
		const oldFinish: KnownRunEvent = {
			type: 'node_finished',
			runId: 'run-old',
			at: '2026-03-01T00:00:16Z',
			nodeId: 'n_transform',
			status: 'failed'
		};
		state = apply(state, oldFinish, runId);
		state = apply(state, oldFinish, runId);
		const after = b(state, 'n_transform');
		expect(displayStatusFromBinding(after)).toBe('running');
		expect(after.currentRunId).toBe(runId);
		expect(after.current).toEqual(before.current);
		expect(after.last).toEqual(before.last);
	});

	it('events for nodes outside planned set still apply when runId matches', () => {
		const runId = 'run-1';
		const state = apply(
			makeState(runId),
			{
				type: 'cache_decision',
				runId,
				at: '2026-03-01T00:00:09Z',
				nodeId: 'n_source',
				nodeKind: 'source',
				decision: 'cache_hit',
				execKey: 'ek-source',
				artifactId: 'art-source-v2'
			},
			runId
		);
		expect(state.activeRunNodeSet.has('n_source')).toBe(false);
		expect(state.nodeBindings.n_source.current.execKey).toBe('ek-source');
		expect(state.nodeBindings.n_source.current.artifactId).toBe('art-source-v2');
	});

	it('node_output without cache hit clears stale cached pill state to cache_miss', () => {
		const runId = 'run-cache-reset';
		const seed = makeStateWith(
			{
				nodeOutputs: {
					n_source: {
						preview: 'old-preview',
						mimeType: 'text/plain',
						payloadType: 'text',
						cached: true,
						cacheDecision: 'cache_hit'
					}
				},
				nodeBindings: {
					n_source: __normalizeBindingForTest(
						{
							status: 'running',
							currentRunId: runId,
							current: { execKey: 'ek-old', artifactId: 'art-old' },
							last: { execKey: 'ek-old', artifactId: 'art-old' }
						},
						'n_source'
					)
				} as any
			},
			runId
		);
		const next = apply(
			seed,
			{
				type: 'node_output',
				runId,
				at: '2026-03-03T00:00:00Z',
				nodeId: 'n_source',
				artifactId: 'art-new',
				preview: 'new-preview',
				mimeType: 'text/plain',
				payloadType: 'text',
				cached: false
			} as any,
			runId
		);
		expect(next.nodeOutputs.n_source.cached).toBe(false);
		expect(next.nodeOutputs.n_source.cacheDecision).toBe('cache_miss');
	});

	it('no outputs present remains safe and preserves last binding under stale/mismatch', () => {
		const runId = 'run-1';
		let state = makeStateWith(
			{
				nodeOutputs: {},
				nodeBindings: {
					n_source: __normalizeBindingForTest(
						{
							status: 'succeeded_up_to_date',
							isUpToDate: true,
							last: { execKey: 'ek-source-last', artifactId: 'art-source-last' }
						},
						'n_source'
					),
					n_transform: __normalizeBindingForTest(
						{
							status: 'succeeded_up_to_date',
							isUpToDate: true,
							last: { execKey: 'ek-transform-last', artifactId: 'art-transform-last' }
						},
						'n_transform'
					),
					n_sink: __normalizeBindingForTest(
						{
							status: 'succeeded_up_to_date',
							isUpToDate: true,
							last: { execKey: 'ek-sink-last', artifactId: 'art-sink-last' }
						},
						'n_sink'
					)
				}
			},
			runId
		);
		state = __markStaleFromNodeForTest(state, 'n_source');
		expect(displayStatusFromBinding(b(state, 'n_transform'))).toBe('stale');
		expect(state.nodeBindings.n_transform.last.execKey).toBe('ek-transform-last');
		expect(state.nodeBindings.n_transform.last.artifactId).toBe('art-transform-last');

		state = apply(
			state,
			{
				type: 'cache_decision',
				runId,
				at: '2026-03-01T00:00:17Z',
				nodeId: 'n_transform',
				nodeKind: 'transform',
				decision: 'cache_hit_contract_mismatch',
				execKey: 'ek-new',
				expectedContractFingerprint: 'exp',
				actualContractFingerprint: 'act'
			} as KnownRunEvent,
			runId
		);
		expect(state.nodeOutputs.n_transform?.preview).toBeUndefined();
		expect(state.nodeBindings.n_transform.last.execKey).toBe('ek-transform-last');
		expect(state.nodeBindings.n_transform.last.artifactId).toBe('art-transform-last');
	});

	it('multi-item re-start: node_started after node_finished for a later item correctly sets running', () => {
		// A model node in "single" mode processes 3 items sequentially within one run.
		// Each item produces node_started → node_finished (succeeded) → node_started → ...
		// The second and third node_started events must NOT be dropped (they are legitimate
		// re-starts with at timestamps strictly AFTER the previous node_finished).
		const runId = 'run-multi';
		let state = makeState(runId);

		// Item 1: start → finish
		state = apply(state, { type: 'node_started', runId, at: '2026-03-01T00:00:00Z', nodeId: 'n_transform' }, runId);
		expect(displayStatusFromBinding(b(state, 'n_transform'))).toBe('running');

		state = apply(state, {
			type: 'node_finished', runId, at: '2026-03-01T00:00:30Z',
			nodeId: 'n_transform', status: 'succeeded'
		}, runId);
		expect(displayStatusFromBinding(b(state, 'n_transform'))).toBe('succeeded');
		expect(b(state, 'n_transform').lastFinishedAt).toBe('2026-03-01T00:00:30Z');

		// Item 2: start (at > lastFinishedAt) → must transition back to running
		state = apply(state, { type: 'node_started', runId, at: '2026-03-01T00:00:31Z', nodeId: 'n_transform' }, runId);
		expect(displayStatusFromBinding(b(state, 'n_transform'))).toBe('running');

		state = apply(state, {
			type: 'node_finished', runId, at: '2026-03-01T00:01:05Z',
			nodeId: 'n_transform', status: 'succeeded'
		}, runId);
		expect(displayStatusFromBinding(b(state, 'n_transform'))).toBe('succeeded');

		// Item 3: start (at > lastFinishedAt) → must transition back to running again
		state = apply(state, { type: 'node_started', runId, at: '2026-03-01T00:01:06Z', nodeId: 'n_transform' }, runId);
		expect(displayStatusFromBinding(b(state, 'n_transform'))).toBe('running');

		// Stale/duplicate start for item 1 (at < lastFinishedAt) → must be dropped
		const stale = apply(state, { type: 'node_started', runId, at: '2026-03-01T00:00:00Z', nodeId: 'n_transform' }, runId);
		expect(displayStatusFromBinding(b(stale, 'n_transform'))).toBe('running'); // unchanged
	});
});

