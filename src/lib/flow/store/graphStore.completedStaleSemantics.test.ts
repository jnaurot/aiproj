import { describe, expect, it } from 'vitest';

import type { GraphState } from './graphStore';
import {
	__applyRunEventForTest,
	__markStaleFromNodeForTest,
	__normalizeBindingForTest,
	__resetRunUiStateForTest
} from './graphStore';
import { projectNodeStatus } from './statusModel';

function makeState(): GraphState {
	return {
		graphId: 'g-completed-stale',
		nodes: [
			{ id: 'n1', data: { kind: 'transform', meta: {} } },
			{ id: 'n2', data: { kind: 'model', meta: {} } },
			{ id: 'n3', data: { kind: 'source', meta: {} } }
		] as any,
		edges: [
			{ id: 'e12', source: 'n1', target: 'n2', targetHandle: 'in', data: { mode: 'work', exec: 'idle' } }
		] as any,
		selectedNodeId: null,
		inspector: { nodeId: null, draftParams: {}, dirty: false } as any,
		logs: [],
		runStatus: 'idle',
		lastRunStatus: 'succeeded',
		freshness: 'up_to_date',
		staleNodeCount: 0,
		activeRunMode: 'from_start',
		activeRunFrom: null,
		activeRunNodeSet: new Set<string>(),
		nodeOutputs: {},
		nodeBindings: {},
		activeRunId: null
	};
}

function succeededBinding(nodeId: string, execKey: string, artifactId: string) {
	return __normalizeBindingForTest(
		{
			status: 'succeeded_up_to_date',
			isUpToDate: true,
			cacheValid: true,
			current: { execKey, artifactId },
			last: { execKey, artifactId },
			currentRunId: 'run-1',
			lastRunId: 'run-1'
		},
		nodeId
	);
}

describe('completed vs completed(stale) semantics', () => {
	it('1) completed: fresh successful node remains completed', () => {
		const state = makeState();
		state.nodeBindings.n1 = succeededBinding('n1', 'exec-1', 'art-1') as any;
		state.nodeOutputs.n1 = { preview: { ok: true } } as any;
		const projection = projectNodeStatus(state.nodeBindings.n1 as any);
		expect(projection.lifecycle).toBe('completed');
		expect(projection.freshness).toBe('fresh');
		expect(projection.display).toBe('succeeded');
		expect(state.nodeBindings.n1?.currentArtifactId).toBe('art-1');
	});

	it('2) completed(stale): local params change after success preserves prior output and flags recompute', () => {
		const state = makeState();
		state.nodeBindings.n1 = succeededBinding('n1', 'exec-1', 'art-1') as any;
		state.nodeOutputs.n1 = { preview: { value: 'kept' } } as any;
		const next = __markStaleFromNodeForTest(state, 'n1');
		const projection = projectNodeStatus(next.nodeBindings.n1 as any);
		expect(projection.lifecycle).toBe('completed');
		expect(projection.freshness).toBe('stale');
		expect(projection.display).toBe('stale');
		expect(next.nodeOutputs.n1?.preview).toEqual({ value: 'kept' });
		expect(next.nodeBindings.n1?.status).toBe('stale');
		expect(next.nodeBindings.n1?.currentArtifactId ?? null).toBeNull();
		expect(next.nodeBindings.n1?.lastArtifactId).toBe('art-1');
	});

	it('3) completed(stale): upstream change after downstream success stales downstream and preserves output', () => {
		const state = makeState();
		state.nodeBindings.n1 = succeededBinding('n1', 'exec-up', 'art-up') as any;
		state.nodeBindings.n2 = succeededBinding('n2', 'exec-down', 'art-down') as any;
		state.nodeOutputs.n2 = { preview: { downstream: true } } as any;
		const next = __markStaleFromNodeForTest(state, 'n1');
		const projection = projectNodeStatus(next.nodeBindings.n2 as any);
		expect(projection.lifecycle).toBe('completed');
		expect(projection.freshness).toBe('stale');
		expect(next.nodeOutputs.n2?.preview).toEqual({ downstream: true });
		expect(next.nodeBindings.n2?.lastArtifactId).toBe('art-down');
	});

	it('4) completed(stale): graph wiring/dependency change invalidates prior success', () => {
		const state = makeState();
		state.edges = [
			{ id: 'e32', source: 'n3', target: 'n2', targetHandle: 'in', data: { mode: 'work', exec: 'idle' } }
		] as any;
		state.nodeBindings.n3 = succeededBinding('n3', 'exec-3', 'art-3') as any;
		state.nodeBindings.n2 = succeededBinding('n2', 'exec-2', 'art-2') as any;
		const next = __markStaleFromNodeForTest(state, 'n3');
		const projection = projectNodeStatus(next.nodeBindings.n2 as any);
		expect(projection.lifecycle).toBe('completed');
		expect(projection.freshness).toBe('stale');
		expect(next.nodeBindings.n2?.lastArtifactId).toBe('art-2');
	});

	it('5) completed(stale): determinism/exec identity drift marks prior success stale', () => {
		const projection = projectNodeStatus({
			status: 'succeeded_up_to_date',
			isUpToDate: true,
			currentExecKey: 'exec-new',
			lastExecKey: 'exec-old',
			currentArtifactId: 'art-1',
			lastArtifactId: 'art-1'
		});
		expect(projection.lifecycle).toBe('completed');
		expect(projection.freshness).toBe('stale');
		expect(projection.display).toBe('stale');
	});

	it('6) completed(stale): schema/contract mismatch invalidates reuse while preserving output', () => {
		const state = makeState();
		state.nodeBindings.n1 = succeededBinding('n1', 'exec-1', 'art-1') as any;
		state.nodeOutputs.n1 = { preview: { schema: 'old' } } as any;
		const next = __applyRunEventForTest(
			state,
			{
				type: 'cache_decision',
				runId: 'run-1',
				at: '2026-04-08T12:00:00Z',
				nodeId: 'n1',
				decision: 'cache_hit_contract_mismatch',
				expectedContractFingerprint: 'fp-old',
				actualContractFingerprint: 'fp-new',
				mismatchKind: 'schema'
			} as any,
			'run-1'
		);
		const projection = projectNodeStatus(next.nodeBindings.n1 as any);
		expect(projection.lifecycle).toBe('completed');
		expect(projection.freshness).toBe('stale');
		expect(next.nodeOutputs.n1?.preview).toEqual({ schema: 'old' });
		expect(next.nodeBindings.n1?.status).toBe('stale');
	});

	it('7) never-run node is idle, not completed or completed(stale)', () => {
		const projection = projectNodeStatus({ status: 'idle', isUpToDate: false });
		expect(projection.lifecycle).toBe('idle');
		expect(projection.display).toBe('idle');
		expect(projection.freshness).toBe('unknown');
	});

	it('8) failed node is not relabeled completed(stale) even with prior artifact history', () => {
		const projection = projectNodeStatus({
			status: 'failed',
			isUpToDate: false,
			lastArtifactId: 'art-last-good'
		});
		expect(projection.lifecycle).toBe('failed');
		expect(projection.display).toBe('failed');
	});

	it('9) reset produces idle (not completed/completed-stale) while lineage may remain', () => {
		const state = makeState();
		state.nodeBindings.n1 = succeededBinding('n1', 'exec-1', 'art-1') as any;
		const next = __resetRunUiStateForTest(state);
		const projection = projectNodeStatus(next.nodeBindings.n1 as any);
		expect(next.nodeBindings.n1?.status).toBe('idle');
		expect(projection.lifecycle).toBe('idle');
		expect(projection.display).toBe('idle');
		expect(next.nodeBindings.n1?.lastArtifactId).toBe('art-1');
	});

	it('10) rerun after stale restores completed fresh and clears stale marker', () => {
		const state = makeState();
		state.nodeBindings.n1 = succeededBinding('n1', 'exec-1', 'art-1') as any;
		const stale = __markStaleFromNodeForTest(state, 'n1');
		const afterOutput = __applyRunEventForTest(
			stale,
			{
				type: 'node_output',
				runId: 'run-2',
				at: '2026-04-08T12:30:00Z',
				nodeId: 'n1',
				artifactId: 'art-2',
				mimeType: 'application/json',
				payloadType: 'json'
			} as any,
			'run-2'
		);
		const rerun = __applyRunEventForTest(
			afterOutput,
			{
				type: 'node_finished',
				runId: 'run-2',
				at: '2026-04-08T12:30:02Z',
				nodeId: 'n1',
				status: 'succeeded'
			} as any,
			'run-2'
		);
		const projection = projectNodeStatus(rerun.nodeBindings.n1 as any);
		expect(projection.lifecycle).toBe('completed');
		expect(projection.freshness).toBe('fresh');
		expect(projection.display).toBe('succeeded');
		expect(rerun.nodeBindings.n1?.staleReason).toBeNull();
		expect(rerun.nodeBindings.n1?.currentArtifactId).toBe('art-2');
	});
});

