import { describe, expect, it } from 'vitest';

import type { GraphState } from './graphStore';
import {
	__applyRunEventForTest,
	__hydrateFromRunSnapshotForTest,
	__markStaleFromNodeForTest,
	__normalizeBindingForTest
} from './graphStore';

function nb(binding: Record<string, unknown>, nodeId: string) {
	return __normalizeBindingForTest(binding as any, nodeId);
}

function makeState(): GraphState {
	return {
		graphId: 'graph-memo-state',
		nodes: [{ id: 'n1', data: { kind: 'model', label: 'Model 1' } }] as any,
		edges: [] as any,
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
			n1: nb(
				{
					status: 'succeeded_up_to_date',
					isUpToDate: true,
					cacheValid: true,
					currentArtifactId: 'art-1',
					currentExecKey: 'exec-1'
				},
				'n1'
			)
		},
		activeRunId: 'run-1'
	};
}

describe('graphStore memo state projection', () => {
	it('writes memoState on reuse trace log', () => {
		const runId = 'run-1';
		const state = makeState();
		const next = __applyRunEventForTest(
			state,
			{
				type: 'log',
				runId,
				at: '2026-04-10T00:00:00Z',
				level: 'info',
				nodeId: 'n1',
				message:
					'[trace][memo.execute_decision] {"decision":"reuse","memoKey":"abc123"}'
			} as any,
			runId
		);

		expect(next.nodeBindings.n1?.memoState?.decision).toBe('reuse');
		expect(next.nodeBindings.n1?.memoState?.memoKey).toBe('abc123');
		expect(typeof next.nodeBindings.n1?.memoState?.resolvedAt).toBe('string');
	});

	it('writes memoState on compute trace log', () => {
		const runId = 'run-1';
		const state = makeState();
		const next = __applyRunEventForTest(
			state,
			{
				type: 'log',
				runId,
				at: '2026-04-10T00:00:00Z',
				level: 'info',
				nodeId: 'n1',
				message:
					'[trace][memo.execute_decision] {"decision":"compute","memoKey":"def456"}'
			} as any,
			runId
		);

		expect(next.nodeBindings.n1?.memoState?.decision).toBe('compute');
		expect(next.nodeBindings.n1?.memoState?.memoKey).toBe('def456');
	});

	it('clears memoState only for planned nodes on run_started (partial run)', () => {
		// n1 is planned; n2 is a sibling NOT included in this partial run.
		// After the run_started event n1 must have its memoState reset (it is
		// about to be re-executed) while n2's memoState must be preserved so
		// the inspector can still offer the "Save checkpoint" action for n2.
		//
		// The state here reflects what exists AFTER resetRunUiState() has been
		// called (which always zeroes current.execKey / current.artifactId before
		// a run starts).  memoState is preserved by resetRunUiState via spread.
		// Only nodes whose current.execKey is already set (fast all-cache race,
		// where the snapshot arrived before run_started) bypass the clear.
		const runId = 'run-2';
		const state = {
			...makeState(),
			// Both nodes must appear in state.nodes so withGraphMeta does not prune
			// their bindings via ensureNormalizedBindingsForNodes.
			nodes: [
				{ id: 'n1', data: { kind: 'model', label: 'Model 1' } },
				{ id: 'n2', data: { kind: 'model', label: 'Model 2' } }
			] as any,
			nodeBindings: {
				n1: nb(
					{
						status: 'idle',          // resetRunUiState sets idle + zeros current
						isUpToDate: false,
						cacheValid: false,
						current: { execKey: null, artifactId: null },
						last: { execKey: 'exec-1', artifactId: 'art-1' },
						memoState: { decision: 'reuse', memoKey: 'abc123', resolvedAt: '2026-04-10T00:00:00.000Z' }
					},
					'n1'
				),
				n2: nb(
					{
						status: 'idle',
						isUpToDate: false,
						cacheValid: false,
						current: { execKey: null, artifactId: null },
						last: { execKey: 'exec-2', artifactId: 'art-2' },
						memoState: { decision: 'reuse', memoKey: 'def456', resolvedAt: '2026-04-10T00:00:00.000Z' }
					},
					'n2'
				)
			}
		} as GraphState;

		const next = __applyRunEventForTest(
			state,
			{
				type: 'run_started',
				runId,
				at: '2026-04-10T00:01:00Z',
				runMode: 'from_start',
				plannedNodeIds: ['n1']
			} as any,
			runId
		);

		// Planned node — memoState must be cleared so fresh trace events repopulate it.
		expect(next.nodeBindings.n1?.memoState).toBeUndefined();
		// Non-planned sibling — memoState must survive so checkpoint remains available.
		expect(next.nodeBindings.n2?.memoState?.decision).toBe('reuse');
		expect(next.nodeBindings.n2?.memoState?.memoKey).toBe('def456');
	});

	it('clears memoState for all planned nodes on a full run_started', () => {
		// When ALL nodes are planned every memoState should be reset.
		// State represents post-resetRunUiState: current is zeroed, last retains lineage.
		const runId = 'run-full';
		const state = {
			...makeState(),
			nodes: [
				{ id: 'n1', data: { kind: 'model', label: 'Model 1' } },
				{ id: 'n2', data: { kind: 'model', label: 'Model 2' } }
			] as any,
			nodeBindings: {
				n1: nb(
					{
						status: 'idle',
						isUpToDate: false,
						cacheValid: false,
						current: { execKey: null, artifactId: null },
						last: { execKey: 'exec-1', artifactId: 'art-1' },
						memoState: { decision: 'reuse', memoKey: 'abc123', resolvedAt: '2026-04-10T00:00:00.000Z' }
					},
					'n1'
				),
				n2: nb(
					{
						status: 'idle',
						isUpToDate: false,
						cacheValid: false,
						current: { execKey: null, artifactId: null },
						last: { execKey: 'exec-2', artifactId: 'art-2' },
						memoState: { decision: 'reuse', memoKey: 'def456', resolvedAt: '2026-04-10T00:00:00.000Z' }
					},
					'n2'
				)
			}
		} as GraphState;

		const next = __applyRunEventForTest(
			state,
			{
				type: 'run_started',
				runId,
				at: '2026-04-10T00:01:00Z',
				runMode: 'from_start',
				plannedNodeIds: ['n1', 'n2']
			} as any,
			runId
		);

		expect(next.nodeBindings.n1?.memoState).toBeUndefined();
		expect(next.nodeBindings.n2?.memoState).toBeUndefined();
	});

	it('non-planned node retains checkpointable flag on partial run_started', () => {
		// A node that was previously marked checkpointable but is not in the
		// current run's plan must keep that flag (same root cause as memoState).
		const runId = 'run-partial';
		const state = {
			...makeState(),
			nodes: [
				{ id: 'n1', data: { kind: 'model', label: 'Model 1' } },
				{ id: 'n2', data: { kind: 'model', label: 'Model 2' } }
			] as any,
			nodeBindings: {
				n1: nb({ status: 'running', checkpointable: false }, 'n1'),
				n2: nb(
					{
						status: 'succeeded_up_to_date',
						isUpToDate: true,
						cacheValid: true,
						currentArtifactId: 'art-2',
						currentExecKey: 'exec-2',
						checkpointable: true,
						memoState: { decision: 'reuse', memoKey: 'aaaa'.repeat(16), resolvedAt: '2026-04-10T00:00:00.000Z' }
					},
					'n2'
				)
			}
		} as GraphState;

		const next = __applyRunEventForTest(
			state,
			{
				type: 'run_started',
				runId,
				at: '2026-04-10T00:02:00Z',
				runMode: 'from_selected_onward',
				plannedNodeIds: ['n1']
			} as any,
			runId
		);

		// n2 was not planned — its checkpoint eligibility must be preserved.
		expect((next.nodeBindings.n2 as any)?.checkpointable).toBe(true);
		expect(next.nodeBindings.n2?.memoState?.memoKey).toBe('aaaa'.repeat(16));
	});

	it('keeps prior memoState visible when node is marked stale before next run', () => {
		const state = {
			...makeState(),
			nodeBindings: {
				n1: nb(
					{
						status: 'succeeded_up_to_date',
						isUpToDate: true,
						cacheValid: true,
						currentArtifactId: 'art-1',
						currentExecKey: 'exec-1',
						memoState: { decision: 'reuse', memoKey: 'abc123', resolvedAt: '2026-04-10T00:00:00.000Z' }
					},
					'n1'
				)
			}
		} as GraphState;

		const next = __markStaleFromNodeForTest(state, 'n1');
		expect(next.nodeBindings.n1?.memoState?.decision).toBe('reuse');
		expect(next.nodeBindings.n1?.memoState?.memoKey).toBe('abc123');
	});

	it('run_started does not stale-mark a node whose binding already reflects THIS run (fast all-cache race)', () => {
		// Scenario: the initial getRun snapshot arrives and is applied BEFORE the event
		// stream delivers run_started.  The binding already shows succeeded_up_to_date
		// with currentRunId === the new run's ID.  The run_started handler must NOT
		// clobber it with 'stale', which would cause a transient loss of canSaveCheckpoint.
		const runId = 'run-fast';
		const execKey = 'a'.repeat(64); // 64-hex SHA-256

		// Simulate the state AFTER the initial getRun snapshot was applied:
		// - runStatus was set to 'succeeded' (run already done)
		// - nodeBindings reflect the completed state with currentRunId = runId
		const state: GraphState = {
			...makeState(),
			graphId: 'graph-fast',
			activeRunId: runId,
			runStatus: 'succeeded' as any,
			nodes: [
				{ id: 'n1', data: { kind: 'model', label: 'Model 1' } },
				{ id: 'n2', data: { kind: 'model', label: 'Model 2' } }
			] as any,
			activeRunNodeSet: new Set(['n1', 'n2']),
			nodeBindings: {
				n1: nb(
					{
						status: 'succeeded_up_to_date',
						isUpToDate: true,
						cacheValid: true,
						currentRunId: runId,
						currentArtifactId: execKey,
						currentExecKey: execKey
					},
					'n1'
				),
				n2: nb(
					{
						status: 'succeeded_up_to_date',
						isUpToDate: true,
						cacheValid: true,
						currentRunId: runId,
						currentArtifactId: execKey,
						currentExecKey: execKey
					},
					'n2'
				)
			}
		};

		// Now run_started arrives late (after the snapshot was already applied).
		const next = __applyRunEventForTest(
			state,
			{
				type: 'run_started',
				runId,
				at: '2026-04-10T00:01:00Z',
				runMode: 'from_start',
				plannedNodeIds: ['n1', 'n2']
			} as any,
			runId
		);

		// Both nodes already have the current run's result — they must NOT be set to stale.
		expect(next.nodeBindings.n1?.status).toBe('succeeded_up_to_date');
		expect(next.nodeBindings.n2?.status).toBe('succeeded_up_to_date');
		// currentRunId must be preserved (not nulled out by the stale transition)
		expect(next.nodeBindings.n1?.currentRunId).toBe(runId);
		expect(next.nodeBindings.n2?.currentRunId).toBe(runId);
	});

	it('run_started still marks stale for nodes that do NOT yet reflect the current run', () => {
		// After resetRunUiState, current.execKey is always null.  When run_started
		// fires and a node has last.artifactId (retained lineage) but current.execKey
		// is null, it means the snapshot has NOT yet reflected the new run — the node
		// must still be stale-marked to clear the UI before fresh events arrive.
		const runId = 'run-new';
		const prevExecKey = 'b'.repeat(64);

		const state: GraphState = {
			...makeState(),
			graphId: 'graph-stale',
			activeRunId: runId,
			runStatus: 'running' as any,
			nodes: [{ id: 'n1', data: { kind: 'model', label: 'Model 1' } }] as any,
			activeRunNodeSet: new Set(['n1']),
			nodeBindings: {
				n1: nb(
					{
						status: 'idle',
						isUpToDate: false,
						cacheValid: false,
						// current is zeroed by resetRunUiState; last retains old lineage.
						current: { execKey: null, artifactId: null },
						last: { execKey: prevExecKey, artifactId: prevExecKey }
					},
					'n1'
				)
			}
		};

		const next = __applyRunEventForTest(
			state,
			{
				type: 'run_started',
				runId,
				at: '2026-04-10T00:01:00Z',
				runMode: 'from_start',
				plannedNodeIds: ['n1']
			} as any,
			runId
		);

		// Node has no current.execKey — the snapshot hasn't reflected this run yet.
		// Must be set to stale/RUN_PENDING so the UI shows the correct pending state.
		expect(next.nodeBindings.n1?.status).toBe('stale');
		expect((next.nodeBindings.n1 as any)?.staleReason).toBe('RUN_PENDING');
	});
});
