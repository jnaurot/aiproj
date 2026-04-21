import { describe, expect, it } from 'vitest';

import type { GraphState } from './graphStore';
import {
	__applyRunEventForTest,
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
						status: 'succeeded_up_to_date',
						isUpToDate: true,
						cacheValid: true,
						currentArtifactId: 'art-1',
						currentExecKey: 'exec-1',
						memoState: { decision: 'reuse', memoKey: 'abc123', resolvedAt: '2026-04-10T00:00:00.000Z' }
					},
					'n1'
				),
				n2: nb(
					{
						status: 'succeeded_up_to_date',
						isUpToDate: true,
						cacheValid: true,
						currentArtifactId: 'art-2',
						currentExecKey: 'exec-2',
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
						status: 'succeeded_up_to_date',
						isUpToDate: true,
						cacheValid: true,
						currentArtifactId: 'art-1',
						currentExecKey: 'exec-1',
						memoState: { decision: 'reuse', memoKey: 'abc123', resolvedAt: '2026-04-10T00:00:00.000Z' }
					},
					'n1'
				),
				n2: nb(
					{
						status: 'succeeded_up_to_date',
						isUpToDate: true,
						cacheValid: true,
						currentArtifactId: 'art-2',
						currentExecKey: 'exec-2',
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
});
