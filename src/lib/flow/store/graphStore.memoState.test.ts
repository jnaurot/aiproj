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

	it('clears memoState for all bindings on run_started', () => {
		const runId = 'run-2';
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
				),
				n2: nb(
					{
						status: 'succeeded_up_to_date',
						isUpToDate: true,
						cacheValid: true,
						currentArtifactId: 'art-2',
						currentExecKey: 'exec-2',
						memoState: { decision: 'compute', memoKey: 'def456', resolvedAt: '2026-04-10T00:00:00.000Z' }
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

		expect(next.nodeBindings.n1?.memoState).toBeUndefined();
		expect(next.nodeBindings.n2?.memoState).toBeUndefined();
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
