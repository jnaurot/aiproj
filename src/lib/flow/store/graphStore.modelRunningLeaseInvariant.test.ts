import { describe, expect, it } from 'vitest';

import type { GraphState } from './graphStore';
import { __applyRunEventForTest } from './graphStore';
import type { KnownRunEvent } from '$lib/flow/types/run';

function makeBaseState(overrides?: Partial<GraphState>): GraphState {
	return {
		graphId: 'graph-model-lease-invariant',
		nodes: [],
		edges: [] as any,
		selectedNodeId: null,
		inspector: { nodeId: null, draftParams: {}, dirty: false } as any,
		logs: [],
		runStatus: 'running',
		lastRunStatus: 'succeeded',
		freshness: 'up_to_date',
		staleNodeCount: 0,
		activeRunMode: 'from_start',
		activeRunFrom: null,
		activeRunNodeSet: new Set<string>(),
		nodeOutputs: {},
		nodeBindings: {} as any,
		activeRunId: 'run-model-lease-invariant',
		queueRuntime: {
			appliedControlSeq: 0,
			llmLease: {
				state: 'released',
				nodeId: 'n_model',
				holderNodeId: null,
				activeNodeIds: [],
				waitQueueLength: 0,
				waitingNodeIds: [],
				updatedAt: '2026-04-04T00:00:00Z'
			}
		} as any,
		...overrides
	} as GraphState;
}

describe('graphStore model running/star invariant', () => {
	it('forces running model nodes to lose running state when lease star is absent', () => {
		const state = makeBaseState({
			nodes: [{ id: 'n_model', data: { kind: 'model', meta: {} } }] as any,
			nodeBindings: {
				n_model: {
					status: 'running',
					isUpToDate: false,
					cacheValid: false,
					currentRunId: 'run-model-lease-invariant',
					current: { execKey: null, artifactId: null },
					last: { execKey: null, artifactId: null },
					staleReason: null
				}
			} as any
		});

		const next = __applyRunEventForTest(
			state,
			{
				type: 'scheduler_snapshot',
				runId: 'run-model-lease-invariant',
				at: '2026-04-04T00:00:01Z',
				readyCount: 0,
				inflightCount: 0,
				pendingQueueDepth: 0,
				runnableNodeCount: 0,
				stalled: false,
				perNode: []
			} as KnownRunEvent,
			'run-model-lease-invariant'
		);

		const status = String((next as any)?.nodeBindings?.n_model?.status ?? '');
		const star = Boolean(((next as any)?.nodes ?? []).find((n: any) => String(n?.id) === 'n_model')?.data?.meta?.llmAllocated);

		expect(status).not.toBe('running');
		expect(star).toBe(false);
	});

	it('forces leased model nodes to running and visible star while run is active', () => {
		const state = makeBaseState({
			nodes: [{ id: 'n_model', data: { kind: 'model', meta: {} } }] as any,
			nodeBindings: {
				n_model: {
					status: 'waiting',
					isUpToDate: false,
					cacheValid: false,
					currentRunId: 'run-model-lease-invariant',
					current: { execKey: null, artifactId: null },
					last: { execKey: null, artifactId: null },
					staleReason: null
				}
			} as any,
			queueRuntime: {
				appliedControlSeq: 0,
				llmLease: {
					state: 'acquired',
					nodeId: 'n_model',
					holderNodeId: 'n_model',
					activeNodeIds: ['n_model'],
					waitQueueLength: 0,
					waitingNodeIds: [],
					updatedAt: '2026-04-04T00:00:00Z'
				}
			} as any
		});

		const next = __applyRunEventForTest(
			state,
			{
				type: 'scheduler_snapshot',
				runId: 'run-model-lease-invariant',
				at: '2026-04-04T00:00:01Z',
				readyCount: 0,
				inflightCount: 0,
				pendingQueueDepth: 0,
				runnableNodeCount: 0,
				stalled: false,
				perNode: []
			} as KnownRunEvent,
			'run-model-lease-invariant'
		);

		const status = String((next as any)?.nodeBindings?.n_model?.status ?? '');
		const star = Boolean(((next as any)?.nodes ?? []).find((n: any) => String(n?.id) === 'n_model')?.data?.meta?.llmAllocated);

		expect(status).toBe('running');
		expect(star).toBe(true);
	});

	it('does not force non-model nodes to lease/star invariants', () => {
		const state = makeBaseState({
			nodes: [{ id: 'n_transform', data: { kind: 'transform', meta: {} } }] as any,
			nodeBindings: {
				n_transform: {
					status: 'running',
					isUpToDate: false,
					cacheValid: false,
					currentRunId: 'run-model-lease-invariant',
					current: { execKey: null, artifactId: null },
					last: { execKey: null, artifactId: null },
					staleReason: null
				}
			} as any
		});

		const next = __applyRunEventForTest(
			state,
			{
				type: 'scheduler_snapshot',
				runId: 'run-model-lease-invariant',
				at: '2026-04-04T00:00:01Z',
				readyCount: 0,
				inflightCount: 0,
				pendingQueueDepth: 0,
				runnableNodeCount: 0,
				stalled: false,
				perNode: []
			} as KnownRunEvent,
			'run-model-lease-invariant'
		);

		const status = String((next as any)?.nodeBindings?.n_transform?.status ?? '');
		const star = Boolean(((next as any)?.nodes ?? []).find((n: any) => String(n?.id) === 'n_transform')?.data?.meta?.llmAllocated);

		expect(status).toBe('running');
		expect(star).toBe(false);
	});
});

