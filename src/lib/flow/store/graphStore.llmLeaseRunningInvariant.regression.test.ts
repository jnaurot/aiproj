import { describe, expect, it } from 'vitest';

import type { GraphState } from './graphStore';
import { __applyRunEventForTest } from './graphStore';
import type { KnownRunEvent } from '$lib/flow/types/run';

function makeState(): GraphState {
	return {
		graphId: 'graph-lease-invariant',
		nodes: [{ id: 'n_model', data: { kind: 'model', meta: { llmAllocated: true } } }] as any,
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
		nodeBindings: {
			n_model: {
				status: 'running',
				isUpToDate: false,
				cacheValid: false,
				currentRunId: 'run-lease-invariant',
				current: { execKey: null, artifactId: null },
				last: { execKey: null, artifactId: null },
				staleReason: null
			}
		} as any,
		activeRunId: 'run-lease-invariant',
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
	};
}

describe('graphStore model running/lease star invariant', () => {
	it('keeps model running and lease-star state coherent when control seq snapshot arrives before llm_released signal', () => {
		const initial = makeState();

		const afterSnapshot = __applyRunEventForTest(
			initial,
			{
				type: 'scheduler_snapshot',
				runId: 'run-lease-invariant',
				at: '2026-04-04T00:00:01Z',
				readyCount: 0,
				inflightCount: 0,
				pendingQueueDepth: 0,
				runnableNodeCount: 0,
				stalled: false,
				lastControlSeq: 12,
				perNode: []
			} as KnownRunEvent,
			'run-lease-invariant'
		);

		const afterLateControlRelease = __applyRunEventForTest(
			afterSnapshot as any,
			{
				type: 'control_signal',
				runId: 'run-lease-invariant',
				at: '2026-04-04T00:00:02Z',
				nodeId: 'n_model',
				signal: 'llm_released',
				seq: 11
			} as any,
			'run-lease-invariant'
		);

		const afterLeaseReleased = __applyRunEventForTest(
			afterLateControlRelease as any,
			{
				type: 'llm_lease',
				runId: 'run-lease-invariant',
				at: '2026-04-04T00:00:03Z',
				state: 'released',
				nodeId: 'n_model',
				holderNodeId: null,
				waitQueueLength: 0,
				waitingNodeIds: []
			} as KnownRunEvent,
			'run-lease-invariant'
		);

		const bindingStatus = String((afterLeaseReleased as any)?.nodeBindings?.n_model?.status ?? '');
		const llmAllocated = Boolean(
			((afterLeaseReleased as any)?.nodes ?? []).find((n: any) => String(n?.id) === 'n_model')?.data?.meta
				?.llmAllocated
		);

		// Invariant: a model cannot remain running if its lease/star has been released.
		expect(bindingStatus).not.toBe('running');
		expect(llmAllocated).toBe(false);
	});
});
