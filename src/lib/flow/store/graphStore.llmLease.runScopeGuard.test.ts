import { describe, expect, it } from 'vitest';

import type { GraphState } from './graphStore';
import { __applyRunEventForTest } from './graphStore';

function makeState(): GraphState {
	return {
		graphId: 'graph-llm-scope',
		nodes: [{ id: 'a', data: { kind: 'model', meta: { llmAllocated: true } } }] as any,
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
		activeRunNodeSet: new Set<string>(['a']),
		nodeOutputs: {},
		nodeBindings: {
			a: {
				status: 'running',
				isUpToDate: false,
				cacheValid: false,
				currentRunId: 'run-active',
				current: { execKey: null, artifactId: null },
				last: { execKey: null, artifactId: null },
				staleReason: null
			}
		} as any,
		activeRunId: 'run-active',
		queueRuntime: {
			llmLease: {
				state: 'acquired',
				nodeId: 'a',
				holderNodeId: 'a',
				activeNodeIds: ['a'],
				waitQueueLength: 0,
				waitingNodeIds: [],
				updatedAt: '2026-04-22T00:00:00Z'
			}
		} as any
	};
}

describe('graphStore llm lease run-scope guard', () => {
	it('ignores llm_lease from non-active run id', () => {
		const next = __applyRunEventForTest(
			makeState(),
			{
				type: 'llm_lease',
				runId: 'run-old',
				at: '2026-04-22T00:00:01Z',
				state: 'released',
				nodeId: 'a',
				holderNodeId: null,
				waitQueueLength: 0,
				waitingNodeIds: []
			} as any,
			'run-active'
		);

		expect(new Set((next as any)?.queueRuntime?.llmLease?.activeNodeIds ?? [])).toEqual(new Set(['a']));
		expect(Boolean(((next.nodes as any[])?.find((n) => n.id === 'a')?.data?.meta ?? {}).llmAllocated)).toBe(true);
		expect(String((next as any)?.nodeBindings?.a?.status ?? '')).toBe('running');
	});
});
