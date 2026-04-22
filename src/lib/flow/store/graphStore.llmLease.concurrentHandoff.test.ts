import { describe, expect, it } from 'vitest';

import type { GraphState } from './graphStore';
import { __applyRunEventForTest } from './graphStore';

function makeState(): GraphState {
	return {
		graphId: 'graph-llm-handoff',
		nodes: [
			{ id: 'a', data: { kind: 'model', meta: { llmAllocated: true } } },
			{ id: 'b', data: { kind: 'model', meta: { llmAllocated: true } } }
		] as any,
		edges: [] as any,
		selectedNodeId: null,
		inspector: { nodeId: null, draftParams: {}, dirty: false } as any,
		logs: [],
		runStatus: 'running',
		lastRunStatus: 'succeeded',
		freshness: 'up_to_date',
		staleNodeCount: 0,
		activeRunMode: 'from_selected_onward',
		activeRunFrom: 'b',
		activeRunNodeSet: new Set<string>(['b']),
		nodeOutputs: {},
		nodeBindings: {
			a: {
				status: 'running',
				isUpToDate: false,
				cacheValid: false,
				currentRunId: 'run-handoff',
				current: { execKey: null, artifactId: null },
				last: { execKey: null, artifactId: null },
				staleReason: null
			},
			b: {
				status: 'running',
				isUpToDate: false,
				cacheValid: false,
				currentRunId: 'run-handoff',
				current: { execKey: null, artifactId: null },
				last: { execKey: null, artifactId: null },
				staleReason: null
			}
		} as any,
		activeRunId: 'run-handoff',
		queueRuntime: {
			llmLease: {
				state: 'acquired',
				nodeId: 'b',
				holderNodeId: 'b',
				activeNodeIds: ['a', 'b'],
				waitQueueLength: 0,
				waitingNodeIds: [],
				updatedAt: '2026-04-22T00:00:00Z'
			}
		} as any
	};
}

describe('graphStore llm lease concurrent handoff', () => {
	it('keeps holder B allocated/running when A releases during concurrent lease', () => {
		const next = __applyRunEventForTest(
			makeState(),
			{
				type: 'llm_lease',
				runId: 'run-handoff',
				at: '2026-04-22T00:00:01Z',
				state: 'released',
				nodeId: 'a',
				holderNodeId: 'b',
				waitQueueLength: 0,
				waitingNodeIds: []
			} as any,
			'run-handoff'
		);

		const active = new Set((next as any)?.queueRuntime?.llmLease?.activeNodeIds ?? []);
		expect(active).toEqual(new Set(['b']));
		expect(Boolean(((next.nodes as any[])?.find((n) => n.id === 'b')?.data?.meta ?? {}).llmAllocated)).toBe(true);
		expect(String((next as any)?.nodeBindings?.b?.status ?? '')).toBe('running');
	});
});
