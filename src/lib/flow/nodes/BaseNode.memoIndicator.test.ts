import { describe, expect, it, vi } from 'vitest';
import { render } from 'svelte/server';

import type { GraphState } from '../store/graphStore';

function makeState(decision: 'reuse' | 'compute' | 'skip_non_memoizable'): GraphState {
	return {
		graphId: 'graph-memo-node',
		nodes: [{ id: 'n1', data: { kind: 'model', label: 'Model' } }] as any,
		edges: [] as any,
		selectedNodeId: null,
		inspector: { nodeId: null, draftParams: {}, dirty: false } as any,
		logs: [],
		runStatus: 'succeeded',
		lastRunStatus: 'succeeded',
		freshness: 'up_to_date',
		staleNodeCount: 0,
		activeRunMode: 'from_start',
		activeRunFrom: null,
		activeRunNodeSet: new Set<string>(['n1']),
		nodeOutputs: {},
		nodeBindings: {
			n1: {
				graphId: 'graph-memo-node',
				status: 'succeeded_up_to_date',
				lastArtifactId: 'art-1',
				lastRunId: 'run-1',
				lastExecKey: 'exec-1',
				currentExecKey: 'exec-1',
				currentArtifactId: 'art-1',
				currentRunId: 'run-1',
				isUpToDate: true,
				cacheValid: true,
				staleReason: null,
				memoState: { decision, memoKey: 'memo-1', resolvedAt: '2026-04-10T00:00:00.000Z' }
			} as any
		},
		activeRunId: 'run-1'
	};
}

vi.mock('$lib/flow/store/graphStore', async () => {
	const { writable } = await import('svelte/store');
	const graphStore = writable({} as GraphState);
	return {
		graphStore,
		deriveNodeIoForData: () => ({ in: null, out: null }),
		getNodeDocExplanationModeFromState: () => 'none',
		getNodeDocPlanesExpansionDelayMsFromState: () => 1200,
		getNodeDocPlanesExpansionEnabledFromState: () => true,
		getNodeDocTrainingModeFromState: () => 'off',
		getNodeDocTooltipEnabledFromState: () => false,
		getNodeDocTooltipOpenDelayMsFromState: () => 500,
		getNodeDocResolvedFromState: () => null,
		__setGraphStoreMockStateForTest: (nextState: GraphState) => graphStore.set(nextState)
	};
});

vi.mock('@xyflow/svelte', () => ({
	Handle: (() => '') as any,
	Position: {
		Left: 'left',
		Right: 'right'
	},
	useUpdateNodeInternals: () => (() => {})
}));

describe('BaseNode memo indicator', () => {
	it('renders cached badge for reuse decision', async () => {
		const mockedStoreModule = (await import('$lib/flow/store/graphStore')) as any;
		mockedStoreModule.__setGraphStoreMockStateForTest(makeState('reuse'));
		const { default: BaseNode } = await import('./BaseNode.svelte');
		const rendered = render(BaseNode as any, {
			props: {
				id: 'n1',
				data: { kind: 'model', label: 'Model', meta: {} }
			}
		});
		expect(rendered.body).toContain('cached');
		expect(rendered.body).not.toContain('computed');
	});
});
