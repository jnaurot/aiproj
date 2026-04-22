import { describe, expect, it, vi } from 'vitest';
import { render } from 'svelte/server';

import type { GraphState } from '../store/graphStore';

const mockState = vi.hoisted(() => ({ value: {} as GraphState }));

vi.mock('$lib/flow/store/graphStore', async () => {
	const { writable } = await import('svelte/store');
	const graphStore = writable(mockState.value);
	return {
		graphStore,
		deriveNodeIoForData: () => ({ in: null, out: null }),
		getNodeDocExplanationModeFromState: () => 'none',
		getNodeDocPlanesExpansionDelayMsFromState: () => 1200,
		getNodeDocPlanesExpansionEnabledFromState: () => true,
		getNodeDocTrainingModeFromState: () => 'off',
		getNodeDocTooltipEnabledFromState: () => false,
		getNodeDocTooltipOpenDelayMsFromState: () => 500,
		getNodeDocResolvedFromState: () => null
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

function makeState(nodeMeta: Record<string, unknown>, bindingStatus: string): GraphState {
	return {
		graphId: 'graph-model-lease-visual',
		nodes: [
			{
				id: 'n_model',
				data: {
					kind: 'model',
					label: 'ResumeBuilder',
					modelKind: 'llm',
					llmKind: 'ollama',
					params: { model: 'glm-4.7-flash:latest', output: { mode: 'json' } },
					meta: nodeMeta
				}
			}
		] as any,
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
		activeRunNodeSet: new Set<string>(['n_model']),
		nodeOutputs: {},
		nodeBindings: {
			n_model: {
				status: bindingStatus,
				isUpToDate: false,
				cacheValid: false,
				currentRunId: 'run-lease-visual',
				current: { execKey: null, artifactId: null },
				last: { execKey: null, artifactId: null },
				staleReason: null
			}
		} as any,
		activeRunId: 'run-lease-visual',
		queueRuntime: {
			schedulerSnapshot: {
				perNode: [{ nodeId: 'n_model', readyWork: false, inflight: 0, pendingInputCount: 0 }]
			}
		} as any
	};
}

describe('ModelNode lease visual truth', () => {
	it('renders running + star when lease holder is active even if binding is waiting/busy', async () => {
		mockState.value = makeState({ llmAllocated: true }, 'busy');
		const { default: ModelNode } = await import('./ModelNode.svelte');
		const rendered = render(ModelNode as any, {
			props: {
				id: 'n_model',
				data: mockState.value.nodes[0].data,
				selected: false
			}
		});

		expect(rendered.body).toContain('running');
		expect(rendered.body).toContain('★');
	});

	it('demotes to waiting and hides star when lease is absent', async () => {
		mockState.value = makeState({}, 'running');
		const { default: ModelNode } = await import('./ModelNode.svelte');
		const rendered = render(ModelNode as any, {
			props: {
				id: 'n_model',
				data: mockState.value.nodes[0].data,
				selected: false
			}
		});

		expect(rendered.body).toContain('waiting');
		expect(rendered.body).not.toContain('★');
	});
});
