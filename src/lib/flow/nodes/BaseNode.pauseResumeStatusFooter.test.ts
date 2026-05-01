import { describe, expect, it, vi } from 'vitest';
import { render } from 'svelte/server';

import type { GraphState } from '../store/graphStore';

function makeState(): GraphState {
	return {
		graphId: 'graph-ui-pause',
		nodes: [{ id: 'n_component', data: { kind: 'component', label: 'Component', meta: {} } }] as any,
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
		activeRunNodeSet: new Set<string>(['n_component']),
		nodeOutputs: {},
		nodeBindings: {
			n_component: {
				graphId: 'graph-ui-pause',
				status: 'stale',
				lastArtifactId: 'art-old',
				lastRunId: 'run-prev',
				lastExecKey: 'exec-old',
				currentExecKey: null,
				currentArtifactId: null,
				currentRunId: null,
				isUpToDate: false,
				cacheValid: false,
				staleReason: 'RUN_PENDING'
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

describe('BaseNode footer status during pause snapshot merge', () => {
	it('renders completed (not completed stale) after run->pause->snapshot frontier binding merge', async () => {
		const paused: GraphState = {
			...makeState(),
			runStatus: 'paused',
			nodeBindings: {
				n_component: {
					graphId: 'graph-ui-pause',
					status: 'succeeded_up_to_date',
					lastArtifactId: 'art-new',
					lastRunId: 'run-1',
					lastExecKey: 'exec-new',
					currentExecKey: 'exec-new',
					currentArtifactId: 'art-new',
					currentRunId: 'run-1',
					isUpToDate: true,
					cacheValid: true,
					staleReason: null
				} as any
			}
		};

		const mockedStoreModule = (await import('$lib/flow/store/graphStore')) as any;
		mockedStoreModule.__setGraphStoreMockStateForTest(paused);
		const { default: BaseNode } = await import('./BaseNode.svelte');
		const rendered = render(BaseNode as any, {
			props: {
				id: 'n_component',
				data: { kind: 'component', label: 'Component', meta: {} }
			}
		});

		expect(rendered.body).toContain('completed');
		expect(rendered.body).not.toContain('completed (stale)');
	});

	it('keeps terminalized streaming nodes completed during active run', async () => {
		const runningTerminalized: GraphState = {
			...makeState(),
			runStatus: 'running',
			nodes: [
				{
					id: 'n_component',
					data: {
						kind: 'component',
						label: 'Component',
						meta: {},
						processingPolicy: { consume_mode: 'single_item' }
					}
				}
			] as any,
			nodeBindings: {
				n_component: {
					graphId: 'graph-ui-pause',
					status: 'succeeded_up_to_date',
					lastArtifactId: 'art-new',
					lastRunId: 'run-1',
					lastExecKey: 'exec-new',
					currentExecKey: 'exec-new',
					currentArtifactId: 'art-new',
					currentRunId: 'run-1',
					isUpToDate: true,
					cacheValid: true,
					staleReason: null
				} as any
			},
			queueRuntime: {
				schedulerSnapshot: {
					perNode: [
						{
							nodeId: 'n_component',
							readyWork: false,
							inflight: 0,
							pendingInputCount: 0,
							lastBlockedReasonCode: 'WAITING_REQUIRED_INPUT'
						}
					]
				},
				controlPlaneNodeState: {
					n_component: {
						nodeId: 'n_component',
						lastSignal: 'node_terminal',
						terminalReasonCode: 'completed'
					}
				}
			} as any
		};

		const mockedStoreModule = (await import('$lib/flow/store/graphStore')) as any;
		mockedStoreModule.__setGraphStoreMockStateForTest(runningTerminalized);
		const { default: BaseNode } = await import('./BaseNode.svelte');
		const rendered = render(BaseNode as any, {
			props: {
				id: 'n_component',
				data: {
					kind: 'component',
					label: 'Component',
					meta: {},
					processingPolicy: { consume_mode: 'single_item' }
				}
			}
		});

		expect(rendered.body).toContain('completed');
		expect(rendered.body).not.toContain('waiting');
	});

	it('renders status and mode badges on dedicated footer rows', async () => {
		const mockedStoreModule = (await import('$lib/flow/store/graphStore')) as any;
		mockedStoreModule.__setGraphStoreMockStateForTest(makeState());
		const { default: BaseNode } = await import('./BaseNode.svelte');
		const rendered = render(BaseNode as any, {
			props: {
				id: 'n_component',
				data: { kind: 'component', label: 'Component', meta: {} }
			}
		});

		expect(rendered.body).toContain('footerStatusRow');
		expect(rendered.body).toContain('footerMetaRow');
	});
});
