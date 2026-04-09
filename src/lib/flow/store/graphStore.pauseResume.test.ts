import { describe, expect, it } from 'vitest';

import type { GraphState } from './graphStore';
import {
	__applyRunEventForTest,
	__hydrateFromRunSnapshotForTest,
	__resetRunUiStateForTest,
	__setPauseResumeTraceEnabledForTest
} from './graphStore';
import { displayStatusFromBinding } from './runScope';
import type { KnownRunEvent } from '$lib/flow/types/run';

function makeState(runStatus: GraphState['runStatus'] = 'running'): GraphState {
	return {
		graphId: 'graph-pause',
		nodes: [{ id: 'n1', data: { kind: 'model', meta: { llmAllocated: true } } }] as any,
		edges: [
			{
				id: 'e_work',
				source: 'src',
				sourceHandle: 'out',
				target: 'n1',
				targetHandle: 'in',
				data: { mode: 'work', exec: 'active' }
			},
			{
				id: 'e_param',
				source: 'cfg',
				sourceHandle: 'out',
				target: 'n1',
				targetHandle: 'param_config',
				data: { mode: 'param', exec: 'idle' }
			}
		] as any,
		selectedNodeId: null,
		inspector: { nodeId: null, draftParams: {}, dirty: false } as any,
		logs: [],
		runStatus,
		lastRunStatus: 'succeeded',
		freshness: 'up_to_date',
		staleNodeCount: 0,
		activeRunMode: 'from_start',
		activeRunFrom: null,
		activeRunNodeSet: new Set<string>(['n1']),
		nodeOutputs: {},
		nodeBindings: {},
		activeRunId: 'run-pause'
	};
}

describe('graphStore pause/resume lifecycle', () => {
	it('handles pausing -> paused -> resuming -> running transitions', () => {
		let state = makeState('running');
		state = __applyRunEventForTest(
			state,
			{ type: 'run_pausing', runId: 'run-pause', at: '2026-03-30T00:00:00Z' } as any,
			'run-pause'
		);
		expect(state.runStatus).toBe('pausing');
		state = __applyRunEventForTest(
			state,
			{ type: 'run_paused', runId: 'run-pause', at: '2026-03-30T00:00:01Z' } as any,
			'run-pause'
		);
		expect(state.runStatus).toBe('paused');
		state = __applyRunEventForTest(
			state,
			{ type: 'run_resuming', runId: 'run-pause', at: '2026-03-30T00:00:02Z' } as any,
			'run-pause'
		);
		expect(state.runStatus).toBe('resuming');
		state = __applyRunEventForTest(
			state,
			{
				type: 'run_resumed',
				runId: 'run-pause',
				at: '2026-03-30T00:00:03Z',
				runMode: 'from_selected_onward',
				runFrom: 'n1',
				plannedNodeIds: ['n1']
			} as any,
			'run-pause'
		);
		expect(state.runStatus).toBe('running');
	});

	it('paused state clears running visuals and llm ownership', () => {
		const state = makeState('running');
		const next = __applyRunEventForTest(
			state,
			{ type: 'run_paused', runId: 'run-pause', at: '2026-03-30T00:00:01Z' } as any,
			'run-pause'
		);
		const workEdge = (next.edges as any[]).find((e) => String(e.id) === 'e_work');
		const paramEdge = (next.edges as any[]).find((e) => String(e.id) === 'e_param');
		expect(next.runStatus).toBe('paused');
		expect(String(workEdge?.data?.exec ?? '')).toBe('idle');
		expect(String(paramEdge?.data?.exec ?? '')).toBe('idle');
		expect(Boolean((((next.nodes as any[])[0]?.data ?? {})?.meta ?? {}).llmAllocated)).toBe(false);
	});

	it('run_resume_failed leaves run in paused state', () => {
		const state = makeState('resuming');
		const next = __applyRunEventForTest(
			state,
			{
				type: 'run_resume_failed',
				runId: 'run-pause',
				at: '2026-03-30T00:00:04Z',
				errorCode: 'RESUME_FRONTIER_VALIDATION_FAILED'
			} as any,
			'run-pause'
		);
		expect(next.runStatus).toBe('paused');
	});

	it('non-work edge_exec active remains ignored during pause lifecycle', () => {
		let state = makeState('running');
		state = __applyRunEventForTest(
			state,
			{ type: 'run_pausing', runId: 'run-pause', at: '2026-03-30T00:00:05Z' } as any,
			'run-pause'
		);
		const next = __applyRunEventForTest(
			state,
			{ type: 'edge_exec', runId: 'run-pause', at: '2026-03-30T00:00:06Z', edgeId: 'e_param', exec: 'active' } as KnownRunEvent,
			'run-pause'
		);
		const paramEdge = (next.edges as any[]).find((e) => String(e.id) === 'e_param');
		expect(String(paramEdge?.data?.exec ?? '')).toBe('idle');
	});

	it('test_paused_has_no_running_nodes', () => {
		const state = makeState('running');
		const next = __applyRunEventForTest(
			state,
			{ type: 'run_paused', runId: 'run-pause', at: '2026-03-30T00:00:01Z' } as any,
			'run-pause'
		);
		expect(next.runStatus).toBe('paused');
		expect(Boolean((((next.nodes as any[])[0]?.data ?? {})?.meta ?? {}).llmAllocated)).toBe(false);
	});

	it('test_paused_has_no_active_edges', () => {
		const state = makeState('running');
		const next = __applyRunEventForTest(
			state,
			{ type: 'run_paused', runId: 'run-pause', at: '2026-03-30T00:00:01Z' } as any,
			'run-pause'
		);
		for (const edge of next.edges as any[]) {
			expect(String((edge?.data ?? {}).exec ?? 'idle')).not.toBe('active');
		}
	});

	it('test_paused_has_no_running_star', () => {
		const state = makeState('running');
		const next = __applyRunEventForTest(
			state,
			{ type: 'run_paused', runId: 'run-pause', at: '2026-03-30T00:00:01Z' } as any,
			'run-pause'
		);
		expect(Boolean((((next.nodes as any[])[0]?.data ?? {})?.meta ?? {}).llmAllocated)).toBe(false);
	});

	it('test_pausing_allows_only_existing_work', () => {
		let state = makeState('running');
		state = __applyRunEventForTest(
			state,
			{ type: 'run_pausing', runId: 'run-pause', at: '2026-03-30T00:00:05Z' } as any,
			'run-pause'
		);
		const next = __applyRunEventForTest(
			state,
			{ type: 'edge_exec', runId: 'run-pause', at: '2026-03-30T00:00:06Z', edgeId: 'e_work', exec: 'active' } as any,
			'run-pause'
		);
		expect(next.runStatus).toBe('pausing');
		const workEdge = (next.edges as any[]).find((e) => String(e.id) === 'e_work');
		expect(String(workEdge?.data?.exec ?? '')).toBe('active');
	});

	it('test_resuming_has_no_speculative_running', () => {
		let state = makeState('running');
		state = __applyRunEventForTest(
			state,
			{ type: 'run_paused', runId: 'run-pause', at: '2026-03-30T00:00:01Z' } as any,
			'run-pause'
		);
		const next = __applyRunEventForTest(
			state,
			{ type: 'run_resuming', runId: 'run-pause', at: '2026-03-30T00:00:02Z' } as any,
			'run-pause'
		);
		expect(next.runStatus).toBe('resuming');
		for (const edge of next.edges as any[]) {
			expect(String((edge?.data ?? {}).exec ?? 'idle')).not.toBe('active');
		}
		expect(Boolean((((next.nodes as any[])[0]?.data ?? {})?.meta ?? {}).llmAllocated)).toBe(false);
	});

	it('emits pause/resume trace log for explicit pause request event', () => {
		__setPauseResumeTraceEnabledForTest(true);
		const state = makeState('running');
		const next = __applyRunEventForTest(
			state,
			{ type: 'run_pause_requested', runId: 'run-pause', at: '2026-04-09T00:00:00Z' } as any,
			'run-pause'
		);
		__setPauseResumeTraceEnabledForTest(false);
		const hasTrace = (next.logs ?? []).some((entry: any) =>
			String(entry?.message ?? '').includes('[trace][pause-resume] evt=run_pause_requested')
		);
		expect(hasTrace).toBe(true);
	});

	it('emits pause/resume trace logs for non-control events while pausing', () => {
		__setPauseResumeTraceEnabledForTest(true);
		const state: GraphState = {
			...makeState('pausing'),
			nodeBindings: {
				n1: {
					graphId: 'graph-pause',
					status: 'running',
					lastArtifactId: null,
					lastRunId: null,
					lastExecKey: null,
					currentExecKey: null,
					currentArtifactId: null,
					currentRunId: null,
					isUpToDate: false,
					cacheValid: false,
					staleReason: null
				} as any
			}
		};
		const next = __applyRunEventForTest(
			state,
			{ type: 'node_finished', runId: 'run-pause', nodeId: 'n1', status: 'succeeded', at: '2026-04-09T00:00:01Z' } as any,
			'run-pause'
		);
		__setPauseResumeTraceEnabledForTest(false);
		const hasTrace = (next.logs ?? []).some((entry: any) =>
			String(entry?.message ?? '').includes('[trace][pause-resume] evt=node_finished')
		);
		expect(hasTrace).toBe(true);
	});

	it('run_paused snapshot preserves component parent boundary binding lineage', () => {
		const state: GraphState = {
			...makeState('running'),
			nodes: [
				{ id: 'n_component', data: { kind: 'component', meta: { llmAllocated: true } } },
				{ id: 'n_down', data: { kind: 'model', meta: { llmAllocated: false } } }
			] as any,
			edges: [
				{
					id: 'e_component_down',
					source: 'n_component',
					sourceHandle: 'summary',
					target: 'n_down',
					targetHandle: 'in',
					data: { mode: 'work', exec: 'active' }
				}
			] as any,
			nodeBindings: {
				n_component: {
					graphId: 'graph-pause',
					status: 'idle',
					lastArtifactId: null,
					lastRunId: null,
					lastExecKey: null,
					currentExecKey: null,
					currentArtifactId: null,
					currentRunId: null,
					isUpToDate: false,
					cacheValid: false,
					staleReason: null
				} as any
			}
		};
		const next = __applyRunEventForTest(
			state,
			{
				type: 'run_paused',
				runId: 'run-pause',
				at: '2026-04-08T22:45:58Z',
				snapshot: {
					frontierValidationBasis: {
						nodes: {
							n_component: {
								binding: {
									currentExecKey: 'exec-component',
									currentArtifactId: 'art-component'
								},
								upstreamBindings: {}
							},
							n_down: {
								binding: { currentExecKey: '', currentArtifactId: '' },
								upstreamBindings: {
									n_component: {
										currentExecKey: 'exec-component',
										currentArtifactId: 'art-component'
									}
								}
							}
						}
					}
				}
			} as any,
			'run-pause'
		);
		expect(next.runStatus).toBe('paused');
		expect(next.nodeBindings.n_component?.currentExecKey).toBe('exec-component');
		expect(next.nodeBindings.n_component?.currentArtifactId).toBe('art-component');
		expect(next.nodeBindings.n_component?.lastExecKey).toBe('exec-component');
		expect(next.nodeBindings.n_component?.lastArtifactId).toBe('art-component');
		expect(next.nodeBindings.n_component?.status).toBe('succeeded_up_to_date');
		expect(next.nodeBindings.n_component?.isUpToDate).toBe(true);
		expect(next.nodeBindings.n_component?.cacheValid).toBe(true);
		expect(displayStatusFromBinding(next.nodeBindings.n_component as any)).toBe('succeeded');
	});

	it('run_paused snapshot clears stale freshness for just-completed component boundary binding', () => {
		const state: GraphState = {
			...makeState('running'),
			nodes: [{ id: 'n_component', data: { kind: 'component', meta: {} } }] as any,
			nodeBindings: {
				n_component: {
					graphId: 'graph-pause',
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
			}
		};
		const next = __applyRunEventForTest(
			state,
			{
				type: 'run_paused',
				runId: 'run-pause',
				at: '2026-04-08T23:00:00Z',
				snapshot: {
					frontierValidationBasis: {
						nodes: {
							n_component: {
								binding: {
									currentExecKey: 'exec-new',
									currentArtifactId: 'art-new'
								},
								upstreamBindings: {}
							}
						}
					}
				}
			} as any,
			'run-pause'
		);
		expect(next.nodeBindings.n_component?.status).toBe('succeeded_up_to_date');
		expect(next.nodeBindings.n_component?.currentExecKey).toBe('exec-new');
		expect(next.nodeBindings.n_component?.lastExecKey).toBe('exec-new');
		expect(next.nodeBindings.n_component?.currentArtifactId).toBe('art-new');
		expect(next.nodeBindings.n_component?.lastArtifactId).toBe('art-new');
		expect(next.nodeBindings.n_component?.isUpToDate).toBe(true);
		expect(next.nodeBindings.n_component?.staleReason).toBeNull();
		expect(displayStatusFromBinding(next.nodeBindings.n_component as any)).toBe('succeeded');
	});

	it('run_finished(succeeded) clears stale freshness for component parent with artifact lineage', () => {
		const state: GraphState = {
			...makeState('running'),
			nodes: [{ id: 'n_component', data: { kind: 'component', meta: {} } }] as any,
			nodeBindings: {
				n_component: {
					graphId: 'graph-pause',
					status: 'stale',
					lastArtifactId: 'art-component',
					lastRunId: 'run-prev',
					lastExecKey: 'exec-component',
					currentExecKey: 'exec-component',
					currentArtifactId: 'art-component',
					currentRunId: 'run-pause',
					isUpToDate: false,
					cacheValid: false,
					staleReason: 'RUN_PENDING'
				} as any
			}
		};
		const next = __applyRunEventForTest(
			state,
			{ type: 'run_finished', runId: 'run-pause', at: '2026-04-08T23:20:00Z', status: 'succeeded' } as any,
			'run-pause'
		);
		expect(next.nodeBindings.n_component?.status).toBe('succeeded_up_to_date');
		expect(next.nodeBindings.n_component?.isUpToDate).toBe(true);
		expect(next.nodeBindings.n_component?.cacheValid).toBe(true);
		expect(next.nodeBindings.n_component?.staleReason).toBeNull();
		expect(displayStatusFromBinding(next.nodeBindings.n_component as any)).toBe('succeeded');
	});

	it('run_finished(succeeded) normalizes component parent lineage pair and clears exec drift', () => {
		const state: GraphState = {
			...makeState('running'),
			nodes: [{ id: 'n_component', data: { kind: 'component', meta: {} } }] as any,
			nodeBindings: {
				n_component: {
					graphId: 'graph-pause',
					status: 'succeeded_up_to_date',
					lastArtifactId: 'art-old',
					lastRunId: 'run-prev',
					lastExecKey: 'exec-old',
					currentExecKey: 'exec-new',
					currentArtifactId: 'art-new',
					currentRunId: 'run-pause',
					isUpToDate: true,
					cacheValid: true,
					staleReason: null
				} as any
			}
		};
		const next = __applyRunEventForTest(
			state,
			{ type: 'run_finished', runId: 'run-pause', at: '2026-04-08T23:23:00Z', status: 'succeeded' } as any,
			'run-pause'
		);
		expect(next.nodeBindings.n_component?.currentExecKey).toBe('exec-new');
		expect(next.nodeBindings.n_component?.currentArtifactId).toBe('art-new');
		expect(next.nodeBindings.n_component?.lastExecKey).toBe('exec-new');
		expect(next.nodeBindings.n_component?.lastArtifactId).toBe('art-new');
		expect(displayStatusFromBinding(next.nodeBindings.n_component as any)).toBe('succeeded');
	});

	it('run_finished(succeeded) does not overwrite failed component status', () => {
		const state: GraphState = {
			...makeState('running'),
			nodes: [{ id: 'n_component', data: { kind: 'component', meta: {} } }] as any,
			nodeBindings: {
				n_component: {
					graphId: 'graph-pause',
					status: 'failed',
					lastArtifactId: 'art-component',
					lastRunId: 'run-prev',
					lastExecKey: 'exec-component',
					currentExecKey: 'exec-component',
					currentArtifactId: 'art-component',
					currentRunId: 'run-pause',
					isUpToDate: false,
					cacheValid: false,
					staleReason: 'COMPONENT_FAILED'
				} as any
			}
		};
		const next = __applyRunEventForTest(
			state,
			{ type: 'run_finished', runId: 'run-pause', at: '2026-04-08T23:25:00Z', status: 'succeeded' } as any,
			'run-pause'
		);
		expect(next.nodeBindings.n_component?.status).toBe('failed');
	});

	it('snapshot hydrate does not downgrade fresh completed component to stale/idle when incoming snapshot lacks current pair', () => {
		const state: GraphState = {
			...makeState('paused'),
			nodes: [{ id: 'n_component', data: { kind: 'component', meta: {} } }] as any,
			nodeBindings: {
				n_component: {
					graphId: 'graph-pause',
					status: 'succeeded_up_to_date',
					lastArtifactId: 'art-component',
					lastRunId: 'run-pause',
					lastExecKey: 'exec-component',
					currentExecKey: 'exec-component',
					currentArtifactId: 'art-component',
					currentRunId: 'run-pause',
					isUpToDate: true,
					cacheValid: true,
					staleReason: null
				} as any
			}
		};
		const next = __hydrateFromRunSnapshotForTest(state, {
			status: 'paused',
			nodeBindings: {
				n_component: {
					status: 'stale',
					isUpToDate: false,
					cacheValid: false,
					currentExecKey: null,
					currentArtifactId: null,
					staleReason: 'RUN_PENDING'
				}
			}
		} as any);
		expect(next.nodeBindings.n_component?.status).toBe('succeeded_up_to_date');
		expect(next.nodeBindings.n_component?.isUpToDate).toBe(true);
		expect(next.nodeBindings.n_component?.currentExecKey).toBe('exec-component');
		expect(next.nodeBindings.n_component?.currentArtifactId).toBe('art-component');
		expect(displayStatusFromBinding(next.nodeBindings.n_component as any)).toBe('succeeded');
	});

	it('snapshot hydrate preserves completed component from last lineage when current lineage is missing', () => {
		const state: GraphState = {
			...makeState('paused'),
			nodes: [{ id: 'n_component', data: { kind: 'component', meta: {} } }] as any,
			nodeBindings: {
				n_component: {
					graphId: 'graph-pause',
					status: 'succeeded_up_to_date',
					lastArtifactId: 'art-last',
					lastRunId: 'run-prev',
					lastExecKey: 'exec-last',
					currentExecKey: null,
					currentArtifactId: null,
					currentRunId: null,
					isUpToDate: true,
					cacheValid: true,
					staleReason: null
				} as any
			}
		};
		const next = __hydrateFromRunSnapshotForTest(state, {
			status: 'paused',
			nodeBindings: {
				n_component: {
					status: 'idle',
					isUpToDate: false,
					cacheValid: false,
					currentExecKey: null,
					currentArtifactId: null,
					staleReason: null
				}
			}
		} as any);
		expect(next.nodeBindings.n_component?.status).toBe('succeeded_up_to_date');
		expect(next.nodeBindings.n_component?.currentExecKey).toBe('exec-last');
		expect(next.nodeBindings.n_component?.currentArtifactId).toBe('art-last');
		expect(next.nodeBindings.n_component?.lastExecKey).toBe('exec-last');
		expect(next.nodeBindings.n_component?.lastArtifactId).toBe('art-last');
		expect(displayStatusFromBinding(next.nodeBindings.n_component as any)).toBe('succeeded');
	});

	it('snapshot hydrate does not downgrade completed component on RUN_PENDING stale payload even when current pair is present', () => {
		const state: GraphState = {
			...makeState('running'),
			nodes: [{ id: 'n_component', data: { kind: 'component', meta: {} } }] as any,
			nodeBindings: {
				n_component: {
					graphId: 'graph-pause',
					status: 'succeeded_up_to_date',
					lastArtifactId: 'art-component',
					lastRunId: 'run-prev',
					lastExecKey: 'exec-component',
					currentExecKey: 'exec-component',
					currentArtifactId: 'art-component',
					currentRunId: 'run-prev',
					isUpToDate: true,
					cacheValid: true,
					staleReason: null
				} as any
			}
		};
		const next = __hydrateFromRunSnapshotForTest(state, {
			status: 'running',
			nodeBindings: {
				n_component: {
					status: 'stale',
					isUpToDate: false,
					cacheValid: false,
					currentExecKey: 'exec-component',
					currentArtifactId: 'art-component',
					lastExecKey: 'exec-component',
					lastArtifactId: 'art-component',
					staleReason: 'RUN_PENDING'
				}
			}
		} as any);
		expect(next.nodeBindings.n_component?.status).toBe('succeeded_up_to_date');
		expect(next.nodeBindings.n_component?.isUpToDate).toBe(true);
		expect(next.nodeBindings.n_component?.staleReason).toBeNull();
		expect(displayStatusFromBinding(next.nodeBindings.n_component as any)).toBe('succeeded');
	});

	it('snapshot hydrate does not downgrade completed component on stale payload with empty staleReason', () => {
		const state: GraphState = {
			...makeState('paused'),
			nodes: [{ id: 'n_component', data: { kind: 'component', meta: {} } }] as any,
			nodeBindings: {
				n_component: {
					graphId: 'graph-pause',
					status: 'succeeded_up_to_date',
					lastArtifactId: 'art-component',
					lastRunId: 'run-prev',
					lastExecKey: 'exec-component',
					currentExecKey: 'exec-component',
					currentArtifactId: 'art-component',
					currentRunId: 'run-prev',
					isUpToDate: true,
					cacheValid: true,
					staleReason: null
				} as any
			}
		};
		const next = __hydrateFromRunSnapshotForTest(state, {
			status: 'paused',
			nodeBindings: {
				n_component: {
					status: 'stale',
					isUpToDate: false,
					cacheValid: false,
					currentExecKey: 'exec-component',
					currentArtifactId: 'art-component',
					lastExecKey: 'exec-component',
					lastArtifactId: 'art-component',
					staleReason: null
				}
			}
		} as any);
		expect(next.nodeBindings.n_component?.status).toBe('succeeded_up_to_date');
		expect(next.nodeBindings.n_component?.isUpToDate).toBe(true);
		expect(next.nodeBindings.n_component?.staleReason).toBeNull();
		expect(displayStatusFromBinding(next.nodeBindings.n_component as any)).toBe('succeeded');
	});

	it('snapshot hydrate does not downgrade completed component on idle payload with current lineage', () => {
		const state: GraphState = {
			...makeState('paused'),
			nodes: [{ id: 'n_component', data: { kind: 'component', meta: {} } }] as any,
			nodeBindings: {
				n_component: {
					graphId: 'graph-pause',
					status: 'succeeded_up_to_date',
					lastArtifactId: 'art-component',
					lastRunId: 'run-prev',
					lastExecKey: 'exec-component',
					currentExecKey: 'exec-component',
					currentArtifactId: 'art-component',
					currentRunId: 'run-prev',
					isUpToDate: true,
					cacheValid: true,
					staleReason: null
				} as any
			}
		};
		const next = __hydrateFromRunSnapshotForTest(state, {
			status: 'paused',
			nodeBindings: {
				n_component: {
					status: 'idle',
					isUpToDate: false,
					cacheValid: false,
					currentExecKey: 'exec-component',
					currentArtifactId: 'art-component',
					lastExecKey: 'exec-component',
					lastArtifactId: 'art-component',
					staleReason: null
				}
			}
		} as any);
		expect(next.nodeBindings.n_component?.status).toBe('succeeded_up_to_date');
		expect(next.nodeBindings.n_component?.isUpToDate).toBe(true);
		expect(next.nodeBindings.n_component?.staleReason).toBeNull();
		expect(displayStatusFromBinding(next.nodeBindings.n_component as any)).toBe('succeeded');
	});

	it('snapshot hydrate preserves completed component when incoming stale reason is explicit but lineage pair is unchanged', () => {
		const state: GraphState = {
			...makeState('paused'),
			nodes: [{ id: 'n_component', data: { kind: 'component', meta: {} } }] as any,
			nodeBindings: {
				n_component: {
					graphId: 'graph-pause',
					status: 'succeeded_up_to_date',
					lastArtifactId: 'art-component',
					lastRunId: 'run-prev',
					lastExecKey: 'exec-component',
					currentExecKey: 'exec-component',
					currentArtifactId: 'art-component',
					currentRunId: 'run-prev',
					isUpToDate: true,
					cacheValid: true,
					staleReason: null
				} as any
			}
		};
		const next = __hydrateFromRunSnapshotForTest(state, {
			status: 'paused',
			nodeBindings: {
				n_component: {
					status: 'stale',
					isUpToDate: false,
					cacheValid: false,
					currentExecKey: 'exec-component',
					currentArtifactId: 'art-component',
					lastExecKey: 'exec-component',
					lastArtifactId: 'art-component',
					staleReason: 'UPSTREAM_CHANGED'
				}
			}
		} as any);
		expect(next.nodeBindings.n_component?.status).toBe('succeeded_up_to_date');
		expect(next.nodeBindings.n_component?.isUpToDate).toBe(true);
		expect(next.nodeBindings.n_component?.staleReason).toBeNull();
		expect(displayStatusFromBinding(next.nodeBindings.n_component as any)).toBe('succeeded');
	});

	it('snapshot hydrate keeps active-run fresh binding when incoming stale snapshot is weaker', () => {
		const state: GraphState = {
			...makeState('pausing'),
			activeRunId: 'run-pause',
			nodes: [{ id: 'n1', data: { kind: 'model', meta: {} } }] as any,
			nodeBindings: {
				n1: {
					graphId: 'graph-pause',
					status: 'succeeded_up_to_date',
					lastArtifactId: 'art-n1',
					lastRunId: 'run-pause',
					lastExecKey: 'exec-n1',
					currentExecKey: 'exec-n1',
					currentArtifactId: 'art-n1',
					currentRunId: 'run-pause',
					isUpToDate: true,
					cacheValid: true,
					staleReason: null
				} as any
			}
		};
		const next = __hydrateFromRunSnapshotForTest(state, {
			status: 'paused',
			nodeBindings: {
				n1: {
					status: 'stale',
					isUpToDate: false,
					cacheValid: false,
					currentExecKey: 'exec-n1',
					currentArtifactId: 'art-n1',
					lastExecKey: 'exec-n1',
					lastArtifactId: 'art-n1',
					staleReason: 'UPSTREAM_CHANGED'
				}
			}
		} as any);
		expect(next.nodeBindings.n1?.status).toBe('succeeded_up_to_date');
		expect(next.nodeBindings.n1?.isUpToDate).toBe(true);
		expect(next.nodeBindings.n1?.staleReason).toBeNull();
		expect(displayStatusFromBinding(next.nodeBindings.n1 as any)).toBe('succeeded');
	});

});
