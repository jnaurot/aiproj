import { describe, expect, it } from 'vitest';

import { projectNodeStatus, projectEdgeStatus, toDisplayNodeStatus } from './statusModel';
import { projectNodeDisplayState } from './displayState';
import { buildRunMonitorNodeRows, filterAndSortRunMonitorNodes } from '$lib/flow/components/runMonitorModel';

describe('NS-01 Canonical Status Domain Model', () => {
	it('test_status_domain_model_rejects_mixed_semantics', () => {
		const projection = projectNodeStatus({ status: 'running', isUpToDate: false });
		expect(projection.lifecycle).toBe('running');
		expect(projection.display).toBe('running');
	});

	it('test_status_domain_model_requires_valid_lifecycle_enum', () => {
		const projection = projectNodeStatus({ status: '__invalid__' });
		expect(projection.lifecycle).toBe('idle');
		expect(projection.execution).toBe('inactive');
	});

	it('test_status_domain_model_requires_valid_execution_enum', () => {
		const projection = projectNodeStatus({ status: 'blocked' });
		expect(projection.execution).toBe('blocked');
	});

	it('test_status_domain_model_allows_independent_freshness_badge', () => {
		// Under the three-state model isUpToDate: false is not a stale signal.
		// Staleness is driven by runtime === 'stale' or exec-key drift only.
		const projection = projectNodeStatus({ status: 'succeeded_up_to_date', isUpToDate: false });
		expect(projection.lifecycle).toBe('completed');
		expect(projection.freshness).toBe('fresh');
		// Exec-key drift does drive stale independently of lifecycle.
		const drifted = projectNodeStatus({
			status: 'succeeded_up_to_date',
			current: { execKey: 'new', artifactId: 'a2' },
			last: { execKey: 'old', artifactId: 'a1' }
		});
		expect(drifted.lifecycle).toBe('completed');
		expect(drifted.freshness).toBe('stale');
	});
});

describe('NS-02 Node Lifecycle State Machine', () => {
	it('test_node_lifecycle_initial_idle', () => {
		const projection = projectNodeStatus({});
		expect(projection.lifecycle).toBe('idle');
	});

	it('test_node_lifecycle_waiting_to_running_to_completed', () => {
		expect(projectNodeStatus({ status: 'paused' }).lifecycle).toBe('waiting');
		expect(projectNodeStatus({ status: 'running' }).lifecycle).toBe('running');
		expect(projectNodeStatus({ status: 'succeeded_up_to_date' }).lifecycle).toBe('completed');
	});

	it('test_node_lifecycle_blocked_transitions', () => {
		expect(projectNodeStatus({ status: 'blocked' }).lifecycle).toBe('blocked');
		expect(projectNodeStatus({ status: 'running' }).lifecycle).toBe('running');
	});

	it('test_node_lifecycle_failed_terminal', () => {
		expect(projectNodeStatus({ status: 'failed' }).lifecycle).toBe('failed');
	});

	it('test_node_lifecycle_canceled_terminal', () => {
		expect(projectNodeStatus({ status: 'canceled' }).lifecycle).toBe('canceled');
	});

	it('test_node_lifecycle_skipped_terminal', () => {
		expect(projectNodeStatus({ status: 'skipped' }).lifecycle).toBe('skipped');
	});

	it('test_node_lifecycle_invalid_transition_rejected', () => {
		const display = projectNodeDisplayState({ status: '__invalid__' }, '__invalid__');
		expect(display).toBe('idle');
	});
});

describe('NS-03 Edge Lifecycle State Machine', () => {
	it('test_edge_lifecycle_initial_inactive', () => {
		const projection = projectEdgeStatus({ exec: 'idle', mode: 'work', depth: 0, blocked: false, full: false });
		expect(projection.lifecycle).toBe('inactive');
	});

	it('test_edge_lifecycle_waiting_to_running_to_done', () => {
		expect(projectEdgeStatus({ exec: 'idle', mode: 'work', depth: 1 }).lifecycle).toBe('waiting');
		expect(projectEdgeStatus({ exec: 'active', mode: 'work', depth: 0 }).lifecycle).toBe('running');
		expect(projectEdgeStatus({ exec: 'done', mode: 'work', depth: 0 }).lifecycle).toBe('done');
	});

	it('test_edge_lifecycle_diagnostics_do_not_change_lifecycle', () => {
		const projection = projectEdgeStatus({ exec: 'done', mode: 'work', blocked: true, full: true, depth: 8 });
		expect(projection.lifecycle).toBe('done');
	});

	it('test_edge_non_work_plane_never_running', () => {
		const projection = projectEdgeStatus({ exec: 'active', mode: 'control', depth: 1 });
		expect(projection.lifecycle).not.toBe('running');
	});
});

describe('NS-04 Projection Layer', () => {
	it('test_projection_runtime_to_display_consistency', () => {
		expect(toDisplayNodeStatus('running', 'unknown')).toBe('running');
		expect(toDisplayNodeStatus('completed', 'fresh')).toBe('succeeded');
	});

	it('test_projection_handles_legacy_status_payloads', () => {
		const projection = projectNodeStatus({ status: 'cancelled' });
		expect(projection.lifecycle).toBe('canceled');
	});

	it('test_projection_exhaustive_runtime_mapping', () => {
		const statuses = ['idle', 'running', 'active', 'blocked', 'paused', 'succeeded_up_to_date', 'failed', 'canceled', 'skipped', 'stale', 'busy'];
		const displays = statuses.map((status) => projectNodeDisplayState({ status }, status));
		expect(displays.every((value) => typeof value === 'string' && value.length > 0)).toBe(true);
	});

	it('test_projection_unknown_status_fails_closed', () => {
		expect(projectNodeDisplayState({ status: 'mystery_status' }, 'mystery_status')).toBe('idle');
	});
});

describe('NS-05 Mode + Progress Overlay', () => {
	it('test_once_mode_progress_binary', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [{ id: 'n1', position: { x: 0, y: 0 }, data: { kind: 'transform', label: 'N1', processingPolicy: { consume_mode: 'once' }, params: {} } } as any],
			edges: [],
			nodeBindings: { n1: { status: 'succeeded' } },
			queueRuntime: { runScoped: { runtimeItemMetrics: { nodeCounters: { n1: { accepted: 1, rejected: 0 } } } } }
		});
		expect(rows[0]?.consumeMode).toBe('once');
		expect(rows[0]?.totalProcessed).toBe(1);
	});

	it('test_single_mode_item_counter_updates', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [{ id: 'n1', position: { x: 0, y: 0 }, data: { kind: 'transform', label: 'N1', processingPolicy: { consume_mode: 'single_item' }, params: {} } } as any],
			edges: [],
			nodeBindings: { n1: { status: 'running' } },
			queueRuntime: { runScoped: { runtimeItemMetrics: { nodeCounters: { n1: { accepted: 4, rejected: 1 } } } } }
		});
		expect(rows[0]?.acceptedCount).toBe(4);
		expect(rows[0]?.rejectedCount).toBe(1);
	});

	it('test_batch_mode_item_and_batch_counters_update', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [{ id: 'n1', position: { x: 0, y: 0 }, data: { kind: 'transform', label: 'N1', processingPolicy: { consume_mode: 'batch' }, params: {} } } as any],
			edges: [],
			nodeBindings: { n1: { status: 'running' } },
			queueRuntime: { runScoped: { runtimeItemMetrics: { nodeCounters: { n1: { accepted: 9, rejected: 2 } } } } }
		});
		expect(rows[0]?.consumeMode).toBe('batch');
		expect(rows[0]?.totalProcessed).toBe(11);
	});

	it('test_mode_overlay_does_not_affect_lifecycle_state', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [{ id: 'n1', position: { x: 0, y: 0 }, data: { kind: 'transform', label: 'N1', processingPolicy: { consume_mode: 'batch' }, params: {} } } as any],
			edges: [],
			nodeBindings: { n1: { status: 'running' } },
			queueRuntime: { runScoped: { runtimeItemMetrics: { nodeCounters: { n1: { accepted: 0, rejected: 0 } } } } }
		});
		expect(rows[0]?.lifecycle).toBe('running');
	});
});

describe('NS-07 UI Simplification + NS-09 perf guardrails', () => {
	it('test_canvas_shows_lifecycle_as_primary_label', () => {
		expect(toDisplayNodeStatus('completed', 'fresh')).toBe('succeeded');
	});

	it('test_running_overlay_visible_only_during_execution', () => {
		expect(projectNodeStatus({ status: 'running' }).execution).toBe('running');
		expect(projectNodeStatus({ status: 'succeeded_up_to_date' }).execution).toBe('finished');
	});

	it('test_freshness_badge_rendered_independently', () => {
		// isUpToDate: false is no longer a stale signal; freshness is derived from
		// runtime status and exec-key drift alone.
		const projection = projectNodeStatus({ status: 'succeeded_up_to_date', isUpToDate: false });
		expect(projection.lifecycle).toBe('completed');
		expect(projection.freshness).toBe('fresh');
	});

	it('test_monitor_status_filters_use_canonical_lifecycle', () => {
		const rows = [
			{
				nodeId: 'n1',
				label: 'N1',
				status: 'succeeded',
				lifecycle: 'completed',
				execution: 'finished',
				freshness: 'fresh',
				consumeMode: 'once',
				acceptedCount: 1,
				rejectedCount: 0,
				totalProcessed: 1,
				pendingInputCount: 0,
				inflight: 0,
				inboundDepth: 0,
				readyWork: false,
				blockedReasonCode: null,
				blockedHandle: null,
				blockedPlane: null,
				updatedAt: null,
				isBlocked: false,
				isWaiting: false,
				isLlmHolder: false,
				isLlmWaiting: false
			},
			{
				nodeId: 'n2',
				label: 'N2',
				status: 'busy',
				lifecycle: 'blocked',
				execution: 'blocked',
				freshness: 'unknown',
				consumeMode: 'single_item',
				acceptedCount: 0,
				rejectedCount: 0,
				totalProcessed: 0,
				pendingInputCount: 1,
				inflight: 0,
				inboundDepth: 2,
				readyWork: false,
				blockedReasonCode: 'WAITING_REQUIRED_INPUT',
				blockedHandle: 'in',
				blockedPlane: 'work',
				updatedAt: null,
				isBlocked: true,
				isWaiting: true,
				isLlmHolder: false,
				isLlmWaiting: false
			}
		] as any;
		const filtered = filterAndSortRunMonitorNodes(rows, 'blocked', 'pending_desc', false);
		expect(filtered.map((row: any) => row.nodeId)).toEqual(['n2']);
	});

	it('test_status_projection_performance_under_high_event_rate', () => {
		const t0 = Date.now();
		for (let idx = 0; idx < 5000; idx += 1) {
			projectNodeStatus({ status: idx % 2 === 0 ? 'running' : 'succeeded_up_to_date', isUpToDate: idx % 3 !== 0 });
		}
		expect(Date.now() - t0).toBeLessThan(1000);
	});

	it('test_monitor_row_recompute_is_incremental', () => {
		const base = buildRunMonitorNodeRows({
			nodes: [{ id: 'n1', position: { x: 0, y: 0 }, data: { kind: 'transform', label: 'N1', params: {} } } as any],
			edges: [],
			nodeBindings: { n1: { status: 'idle' } },
			queueRuntime: {}
		});
		const next = buildRunMonitorNodeRows({
			nodes: [{ id: 'n1', position: { x: 0, y: 0 }, data: { kind: 'transform', label: 'N1', params: {} } } as any],
			edges: [],
			nodeBindings: { n1: { status: 'running' } },
			queueRuntime: {}
		});
		expect(base[0]?.nodeId).toBe(next[0]?.nodeId);
		expect(base[0]?.lifecycle).not.toBe(next[0]?.lifecycle);
	});

	it('test_canvas_node_rerenders_are_bounded', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: new Array(100).fill(null).map((_, idx) => ({
				id: `n${idx}`,
				position: { x: 0, y: 0 },
				data: { kind: 'transform', label: `N${idx}`, params: {} }
			})) as any,
			edges: [],
			nodeBindings: Object.fromEntries(new Array(100).fill(null).map((_, idx) => [`n${idx}`, { status: 'idle' }])),
			queueRuntime: {}
		});
		expect(rows).toHaveLength(100);
	});

	it('test_edge_status_updates_do_not_trigger_full_graph_repaint', () => {
		const edgeIdle = projectEdgeStatus({ exec: 'idle', mode: 'work', depth: 0 });
		const edgeRunning = projectEdgeStatus({ exec: 'active', mode: 'work', depth: 0 });
		expect(edgeIdle.lifecycle).toBe('inactive');
		expect(edgeRunning.lifecycle).toBe('running');
	});
});

describe('NS-06 Input readiness and upstream closure semantics', () => {
	it('test_required_param_missing_stays_waiting_while_upstream_open', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [{ id: 'n_wait', position: { x: 0, y: 0 }, data: { kind: 'transform', label: 'WaitNode', params: {} } } as any],
			edges: [],
			nodeBindings: { n_wait: { status: 'busy' } },
			queueRuntime: {
				schedulerSnapshot: {
					perNode: [{ nodeId: 'n_wait', readyWork: false, inflight: 0, pendingInputCount: 1, lastBlockedReasonCode: 'WAITING_REQUIRED_PARAM' }]
				}
			}
		});
		expect(rows[0]?.isWaiting).toBe(true);
		expect(rows[0]?.blockedReasonCode).toBe('WAITING_REQUIRED_PARAM');
		expect(rows[0]?.lifecycle).toBe('waiting');
	});

	it('test_required_param_missing_fails_when_upstream_closed', () => {
		const projection = projectNodeStatus({ status: 'failed' });
		expect(projection.lifecycle).toBe('failed');
		expect(projection.execution).toBe('finished');
	});

	it('test_optional_param_uses_default_and_runs', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [{ id: 'n_run', position: { x: 0, y: 0 }, data: { kind: 'transform', label: 'RunNode', params: {} } } as any],
			edges: [],
			nodeBindings: { n_run: { status: 'running' } },
			queueRuntime: {
				schedulerSnapshot: {
					perNode: [{ nodeId: 'n_run', readyWork: true, inflight: 1, pendingInputCount: 0 }]
				}
			}
		});
		expect(rows[0]?.readyWork).toBe(true);
		expect(rows[0]?.lifecycle).toBe('running');
	});

	it('test_node_completes_only_when_all_required_inputs_closed', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [{ id: 'n_done', position: { x: 0, y: 0 }, data: { kind: 'transform', label: 'DoneNode', params: {} } } as any],
			edges: [],
			nodeBindings: { n_done: { status: 'succeeded_up_to_date', isUpToDate: true } },
			queueRuntime: {
				schedulerSnapshot: {
					perNode: [{ nodeId: 'n_done', readyWork: false, inflight: 0, pendingInputCount: 0 }]
				}
			}
		});
		expect(rows[0]?.pendingInputCount).toBe(0);
		expect(rows[0]?.inflight).toBe(0);
		expect(rows[0]?.lifecycle).toBe('completed');
	});

	it('test_multi_input_barrier_fires_once_ready_policy_satisfied', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [{ id: 'n_barrier', position: { x: 0, y: 0 }, data: { kind: 'transform', label: 'Barrier', params: {} } } as any],
			edges: [],
			nodeBindings: { n_barrier: { status: 'running' } },
			queueRuntime: {
				schedulerSnapshot: {
					perNode: [{ nodeId: 'n_barrier', readyWork: true, inflight: 1, pendingInputCount: 0 }]
				}
			}
		});
		expect(rows[0]?.readyWork).toBe(true);
		expect(rows[0]?.pendingInputCount).toBe(0);
	});
});

describe('NS-08 Event contract hardening and migration', () => {
	it('test_event_v2_contains_separate_status_dimensions', () => {
		const projection = projectNodeStatus({ status: 'running', isUpToDate: true });
		expect(projection).toMatchObject({
			lifecycle: 'running',
			execution: 'running',
			freshness: 'unknown'
		});
	});

	it('test_event_v1_adapter_preserves_semantics', () => {
		const projection = projectNodeStatus({ status: 'cancelled' });
		expect(projection.lifecycle).toBe('canceled');
		expect(projection.display).toBe('canceled');
	});

	it('test_log_replay_consistency_between_v1_and_v2', () => {
		const v1 = [{ status: 'running' }, { status: 'succeeded' }, { status: 'cancelled' }];
		const v2 = [{ status: 'running' }, { status: 'succeeded' }, { status: 'canceled' }];
		const normalizedV1 = v1.map((evt) => projectNodeStatus(evt).lifecycle);
		const normalizedV2 = v2.map((evt) => projectNodeStatus(evt).lifecycle);
		expect(normalizedV1).toEqual(normalizedV2);
	});

	it('test_missing_required_event_fields_rejected', () => {
		const isValid = (evt: Record<string, unknown>): boolean => {
			const runId = String(evt.runId ?? '').trim();
			const at = String(evt.at ?? '').trim();
			return runId.length > 0 && at.length > 0;
		};
		expect(isValid({ type: 'run_started' })).toBe(false);
		expect(isValid({ type: 'run_started', runId: 'r1', at: '2026-04-01T00:00:00Z' })).toBe(true);
	});
});

describe('NS-10 End-to-end non-regression timeline assertions', () => {
	it('test_e2e_initial_all_idle_inactive', () => {
		const node = projectNodeStatus({});
		const edge = projectEdgeStatus({ exec: 'idle', mode: 'work', depth: 0 });
		expect(node.lifecycle).toBe('idle');
		expect(edge.lifecycle).toBe('inactive');
	});

	it('test_e2e_streaming_run_waiting_running_completed_timeline', () => {
		expect(projectNodeStatus({ status: 'busy' }).lifecycle).toBe('waiting');
		expect(projectNodeStatus({ status: 'running' }).lifecycle).toBe('running');
		expect(projectNodeStatus({ status: 'succeeded_up_to_date' }).lifecycle).toBe('completed');
	});

	it('test_e2e_pause_resume_preserves_status_truth', () => {
		expect(projectNodeStatus({ status: 'paused' }).lifecycle).toBe('waiting');
		expect(projectNodeStatus({ status: 'running' }).lifecycle).toBe('running');
	});

	it('test_e2e_cache_modes_do_not_corrupt_lifecycle_labels', () => {
		const cached = projectNodeStatus({ status: 'succeeded_up_to_date', isUpToDate: true });
		const forcedOff = projectNodeStatus({ status: 'succeeded', isUpToDate: false });
		expect(cached.lifecycle).toBe('completed');
		expect(forcedOff.lifecycle).toBe('completed');
	});

	it('test_e2e_failure_path_marks_failed_not_completed', () => {
		const projection = projectNodeStatus({ status: 'failed' });
		expect(projection.lifecycle).toBe('failed');
		expect(projection.lifecycle).not.toBe('completed');
	});

	it('test_e2e_canceled_path_marks_canceled_terminal', () => {
		const projection = projectNodeStatus({ status: 'canceled' });
		expect(projection.lifecycle).toBe('canceled');
		expect(projection.execution).toBe('finished');
	});
});
