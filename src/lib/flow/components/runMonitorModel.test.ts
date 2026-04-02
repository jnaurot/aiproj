import { describe, expect, it } from 'vitest';

import {
	buildRunMonitorEdgeRows,
	buildRunMonitorNodeRows,
	edgeStatusesForFilter,
	filterRunMonitorEdgeRows,
	filterAndSortRunMonitorNodes,
	preferredMonitorEdgeFocusNodeId
} from './runMonitorModel';

describe('runMonitorModel', () => {
	it('builds node rows from scheduler, blocked, and llm lease telemetry', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [
				{
					id: 'n_model',
					position: { x: 0, y: 0 },
					data: { kind: 'model', label: 'Model_ScoreJob', params: {} }
				} as any,
				{
					id: 'n_letter',
					position: { x: 0, y: 0 },
					data: { kind: 'model', label: 'LetterBuilder', params: {} }
				} as any
			],
			edges: [
				{
					id: 'e_score',
					source: 'n_model',
					target: 'n_letter',
					targetHandle: 'in',
					data: {}
				} as any
			],
			nodeBindings: {
				n_model: { status: 'running' },
				n_letter: { status: 'stale' }
			},
			queueRuntime: {
				metrics: {
					edges: {
						'e_score:in': { depth: 4, blocked: false, full: false, oldestAgeSec: 13 }
					}
				},
				schedulerSnapshot: {
					perNode: [
						{
							nodeId: 'n_model',
							readyWork: true,
							inflight: 1,
							pendingInputCount: 0
						},
						{
							nodeId: 'n_letter',
							readyWork: false,
							inflight: 0,
							pendingInputCount: 2,
							lastBlockedReasonCode: 'WAITING_REQUIRED_PARAM'
						}
					]
				},
				blockedByNode: {
					n_letter: {
						nodeId: 'n_letter',
						reasonCode: 'WAITING_REQUIRED_PARAM',
						handle: 'param_context',
						plane: 'param'
					}
				},
				llmLease: {
					state: 'waiting',
					nodeId: 'n_letter',
					holderNodeId: 'n_model',
					waitQueueLength: 1,
					waitingNodeIds: ['n_letter']
				}
			}
		});

		const modelRow = rows.find((row) => row.nodeId === 'n_model');
		const letterRow = rows.find((row) => row.nodeId === 'n_letter');
		expect(modelRow?.isLlmHolder).toBe(true);
		expect(modelRow?.inboundDepth).toBe(0);
		expect(modelRow?.status).toBe('running');
		expect(modelRow?.lifecycle).toBe('running');
		expect(modelRow?.execution).toBe('running');
		expect(modelRow?.consumeMode).toBe('once');
		expect(letterRow?.isBlocked).toBe(true);
		expect(letterRow?.blockedReasonCode).toBe('WAITING_REQUIRED_PARAM');
		expect(letterRow?.isWaiting).toBe(true);
		expect(letterRow?.isLlmWaiting).toBe(true);
		expect(letterRow?.inboundDepth).toBe(4);
		expect(letterRow?.freshness).toBe('stale');
	});

	it('builds edge rows with queue metric details and labels', () => {
		const rows = buildRunMonitorEdgeRows({
			nodes: [
				{ id: 'n1', position: { x: 0, y: 0 }, data: { kind: 'model', label: 'Model A', params: {} } } as any,
				{ id: 'n2', position: { x: 0, y: 0 }, data: { kind: 'transform', label: 'JobDescription', params: {} } } as any
			],
			edges: [
				{
					id: 'e_1',
					source: 'n1',
					target: 'n2',
					targetHandle: 'in',
					data: {}
				} as any
			],
			queueRuntime: {
				metrics: {
					edges: {
						'e_1:in': { depth: 6, blocked: true, full: false, oldestAgeSec: 44.2 }
					}
				}
			}
		});
		expect(rows).toHaveLength(1);
		expect(rows[0]).toMatchObject({
			edgeId: 'e_1',
			sourceLabel: 'Model A',
			targetLabel: 'JobDescription',
			lifecycle: 'waiting',
			exec: 'idle',
			depth: 6,
			blocked: true,
			full: false
		});
		expect(rows[0]?.oldestAgeSec).toBeCloseTo(44.2);
	});

	it('filters and sorts rows deterministically', () => {
		const base = [
			{
				nodeId: 'a',
				label: 'A',
				status: 'stale',
				lifecycle: 'completed',
				execution: 'finished',
				freshness: 'stale',
				consumeMode: 'once',
				acceptedCount: 0,
				rejectedCount: 0,
				totalProcessed: 0,
				pendingInputCount: 2,
				inflight: 0,
				inboundDepth: 5,
				readyWork: false,
				blockedReasonCode: 'WAITING_REQUIRED_INPUT',
				blockedHandle: 'in',
				blockedPlane: 'work',
				updatedAt: null,
				isBlocked: true,
				isWaiting: true,
				isLlmHolder: false,
				isLlmWaiting: false
			},
			{
				nodeId: 'b',
				label: 'B',
				status: 'running',
				lifecycle: 'running',
				execution: 'running',
				freshness: 'unknown',
				consumeMode: 'single_item',
				acceptedCount: 2,
				rejectedCount: 1,
				totalProcessed: 3,
				pendingInputCount: 0,
				inflight: 1,
				inboundDepth: 1,
				readyWork: true,
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
				nodeId: 'c',
				label: 'C',
				status: 'stale',
				lifecycle: 'completed',
				execution: 'finished',
				freshness: 'stale',
				consumeMode: 'batch',
				acceptedCount: 4,
				rejectedCount: 2,
				totalProcessed: 6,
				pendingInputCount: 3,
				inflight: 0,
				inboundDepth: 3,
				readyWork: false,
				blockedReasonCode: null,
				blockedHandle: null,
				blockedPlane: null,
				updatedAt: null,
				isBlocked: false,
				isWaiting: true,
				isLlmHolder: false,
				isLlmWaiting: false
			}
		] as const;

		expect(filterAndSortRunMonitorNodes(base as any, 'blocked', 'label_asc', true).map((row) => row.nodeId)).toEqual([
			'a'
		]);
		expect(filterAndSortRunMonitorNodes(base as any, 'waiting', 'pending_desc', true).map((row) => row.nodeId)).toEqual([
			'c',
			'a'
		]);
		expect(filterAndSortRunMonitorNodes(base as any, 'stalled', 'depth_desc', false)).toHaveLength(0);
	});

	it('prefers edge target node for monitor focus', () => {
		expect(preferredMonitorEdgeFocusNodeId('n_source', 'n_target')).toBe('n_target');
		expect(preferredMonitorEdgeFocusNodeId('n_source', '')).toBe('n_source');
		expect(preferredMonitorEdgeFocusNodeId('', '')).toBe('');
	});

	it('marks active work edges as running lifecycle', () => {
		const rows = buildRunMonitorEdgeRows({
			nodes: [
				{ id: 'n1', position: { x: 0, y: 0 }, data: { kind: 'source', label: 'src', params: {} } } as any,
				{ id: 'n2', position: { x: 0, y: 0 }, data: { kind: 'transform', label: 'dst', params: {} } } as any
			],
			edges: [
				{
					id: 'e_active',
					source: 'n1',
					target: 'n2',
					targetHandle: 'in',
					data: { exec: 'active', mode: 'work' }
				} as any
			],
			queueRuntime: { metrics: { edges: { 'e_active:in': { depth: 0, blocked: false, full: false } } } }
		});
		expect(rows[0]?.lifecycle).toBe('running');
		expect(rows[0]?.exec).toBe('active');
	});

	it('preserves control-gate blocked reason in monitor rows', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [
				{
					id: 'n_sink',
					position: { x: 0, y: 0 },
					data: { kind: 'tool', label: 'Sink', params: {} }
				} as any
			],
			edges: [],
			nodeBindings: { n_sink: { status: 'stale' } },
			queueRuntime: {
				blockedByNode: {
					n_sink: {
						nodeId: 'n_sink',
						reasonCode: 'CONTROL_GATE_BLOCKED',
						handle: 'control_gate',
						plane: 'control'
					}
				}
			}
		});
		expect(rows[0]?.blockedReasonCode).toBe('CONTROL_GATE_BLOCKED');
		expect(rows[0]?.blockedPlane).toBe('control');
	});

	it('projects consume mode and processed counts from runtime metrics', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [
				{
					id: 'n_batch',
					position: { x: 0, y: 0 },
					data: {
						kind: 'transform',
						label: 'BatchNode',
						processingPolicy: { consume_mode: 'batch' },
						params: {}
					}
				} as any,
				{
					id: 'n_single',
					position: { x: 0, y: 0 },
					data: {
						kind: 'transform',
						label: 'SingleNode',
						processingPolicy: { consume_mode: 'single_item' },
						params: {}
					}
				} as any
			],
			edges: [],
			nodeBindings: {
				n_batch: { status: 'succeeded' },
				n_single: { status: 'succeeded' }
			},
			queueRuntime: {
				runScoped: {
					runtimeItemMetrics: {
						nodeCounters: {
							n_batch: { accepted: 5, rejected: 2 },
							n_single: { accepted: 3, rejected: 0 }
						}
					}
				}
			}
		});
		const batch = rows.find((row) => row.nodeId === 'n_batch');
		const single = rows.find((row) => row.nodeId === 'n_single');
		expect(batch?.consumeMode).toBe('batch');
		expect(batch?.acceptedCount).toBe(5);
		expect(batch?.rejectedCount).toBe(2);
		expect(batch?.totalProcessed).toBe(7);
		expect(single?.consumeMode).toBe('single_item');
		expect(single?.acceptedCount).toBe(3);
		expect(single?.rejectedCount).toBe(0);
		expect(single?.totalProcessed).toBe(3);
	});

	it('does not mark single-item nodes completed while run is active', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [
				{
					id: 'n_single',
					position: { x: 0, y: 0 },
					data: {
						kind: 'model',
						label: 'ResumeBuilder',
						processingPolicy: { consume_mode: 'single_item' },
						params: {}
					}
				} as any
			],
			edges: [],
			nodeBindings: {
				n_single: { status: 'succeeded' }
			},
			queueRuntime: {
				schedulerSnapshot: {
					perNode: [{ nodeId: 'n_single', readyWork: false, inflight: 0, pendingInputCount: 0 }]
				}
			},
			runStatus: 'running'
		});
		expect(rows[0]?.consumeMode).toBe('single_item');
		expect(rows[0]?.lifecycle).toBe('waiting');
	});

	it('maps edge lifecycle to filter statuses with active alias compatibility', () => {
		expect(
			edgeStatusesForFilter({
				edgeId: 'e1',
				handle: 'in',
				sourceNodeId: 'a',
				sourceLabel: 'A',
				targetNodeId: 'b',
				targetLabel: 'B',
				lifecycle: 'running',
				exec: 'active',
				depth: 0,
				blocked: false,
				full: false,
				oldestAgeSec: null
			})
		).toEqual(['running', 'active']);
		expect(
			edgeStatusesForFilter({
				edgeId: 'e2',
				handle: 'in',
				sourceNodeId: 'a',
				sourceLabel: 'A',
				targetNodeId: 'b',
				targetLabel: 'B',
				lifecycle: 'done',
				exec: 'done',
				depth: 0,
				blocked: true,
				full: true,
				oldestAgeSec: 1.2
			})
		).toEqual(['done', 'blocked', 'full']);
	});

	it('filters edge rows by lifecycle and diagnostics statuses', () => {
		const rows = [
			{
				edgeId: 'e_inactive',
				handle: 'in',
				sourceNodeId: 'a',
				sourceLabel: 'A',
				targetNodeId: 'b',
				targetLabel: 'B',
				lifecycle: 'inactive',
				exec: 'idle',
				depth: 0,
				blocked: false,
				full: false,
				oldestAgeSec: null
			},
			{
				edgeId: 'e_waiting',
				handle: 'in',
				sourceNodeId: 'b',
				sourceLabel: 'B',
				targetNodeId: 'c',
				targetLabel: 'C',
				lifecycle: 'waiting',
				exec: 'idle',
				depth: 4,
				blocked: false,
				full: false,
				oldestAgeSec: 3.2
			},
			{
				edgeId: 'e_running',
				handle: 'in',
				sourceNodeId: 'c',
				sourceLabel: 'C',
				targetNodeId: 'd',
				targetLabel: 'D',
				lifecycle: 'running',
				exec: 'active',
				depth: 0,
				blocked: false,
				full: false,
				oldestAgeSec: 0.2
			},
			{
				edgeId: 'e_done',
				handle: 'in',
				sourceNodeId: 'd',
				sourceLabel: 'D',
				targetNodeId: 'e',
				targetLabel: 'E',
				lifecycle: 'done',
				exec: 'done',
				depth: 0,
				blocked: true,
				full: false,
				oldestAgeSec: 0.1
			}
		] as const;
		expect(filterRunMonitorEdgeRows(rows as any, ['inactive']).map((row) => row.edgeId)).toEqual(['e_inactive']);
		expect(filterRunMonitorEdgeRows(rows as any, ['active']).map((row) => row.edgeId)).toEqual(['e_running']);
		expect(filterRunMonitorEdgeRows(rows as any, ['running']).map((row) => row.edgeId)).toEqual(['e_running']);
		expect(filterRunMonitorEdgeRows(rows as any, ['done']).map((row) => row.edgeId)).toEqual(['e_done']);
		expect(filterRunMonitorEdgeRows(rows as any, ['blocked']).map((row) => row.edgeId)).toEqual(['e_done']);
	});
});
