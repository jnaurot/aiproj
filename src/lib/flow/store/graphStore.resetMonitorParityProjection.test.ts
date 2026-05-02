import { describe, expect, it } from 'vitest';

import { __resetRunUiStateForTest, type GraphState } from './graphStore';
import { buildRunMonitorNodeRows, groupMonitorNodeRows } from '$lib/flow/components/runMonitorModel';

describe('graphStore reset monitor parity projection', () => {
	it('projects no active or waiting monitor rows after reset', () => {
		const state = {
			graphId: 'g-reset-monitor-projection',
			nodes: [
				{
					id: 'n1',
					type: 'model',
					position: { x: 0, y: 0 },
					data: { kind: 'model', label: 'Model', params: {} }
				},
				{
					id: 'n2',
					type: 'transform',
					position: { x: 120, y: 0 },
					data: { kind: 'transform', label: 'Transform', params: {} }
				}
			] as any,
			edges: [{ id: 'e1', source: 'n1', target: 'n2', data: { exec: 'active', mode: 'work' } }] as any,
			nodeBindings: {
				n1: { status: 'running' },
				n2: { status: 'stale' }
			} as any,
			queueRuntime: {
				schedulerSnapshot: {
					stalled: true,
					perNode: [
						{ nodeId: 'n1', readyWork: true, inflight: 1, pendingInputCount: 0 },
						{ nodeId: 'n2', readyWork: false, inflight: 0, pendingInputCount: 2, lastBlockedReasonCode: 'WAITING_REQUIRED_INPUT' }
					]
				},
				llmLease: { state: 'acquired', holderNodeId: 'n1', activeNodeIds: ['n1'] },
				blockedByNode: {
					n2: { nodeId: 'n2', reasonCode: 'WAITING_REQUIRED_INPUT', updatedAt: '2026-01-01T00:00:00.000Z' }
				}
			} as any,
			logs: [] as any,
			runStatus: 'running'
		} as unknown as GraphState;

		const next = __resetRunUiStateForTest(state);
		const rows = buildRunMonitorNodeRows({
			nodes: (next.nodes ?? []) as any,
			edges: (next.edges ?? []) as any,
			nodeBindings: (next.nodeBindings ?? {}) as any,
			queueRuntime: (next.queueRuntime ?? {}) as any,
			runStatus: (next.runStatus ?? 'idle') as any
		});
		const grouped = groupMonitorNodeRows(rows, 'all', 'depth_desc', false);

		expect(rows.every((row) => row.lifecycle === 'idle' || row.lifecycle === 'completed')).toBe(true);
		expect(grouped.groups[grouped.activeGroupIndex]?.totalCount ?? -1).toBe(0);
		expect(grouped.groups[grouped.waitingGroupIndex]?.totalCount ?? -1).toBe(0);
	});
});

