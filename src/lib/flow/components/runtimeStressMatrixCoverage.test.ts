import { describe, expect, it } from 'vitest';

import { buildRunAdvisory } from './runAdvisor';
import { buildRunMonitorNodeRows, groupMonitorNodeRows, type RunMonitorNodeRow } from './runMonitorModel';

function makeNode(id: string, kind: string, label: string) {
	return {
		id,
		type: kind,
		position: { x: 0, y: 0 },
		data: { kind, label, params: {} }
	} as any;
}

function advisoryRow(overrides: Partial<RunMonitorNodeRow>): RunMonitorNodeRow {
	return {
		nodeId: 'n',
		label: 'Node',
		status: 'busy',
		lifecycle: 'waiting',
		execution: 'finished',
		freshness: 'unknown',
		consumeMode: 'single_item',
		acceptedCount: 0,
		rejectedCount: 0,
		totalProcessed: 0,
		pendingInputCount: 0,
		inflight: 0,
		inboundDepth: 0,
		readyWork: false,
		blockedReasonCode: 'WAITING_REQUIRED_INPUT',
		blockedHandle: 'in',
		blockedPlane: 'work',
		updatedAt: null,
		terminalReasonCode: null,
		isBlocked: true,
		isWaiting: true,
		isLlmHolder: false,
		isLlmWaiting: false,
		phase: 'AWAITING_INPUT',
		phaseSince: null,
		blocker: null,
		lastBlocker: null,
		blockerHistory: [],
		displayReason: 'WAITING_REQUIRED_INPUT',
		statusParityMismatch: false,
		...overrides
	};
}

describe('runtime stress matrix coverage', () => {
	it('S1 baseline success: all successful nodes project into done group', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [makeNode('n1', 'source', 'Source'), makeNode('n2', 'transform', 'Transform')],
			edges: [{ id: 'e1', source: 'n1', target: 'n2', data: { mode: 'work' } }] as any,
			nodeBindings: {
				n1: { status: 'succeeded_up_to_date' },
				n2: { status: 'succeeded_up_to_date' }
			} as any,
			queueRuntime: { schedulerSnapshot: { perNode: [] } } as any,
			runStatus: 'succeeded'
		});
		const grouped = groupMonitorNodeRows(rows, 'all', 'depth_desc', false);
		expect(grouped.groups[grouped.activeGroupIndex]?.totalCount ?? -1).toBe(0);
		expect(grouped.groups[grouped.waitingGroupIndex]?.totalCount ?? -1).toBe(0);
		expect(grouped.groups[grouped.doneGroupIndex]?.totalCount ?? -1).toBe(2);
	});

	it('S7 single-lease contention: lease holder is projected as active holder', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [makeNode('n1', 'model', 'Model_A'), makeNode('n2', 'model', 'Model_B')],
			edges: [] as any,
			nodeBindings: {
				n1: { status: 'running' },
				n2: { status: 'stale' }
			} as any,
			queueRuntime: {
				schedulerSnapshot: {
					perNode: [
						{ nodeId: 'n1', readyWork: true, inflight: 1, pendingInputCount: 0 },
						{ nodeId: 'n2', readyWork: false, inflight: 0, pendingInputCount: 1 }
					]
				},
				llmLease: {
					state: 'waiting',
					nodeId: 'n2',
					holderNodeId: 'n1',
					waitingNodeIds: ['n2'],
					waitQueueLength: 1
				}
			} as any,
			runStatus: 'running'
		});
		const holder = rows.find((row) => row.nodeId === 'n1');
		expect(holder?.lifecycle).toBe('running');
		expect(holder?.isLlmHolder).toBe(true);
	});

	it('S13 terminal waiting warning: emits only when immediate upstream work edges are all closed', () => {
		const warning = buildRunAdvisory({
			runStatus: 'running',
			rows: [advisoryRow({ nodeId: 'n_wait' })],
			logs: [
				'[status-parity-warning] node=n_wait upstream_work_total=1 upstream_work_closed=1 upstream_work_open=0 upstream_work_unknown=0'
			]
		});
		const infoOnly = buildRunAdvisory({
			runStatus: 'running',
			rows: [advisoryRow({ nodeId: 'n_wait' })],
			logs: [
				'[status-parity-info] node=n_wait upstream_work_total=1 upstream_work_closed=0 upstream_work_open=1 upstream_work_unknown=0'
			]
		});
		expect(warning.some((item) => item.ruleId === 'WAITING_WITHOUT_WORK')).toBe(true);
		expect(infoOnly.some((item) => item.ruleId === 'WAITING_WITHOUT_WORK')).toBe(false);
	});

	it('S14/S15 run-log UX hooks remain wired (filter + context overlay)', async () => {
		const { readFileSync } = await import('node:fs');
		const { resolve } = await import('node:path');
		const text = readFileSync(resolve(process.cwd(), 'src/lib/flow/FlowCanvas.svelte'), 'utf8');
		expect(text.includes('function applyRunLogSelectionFilter')).toBe(true);
		expect(text.includes('function maybeOpenRunLogContext')).toBe(true);
		expect(text.includes('runLogContextOverlayOpen')).toBe(true);
	});
});
