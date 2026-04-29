import { describe, expect, it } from 'vitest';

import { buildRunMonitorNodeRows } from './runMonitorModel';

function node(id: string, label: string, kind = 'model') {
	return {
		id,
		position: { x: 0, y: 0 },
		data: { kind, label, params: {} }
	} as any;
}

describe('Monitor Phase/Blocker Contract - Phase 0 Baseline', () => {
	it('INT-MON-BASE-01 baseline: running lease-holder node has running lifecycle and no blocked code', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [node('n_model', 'Model_ScoreJob'), node('n_wait', 'ResumeBuilder')],
			edges: [],
			nodeBindings: {
				n_model: { status: 'running' },
				n_wait: { status: 'running' }
			},
			queueRuntime: {
				llmLease: {
					state: 'waiting',
					holderNodeId: 'n_model',
					nodeId: 'n_wait',
					waitingNodeIds: ['n_wait']
				},
				schedulerSnapshot: {
					perNode: [
						{ nodeId: 'n_model', readyWork: true, inflight: 1, pendingInputCount: 0 },
						{ nodeId: 'n_wait', readyWork: false, inflight: 0, pendingInputCount: 0, lastBlockedReasonCode: 'MAX_INFLIGHT_REACHED' }
					]
				}
			},
			runStatus: 'running'
		});
		const model = rows.find((row) => row.nodeId === 'n_model');
		expect(model?.lifecycle).toBe('running');
		expect(model?.blockedReasonCode).toBeNull();
	});

	it('INT-MON-BASE-02 baseline: waiting node exposes blocker reason code', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [node('n_wait', 'Join', 'transform')],
			edges: [],
			nodeBindings: { n_wait: { status: 'stale' } },
			queueRuntime: {
				blockedByNode: {
					n_wait: { nodeId: 'n_wait', reasonCode: 'WAITING_REQUIRED_INPUT', handle: 'in', plane: 'work' }
				}
			}
		});
		expect(rows[0]?.blockedReasonCode).toBe('WAITING_REQUIRED_INPUT');
		expect(rows[0]?.displayReason).toBe('WAITING_REQUIRED_INPUT');
	});

	it.fails('target: completed node should not retain transient inflight cap as current reason', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [node('n_done', 'ResumeBuilder')],
			edges: [],
			nodeBindings: { n_done: { status: 'succeeded' } },
			queueRuntime: {
				schedulerSnapshot: {
					perNode: [
						{ nodeId: 'n_done', readyWork: false, inflight: 0, pendingInputCount: 0, lastBlockedReasonCode: 'MAX_INFLIGHT_REACHED' }
					]
				}
			}
		});
		expect(rows[0]?.lifecycle).toBe('completed');
		expect(rows[0]?.displayReason).toBe('');
	});
});

describe('Monitor Phase/Blocker Contract - Phase 0 Target (Expected Fail)', () => {
	it('target: running node should expose phase and not overload reason as blocker', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [node('n_model', 'Model_ScoreJob')],
			edges: [],
			nodeBindings: { n_model: { status: 'running' } },
			queueRuntime: {
				llmLease: { state: 'acquired', holderNodeId: 'n_model', waitingNodeIds: [] }
			},
			runStatus: 'running'
		});
		expect((rows[0] as any).phase).toBe('AWAITING_PROVIDER_RESPONSE');
		expect((rows[0] as any).blocker).toBeNull();
	});

	it('target: waiting node should expose blocker object and legacy reason as derived fallback', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [node('n_wait', 'ResumeBuilder')],
			edges: [],
			nodeBindings: { n_wait: { status: 'running' } },
			queueRuntime: {
				schedulerSnapshot: {
					perNode: [
						{ nodeId: 'n_wait', readyWork: false, inflight: 0, pendingInputCount: 0, lastBlockedReasonCode: 'MAX_INFLIGHT_REACHED:model' }
					]
				}
			},
			runStatus: 'running'
		});
		expect((rows[0] as any).blocker).toMatchObject({ code: 'MAX_INFLIGHT_REACHED:model' });
		expect((rows[0] as any).displayReason).toBe('MAX_INFLIGHT_REACHED:model');
	});
});
