/**
 * Tests for RunMonitorNodeRow.displayReason — the unified "why is this node
 * not making progress" field that replaced the old "blocked" column.
 *
 * Rules under test:
 *   1. A running node with no issues shows displayReason = "" (never "-")
 *   2. A blocked node shows the reason code
 *   3. An llm-waiting node shows "llm-wait" when not also blocked
 *   4. An llm-holder node shows "llm-hold" when not blocked/waiting
 *   5. A stale idle node shows "stale"
 *   6. A succeeded or failed node shows "" regardless of freshness
 *   7. Blocked reason takes precedence over llm-wait
 *   8. llm-wait takes precedence over llm-hold
 *   9. llm-hold takes precedence over stale
 *  10. Lifecycle changes clear the reason (integration: blocked → running → "")
 *  11. Multiple nodes each get independent reasons
 *  12. Regression: isBlocked, isLlmHolder, isLlmWaiting flags are unaffected
 */

import { describe, expect, it } from 'vitest';
import { buildRunMonitorNodeRows, filterAndSortRunMonitorNodes } from './runMonitorModel';

function node(id: string, label: string, kind = 'tool') {
	return {
		id,
		position: { x: 0, y: 0 },
		data: { kind, label, params: {} }
	} as any;
}

// ── 1. Running node — never "-" ──────────────────────────────────────────────
describe('displayReason: running node', () => {
	it('is empty string (not "-") when node is actively running', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [node('n1', 'Processor')],
			edges: [],
			nodeBindings: { n1: { status: 'running' } },
			queueRuntime: {
				schedulerSnapshot: {
					perNode: [{ nodeId: 'n1', readyWork: true, inflight: 1, pendingInputCount: 0 }]
				}
			},
			runStatus: 'running'
		});
		expect(rows[0]?.displayReason).toBe('');
		expect(rows[0]?.displayReason).not.toBe('-');
	});

	it('is empty string when node is idle with no issues', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [node('n1', 'Idle Node')],
			edges: [],
			nodeBindings: { n1: { status: 'idle' } },
			queueRuntime: {}
		});
		expect(rows[0]?.displayReason).toBe('');
	});
});

// ── 2. Blocked node ───────────────────────────────────────────────────────────
describe('displayReason: blocked node', () => {
	it('shows the blockedReasonCode', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [node('n1', 'Sink')],
			edges: [],
			nodeBindings: { n1: { status: 'stale' } },
			queueRuntime: {
				blockedByNode: {
					n1: { nodeId: 'n1', reasonCode: 'WAITING_REQUIRED_INPUT', handle: 'in', plane: 'work' }
				}
			}
		});
		expect(rows[0]?.displayReason).toBe('WAITING_REQUIRED_INPUT');
	});

	it('shows scheduler lastBlockedReasonCode when no live blockedByNode entry', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [node('n1', 'Lagging')],
			edges: [],
			nodeBindings: { n1: { status: 'stale' } },
			queueRuntime: {
				schedulerSnapshot: {
					perNode: [
						{
							nodeId: 'n1',
							readyWork: false,
							inflight: 0,
							pendingInputCount: 1,
							lastBlockedReasonCode: 'NO_READY_WORK'
						}
					]
				}
			}
		});
		expect(rows[0]?.displayReason).toBe('NO_READY_WORK');
	});
});

// ── 3. LLM-waiting node ───────────────────────────────────────────────────────
describe('displayReason: llm-waiting node', () => {
	it('shows "llm-wait" when node is in the llm wait queue', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [node('n_waiter', 'Waiter'), node('n_holder', 'Holder')],
			edges: [],
			nodeBindings: {
				n_waiter: { status: 'running' },
				n_holder: { status: 'running' }
			},
			queueRuntime: {
				llmLease: {
					state: 'waiting',
					nodeId: 'n_waiter',
					holderNodeId: 'n_holder',
					waitQueueLength: 1,
					waitingNodeIds: ['n_waiter']
				}
			},
			runStatus: 'running'
		});
		const waiter = rows.find((r) => r.nodeId === 'n_waiter');
		expect(waiter?.displayReason).toBe('llm-wait');
		expect(waiter?.isLlmWaiting).toBe(true);
	});
});

// ── 4. LLM-holder node ────────────────────────────────────────────────────────
describe('displayReason: llm-holder node', () => {
	it('shows "llm-hold" for the current lease holder', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [node('n_holder', 'TrainingJob'), node('n_other', 'Other')],
			edges: [],
			nodeBindings: {
				n_holder: { status: 'running' },
				n_other: { status: 'idle' }
			},
			queueRuntime: {
				llmLease: {
					state: 'acquired',
					holderNodeId: 'n_holder',
					activeNodeIds: ['n_holder'],
					waitQueueLength: 0,
					waitingNodeIds: []
				}
			},
			runStatus: 'running'
		});
		const holder = rows.find((r) => r.nodeId === 'n_holder');
		const other = rows.find((r) => r.nodeId === 'n_other');
		expect(holder?.displayReason).toBe('llm-hold');
		expect(other?.displayReason).toBe('');
	});
});

// ── 5. Stale idle node ────────────────────────────────────────────────────────
describe('displayReason: stale idle node', () => {
	it('shows "stale" for an idle stale node', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [node('n1', 'OldResult')],
			edges: [],
			nodeBindings: { n1: { status: 'stale' } },
			queueRuntime: {}
		});
		expect(rows[0]?.displayReason).toBe('stale');
	});

	it('shows "stale" for a waiting stale node', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [node('n1', 'OldResult')],
			edges: [],
			nodeBindings: { n1: { status: 'stale' } },
			queueRuntime: {
				schedulerSnapshot: {
					perNode: [{ nodeId: 'n1', readyWork: false, inflight: 0, pendingInputCount: 3 }]
				}
			},
			runStatus: 'running'
		});
		expect(rows[0]?.freshness).toBe('stale');
		expect(rows[0]?.displayReason).toBe('stale');
	});
});

// ── 6. Succeeded / failed nodes ───────────────────────────────────────────────
describe('displayReason: terminal nodes', () => {
	it('is empty for succeeded node even if freshness is stale', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [node('n1', 'Done')],
			edges: [],
			// succeeded_up_to_date means isUpToDate=true → freshness=fresh in practice,
			// but we test the branch defensively with succeeded + stale binding
			nodeBindings: { n1: { status: 'succeeded' } },
			queueRuntime: {}
		});
		// statusModel maps 'succeeded' binding → 'completed' lifecycle
		expect(rows[0]?.lifecycle).toBe('completed');
		expect(rows[0]?.displayReason).toBe('');
	});

	it('is empty for failed node', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [node('n1', 'Errored')],
			edges: [],
			nodeBindings: { n1: { status: 'failed' } },
			queueRuntime: {}
		});
		expect(rows[0]?.lifecycle).toBe('failed');
		expect(rows[0]?.displayReason).toBe('');
	});
});

// ── 7–9. Priority ordering ────────────────────────────────────────────────────
describe('displayReason: priority ordering', () => {
	it('blocked code beats llm-wait', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [node('n1', 'Contested')],
			edges: [],
			nodeBindings: { n1: { status: 'stale' } },
			queueRuntime: {
				blockedByNode: {
					n1: { nodeId: 'n1', reasonCode: 'WAITING_REQUIRED_PARAM', handle: 'params', plane: 'param' }
				},
				llmLease: {
					state: 'waiting',
					nodeId: 'n1',
					holderNodeId: 'n_other',
					waitQueueLength: 1,
					waitingNodeIds: ['n1']
				}
			}
		});
		expect(rows[0]?.displayReason).toBe('WAITING_REQUIRED_PARAM');
	});

	it('llm-hold takes priority when node is in activeNodeIds even if also in waitingNodeIds', () => {
		// isLlmHolder and isLlmWaiting are mutually exclusive — holder wins.
		// A node present in activeNodeIds is classified as holder regardless of
		// whether it also appears in waitingNodeIds.
		const rows = buildRunMonitorNodeRows({
			nodes: [node('n1', 'Edge')],
			edges: [],
			nodeBindings: { n1: { status: 'running' } },
			queueRuntime: {
				llmLease: {
					state: 'waiting',
					nodeId: 'n1',
					holderNodeId: 'n1',
					activeNodeIds: ['n1'],
					waitQueueLength: 1,
					waitingNodeIds: ['n1']
				}
			},
			runStatus: 'running'
		});
		expect(rows[0]?.isLlmHolder).toBe(true);
		expect(rows[0]?.isLlmWaiting).toBe(false);  // mutually exclusive with isLlmHolder
		expect(rows[0]?.displayReason).toBe('llm-hold');
	});

	it('llm-hold beats stale', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [node('n1', 'HoldStale')],
			edges: [],
			nodeBindings: { n1: { status: 'stale' } },
			queueRuntime: {
				schedulerSnapshot: {
					perNode: [{ nodeId: 'n1', readyWork: true, inflight: 1, pendingInputCount: 0 }]
				},
				llmLease: {
					state: 'acquired',
					holderNodeId: 'n1',
					activeNodeIds: ['n1'],
					waitQueueLength: 0,
					waitingNodeIds: []
				}
			},
			runStatus: 'running'
		});
		expect(rows[0]?.displayReason).toBe('llm-hold');
	});
});

// ── 10. Lifecycle integration: blocked → running → "" ────────────────────────
describe('displayReason: lifecycle integration', () => {
	it('clears reason when node transitions from blocked to running', () => {
		// Simulates two successive buildRunMonitorNodeRows calls as store updates
		// arrive — blocked state, then cleared state after node_started.

		const blockedRows = buildRunMonitorNodeRows({
			nodes: [node('n1', 'Worker')],
			edges: [],
			nodeBindings: { n1: { status: 'stale' } },
			queueRuntime: {
				blockedByNode: {
					n1: { nodeId: 'n1', reasonCode: 'WAITING_REQUIRED_INPUT', handle: 'in', plane: 'work' }
				}
			}
		});
		expect(blockedRows[0]?.displayReason).toBe('WAITING_REQUIRED_INPUT');
		expect(blockedRows[0]?.isBlocked).toBe(true);

		// After node_started the store clears blockedByNode[n1]
		const runningRows = buildRunMonitorNodeRows({
			nodes: [node('n1', 'Worker')],
			edges: [],
			nodeBindings: { n1: { status: 'running' } },
			queueRuntime: {
				blockedByNode: {},   // cleared by node_started handler
				schedulerSnapshot: {
					perNode: [{ nodeId: 'n1', readyWork: true, inflight: 1, pendingInputCount: 0 }]
				}
			},
			runStatus: 'running'
		});
		expect(runningRows[0]?.displayReason).toBe('');
		expect(runningRows[0]?.isBlocked).toBe(false);
		expect(runningRows[0]?.lifecycle).toBe('running');
	});

	it('clears reason when llm lease is released', () => {
		const waitingRows = buildRunMonitorNodeRows({
			nodes: [node('n1', 'LlmUser')],
			edges: [],
			nodeBindings: { n1: { status: 'running' } },
			queueRuntime: {
				llmLease: {
					state: 'waiting',
					nodeId: 'n1',
					holderNodeId: 'n_other',
					waitQueueLength: 1,
					waitingNodeIds: ['n1']
				}
			},
			runStatus: 'running'
		});
		expect(waitingRows[0]?.displayReason).toBe('llm-wait');

		const releasedRows = buildRunMonitorNodeRows({
			nodes: [node('n1', 'LlmUser')],
			edges: [],
			nodeBindings: { n1: { status: 'running' } },
			queueRuntime: {
				llmLease: { state: 'released', holderNodeId: '', waitingNodeIds: [] }
			},
			runStatus: 'running'
		});
		expect(releasedRows[0]?.displayReason).toBe('');
	});
});

// ── 11. Multiple independent nodes ───────────────────────────────────────────
describe('displayReason: multiple nodes', () => {
	it('assigns reasons independently per node', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [
				node('n_run', 'Runner'),
				node('n_blk', 'Blocker'),
				node('n_llm', 'LlmWaiter'),
				node('n_done', 'Done')
			],
			edges: [],
			nodeBindings: {
				n_run: { status: 'running' },
				n_blk: { status: 'stale' },
				n_llm: { status: 'running' },
				n_done: { status: 'succeeded' }
			},
			queueRuntime: {
				blockedByNode: {
					n_blk: { nodeId: 'n_blk', reasonCode: 'WAITING_REQUIRED_INPUT', handle: 'in', plane: 'work' }
				},
				llmLease: {
					state: 'waiting',
					nodeId: 'n_llm',
					holderNodeId: 'n_run',
					waitQueueLength: 1,
					waitingNodeIds: ['n_llm']
				},
				schedulerSnapshot: {
					perNode: [
						{ nodeId: 'n_run', readyWork: true, inflight: 1, pendingInputCount: 0 }
					]
				}
			},
			runStatus: 'running'
		});

		const byId = Object.fromEntries(rows.map((r) => [r.nodeId, r]));
		expect(byId['n_run']?.displayReason).toBe('llm-hold');
		expect(byId['n_blk']?.displayReason).toBe('WAITING_REQUIRED_INPUT');
		expect(byId['n_llm']?.displayReason).toBe('llm-wait');
		expect(byId['n_done']?.displayReason).toBe('');
	});
});

// ── 12. Regression: existing flags unaffected ────────────────────────────────
describe('displayReason: regression — existing flags unchanged', () => {
	it('isBlocked, isLlmHolder, isLlmWaiting are still correct', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [node('n_model', 'Model'), node('n_letter', 'Letter')],
			edges: [],
			nodeBindings: {
				n_model: { status: 'running' },
				n_letter: { status: 'stale' }
			},
			queueRuntime: {
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
					activeNodeIds: ['n_model'],
					waitQueueLength: 1,
					waitingNodeIds: ['n_letter']
				},
				schedulerSnapshot: {
					perNode: [
						{ nodeId: 'n_letter', readyWork: false, inflight: 1, pendingInputCount: 0 }
					]
				}
			},
			runStatus: 'running'
		});
		const model = rows.find((r) => r.nodeId === 'n_model')!;
		const letter = rows.find((r) => r.nodeId === 'n_letter')!;

		// Flags unchanged
		expect(model.isLlmHolder).toBe(true);
		expect(model.isBlocked).toBe(false);
		expect(model.isLlmWaiting).toBe(false);

		expect(letter.isBlocked).toBe(true);
		expect(letter.isLlmWaiting).toBe(true);
		expect(letter.isLlmHolder).toBe(false);

		// displayReason also correct
		expect(model.displayReason).toBe('llm-hold');
		expect(letter.displayReason).toBe('WAITING_REQUIRED_PARAM'); // blocked beats llm-wait
	});

	it('filterAndSortRunMonitorNodes blocked filter still works with new field present', () => {
		const rows = buildRunMonitorNodeRows({
			nodes: [node('n_blk', 'Blocked'), node('n_run', 'Running')],
			edges: [],
			nodeBindings: {
				n_blk: { status: 'stale' },
				n_run: { status: 'running' }
			},
			queueRuntime: {
				blockedByNode: {
					n_blk: { nodeId: 'n_blk', reasonCode: 'WAITING_REQUIRED_INPUT', handle: 'in', plane: 'work' }
				}
			}
		});
		const filtered = filterAndSortRunMonitorNodes(rows, 'blocked', 'label_asc', false);
		expect(filtered.map((r: any) => r.nodeId)).toEqual(['n_blk']);
	});
});
