// graphStore.monitorSemantics.test.ts
// Regression tests for stale-blocker sweep semantics in graphStore.run.ts.
// These lock in the fix for Active=0 with all nodes showing MAX_INFLIGHT_REACHED.

import { describe, expect, it } from 'vitest';

import type { GraphState } from './graphStore';
import { __applyRunEventForTest } from './graphStore';

// ── Helpers ──────────────────────────────────────────────────────────────────

function baseState(overrides: Partial<GraphState> = {}): GraphState {
	return {
		graphId: 'graph-monitor-test',
		nodes: [
			{ id: 'n_a', data: { kind: 'model', meta: {} } },
			{ id: 'n_b', data: { kind: 'model', meta: {} } },
			{ id: 'n_c', data: { kind: 'model', meta: {} } }
		] as any,
		edges: [] as any,
		selectedNodeId: null,
		inspector: { nodeId: null, draftParams: {}, dirty: false } as any,
		logs: [],
		runStatus: 'running',
		lastRunStatus: 'succeeded',
		freshness: 'stale',
		staleNodeCount: 3,
		activeRunMode: 'full',
		activeRunFrom: null,
		activeRunNodeSet: new Set<string>(['n_a', 'n_b', 'n_c']),
		nodeOutputs: {},
		nodeBindings: {
			n_a: { status: 'stale', isUpToDate: false, cacheValid: false, currentRunId: 'run-1', current: { execKey: null, artifactId: null }, last: { execKey: null, artifactId: null }, staleReason: null },
			n_b: { status: 'stale', isUpToDate: false, cacheValid: false, currentRunId: 'run-1', current: { execKey: null, artifactId: null }, last: { execKey: null, artifactId: null }, staleReason: null },
			n_c: { status: 'stale', isUpToDate: false, cacheValid: false, currentRunId: 'run-1', current: { execKey: null, artifactId: null }, last: { execKey: null, artifactId: null }, staleReason: null }
		} as any,
		activeRunId: 'run-1',
		...overrides
	};
}

function withInflightBlockers(state: GraphState, ...nodeIds: string[]): GraphState {
	const blockedByNode: Record<string, unknown> = {};
	for (const id of nodeIds) {
		blockedByNode[id] = {
			nodeId: id,
			reasonCode: 'MAX_INFLIGHT_REACHED:global',
			updatedAt: '2026-04-29T15:00:00Z'
		};
	}
	return {
		...state,
		queueRuntime: {
			...(state.queueRuntime ?? {}),
			blockedByNode
		} as any
	};
}

function withLeaseBlockers(state: GraphState, ...nodeIds: string[]): GraphState {
	const blockedByNode: Record<string, unknown> = {};
	for (const id of nodeIds) {
		blockedByNode[id] = {
			nodeId: id,
			reasonCode: 'LEASE_UNAVAILABLE',
			updatedAt: '2026-04-29T15:00:00Z'
		};
	}
	return {
		...state,
		queueRuntime: {
			...(state.queueRuntime ?? {}),
			blockedByNode
		} as any
	};
}

// ── scheduler_snapshot sweep tests ───────────────────────────────────────────

describe('graphStore monitorSemantics: scheduler_snapshot sweeps stale MAX_INFLIGHT_REACHED', () => {
	it('clears all MAX_INFLIGHT_REACHED blockers when inflightCount === 0', () => {
		const state = withInflightBlockers(baseState(), 'n_a', 'n_b', 'n_c');

		const next = __applyRunEventForTest(
			state,
			{
				type: 'scheduler_snapshot',
				runId: 'run-1',
				at: '2026-04-29T15:01:00Z',
				readyCount: 3,
				inflightCount: 0,   // nothing running → cap cannot be reached
				pendingQueueDepth: 0,
				runnableNodeCount: 3,
				stalled: false,
				perNode: [
					{ nodeId: 'n_a', readyWork: true, inflight: 0, pendingInputCount: 0 },
					{ nodeId: 'n_b', readyWork: true, inflight: 0, pendingInputCount: 0 },
					{ nodeId: 'n_c', readyWork: true, inflight: 0, pendingInputCount: 0 }
				]
			} as any,
			'run-1'
		);

		const blocked = (next as any).queueRuntime?.blockedByNode ?? {};
		expect(Object.keys(blocked)).toHaveLength(0);
	});

	it('clears MAX_INFLIGHT_REACHED only for nodes with no inflight and no ready work', () => {
		const state = withInflightBlockers(baseState(), 'n_a', 'n_b', 'n_c');

		// n_a: still has ready work queued → keep its blocker (could still be capped)
		// n_b: no ready work, no inflight → clearly idle, clear it
		// n_c: no ready work, no inflight → clearly idle, clear it
		const next = __applyRunEventForTest(
			state,
			{
				type: 'scheduler_snapshot',
				runId: 'run-1',
				at: '2026-04-29T15:01:00Z',
				readyCount: 1,
				inflightCount: 1,   // something running — cap may still apply
				pendingQueueDepth: 0,
				runnableNodeCount: 1,
				stalled: false,
				perNode: [
					{ nodeId: 'n_a', readyWork: true,  inflight: 1, pendingInputCount: 0 }, // still running
					{ nodeId: 'n_b', readyWork: false, inflight: 0, pendingInputCount: 0 }, // idle
					{ nodeId: 'n_c', readyWork: false, inflight: 0, pendingInputCount: 0 }  // idle
				]
			} as any,
			'run-1'
		);

		const blocked = (next as any).queueRuntime?.blockedByNode ?? {};
		// n_a still running — keep its MAX_INFLIGHT_REACHED (may be legitimately capped)
		expect(blocked['n_a']).toBeDefined();
		// n_b and n_c have no work at all — their MAX_INFLIGHT_REACHED is stale
		expect(blocked['n_b']).toBeUndefined();
		expect(blocked['n_c']).toBeUndefined();
	});

	it('clears MAX_INFLIGHT_REACHED for nodes absent from perNode when globally idle', () => {
		// n_c is NOT mentioned in perNode — scheduler dropped it from tracking
		const state = withInflightBlockers(baseState(), 'n_c');

		const next = __applyRunEventForTest(
			state,
			{
				type: 'scheduler_snapshot',
				runId: 'run-1',
				at: '2026-04-29T15:01:00Z',
				readyCount: 0,
				inflightCount: 0,
				pendingQueueDepth: 0,
				runnableNodeCount: 0,
				stalled: false,
				perNode: [
					{ nodeId: 'n_a', readyWork: false, inflight: 0, pendingInputCount: 0 },
					{ nodeId: 'n_b', readyWork: false, inflight: 0, pendingInputCount: 0 }
					// n_c not present
				]
			} as any,
			'run-1'
		);

		const blocked = (next as any).queueRuntime?.blockedByNode ?? {};
		expect(blocked['n_c']).toBeUndefined();
	});

	it('preserves non-MAX_INFLIGHT blockers during the sweep', () => {
		// n_a has WAITING_REQUIRED_INPUT — must not be swept
		const state: GraphState = {
			...baseState(),
			queueRuntime: {
				blockedByNode: {
					n_a: { nodeId: 'n_a', reasonCode: 'WAITING_REQUIRED_INPUT', updatedAt: '2026-04-29T15:00:00Z' },
					n_b: { nodeId: 'n_b', reasonCode: 'MAX_INFLIGHT_REACHED:global', updatedAt: '2026-04-29T15:00:00Z' }
				}
			} as any
		};

		const next = __applyRunEventForTest(
			state,
			{
				type: 'scheduler_snapshot',
				runId: 'run-1',
				at: '2026-04-29T15:01:00Z',
				readyCount: 0,
				inflightCount: 0,
				pendingQueueDepth: 0,
				runnableNodeCount: 0,
				stalled: false,
				perNode: []
			} as any,
			'run-1'
		);

		const blocked = (next as any).queueRuntime?.blockedByNode ?? {};
		expect(blocked['n_a']).toBeDefined();          // preserved
		expect(blocked['n_a']?.reasonCode).toBe('WAITING_REQUIRED_INPUT');
		expect(blocked['n_b']).toBeUndefined();        // swept (MAX_INFLIGHT + globally idle)
	});

	it('logs the sweep action in run logs', () => {
		const state = withInflightBlockers(baseState(), 'n_a', 'n_b');

		const next = __applyRunEventForTest(
			state,
			{
				type: 'scheduler_snapshot',
				runId: 'run-1',
				at: '2026-04-29T15:01:00Z',
				readyCount: 0,
				inflightCount: 0,
				pendingQueueDepth: 0,
				runnableNodeCount: 0,
				stalled: false,
				perNode: []
			} as any,
			'run-1'
		);

		const lastLog = (next.logs ?? []).at(-1);
		expect(String(lastLog?.message ?? '')).toContain('swept_stale_inflight');
		expect(String(lastLog?.message ?? '')).toContain('count=2');
	});
});

// ── llm_lease sweep tests ─────────────────────────────────────────────────────

describe('graphStore monitorSemantics: llm_lease sweeps stale LEASE_UNAVAILABLE', () => {
	it('clears LEASE_UNAVAILABLE blockers for nodes no longer in wait queue', () => {
		// n_b and n_c were waiting; n_a acquired the lease
		const state: GraphState = {
			...baseState(),
			queueRuntime: {
				blockedByNode: {
					n_b: { nodeId: 'n_b', reasonCode: 'LEASE_UNAVAILABLE', updatedAt: '2026-04-29T15:00:00Z' },
					n_c: { nodeId: 'n_c', reasonCode: 'LEASE_UNAVAILABLE', updatedAt: '2026-04-29T15:00:00Z' }
				},
				llmLease: {
					state: 'acquired',
					nodeId: 'n_a',
					holderNodeId: 'n_a',
					activeNodeIds: ['n_a'],
					waitQueueLength: 1,
					waitingNodeIds: ['n_b'],  // only n_b is still waiting
					updatedAt: '2026-04-29T15:00:00Z'
				}
			} as any
		};

		const next = __applyRunEventForTest(
			state,
			{
				type: 'llm_lease',
				runId: 'run-1',
				at: '2026-04-29T15:01:00Z',
				state: 'acquired',
				nodeId: 'n_a',
				holderNodeId: 'n_a',
				waitQueueLength: 1,
				waitingNodeIds: ['n_b']  // n_c is gone from queue
			} as any,
			'run-1'
		);

		const blocked = (next as any).queueRuntime?.blockedByNode ?? {};
		expect(blocked['n_b']).toBeDefined();    // still in queue → keep
		expect(blocked['n_c']).toBeUndefined();  // left queue → swept
	});

	it('clears all LEASE_UNAVAILABLE blockers when lease becomes fully idle', () => {
		const state = withLeaseBlockers(baseState(), 'n_a', 'n_b', 'n_c');

		const next = __applyRunEventForTest(
			state,
			{
				type: 'llm_lease',
				runId: 'run-1',
				at: '2026-04-29T15:01:00Z',
				state: 'released',
				nodeId: 'n_a',
				holderNodeId: null,
				waitQueueLength: 0,
				waitingNodeIds: []   // nobody waiting, nobody holding
			} as any,
			'run-1'
		);

		const blocked = (next as any).queueRuntime?.blockedByNode ?? {};
		expect(Object.keys(blocked)).toHaveLength(0);
	});

	it('preserves non-LEASE_UNAVAILABLE blockers during lease sweep', () => {
		const state: GraphState = {
			...baseState(),
			queueRuntime: {
				blockedByNode: {
					n_a: { nodeId: 'n_a', reasonCode: 'WAITING_REQUIRED_INPUT', updatedAt: '2026-04-29T15:00:00Z' },
					n_b: { nodeId: 'n_b', reasonCode: 'LEASE_UNAVAILABLE', updatedAt: '2026-04-29T15:00:00Z' }
				}
			} as any
		};

		const next = __applyRunEventForTest(
			state,
			{
				type: 'llm_lease',
				runId: 'run-1',
				at: '2026-04-29T15:01:00Z',
				state: 'released',
				nodeId: 'n_b',
				holderNodeId: null,
				waitQueueLength: 0,
				waitingNodeIds: []
			} as any,
			'run-1'
		);

		const blocked = (next as any).queueRuntime?.blockedByNode ?? {};
		expect(blocked['n_a']).toBeDefined();          // WAITING_REQUIRED_INPUT — preserved
		expect(blocked['n_a']?.reasonCode).toBe('WAITING_REQUIRED_INPUT');
		expect(blocked['n_b']).toBeUndefined();        // LEASE_UNAVAILABLE — swept
	});

	it('logs the lease sweep action in run logs', () => {
		const state = withLeaseBlockers(baseState(), 'n_a', 'n_b');

		const next = __applyRunEventForTest(
			state,
			{
				type: 'llm_lease',
				runId: 'run-1',
				at: '2026-04-29T15:01:00Z',
				state: 'released',
				nodeId: 'n_a',
				holderNodeId: null,
				waitQueueLength: 0,
				waitingNodeIds: []
			} as any,
			'run-1'
		);

		const lastLog = (next.logs ?? []).at(-1);
		expect(String(lastLog?.message ?? '')).toContain('swept_stale_lease');
		expect(String(lastLog?.message ?? '')).toContain('count=2');
	});
});
