import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { __applyRunEventForTest, graphStore } from './graphStore';

describe('graphStore control plane edge state', () => {
	it('tracks open/depth/closed/blocked for edge control signals', () => {
		graphStore.hardResetGraph();
		const edgeId = 'edge_cp_runtime';
		let state = get(graphStore as any);

		state = __applyRunEventForTest(
			state as any,
			{
				type: 'control_signal',
				runId: 'run_cp_edge',
				at: '2026-04-02T12:00:00.000Z',
				signal: 'upstream_opened',
				edgeId,
				seq: 11
			} as any,
			'run_cp_edge'
		);
		state = __applyRunEventForTest(
			state as any,
			{
				type: 'control_signal',
				runId: 'run_cp_edge',
				at: '2026-04-02T12:00:01.000Z',
				signal: 'item_enqueued',
				edgeId,
				seq: 12
			} as any,
			'run_cp_edge'
		);
		state = __applyRunEventForTest(
			state as any,
			{
				type: 'control_signal',
				runId: 'run_cp_edge',
				at: '2026-04-02T12:00:02.000Z',
				signal: 'input_blocked',
				edgeId,
				seq: 13
			} as any,
			'run_cp_edge'
		);
		state = __applyRunEventForTest(
			state as any,
			{
				type: 'control_signal',
				runId: 'run_cp_edge',
				at: '2026-04-02T12:00:03.000Z',
				signal: 'input_drained',
				edgeId,
				seq: 14
			} as any,
			'run_cp_edge'
		);
		state = __applyRunEventForTest(
			state as any,
			{
				type: 'control_signal',
				runId: 'run_cp_edge',
				at: '2026-04-02T12:00:04.000Z',
				signal: 'input_ready',
				edgeId,
				seq: 15
			} as any,
			'run_cp_edge'
		);
		state = __applyRunEventForTest(
			state as any,
			{
				type: 'control_signal',
				runId: 'run_cp_edge',
				at: '2026-04-02T12:00:05.000Z',
				signal: 'upstream_closed',
				edgeId,
				seq: 16
			} as any,
			'run_cp_edge'
		);

		const edgeState = (state as any)?.queueRuntime?.controlPlaneEdgeState?.[edgeId];
		expect(edgeState).toBeTruthy();
		expect(Boolean(edgeState.open)).toBe(false);
		expect(Boolean(edgeState.closed)).toBe(true);
		expect(Number(edgeState.depth ?? -1)).toBe(0);
		expect(Boolean(edgeState.blocked)).toBe(false);
		expect(Number(edgeState.lastSeq ?? 0)).toBe(16);
	});

	it('ignores out-of-order control events by sequence', () => {
		graphStore.hardResetGraph();
		let state = get(graphStore as any);
		state = __applyRunEventForTest(
			state as any,
			{
				type: 'control_signal',
				runId: 'run_cp_seq',
				at: '2026-04-02T12:10:00.000Z',
				signal: 'item_enqueued',
				edgeId: 'edge_seq',
				seq: 10
			} as any,
			'run_cp_seq'
		);
		state = __applyRunEventForTest(
			state as any,
			{
				type: 'control_signal',
				runId: 'run_cp_seq',
				at: '2026-04-02T12:10:01.000Z',
				signal: 'input_drained',
				edgeId: 'edge_seq',
				seq: 9
			} as any,
			'run_cp_seq'
		);
		const edgeState = (state as any)?.queueRuntime?.controlPlaneEdgeState?.edge_seq;
		expect(Number(edgeState?.depth ?? 0)).toBe(1);
		expect(Number(edgeState?.lastSeq ?? 0)).toBe(10);
	});
});
