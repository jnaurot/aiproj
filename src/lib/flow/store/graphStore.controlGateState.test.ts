import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { __applyRunEventForTest, graphStore } from './graphStore';

describe('graphStore control gate state projection', () => {
	it('stores control_gate_state rows by node in queue runtime', () => {
		graphStore.hardResetGraph();
		const next = __applyRunEventForTest(
			get(graphStore as any),
			{
				type: 'control_gate_state',
				runId: 'run_ctrl_1',
				nodeId: 'node_sink',
				state: 'blocked',
				reasonCode: 'CONTROL_GATE_BLOCKED',
				handle: 'control_gate',
				at: '2026-04-01T13:40:00.000Z'
			} as any,
			'run_ctrl_1'
		);
		const rows = ((next as any)?.queueRuntime?.controlGatesByNode ?? {}) as Record<string, any>;
		expect(rows.node_sink).toMatchObject({
			nodeId: 'node_sink',
			state: 'blocked',
			reasonCode: 'CONTROL_GATE_BLOCKED',
			handle: 'control_gate',
			updatedAt: '2026-04-01T13:40:00.000Z'
		});
	});

	it('updates latest state for repeated control_gate_state events on same node', () => {
		graphStore.hardResetGraph();
		let next = __applyRunEventForTest(
			get(graphStore as any),
			{
				type: 'control_gate_state',
				runId: 'run_ctrl_2',
				nodeId: 'node_sink',
				state: 'blocked',
				reasonCode: 'CONTROL_GATE_BLOCKED',
				at: '2026-04-01T13:41:00.000Z'
			} as any,
			'run_ctrl_2'
		);
		next = __applyRunEventForTest(
			next as any,
			{
				type: 'control_gate_state',
				runId: 'run_ctrl_2',
				nodeId: 'node_sink',
				state: 'open',
				at: '2026-04-01T13:41:02.000Z'
			} as any,
			'run_ctrl_2'
		);
		const rows = ((next as any)?.queueRuntime?.controlGatesByNode ?? {}) as Record<string, any>;
		expect(rows.node_sink?.state).toBe('open');
		expect(rows.node_sink?.updatedAt).toBe('2026-04-01T13:41:02.000Z');
		expect(rows.node_sink?.reasonCode).toBeUndefined();
	});
});
