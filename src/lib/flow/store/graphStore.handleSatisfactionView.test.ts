import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { __applyRunEventForTest, graphStore } from './graphStore';

describe('graphStore handle satisfaction diagnostics', () => {
	it('projects node_handle_satisfaction into queueRuntime state', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('tool', { x: 0, y: 0 });
		const base = get(graphStore as any);
		const next = __applyRunEventForTest(
			base as any,
			{
				type: 'node_handle_satisfaction',
				runId: 'run_handle_sat',
				at: '2026-03-23T16:30:00.000Z',
				nodeId,
				handle: 'in',
				status: 'partial',
				connectedEdges: 2,
				providedEdges: 1
			} as any,
			'run_handle_sat'
		);
		const key = `${nodeId}:in`;
		const row = ((next as any)?.queueRuntime?.handleSatisfaction ?? {})[key] as any;
		expect(String(row?.status ?? '')).toBe('partial');
		expect(Number(row?.connectedEdges ?? 0)).toBe(2);
		expect(Number(row?.providedEdges ?? 0)).toBe(1);
	});
});
