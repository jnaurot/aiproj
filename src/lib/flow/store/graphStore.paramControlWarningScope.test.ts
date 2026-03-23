import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { __applyRunEventForTest, graphStore } from './graphStore';

describe('graphStore param/control warning projection', () => {
	it('stores run-scoped param/control input warning rows by edge key', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('tool', { x: 0, y: 0 });
		const base = get(graphStore as any);
		const event = {
			type: 'node_input_warning',
			runId: 'run_param_warning',
			at: '2026-03-23T17:00:00.000Z',
			nodeId,
			handle: 'param_filters',
			edgeId: 'e_param',
			plane: 'param',
			code: 'PARAM_CONTROL_EMPTY_INPUT',
			reasonCode: 'EMPTY_JSON',
			upstreamNodeId: 'src_params'
		} as any;
		const next = __applyRunEventForTest(base as any, event, 'run_param_warning');
		const key = `${nodeId}:param_filters:e_param:param`;
		const row = ((next as any)?.queueRuntime?.paramControlWarnings ?? {})[key] as any;
		expect(String(row?.code ?? '')).toBe('PARAM_CONTROL_EMPTY_INPUT');
		expect(String(row?.plane ?? '')).toBe('param');
		expect(String(row?.reasonCode ?? '')).toBe('EMPTY_JSON');
	});
});
