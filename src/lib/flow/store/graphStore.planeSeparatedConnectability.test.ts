import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { graphStore } from './graphStore';

describe('graphStore plane-separated connectability', () => {
	it('enforces plane-specific edge compatibility at connect time', () => {
		graphStore.hardResetGraph();
		const src = graphStore.addNode('tool', { x: 0, y: 0 });
		const dst = graphStore.addNode('tool', { x: 260, y: 0 });
		graphStore.updateNodePortDeclaration(src, 'out', 'param_out', {
			plane: 'param',
			required: false,
			cardinality: 'many'
		});
		graphStore.updateNodePortDeclaration(dst, 'in', 'param_in', {
			plane: 'param',
			required: false,
			cardinality: 'many',
			behavior: 'single_item'
		});
		const good = graphStore.addEdge({
			id: 'e_param_ok',
			source: src,
			sourceHandle: 'param_out',
			target: dst,
			targetHandle: 'param_in',
			data: { exec: 'idle', mode: 'param' }
		} as any);
		expect(good.ok).toBe(true);

		const bad = graphStore.addEdge({
			id: 'e_work_bad',
			source: src,
			sourceHandle: 'param_out',
			target: dst,
			targetHandle: 'param_in',
			data: { exec: 'idle', mode: 'work' }
		} as any);
		expect(bad.ok).toBe(false);
		expect(String((bad as any).error ?? '')).toContain('mode');
		const state = get(graphStore as any);
		expect(state.edges.some((edge: any) => edge.id === 'e_work_bad')).toBe(false);
	});
});

