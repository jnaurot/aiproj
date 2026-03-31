import { get } from 'svelte/store';
import { describe, expect, it } from 'vitest';

import { graphStore } from './graphStore';

describe('graphStore control-link metadata', () => {
	it('defaults new edges to data_link kind', () => {
		graphStore.hardResetGraph();
		const src = graphStore.addNode('source', { x: 0, y: 0 });
		const dst = graphStore.addNode('transform', { x: 220, y: 0 });
		const res = graphStore.addEdge({
			id: 'e_data_link_default',
			source: src,
			target: dst,
			sourceHandle: 'out',
			targetHandle: 'in',
			data: { exec: 'idle', mode: 'work' }
		} as any);
		expect(res.ok).toBe(true);
		const edge = get(graphStore).edges.find((row) => row.id === 'e_data_link_default');
		expect(String((edge?.data as any)?.linkKind ?? '')).toBe('data_link');
	});

	it('treats control_link as control mode for normalization', () => {
		graphStore.hardResetGraph();
		const src = graphStore.addNode('source', { x: 0, y: 0 });
		const dst = graphStore.addNode('transform', { x: 220, y: 0 });
		const sourcePortRes = graphStore.updateNodePortDeclaration(src, 'out', 'control_out', {
			plane: 'control',
			cardinality: 'many'
		});
		const targetPortRes = graphStore.updateNodePortDeclaration(dst, 'in', 'control_in', {
			plane: 'control',
			cardinality: 'many'
		});
		expect(sourcePortRes.ok).toBe(true);
		expect(targetPortRes.ok).toBe(true);
		const res = graphStore.addEdge({
			id: 'e_control_link',
			source: src,
			target: dst,
			sourceHandle: 'control_out',
			targetHandle: 'control_in',
			data: { exec: 'idle', linkKind: 'control_link' }
		} as any);
		expect(res.ok).toBe(true);
		const edge = get(graphStore).edges.find((row) => row.id === 'e_control_link');
		expect(String((edge?.data as any)?.mode ?? '')).toBe('control');
		expect(String((edge?.data as any)?.linkKind ?? '')).toBe('control_link');
	});
});
