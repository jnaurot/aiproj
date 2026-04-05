import { get } from 'svelte/store';
import { describe, expect, it } from 'vitest';

import { graphStore } from './graphStore';

describe('graphStore node name enforcement', () => {
	it('auto-adjusts duplicate labels during non-dialogmatic addNode calls', () => {
		graphStore.hardResetGraph();
		const first = graphStore.addNode('source', { x: 10, y: 10 }, { label: 'Template Node' });
		const second = graphStore.addNode('transform', { x: 160, y: 10 }, { label: 'template node' });
		const state = get(graphStore);
		const firstLabel = String((state.nodes.find((n) => n.id === first) as any)?.data?.label ?? '');
		const secondLabel = String((state.nodes.find((n) => n.id === second) as any)?.data?.label ?? '');
		expect(firstLabel).toBe('Template Node');
		expect(secondLabel).toBe('template node (2)');
	});

	it('rejects duplicate rename in current scope (trim + case-insensitive)', () => {
		graphStore.hardResetGraph();
		const first = graphStore.addNode('source', { x: 10, y: 10 }, { label: 'Alpha' });
		const second = graphStore.addNode('transform', { x: 160, y: 10 }, { label: 'Beta' });

		const res = graphStore.updateNodeTitle(second, ' alpha ');
		expect((res as any)?.ok).toBe(false);
		expect(String((res as any)?.reason ?? '')).toBe('duplicate_name_in_scope');

		const state = get(graphStore);
		const secondNode = state.nodes.find((n) => n.id === second);
		expect(String((secondNode as any)?.data?.label ?? '')).toBe('Beta');
		const firstNode = state.nodes.find((n) => n.id === first);
		expect(String((firstNode as any)?.data?.label ?? '')).toBe('Alpha');
	});

	it('blocks graph save when duplicate names already exist in the graph', async () => {
		graphStore.hardResetGraph();
		const first = graphStore.addNode('source', { x: 0, y: 0 }, { label: 'One' });
		const second = graphStore.addNode('transform', { x: 220, y: 0 }, { label: 'Two' });
		const state = get(graphStore);
		const duplicateNodes = state.nodes.map((n) =>
			n.id === second ? { ...n, data: { ...n.data, label: ' one ' } } : n
		);
		graphStore.syncFromCanvas(duplicateNodes as any, state.edges as any);
		graphStore.selectNode(first);

		const result = await graphStore.saveGraph('save');
		expect((result as any)?.ok).toBe(false);
		expect(String((result as any)?.reason ?? '')).toBe('preflight_failed');
		expect(String((result as any)?.error ?? '')).toContain('NODE_NAME_DUPLICATE');
	});
});
