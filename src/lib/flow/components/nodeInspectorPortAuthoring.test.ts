import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { graphStore } from '$lib/flow/store/graphStore';
import { collectExpectedInputHandles } from './nodeInspectorSchema';

describe('NodeInspector port authoring model', () => {
	it('surfaces authored input handle before any edge exists', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('tool', { x: 0, y: 0 });
		const authored = graphStore.updateNodePortDeclaration(nodeId, 'in', 'param_filters', {
			plane: 'param',
			required: false,
			cardinality: 'many',
			behavior: 'once'
		});
		expect(authored.ok).toBe(true);

		const state = get(graphStore as any);
		const node = state.nodes.find((n: any) => n.id === nodeId);
		const handles = collectExpectedInputHandles(node as any, []);
		expect(handles.some((h) => h.handle === 'param_filters')).toBe(true);
	});
});

