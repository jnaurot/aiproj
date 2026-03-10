import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { graphStore } from './graphStore';

describe('graphStore transform adapter ports', () => {
	it('updates transform ports when subtype changes to adapters', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('transform', { x: 24, y: 24 });

		let state = get(graphStore);
		let node = state.nodes.find((n) => n.id === nodeId);
		expect(node?.data?.ports?.in).toBe('table');
		expect(node?.data?.ports?.out).toBe('table');

		graphStore.setTransformKind(nodeId, 'json_to_table');
		state = get(graphStore);
		node = state.nodes.find((n) => n.id === nodeId);
		expect(node?.data?.ports?.in).toBe('json');
		expect(node?.data?.ports?.out).toBe('table');

		graphStore.setTransformKind(nodeId, 'text_to_table');
		state = get(graphStore);
		node = state.nodes.find((n) => n.id === nodeId);
		expect(node?.data?.ports?.in).toBe('text');
		expect(node?.data?.ports?.out).toBe('table');

		graphStore.setTransformKind(nodeId, 'table_to_json');
		state = get(graphStore);
		node = state.nodes.find((n) => n.id === nodeId);
		expect(node?.data?.ports?.in).toBe('table');
		expect(node?.data?.ports?.out).toBe('json');
	});
});
