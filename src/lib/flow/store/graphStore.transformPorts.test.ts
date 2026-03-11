import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { graphStore, derivePortsForNodeData } from './graphStore';

describe('graphStore transform adapter ports', () => {
	it('updates transform ports when subtype changes to adapters', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('transform', { x: 24, y: 24 });

		let state = get(graphStore);
		let node = state.nodes.find((n) => n.id === nodeId);
		expect(node ? derivePortsForNodeData(node.data).in : null).toBe('table');
		expect(node ? derivePortsForNodeData(node.data).out : null).toBe('table');

		graphStore.setTransformKind(nodeId, 'json_to_table');
		state = get(graphStore);
		node = state.nodes.find((n) => n.id === nodeId);
		expect(node ? derivePortsForNodeData(node.data).in : null).toBe('json');
		expect(node ? derivePortsForNodeData(node.data).out : null).toBe('table');

		graphStore.setTransformKind(nodeId, 'text_to_table');
		state = get(graphStore);
		node = state.nodes.find((n) => n.id === nodeId);
		expect(node ? derivePortsForNodeData(node.data).in : null).toBe('text');
		expect(node ? derivePortsForNodeData(node.data).out : null).toBe('table');

		graphStore.setTransformKind(nodeId, 'table_to_json');
		state = get(graphStore);
		node = state.nodes.find((n) => n.id === nodeId);
		expect(node ? derivePortsForNodeData(node.data).in : null).toBe('table');
		expect(node ? derivePortsForNodeData(node.data).out : null).toBe('json');
	});
});
