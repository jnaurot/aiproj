import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { graphStore } from './graphStore';

describe('graphStore schema adapter insertion', () => {
	it('inserts text_to_table adapter between incompatible source and transform nodes', () => {
		graphStore.hardResetGraph();
		const sourceId = graphStore.addNode('source', { x: 0, y: 0 });
		const transformId = graphStore.addNode('transform', { x: 300, y: 0 });

		const sourcePatch = graphStore.updateNodeConfig(sourceId, {
			params: {
				file_format: 'txt',
				output: { mode: 'text' }
			}
		});
		expect(sourcePatch.ok).toBe(true);

		const transformPatch = graphStore.updateNodeConfig(transformId, {
			params: {
				op: 'filter',
				filter: { expr: '' }
			}
		});
		expect(transformPatch.ok).toBe(true);

		const added = graphStore.addEdge({
			id: 'e_blocked',
			source: sourceId,
			target: transformId,
			data: { exec: 'idle' }
		} as any);
		expect(added.ok).toBe(true);
		expect(added.adapterKind).toBe('text_to_table');
		if (added.id) graphStore.deleteEdge(added.id);

		const inserted = graphStore.insertSchemaAdapterForEdgeConnection({
			source: sourceId,
			target: transformId
		});
		expect(inserted.ok).toBe(true);
		expect(inserted.adapterKind).toBe('text_to_table');

		const state = get(graphStore);
		const adapterNode = state.nodes.find((n) => n.id === inserted.adapterNodeId);
		expect(adapterNode).toBeTruthy();
		expect(adapterNode?.data.kind).toBe('transform');
		expect(adapterNode?.data.transformKind).toBe('text_to_table');
		expect(adapterNode?.data.ports?.in).toBe('text');
		expect(adapterNode?.data.ports?.out).toBe('table');

		const incoming = state.edges.find((e) => e.id === inserted.incomingEdgeId);
		const outgoing = state.edges.find((e) => e.id === inserted.outgoingEdgeId);
		expect(incoming?.source).toBe(sourceId);
		expect(incoming?.target).toBe(inserted.adapterNodeId);
		expect(outgoing?.source).toBe(inserted.adapterNodeId);
		expect(outgoing?.target).toBe(transformId);
	});
});
