import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { graphStore } from '../store/graphStore';

describe('NodeInspector expected output schema scope', () => {
	it('does not mutate expected input schemas when expected output schema is edited', () => {
		graphStore.hardResetGraph();
		const srcId = graphStore.addNode('transform', { x: 0, y: 0 });
		const dstId = graphStore.addNode('transform', { x: 220, y: 0 });

		expect(
			graphStore.updateNodeConfig(srcId, {
				params: { op: 'filter', filter: { expr: '' } }
			}).ok
		).toBe(true);
		expect(
			graphStore.updateNodeConfig(dstId, {
				params: { op: 'filter', filter: { expr: '' } }
			}).ok
		).toBe(true);

		graphStore.setNodeExpectedInputSchemaForHandle(dstId, 'in', { type: 'text', fields: [] });
		graphStore.setNodeExpectedSchema(dstId, { type: 'json', fields: [] });
		graphStore.setNodeExpectedSchema(srcId, { type: 'text', fields: [] });

		const state = get(graphStore as any);
		const dst = state.nodes.find((node) => node.id === dstId);
		expect((dst?.data as any)?.schema?.expectedSchema?.typedSchema?.type).toBe('json');
		expect((dst?.data as any)?.schema?.expectedInputSchemas?.in?.typedSchema?.type).toBe('text');

		const added = graphStore.addEdge({
			id: 'e_output_scope',
			source: srcId,
			target: dstId,
			sourceHandle: 'out',
			targetHandle: 'in',
			data: { exec: 'idle', mode: 'work' }
		} as any);
		expect(added.ok).toBe(true);
	});
});
