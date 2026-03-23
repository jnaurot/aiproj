import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { graphStore } from './graphStore';

describe('graphStore removes legacy expectedInputSchema singleton', () => {
	it('setNodeExpectedInputSchemaForHandle only writes expectedInputSchemas', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('model', { x: 10, y: 10 });

		const result = graphStore.setNodeExpectedInputSchemaForHandle(nodeId, 'in', {
			type: 'json',
			fields: []
		});
		expect((result as any)?.ok).toBe(true);

		const node = get(graphStore).nodes.find((n) => n.id === nodeId);
		expect((node?.data as any)?.schema?.expectedInputSchemas?.in?.typedSchema?.type).toBe('json');
		expect((node?.data as any)?.schema?.expectedInputSchema).toBeUndefined();
	});

	it('loadGraphDocument migrates legacy expectedInputSchema then drops it', () => {
		graphStore.hardResetGraph();
		const loaded = graphStore.loadGraphDocument(
			{
				nodes: [
					{
						id: 'n_migrate',
						type: 'model',
						position: { x: 0, y: 0 },
						data: {
							kind: 'model',
							label: 'Migrate',
							params: {},
							status: 'idle',
							schema: {
								expectedInputSchema: {
									typedSchema: { type: 'json', fields: [] },
									source: 'declared',
									state: 'fresh'
								}
							}
						}
					}
				],
				edges: []
			},
			'graph_no_singleton_input_schema'
		);
		expect(loaded.ok).toBe(true);
		const node = get(graphStore).nodes.find((item) => item.id === 'n_migrate');
		expect((node?.data as any)?.schema?.expectedInputSchemas?.in?.typedSchema?.type).toBe('json');
		expect((node?.data as any)?.schema?.expectedInputSchema).toBeUndefined();
	});
});
