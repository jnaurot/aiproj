import { get } from 'svelte/store';
import { describe, expect, it } from 'vitest';

import { graphStore } from './graphStore';

describe('graphStore expected input schemas by handle', () => {
	it('sets and clears expected input schema per handle', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('transform', { x: 40, y: 40 });

		const saveIn = graphStore.setNodeExpectedInputSchemaForHandle(nodeId, 'in', {
			type: 'json',
			fields: []
		});
		expect((saveIn as any)?.ok).toBe(true);

		const saveParam = graphStore.setNodeExpectedInputSchemaForHandle(nodeId, 'param_config', {
			type: 'json',
			fields: []
		});
		expect((saveParam as any)?.ok).toBe(true);

		const afterSave = get(graphStore);
		const nodeAfterSave = afterSave.nodes.find((n) => n.id === nodeId);
		expect((nodeAfterSave?.data as any)?.schema?.expectedInputSchemas?.in?.typedSchema?.type).toBe('json');
		expect((nodeAfterSave?.data as any)?.schema?.expectedInputSchemas?.param_config?.typedSchema?.type).toBe(
			'json'
		);
		expect((nodeAfterSave?.data as any)?.schema?.expectedInputSchema).toBeUndefined();

		const clearParam = graphStore.setNodeExpectedInputSchemaForHandle(nodeId, 'param_config', null);
		expect((clearParam as any)?.ok).toBe(true);

		const afterClear = get(graphStore);
		const nodeAfterClear = afterClear.nodes.find((n) => n.id === nodeId);
		expect((nodeAfterClear?.data as any)?.schema?.expectedInputSchemas?.param_config).toBeUndefined();
		expect((nodeAfterClear?.data as any)?.schema?.expectedInputSchemas?.in?.typedSchema?.type).toBe('json');
	});

	it('migrates legacy expectedInputSchema into expectedInputSchemas.in on graph load', () => {
		graphStore.hardResetGraph();
		const loaded = graphStore.loadGraphDocument(
			{
				nodes: [
					{
						id: 'n_model_migrate',
						type: 'model',
						position: { x: 0, y: 0 },
						data: {
							kind: 'model',
							label: 'Model',
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
			'graph_expected_input_migrate'
		);
		expect(loaded.ok).toBe(true);
		const node = get(graphStore).nodes.find((item) => item.id === 'n_model_migrate');
		expect((node?.data as any)?.schema?.expectedInputSchemas?.in?.typedSchema?.type).toBe('json');
		expect((node?.data as any)?.schema?.expectedInputSchema).toBeUndefined();
	});
});
