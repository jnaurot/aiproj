import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import {
	__computeEdgeSchemaConstraintsForTest,
	__stripToDTOForTest,
	graphStore
} from './graphStore';

describe('graphStore edge contract persistence', () => {
	it('persists required input schema for transform->model edges across save/load', () => {
		graphStore.hardResetGraph();
		const sourceId = graphStore.addNode('source', { x: 0, y: 0 });
		const transformId = graphStore.addNode('transform', { x: 220, y: 0 });
		const modelId = graphStore.addNode('model', { x: 460, y: 0 });

		expect(
			graphStore.updateNodeConfig(sourceId, {
				params: { file_format: 'json', output: { mode: 'json' } }
			})
		).toMatchObject({ ok: true });

		expect(
			graphStore.updateNodeConfig(transformId, {
				params: { op: 'json_to_table', json_to_table: { orient: 'object', rowsKey: 'jobs' } }
			})
		).toMatchObject({ ok: true });

		expect(
			graphStore.updateNodeConfig(modelId, {
				params: { inputEncoding: 'table_canonical', output: { mode: 'text' } },
				schema: {
					expectedInputSchemas: {
						in: {
							source: 'declared',
							state: 'fresh',
							typedSchema: { type: 'table', fields: [] }
						}
					}
				}
			} as any)
		).toMatchObject({ ok: true });

		expect(
			graphStore.addEdge({
				id: 'e_source_to_transform',
				source: sourceId,
				target: transformId,
				data: { exec: 'idle' }
			} as any)
		).toMatchObject({ ok: true });

		expect(
			graphStore.addEdge({
				id: 'e_transform_to_model',
				source: transformId,
				target: modelId,
				data: { exec: 'idle' }
			} as any)
		).toMatchObject({ ok: true });

		const state = get(graphStore);
		const dto = __stripToDTOForTest(state.nodes as any, state.edges as any, 'graph_edge_contract_persistence');
		const persistedEdge = (dto.edges as any[]).find((edge) => String(edge?.id ?? '') === 'e_transform_to_model') as any;
		expect(persistedEdge?.data?.contract?.payload?.target?.type).toBe('table');
		expect(String(persistedEdge?.data?.contract?.snapshot?.targetSchemaFingerprint ?? '')).toContain('"type":"table"');

		const constraints = __computeEdgeSchemaConstraintsForTest(state.nodes as any, dto.edges as any);
		expect(constraints.e_transform_to_model?.snapshotDrift).toBe(false);

		const reload = graphStore.loadGraphDocument(
			{
				nodes: dto.nodes as any,
				edges: dto.edges as any
			},
			'graph_edge_contract_persistence_reloaded'
		);
		expect(reload.ok).toBe(true);

		const reloadedState = get(graphStore);
		const reloadedConstraints = __computeEdgeSchemaConstraintsForTest(
			reloadedState.nodes as any,
			reloadedState.edges as any
		);
		expect(reloadedConstraints.e_transform_to_model?.snapshotDrift).toBe(false);
	});
});
