import { get } from 'svelte/store';
import { describe, expect, it } from 'vitest';

import { __applyRunEventForTest, graphStore, type GraphState } from './graphStore';

describe('graphStore schema inference envelope', () => {
	it('derives inferred schema from source params during graph load', () => {
		graphStore.hardResetGraph();
		const applied = graphStore.loadGraphDocument(
			{
				nodes: [
					{
						id: 'src_txt',
						type: 'default',
						position: { x: 0, y: 0 },
						data: {
							kind: 'source',
							label: 'Source',
							sourceKind: 'file',
							params: { file_format: 'txt' },
							status: 'idle'
						}
					}
				],
				edges: []
			},
			'graph_schema_inference'
		);
		expect(applied.ok).toBe(true);

		const state = get(graphStore);
		const node = state.nodes.find((n) => n.id === 'src_txt');
		expect(node).toBeTruthy();
		expect((node?.data as any)?.schema?.inferredSchema?.source).toBe('sample');
		expect((node?.data as any)?.schema?.inferredSchema?.state).toBe('fresh');
		expect((node?.data as any)?.schema?.inferredSchema?.typedSchema?.type).toBe('text');
	});

	it('refreshes observed schema on successive node_output runtime events', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('source', { x: 8, y: 8 });
		const state = get(graphStore) as GraphState;

		const afterText = __applyRunEventForTest(
			state,
			{
				type: 'node_output',
				runId: 'run_schema_obs',
				at: '2026-03-10T12:00:00Z',
				nodeId,
				artifactId: 'artifact_text',
				payloadType: 'text',
				mimeType: 'text/plain',
				preview: 'hello'
			},
			'run_schema_obs'
		);
		expect((afterText.nodes.find((n) => n.id === nodeId)?.data as any)?.schema?.observedSchema?.typedSchema?.type).toBe(
			'text'
		);

		const afterJson = __applyRunEventForTest(
			afterText,
			{
				type: 'node_output',
				runId: 'run_schema_obs',
				at: '2026-03-10T12:00:05Z',
				nodeId,
				artifactId: 'artifact_json',
				payloadType: 'json',
				mimeType: 'application/json',
				preview: '{"ok":true}'
			},
			'run_schema_obs'
		);
		const observed = (afterJson.nodes.find((n) => n.id === nodeId)?.data as any)?.schema?.observedSchema;
		expect(observed?.source).toBe('runtime');
		expect(observed?.state).toBe('fresh');
		expect(observed?.typedSchema?.type).toBe('json');
	});

	it('uses priming artifact inferred fields on source node_output', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('source', { x: 4, y: 4 });
		const state = get(graphStore) as GraphState;
		const next = __applyRunEventForTest(
			state,
			{
				type: 'node_output',
				runId: 'run_schema_priming',
				at: '2026-03-11T12:00:00Z',
				nodeId,
				artifactId: 'artifact_table',
				payloadType: 'table',
				mimeType: 'text/csv',
				primingArtifact: {
					version: 1,
					inferred_schema: {
						type: 'table',
						fields: [
							{ name: 'id', type: 'int' },
							{ name: 'text', type: 'string' }
						]
					}
				}
			},
			'run_schema_priming'
		);
		const observed = (next.nodes.find((n) => n.id === nodeId)?.data as any)?.schema?.observedSchema?.typedSchema;
		expect(observed?.type).toBe('table');
		expect(Array.isArray(observed?.fields)).toBe(true);
		expect(observed?.fields?.[0]?.name).toBe('id');
	});

	it('uses runtime sourceObservability.table_columns for source observed schema fields', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('source', { x: 6, y: 6 });
		const state = get(graphStore) as GraphState;
		const next = __applyRunEventForTest(
			state,
			{
				type: 'node_output',
				runId: 'run_schema_source_obs_cols',
				at: '2026-03-11T12:05:00Z',
				nodeId,
				artifactId: 'artifact_table_cols',
				payloadType: 'table',
				mimeType: 'text/csv',
				sourceObservability: {
					source_kind: 'file',
					table_columns: [
						{ name: 'name', type: 'string' },
						{ name: 'age', type: 'int' }
					]
				}
			},
			'run_schema_source_obs_cols'
		);
		const observed = (next.nodes.find((n) => n.id === nodeId)?.data as any)?.schema?.observedSchema?.typedSchema;
		expect(observed?.type).toBe('table');
		expect(Array.isArray(observed?.fields)).toBe(true);
		expect(observed?.fields?.[0]?.name).toBe('name');
		expect(observed?.fields?.[1]?.name).toBe('age');
	});

	it('retains format-specific sourceObservability metadata on node outputs', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('source', { x: 10, y: 10 });
		const state = get(graphStore) as GraphState;
		const next = __applyRunEventForTest(
			state,
			{
				type: 'node_output',
				runId: 'run_schema_obs_format_meta',
				at: '2026-03-11T12:07:00Z',
				nodeId,
				artifactId: 'artifact_format_meta',
				payloadType: 'text',
				mimeType: 'application/pdf',
				sourceObservability: {
					source_kind: 'file',
					pdf_metadata: {
						requested_mode: 'hybrid',
						selected_pages: [0]
					}
				}
			},
			'run_schema_obs_format_meta'
		);
		expect((next.nodeOutputs[nodeId]?.sourceObservability as any)?.pdf_metadata?.requested_mode).toBe('hybrid');
	});
});

