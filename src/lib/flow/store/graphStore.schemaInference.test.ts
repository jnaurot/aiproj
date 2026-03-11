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
});

