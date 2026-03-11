import { describe, expect, it } from 'vitest';

import { graphStore } from './graphStore';

describe('graphStore edge schema compatibility', () => {
	it('allows adding coercible edges when schema types are compatible', () => {
		graphStore.hardResetGraph();
		const sourceId = graphStore.addNode('source', { x: 0, y: 0 });
		const transformId = graphStore.addNode('transform', { x: 200, y: 0 });

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
			id: 'e_schema_ok',
			source: sourceId,
			target: transformId,
			data: { exec: 'idle' }
		} as any);
		expect(added.ok).toBe(true);
	});

	it('blocks adding edges when required columns are missing despite same port type', () => {
		graphStore.hardResetGraph();
		const sourceId = graphStore.addNode('source', { x: 0, y: 0 });
		const transformId = graphStore.addNode('transform', { x: 200, y: 0 });

		const sourcePatch = graphStore.updateNodeConfig(sourceId, {
			params: {
				file_format: 'txt',
				output: { mode: 'table' }
			}
		});
		expect(sourcePatch.ok).toBe(true);

		const transformPatch = graphStore.updateNodeConfig(transformId, {
			params: {
				op: 'select',
				select: {
					mode: 'include',
					columns: ['id'],
					strict: true
				}
			}
		});
		expect(transformPatch.ok).toBe(true);

		const added = graphStore.addEdge({
			id: 'e_schema_bad',
			source: sourceId,
			target: transformId,
			data: { exec: 'idle' }
		} as any);
		expect(added.ok).toBe(false);
		expect(String(added.error ?? '')).toContain('Missing required typed schema coverage');
	});

	it('uses schema solver compatibility even when source port metadata is absent', () => {
		graphStore.hardResetGraph();
		const applied = graphStore.loadGraphDocument(
			{
				nodes: [
					{
						id: 'cmp_json',
						type: 'default',
						position: { x: 0, y: 0 },
						data: {
							kind: 'component',
							label: 'Component',
							componentKind: 'graph_component',
							params: {
								componentRef: {
									componentId: 'cmp_test',
									revisionId: 'crev_1',
									apiVersion: 'v1'
								},
								api: {
									inputs: [],
									outputs: [
										{
											name: 'default',
											payloadType: 'json',
											required: true,
											typedSchema: { type: 'json', fields: [] }
										}
									]
								},
								bindings: { inputs: {}, config: {}, outputs: {} }
							},
							status: 'idle'
						}
					},
					{
						id: 'xfm_json_to_table',
						type: 'default',
						position: { x: 220, y: 0 },
						data: {
							kind: 'transform',
							transformKind: 'json_to_table',
							params: { op: 'json_to_table' },
							status: 'idle'
						}
					}
				],
				edges: []
			},
			'graph_schema_connect_without_ports'
		);
		expect(applied.ok).toBe(true);

		const added = graphStore.addEdge({
			id: 'e_schema_no_ports',
			source: 'cmp_json',
			sourceHandle: 'default',
			target: 'xfm_json_to_table',
			targetHandle: 'in',
			data: { exec: 'idle' }
		} as any);
		expect(added.ok).toBe(true);
	});

	it('respects coercion policy by blocking lossy conversions unless allow_lossy is set', () => {
		graphStore.hardResetGraph();
		const sourceId = graphStore.addNode('source', { x: 0, y: 0 });
		const llmId = graphStore.addNode('llm', { x: 240, y: 0 });

		const sourcePatch = graphStore.setSourceKind(sourceId, 'api');
		expect(sourcePatch.ok).toBe(true);

		const llmPatch = graphStore.updateNodeConfig(llmId, {
			params: {
				output: { mode: 'text' }
			}
		});
		expect(llmPatch.ok).toBe(true);

		const blocked = graphStore.addEdge({
			id: 'e_lossy_blocked',
			source: sourceId,
			target: llmId,
			data: { exec: 'idle' }
		} as any);
		expect(blocked.ok).toBe(false);

		const reload = graphStore.loadGraphDocument(
			{
				nodes: [
					{
						id: sourceId,
						type: 'default',
						position: { x: 0, y: 0 },
						data: {
							kind: 'source',
							sourceKind: 'api',
							params: {},
							status: 'idle'
						}
					},
					{
						id: llmId,
						type: 'default',
						position: { x: 240, y: 0 },
						data: {
							kind: 'llm',
							llmKind: 'openai',
							params: { output: { mode: 'text' }, coercion_policy: 'allow_lossy' },
							status: 'idle'
						}
					}
				],
				edges: []
			},
			'graph_lossy_policy'
		);
		expect(reload.ok).toBe(true);

		const allowed = graphStore.addEdge({
			id: 'e_lossy_allowed',
			source: sourceId,
			target: llmId,
			data: { exec: 'idle' }
		} as any);
		expect(allowed.ok).toBe(true);
	});
});

