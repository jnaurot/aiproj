import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { __buildNodeSchemaContractSnapshotForTest, graphStore } from './graphStore';

describe('graphStore schema contract snapshot', () => {
	it('reports clean state for compatible edges', () => {
		graphStore.hardResetGraph();
		const sourceId = graphStore.addNode('source', { x: 0, y: 0 });
		const transformId = graphStore.addNode('transform', { x: 200, y: 0 });
		graphStore.updateNodeConfig(sourceId, {
			params: { file_format: 'csv', output: { mode: 'table' } }
		});
		graphStore.updateNodeConfig(transformId, {
			params: { op: 'filter', filter: { expr: '' } }
		});
		const added = graphStore.addEdge({
			id: 'e_clean',
			source: sourceId,
			target: transformId,
			data: { exec: 'idle' }
		} as any);
		expect(added.ok).toBe(true);
		const snapshot = __buildNodeSchemaContractSnapshotForTest(get(graphStore), transformId);
		expect(snapshot.status).toBe('clean');
	});

	it('reports warning state for lossy coercion edges', () => {
		graphStore.hardResetGraph();
		const loaded = graphStore.loadGraphDocument(
			{
				nodes: [
					{
						id: 'src_warn',
						type: 'default',
						position: { x: 0, y: 0 },
						data: {
							kind: 'source',
							label: 'Source',
							sourceKind: 'api',
							params: {},
							status: 'idle'
						}
					},
					{
						id: 'llm_warn',
						type: 'default',
						position: { x: 220, y: 0 },
						data: {
							kind: 'llm',
							label: 'LLM',
							params: { output: { mode: 'text' }, coercion_policy: 'allow_lossy' },
							status: 'idle'
						}
					}
				],
				edges: [{ id: 'e_warn', source: 'src_warn', target: 'llm_warn', data: { exec: 'idle' } as any }]
			},
			'graph_schema_warn'
		);
		expect(loaded.ok).toBe(true);
		const snapshot = __buildNodeSchemaContractSnapshotForTest(get(graphStore), 'llm_warn');
		expect(snapshot.status).toBe('warning');
	});

	it('reports error state for missing required columns', () => {
		graphStore.hardResetGraph();
		const sourceId = graphStore.addNode('source', { x: 0, y: 0 });
		const transformId = graphStore.addNode('transform', { x: 220, y: 0 });
		graphStore.updateNodeConfig(sourceId, {
			params: { file_format: 'txt', output: { mode: 'table' } }
		});
		graphStore.updateNodeConfig(transformId, {
			params: { op: 'select', select: { mode: 'include', columns: ['id'] } }
		});
		const state = get(graphStore);
		const synthetic = {
			...state,
			edges: [
				{
					id: 'e_error',
					source: sourceId,
					target: transformId,
					data: { exec: 'idle' }
				} as any
			]
		};
		const snapshot = __buildNodeSchemaContractSnapshotForTest(synthetic as any, transformId);
		expect(snapshot.status).toBe('error');
	});

	it('emits stable diagnostic summaries for warning vs error snapshots', () => {
		graphStore.hardResetGraph();
		const warnLoaded = graphStore.loadGraphDocument(
			{
				nodes: [
					{
						id: 'src_warn_snapshot',
						type: 'default',
						position: { x: 0, y: 0 },
						data: {
							kind: 'source',
							label: 'Source',
							sourceKind: 'api',
							params: {},
							status: 'idle'
						}
					},
					{
						id: 'llm_warn_snapshot',
						type: 'default',
						position: { x: 220, y: 0 },
						data: {
							kind: 'llm',
							label: 'LLM',
							params: { output: { mode: 'text' }, coercion_policy: 'allow_lossy' },
							status: 'idle'
						}
					}
				],
				edges: [
					{
						id: 'e_warn_snapshot',
						source: 'src_warn_snapshot',
						target: 'llm_warn_snapshot',
						data: { exec: 'idle' } as any
					}
				]
			},
			'graph_schema_warn_snapshot'
		);
		expect(warnLoaded.ok).toBe(true);
		const warnSnapshot = __buildNodeSchemaContractSnapshotForTest(
			get(graphStore),
			'llm_warn_snapshot'
		);
		const warnSummary = {
			status: warnSnapshot.status,
			edges: (warnSnapshot.edges ?? []).map((edge: any) => ({
				severity: String(edge?.severity ?? ''),
				adapterKind: edge?.adapterKind ?? null
			}))
		};
		expect(warnSummary).toMatchInlineSnapshot(`
			{
			  "edges": [
			    {
			      "adapterKind": null,
			      "severity": "warning",
			    },
			  ],
			  "status": "warning",
			}
		`);

		graphStore.hardResetGraph();
		const txtSourceId = graphStore.addNode('source', { x: 0, y: 0 });
		const transformId = graphStore.addNode('transform', { x: 220, y: 0 });
		graphStore.updateNodeConfig(txtSourceId, {
			params: { file_format: 'txt', output: { mode: 'table' } }
		});
		graphStore.updateNodeConfig(transformId, {
			params: { op: 'select', select: { mode: 'include', columns: ['id'] } }
		});
		const errState = get(graphStore);
		const syntheticErrorState = {
			...errState,
			edges: [
				{
					id: 'e_error_snapshot',
					source: txtSourceId,
					target: transformId,
					data: { exec: 'idle' }
				} as any
			]
		};
		const errorSnapshot = __buildNodeSchemaContractSnapshotForTest(
			syntheticErrorState as any,
			transformId
		);
		const errorSummary = {
			status: errorSnapshot.status,
			edges: (errorSnapshot.edges ?? []).map((edge: any) => ({
				severity: String(edge?.severity ?? ''),
				adapterKind: edge?.adapterKind ?? null
			}))
		};
		expect(errorSummary).toMatchInlineSnapshot(`
			{
			  "edges": [
			    {
			      "adapterKind": null,
			      "severity": "error",
			    },
			  ],
			  "status": "error",
			}
		`);
	});
});
