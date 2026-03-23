import { describe, expect, it } from 'vitest';

import { graphStore } from './graphStore';

describe('graphStore multi-edge same-handle schema guard', () => {
	it('blocks second inbound work edge when provided schemas differ on same target handle', () => {
		graphStore.hardResetGraph();
		const loaded = graphStore.loadGraphDocument(
			{
				nodes: [
					{
						id: 'src_a',
						type: 'default',
						position: { x: 0, y: 0 },
						data: {
							kind: 'source',
							sourceKind: 'file',
							params: { output: { mode: 'table' } },
							schema: {
								expectedSchema: {
									source: 'declared',
									typedSchema: { type: 'table', fields: [{ name: 'a', type: 'text', nullable: true }] }
								}
							},
							status: 'idle'
						}
					},
					{
						id: 'src_b',
						type: 'default',
						position: { x: 0, y: 120 },
						data: {
							kind: 'source',
							sourceKind: 'file',
							params: { output: { mode: 'table' } },
							schema: {
								expectedSchema: {
									source: 'declared',
									typedSchema: { type: 'table', fields: [{ name: 'b', type: 'text', nullable: true }] }
								}
							},
							status: 'idle'
						}
					},
					{
						id: 'dst',
						type: 'default',
						position: { x: 300, y: 60 },
						data: {
							kind: 'transform',
							transformKind: 'filter',
							params: { op: 'filter', filter: { expr: '' } },
							status: 'idle'
						}
					}
				],
				edges: []
			},
			'graph_multi_edge_same_handle_schema'
		);
		expect(loaded.ok).toBe(true);

		const edgeA = graphStore.addEdge({
			id: 'e_a',
			source: 'src_a',
			target: 'dst',
			sourceHandle: 'out',
			targetHandle: 'in',
			data: { exec: 'idle', mode: 'work' }
		} as any);
		expect(edgeA.ok).toBe(true);

		const edgeB = graphStore.addEdge({
			id: 'e_b',
			source: 'src_b',
			target: 'dst',
			sourceHandle: 'out',
			targetHandle: 'in',
			data: { exec: 'idle', mode: 'work' }
		} as any);
		expect(edgeB.ok).toBe(false);
		expect(String(edgeB.error ?? '')).toContain('identical schemas');
	});

	it('allows multiple inbound work edges when provided schemas are identical on same target handle', () => {
		graphStore.hardResetGraph();
		const loaded = graphStore.loadGraphDocument(
			{
				nodes: [
					{
						id: 'src_a',
						type: 'default',
						position: { x: 0, y: 0 },
						data: {
							kind: 'source',
							sourceKind: 'file',
							params: { output: { mode: 'table' } },
							schema: {
								expectedSchema: {
									source: 'declared',
									typedSchema: { type: 'table', fields: [{ name: 'a', type: 'text', nullable: true }] }
								}
							},
							status: 'idle'
						}
					},
					{
						id: 'src_b',
						type: 'default',
						position: { x: 0, y: 120 },
						data: {
							kind: 'source',
							sourceKind: 'file',
							params: { output: { mode: 'table' } },
							schema: {
								expectedSchema: {
									source: 'declared',
									typedSchema: { type: 'table', fields: [{ name: 'a', type: 'text', nullable: true }] }
								}
							},
							status: 'idle'
						}
					},
					{
						id: 'dst',
						type: 'default',
						position: { x: 300, y: 60 },
						data: {
							kind: 'transform',
							transformKind: 'filter',
							params: { op: 'filter', filter: { expr: '' } },
							status: 'idle'
						}
					}
				],
				edges: []
			},
			'graph_multi_edge_same_handle_schema_allowed'
		);
		expect(loaded.ok).toBe(true);

		const edgeA = graphStore.addEdge({
			id: 'e_a',
			source: 'src_a',
			target: 'dst',
			sourceHandle: 'out',
			targetHandle: 'in',
			data: { exec: 'idle', mode: 'work' }
		} as any);
		expect(edgeA.ok).toBe(true);

		const edgeB = graphStore.addEdge({
			id: 'e_b',
			source: 'src_b',
			target: 'dst',
			sourceHandle: 'out',
			targetHandle: 'in',
			data: { exec: 'idle', mode: 'work' }
		} as any);
		expect(edgeB.ok).toBe(true);
	});
});
