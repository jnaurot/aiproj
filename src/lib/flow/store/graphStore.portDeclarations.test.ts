import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { graphStore } from './graphStore';

describe('graphStore port declarations', () => {
	it('loads node portDeclarations and blocks second inbound edge when cardinality=one', () => {
		graphStore.hardResetGraph();
		const loaded = graphStore.loadGraphDocument(
			{
				nodes: [
					{
						id: 'src_a',
						type: 'tool',
						position: { x: 0, y: 0 },
						data: {
							kind: 'tool',
							label: 'A',
							params: { provider: 'builtin', builtin: { toolId: 'noop' } },
							status: 'idle'
						}
					},
					{
						id: 'src_b',
						type: 'tool',
						position: { x: 0, y: 100 },
						data: {
							kind: 'tool',
							label: 'B',
							params: { provider: 'builtin', builtin: { toolId: 'noop' } },
							status: 'idle'
						}
					},
					{
						id: 'dst',
						type: 'tool',
						position: { x: 200, y: 0 },
						data: {
							kind: 'tool',
							label: 'Dst',
							params: { provider: 'builtin', builtin: { toolId: 'noop' } },
							status: 'idle',
							portDeclarations: {
								in: {
									in: { plane: 'work', required: true, cardinality: 'one', behavior: 'single_item' }
								},
								out: {
									out: { plane: 'work', required: false, cardinality: 'many' }
								}
							}
						}
					}
				],
				edges: []
			},
			'graph_port_declarations_cardinality'
		);
		expect(loaded.ok).toBe(true);

		const first = graphStore.addEdge({
			id: 'e1',
			source: 'src_a',
			target: 'dst',
			sourceHandle: 'out',
			targetHandle: 'in',
			data: { exec: 'idle', mode: 'work' }
		} as any);
		expect(first.ok).toBe(true);

		const second = graphStore.preflightConnection({
			source: 'src_b',
			target: 'dst',
			sourceHandle: 'out',
			targetHandle: 'in',
			mode: 'work'
		});
		expect(second.ok).toBe(false);
		expect(String((second as any).error ?? '')).toContain('only one inbound edge');

		const state = get(graphStore as any);
		const dst = state.nodes.find((n: any) => n.id === 'dst');
		expect(dst?.data?.portDeclarations?.in?.in?.cardinality).toBe('one');
		expect(dst?.data?.portContracts?.in?.in?.affinity).toBe('work');
	});
});
