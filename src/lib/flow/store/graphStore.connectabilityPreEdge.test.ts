import { describe, expect, it } from 'vitest';

import { graphStore } from './graphStore';

describe('graphStore preflight connection', () => {
	it('defers handle-specific checks when target handle is not selected yet', () => {
		graphStore.hardResetGraph();
		const loaded = graphStore.loadGraphDocument(
			{
				nodes: [
					{
						id: 'src',
						type: 'source',
						position: { x: 0, y: 0 },
						data: { kind: 'source', label: 'src', sourceKind: 'file', params: {}, status: 'idle' }
					},
					{
						id: 'dst',
						type: 'model',
						position: { x: 220, y: 0 },
						data: {
							kind: 'model',
							label: 'dst',
							params: {},
							status: 'idle',
							portContracts: {
								in: {
									default: { affinity: 'work' },
									param_filters: { affinity: 'param' }
								}
							}
						}
					}
				],
				edges: []
			},
			'graph_preflight_deferred'
		);
		expect(loaded.ok).toBe(true);

		const preflight = graphStore.preflightConnection({
			source: 'src',
			target: 'dst',
			sourceHandle: 'out',
			targetHandle: null
		});
		expect(preflight.ok).toBe(true);
		expect((preflight as any).deferred).toBe(true);
	});

	it('fails preflight when target handle is not declared', () => {
		graphStore.hardResetGraph();
		const loaded = graphStore.loadGraphDocument(
			{
				nodes: [
					{
						id: 'src',
						type: 'source',
						position: { x: 0, y: 0 },
						data: { kind: 'source', label: 'src', sourceKind: 'file', params: {}, status: 'idle' }
					},
					{
						id: 'dst',
						type: 'model',
						position: { x: 220, y: 0 },
						data: {
							kind: 'model',
							label: 'dst',
							params: {},
							status: 'idle',
							portContracts: {
								in: {
									param_filters: { affinity: 'param' }
								}
							}
						}
					}
				],
				edges: []
			},
			'graph_preflight_declared'
		);
		expect(loaded.ok).toBe(true);

		const preflight = graphStore.preflightConnection({
			source: 'src',
			target: 'dst',
			sourceHandle: 'out',
			targetHandle: 'in'
		});
		expect(preflight.ok).toBe(false);
		expect(String((preflight as any).error ?? '')).toContain('not declared');
	});
});
