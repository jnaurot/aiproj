import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { graphStore } from './graphStore';

describe('graphStore edge mode compatibility', () => {
	it('blocks work mode edge targeting a param handle', () => {
		graphStore.hardResetGraph();
		const src = graphStore.addNode('source', { x: 0, y: 0 });
		const dst = graphStore.addNode('model', { x: 200, y: 0 });

		const result = graphStore.addEdge({
			id: 'e_mode_block',
			source: src,
			target: dst,
			targetHandle: 'param_filters',
			data: { exec: 'idle', mode: 'work' }
		} as any);
		expect(result.ok).toBe(false);
		expect(String(result.error ?? '')).toContain('Edge mode');
	});

	it('allows param mode edge targeting a param handle', () => {
		graphStore.hardResetGraph();
		const src = graphStore.addNode('source', { x: 0, y: 0 });
		const dst = graphStore.addNode('model', { x: 200, y: 0 });

		const result = graphStore.addEdge({
			id: 'e_mode_ok',
			source: src,
			target: dst,
			targetHandle: 'param_filters',
			data: { exec: 'idle', mode: 'param' }
		} as any);
		expect(result.ok).toBe(true);
	});

	it('infers legacy edge mode from param handle when mode is missing', () => {
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
						position: { x: 200, y: 0 },
						data: { kind: 'model', label: 'dst', params: {}, status: 'idle' }
					}
				],
				edges: [
					{
						id: 'e_legacy_param',
						source: 'src',
						sourceHandle: 'out',
						target: 'dst',
						targetHandle: 'param_filters',
						data: { exec: 'idle' }
					}
				]
			},
			'graph_mode_infer_ui'
		);
		expect(loaded.ok).toBe(true);
		const edge = get(graphStore).edges.find((item) => item.id === 'e_legacy_param');
		expect(String((edge?.data as any)?.mode ?? '')).toBe('param');
	});

	it('blocks preflight when selected mode is incompatible with known port affinities', () => {
		graphStore.hardResetGraph();
		const src = graphStore.addNode('source', { x: 0, y: 0 });
		const dst = graphStore.addNode('model', { x: 200, y: 0 });

		const preflight = graphStore.preflightConnection({
			source: src,
			target: dst,
			sourceHandle: 'out',
			targetHandle: 'param_filters',
			mode: 'work'
		});
		expect(preflight.ok).toBe(false);
		expect(String((preflight as any).error ?? '')).toContain('incompatible');
	});
});
