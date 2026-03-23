import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { graphStore } from './graphStore';

describe('graphStore port model migration compatibility', () => {
	it('hydrates legacy graph docs with port declarations, processing policy defaults, and edge snapshots', () => {
		graphStore.hardResetGraph();
		const loaded = graphStore.loadGraphDocument(
			{
				nodes: [
					{
						id: 'src',
						type: 'source',
						data: {
							kind: 'source',
							params: { output: { mode: 'text' } },
							processingPolicy: { consumeMode: 'read_once', batchSize: 2 }
						}
					},
					{
						id: 'dst',
						type: 'model',
						data: { kind: 'model', params: { model: 'stub' } }
					}
				],
				edges: [
					{
						id: 'e1',
						source: 'src',
						target: 'dst',
						data: {
							mode: 'work',
							contract: {
								payload: {
									source: { type: 'text' },
									target: { type: 'text' }
								}
							}
						}
					}
				]
			} as any,
			'graph_port_mig'
		);
		expect(loaded.ok).toBe(true);
		const state = get(graphStore as any);
		const src = state.nodes.find((node: any) => node.id === 'src');
		expect(Boolean(src?.data?.processingPolicy?.read_once)).toBe(true);
		expect(src?.data?.processingPolicy?.consume_mode).toBe('once');
		expect(src?.data?.portDeclarations?.in).toBeTruthy();
		const edge = state.edges.find((candidate: any) => candidate.id === 'e1');
		expect(String(edge?.data?.contract?.snapshot?.sourceSchemaFingerprint ?? '')).not.toBe('');
		expect(String(edge?.data?.contract?.snapshot?.targetSchemaFingerprint ?? '')).not.toBe('');
	});
});

