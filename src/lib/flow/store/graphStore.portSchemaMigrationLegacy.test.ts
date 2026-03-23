import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { graphStore } from './graphStore';

describe('graphStore legacy processing policy migration', () => {
	it('normalizes legacy consumeMode aliases while preserving semantics', () => {
		graphStore.hardResetGraph();
		const loaded = graphStore.loadGraphDocument(
			{
				nodes: [
					{
						id: 'n1',
						type: 'default',
						position: { x: 0, y: 0 },
						data: {
							kind: 'transform',
							params: { op: 'filter', filter: { expr: '' } },
							processingPolicy: {
								consumeMode: 'read_once',
								batchSize: 2,
								maxInflight: 3,
								input_handles: {
									in: { consumeMode: 'continuous', batchSize: 4, maxInflight: 5 }
								}
							},
							status: 'idle'
						}
					}
				],
				edges: []
			},
			'graph_port_policy_migration_legacy'
		);
		expect(loaded.ok).toBe(true);
		const state = get(graphStore as any);
		const node = state.nodes.find((n: any) => n.id === 'n1');
		expect(node?.data?.processingPolicy?.consume_mode).toBe('once');
		expect(node?.data?.processingPolicy?.batch_size).toBe(2);
		expect(node?.data?.processingPolicy?.max_inflight).toBe(3);
		expect(node?.data?.processingPolicy?.input_handles?.in?.consume_mode).toBe('single_item');
		expect(node?.data?.processingPolicy?.input_handles?.in?.batch_size).toBe(4);
		expect(node?.data?.processingPolicy?.input_handles?.in?.max_inflight).toBe(5);
	});
});
