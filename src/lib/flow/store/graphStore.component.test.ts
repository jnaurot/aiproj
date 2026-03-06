import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { graphStore } from './graphStore';

describe('graphStore component integration', () => {
	it('applies component revision to node and derives immutable ports from API contract', async () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('component', { x: 20, y: 20 });
		graphStore.selectNode(nodeId);

		const originalFetch = globalThis.fetch;
		(globalThis as any).fetch = async (input: RequestInfo | URL) => {
			const url = String(input);
			if (url.includes('/api/components/cmp_test/revisions/crev_1')) {
				return new Response(
					JSON.stringify({
						schemaVersion: 1,
						componentId: 'cmp_test',
						revisionId: 'crev_1',
						parentRevisionId: null,
						createdAt: '2026-03-06T00:00:00Z',
						message: 'seed',
						revisionSchemaVersion: 1,
						checksum: 'abc',
						definition: {
							graph: { nodes: [], edges: [] },
							api: {
								inputs: [
									{
										name: 'in_data',
										portType: 'table',
										required: true,
										typedSchema: { type: 'table', fields: [] },
									},
								],
								outputs: [
									{
										name: 'out_data',
										portType: 'text',
										required: true,
										typedSchema: { type: 'text', fields: [] },
									},
								],
							},
							configSchema: {},
						},
					}),
					{ status: 200 }
				);
			}
			if (url.includes('/api/components/cmp_test/revisions?')) {
				return new Response(
					JSON.stringify({
						schemaVersion: 1,
						componentId: 'cmp_test',
						revisions: [
							{
								revisionId: 'crev_1',
								componentId: 'cmp_test',
								parentRevisionId: null,
								createdAt: '2026-03-06T00:00:00Z',
								message: 'seed',
								schemaVersion: 1,
								checksum: 'abc',
							},
						],
					}),
					{ status: 200 }
				);
			}
			return new Response('{}', { status: 200 });
		};

		try {
			const res = await graphStore.applyComponentRevisionToNode(nodeId, 'cmp_test', 'crev_1');
			expect((res as any)?.ok).toBe(true);

			const state = get(graphStore);
			const node = state.nodes.find((n) => n.id === nodeId);
			expect(node).toBeTruthy();
			expect(node?.data?.ports?.in).toBe('table');
			expect(node?.data?.ports?.out).toBe('text');
			expect((node?.data?.params as any)?.componentRef?.componentId).toBe('cmp_test');
			expect((node?.data?.params as any)?.componentRef?.revisionId).toBe('crev_1');
		} finally {
			(globalThis as any).fetch = originalFetch;
		}
	});
});
