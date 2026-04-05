import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { graphStore } from '$lib/flow/store/graphStore';

describe('component return-to-graph projection', () => {
	it('projects published/debug profiles onto component params when applying revision', async () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('component', { x: 20, y: 20 });
		const originalFetch = globalThis.fetch;
		(globalThis as any).fetch = async (input: RequestInfo | URL) => {
			const url = String(input);
			if (url.includes('/api/components/cmp_projection/revisions/crev_1')) {
				return new Response(
					JSON.stringify({
						schemaVersion: 1,
						componentId: 'cmp_projection',
						revisionId: 'crev_1',
						definition: {
							graph: { nodes: [], edges: [] },
							api: { inputs: [], outputs: [{ name: 'out_data', typedSchema: { type: 'json', fields: [] } }] },
							exposureRegistry: [
								{
									handle_id: 'data_out::out_data',
									alias: 'out_data',
									internal_source_path: 'out:out_data',
									kind: 'data_output',
									native_contract: { type: 'json', fields: [] },
									exposed: true,
									published: true,
									debug_visible: false
								}
							]
						}
					}),
					{ status: 200 }
				);
			}
			if (url.includes('/api/components/cmp_projection/revisions?')) {
				return new Response(
					JSON.stringify({ schemaVersion: 1, componentId: 'cmp_projection', revisions: [{ revisionId: 'crev_1' }] }),
					{ status: 200 }
				);
			}
			return new Response('{}', { status: 200 });
		};
		try {
			const applied = await graphStore.applyComponentRevisionToNode(nodeId, 'cmp_projection', 'crev_1');
			expect((applied as any)?.ok).toBe(true);
			const state = get(graphStore);
			const node = state.nodes.find((n) => n.id === nodeId);
			const params = (node?.data?.params ?? {}) as any;
			expect(Array.isArray(params.exposureRegistry)).toBe(true);
			expect(Array.isArray(params.published_profile)).toBe(true);
			expect(Array.isArray(params.debug_profile)).toBe(true);
			expect(params.published_profile.length).toBeGreaterThan(0);
		} finally {
			(globalThis as any).fetch = originalFetch;
		}
	});
});

