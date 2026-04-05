import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { graphStore } from '$lib/flow/store/graphStore';

describe('component authoring roundtrip', () => {
	it('keeps component edit session and restores parent graph on return', async () => {
		graphStore.hardResetGraph();
		const parentNodeId = graphStore.addNode('component', { x: 20, y: 20 });
		const originalFetch = globalThis.fetch;
		(globalThis as any).fetch = async (input: RequestInfo | URL) => {
			const url = String(input);
			if (url.includes('/api/components/cmp_roundtrip/revisions/crev_1')) {
				return new Response(
					JSON.stringify({
						schemaVersion: 1,
						componentId: 'cmp_roundtrip',
						revisionId: 'crev_1',
						definition: {
							graph: { nodes: [{ id: 'internal_1', data: { kind: 'source', label: 'Source' } }], edges: [] },
							api: { inputs: [], outputs: [] },
							exposureRegistry: []
						}
					}),
					{ status: 200 }
				);
			}
			return new Response('{}', { status: 200 });
		};
		try {
			const opened = await graphStore.openComponentRevisionForEditing('cmp_roundtrip', 'crev_1', parentNodeId);
			expect((opened as any)?.ok).toBe(true);
			const inSession = get(graphStore);
			expect(inSession.editingContext).toBe('component');
			expect(inSession.componentEditSession?.componentId).toBe('cmp_roundtrip');

			const returned = graphStore.returnFromComponentEditSession();
			expect((returned as any)?.ok).toBe(true);
			const restored = get(graphStore);
			expect(restored.editingContext).toBe('graph');
			expect(restored.nodes.some((n) => n.id === parentNodeId)).toBe(true);
		} finally {
			(globalThis as any).fetch = originalFetch;
		}
	});
});
