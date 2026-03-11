import { describe, expect, it, vi } from 'vitest';
import { get } from 'svelte/store';

import { graphStore } from './graphStore';
import { selectedNode } from './graphStore';
import { displayStatusFromBinding } from './runScope';
import { getHeaderCachePill } from '$lib/flow/components/inspectorCachePill';

function setupSourceNode(): string {
	graphStore.hardResetGraph();
	const nodeId = graphStore.addNode('source', { x: 10, y: 10 });
	graphStore.selectNode(nodeId);
	return nodeId;
}

describe('graphStore cache pill transitions around snapshot uploads', () => {
	it('shows cached green before new upload and hides cache pill after new upload with no rehydration', async () => {
		const nodeId = setupSourceNode();
		const sidA = 'a'.repeat(64);
		const sidB = 'b'.repeat(64);
		let resolveCalls = 0;
		const fetchSpy = vi.spyOn(globalThis, 'fetch').mockImplementation(async (input: RequestInfo | URL) => {
			const url = String(input);
			if (!url.includes('/api/runs/resolve/source')) {
				return new Response(JSON.stringify({}), { status: 200 });
			}
			resolveCalls += 1;
			if (resolveCalls === 1) {
				return new Response(
					JSON.stringify({
						graphId: 'graph_test',
						nodeId,
						execKey: 'exec_a',
						artifactId: 'artifact_a',
						cacheHit: true,
						artifact: { artifactId: 'artifact_a', mimeType: 'text/plain', payloadType: 'text' }
					}),
					{ status: 200 }
				);
			}
			return new Response(
				JSON.stringify({
					graphId: 'graph_test',
					nodeId,
					execKey: 'exec_b',
					artifactId: null,
					cacheHit: false
				}),
				{ status: 200 }
			);
		});

		try {
			await graphStore.commitSnapshotSelection({
				snapshotId: sidA,
				snapshotMetadata: {
					snapshotId: sidA,
					originalFilename: 'first.txt',
					byteSize: 10
				},
				recentSnapshotIds: [sidA],
				recentSnapshots: [{ id: sidA, filename: 'first.txt', size: 10 }]
			});

			let state = get(graphStore);
			let binding = state.nodeBindings[nodeId];
			let out = state.nodeOutputs[nodeId];
			let status = displayStatusFromBinding(binding);
			let pill = getHeaderCachePill(out, binding, status);
			expect(pill?.className).toContain('st-succeeded');
			expect(pill?.label).toBe('cached');

			await graphStore.commitSnapshotSelection({
				snapshotId: sidB,
				snapshotMetadata: {
					snapshotId: sidB,
					originalFilename: 'second.txt',
					byteSize: 12
				},
				recentSnapshotIds: [sidB, sidA],
				recentSnapshots: [
					{ id: sidB, filename: 'second.txt', size: 12 },
					{ id: sidA, filename: 'first.txt', size: 10 }
				]
			});

			state = get(graphStore);
			binding = state.nodeBindings[nodeId];
			out = state.nodeOutputs[nodeId];
			status = displayStatusFromBinding(binding);
			pill = getHeaderCachePill(out, binding, status);
			expect(status).toBe('stale');
			expect(pill).toBeNull();
		} finally {
			fetchSpy.mockRestore();
			// keep singleton store clean for subsequent tests
			graphStore.hardResetGraph();
			graphStore.selectNode(null);
			expect(get(selectedNode)).toBeNull();
		}
	});
});

