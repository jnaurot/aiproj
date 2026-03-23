import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { __buildNodeSchemaContractSnapshotForTest, graphStore } from './graphStore';

describe('graphStore contract diff view model', () => {
	it('exposes snapshot and current fingerprints for inspector contract diff panel', () => {
		graphStore.hardResetGraph();
		const src = graphStore.addNode('source', { x: 0, y: 0 });
		const dst = graphStore.addNode('model', { x: 300, y: 0 });
		graphStore.addEdge({
			id: 'e_diff',
			source: src,
			target: dst,
			sourceHandle: 'out',
			targetHandle: 'in',
			data: {
				exec: 'idle',
				mode: 'work',
				contract: {
					snapshot: {
						sourceSchemaFingerprint: '{"type":"json"}',
						targetSchemaFingerprint: '{"type":"json"}',
						compatible: true,
						decision: 'native'
					}
				}
			}
		} as any);
		const state = get(graphStore as any);
		const snapshot = __buildNodeSchemaContractSnapshotForTest(state as any, dst);
		const edge = (snapshot.edges ?? []).find((candidate: any) => candidate.edgeId === 'e_diff');
		expect(edge).toBeTruthy();
		expect(String(edge?.snapshotSourceSchemaFingerprint ?? '')).not.toBe('');
		expect(String(edge?.currentSourceSchemaFingerprint ?? '')).not.toBe('');
		expect(typeof edge?.snapshotDrift).toBe('boolean');
	});
});

