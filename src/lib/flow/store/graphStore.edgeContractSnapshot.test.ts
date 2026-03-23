import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { graphStore } from './graphStore';

describe('graphStore edge contract snapshot', () => {
	it('persists resolved contract snapshot on edge creation', () => {
		graphStore.hardResetGraph();
		const src = graphStore.addNode('tool', { x: 0, y: 0 });
		const dst = graphStore.addNode('tool', { x: 240, y: 0 });
		const added = graphStore.addEdge({
			id: 'e_snapshot',
			source: src,
			target: dst,
			sourceHandle: 'out',
			targetHandle: 'in',
			data: { exec: 'idle', mode: 'work' }
		} as any);
		expect(added.ok).toBe(true);
		const state = get(graphStore as any);
		const edge = state.edges.find((candidate: any) => candidate.id === 'e_snapshot');
		const snapshot = (edge?.data as any)?.contract?.snapshot;
		expect(String(snapshot?.sourceSchemaFingerprint ?? '')).not.toBe('');
		expect(String(snapshot?.targetSchemaFingerprint ?? '')).not.toBe('');
		expect(typeof snapshot?.compatible).toBe('boolean');
		expect(String(snapshot?.decision ?? '')).not.toBe('');
	});
});

