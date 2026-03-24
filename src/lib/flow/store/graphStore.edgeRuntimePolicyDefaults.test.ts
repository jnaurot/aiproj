import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { graphStore } from './graphStore';

describe('graphStore edge runtime policy defaults', () => {
	it('defaults queue policy to fifo and validates policy updates', () => {
		graphStore.hardResetGraph();
		const srcId = graphStore.addNode('tool', { x: 0, y: 0 });
		const dstId = graphStore.addNode('tool', { x: 180, y: 0 });
		const addResult = graphStore.addEdge({
			source: srcId,
			target: dstId,
			sourceHandle: 'out',
			targetHandle: 'in',
			data: { exec: 'idle', mode: 'work' }
		} as any);
		expect(addResult.ok).toBe(true);
		const edgeId = String((addResult as any)?.id ?? '').trim();
		expect(edgeId.length).toBeGreaterThan(0);

		let state = get(graphStore as any) as any;
		let edge = (state.edges ?? []).find((candidate: any) => String(candidate.id ?? '') === edgeId);
		expect(String(edge?.data?.queue?.policy ?? '')).toBe('fifo');

		const patchOk = graphStore.updateEdgeConfig(edgeId, { queue: { policy: 'round_robin' } });
		expect(patchOk.ok).toBe(true);
		state = get(graphStore as any) as any;
		edge = (state.edges ?? []).find((candidate: any) => String(candidate.id ?? '') === edgeId);
		expect(String(edge?.data?.queue?.policy ?? '')).toBe('round_robin');

		const patchInvalid = graphStore.updateEdgeConfig(edgeId, { queue: { policy: 'bogus' as any } });
		expect(patchInvalid.ok).toBe(false);
		expect(String((patchInvalid as any)?.error ?? '')).toContain('arbitration policy');
	});

	it('auto-defaults work item_mode by source payload type for new edges', () => {
		graphStore.hardResetGraph();

		const addAndReadItemMode = (sourceType: 'table' | 'json' | 'text') => {
			const srcId = graphStore.addNode('transform', { x: 0, y: 0 });
			const dstId = graphStore.addNode('transform', { x: 220, y: 0 });
			expect(graphStore.setNodeExpectedSchema(srcId, { type: sourceType, fields: [] }).ok).toBe(true);
			expect(graphStore.setNodeExpectedInputSchemaForHandle(dstId, 'in', { type: sourceType, fields: [] }).ok).toBe(true);
			const edgeId = `e_${sourceType}`;
			const added = graphStore.addEdge({
				id: edgeId,
				source: srcId,
				target: dstId,
				sourceHandle: 'out',
				targetHandle: 'in',
				data: { exec: 'idle', mode: 'work' }
			} as any);
			expect(added.ok).toBe(true);
			const state = get(graphStore as any) as any;
			const edge = (state.edges ?? []).find((candidate: any) => String(candidate.id ?? '') === edgeId);
			return String(edge?.data?.work?.item_mode ?? '');
		};

		expect(addAndReadItemMode('table')).toBe('table_rows');
		expect(addAndReadItemMode('json')).toBe('json_items');
		expect(addAndReadItemMode('text')).toBe('artifact');
	});
});
