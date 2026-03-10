import { describe, expect, it } from 'vitest';

import { graphStore } from './graphStore';

describe('graphStore edge schema compatibility', () => {
	it('allows adding coercible edges when schema types are compatible', () => {
		graphStore.hardResetGraph();
		const sourceId = graphStore.addNode('source', { x: 0, y: 0 });
		const transformId = graphStore.addNode('transform', { x: 200, y: 0 });

		const sourcePatch = graphStore.updateNodeConfig(sourceId, {
			params: {
				file_format: 'txt',
				output_mode: 'text'
			},
			ports: { in: null, out: 'text' }
		});
		expect(sourcePatch.ok).toBe(true);

		const transformPatch = graphStore.updateNodeConfig(transformId, {
			params: {
				op: 'filter',
				filter: { expr: '' }
			},
			ports: { in: 'table', out: 'table' }
		});
		expect(transformPatch.ok).toBe(true);

		const added = graphStore.addEdge({
			id: 'e_schema_ok',
			source: sourceId,
			target: transformId,
			data: { exec: 'idle' }
		} as any);
		expect(added.ok).toBe(true);
	});

	it('blocks adding edges when required columns are missing despite same port type', () => {
		graphStore.hardResetGraph();
		const sourceId = graphStore.addNode('source', { x: 0, y: 0 });
		const transformId = graphStore.addNode('transform', { x: 200, y: 0 });

		const sourcePatch = graphStore.updateNodeConfig(sourceId, {
			params: {
				file_format: 'txt',
				output_mode: 'text'
			},
			ports: { in: null, out: 'table' }
		});
		expect(sourcePatch.ok).toBe(true);

		const transformPatch = graphStore.updateNodeConfig(transformId, {
			params: {
				op: 'select',
				select: {
					mode: 'include',
					columns: ['id'],
					strict: true
				}
			},
			ports: { in: 'table', out: 'table' }
		});
		expect(transformPatch.ok).toBe(true);

		const added = graphStore.addEdge({
			id: 'e_schema_bad',
			source: sourceId,
			target: transformId,
			data: { exec: 'idle' }
		} as any);
		expect(added.ok).toBe(false);
		expect(String(added.error ?? '')).toContain('Missing required columns');
	});
});
