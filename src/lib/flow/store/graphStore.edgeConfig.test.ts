import { describe, expect, it } from 'vitest';

import { graphStore } from './graphStore';

describe('graphStore edge config', () => {
	it('updates edge mode/fatal/queue config', () => {
		graphStore.hardResetGraph();
		const src = graphStore.addNode('source', { x: 0, y: 0 });
		const dst = graphStore.addNode('transform', { x: 200, y: 0 });
		graphStore.updateNodeConfig(src, { params: { file_format: 'txt', output: { mode: 'text' } } });
		graphStore.updateNodeConfig(dst, { params: { op: 'text_to_table', text_to_table: { mode: 'lines', column: 'text' } } });
		const added = graphStore.addEdge({
			id: 'e_cfg',
			source: src,
			target: dst,
			data: { exec: 'idle', mode: 'work' }
		} as any);
		expect(added.ok).toBe(true);

		const updated = graphStore.updateEdgeConfig('e_cfg', {
			mode: 'work',
			fatal: true,
			queue: { max: 50, overflow: 'block' },
			work: { item_mode: 'json_items', max_items: 25 }
		});
		expect(updated.ok).toBe(true);
	});

	it('rejects incompatible mode changes', () => {
		graphStore.hardResetGraph();
		const src = graphStore.addNode('source', { x: 0, y: 0 });
		const dst = graphStore.addNode('model', { x: 200, y: 0 });
		const added = graphStore.addEdge({
			id: 'e_param',
			source: src,
			sourceHandle: 'out',
			target: dst,
			targetHandle: 'param_filters',
			data: { exec: 'idle', mode: 'param' }
		} as any);
		expect(added.ok).toBe(true);

		const incompatible = graphStore.updateEdgeConfig('e_param', { mode: 'work' });
		expect(incompatible.ok).toBe(false);
		expect(String(incompatible.error ?? '')).toContain('incompatible');
	});
});
