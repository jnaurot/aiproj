import { describe, expect, it } from 'vitest';

import { graphStore } from './graphStore';

describe('graphStore adapter guidance', () => {
	it('returns adapter suggestion metadata when connection requires adaptation', () => {
		graphStore.hardResetGraph();
		const sourceId = graphStore.addNode('source', { x: 0, y: 0 });
		const transformId = graphStore.addNode('transform', { x: 280, y: 0 });
		graphStore.updateNodeConfig(sourceId, {
			params: {
				file_format: 'txt',
				output: { mode: 'text' }
			}
		});
		graphStore.updateNodeConfig(transformId, {
			params: {
				op: 'filter',
				filter: { expr: '' }
			}
		});

		const added = graphStore.addEdge({
			id: 'e_adapter_guidance',
			source: sourceId,
			target: transformId,
			data: { exec: 'idle', mode: 'work' }
		}) as any;
		expect(added.ok).toBe(true);
		expect(added.adapterKind).toBe('text_to_table');
		expect(String(added.suggestion ?? '')).toContain('adapter');
	});
});
