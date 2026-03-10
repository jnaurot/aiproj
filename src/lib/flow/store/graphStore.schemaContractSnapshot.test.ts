import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { __buildNodeSchemaContractSnapshotForTest, graphStore } from './graphStore';

describe('graphStore schema contract snapshot', () => {
	it('reports clean state for compatible edges', () => {
		graphStore.hardResetGraph();
		const sourceId = graphStore.addNode('source', { x: 0, y: 0 });
		const transformId = graphStore.addNode('transform', { x: 200, y: 0 });
		graphStore.updateNodeConfig(sourceId, {
			params: { file_format: 'csv', output_mode: 'table' },
			ports: { in: null, out: 'table' }
		});
		graphStore.updateNodeConfig(transformId, {
			params: { op: 'filter', filter: { expr: '' } },
			ports: { in: 'table', out: 'table' }
		});
		const added = graphStore.addEdge({
			id: 'e_clean',
			source: sourceId,
			target: transformId,
			data: { exec: 'idle' }
		} as any);
		expect(added.ok).toBe(true);
		const snapshot = __buildNodeSchemaContractSnapshotForTest(get(graphStore), transformId);
		expect(snapshot.status).toBe('clean');
	});

	it('reports warning state for lossy coercion edges', () => {
		graphStore.hardResetGraph();
		const sourceId = graphStore.addNode('source', { x: 0, y: 0 });
		const llmId = graphStore.addNode('llm', { x: 220, y: 0 });
		graphStore.updateNodeConfig(sourceId, {
			params: { output_mode: 'json' },
			ports: { in: null, out: 'json' }
		});
		graphStore.updateNodeConfig(llmId, { ports: { in: 'text', out: 'text' } });
		const added = graphStore.addEdge({
			id: 'e_warn',
			source: sourceId,
			target: llmId,
			data: { exec: 'idle' }
		} as any);
		expect(added.ok).toBe(true);
		const snapshot = __buildNodeSchemaContractSnapshotForTest(get(graphStore), llmId);
		expect(snapshot.status).toBe('warning');
	});

	it('reports error state for missing required columns', () => {
		graphStore.hardResetGraph();
		const sourceId = graphStore.addNode('source', { x: 0, y: 0 });
		const transformId = graphStore.addNode('transform', { x: 220, y: 0 });
		graphStore.updateNodeConfig(sourceId, {
			params: { file_format: 'txt', output_mode: 'table' },
			ports: { in: null, out: 'table' }
		});
		graphStore.updateNodeConfig(transformId, {
			params: { op: 'select', select: { mode: 'include', columns: ['id'] } },
			ports: { in: 'table', out: 'table' }
		});
		const state = get(graphStore);
		const synthetic = {
			...state,
			edges: [
				{
					id: 'e_error',
					source: sourceId,
					target: transformId,
					data: { exec: 'idle' }
				} as any
			]
		};
		const snapshot = __buildNodeSchemaContractSnapshotForTest(synthetic as any, transformId);
		expect(snapshot.status).toBe('error');
	});
});
