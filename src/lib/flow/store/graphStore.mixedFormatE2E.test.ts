import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { __setStrictSchemaFeatureFlagsForTest } from '$lib/flow/schemaCapabilities';
import { graphStore } from './graphStore';

describe('schema-first mixed-format pipeline scenarios', () => {
	it('supports source(text) -> transform(table) with adapter insertion path', () => {
		__setStrictSchemaFeatureFlagsForTest({ STRICT_SCHEMA_EDGE_CHECKS_V2: true });
		graphStore.hardResetGraph();
		const sourceId = graphStore.addNode('source', { x: 0, y: 0 });
		const transformId = graphStore.addNode('transform', { x: 280, y: 0 });
		graphStore.updateNodeConfig(sourceId, {
			params: { file_format: 'txt', output: { mode: 'text' } }
		});
		graphStore.updateNodeConfig(transformId, {
			params: { op: 'filter', filter: { expr: '' } }
		});
		const added = graphStore.addEdge({
			id: 'e_text_table',
			source: sourceId,
			target: transformId,
			data: { exec: 'idle' }
		} as any);
		expect(added.ok).toBe(true);
		expect(added.adapterKind).toBe('text_to_table');
		if (added.id) graphStore.deleteEdge(added.id);
		const inserted = graphStore.insertSchemaAdapterForEdgeConnection({
			source: sourceId,
			target: transformId,
			adapterKind: 'text_to_table'
		});
		expect(inserted.ok).toBe(true);
	});

	it('supports tool(json) -> transform(table) with suggestion and adapter fix', () => {
		__setStrictSchemaFeatureFlagsForTest({ STRICT_SCHEMA_EDGE_CHECKS_V2: true });
		graphStore.hardResetGraph();
		const toolId = graphStore.addNode('tool', { x: 0, y: 0 });
		const transformId = graphStore.addNode('transform', { x: 280, y: 0 });
		graphStore.updateNodeConfig(toolId, {
			params: {
				provider: 'builtin',
				builtin: {
					toolId: 'core.noop',
					profileId: 'core',
					args: {}
				}
			}
		});
		graphStore.updateNodeConfig(transformId, {
			params: { op: 'filter', filter: { expr: '' } }
		});
		const added = graphStore.addEdge({
			id: 'e_json_table',
			source: toolId,
			target: transformId,
			data: { exec: 'idle' }
		} as any);
		expect(added.ok).toBe(true);
		expect(added.adapterKind).toBe('json_to_table');
		if (added.id) graphStore.deleteEdge(added.id);
		const inserted = graphStore.insertSchemaAdapterForEdgeConnection({
			source: toolId,
			target: transformId,
			adapterKind: 'json_to_table'
		});
		expect(inserted.ok).toBe(true);
	});

	it('preserves typed-table flow while enabling table_to_json downstream compatibility', () => {
		__setStrictSchemaFeatureFlagsForTest({ STRICT_SCHEMA_EDGE_CHECKS_V2: true });
		graphStore.hardResetGraph();
		const sourceId = graphStore.addNode('source', { x: 0, y: 0 });
		const transformId = graphStore.addNode('transform', { x: 220, y: 0 });
		const toolId = graphStore.addNode('tool', { x: 460, y: 0 });
		graphStore.updateNodeConfig(sourceId, {
			params: { file_format: 'csv', output: { mode: 'table' } }
		});
		graphStore.setTransformKind(transformId, 'table_to_json');
		graphStore.updateNodeConfig(toolId, {
			params: {
				provider: 'builtin',
				builtin: {
					toolId: 'core.noop',
					profileId: 'core',
					args: {}
				}
			}
		});

		const edge1 = graphStore.addEdge({
			id: 'e_table_json',
			source: sourceId,
			target: transformId,
			data: { exec: 'idle' }
		} as any);
		expect(edge1.ok).toBe(true);
		expect(edge1.adapterKind ?? null).toBeNull();

		const edge2 = graphStore.addEdge({
			id: 'e_json_tool',
			source: transformId,
			target: toolId,
			data: { exec: 'idle' }
		} as any);
		expect(edge2.ok).toBe(true);
		expect(edge2.adapterKind ?? null).toBeNull();

		const state = get(graphStore);
		expect(state.edges.length).toBe(2);
	});
});
