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

	it('supports mixed mode wiring for jobflow-style work + param inputs', () => {
		__setStrictSchemaFeatureFlagsForTest({ STRICT_SCHEMA_EDGE_CHECKS_V2: true });
		graphStore.hardResetGraph();
		const jobsId = graphStore.addNode('source', { x: 0, y: 0 });
		const resumeId = graphStore.addNode('source', { x: 0, y: 180 });
		const selectId = graphStore.addNode('model', { x: 300, y: 0 });
		const generateId = graphStore.addNode('model', { x: 620, y: 0 });

		graphStore.updateNodeConfig(jobsId, {
			params: { file_format: 'json', output: { mode: 'json' } }
		});
		graphStore.updateNodeConfig(resumeId, {
			params: { file_format: 'txt', output: { mode: 'json' } }
		});
		graphStore.setNodeExpectedInputSchema(selectId, { type: 'json', fields: [] });
		graphStore.setNodeExpectedSchema(selectId, { type: 'json', fields: [] });
		graphStore.setNodeExpectedInputSchema(generateId, { type: 'json', fields: [] });
		graphStore.setNodeExpectedSchema(generateId, { type: 'json', fields: [] });

		const workEdge = graphStore.addEdge({
			id: 'e_jobs_to_select_work',
			source: jobsId,
			target: selectId,
			targetHandle: 'in',
			data: { exec: 'idle', mode: 'work' }
		} as any);
		expect(workEdge.ok).toBe(true);

		const paramEdge = graphStore.addEdge({
			id: 'e_resume_to_select_param',
			source: resumeId,
			target: selectId,
			targetHandle: 'param_filters',
			data: { exec: 'idle', mode: 'param' }
		} as any);
		expect(paramEdge.ok).toBe(true);
		expect(paramEdge.adapterKind ?? null).toBeNull();

		const downstreamWork = graphStore.addEdge({
			id: 'e_select_to_generate_work',
			source: selectId,
			target: generateId,
			targetHandle: 'in',
			data: { exec: 'idle', mode: 'work' }
		} as any);
		expect(downstreamWork.ok).toBe(true);

		const state = get(graphStore);
		expect(state.edges.length).toBe(3);
		expect(state.edges.map((e) => String((e.data as any)?.mode || 'work')).sort()).toEqual([
			'param',
			'work',
			'work'
		]);
	});

	it('supports mixed work/param/control wiring without false type mismatch', () => {
		__setStrictSchemaFeatureFlagsForTest({ STRICT_SCHEMA_EDGE_CHECKS_V2: true });
		graphStore.hardResetGraph();
		const srcWork = graphStore.addNode('source', { x: 0, y: 0 });
		const srcParam = graphStore.addNode('source', { x: 0, y: 180 });
		const srcControl = graphStore.addNode('source', { x: 0, y: 360 });
		const dst = graphStore.addNode('model', { x: 280, y: 0 });

		graphStore.updateNodeConfig(srcWork, {
			params: { file_format: 'json', output: { mode: 'json' } }
		});
		graphStore.updateNodeConfig(srcParam, {
			params: { file_format: 'txt', output: { mode: 'json' } }
		});
		graphStore.updateNodeConfig(srcControl, {
			params: { file_format: 'txt', output: { mode: 'json' } }
		});
		graphStore.setNodeExpectedInputSchemaForHandle(dst, 'in', { type: 'json', fields: [] });
		graphStore.setNodeExpectedInputSchemaForHandle(dst, 'param_filters', { type: 'json', fields: [] });
		graphStore.updateNodePortDeclaration(srcControl, 'out', 'control_out', {
			plane: 'control',
			cardinality: 'one'
		});

		const edgeWork = graphStore.addEdge({
			id: 'e_mixed_work',
			source: srcWork,
			target: dst,
			targetHandle: 'in',
			data: { exec: 'idle', mode: 'work' }
		} as any);
		const edgeParam = graphStore.addEdge({
			id: 'e_mixed_param',
			source: srcParam,
			target: dst,
			targetHandle: 'param_filters',
			data: { exec: 'idle', mode: 'param' }
		} as any);
		const edgeControl = graphStore.addEdge({
			id: 'e_mixed_control',
			source: srcControl,
			sourceHandle: 'control_out',
			target: dst,
			targetHandle: 'control_in',
			data: { exec: 'idle', mode: 'control' }
		} as any);

		expect(edgeWork.ok).toBe(true);
		expect(edgeParam.ok).toBe(true);
		expect(edgeControl.ok).toBe(true);
		const state = get(graphStore);
		expect(state.edges).toHaveLength(3);
		expect(state.edges.map((e) => String((e.data as any)?.mode ?? 'work')).sort()).toEqual([
			'control',
			'param',
			'work'
		]);
	});
});
