import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { graphStore, __hardResetGraphForTest, __stripToDTOForTest } from './graphStore';
import { emptySchemaPlaneState, recomputeSchemaPlane } from './graphStore.schemaPlane';

describe('graphStore schema plane integration', () => {
	it('hard reset state has schemaPlane field', () => {
		const reset = __hardResetGraphForTest({} as any, 'graph-schema-plane');
		expect(reset.schemaPlane).toBeTruthy();
		expect(reset.schemaPlane).toEqual(emptySchemaPlaneState());
	});

	it('addNode creates node schema entry synchronously', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('source', { x: 0, y: 0 });
		const state = get(graphStore as any);
		expect(state.schemaPlane?.nodeSchemas?.[nodeId]).toBeTruthy();
	});

	it('addEdge creates edge schema entry', () => {
		graphStore.hardResetGraph();
		const a = graphStore.addNode('source', { x: 0, y: 0 });
		const b = graphStore.addNode('transform', { x: 10, y: 10 });
		const res = graphStore.addEdge({
			id: 'e_schema',
			source: a,
			target: b,
			targetHandle: 'in',
			data: { exec: 'idle' }
		} as any);
		expect(res.ok).toBe(true);
		const state = get(graphStore as any);
		expect(state.schemaPlane?.edgeSchemas?.e_schema).toBeTruthy();
	});

	it('removeNode removes schema entry', () => {
		graphStore.hardResetGraph();
		const n = graphStore.addNode('source', { x: 0, y: 0 });
		graphStore.deleteNode(n);
		const state = get(graphStore as any);
		expect(state.schemaPlane?.nodeSchemas?.[n]).toBeUndefined();
	});

	it('schemaPlane is not persisted to graph dto', () => {
		graphStore.hardResetGraph();
		const s = get(graphStore as any);
		const dto = __stripToDTOForTest(s.nodes, s.edges, s.graphId, s.checkpointRegistry);
		expect((dto as any).schemaPlane).toBeUndefined();
	});

	it('edge validation reports warning for opaque upstream schema', () => {
		graphStore.hardResetGraph();
		const source = graphStore.addNode('model', { x: 0, y: 0 });
		const target = graphStore.addNode('transform', { x: 80, y: 0 });
		graphStore.addEdge({
			id: 'e_opaque',
			source,
			target,
			targetHandle: 'in',
			data: { exec: 'idle' }
		} as any);
		const validation = (graphStore as any).getEdgeSchemaValidationState?.('e_opaque');
		expect(validation?.state).toBe('warning');
	});

	it('edge validation reports error when schema error exists on edge path', () => {
		graphStore.hardResetGraph();
		graphStore.loadGraphDocument({
			nodes: [
				{
					id: 'a',
					position: { x: 0, y: 0 },
					data: { kind: 'transform', transformKind: 'filter', status: 'idle', label: 'A', params: {} }
				},
				{
					id: 'b',
					position: { x: 100, y: 0 },
					data: { kind: 'transform', transformKind: 'filter', status: 'idle', label: 'B', params: {} }
				}
			],
			edges: [
				{ id: 'e1', source: 'a', target: 'b', targetHandle: 'in', data: { exec: 'idle' } },
				{ id: 'e2', source: 'b', target: 'a', targetHandle: 'in', data: { exec: 'idle' } }
			]
		} as any);
		const validation = (graphStore as any).getEdgeSchemaValidationState?.('e1');
		expect(validation?.state).toBe('error');
		expect(String(validation?.code ?? '')).toBe('CYCLE_DETECTED');
	});

	it('loadGraphDocument triggers immediate schema recomputation', () => {
		graphStore.hardResetGraph();
		graphStore.loadGraphDocument({
			nodes: [
				{
					id: 'n_source',
					position: { x: 0, y: 0 },
					data: {
						kind: 'source',
						sourceKind: 'file',
						label: 'source',
						status: 'idle',
						params: {}
					}
				}
			],
			edges: []
		} as any);
		const state = get(graphStore as any);
		expect(state.schemaPlane?.nodeSchemas?.n_source).toBeTruthy();
	});

	it('component node derives schema from cached internal draft graph', () => {
		const state = __hardResetGraphForTest({} as any, 'graph-component-schema');
		const componentNode = {
			id: 'cmp_parent',
			position: { x: 0, y: 0 },
			data: {
				kind: 'component',
				label: 'Component',
				status: 'idle',
				params: {
					componentRef: {
						componentId: 'cmp_id',
						revisionId: 'rev_1'
					}
				}
			}
		};
		const internalSource = {
			id: 'inner_src',
			position: { x: 0, y: 0 },
			data: {
				kind: 'source',
				sourceKind: 'file',
				label: 'src',
				status: 'idle',
				params: {
					priming: {
						sample_schema: {
							fields: [{ name: 'text', type: 'string', nullable: false }]
						}
					}
				}
			}
		};
		const recomputed = recomputeSchemaPlane({
			...state,
			nodes: [componentNode] as any,
			edges: [],
			componentContractDraftCache: {
				'cmp_id@rev_1': {
					__graphDraft: {
						nodes: [internalSource],
						edges: []
					}
				}
			}
		} as any);
		expect(recomputed.nodeSchemas.cmp_parent?.ok).toBe(true);
		if (recomputed.nodeSchemas.cmp_parent?.ok) {
			expect(recomputed.nodeSchemas.cmp_parent.output.columns[0]?.name).toBe('text');
		}
	});
});
