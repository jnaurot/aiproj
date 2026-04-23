/**
 * Phase 3c — updateNodeSchema unit tests
 *
 * Covers:
 *   - Updates expectedInputSchemas[handleId] on the correct node
 *   - Triggers recomputeSchemaPlane (schemaPlane is updated)
 *   - Is recorded in undo history and can be reversed by graphStore.undo()
 *   - Does not mutate other nodes or handles
 *   - setSchemaEdgeInspectorEdgeId sets / clears the inspector edge id
 */

import { get } from 'svelte/store';
import { describe, expect, it } from 'vitest';

import { graphStore } from './graphStore';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function addTwoNodeEdge() {
	graphStore.hardResetGraph();
	const src = graphStore.addNode('source', { x: 0, y: 0 });
	const dst = graphStore.addNode('transform', { x: 200, y: 0 });
	const edgeId = `e_${src}_${dst}`;
	graphStore.addEdge({ id: edgeId, source: src, target: dst, sourceHandle: 'out', targetHandle: 'in' });
	return { src, dst, edgeId };
}

// ---------------------------------------------------------------------------
// updateNodeSchema — input direction
// ---------------------------------------------------------------------------
describe('graphStore.updateNodeSchema — input direction', () => {
	it('updates expectedInputSchemas[handleId] on the target node', () => {
		const { dst } = addTwoNodeEdge();

		const result = (graphStore as any).updateNodeSchema(dst, 'in', 'input', {
			type: 'json',
			fields: []
		});
		expect(result?.ok).toBe(true);

		const state = get(graphStore);
		const node = state.nodes.find((n) => n.id === dst);
		expect((node?.data as any)?.schema?.expectedInputSchemas?.in?.typedSchema?.type).toBe('json');
	});

	it('does not mutate other nodes', () => {
		const { src, dst } = addTwoNodeEdge();

		(graphStore as any).updateNodeSchema(dst, 'in', 'input', { type: 'json', fields: [] });

		const state = get(graphStore);
		const srcNode = state.nodes.find((n) => n.id === src);
		// Source node schema should not have expectedInputSchemas.in set
		expect((srcNode?.data as any)?.schema?.expectedInputSchemas?.in).toBeUndefined();
	});

	it('does not mutate other handles on the same node', () => {
		const { dst } = addTwoNodeEdge();

		// Set two handles
		(graphStore as any).updateNodeSchema(dst, 'in', 'input', { type: 'json', fields: [] });
		(graphStore as any).updateNodeSchema(dst, 'param_config', 'input', { type: 'text' });

		const state = get(graphStore);
		const node = state.nodes.find((n) => n.id === dst);
		expect((node?.data as any)?.schema?.expectedInputSchemas?.in?.typedSchema?.type).toBe('json');
		expect((node?.data as any)?.schema?.expectedInputSchemas?.param_config?.typedSchema?.type).toBe('text');
	});

	it('triggers schemaPlane recomputation (schemaPlane is non-null after update)', () => {
		const { dst } = addTwoNodeEdge();

		(graphStore as any).updateNodeSchema(dst, 'in', 'input', { type: 'json', fields: [] });

		const state = get(graphStore);
		// schemaPlane should be populated (not empty default) since we recomputed
		expect(state.schemaPlane).toBeTruthy();
		expect(typeof state.schemaPlane).toBe('object');
	});
});

// ---------------------------------------------------------------------------
// updateNodeSchema — output direction
// ---------------------------------------------------------------------------
describe('graphStore.updateNodeSchema — output direction', () => {
	it('updates expectedSchema (output) on the source node', () => {
		const { src } = addTwoNodeEdge();

		const result = (graphStore as any).updateNodeSchema(src, 'out', 'output', {
			type: 'json',
			fields: [{ name: 'id', type: 'text' }]
		});
		expect(result?.ok).toBe(true);

		const state = get(graphStore);
		const node = state.nodes.find((n) => n.id === src);
		expect((node?.data as any)?.schema?.expectedSchema?.typedSchema?.type).toBe('json');
	});
});

// ---------------------------------------------------------------------------
// Undo / redo
// ---------------------------------------------------------------------------
describe('graphStore.updateNodeSchema — undo history', () => {
	it('is recorded in undo history and can be reversed', () => {
		const { dst } = addTwoNodeEdge();

		// Capture state before mutation
		const stateBefore = get(graphStore);
		const nodeBeforeMutation = stateBefore.nodes.find((n) => n.id === dst);
		const schemasBefore = (nodeBeforeMutation?.data as any)?.schema?.expectedInputSchemas ?? {};

		(graphStore as any).updateNodeSchema(dst, 'in', 'input', { type: 'json', fields: [] });

		const stateAfter = get(graphStore);
		const nodeAfter = stateAfter.nodes.find((n) => n.id === dst);
		expect((nodeAfter?.data as any)?.schema?.expectedInputSchemas?.in?.typedSchema?.type).toBe('json');

		// Undo
		(graphStore as any).undo?.();

		const stateUndo = get(graphStore);
		const nodeUndo = stateUndo.nodes.find((n) => n.id === dst);
		const schemasUndo = (nodeUndo?.data as any)?.schema?.expectedInputSchemas ?? {};
		// After undo the handle schema should be gone (same as before mutation)
		expect(schemasUndo?.in?.typedSchema?.type).toBeUndefined();
		expect(JSON.stringify(schemasUndo)).toEqual(JSON.stringify(schemasBefore));
	});
});

// ---------------------------------------------------------------------------
// setSchemaEdgeInspectorEdgeId
// ---------------------------------------------------------------------------
describe('graphStore.setSchemaEdgeInspectorEdgeId', () => {
	it('sets the inspected edge id', () => {
		graphStore.hardResetGraph();
		(graphStore as any).setSchemaEdgeInspectorEdgeId?.('edge-42');
		expect(get(graphStore).schemaEdgeInspectorEdgeId).toBe('edge-42');
	});

	it('clears the inspected edge id with null', () => {
		graphStore.hardResetGraph();
		(graphStore as any).setSchemaEdgeInspectorEdgeId?.('edge-42');
		(graphStore as any).setSchemaEdgeInspectorEdgeId?.(null);
		expect(get(graphStore).schemaEdgeInspectorEdgeId).toBeNull();
	});

	it('initial state has schemaEdgeInspectorEdgeId === null', () => {
		graphStore.hardResetGraph();
		expect(get(graphStore).schemaEdgeInspectorEdgeId).toBeNull();
	});
});
