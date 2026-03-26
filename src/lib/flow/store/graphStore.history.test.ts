import { get } from 'svelte/store';
import { describe, expect, it } from 'vitest';

import { graphStore } from './graphStore';
import { __stripToDTOForTest } from './graphStore';

describe('graphStore history foundation', () => {
	it('supports undo/redo across graph edits', () => {
		graphStore.hardResetGraph();
		graphStore.clearHistory();

		expect(graphStore.canUndo()).toBe(false);
		expect(graphStore.canRedo()).toBe(false);

		const id = graphStore.addNode('source', { x: 10, y: 10 });
		expect(get(graphStore).nodes.some((n) => n.id === id)).toBe(true);
		expect(graphStore.canUndo()).toBe(true);

		const undone = graphStore.undo();
		expect(undone.ok).toBe(true);
		expect(get(graphStore).nodes.some((n) => n.id === id)).toBe(false);
		expect(graphStore.canRedo()).toBe(true);

		const redone = graphStore.redo();
		expect(redone.ok).toBe(true);
		expect(get(graphStore).nodes.some((n) => n.id === id)).toBe(true);
	});

	it('clears redo stack after new edit post-undo', () => {
		graphStore.hardResetGraph();
		graphStore.clearHistory();

		const first = graphStore.addNode('source', { x: 0, y: 0 });
		expect(get(graphStore).nodes.some((n) => n.id === first)).toBe(true);

		expect(graphStore.undo().ok).toBe(true);
		expect(graphStore.canRedo()).toBe(true);

		const second = graphStore.addNode('transform', { x: 120, y: 0 });
		expect(get(graphStore).nodes.some((n) => n.id === second)).toBe(true);
		expect(graphStore.canRedo()).toBe(false);
	});

	it('enforces history cap', () => {
		graphStore.hardResetGraph();
		graphStore.setHistoryLimit(2);
		graphStore.clearHistory();

		const a = graphStore.addNode('source', { x: 0, y: 0 });
		const b = graphStore.addNode('transform', { x: 120, y: 0 });
		const c = graphStore.addNode('model', { x: 240, y: 0 });
		expect(get(graphStore).nodes.some((n) => n.id === a)).toBe(true);
		expect(get(graphStore).nodes.some((n) => n.id === b)).toBe(true);
		expect(get(graphStore).nodes.some((n) => n.id === c)).toBe(true);

		expect(graphStore.undo().ok).toBe(true);
		expect(graphStore.undo().ok).toBe(true);
		const thirdUndo = graphStore.undo();
		expect(thirdUndo.ok).toBe(false);
		expect(thirdUndo.reason).toBe('at_oldest');

		graphStore.setHistoryLimit(100);
	});

	it('groups subtype switch into a single undo step', () => {
		graphStore.hardResetGraph();
		graphStore.clearHistory();

		const nodeId = graphStore.addNode('transform', { x: 24, y: 24 });
		const before = get(graphStore).nodes.find((n) => n.id === nodeId);
		const beforeKind = String(before?.data?.transformKind ?? '');

		const switched = graphStore.setTransformKind(nodeId, 'derive');
		expect(switched.ok).toBe(true);
		const after = get(graphStore).nodes.find((n) => n.id === nodeId);
		expect(String(after?.data?.transformKind ?? '')).toBe('derive');

		const undone = graphStore.undo();
		expect(undone.ok).toBe(true);
		const restored = get(graphStore).nodes.find((n) => n.id === nodeId);
		expect(String(restored?.data?.transformKind ?? '')).toBe(beforeKind);
	});

	it('coalesces canvas sync updates inside a transaction into one undo step', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('source', { x: 10, y: 10 });
		graphStore.clearHistory();

		const beforeState = get(graphStore);
		const beforeNode = beforeState.nodes.find((n) => n.id === nodeId);
		expect(beforeNode).toBeTruthy();

		graphStore.beginHistoryTransaction();
		const firstMoveNodes = beforeState.nodes.map((n) =>
			n.id === nodeId ? { ...n, position: { x: 110, y: 10 } } : n
		);
		graphStore.syncFromCanvas(firstMoveNodes as any, beforeState.edges as any);
		const secondMoveNodes = firstMoveNodes.map((n) =>
			n.id === nodeId ? { ...n, position: { x: 210, y: 10 } } : n
		);
		graphStore.syncFromCanvas(secondMoveNodes as any, beforeState.edges as any);
		graphStore.endHistoryTransaction();

		const moved = get(graphStore).nodes.find((n) => n.id === nodeId);
		expect(Number(moved?.position?.x ?? 0)).toBe(210);

		expect(graphStore.undo().ok).toBe(true);
		const restored = get(graphStore).nodes.find((n) => n.id === nodeId);
		expect(Number(restored?.position?.x ?? 0)).toBe(10);
	});

	it('persists node fields after switching selection', () => {
		graphStore.hardResetGraph();
		graphStore.clearHistory();

		const transformId = graphStore.addNode('transform', { x: 20, y: 20 });
		const modelId = graphStore.addNode('model', { x: 240, y: 20 });
		const setKind = graphStore.setTransformKind(transformId, 'derive');
		expect(setKind.ok).toBe(true);
		const cfg = graphStore.updateNodeConfig(transformId, {
			params: {
				op: 'derive',
				derive: {
					mode: 'sql',
					columns: [
						{
							name: 'description_text',
							expr: `trim(regexp_replace(coalesce("description", ''), '<[^>]+>', ' ', 'g'))`
						}
					]
				}
			}
		});
		expect(cfg.ok).toBe(true);

		graphStore.selectNode(modelId);
		graphStore.selectNode(transformId);

		const selected = get(graphStore).nodes.find((n) => n.id === transformId);
		expect(String(selected?.data?.transformKind ?? '')).toBe('derive');
		const params = (selected?.data?.params ?? {}) as Record<string, unknown>;
		expect(String((params.derive as any)?.mode ?? '')).toBe('sql');
		expect(String((params.derive as any)?.columns?.[0]?.name ?? '')).toBe('description_text');
	});

	it('round-trips graph state after undo/redo through save-load path', () => {
		graphStore.hardResetGraph();
		graphStore.clearHistory();

		const sourceId = graphStore.addNode('source', { x: 10, y: 10 });
		const modelId = graphStore.addNode('model', { x: 260, y: 10 });
		const connect = graphStore.addEdge({
			source: sourceId,
			sourceHandle: 'out',
			target: modelId,
			targetHandle: 'in'
		} as any);
		expect(connect.ok).toBe(true);

		const beforeUndoState = get(graphStore);
		const beforeUndo = __stripToDTOForTest(
			beforeUndoState.nodes as any,
			beforeUndoState.edges as any,
			beforeUndoState.graphId
		);
		expect(Array.isArray(beforeUndo.nodes)).toBe(true);
		expect(Array.isArray(beforeUndo.edges)).toBe(true);

		expect(graphStore.undo().ok).toBe(true);
		expect(get(graphStore).edges.length).toBe(0);
		expect(graphStore.redo().ok).toBe(true);
		expect(get(graphStore).edges.length).toBe(1);

		const savedState = get(graphStore);
		const saved = __stripToDTOForTest(savedState.nodes as any, savedState.edges as any, savedState.graphId);
		graphStore.hardResetGraph();
		const applied = graphStore.loadGraphDocument(
			{
				nodes: saved.nodes,
				edges: saved.edges
			},
			saved.meta?.graphId ?? null
		);
		expect(applied.ok).toBe(true);

		const reloaded = get(graphStore);
		expect(reloaded.nodes.length).toBe(2);
		expect(reloaded.edges.length).toBe(1);
		expect(
			reloaded.edges.some(
				(e) => e.source === sourceId && e.target === modelId && String(e.sourceHandle ?? '') === 'out'
			)
		).toBe(true);
	});
});
