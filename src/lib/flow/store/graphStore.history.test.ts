import { get } from 'svelte/store';
import { describe, expect, it } from 'vitest';

import { graphStore } from './graphStore';

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
});
