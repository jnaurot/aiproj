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
});
