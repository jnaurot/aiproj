import { beforeEach, describe, expect, it } from 'vitest';
import { get } from 'svelte/store';
import {
	graphStore,
	getNodeDocExplanationModeFromState,
	getNodeDocTrainingModeFromState
} from './graphStore';

describe('graphStore node doc explanation mode', () => {
	beforeEach(() => {
		graphStore.setNodeDocExplanationMode('default');
	});

	it('defaults to default mode', () => {
		const mode = graphStore.getNodeDocExplanationMode();
		expect(mode).toBe('default');
	});

	it('toggles to llm globally', () => {
		graphStore.setNodeDocExplanationMode('llm');
		expect(graphStore.getNodeDocExplanationMode()).toBe('llm');
		const state = get(graphStore as any);
		expect(getNodeDocExplanationModeFromState(state as any)).toBe('llm');
	});

	it('rejects unknown mode and falls back to default', () => {
		graphStore.setNodeDocExplanationMode('hybrid' as any);
		expect(graphStore.getNodeDocExplanationMode()).toBe('default');
	});

	it('does not mutate run/scheduler semantics when toggled', () => {
		graphStore.hardResetGraph();
		const sourceId = graphStore.addNode('source', { x: 0, y: 0 });
		const modelId = graphStore.addNode('model', { x: 220, y: 0 });
		graphStore.addEdge({
			id: 'e_mode_toggle',
			source: sourceId,
			target: modelId,
			sourceHandle: 'out',
			targetHandle: 'in',
			data: { mode: 'work' }
		} as any);
		const before = get(graphStore as any);
		graphStore.setNodeDocExplanationMode('llm');
		const after = get(graphStore as any);
		expect(after.runStatus).toBe(before.runStatus);
		expect(after.activeRunId).toBe(before.activeRunId);
		expect(after.edges.length).toBe(before.edges.length);
		expect(after.nodes.length).toBe(before.nodes.length);
	});

	it('supports training mode toggle without mutating runtime state', () => {
		graphStore.hardResetGraph();
		const before = get(graphStore as any);
		graphStore.setNodeDocTrainingMode('on');
		expect(graphStore.getNodeDocTrainingMode()).toBe('on');
		const after = get(graphStore as any);
		expect(getNodeDocTrainingModeFromState(after as any)).toBe('on');
		expect(after.runStatus).toBe(before.runStatus);
		expect(after.activeRunId).toBe(before.activeRunId);
	});
});
