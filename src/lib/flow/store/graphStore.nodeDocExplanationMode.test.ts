import { beforeEach, describe, expect, it } from 'vitest';
import { get } from 'svelte/store';
import { graphStore, getNodeDocExplanationModeFromState } from './graphStore';

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
});

