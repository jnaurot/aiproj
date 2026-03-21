import { describe, expect, it } from 'vitest';
import { guidedControlsForModelKind, taskKindsForModelKind } from './modelAssist';

describe('modelAssist', () => {
	it('returns valid task kinds per model kind', () => {
		expect(taskKindsForModelKind('embedding')).toEqual(['embed']);
		expect(taskKindsForModelKind('reranker')).toEqual(['rerank']);
		expect(taskKindsForModelKind('vision')).toEqual(['caption', 'classify', 'extract', 'generate']);
	});

	it('returns 3-5 guided controls per model kind', () => {
		const kinds = ['llm', 'vision', 'audio', 'embedding', 'reranker', 'multimodal'] as const;
		for (const kind of kinds) {
			const controls = guidedControlsForModelKind(kind);
			expect(controls.length).toBeGreaterThanOrEqual(3);
			expect(controls.length).toBeLessThanOrEqual(5);
			expect(controls.some((c) => c.id === 'provider')).toBe(true);
			expect(controls.some((c) => c.id === 'model')).toBe(true);
			expect(controls.some((c) => c.id === 'output')).toBe(true);
		}
	});
});
