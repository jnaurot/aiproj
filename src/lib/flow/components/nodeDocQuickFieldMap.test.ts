import { describe, expect, it } from 'vitest';
import { pickQuickFields, resolveQuickFieldsForNode } from './nodeDocQuickFieldMap';

describe('nodeDocQuickFieldMap', () => {
	it('resolves model quick fields with user_prompt', () => {
		const keys = resolveQuickFieldsForNode('model', 'ollama');
		expect(keys.has('user_prompt')).toBe(true);
		expect(keys.has('model')).toBe(true);
	});

	it('filters settings to allowed quick fields', () => {
		const picked = pickQuickFields('model', 'ollama', {
			model: 'glm-4.7-flash:latest',
			user_prompt: 'Score this job',
			debug_enabled: 'true'
		} as any);
		expect(picked.model).toContain('glm-4.7');
		expect(picked.user_prompt).toContain('Score');
		expect((picked as any).debug_enabled).toBeUndefined();
	});
});

