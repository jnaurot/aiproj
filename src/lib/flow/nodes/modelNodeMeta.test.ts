import { describe, expect, it } from 'vitest';
import { modelNodeMeta } from './modelNodeMeta';

describe('modelNodeMeta', () => {
	it('resolves defaults for incomplete node data', () => {
		expect(modelNodeMeta(undefined as any)).toEqual({
			model: '-',
			modelKind: 'llm',
			taskKind: 'generate',
			provider: 'ollama',
			outputMode: 'text'
		});
	});

	it('resolves explicit model/task/provider/output values', () => {
		expect(
			modelNodeMeta({
				kind: 'model',
				label: 'Model',
				status: 'idle',
				llmKind: 'openai_compat',
				modelKind: 'multimodal',
				taskKind: 'extract',
				params: {
					model: 'gpt-4.1',
					baseUrl: 'https://example.test',
					user_prompt: 'hello',
					output: { mode: 'json', jsonSchema: { type: 'object' } }
				}
			} as any)
		).toEqual({
			model: 'gpt-4.1',
			modelKind: 'multimodal',
			taskKind: 'extract',
			provider: 'openai_compat',
			outputMode: 'json'
		});
	});

	it('uses ascii-safe fallback model label', () => {
		const meta = modelNodeMeta(undefined as any);
		expect(meta.model).toBe('-');
		expect(/^[\x20-\x7E]+$/.test(meta.model)).toBe(true);
	});
});

