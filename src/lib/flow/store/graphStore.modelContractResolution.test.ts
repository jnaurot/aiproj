import { describe, expect, it } from 'vitest';
import { deriveNodeIoForData } from '$lib/flow/store/graphStore';

describe('model contract resolution (frontend)', () => {
	const baseNode = {
		kind: 'model',
		label: 'Model',
		status: 'idle',
		llmKind: 'ollama',
		modelKind: 'llm',
		taskKind: 'generate',
		params: {
			baseUrl: 'http://localhost:11434',
			model: 'demo-model',
			user_prompt: 'hello'
		}
	} as any;

	it('uses declared schema before params output mode', () => {
		const io = deriveNodeIoForData({
			...baseNode,
			schema: {
				expectedSchema: {
					typedSchema: { type: 'text', fields: [] }
				}
			},
			params: {
				...baseNode.params,
				output: { mode: 'json', jsonSchema: { type: 'object' } }
			}
		} as any);
		expect(io).toEqual({ in: 'text', out: 'text' });
	});

	it('uses params output mode when no declared schema exists', () => {
		const io = deriveNodeIoForData({
			...baseNode,
			params: {
				...baseNode.params,
				output: { mode: 'embeddings', embedding: { dims: 8, dtype: 'float32', layout: '1d' } }
			}
		} as any);
		expect(io).toEqual({ in: 'text', out: 'embeddings' });
	});

	it('falls back to text output by default', () => {
		const io = deriveNodeIoForData(baseNode as any);
		expect(io).toEqual({ in: 'text', out: 'text' });
	});

	it('resolves deterministic contract snapshots across vectors', () => {
		const vectors = [
			{
				id: 'declared_overrides_params',
				node: {
					...baseNode,
					schema: { expectedSchema: { typedSchema: { type: 'json', fields: [] } } },
					params: { ...baseNode.params, output: { mode: 'text' } }
				}
			},
			{
				id: 'params_used_when_no_declared',
				node: {
					...baseNode,
					params: { ...baseNode.params, output: { mode: 'embeddings', embedding: { dims: 8 } } }
				}
			},
			{ id: 'default_text', node: { ...baseNode } }
		] as const;
		const summary = vectors.map((v) => ({ id: v.id, io: deriveNodeIoForData(v.node as any) }));
		expect(summary).toEqual([
			{ id: 'declared_overrides_params', io: { in: 'text', out: 'json' } },
			{ id: 'params_used_when_no_declared', io: { in: 'text', out: 'embeddings' } },
			{ id: 'default_text', io: { in: 'text', out: 'text' } }
		]);
	});
});