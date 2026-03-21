import { describe, expect, it } from 'vitest';

import type { Node } from '@xyflow/svelte';

import type { PipelineNodeData } from '$lib/flow/types';
import { updateNodeParamsValidated } from './graph';

function toolNodeWithArgs(args: Record<string, unknown>): Node<PipelineNodeData> {
	return {
		id: 'n_tool',
		type: 'tool',
		position: { x: 0, y: 0 },
		data: {
			kind: 'tool',
			label: 'Tool',
			params: {
				provider: 'builtin',
				builtin: {
					toolId: 'core.datetime.normalize_tz',
					profileId: 'core',
					args
				}
			}
		} as PipelineNodeData
	};
}

function modelNodeWithJsonOutput(): Node<PipelineNodeData> {
	return {
		id: 'n_model',
		type: 'model',
		position: { x: 0, y: 0 },
		data: {
			kind: 'model',
			label: 'Model',
			llmKind: 'ollama',
			modelKind: 'llm',
			taskKind: 'generate',
			params: {
				baseUrl: 'http://localhost:11434',
				model: 'demo-model',
				user_prompt: 'hello',
				output: {
					mode: 'json',
					strict: true,
					jsonSchema: { type: 'object', properties: { ok: { type: 'boolean' } } }
				}
			}
		} as PipelineNodeData
	};
}

describe('updateNodeParamsValidated builtin args replacement', () => {
	it('replaces builtin args object on operation switch instead of deep-merging keys', () => {
		const nodes = [
			toolNodeWithArgs({
				value: '2026-03-09T18:00:00-05:00',
				target_tz: 'UTC',
				values: [1, 2, 3]
			})
		];
		const result = updateNodeParamsValidated(nodes, 'n_tool', {
			provider: 'builtin',
			builtin: {
				toolId: 'core.http.request_text',
				args: {
					url: 'https://example.com',
					method: 'GET',
					headers: {}
				}
			}
		});
		expect(result.error).toBeUndefined();
		const nextNode = result.nodes.find((n) => n.id === 'n_tool');
		const nextArgs = (((nextNode?.data as any)?.params?.builtin ?? {}) as any).args ?? {};
		expect(nextArgs).toEqual({
			url: 'https://example.com',
			method: 'GET',
			headers: {}
		});
		expect(Object.prototype.hasOwnProperty.call(nextArgs, 'value')).toBe(false);
		expect(Object.prototype.hasOwnProperty.call(nextArgs, 'target_tz')).toBe(false);
		expect(Object.prototype.hasOwnProperty.call(nextArgs, 'values')).toBe(false);
	});
});

describe('updateNodeParamsValidated model output mode switching', () => {
	it('drops jsonSchema when switching output mode from json to text', () => {
		const nodes = [modelNodeWithJsonOutput()];
		const result = updateNodeParamsValidated(nodes, 'n_model', {
			output: { mode: 'text', strict: true }
		});
		expect(result.error).toBeUndefined();
		const params = ((result.nodes.find((n) => n.id === 'n_model')?.data as any)?.params ?? {}) as Record<string, any>;
		expect(params.output.mode).toBe('text');
		expect(Object.prototype.hasOwnProperty.call(params.output, 'jsonSchema')).toBe(false);
	});
});
