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

function sourceApiNodeWithQuery(query: Record<string, string>): Node<PipelineNodeData> {
	return {
		id: 'n_source_api',
		type: 'source',
		position: { x: 0, y: 0 },
		data: {
			kind: 'source',
			sourceKind: 'api',
			label: 'API Source',
			params: {
				method: 'GET',
				url: 'https://remotive.com/api/remote-jobs',
				query,
				headers: { accept: 'application/json', 'Content-Type': 'application/json' },
				bodyMode: 'none',
				output: { mode: 'json' }
			}
		} as PipelineNodeData
	};
}

function transformFilterNode(): Node<PipelineNodeData> {
	return {
		id: 'n_transform_filter',
		type: 'transform',
		position: { x: 0, y: 0 },
		data: {
			kind: 'transform',
			transformKind: 'filter',
			label: 'Filter',
			params: {
				op: 'filter',
				filter: {
					mode: 'sql',
					expr: '"salary" > 40000',
					rules: { kind: 'group', op: 'all', conditions: [] }
				}
			}
		} as PipelineNodeData
	};
}

function transformDeriveNode(): Node<PipelineNodeData> {
	return {
		id: 'n_transform_derive',
		type: 'transform',
		position: { x: 0, y: 0 },
		data: {
			kind: 'transform',
			transformKind: 'derive',
			label: 'Derive',
			params: {
				op: 'derive',
				derive: {
					mode: 'sql',
					columns: [{ name: 'x2', expr: '"value" * 2' }],
					rules: [
						{
							name: 'value_plus_bonus',
							formula: {
								op: 'add',
								args: [{ column: 'value' }, { valueFrom: { handle: 'param_config', path: 'prefs.bonus' } }]
							}
						}
					]
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

describe('updateNodeParamsValidated source api map replacement', () => {
	it('replaces query object so deleted keys do not reappear on subsequent commits', () => {
		const nodes = [
			sourceApiNodeWithQuery({
				limit: '500',
				search: 'software OR data'
			})
		];
		const result = updateNodeParamsValidated(nodes, 'n_source_api', {
			query: {
				limit: '500'
			}
		});
		expect(result.error).toBeUndefined();
		const params = ((result.nodes.find((n) => n.id === 'n_source_api')?.data as any)?.params ?? {}) as Record<
			string,
			any
		>;
		expect(params.query).toEqual({ limit: '500' });
		expect(Object.prototype.hasOwnProperty.call(params.query ?? {}, 'search')).toBe(false);
	});

	it('persists empty query object when user deletes all query params', () => {
		const nodes = [
			sourceApiNodeWithQuery({
				limit: '500',
				search: 'software OR data'
			})
		];
		const result = updateNodeParamsValidated(nodes, 'n_source_api', {
			query: {}
		});
		expect(result.error).toBeUndefined();
		const params = ((result.nodes.find((n) => n.id === 'n_source_api')?.data as any)?.params ?? {}) as Record<
			string,
			any
		>;
		expect(params.query).toEqual({});
	});

	it('persists json_item_path and json_item_strict settings', () => {
		const nodes = [sourceApiNodeWithQuery({ limit: '50' })];
		const result = updateNodeParamsValidated(nodes, 'n_source_api', {
			json_item_path: '$.jobs[]',
			json_item_strict: true
		});
		expect(result.error).toBeUndefined();
		const params = ((result.nodes.find((n) => n.id === 'n_source_api')?.data as any)?.params ?? {}) as Record<
			string,
			any
		>;
		expect(params.json_item_path).toBe('$.jobs[]');
		expect(params.json_item_strict).toBe(true);
	});
});

describe('updateNodeParamsValidated transform dual-mode patch canonicalization', () => {
	it('persists filter mode/rules when editor patches flat fields', () => {
		const nodes = [transformFilterNode()];
		const result = updateNodeParamsValidated(nodes, 'n_transform_filter', {
			mode: 'rules',
			expr: '',
			rules: {
				kind: 'group',
				op: 'all',
				conditions: [
					{
						kind: 'condition',
						column: 'job_type',
						op: 'eq',
						value: { valueFrom: { handle: 'param_config', path: 'preferences.type' } }
					}
				]
			}
		});
		expect(result.error).toBeUndefined();
		const params = ((result.nodes.find((n) => n.id === 'n_transform_filter')?.data as any)?.params ?? {}) as Record<
			string,
			any
		>;
		expect(params.op).toBe('filter');
		expect(params.filter.mode).toBe('rules');
		expect(Array.isArray(params.filter.rules.conditions)).toBe(true);
		expect(params.filter.rules.conditions[0].column).toBe('job_type');
	});

	it('persists derive mode/rules when editor patches flat fields', () => {
		const nodes = [transformDeriveNode()];
		const result = updateNodeParamsValidated(nodes, 'n_transform_derive', {
			mode: 'rules',
			rules: [
				{
					name: 'value_plus_bonus',
					formula: {
						op: 'add',
						args: [{ column: 'value' }, { valueFrom: { handle: 'param_config', path: 'prefs.bonus' } }]
					}
				}
			],
			columns: []
		});
		expect(result.error).toBeUndefined();
		const params = ((result.nodes.find((n) => n.id === 'n_transform_derive')?.data as any)?.params ?? {}) as Record<
			string,
			any
		>;
		expect(params.op).toBe('derive');
		expect(params.derive.mode).toBe('rules');
		expect(Array.isArray(params.derive.rules)).toBe(true);
		expect(params.derive.rules[0].formula.op).toBe('add');
	});
});
