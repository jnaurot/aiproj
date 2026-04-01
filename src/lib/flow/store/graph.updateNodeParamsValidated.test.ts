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

function transformSelectNode(): Node<PipelineNodeData> {
	return {
		id: 'n_transform_select',
		type: 'transform',
		position: { x: 0, y: 0 },
		data: {
			kind: 'transform',
			transformKind: 'select',
			label: 'Select',
			params: {
				op: 'select',
				select: { mode: 'include', columns: ['id', 'title'], keepOrder: 'custom', strict: true }
			}
		} as PipelineNodeData
	};
}

function transformJoinNode(): Node<PipelineNodeData> {
	return {
		id: 'n_transform_join',
		type: 'transform',
		position: { x: 0, y: 0 },
		data: {
			kind: 'transform',
			transformKind: 'join',
			label: 'Join',
			params: {
				op: 'join',
				join: {
					clauses: [
						{
							leftNodeId: 'n_left',
							leftCol: 'id',
							rightNodeId: 'n_right',
							rightCol: 'job_id',
							how: 'inner'
						}
					]
				}
			}
		} as PipelineNodeData
	};
}

function transformSqlNode(): Node<PipelineNodeData> {
	return {
		id: 'n_transform_sql',
		type: 'transform',
		position: { x: 0, y: 0 },
		data: {
			kind: 'transform',
			transformKind: 'sql',
			label: 'SQL',
			params: {
				op: 'sql',
				sql: {
					query: 'select * from input',
					max_runtime_ms: 4000,
					max_output_rows: 1000,
					safe_mode: true
				}
			}
		} as PipelineNodeData
	};
}

function transformSplitNode(): Node<PipelineNodeData> {
	return {
		id: 'n_transform_split',
		type: 'transform',
		position: { x: 0, y: 0 },
		data: {
			kind: 'transform',
			transformKind: 'split',
			label: 'Split',
			params: {
				op: 'split',
				split: { sourceColumn: 'description', mode: 'regex', delimiterRegex: '\\\\s+', outColumn: 'token' }
			}
		} as PipelineNodeData
	};
}

function transformMlContractNode(): Node<PipelineNodeData> {
	return {
		id: 'n_transform_ml_contract',
		type: 'transform',
		position: { x: 0, y: 0 },
		data: {
			kind: 'transform',
			transformKind: 'ml_contract',
			label: 'ML Contract',
			params: {
				op: 'ml_contract',
				ml_contract: {
					labelColumn: 'label',
					featureColumns: ['f1', 'f2'],
					taskType: 'classification'
				}
			}
		} as PipelineNodeData
	};
}

function transformTextToTableNode(): Node<PipelineNodeData> {
	return {
		id: 'n_transform_text_to_table',
		type: 'transform',
		position: { x: 0, y: 0 },
		data: {
			kind: 'transform',
			transformKind: 'text_to_table',
			label: 'TextToTable',
			params: { op: 'text_to_table', text_to_table: { mode: 'csv', delimiter: ',', hasHeader: true } }
		} as PipelineNodeData
	};
}

function transformTableToJsonNode(): Node<PipelineNodeData> {
	return {
		id: 'n_transform_table_to_json',
		type: 'transform',
		position: { x: 0, y: 0 },
		data: {
			kind: 'transform',
			transformKind: 'table_to_json',
			label: 'TableToJson',
			params: { op: 'table_to_json', table_to_json: { orient: 'split', pretty: true } }
		} as PipelineNodeData
	};
}

function transformJsonFilterNode(): Node<PipelineNodeData> {
	return {
		id: 'n_transform_json_filter',
		type: 'transform',
		position: { x: 0, y: 0 },
		data: {
			kind: 'transform',
			transformKind: 'json_filter',
			label: 'JsonFilter',
			params: {
				op: 'json_filter',
				json_filter: {
					mode: 'rules',
					rules: { kind: 'group', op: 'all', conditions: [] },
					route_reject: true,
					include_reject_meta: true
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

describe('updateNodeParamsValidated transform editor roundtrip parity', () => {
	it('persists select mode/columns/keepOrder', () => {
		const nodes = [transformSelectNode()];
		const result = updateNodeParamsValidated(nodes, 'n_transform_select', {
			op: 'select',
			select: { mode: 'exclude', columns: ['salary'], keepOrder: 'input', strict: true }
		});
		expect(result.error).toBeUndefined();
		const params = ((result.nodes.find((n) => n.id === 'n_transform_select')?.data as any)?.params ?? {}) as Record<string, any>;
		expect(params.select.mode).toBe('exclude');
		expect(params.select.columns).toEqual(['salary']);
		expect(params.select.keepOrder).toBe('input');
	});

	it('persists join clauses without key loss', () => {
		const nodes = [transformJoinNode()];
		const result = updateNodeParamsValidated(nodes, 'n_transform_join', {
			op: 'join',
			join: {
				clauses: [{ leftNodeId: 'n_left', leftCol: 'id', rightNodeId: 'n_aux', rightCol: 'id', how: 'left' }]
			}
		});
		expect(result.error).toBeUndefined();
		const params = ((result.nodes.find((n) => n.id === 'n_transform_join')?.data as any)?.params ?? {}) as Record<string, any>;
		expect(params.join.clauses[0].how).toBe('left');
		expect(params.join.clauses[0].rightNodeId).toBe('n_aux');
	});

	it('persists sql safety controls on update', () => {
		const nodes = [transformSqlNode()];
		const result = updateNodeParamsValidated(nodes, 'n_transform_sql', {
			op: 'sql',
			sql: { query: 'select id from input', max_runtime_ms: 2500, max_output_rows: 250, safe_mode: false }
		});
		expect(result.error).toBeUndefined();
		const params = ((result.nodes.find((n) => n.id === 'n_transform_sql')?.data as any)?.params ?? {}) as Record<string, any>;
		expect(params.sql.max_runtime_ms).toBe(2500);
		expect(params.sql.max_output_rows).toBe(250);
		expect(params.sql.safe_mode).toBe(false);
	});

	it('persists split mode and output field config', () => {
		const nodes = [transformSplitNode()];
		const result = updateNodeParamsValidated(nodes, 'n_transform_split', {
			op: 'split',
			split: { sourceColumn: 'description', mode: 'delimiter', delimiter: ',', outColumn: 'part' }
		});
		expect(result.error).toBeUndefined();
		const params = ((result.nodes.find((n) => n.id === 'n_transform_split')?.data as any)?.params ?? {}) as Record<string, any>;
		expect(params.split.mode).toBe('delimiter');
		expect(params.split.outColumn).toBe('part');
	});

	it('persists ml_contract required columns', () => {
		const nodes = [transformMlContractNode()];
		const result = updateNodeParamsValidated(nodes, 'n_transform_ml_contract', {
			op: 'ml_contract',
			ml_contract: { labelColumn: 'target', featureColumns: ['f1'], taskType: 'classification' }
		});
		expect(result.error).toBeUndefined();
		const params = ((result.nodes.find((n) => n.id === 'n_transform_ml_contract')?.data as any)?.params ?? {}) as Record<string, any>;
		expect(params.ml_contract.labelColumn).toBe('target');
		expect(params.ml_contract.featureColumns).toEqual(['f1']);
	});

	it('persists text_to_table mode options', () => {
		const nodes = [transformTextToTableNode()];
		const result = updateNodeParamsValidated(nodes, 'n_transform_text_to_table', {
			op: 'text_to_table',
			text_to_table: { mode: 'lines', column: 'line_text' }
		});
		expect(result.error).toBeUndefined();
		const params = ((result.nodes.find((n) => n.id === 'n_transform_text_to_table')?.data as any)?.params ?? {}) as Record<string, any>;
		expect(params.text_to_table.mode).toBe('lines');
		expect(params.text_to_table.column).toBe('line_text');
	});

	it('persists table_to_json orient config', () => {
		const nodes = [transformTableToJsonNode()];
		const result = updateNodeParamsValidated(nodes, 'n_transform_table_to_json', {
			op: 'table_to_json',
			table_to_json: { orient: 'records', pretty: false }
		});
		expect(result.error).toBeUndefined();
		const params = ((result.nodes.find((n) => n.id === 'n_transform_table_to_json')?.data as any)?.params ?? {}) as Record<string, any>;
		expect(params.table_to_json.orient).toBe('records');
		expect(params.table_to_json.pretty).toBe(false);
	});

	it('persists json_filter route reject settings', () => {
		const nodes = [transformJsonFilterNode()];
		const result = updateNodeParamsValidated(nodes, 'n_transform_json_filter', {
			op: 'json_filter',
			json_filter: { mode: 'rules', rules: { kind: 'group', op: 'all', conditions: [] }, route_reject: false, include_reject_meta: false }
		});
		expect(result.error).toBeUndefined();
		const params = ((result.nodes.find((n) => n.id === 'n_transform_json_filter')?.data as any)?.params ?? {}) as Record<string, any>;
		expect(params.json_filter.route_reject).toBe(false);
		expect(params.json_filter.include_reject_meta).toBe(false);
	});
});
