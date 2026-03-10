// src/lib/flow/types/paramsMap.ts
import type {
	SourceFileParams,
	SourceDatabaseParams,
	SourceAPIParams
} from '$lib/flow/schema/source';
import type { ComponentParams } from '$lib/flow/schema/component';
export type SourceKind = 'file' | 'database' | 'api';
export type SourceParamsByKind = {
	file: SourceFileParams;
	database: SourceDatabaseParams;
	api: SourceAPIParams;
};

// --- LLM ---
import type { LlmParams } from '$lib/flow/schema/llm';

export type LlmKind = 'ollama' | 'openai_compat';

/**
 * If/when you later split params by kind, update this mapping.
 * For now both llm kinds share the same params shape.
 */
export type LlmParamsByKind = {
	ollama: LlmParams;
	openai_compat: LlmParams;
};

import type {
	TransformFilterParams,
	TransformSelectParams,
	TransformRenameParams,
	TransformDeriveParams,
	TransformAggregateParams,
	TransformJoinParams,
	TransformSortParams,
	TransformLimitParams,
	TransformDedupeParams,
	TransformSplitParams,
	TransformQualityGateParams,
	TransformSqlParams,
	TransformJsonToTableParams,
	TransformTextToTableParams,
	TransformTableToJsonParams
} from '$lib/flow/schema/transform';
export type TransformKind =
	| 'filter'
	| 'select'
	| 'rename'
	| 'derive'
	| 'aggregate'
	| 'join'
	| 'sort'
	| 'limit'
	| 'dedupe'
	| 'split'
	| 'quality_gate'
	| 'sql'
	| 'json_to_table'
	| 'text_to_table'
	| 'table_to_json';

export type TransformParamsByKind = {
	filter: TransformFilterParams;
	select: TransformSelectParams;
	rename: TransformRenameParams;
	derive: TransformDeriveParams;
	aggregate: TransformAggregateParams;
	join: TransformJoinParams;
	sort: TransformSortParams;
	limit: TransformLimitParams;
	dedupe: TransformDedupeParams;
	split: TransformSplitParams;
	quality_gate: TransformQualityGateParams;
	sql: TransformSqlParams;
	json_to_table: TransformJsonToTableParams;
	text_to_table: TransformTextToTableParams;
	table_to_json: TransformTableToJsonParams;
};

export type ToolProvider = 'mcp' | 'http' | 'function' | 'python' | 'js' | 'shell' | 'db' | 'builtin';

export type ComponentKind = 'graph_component';

export type ComponentParamsByKind = {
	graph_component: ComponentParams;
};
