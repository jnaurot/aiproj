// src/lib/flow/types/paramsMap.ts
import type {
	SourceFileParams,
	SourceDatabaseParams,
	SourceAPIParams
} from '$lib/flow/schema/source';
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
	TransformSqlParams
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
	| 'sql';

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
	sql: TransformSqlParams;
};

export type ToolProvider = 'mcp' | 'http' | 'function' | 'python' | 'js' | 'shell' | 'db' | 'builtin';
