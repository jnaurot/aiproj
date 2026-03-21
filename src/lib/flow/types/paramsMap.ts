// src/lib/flow/types/paramsMap.ts
import type {
	SourceFileParams,
	SourceDatabaseParams,
	SourceAPIParams,
	SourceObjectStoreParams,
	SourceWarehouseParams
} from '$lib/flow/schema/source';
import type { ComponentParams } from '$lib/flow/schema/component';
export type SourceKind = 'file' | 'database' | 'api' | 'object_store' | 'warehouse';
export type SourceParamsByKind = {
	file: SourceFileParams;
	database: SourceDatabaseParams;
	api: SourceAPIParams;
	object_store: SourceObjectStoreParams;
	warehouse: SourceWarehouseParams;
};

// --- LLM ---
import type { LlmParams } from '$lib/flow/schema/llm';

export type LlmKind = 'ollama' | 'openai_compat';
export type ModelKind = 'llm' | 'vision' | 'audio' | 'embedding' | 'reranker' | 'multimodal';
export type ModelTaskKind =
	| 'generate'
	| 'classify'
	| 'extract'
	| 'embed'
	| 'rerank'
	| 'transcribe'
	| 'caption';

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
	TransformNullPolicyParams,
	TransformOutlierPolicyParams,
	TransformTextCleanParams,
	TransformNlpNormalizeParams,
	TransformTokenizeChunkParams,
	TransformDatasetSplitParams,
	TransformClassImbalanceParams,
	TransformCategoricalEncodeParams,
	TransformNumericScaleParams,
	TransformEmbeddingParams,
	TransformFeatureSelectionParams,
	TransformLeakageDetectParams,
	TransformQualityProfileParams,
	TransformDriftCompareParams,
	TransformDeterminismProfileParams,
	TransformFitStateRegistryParams,
	TransformPiiGuardParams,
	TransformInferenceParityParams,
	TransformSplitParams,
	TransformQualityGateParams,
	TransformMlContractParams,
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
	| 'null_policy'
	| 'outlier_policy'
	| 'text_clean'
	| 'nlp_normalize'
	| 'tokenize_chunk'
	| 'dataset_split'
	| 'class_imbalance'
	| 'categorical_encode'
	| 'numeric_scale'
	| 'embedding'
	| 'feature_selection'
	| 'leakage_detect'
	| 'quality_profile'
	| 'drift_compare'
	| 'determinism_profile'
	| 'fit_state_registry'
	| 'pii_guard'
	| 'inference_parity'
	| 'split'
	| 'quality_gate'
	| 'ml_contract'
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
	null_policy: TransformNullPolicyParams;
	outlier_policy: TransformOutlierPolicyParams;
	text_clean: TransformTextCleanParams;
	nlp_normalize: TransformNlpNormalizeParams;
	tokenize_chunk: TransformTokenizeChunkParams;
	dataset_split: TransformDatasetSplitParams;
	class_imbalance: TransformClassImbalanceParams;
	categorical_encode: TransformCategoricalEncodeParams;
	numeric_scale: TransformNumericScaleParams;
	embedding: TransformEmbeddingParams;
	feature_selection: TransformFeatureSelectionParams;
	leakage_detect: TransformLeakageDetectParams;
	quality_profile: TransformQualityProfileParams;
	drift_compare: TransformDriftCompareParams;
	determinism_profile: TransformDeterminismProfileParams;
	fit_state_registry: TransformFitStateRegistryParams;
	pii_guard: TransformPiiGuardParams;
	inference_parity: TransformInferenceParityParams;
	split: TransformSplitParams;
	quality_gate: TransformQualityGateParams;
	ml_contract: TransformMlContractParams;
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
