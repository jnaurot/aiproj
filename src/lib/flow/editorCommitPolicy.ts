import type { LlmKind, SourceKind, ToolProvider, TransformKind } from '$lib/flow/types/paramsMap';

export type EditorCommitMode = 'draft' | 'immediate';

export const TransformEditorCommitModeByKind: Record<TransformKind, EditorCommitMode> = {
	filter: 'draft',
	json_filter: 'draft',
	select: 'draft',
	rename: 'draft',
	derive: 'draft',
	aggregate: 'immediate',
	join: 'immediate',
	sort: 'immediate',
	limit: 'draft',
	dedupe: 'immediate',
	null_policy: 'draft',
	outlier_policy: 'draft',
	text_clean: 'immediate',
	nlp_normalize: 'draft',
	tokenize_chunk: 'draft',
	dataset_split: 'immediate',
	class_imbalance: 'immediate',
	categorical_encode: 'immediate',
	numeric_scale: 'immediate',
	embedding: 'immediate',
	feature_selection: 'immediate',
	leakage_detect: 'immediate',
	quality_profile: 'immediate',
	drift_compare: 'immediate',
	determinism_profile: 'immediate',
	fit_state_registry: 'immediate',
	pii_guard: 'immediate',
	inference_parity: 'immediate',
	split: 'draft',
	quality_gate: 'immediate',
	ml_contract: 'draft',
	sql: 'draft',
	json_to_table: 'draft',
	text_to_table: 'draft',
	table_to_json: 'immediate'
};

export function normalizeTransformKind(value: unknown): TransformKind {
	const raw = String(value ?? '').trim().toLowerCase();
	if (raw in TransformEditorCommitModeByKind) return raw as TransformKind;
	return 'select';
}

export function getTransformEditorCommitMode(value: unknown): EditorCommitMode {
	return TransformEditorCommitModeByKind[normalizeTransformKind(value)] ?? 'immediate';
}

export const SourceEditorCommitModeByKind: Record<SourceKind, EditorCommitMode> = {
	file: 'draft',
	database: 'draft',
	api: 'draft',
	object_store: 'draft',
	warehouse: 'draft'
};

export const LlmEditorCommitModeByKind: Record<LlmKind, EditorCommitMode> = {
	ollama: 'draft',
	openai_compat: 'draft'
};

export const ToolEditorCommitModeByProvider: Record<ToolProvider, EditorCommitMode> = {
	mcp: 'draft',
	http: 'draft',
	function: 'draft',
	python: 'draft',
	js: 'draft',
	shell: 'draft',
	db: 'draft',
	builtin: 'draft'
};

export function normalizeSourceKind(value: unknown): SourceKind {
	const raw = String(value ?? '').trim().toLowerCase();
	if (raw in SourceEditorCommitModeByKind) return raw as SourceKind;
	return 'file';
}

export function normalizeLlmKind(value: unknown): LlmKind {
	const raw = String(value ?? '').trim().toLowerCase();
	if (raw in LlmEditorCommitModeByKind) return raw as LlmKind;
	return 'ollama';
}

export function normalizeToolProvider(value: unknown): ToolProvider {
	const raw = String(value ?? '').trim().toLowerCase();
	if (raw in ToolEditorCommitModeByProvider) return raw as ToolProvider;
	return 'mcp';
}

export function getSourceEditorCommitMode(value: unknown): EditorCommitMode {
	return SourceEditorCommitModeByKind[normalizeSourceKind(value)] ?? 'immediate';
}

export function getLlmEditorCommitMode(value: unknown): EditorCommitMode {
	return LlmEditorCommitModeByKind[normalizeLlmKind(value)] ?? 'immediate';
}

export function getToolEditorCommitMode(value: unknown): EditorCommitMode {
	return ToolEditorCommitModeByProvider[normalizeToolProvider(value)] ?? 'immediate';
}
