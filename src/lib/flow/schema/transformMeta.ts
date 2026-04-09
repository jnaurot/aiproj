import type { TransformKind } from '$lib/flow/schema/transform';

export type TransformCategory = 'reshape' | 'compute' | 'text' | 'convert' | 'ml_prep' | 'quality';

export type TransformMeta = {
	label: string;
	description: string;
	category: TransformCategory;
};

export const TRANSFORM_CATEGORY_LABEL: Record<TransformCategory, string> = {
	reshape: 'Reshape',
	compute: 'Compute',
	text: 'Text',
	convert: 'Convert',
	ml_prep: 'ML Prep',
	quality: 'Quality'
};

export const TRANSFORM_META: Record<TransformKind, TransformMeta> = {
	filter: {
		label: 'Filter',
		description: 'Keep only rows that match the specified conditions.',
		category: 'compute'
	},
	json_filter: {
		label: 'JSON Filter',
		description: 'Keep JSON records that match JSON-path based conditions.',
		category: 'convert'
	},
	select: {
		label: 'Select',
		description: 'Keep or exclude selected columns from the table.',
		category: 'reshape'
	},
	rename: {
		label: 'Rename',
		description: 'Rename columns using a source-to-target map.',
		category: 'reshape'
	},
	derive: {
		label: 'Derive',
		description: 'Create new columns from formulas or SQL expressions.',
		category: 'compute'
	},
	aggregate: {
		label: 'Aggregate',
		description: 'Group rows and calculate summary metrics.',
		category: 'compute'
	},
	join: {
		label: 'Join',
		description: 'Join this input with rows from another node.',
		category: 'reshape'
	},
	sort: {
		label: 'Sort',
		description: 'Sort rows by one or more columns.',
		category: 'reshape'
	},
	limit: {
		label: 'Limit',
		description: 'Keep the first N rows only.',
		category: 'reshape'
	},
	dedupe: {
		label: 'Dedupe',
		description: 'Remove duplicate rows by selected columns.',
		category: 'reshape'
	},
	null_policy: {
		label: 'Null Policy',
		description: 'Report, drop, or fill null values.',
		category: 'quality'
	},
	outlier_policy: {
		label: 'Outlier Policy',
		description: 'Detect and clip, winsorize, or drop outliers.',
		category: 'quality'
	},
	text_clean: {
		label: 'Text Clean',
		description: 'Normalize and clean text formatting noise.',
		category: 'text'
	},
	nlp_normalize: {
		label: 'NLP Normalize',
		description: 'Apply language-aware normalization before NLP tasks.',
		category: 'text'
	},
	tokenize_chunk: {
		label: 'Tokenize Chunk',
		description: 'Split text into token-based chunks for downstream tasks.',
		category: 'text'
	},
	dataset_split: {
		label: 'Dataset Split',
		description: 'Split rows into train/validation/test partitions.',
		category: 'ml_prep'
	},
	class_imbalance: {
		label: 'Class Imbalance',
		description: 'Inspect or rebalance label distribution.',
		category: 'ml_prep'
	},
	categorical_encode: {
		label: 'Categorical Encode',
		description: 'Encode categorical columns into model-ready values.',
		category: 'ml_prep'
	},
	numeric_scale: {
		label: 'Numeric Scale',
		description: 'Scale numeric features with standard scaling methods.',
		category: 'ml_prep'
	},
	embedding: {
		label: 'Embedding',
		description: 'Convert text columns into vector embeddings.',
		category: 'ml_prep'
	},
	feature_selection: {
		label: 'Feature Selection',
		description: 'Select the most useful feature columns.',
		category: 'ml_prep'
	},
	leakage_detect: {
		label: 'Leakage Detect',
		description: 'Detect train/test leakage before model training.',
		category: 'quality'
	},
	quality_profile: {
		label: 'Quality Profile',
		description: 'Build a quality profile of selected columns.',
		category: 'quality'
	},
	drift_compare: {
		label: 'Drift Compare',
		description: 'Compare current data against a baseline for drift.',
		category: 'quality'
	},
	determinism_profile: {
		label: 'Determinism Profile',
		description: 'Enforce deterministic behavior for reproducible runs.',
		category: 'quality'
	},
	fit_state_registry: {
		label: 'Fit State Registry',
		description: 'Store or apply fit state for repeatable transforms.',
		category: 'quality'
	},
	pii_guard: {
		label: 'PII Guard',
		description: 'Detect and handle personally identifiable information.',
		category: 'quality'
	},
	inference_parity: {
		label: 'Inference Parity',
		description: 'Check parity between training and inference signatures.',
		category: 'quality'
	},
	split: {
		label: 'Split',
		description: 'Split text rows into multiple output rows.',
		category: 'text'
	},
	quality_gate: {
		label: 'Quality Gate',
		description: 'Run quality checks and optionally fail on violations.',
		category: 'quality'
	},
	ml_contract: {
		label: 'ML Contract',
		description: 'Declare expected ML label and feature contract.',
		category: 'quality'
	},
	sql: {
		label: 'SQL',
		description: 'Transform rows using SQL over input data.',
		category: 'compute'
	},
	json_to_table: {
		label: 'JSON to Table',
		description: 'Convert JSON payloads into table rows.',
		category: 'convert'
	},
	text_to_table: {
		label: 'Text to Table',
		description: 'Parse text into table rows and columns.',
		category: 'convert'
	},
	table_to_json: {
		label: 'Table to JSON',
		description: 'Serialize table rows into JSON output.',
		category: 'convert'
	}
};

export const TRANSFORM_CATEGORY_ORDER: TransformCategory[] = [
	'reshape',
	'compute',
	'text',
	'convert',
	'ml_prep',
	'quality'
];

export function getTransformMeta(kind: TransformKind): TransformMeta {
	return TRANSFORM_META[kind];
}

