import type {
	TransformParams,
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
	TransformQualityGateParams,
	TransformSqlParams,
	TransformSplitParams,
	TransformJsonToTableParams,
	TransformTextToTableParams,
	TransformTableToJsonParams,
	TransformMlContractParams
} from '$lib/flow/schema/transform';

export const defaultTransformFilterParams: TransformFilterParams = {
	expr: ''
};

export const defaultTransformSelectParams: TransformSelectParams = {
	mode: 'include',
	columns: [],
	keepOrder: 'custom',
	strict: true
};

export const defaultTransformRenameParams: TransformRenameParams = {
	map: {}
};

export const defaultTransformDeriveParams: TransformDeriveParams = {
	columns: [
		{
			name: 'length_text',
			expr: 'length(text)'
		},
		{
			name: 'is_long',
			expr: 'length(text) > 50'
		}
	]
};

export const defaultTransformAggregateParams: TransformAggregateParams = {
	groupBy: [],
	metrics: [
		{
			name: 'row_count',
			op: 'count_rows',
			column: null
		}
	]
};

export const defaultTransformJoinParams: TransformJoinParams = {
	clauses: [
		{
			leftNodeId: 'upstream_left',
			leftCol: 'id',
			rightNodeId: 'upstream_right',
			rightCol: 'id',
			how: 'inner'
		}
	]
};

export const defaultTransformSortParams: TransformSortParams = {
	by: [
		{
			col: 'text',
			dir: 'asc'
		}
	]
};

export const defaultTransformLimitParams: TransformLimitParams = {
	n: 100
};

export const defaultTransformDedupeParams: TransformDedupeParams = {
	allColumns: true,
	by: []
};

export const defaultTransformNullPolicyParams: TransformNullPolicyParams = {
	mode: 'report',
	columns: [],
	fillValue: '',
	stat: 'mean',
	rules: []
};

export const defaultTransformOutlierPolicyParams: TransformOutlierPolicyParams = {
	mode: 'clip',
	method: 'iqr',
	columns: [],
	iqrMultiplier: 1.5,
	zscoreThreshold: 3,
	lowerQuantile: 0.01,
	upperQuantile: 0.99
};

export const defaultTransformTextCleanParams: TransformTextCleanParams = {
	columns: [],
	lowercase: true,
	unicodeNormalize: 'nfkc',
	removePunctuation: false,
	removeUrls: true,
	removeEmails: true,
	removeEmoji: false,
	normalizeWhitespace: true
};

export const defaultTransformNlpNormalizeParams: TransformNlpNormalizeParams = {
	columns: [],
	language: 'en',
	removeStopwords: true,
	stemmer: 'none',
	lemmatizer: 'none',
	tokenPattern: '\\w+'
};

export const defaultTransformTokenizeChunkParams: TransformTokenizeChunkParams = {
	columns: [],
	tokenizer: 'whitespace',
	tokenPattern: '\\w+',
	maxTokens: 256,
	overlap: 32,
	sentenceAware: true,
	outColumn: 'chunk'
};

export const defaultTransformDatasetSplitParams: TransformDatasetSplitParams = {
	strategy: 'random',
	trainRatio: 0.8,
	valRatio: 0.1,
	testRatio: 0.1,
	seed: 42,
	shuffle: true,
	stratifyColumn: '',
	groupColumn: '',
	timeColumn: '',
	leakageGuard: true
};

export const defaultTransformClassImbalanceParams: TransformClassImbalanceParams = {
	strategy: 'report',
	labelColumn: 'label',
	targetRatio: 1,
	seed: 42
};

export const defaultTransformCategoricalEncodeParams: TransformCategoricalEncodeParams = {
	columns: [],
	encoding: 'one_hot',
	unknownPolicy: 'ignore',
	rareThreshold: 0,
	dropFirst: false
};

export const defaultTransformNumericScaleParams: TransformNumericScaleParams = {
	columns: [],
	method: 'standard',
	withCenter: true,
	withScale: true,
	clip: false
};

export const defaultTransformEmbeddingParams: TransformEmbeddingParams = {
	columns: [],
	provider: 'local_hash',
	model: 'text-embedding-3-small',
	dimensions: 16,
	batchSize: 64,
	cacheEmbeddings: true,
	outputColumn: 'embedding'
};

export const defaultTransformFeatureSelectionParams: TransformFeatureSelectionParams = {
	method: 'variance',
	columns: [],
	topK: 50,
	varianceThreshold: 0,
	targetColumn: 'label',
	selectedColumns: []
};

export const defaultTransformLeakageDetectParams: TransformLeakageDetectParams = {
	splitColumn: 'split',
	keyColumns: ['id'],
	labelColumn: 'label',
	maxAllowedOverlap: 0
};

export const defaultTransformQualityProfileParams: TransformQualityProfileParams = {
	columns: [],
	includeHistograms: true,
	includeSamples: true
};

export const defaultTransformDriftCompareParams: TransformDriftCompareParams = {
	baselineRef: '',
	compareColumns: [],
	metric: 'psi',
	threshold: 0.2,
	failOnDrift: false
};

export const defaultTransformDeterminismProfileParams: TransformDeterminismProfileParams = {
	strict: true,
	seed: 42,
	stableSort: true,
	stableCoercion: true
};

export const defaultTransformFitStateRegistryParams: TransformFitStateRegistryParams = {
	mode: 'fit',
	stateKey: 'default',
	includeColumns: []
};

export const defaultTransformPiiGuardParams: TransformPiiGuardParams = {
	columns: [],
	action: 'report',
	failOnDetect: false
};

export const defaultTransformInferenceParityParams: TransformInferenceParityParams = {
	trainSignature: '',
	inferenceSignature: '',
	failOnMismatch: true
};

export const defaultTransformSqlParams: TransformSqlParams = {
	dialect: 'duckdb',
	query: 'SELECT * FROM input LIMIT 10'
};

export const defaultTransformJsonToTableParams: TransformJsonToTableParams = {
	orient: 'records',
	rowsKey: 'rows'
};

export const defaultTransformTextToTableParams: TransformTextToTableParams = {
	mode: 'lines',
	column: 'text',
	delimiter: ',',
	hasHeader: true
};

export const defaultTransformTableToJsonParams: TransformTableToJsonParams = {
	orient: 'records',
	pretty: false
};

export const defaultTransformSplitParams: TransformSplitParams = {
	sourceColumn: 'text',
	outColumn: 'part',
	mode: 'sentences',
	lineBreak: 'any',
	pattern: '(?<=[.!?])\\s+',
	delimiter: '\n',
	flags: '',
	trim: true,
	dropEmpty: true,
	emitIndex: true,
	emitSourceRow: true,
	maxParts: 5000
};

export const defaultTransformQualityGateParams: TransformQualityGateParams = {
	checks: [],
	stopOnFail: true
};

export const defaultTransformMlContractParams: TransformMlContractParams = {
	taskType: 'other',
	labelColumn: 'label',
	featureColumns: ['text'],
	idColumn: '',
	timestampColumn: '',
	allowExtraFeatures: true,
	requireNonNullLabel: true
};

export const defaultTransformParamsByKind = {
	filter: {
		op: 'filter',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		filter: defaultTransformFilterParams
	},
	select: {
		op: 'select',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		select: defaultTransformSelectParams
	},
	rename: {
		op: 'rename',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		rename: defaultTransformRenameParams
	},
	derive: {
		op: 'derive',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		derive: defaultTransformDeriveParams
	},
	aggregate: {
		op: 'aggregate',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		aggregate: defaultTransformAggregateParams
	},
	join: {
		op: 'join',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		join: defaultTransformJoinParams
	},
	sort: {
		op: 'sort',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		sort: defaultTransformSortParams
	},
	limit: {
		op: 'limit',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		limit: defaultTransformLimitParams
	},
	dedupe: {
		op: 'dedupe',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		dedupe: defaultTransformDedupeParams
	},
	null_policy: {
		op: 'null_policy',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		null_policy: defaultTransformNullPolicyParams
	},
	outlier_policy: {
		op: 'outlier_policy',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		outlier_policy: defaultTransformOutlierPolicyParams
	},
	text_clean: {
		op: 'text_clean',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		text_clean: defaultTransformTextCleanParams
	},
	nlp_normalize: {
		op: 'nlp_normalize',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		nlp_normalize: defaultTransformNlpNormalizeParams
	},
	tokenize_chunk: {
		op: 'tokenize_chunk',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		tokenize_chunk: defaultTransformTokenizeChunkParams
	},
	dataset_split: {
		op: 'dataset_split',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		dataset_split: defaultTransformDatasetSplitParams
	},
	class_imbalance: {
		op: 'class_imbalance',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		class_imbalance: defaultTransformClassImbalanceParams
	},
	categorical_encode: {
		op: 'categorical_encode',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		categorical_encode: defaultTransformCategoricalEncodeParams
	},
	numeric_scale: {
		op: 'numeric_scale',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		numeric_scale: defaultTransformNumericScaleParams
	},
	embedding: {
		op: 'embedding',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		embedding: defaultTransformEmbeddingParams
	},
	feature_selection: {
		op: 'feature_selection',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		feature_selection: defaultTransformFeatureSelectionParams
	},
	leakage_detect: {
		op: 'leakage_detect',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		leakage_detect: defaultTransformLeakageDetectParams
	},
	quality_profile: {
		op: 'quality_profile',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		quality_profile: defaultTransformQualityProfileParams
	},
	drift_compare: {
		op: 'drift_compare',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		drift_compare: defaultTransformDriftCompareParams
	},
	determinism_profile: {
		op: 'determinism_profile',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		determinism_profile: defaultTransformDeterminismProfileParams
	},
	fit_state_registry: {
		op: 'fit_state_registry',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		fit_state_registry: defaultTransformFitStateRegistryParams
	},
	pii_guard: {
		op: 'pii_guard',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		pii_guard: defaultTransformPiiGuardParams
	},
	inference_parity: {
		op: 'inference_parity',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		inference_parity: defaultTransformInferenceParityParams
	},
	split: {
		op: 'split',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		split: defaultTransformSplitParams
	},
	quality_gate: {
		op: 'quality_gate',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		quality_gate: defaultTransformQualityGateParams
	},
	ml_contract: {
		op: 'ml_contract',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		ml_contract: defaultTransformMlContractParams
	},
	sql: {
		op: 'sql',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		sql: defaultTransformSqlParams
	},
	json_to_table: {
		op: 'json_to_table',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		json_to_table: defaultTransformJsonToTableParams
	},
	text_to_table: {
		op: 'text_to_table',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		text_to_table: defaultTransformTextToTableParams
	},
	table_to_json: {
		op: 'table_to_json',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		table_to_json: defaultTransformTableToJsonParams
	}
} as const;

export const defaultTransformParams: TransformParams = {
	op: 'filter',
	enabled: true,
	notes: '',
	cache: { enabled: false },
	filter: { expr: 'length(text) > 10' }
};

export const defaultTransformNodeData = {
	kind: 'transform' as const,
	transformKind: 'filter' as const,
	label: 'Transform',
	params: defaultTransformParams,
	status: 'idle' as const,
};
