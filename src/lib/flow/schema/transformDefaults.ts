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
	TransformQualityGateParams,
	TransformSqlParams,
	TransformSplitParams,
	TransformJsonToTableParams,
	TransformTextToTableParams,
	TransformTableToJsonParams
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
	clauses: []
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
	allColumns: false,
	by: []
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
