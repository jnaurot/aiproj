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
	TransformSqlParams,
	TransformSplitParams
} from '$lib/flow/schema/transform';

export const defaultTransformFilterParams: TransformFilterParams = {
	expr: 'length(text) > 10'
};

export const defaultTransformSelectParams: TransformSelectParams = {
	columns: ['text', 'id']
};

export const defaultTransformRenameParams: TransformRenameParams = {
	map: {
		text: 'description',
		column0: 'value'
	}
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
	groupBy: ['category'],
	metrics: [
		{
			as: 'row_count',
			expr: 'count(*)'
		},
		{
			as: 'avg_length',
			expr: 'avg(length(text))'
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

export const defaultTransformSplitParams: TransformSplitParams = {
	sourceColumn: 'text',
	outColumn: 'part',
	mode: 'sentences',
	pattern: '(?<=[.!?])\\s+',
	delimiter: '\n',
	flags: '',
	trim: true,
	dropEmpty: true,
	emitIndex: true,
	emitSourceRow: true,
	maxParts: 5000
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
	sql: {
		op: 'sql',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		sql: defaultTransformSqlParams
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
	ports: { in: 'table', out: 'table' as const }
};
