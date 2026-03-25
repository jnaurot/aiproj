export type FilterMode = 'rules' | 'sql';
export type FilterOperator =
	| 'eq'
	| 'ne'
	| 'gt'
	| 'gte'
	| 'lt'
	| 'lte'
	| 'contains'
	| 'in'
	| 'not_in'
	| 'regex'
	| 'exists'
	| 'between'
	| 'is_null'
	| 'not_null';

export type FilterValueSource = 'literal' | 'param_config';

export type FilterCondition = {
	kind: 'condition';
	column: string;
	op: FilterOperator;
	valueSource?: FilterValueSource;
	literalValue?: string;
	paramPath?: string;
};

export type FilterGroup = {
	kind: 'group';
	op: 'all' | 'any';
	conditions: FilterRuleNode[];
};

export type FilterRuleNode = FilterGroup | FilterCondition;

export const FILTER_OPERATORS: Array<{
	value: FilterOperator;
	label: string;
	needsValue: boolean;
}> = [
	{ value: 'eq', label: '=', needsValue: true },
	{ value: 'ne', label: '!=', needsValue: true },
	{ value: 'gt', label: '>', needsValue: true },
	{ value: 'gte', label: '>=', needsValue: true },
	{ value: 'lt', label: '<', needsValue: true },
	{ value: 'lte', label: '<=', needsValue: true },
	{ value: 'contains', label: 'contains', needsValue: true },
	{ value: 'in', label: 'in', needsValue: true },
	{ value: 'not_in', label: 'not in', needsValue: true },
	{ value: 'regex', label: 'regex', needsValue: true },
	{ value: 'exists', label: 'exists', needsValue: false },
	{ value: 'between', label: 'between', needsValue: true },
	{ value: 'is_null', label: 'is null', needsValue: false },
	{ value: 'not_null', label: 'not null', needsValue: false }
];

export function defaultFilterRules(): FilterGroup {
	return {
		kind: 'group',
		op: 'all',
		conditions: []
	};
}

export function normalizeFilterParams(raw: Record<string, unknown> | undefined): {
	mode: FilterMode;
	expr: string;
	rules: FilterGroup;
} {
	const expr = typeof raw?.expr === 'string' ? raw.expr : '';
	const modeRaw = typeof raw?.mode === 'string' ? raw.mode : '';
	const mode: FilterMode = modeRaw === 'sql' || modeRaw === 'rules' ? modeRaw : expr.trim().length > 0 ? 'sql' : 'rules';
	const rules = normalizeGroup(raw?.rules);
	return { mode, expr, rules };
}

function normalizeGroup(raw: unknown): FilterGroup {
	if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return defaultFilterRules();
	const record = raw as Record<string, unknown>;
	const op = record.op === 'any' ? 'any' : 'all';
	const conditionsRaw = Array.isArray(record.conditions) ? record.conditions : [];
	const conditions: FilterRuleNode[] = conditionsRaw
		.map((entry) => normalizeRuleNode(entry))
		.filter((entry): entry is FilterRuleNode => entry != null);
	return {
		kind: 'group',
		op,
		conditions
	};
}

function normalizeRuleNode(raw: unknown): FilterRuleNode | null {
	if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return null;
	const record = raw as Record<string, unknown>;
	const kind = String(record.kind ?? '').trim().toLowerCase();
	if (kind === 'group') return normalizeGroup(record);
	const op = normalizeOperator(record.op);
	if (op == null) return null;
	const parsedValue = normalizeConditionValue(record);
	return {
		kind: 'condition',
		column: String(record.column ?? record.path ?? ''),
		op,
		valueSource: parsedValue.valueSource,
		literalValue: parsedValue.literalValue,
		paramPath: parsedValue.paramPath
	};
}

function normalizeOperator(raw: unknown): FilterOperator | null {
	const value = String(raw ?? '').trim() as FilterOperator;
	return FILTER_OPERATORS.some((entry) => entry.value === value) ? value : 'eq';
}

function normalizeConditionValue(record: Record<string, unknown>): {
	valueSource: FilterValueSource;
	literalValue: string;
	paramPath: string;
} {
	const explicitSource = record.valueSource === 'param_config' ? 'param_config' : 'literal';
	const explicitLiteral = record.literalValue;
	const explicitParamPath = record.paramPath;
	if (explicitSource === 'param_config' || explicitLiteral != null || explicitParamPath != null) {
		return {
			valueSource: explicitSource,
			literalValue: String(explicitLiteral ?? ''),
			paramPath: String(explicitParamPath ?? '')
		};
	}

	// Persisted schema form:
	// - literal: { value: <scalar|array> }
	// - param:   { value: { valueFrom: { handle: "param_config", path: "..." } } }
	const rawValue = record.value;
	if (rawValue && typeof rawValue === 'object' && !Array.isArray(rawValue)) {
		const valueRecord = rawValue as Record<string, unknown>;
		const valueFrom = valueRecord.valueFrom;
		if (valueFrom && typeof valueFrom === 'object' && !Array.isArray(valueFrom)) {
			const vf = valueFrom as Record<string, unknown>;
			const handle = String(vf.handle ?? '').trim();
			const path = String(vf.path ?? '').trim();
			if (handle === 'param_config' || path.length > 0) {
				return {
					valueSource: 'param_config',
					literalValue: '',
					paramPath: path
				};
			}
		}
	}

	if (Array.isArray(rawValue)) {
		return {
			valueSource: 'literal',
			literalValue: rawValue.map((item) => String(item ?? '')).join(','),
			paramPath: ''
		};
	}

	if (rawValue != null) {
		return {
			valueSource: 'literal',
			literalValue: String(rawValue),
			paramPath: ''
		};
	}

	return {
		valueSource: 'literal',
		literalValue: '',
		paramPath: ''
	};
}
