import type { TransformDeriveParams } from '$lib/flow/schema/transform';

export type DeriveMode = 'rules' | 'sql';
export type DeriveFormulaOp =
	| 'add'
	| 'sub'
	| 'mul'
	| 'div'
	| 'concat'
	| 'lower'
	| 'upper'
	| 'trim'
	| 'length'
	| 'coalesce';

export type DeriveArgSource = 'literal' | 'column' | 'param_config';

export type DeriveSqlColumn = {
	name: string;
	expr: string;
};

export type DeriveRuleArg = {
	source: DeriveArgSource;
	literalValue?: string;
	column?: string;
	paramPath?: string;
};

export type DeriveRule = {
	name: string;
	op: DeriveFormulaOp;
	args: DeriveRuleArg[];
};

export type NormalizedDeriveParams = {
	mode: DeriveMode;
	columns: DeriveSqlColumn[];
	rules: DeriveRule[];
};

export const DERIVE_FORMULA_OPS: Array<{
	value: DeriveFormulaOp;
	label: string;
	minArgs: number;
	maxArgs: number | null;
}> = [
	{ value: 'add', label: 'add', minArgs: 2, maxArgs: 2 },
	{ value: 'sub', label: 'sub', minArgs: 2, maxArgs: 2 },
	{ value: 'mul', label: 'mul', minArgs: 2, maxArgs: 2 },
	{ value: 'div', label: 'div', minArgs: 2, maxArgs: 2 },
	{ value: 'concat', label: 'concat', minArgs: 2, maxArgs: null },
	{ value: 'lower', label: 'lower', minArgs: 1, maxArgs: 1 },
	{ value: 'upper', label: 'upper', minArgs: 1, maxArgs: 1 },
	{ value: 'trim', label: 'trim', minArgs: 1, maxArgs: 1 },
	{ value: 'length', label: 'length', minArgs: 1, maxArgs: 1 },
	{ value: 'coalesce', label: 'coalesce', minArgs: 1, maxArgs: null }
];

const DEFAULT_SQL_COLUMNS: DeriveSqlColumn[] = [{ name: 'length_text', expr: 'length("text")' }];
const DEFAULT_RULES: DeriveRule[] = [
	{
		name: 'length_text',
		op: 'length',
		args: [{ source: 'column', column: 'text' }]
	}
];

const VALID_OPS = new Set<DeriveFormulaOp>(DERIVE_FORMULA_OPS.map((entry) => entry.value));

function asRecord(value: unknown): Record<string, unknown> | null {
	if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
	return value as Record<string, unknown>;
}

function normalizeSqlColumns(raw: unknown): DeriveSqlColumn[] {
	if (!Array.isArray(raw)) return [];
	return raw
		.map((entry) => {
			const record = asRecord(entry);
			if (!record) return null;
			return {
				name: String(record.name ?? '').trim(),
				expr: String(record.expr ?? '').trim()
			};
		})
		.filter((entry): entry is DeriveSqlColumn => Boolean(entry && entry.name.length > 0 && entry.expr.length > 0))
		.filter((entry, index, arr) => arr.findIndex((candidate) => candidate.name === entry.name) === index);
}

function normalizeArg(raw: unknown): DeriveRuleArg {
	const record = asRecord(raw);
	if (!record) {
		return {
			source: 'literal',
			literalValue: String(raw ?? '')
		};
	}
	if (typeof record.column === 'string') {
		return {
			source: 'column',
			column: String(record.column ?? '').trim()
		};
	}
	const valueFrom = asRecord(record.valueFrom);
	if (valueFrom && String(valueFrom.handle ?? '') === 'param_config') {
		return {
			source: 'param_config',
			paramPath: String(valueFrom.path ?? '').trim()
		};
	}
	return {
		source: 'literal',
		literalValue: String(raw ?? '')
	};
}

function normalizeRules(raw: unknown): DeriveRule[] {
	if (!Array.isArray(raw)) return [];
	return raw
		.map((entry) => {
			const record = asRecord(entry);
			if (!record) return null;
			const formula = asRecord(record.formula);
			const opRaw = String(formula?.op ?? '').trim().toLowerCase();
			const op = (VALID_OPS.has(opRaw as DeriveFormulaOp) ? opRaw : 'add') as DeriveFormulaOp;
			const argsRaw = Array.isArray(formula?.args) ? formula?.args : [];
			return {
				name: String(record.name ?? '').trim(),
				op,
				args: argsRaw.map((arg) => normalizeArg(arg))
			};
		})
		.filter((entry): entry is DeriveRule => Boolean(entry && entry.name.length > 0));
}

export function defaultDeriveRules(): DeriveRule[] {
	return DEFAULT_RULES.map((rule) => ({
		name: rule.name,
		op: rule.op,
		args: rule.args.map((arg) => ({ ...arg }))
	}));
}

export function defaultDeriveSqlColumns(): DeriveSqlColumn[] {
	return DEFAULT_SQL_COLUMNS.map((column) => ({ ...column }));
}

export function normalizeDeriveParams(raw: Partial<TransformDeriveParams> | Record<string, unknown>): NormalizedDeriveParams {
	const root = asRecord(raw) ?? {};
	const nested = asRecord(root.derive);
	const params = nested ?? root;
	const columns = normalizeSqlColumns(params.columns);
	const rules = normalizeRules(params.rules);
	const explicitMode = String(params.mode ?? '').trim();
	const mode: DeriveMode = explicitMode === 'sql' || explicitMode === 'rules' ? (explicitMode as DeriveMode) : columns.length > 0 ? 'sql' : 'rules';
	return {
		mode,
		columns: columns.length > 0 ? columns : defaultDeriveSqlColumns(),
		rules: rules.length > 0 ? rules : defaultDeriveRules()
	};
}

export function validateRule(rule: DeriveRule): string[] {
	const messages: string[] = [];
	if (String(rule.name ?? '').trim().length === 0) messages.push('Output column name is required.');
	const spec = DERIVE_FORMULA_OPS.find((entry) => entry.value === rule.op);
	if (!spec) return ['Invalid formula operator.'];
	if (rule.args.length < spec.minArgs) messages.push(`'${spec.value}' requires at least ${spec.minArgs} argument(s).`);
	if (spec.maxArgs !== null && rule.args.length > spec.maxArgs) {
		messages.push(`'${spec.value}' allows at most ${spec.maxArgs} argument(s).`);
	}
	for (let i = 0; i < rule.args.length; i += 1) {
		const arg = rule.args[i];
		const label = `arg ${i + 1}`;
		if (arg.source === 'column' && String(arg.column ?? '').trim().length === 0) messages.push(`${label}: choose a column.`);
		if (arg.source === 'param_config' && String(arg.paramPath ?? '').trim().length === 0) messages.push(`${label}: param path is required.`);
		if (arg.source === 'literal' && String(arg.literalValue ?? '').trim().length === 0) messages.push(`${label}: literal value is required.`);
	}
	return messages;
}
