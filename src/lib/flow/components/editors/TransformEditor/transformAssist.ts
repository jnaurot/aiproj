import type { NodeExecutionError, NodeSchemaContractEdge } from '$lib/flow/store/graphStore';
import type { TransformKind } from '$lib/flow/types/paramsMap';

export type TransformGuidedControl = {
	id: string;
	label: string;
	description: string;
};

export type TransformAutoFix = {
	id: string;
	label: string;
	patch: Record<string, unknown>;
};

export type TransformPreviewDiff = {
	beforeColumns: string[];
	afterColumns: string[];
	beforeRows: Array<Record<string, unknown>>;
	afterRows: Array<Record<string, unknown>>;
	notes: string[];
};

function uniq(values: string[]): string[] {
	const seen = new Set<string>();
	const out: string[] = [];
	for (const raw of values) {
		const value = String(raw ?? '').trim();
		if (!value || seen.has(value)) continue;
		seen.add(value);
		out.push(value);
	}
	return out;
}

function asObject(value: unknown): Record<string, unknown> {
	return value && typeof value === 'object' && !Array.isArray(value)
		? (value as Record<string, unknown>)
		: {};
}

function readNestedParams(params: Record<string, unknown>, op: TransformKind): Record<string, unknown> {
	const nested = asObject(params[op]);
	if (Object.keys(nested).length > 0) return nested;
	return params;
}

export function guidedControlsForTransform(kind: TransformKind): TransformGuidedControl[] {
	const map: Record<string, TransformGuidedControl[]> = {
		filter: [
			{ id: 'expr', label: 'Filter Expression', description: 'Keep rows matching a boolean expression.' },
			{ id: 'schema_cols', label: 'Schema Columns', description: 'Insert known columns directly into the expression.' },
			{ id: 'validate', label: 'Validation', description: 'Run with schema diagnostics to catch unknown columns.' }
		],
		select: [
			{ id: 'mode', label: 'Mode', description: 'Choose include/exclude behavior.' },
			{ id: 'columns', label: 'Columns', description: 'Pick 1-N key columns to keep/drop.' },
			{ id: 'strict', label: 'Strict Check', description: 'Fail early on missing columns when enabled.' }
		],
		rename: [
			{ id: 'map', label: 'Rename Map', description: 'Map old names to stable new names.' },
			{ id: 'dupes', label: 'Collision Check', description: 'Prevent duplicate target names.' },
			{ id: 'schema_preview', label: 'Schema Preview', description: 'Confirm output schema before run.' }
		],
		derive: [
			{ id: 'columns', label: 'Derived Columns', description: 'Define each output column and expression.' },
			{ id: 'types', label: 'Type Awareness', description: 'Use input types to avoid coercion surprises.' },
			{ id: 'preview', label: 'Preview', description: 'Review new columns on sample rows.' }
		],
		aggregate: [
			{ id: 'groupBy', label: 'Group By', description: 'Choose grouping keys first.' },
			{ id: 'metrics', label: 'Metrics', description: 'Add 1-N aggregate metrics.' },
			{ id: 'schema', label: 'Output Schema', description: 'Validate resulting metric column names.' }
		],
		join: [
			{ id: 'clauses', label: 'Join Clauses', description: 'Build left/right key pairs.' },
			{ id: 'how', label: 'Join Type', description: 'Pick inner/left/right/full semantics.' },
			{ id: 'chain', label: 'Connected Chain', description: 'Ensure each clause links to joined nodes.' }
		],
		sort: [
			{ id: 'by', label: 'Sort Keys', description: 'Pick ordered sort keys.' },
			{ id: 'direction', label: 'Direction', description: 'Set asc/desc for each key.' },
			{ id: 'stability', label: 'Stable Order', description: 'Prefer deterministic key combinations.' }
		],
		limit: [
			{ id: 'n', label: 'Row Limit', description: 'Set the output row cap.' },
			{ id: 'preview', label: 'Preview', description: 'Preview first N rows from current input.' },
			{ id: 'cost', label: 'Cost Guard', description: 'Use early limits to reduce downstream cost.' }
		],
		dedupe: [
			{ id: 'all', label: 'All Columns', description: 'Toggle full-row dedupe mode.' },
			{ id: 'by', label: 'Key Columns', description: 'Choose dedupe keys when not all-columns.' },
			{ id: 'keep', label: 'Keep Policy', description: 'Control deterministic first-row retention.' }
		],
		split: [
			{ id: 'source', label: 'Source Column', description: 'Select text column to split.' },
			{ id: 'mode', label: 'Split Mode', description: 'Pick sentence/line/regex/delimiter.' },
			{ id: 'maxParts', label: 'Safety Cap', description: 'Limit emitted fragments for robustness.' }
		],
		quality_gate: [
			{ id: 'checks', label: 'Checks', description: 'Add high-value quality checks first.' },
			{ id: 'severity', label: 'Severity', description: 'Set warn/fail behavior per check.' },
			{ id: 'stop', label: 'Stop On Fail', description: 'Block downstream when quality fails.' }
		],
		sql: [
			{ id: 'dialect', label: 'Dialect', description: 'Set SQL dialect compatibility.' },
			{ id: 'query', label: 'Query', description: 'Author SQL against input table(s).' },
			{ id: 'schema', label: 'Schema Check', description: 'Validate referenced columns before run.' }
		],
		json_to_table: [
			{ id: 'orient', label: 'JSON Orient', description: 'Select records/object layout.' },
			{ id: 'rowsKey', label: 'Rows Key', description: 'Set the key containing row arrays.' },
			{ id: 'schema', label: 'Table Schema', description: 'Verify extracted output columns.' }
		],
		text_to_table: [
			{ id: 'mode', label: 'Parse Mode', description: 'Use lines/csv/tsv parsing.' },
			{ id: 'column', label: 'Column Name', description: 'Name emitted text column in lines mode.' },
			{ id: 'header', label: 'Header', description: 'Control CSV/TSV header handling.' }
		],
		table_to_json: [
			{ id: 'orient', label: 'JSON Orient', description: 'Choose records/split serialization.' },
			{ id: 'pretty', label: 'Pretty Output', description: 'Toggle human-readable formatting.' },
			{ id: 'payload', label: 'Payload Size', description: 'Watch output growth and memory cost.' }
		]
	};
	return map[kind] ?? [];
}

export function buildTransformAutoFixes(input: {
	kind: TransformKind;
	params: Record<string, unknown>;
	nodeError: NodeExecutionError | null;
	availableColumns: string[];
}): TransformAutoFix[] {
	const err = input.nodeError;
	if (!err) return [];
	const code = String(err.errorCode ?? '').trim();
	const path = String(err.paramPath ?? '').trim();
	const missing = uniq((err.missingColumns ?? []).map((v) => String(v)));
	const available = uniq([...(input.availableColumns ?? []), ...((err.availableColumns ?? []).map((v) => String(v)))]);
	const fixes: TransformAutoFix[] = [];
	if (code === 'MISSING_COLUMN' && missing.length > 0) {
		if (input.kind === 'select') {
			const nested = readNestedParams(input.params, 'select');
			const mode = String(nested.mode ?? 'include');
			const strict = Boolean(nested.strict ?? true);
			const cols = uniq(((nested.columns as unknown[]) ?? []).map((v) => String(v))).filter((c) => !missing.includes(c));
			fixes.push({
				id: 'select_drop_missing',
				label: 'Drop Missing Select Columns',
				patch: { op: 'select', select: { mode, strict, keepOrder: String(nested.keepOrder ?? 'custom'), columns: cols } }
			});
			if (strict) {
				fixes.push({
					id: 'select_disable_strict',
					label: 'Disable Strict For Now',
					patch: { op: 'select', select: { ...nested, strict: false } }
				});
			}
		}
		if (input.kind === 'sort') {
			const nested = readNestedParams(input.params, 'sort');
			const by = Array.isArray(nested.by) ? (nested.by as Array<Record<string, unknown>>) : [];
			const nextBy = by.filter((item) => !missing.includes(String(item?.col ?? '')));
			if (nextBy.length > 0) {
				fixes.push({ id: 'sort_drop_missing', label: 'Drop Missing Sort Keys', patch: { op: 'sort', sort: { by: nextBy } } });
			}
		}
		if (input.kind === 'dedupe') {
			const nested = readNestedParams(input.params, 'dedupe');
			const by = uniq(((nested.by as unknown[]) ?? []).map((v) => String(v))).filter((c) => !missing.includes(c));
			fixes.push({
				id: 'dedupe_drop_missing',
				label: 'Drop Missing Dedupe Keys',
				patch: { op: 'dedupe', dedupe: { ...nested, allColumns: by.length === 0, by } }
			});
		}
	}
	if (code === 'COLUMN_SELECTION_REQUIRED' && input.kind === 'dedupe') {
		fixes.push({
			id: 'dedupe_all_cols',
			label: 'Use Full-Row Dedupe',
			patch: { op: 'dedupe', dedupe: { allColumns: true, by: [] } }
		});
	}
	if (path.includes('join.clauses') && available.length > 1) {
		fixes.push({
			id: 'switch_to_select',
			label: 'Switch To Select Before Join',
			patch: { op: 'select', select: { mode: 'include', strict: true, keepOrder: 'input', columns: available.slice(0, 3) } }
		});
	}
	return fixes;
}

function projectColumns(kind: TransformKind, params: Record<string, unknown>, before: string[]): string[] {
	const b = uniq(before);
	if (kind === 'select') {
		const nested = readNestedParams(params, 'select');
		const mode = String(nested.mode ?? 'include');
		const cols = uniq(((nested.columns as unknown[]) ?? []).map((v) => String(v)));
		if (mode === 'exclude') return b.filter((c) => !cols.includes(c));
		return cols.length > 0 ? cols : b;
	}
	if (kind === 'rename') {
		const nested = readNestedParams(params, 'rename');
		const map = asObject(nested.map);
		return b.map((c) => {
			const mapped = String(map[c] ?? '').trim();
			return mapped || c;
		});
	}
	if (kind === 'derive') {
		const nested = readNestedParams(params, 'derive');
		const columns = Array.isArray(nested.columns) ? (nested.columns as Array<Record<string, unknown>>) : [];
		const derived = uniq(columns.map((c) => String(c?.name ?? '')));
		return uniq([...b, ...derived]);
	}
	if (kind === 'aggregate') {
		const nested = readNestedParams(params, 'aggregate');
		const groupBy = uniq(((nested.groupBy as unknown[]) ?? []).map((v) => String(v)));
		const metrics = Array.isArray(nested.metrics) ? (nested.metrics as Array<Record<string, unknown>>) : [];
		const metricNames = uniq(metrics.map((m) => String(m?.name ?? '')));
		return uniq([...groupBy, ...metricNames]);
	}
	if (kind === 'split') {
		const nested = readNestedParams(params, 'split');
		const outColumn = String(nested.outColumn ?? 'part').trim() || 'part';
		const next = uniq([...b, outColumn]);
		if (Boolean(nested.emitIndex ?? true)) next.push('index');
		if (Boolean(nested.emitSourceRow ?? true)) next.push('source_row');
		return uniq(next);
	}
	if (kind === 'text_to_table') {
		const nested = readNestedParams(params, 'text_to_table');
		const mode = String(nested.mode ?? 'lines');
		if (mode === 'lines') return [String(nested.column ?? 'text').trim() || 'text'];
		return b;
	}
	if (kind === 'table_to_json') return ['json'];
	return b;
}

function projectRows(kind: TransformKind, params: Record<string, unknown>, rows: Array<Record<string, unknown>>, afterColumns: string[]): Array<Record<string, unknown>> {
	const sample = rows.slice(0, 5).map((row) => ({ ...row }));
	if (kind === 'select') {
		return sample.map((row) => {
			const out: Record<string, unknown> = {};
			for (const col of afterColumns) out[col] = row[col];
			return out;
		});
	}
	if (kind === 'rename') {
		const nested = readNestedParams(params, 'rename');
		const map = asObject(nested.map);
		return sample.map((row) => {
			const out: Record<string, unknown> = {};
			for (const [key, value] of Object.entries(row)) {
				const renamed = String(map[key] ?? '').trim();
				out[renamed || key] = value;
			}
			return out;
		});
	}
	if (kind === 'derive') {
		const nested = readNestedParams(params, 'derive');
		const columns = Array.isArray(nested.columns) ? (nested.columns as Array<Record<string, unknown>>) : [];
		const names = uniq(columns.map((c) => String(c?.name ?? '')));
		return sample.map((row) => {
			const out = { ...row };
			for (const name of names) {
				if (!(name in out)) out[name] = null;
			}
			return out;
		});
	}
	if (kind === 'limit') {
		const nested = readNestedParams(params, 'limit');
		const n = Math.max(1, Number(nested.n ?? 1));
		return sample.slice(0, n);
	}
	if (kind === 'table_to_json') {
		return sample.map((row) => ({ json: JSON.stringify(row) }));
	}
	return sample;
}

export function buildTransformPreviewDiff(input: {
	kind: TransformKind;
	params: Record<string, unknown>;
	inputColumns: string[];
	sampleRows: Array<Record<string, unknown>>;
}): TransformPreviewDiff {
	const beforeColumns = uniq(input.inputColumns ?? []);
	const afterColumns = projectColumns(input.kind, input.params, beforeColumns);
	const beforeRows = (input.sampleRows ?? []).slice(0, 5).map((row) => ({ ...row }));
	const afterRows = projectRows(input.kind, input.params, beforeRows, afterColumns);
	const notes: string[] = [];
	if (input.kind === 'filter') notes.push('Filter preview does not evaluate expression until run.');
	if (input.kind === 'sql') notes.push('SQL preview is schema-only until run.');
	if (input.kind === 'join') notes.push('Join preview uses current input schema union, not executed rows.');
	return { beforeColumns, afterColumns, beforeRows, afterRows, notes };
}

export type NextTransformSuggestion = {
	op: TransformKind;
	reason: string;
};

export function suggestNextTransformOps(input: {
	kind: TransformKind;
	nodeError: NodeExecutionError | null;
	schemaEdges: NodeSchemaContractEdge[];
}): NextTransformSuggestion[] {
	const out: NextTransformSuggestion[] = [];
	const normalizeType = (raw: string): string => {
		const t = String(raw ?? '').trim().toLowerCase();
		if (t === 'string') return 'text';
		return t;
	};
	for (const edge of input.schemaEdges ?? []) {
		if (edge.direction !== 'incoming') continue;
		const providedType = normalizeType(String(edge.providedSchema?.type ?? ''));
		const requiredType = normalizeType(String(edge.requiredSchema?.type ?? ''));
		if (providedType === 'text' && requiredType === 'table') {
			out.push({ op: 'text_to_table', reason: 'Incoming text must be converted to table.' });
		}
		if (providedType === 'json' && requiredType === 'table') {
			out.push({ op: 'json_to_table', reason: 'Incoming JSON must be converted to table.' });
		}
		if (providedType === 'table' && requiredType === 'json') {
			out.push({ op: 'table_to_json', reason: 'Downstream requires JSON payload.' });
		}
		if (edge.severity === 'error' && requiredType === 'table' && providedType === 'table') {
			out.push({ op: 'select', reason: 'Schema contract indicates table-shape mismatch.' });
			out.push({ op: 'rename', reason: 'Rename can align columns to contract requirements.' });
		}
	}
	const errCode = String(input.nodeError?.errorCode ?? '').trim();
	if (errCode === 'MISSING_COLUMN') {
		out.push({ op: 'select', reason: 'Runtime missing columns can be narrowed with Select.' });
		out.push({ op: 'rename', reason: 'Runtime missing columns may be naming mismatch.' });
	}
	if (errCode === 'EXPR_INVALID') {
		out.push({ op: 'derive', reason: 'Derive can stage clean intermediate expressions.' });
	}
	const seen = new Set<string>();
	return out.filter((item) => {
		const key = `${item.op}|${item.reason}`;
		if (seen.has(key)) return false;
		seen.add(key);
		return true;
	}).slice(0, 5);
}
