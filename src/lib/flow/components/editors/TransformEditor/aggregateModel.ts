import type { TransformAggregateParams } from '$lib/flow/schema/transform';
import type { NodeExecutionError } from '$lib/flow/store/graphStore';
import { normalizeColumnNames } from './columnSelectionModel';

export type AggregateMetric = NonNullable<TransformAggregateParams['metrics']>[number];
export type AggregateOp = AggregateMetric['op'];

export const AGG_OPS: AggregateOp[] = [
	'count_rows',
	'count',
	'count_distinct',
	'min',
	'max',
	'sum',
	'mean',
	'avg_length',
	'min_length',
	'max_length'
];

const OP_NEEDS_COLUMN = new Set<AggregateOp>([
	'count',
	'count_distinct',
	'min',
	'max',
	'sum',
	'mean',
	'avg_length',
	'min_length',
	'max_length'
]);

function parseLegacyMetric(item: Record<string, unknown>): AggregateMetric | null {
	const legacyName = String(item.as ?? '').trim();
	const legacyExpr = String(item.expr ?? '').trim();
	if (!legacyName || !legacyExpr) return null;
	const m = legacyExpr.match(/^\s*([a-z_]+)\((.*)\)\s*$/i);
	if (!m) {
		return { name: legacyName, op: 'count_rows', column: null };
	}
	const fn = String(m[1] ?? '').toLowerCase().trim();
	const argRaw = String(m[2] ?? '').trim();
	if (fn === 'count' && argRaw === '*') {
		return { name: legacyName, op: 'count_rows', column: null };
	}
	const lengthArg = argRaw.match(/^length\(([^)]+)\)$/i);
	if (fn === 'avg' && lengthArg) {
		return { name: legacyName, op: 'avg_length', column: String(lengthArg[1] ?? '').trim().replace(/^"|"$/g, '') };
	}
	if (fn === 'min' && lengthArg) {
		return { name: legacyName, op: 'min_length', column: String(lengthArg[1] ?? '').trim().replace(/^"|"$/g, '') };
	}
	if (fn === 'max' && lengthArg) {
		return { name: legacyName, op: 'max_length', column: String(lengthArg[1] ?? '').trim().replace(/^"|"$/g, '') };
	}
	const arg = argRaw.replace(/^"|"$/g, '').trim();
	if (fn === 'avg') return { name: legacyName, op: 'mean', column: arg };
	if (fn === 'count_distinct') return { name: legacyName, op: 'count_distinct', column: arg };
	if ((AGG_OPS as string[]).includes(fn)) {
		return {
			name: legacyName,
			op: fn as AggregateOp,
			column: OP_NEEDS_COLUMN.has(fn as AggregateOp) ? arg : null
		};
	}
	return { name: legacyName, op: 'count_rows', column: null };
}

export function opNeedsColumn(op: AggregateOp): boolean {
	return OP_NEEDS_COLUMN.has(op);
}

export function defaultMetricName(op: AggregateOp): string {
	switch (op) {
		case 'count_rows':
			return 'row_count';
		case 'count':
			return 'count';
		case 'count_distinct':
			return 'count_distinct';
		case 'min':
			return 'min';
		case 'max':
			return 'max';
		case 'sum':
			return 'sum';
		case 'mean':
			return 'mean';
		case 'avg_length':
			return 'avg_length';
		case 'min_length':
			return 'min_length';
		case 'max_length':
			return 'max_length';
		default:
			return 'metric';
	}
}

type AggregateEditorParams = Partial<TransformAggregateParams> & {
	aggregate?: Partial<TransformAggregateParams>;
};

export function normalizeAggregateParams(
	params: AggregateEditorParams | undefined
): TransformAggregateParams {
	const rawGroupBy = Array.isArray(params?.groupBy)
		? params.groupBy
		: Array.isArray(params?.aggregate?.groupBy)
			? params.aggregate.groupBy
			: [];
	const groupBy = normalizeColumnNames(rawGroupBy as unknown[]);

	const rawMetrics = Array.isArray(params?.metrics)
		? params.metrics
		: Array.isArray(params?.aggregate?.metrics)
			? params.aggregate.metrics
			: [];
	const metrics: AggregateMetric[] = [];
	for (const item of rawMetrics as unknown[]) {
		if (!item || typeof item !== 'object') continue;
		const candidate = item as Record<string, unknown>;
		if ('as' in candidate || 'expr' in candidate) {
			const parsedLegacy = parseLegacyMetric(candidate);
			if (parsedLegacy) metrics.push(parsedLegacy);
			continue;
		}
		const name = String(candidate.name ?? '').trim();
		const opRaw = String(candidate.op ?? '').trim();
		if (!name || !(AGG_OPS as string[]).includes(opRaw)) continue;
		const op = opRaw as AggregateOp;
		const columnVal = candidate.column;
		const column =
			typeof columnVal === 'string' && columnVal.trim().length > 0 ? columnVal.trim() : null;
		metrics.push({ name, op, column: opNeedsColumn(op) ? column : null });
	}
	if (metrics.length === 0) {
		metrics.push({ name: 'row_count', op: 'count_rows', column: null });
	}
	const seen = new Set<string>();
	const deduped: AggregateMetric[] = [];
	for (const metric of metrics) {
		const key = metric.name;
		if (seen.has(key)) continue;
		seen.add(key);
		deduped.push(metric);
	}
	return { groupBy, metrics: deduped };
}

export function metricNameIsDuplicate(metrics: AggregateMetric[], name: string, index: number): boolean {
	const needle = String(name ?? '').trim();
	if (!needle) return false;
	for (let i = 0; i < metrics.length; i += 1) {
		if (i === index) continue;
		if (String(metrics[i]?.name ?? '').trim() === needle) return true;
	}
	return false;
}

export function missingAggregateColumnsFromError(err: NodeExecutionError | null): string[] {
	if (!err) return [];
	const code = String(err.errorCode ?? '');
	const path = String(err.paramPath ?? '');
	if (code !== 'MISSING_COLUMN') return [];
	if (!(path === 'params.aggregate.groupBy' || path.startsWith('params.aggregate.metrics'))) return [];
	return normalizeColumnNames((Array.isArray(err.missingColumns) ? err.missingColumns : []) as unknown[]);
}

export function availableAggregateColumnsFromError(err: NodeExecutionError | null): string[] {
	if (!err) return [];
	const code = String(err.errorCode ?? '');
	const path = String(err.paramPath ?? '');
	if (code !== 'MISSING_COLUMN') return [];
	if (!(path === 'params.aggregate.groupBy' || path.startsWith('params.aggregate.metrics'))) return [];
	return normalizeColumnNames((Array.isArray(err.availableColumns) ? err.availableColumns : []) as unknown[]);
}

export function validateAggregateDraft(params: TransformAggregateParams): { ok: boolean; message?: string } {
	if (!Array.isArray(params.metrics) || params.metrics.length === 0) {
		return { ok: false, message: 'At least one metric is required.' };
	}
	for (let i = 0; i < params.metrics.length; i += 1) {
		const metric = params.metrics[i];
		const name = String(metric.name ?? '').trim();
		if (!name) return { ok: false, message: `Metric ${i + 1} requires a name.` };
		if (metricNameIsDuplicate(params.metrics, name, i)) {
			return { ok: false, message: `Metric name '${name}' is duplicated.` };
		}
		if (opNeedsColumn(metric.op) && !String(metric.column ?? '').trim()) {
			return { ok: false, message: `Metric '${name}' requires a column.` };
		}
	}
	return { ok: true };
}
