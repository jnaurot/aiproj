import { uniqueStrings } from '$lib/flow/components/editors/shared';
import type { TransformDedupeParams } from '$lib/flow/schema/transform';
import type { NodeExecutionError } from '$lib/flow/store/graphStore';
import { SCHEMA_UNAVAILABLE_VALUE } from '$lib/flow/constants';

type LegacyDedupeParams = {
	by?: unknown;
	allColumns?: unknown;
};

function quoteSqlIdent(identifier: string): string {
	return `"${String(identifier).replaceAll('"', '""')}"`;
}

export function normalizeDedupeParams(
	params: (Partial<TransformDedupeParams> & LegacyDedupeParams) | undefined
): TransformDedupeParams {
	const by = uniqueStrings((params?.by ?? []).map((v) => String(v).trim()).filter(Boolean));
	const allColumns = params?.allColumns === true;
	return {
		allColumns,
		by: allColumns ? [] : by
	};
}

export function dedupePreviewSql(params: Pick<TransformDedupeParams, 'by'>): string {
	const by = uniqueStrings((params.by ?? []).map((v) => String(v).trim()).filter(Boolean));
	const partition = by.length === 0 ? '<all columns>' : by.map((c) => quoteSqlIdent(c)).join(',');
	return `WITH ordered AS (
  SELECT *, row_number() OVER () AS "__rowid"
	FROM input
)
SELECT * EXCLUDE (rn, __rowid)
FROM (
  SELECT *,
	row_number() OVER (PARTITION BY ${partition} ORDER BY "__rowid") AS rn
	FROM ordered
)
WHERE rn = 1`;
}

export function canCommitDedupeDraft(useByColumns: boolean, by: string[]): boolean {
	const normalizedBy = uniqueStrings((by ?? []).map((v) => String(v).trim()).filter(Boolean));
	if (!useByColumns) return true;
	const hasReal = normalizedBy.some((c) => c !== SCHEMA_UNAVAILABLE_VALUE);
	const hasPlaceholder = normalizedBy.includes(SCHEMA_UNAVAILABLE_VALUE);
	return hasReal && !hasPlaceholder;
}

export function resolveDedupeAvailableColumns(
	inputColumns: string[],
	errorAvailableColumns: string[],
	by: string[] = []
): string[] {
	const base = errorAvailableColumns.length > 0 ? errorAvailableColumns : inputColumns;
	return uniqueStrings([...base, ...by].map((c) => String(c).trim()).filter(Boolean));
}

export function missingDedupeColumnsFromError(nodeError: NodeExecutionError | null): string[] {
	const path = String(nodeError?.paramPath ?? '');
	const validPath =
		path === 'by' ||
		path === 'params.dedupe.by' ||
		path.endsWith('/by') ||
		path.endsWith('.by');
	const code = String(nodeError?.errorCode ?? '');
	if (!['MISSING_COLUMN', 'COLUMN_SELECTION_REQUIRED'].includes(code) || !validPath) return [];
	return uniqueStrings(
		(Array.isArray(nodeError?.missingColumns) ? nodeError.missingColumns : [])
			.map((c) => String(c).trim())
			.filter(Boolean)
	);
}
