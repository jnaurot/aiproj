import { uniqueStrings } from '$lib/flow/components/editors/shared';
import type { TransformDedupeParams } from '$lib/flow/schema/transform';
import type { NodeExecutionError } from '$lib/flow/store/graphStore';
import { isByParamPath, missingColumnsFromError, normalizeColumnNames } from './columnSelectionModel';

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
	return !useByColumns || normalizedBy.length > 0;
}

export function resolveDedupeAvailableColumns(
	inputColumns: string[],
	errorAvailableColumns: string[],
	by: string[] = []
): string[] {
	const base = errorAvailableColumns.length > 0 ? errorAvailableColumns : inputColumns;
	return normalizeColumnNames([...base, ...by] as unknown[]);
}

export function missingDedupeColumnsFromError(nodeError: NodeExecutionError | null): string[] {
	return missingColumnsFromError(
		nodeError,
		['MISSING_COLUMN', 'COLUMN_SELECTION_REQUIRED'],
		(path) => path === 'params.dedupe.by' || isByParamPath(path)
	);
}
