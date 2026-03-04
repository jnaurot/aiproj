import type { TransformSortParams } from '$lib/flow/schema/transform';
import type { NodeExecutionError } from '$lib/flow/store/graphStore';
import {
	computeColumnUniverse,
	isByParamPath,
	missingColumnsFromError,
	normalizeColumnNames,
	toSchemaColumns
} from './columnSelectionModel';

export type SortItem = NonNullable<TransformSortParams['by']>[number];
export type SortDir = SortItem['dir'];
export type SortEditorParams = Partial<TransformSortParams> & { sort?: Partial<TransformSortParams> };

export function normalizeSortItems(raw: TransformSortParams['by'] | undefined): SortItem[] {
	const out: SortItem[] = [];
	for (const item of raw ?? []) {
		const col = String(item?.col ?? '').trim();
		if (!col) continue;
		if (out.some((x) => x.col === col)) continue;
		const dir: SortDir = item?.dir === 'desc' ? 'desc' : 'asc';
		out.push({ col, dir });
	}
	return out;
}

export function readSortBy(rawParams: SortEditorParams | undefined): TransformSortParams['by'] | undefined {
	if (Array.isArray(rawParams?.by)) return rawParams.by;
	if (Array.isArray(rawParams?.sort?.by)) return rawParams.sort.by;
	return undefined;
}

export function computeSortColumnState(
	inputColumns: string[],
	stickyKnownColumns: string[],
	selectedCols: string[]
): {
	hasKnownSchema: boolean;
	nextStickyKnownColumns: string[];
	knownColumns: string[];
	availableCols: string[];
	unknownFromSchema: string[];
} {
	const schemaColumns = toSchemaColumns(inputColumns);
	const universe = computeColumnUniverse({
		stickyColumns: stickyKnownColumns,
		schemaColumns,
		selectedColumns: selectedCols
	});
	return {
		hasKnownSchema: universe.hasKnownSchema,
		nextStickyKnownColumns: universe.nextStickyColumns,
		knownColumns: universe.knownColumns,
		availableCols: universe.availableColumns,
		unknownFromSchema: universe.unknownFromSchema
	};
}

export function schemaColumnsFromInput(inputColumns: string[]): string[] {
	return toSchemaColumns(inputColumns);
}

export function normalizeSortDraftColumn(value: string): string {
	return normalizeColumnNames([value] as unknown[])[0] ?? '';
}

export function missingSortColumnsFromError(err: NodeExecutionError | null): string[] {
	return missingColumnsFromError(
		err,
		['MISSING_COLUMN'],
		(path) => path === 'params.sort.by' || isByParamPath(path)
	);
}
