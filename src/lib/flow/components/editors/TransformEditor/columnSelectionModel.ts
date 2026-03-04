import { uniqueStrings } from '$lib/flow/components/editors/shared';
import type { NodeExecutionError } from '$lib/flow/store/graphStore';

export function normalizeColumnNames(values: unknown[]): string[] {
	return uniqueStrings((values ?? []).map((v) => String(v).trim()).filter(Boolean));
}

export function toSchemaColumns(inputColumns: string[]): string[] {
	return normalizeColumnNames(inputColumns as unknown[]).sort((a, b) => a.localeCompare(b));
}

export type ColumnUniverseInput = {
	stickyColumns: string[];
	schemaColumns: string[];
	selectedColumns: string[];
};

export type ColumnUniverse = {
	hasKnownSchema: boolean;
	nextStickyColumns: string[];
	knownColumns: string[];
	availableColumns: string[];
	unknownFromSchema: string[];
};

export function computeColumnUniverse(input: ColumnUniverseInput): ColumnUniverse {
	const schemaColumns = toSchemaColumns(input.schemaColumns);
	const selectedColumns = normalizeColumnNames(input.selectedColumns as unknown[]);
	const hasKnownSchema = schemaColumns.length > 0;
	const nextStickyColumns = hasKnownSchema
		? normalizeColumnNames(schemaColumns as unknown[])
		: normalizeColumnNames(input.stickyColumns as unknown[]);
	const knownColumns = normalizeColumnNames([
		...nextStickyColumns,
		...schemaColumns,
		...selectedColumns
	] as unknown[]).sort((a, b) => a.localeCompare(b));
	const availableColumns = knownColumns.filter((col) => !selectedColumns.includes(col));
	const unknownFromSchema = hasKnownSchema
		? selectedColumns.filter((col) => !schemaColumns.includes(col))
		: [];
	return {
		hasKnownSchema,
		nextStickyColumns,
		knownColumns,
		availableColumns,
		unknownFromSchema
	};
}

export function missingColumnsFromError(
	err: NodeExecutionError | null,
	allowedErrorCodes: string[],
	validPathMatcher: (path: string) => boolean
): string[] {
	const code = String(err?.errorCode ?? '');
	const path = String(err?.paramPath ?? '');
	if (!allowedErrorCodes.includes(code) || !validPathMatcher(path)) return [];
	return normalizeColumnNames(
		(Array.isArray(err?.missingColumns) ? err.missingColumns : []) as unknown[]
	);
}

export function isByParamPath(path: string): boolean {
	return path === 'by' || path.endsWith('/by') || path.endsWith('.by');
}
