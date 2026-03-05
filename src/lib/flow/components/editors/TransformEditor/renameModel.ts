import type { TransformRenameParams } from '$lib/flow/schema/transform';
import type { NodeExecutionError } from '$lib/flow/store/graphStore';
import { normalizeColumnNames, toSchemaColumns } from './columnSelectionModel';

export type RenamePair = { from: string; to: string };
export type RenameEditorParams = Partial<TransformRenameParams> & {
	rename?: Partial<TransformRenameParams>;
};

export function readRenameMap(raw: RenameEditorParams | undefined): Record<string, string> {
	const direct = raw?.map;
	if (direct && typeof direct === 'object' && !Array.isArray(direct)) {
		return direct as Record<string, string>;
	}
	const nested = raw?.rename?.map;
	if (nested && typeof nested === 'object' && !Array.isArray(nested)) {
		return nested as Record<string, string>;
	}
	return {};
}

export function mapToPairs(map: Record<string, string>): RenamePair[] {
	const out: RenamePair[] = [];
	for (const [fromRaw, toRaw] of Object.entries(map ?? {})) {
		const from = String(fromRaw ?? '').trim();
		const to = String(toRaw ?? '').trim();
		if (!from || !to) continue;
		out.push({ from, to });
	}
	return out;
}

export function pairsToMap(pairs: RenamePair[]): Record<string, string> {
	const out: Record<string, string> = {};
	for (const pair of pairs ?? []) {
		const from = String(pair?.from ?? '').trim();
		const to = String(pair?.to ?? '').trim();
		if (!from || !to) continue;
		out[from] = to;
	}
	return out;
}

export function normalizeRenameParams(raw: RenameEditorParams | undefined): TransformRenameParams {
	return { map: pairsToMap(mapToPairs(readRenameMap(raw))) };
}

export function availableRenameColumnsFromError(err: NodeExecutionError | null): string[] {
	if (!err) return [];
	const code = String(err.errorCode ?? '');
	const path = String(err.paramPath ?? '');
	if (code !== 'MISSING_COLUMN') return [];
	if (!(path === 'rename.map' || path === 'params.rename.map' || path.endsWith('.rename.map'))) {
		return [];
	}
	return normalizeColumnNames((Array.isArray(err.availableColumns) ? err.availableColumns : []) as unknown[]);
}

export function missingRenameColumnsFromError(err: NodeExecutionError | null): string[] {
	if (!err) return [];
	const code = String(err.errorCode ?? '');
	const path = String(err.paramPath ?? '');
	if (code !== 'MISSING_COLUMN') return [];
	if (!(path === 'rename.map' || path === 'params.rename.map' || path.endsWith('.rename.map'))) {
		return [];
	}
	return normalizeColumnNames((Array.isArray(err.missingColumns) ? err.missingColumns : []) as unknown[]);
}

export function computeRenameIssues(rows: RenamePair[], schemaColumns: string[]): {
	unknownSources: string[];
	duplicateSources: string[];
	duplicateTargets: string[];
	noOps: string[];
} {
	const schemaSet = new Set(schemaColumns);
	const sources = rows
		.map((r) => String(r.from ?? '').trim())
		.filter(Boolean);
	const targets = rows
		.map((r) => String(r.to ?? '').trim())
		.filter(Boolean);
	const unknownSources = normalizeColumnNames(
		schemaColumns.length > 0 ? sources.filter((s) => !schemaSet.has(s)) : []
	);
	const duplicateSources = normalizeColumnNames(
		sources.filter((s, i) => sources.indexOf(s) !== i)
	);
	const duplicateTargets = normalizeColumnNames(
		targets.filter((t, i) => targets.indexOf(t) !== i)
	);
	const noOps = normalizeColumnNames(
		rows
			.filter((r) => String(r.from ?? '').trim() && String(r.to ?? '').trim())
			.filter((r) => String(r.from).trim() === String(r.to).trim())
			.map((r) => String(r.from).trim())
	);
	return { unknownSources, duplicateSources, duplicateTargets, noOps };
}

export function computeRenamePreview(inputColumns: string[], map: Record<string, string>): {
	input: string;
	output: string;
	changed: boolean;
}[] {
	const cols = toSchemaColumns(inputColumns);
	return cols.map((input) => {
		const output = String(map?.[input] ?? input).trim() || input;
		return {
			input,
			output,
			changed: input !== output
		};
	});
}

export function findPreviewCollisions(preview: { input: string; output: string }[]): string[] {
	const outputs = preview.map((r) => r.output);
	return normalizeColumnNames(outputs.filter((name, i) => outputs.indexOf(name) !== i));
}

