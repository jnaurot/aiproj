import type { TransformJoinParams } from '$lib/flow/schema/transform';
import type { NodeExecutionError } from '$lib/flow/store/graphStore';
import type { InputSchemaView } from './inputSchema';

export type JoinClause = NonNullable<TransformJoinParams['clauses']>[number];
export type JoinHow = JoinClause['how'];

export type JoinEditorParams = Partial<TransformJoinParams> & { join?: Partial<TransformJoinParams> };

const joinModes: JoinHow[] = ['inner', 'left', 'right', 'full'];

export function supportedJoinModes(): JoinHow[] {
	return [...joinModes];
}

export function shortNodeId(nodeId: string): string {
	const id = String(nodeId ?? '');
	if (id.startsWith('n_')) return `n_${id.slice(2, 10)}`;
	return id.slice(0, 10);
}

export function normalizeJoinClauses(raw: TransformJoinParams['clauses'] | undefined): JoinClause[] {
	const out: JoinClause[] = [];
	for (const clause of raw ?? []) {
		const leftNodeId = String(clause?.leftNodeId ?? '').trim();
		const leftCol = String(clause?.leftCol ?? '').trim();
		const rightNodeId = String(clause?.rightNodeId ?? '').trim();
		const rightCol = String(clause?.rightCol ?? '').trim();
		const how = joinModes.includes((clause?.how as JoinHow) ?? 'inner')
			? ((clause?.how as JoinHow) ?? 'inner')
			: 'inner';
		if (!leftNodeId || !leftCol || !rightNodeId || !rightCol) continue;
		out.push({ leftNodeId, leftCol, rightNodeId, rightCol, how });
	}
	return out;
}

export function readJoinParams(raw: JoinEditorParams | undefined): Partial<TransformJoinParams> {
	if (!raw) return {};
	if (raw.join && typeof raw.join === 'object') return raw.join;
	return raw;
}

export function normalizeJoinParams(raw: JoinEditorParams | undefined): TransformJoinParams {
	const p = readJoinParams(raw);
	const clauses = normalizeJoinClauses(p?.clauses);
	return { clauses };
}

export type JoinNodeColumns = {
	nodeId: string;
	shortId: string;
	displayName: string;
	label: string;
	columns: string[];
};

export function resolveJoinNodeColumns(inputSchemas: InputSchemaView[]): JoinNodeColumns[] {
	const baseRows = (inputSchemas ?? [])
		.filter((s) => String(s?.sourceNodeId ?? '').trim().length > 0)
		.map((s) => ({
			nodeId: String(s.sourceNodeId ?? ''),
			shortId: shortNodeId(String(s.sourceNodeId ?? '')),
			displayName: String(s.label ?? s.sourceNodeId ?? '')
				.split('.')
				.slice(0, 1)
				.join('.')
				.trim(),
			label: String(s.label ?? s.sourceNodeId ?? ''),
			columns: Array.from(
				new Set(
					(s.columns ?? [])
						.map((c) => String(c.name ?? '').trim())
						.filter(Boolean)
				)
			).sort((a, b) => a.localeCompare(b))
		}));
	const nameCounts = new Map<string, number>();
	for (const row of baseRows) {
		const key = row.displayName || row.shortId;
		nameCounts.set(key, (nameCounts.get(key) ?? 0) + 1);
	}
	const rows = baseRows.map((row) => {
		const key = row.displayName || row.shortId;
		if ((nameCounts.get(key) ?? 0) <= 1) return row;
		return {
			...row,
			displayName: `${key} (${row.shortId})`
		};
	});
	return rows.sort((a, b) => a.displayName.localeCompare(b.displayName));
}

export function joinMismatchColumnsFromError(err: NodeExecutionError | null): string[] {
	if (!err) return [];
	const path = String(err.paramPath ?? '');
	const code = String(err.errorCode ?? '');
	if (code !== 'MISSING_COLUMN') return [];
	if (!(path === 'params.join.clauses' || path.endsWith('/clauses') || path.endsWith('.clauses'))) return [];
	return Array.from(
		new Set(
			(Array.isArray(err.missingColumns) ? err.missingColumns : [])
				.map((c) => String(c).trim())
				.filter(Boolean)
		)
	);
}

export function canCommitJoin(clauses: JoinClause[]): boolean {
	return normalizeJoinClauses(clauses).length > 0;
}

export function qualified(nodeId: string, col: string): string {
	return `${shortNodeId(nodeId)}.${String(col ?? '').trim()}`;
}
