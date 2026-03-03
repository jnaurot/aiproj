export type InputSchemaColumn = {
	name: string;
	type: string;
};

export type InputSchemaProvenance = {
	sourceKind?: string;
	dbName?: string;
	dbSchema?: string;
	tableName?: string;
	query?: string;
	endpoint?: string;
	upstream?: { nodeId?: string; port?: string } | Array<{ nodeId?: string; port?: string }>;
};

export type InputSchemaEnvelope = {
	contract: string;
	version?: number;
	table?: { columns?: InputSchemaColumn[] };
	stats?: { rowCount?: number };
	provenance?: InputSchemaProvenance;
};

export type InputSchemaView = {
	artifactId: string;
	label: string;
	columns: InputSchemaColumn[];
	rowCount: number | null;
	provenance: InputSchemaProvenance | null;
};

function normalizeColumns(columns: unknown): InputSchemaColumn[] {
	if (!Array.isArray(columns)) return [];
	const out: InputSchemaColumn[] = [];
	for (const col of columns) {
		if (col && typeof col === 'object') {
			const c = col as Record<string, unknown>;
			const name = String(c.name ?? '').trim();
			if (!name) continue;
			const type = String(c.type ?? c.dtype ?? 'unknown').trim() || 'unknown';
			out.push({ name, type });
			continue;
		}
		const name = String(col ?? '').trim();
		if (!name) continue;
		out.push({ name, type: 'unknown' });
	}
	return out;
}

export function parseInputSchemaView(
	artifactId: string,
	label: string,
	payloadSchema: unknown
): InputSchemaView {
	const ps = (payloadSchema ?? {}) as Record<string, unknown>;
	const schema = (ps.schema ?? null) as InputSchemaEnvelope | null;
	const schemaCols = normalizeColumns(schema?.table?.columns);
	const fallbackCols = normalizeColumns(ps.columns);
	const rowCountRaw = (schema?.stats?.rowCount ?? ps.row_count ?? null) as number | null;
	return {
		artifactId,
		label,
		columns: schemaCols.length > 0 ? schemaCols : fallbackCols,
		rowCount: typeof rowCountRaw === 'number' ? rowCountRaw : null,
		provenance: schema?.provenance ?? null
	};
}
