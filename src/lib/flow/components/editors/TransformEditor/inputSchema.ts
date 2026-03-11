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
	upstream?: { nodeId?: string; sourceHandle?: string } | Array<{ nodeId?: string; sourceHandle?: string }>;
};

export type InputSchemaEnvelope = {
	contract: string;
	version?: number;
	source?: string;
	state?: string;
	table?: {
		columns?: InputSchemaColumn[];
		coercion?: { mode?: string; lossy?: boolean; notes?: string };
	};
	stats?: { rowCount?: number };
	provenance?: InputSchemaProvenance;
};

export type InputSchemaView = {
	artifactId: string;
	label: string;
	sourceNodeId?: string;
	inputHandle?: string;
	columns: InputSchemaColumn[];
	rowCount: number | null;
	provenance: InputSchemaProvenance | null;
	coercion: { mode: string; lossy: boolean; notes?: string } | null;
	schemaSource: string;
	schemaState: string;
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
	payloadSchema: unknown,
	ctx?: { sourceNodeId?: string; inputHandle?: string }
): InputSchemaView {
	const ps = (payloadSchema ?? {}) as Record<string, unknown>;
	const looksLikeEnvelope =
		typeof ps.contract === 'string' &&
		(ps.table !== undefined || ps.stats !== undefined || ps.provenance !== undefined);
	const schema = (looksLikeEnvelope
		? (ps as InputSchemaEnvelope)
		: ((ps.schema ?? (ps.artifactMetadataV1 as any)?.schema ?? null) as InputSchemaEnvelope | null));
	const schemaCols = normalizeColumns(schema?.table?.columns);
	const fallbackCols = normalizeColumns(ps.columns);
	const payloadType = String(ps.type ?? '').toLowerCase();
	const textLikeCols =
		schemaCols.length === 0 && fallbackCols.length === 0 && (payloadType === 'text' || payloadType === 'string')
			? [{ name: 'text', type: 'string' }]
			: [];
	const rowCountRaw = (schema?.stats?.rowCount ?? ps.row_count ?? null) as number | null;
	return {
		artifactId,
		label,
		sourceNodeId: ctx?.sourceNodeId,
		inputHandle: ctx?.inputHandle,
		columns: schemaCols.length > 0 ? schemaCols : (fallbackCols.length > 0 ? fallbackCols : textLikeCols),
		rowCount: typeof rowCountRaw === 'number' ? rowCountRaw : null,
		provenance: schema?.provenance ?? null,
		coercion:
			schema?.table?.coercion && typeof schema.table.coercion === 'object'
				? {
						mode: String(schema.table.coercion.mode ?? 'native'),
						lossy: Boolean(schema.table.coercion.lossy),
						...(schema.table.coercion.notes
							? { notes: String(schema.table.coercion.notes) }
							: {})
					}
				: null,
		schemaSource: String(schema?.source ?? (ps.source as string | undefined) ?? 'unknown'),
		schemaState: String(schema?.state ?? (ps.state as string | undefined) ?? 'unknown')
	};
}
