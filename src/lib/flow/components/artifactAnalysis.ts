export type AnalysisArtifactField = {
	name: string;
	type: string;
	nullable: boolean;
};

export type AnalysisArtifact = {
	name: string;
	kind: string;
	description: string;
	rowCount: number;
	fields: AnalysisArtifactField[];
};

function isObject(value: unknown): value is Record<string, unknown> {
	return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

export function extractAnalysisArtifacts(payload: unknown): AnalysisArtifact[] {
	if (!isObject(payload)) return [];
	const raw = payload.analysis_artifacts;
	if (!Array.isArray(raw)) return [];
	const out: AnalysisArtifact[] = [];
	for (const item of raw) {
		if (!isObject(item)) continue;
		const name = String(item.name ?? '').trim();
		if (!name) continue;
		const kind = String(item.kind ?? 'unknown').trim() || 'unknown';
		const description = String(item.description ?? '').trim();
		const rowCountRaw = item.row_count;
		const rowCount = Number.isFinite(Number(rowCountRaw)) ? Math.max(0, Number(rowCountRaw)) : 0;
		const typedSchema = isObject(item.typed_schema) ? item.typed_schema : {};
		const fieldsRaw = typedSchema.fields;
		const fields: AnalysisArtifactField[] = Array.isArray(fieldsRaw)
			? fieldsRaw
					.filter((f) => isObject(f) && String(f.name ?? '').trim().length > 0)
					.map((f) => ({
						name: String(f.name ?? '').trim(),
						type: String(f.type ?? 'unknown').trim() || 'unknown',
						nullable: Boolean(f.nullable),
					}))
			: [];
		out.push({ name, kind, description, rowCount, fields });
	}
	return out;
}
