export type DownstreamSchemaSuggestion = {
	summary: string;
	targetSchema: Record<string, unknown>;
};

function asRecord(value: unknown): Record<string, unknown> {
	return value && typeof value === 'object' ? (value as Record<string, unknown>) : {};
}

function extractColumns(schema: unknown): Array<{ name: string; type: string; nullable: boolean }> {
	const rec = asRecord(schema);
	const cols = Array.isArray(rec.columns) ? rec.columns : [];
	const out: Array<{ name: string; type: string; nullable: boolean }> = [];
	for (const col of cols) {
		const c = asRecord(col);
		const name = String(c.name ?? '').trim();
		if (!name) continue;
		const type = String(c.type ?? 'string').trim() || 'string';
		const nullable = typeof c.nullable === 'boolean' ? Boolean(c.nullable) : true;
		out.push({ name, type, nullable });
	}
	return out;
}

function extractFields(schema: unknown): Array<{ name: string; type: string; nullable: boolean }> {
	const rec = asRecord(schema);
	const fields = Array.isArray(rec.fields) ? rec.fields : [];
	const out: Array<{ name: string; type: string; nullable: boolean }> = [];
	for (const field of fields) {
		const f = asRecord(field);
		const name = String(f.name ?? '').trim();
		if (!name) continue;
		const type = String(f.type ?? 'string').trim() || 'string';
		const nullable = typeof f.nullable === 'boolean' ? Boolean(f.nullable) : true;
		out.push({ name, type, nullable });
	}
	return out;
}

function findMissingColumnFromMessage(message: string): string | null {
	const match = message.match(/Column ['"`]?([^'"`]+)['"`]? not found in input schema/i);
	const name = String(match?.[1] ?? '').trim();
	return name || null;
}

export function buildDownstreamSchemaSuggestion(input: {
	mismatchMessage: string | null | undefined;
	sourceSchema: unknown;
	targetSchema: unknown;
	targetLabel: string;
}): DownstreamSchemaSuggestion | null {
	const message = String(input.mismatchMessage ?? '').trim();
	if (!message) return null;

	const missingColumn = findMissingColumnFromMessage(message);
	if (!missingColumn) return null;

	const sourceColumns = extractColumns(input.sourceSchema);
	const targetFields = extractFields(input.targetSchema);

	const baseFields = targetFields.length > 0
		? [...targetFields]
		: sourceColumns.map((col) => ({ name: col.name, type: col.type, nullable: col.nullable }));

	const existing = new Set(baseFields.map((f) => f.name));
	if (!existing.has(missingColumn)) {
		const sourceMatch = sourceColumns.find((col) => col.name === missingColumn);
		baseFields.push({
			name: missingColumn,
			type: String(sourceMatch?.type ?? 'string').trim() || 'string',
			nullable: sourceMatch?.nullable ?? true
		});
	}

	return {
		summary: `Suggested ${String(input.targetLabel ?? 'downstream node')} input schema includes '${missingColumn}' and uses valid typed-schema shape.`,
		targetSchema: {
			type: 'table',
			fields: baseFields
		}
	};
}
