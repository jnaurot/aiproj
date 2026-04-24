import type { InputSchemaView } from './inputSchema';

export type JoinInputRelation = {
	sourceNodeId: string;
	relationDisplayName: string;
	columns: Array<{ name: string; type: string }>;
};

function relationDisplayNameForSchema(schema: InputSchemaView): string {
	const explicit = String(schema.sourceDisplayName ?? '').trim();
	if (explicit) return explicit;
	const byLabel = String(schema.label ?? '').trim();
	if (byLabel.includes('.')) {
		return byLabel.slice(0, byLabel.indexOf('.')).trim() || byLabel;
	}
	if (byLabel) return byLabel;
	return String(schema.sourceNodeId ?? '').trim();
}

function stableSortRelations(a: JoinInputRelation, b: JoinInputRelation): number {
	const byDisplay = a.relationDisplayName.localeCompare(b.relationDisplayName);
	if (byDisplay !== 0) return byDisplay;
	return a.sourceNodeId.localeCompare(b.sourceNodeId);
}

export function buildJoinInputRelations(inputSchemas: InputSchemaView[]): JoinInputRelation[] {
	const grouped = new Map<string, JoinInputRelation>();
	for (const schema of inputSchemas ?? []) {
		const sourceNodeId = String(schema?.sourceNodeId ?? '').trim();
		if (!sourceNodeId) continue;
		const relationDisplayName = relationDisplayNameForSchema(schema);
		const key = sourceNodeId;
		if (!grouped.has(key)) {
			grouped.set(key, {
				sourceNodeId,
				relationDisplayName,
				columns: []
			});
		}
		const current = grouped.get(key)!;
		for (const col of schema.columns ?? []) {
			const name = String(col?.name ?? '').trim();
			if (!name) continue;
			const type = String(col?.type ?? 'unknown').trim() || 'unknown';
			const existing = current.columns.find((entry) => entry.name === name);
			if (!existing) {
				current.columns.push({ name, type });
				continue;
			}
			if (existing.type === 'unknown' && type !== 'unknown') {
				existing.type = type;
			}
		}
	}
	return Array.from(grouped.values())
		.map((entry) => ({
			...entry,
			columns: [...entry.columns].sort((a, b) => a.name.localeCompare(b.name))
		}))
		.sort(stableSortRelations);
}

