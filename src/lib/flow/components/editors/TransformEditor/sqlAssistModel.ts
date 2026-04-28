import type { InputSchemaView } from './inputSchema';

function uniqueStrings(values: string[]): string[] {
	const seen = new Set<string>();
	const out: string[] = [];
	for (const raw of values) {
		const value = String(raw ?? '').trim();
		if (!value || seen.has(value)) continue;
		seen.add(value);
		out.push(value);
	}
	return out;
}

export function sqlAvailableColumns(inputColumns: string[], inputSchemas: InputSchemaView[]): string[] {
	const colsFromSchemas = (inputSchemas ?? []).flatMap((schema) =>
		(schema?.columns ?? []).map((column) => String(column?.name ?? '').trim())
	);
	return uniqueStrings([...(inputColumns ?? []), ...colsFromSchemas]).sort((a, b) => a.localeCompare(b));
}

export function insertQuotedColumnReference(query: string, columnName: string): string {
	const col = String(columnName ?? '').trim();
	if (!col) return String(query ?? '');
	const token = `"${col.replaceAll('"', '""')}"`;
	const current = String(query ?? '');
	return current.trim().length > 0 ? `${current} ${token}` : token;
}

export function extractQuotedIdentifiers(sql: string): string[] {
	const source = String(sql ?? '');
	const matches = source.matchAll(/"([^"]+)"|`([^`]+)`/g);
	const out: string[] = [];
	for (const match of matches) {
		const token = String(match[1] ?? match[2] ?? '').trim();
		if (token) out.push(token);
	}
	return uniqueStrings(out);
}

export function unknownSqlReferences(sql: string, availableColumns: string[]): string[] {
	const known = new Set((availableColumns ?? []).map((column) => String(column ?? '').trim()).filter(Boolean));
	return extractQuotedIdentifiers(sql).filter((column) => !known.has(column));
}

function stateRank(state: string): number {
	const normalized = String(state ?? 'unknown').trim().toLowerCase();
	if (normalized === 'stale') return 3;
	if (normalized === 'partial') return 2;
	if (normalized === 'fresh') return 1;
	return 0;
}

export function summarizeSchemaAssist(inputSchemas: InputSchemaView[]): {
	source: string;
	state: string;
	hasSchema: boolean;
} {
	const schemas = Array.isArray(inputSchemas) ? inputSchemas : [];
	if (schemas.length === 0) return { source: 'unknown', state: 'unknown', hasSchema: false };
	let topState = 'unknown';
	let source = 'unknown';
	for (const schema of schemas) {
		const candidateState = String(schema?.schemaState ?? 'unknown').trim().toLowerCase();
		if (stateRank(candidateState) > stateRank(topState)) topState = candidateState;
		if (source === 'unknown') {
			const candidateSource = String(schema?.schemaSource ?? 'unknown').trim().toLowerCase();
			if (candidateSource && candidateSource !== 'unknown') source = candidateSource;
		}
	}
	return { source, state: topState, hasSchema: true };
}

