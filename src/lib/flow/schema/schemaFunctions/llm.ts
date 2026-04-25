import type { SchemaFunction, SchemaPlaneColumn, SchemaPlaneColumnType, SchemaPlaneOutput } from '$lib/flow/types/schemaPlane';

const OPAQUE_OUTPUT: SchemaPlaneOutput = {
	mode: 'opaque',
	columns: []
};

function toRecord(value: unknown): Record<string, unknown> | null {
	return value && typeof value === 'object' && !Array.isArray(value) ? (value as Record<string, unknown>) : null;
}

function normalizeJsonType(raw: unknown): string {
	if (typeof raw === 'string') return raw.trim().toLowerCase();
	if (Array.isArray(raw)) {
		for (const item of raw) {
			const normalized = typeof item === 'string' ? item.trim().toLowerCase() : '';
			if (normalized && normalized !== 'null') return normalized;
		}
	}
	return '';
}

function mapJsonTypeToSchemaPlaneType(raw: unknown): SchemaPlaneColumnType {
	const normalized = normalizeJsonType(raw);
	if (normalized === 'string') return 'string';
	if (normalized === 'number' || normalized === 'integer') return 'number';
	if (normalized === 'boolean') return 'boolean';
	if (normalized === 'array' || normalized === 'object') return 'unknown';
	return 'unknown';
}

function additionalPropertiesFlag(root: Record<string, unknown>): boolean {
	const raw = root.additionalProperties;
	if (typeof raw === 'boolean') return raw;
	if (raw && typeof raw === 'object') return true;
	return true;
}

export const schemaFn_llm: SchemaFunction = (_inputs, params) => {
	const output = toRecord((params as any)?.output) ?? {};
	const outputMode = String(output.mode ?? 'text').trim().toLowerCase();
	if (outputMode !== 'json') return { ok: true, output: OPAQUE_OUTPUT };
	const root = toRecord(output.jsonSchema) ?? {};
	const props = toRecord(root.properties) ?? {};
	const required = Array.isArray(root.required)
		? new Set(
				root.required
					.map((name) => String(name ?? '').trim())
					.filter(Boolean)
			)
		: new Set<string>();
	const columns: SchemaPlaneColumn[] = [];
	for (const [name, rawDef] of Object.entries(props)) {
		const trimmed = String(name ?? '').trim();
		if (!trimmed) continue;
		const def = toRecord(rawDef) ?? {};
		columns.push({
			name: trimmed,
			type: mapJsonTypeToSchemaPlaneType(def.type),
			nullable: !required.has(trimmed),
			properties: {}
		});
	}
	return {
		ok: true,
		output: {
			mode: 'table',
			columns,
			properties: {
				additional_properties: additionalPropertiesFlag(root),
				source: 'llm_json_schema'
			}
		}
	};
};
