import type { SchemaFunction, SchemaPlaneColumn, SchemaPlaneOutput } from '$lib/flow/types/schemaPlane';
import { OPAQUE_SCHEMA } from '$lib/flow/schema/schemaRegistry';

function normalizeColumnType(input: unknown): SchemaPlaneColumn['type'] {
	const raw = String(input ?? '').trim().toLowerCase();
	if (raw === 'string' || raw === 'text') return 'string';
	if (raw === 'number' || raw === 'float' || raw === 'integer' || raw === 'int') return 'number';
	if (raw === 'boolean' || raw === 'bool') return 'boolean';
	if (raw === 'datetime' || raw === 'timestamp' || raw === 'date') return 'datetime';
	if (raw === 'binary') return 'binary';
	if (raw === 'tensor') return 'tensor';
	return 'unknown';
}

function columnsFromPriming(params: Record<string, unknown>): SchemaPlaneColumn[] {
	const schema =
		((params?.priming as any)?.sample_schema as any) ??
		((params?.priming as any)?.sampleSchema as any) ??
		((params?.sample_schema as any) ?? null);
	const fields = Array.isArray(schema?.fields) ? schema.fields : [];
	return fields
		.map((field: any) => ({
			name: String(field?.name ?? '').trim(),
			type: normalizeColumnType(field?.type),
			nullable: Boolean(field?.nullable ?? true),
			properties: {}
		}))
		.filter((column: SchemaPlaneColumn) => column.name.length > 0);
}

function columnsFromDeclaredSchema(params: Record<string, unknown>): SchemaPlaneColumn[] {
	const fields = Array.isArray((params?.declared_schema as any)?.fields) ? (params.declared_schema as any).fields : [];
	return fields
		.map((field: any) => ({
			name: String(field?.name ?? '').trim(),
			type: normalizeColumnType(field?.type),
			nullable: Boolean(field?.nullable ?? true),
			properties: {}
		}))
		.filter((column: SchemaPlaneColumn) => column.name.length > 0);
}

function withSourceProperties(base: SchemaPlaneOutput, params: Record<string, unknown>): SchemaPlaneOutput {
	const sourceKind = String((params?.sourceKind as string) ?? '').trim().toLowerCase();
	if (sourceKind === 'stream') {
		return {
			...base,
			properties: {
				...(base.properties ?? {}),
				cardinality: 'stream',
				consume_once: true
			}
		};
	}
	if ((params as any)?.memoizable === false) {
		return {
			...base,
			properties: {
				...(base.properties ?? {}),
				cardinality: 'one',
				consume_once: true
			}
		};
	}
	return base;
}

export const schemaFn_source: SchemaFunction = (inputs, params) => {
	if (inputs.length > 0) {
		return {
			ok: false,
			error: {
				code: 'MISSING_REQUIRED_INPUT',
				message: 'Source nodes cannot have upstream inputs',
				handles: ['in']
			}
		};
	}
	const sourceKind = String((params?.sourceKind as string) ?? '').trim().toLowerCase();
	if (sourceKind === 'db' || sourceKind === 'database') {
		const declaredColumns = columnsFromDeclaredSchema(params);
		if (declaredColumns.length > 0) {
			return {
				ok: true,
				output: withSourceProperties({ mode: 'table', columns: declaredColumns }, params)
			};
		}
		return {
			ok: true,
			output: withSourceProperties({ ...OPAQUE_SCHEMA, note: 'No declared schema on database source.' }, params)
		};
	}
	const primingColumns = columnsFromPriming(params);
	if (primingColumns.length > 0) {
		return {
			ok: true,
			output: withSourceProperties({ mode: 'table', columns: primingColumns }, params)
		};
	}
	return {
		ok: true,
		output: withSourceProperties({
			...OPAQUE_SCHEMA,
			note: 'Run source node to infer schema.'
		}, params)
	};
};
