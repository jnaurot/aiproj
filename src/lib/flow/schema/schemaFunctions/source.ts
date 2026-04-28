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

function columnsFromArtifact(params: Record<string, unknown>): SchemaPlaneColumn[] {
	const schema =
		((params?.introspected_schema as any) ?? null) ??
		(((params as any)?.db as any)?.introspected_schema ?? null) ??
		(((params as any)?.db as any)?.schema ?? null);
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

function columnsFromDeclaredJsonSchema(params: Record<string, unknown>): SchemaPlaneColumn[] {
	const schema =
		((params?.declared_json_schema as any) ?? null) ??
		((params?.declaredJsonSchema as any) ?? null);
	const properties =
		schema && typeof schema === 'object' && (schema as any).properties && typeof (schema as any).properties === 'object'
			? ((schema as any).properties as Record<string, any>)
			: {};
	const required = Array.isArray((schema as any)?.required)
		? new Set(
				((schema as any).required as unknown[])
					.map((value) => String(value ?? '').trim())
					.filter((value) => value.length > 0)
			)
		: new Set<string>();
	const columns: SchemaPlaneColumn[] = [];
	for (const [nameRaw, propertySchema] of Object.entries(properties)) {
		const name = String(nameRaw ?? '').trim();
		if (!name) continue;
		let type: SchemaPlaneColumn['type'] = 'unknown';
		const propertyTypeRaw = String((propertySchema as any)?.type ?? '').trim().toLowerCase();
		if (propertyTypeRaw === 'string') type = 'string';
		else if (propertyTypeRaw === 'number' || propertyTypeRaw === 'integer') type = 'number';
		else if (propertyTypeRaw === 'boolean') type = 'boolean';
		else if (propertyTypeRaw === 'array' || propertyTypeRaw === 'object') type = 'unknown';
		columns.push({
			name,
			type,
			nullable: !required.has(name),
			properties: {}
		});
	}
	return columns;
}

function withSourceProperties(
	base: SchemaPlaneOutput,
	params: Record<string, unknown>,
	sourceProvenance: 'declared' | 'artifact' | 'sample' | 'opaque'
): SchemaPlaneOutput {
	const sourceKind = String((params?.sourceKind as string) ?? '').trim().toLowerCase();
	const common = {
		...(base.properties ?? {}),
		sourceProvenance
	};
	if (sourceKind === 'stream') {
		return {
			...base,
			properties: {
				...common,
				cardinality: 'stream',
				consume_once: true
			}
		};
	}
	if ((params as any)?.memoizable === false) {
		return {
			...base,
			properties: {
				...common,
				cardinality: 'one',
				consume_once: true
			}
		};
	}
	return {
		...base,
		properties: common
	};
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
	const declaredColumns = (() => {
		const typedFields = columnsFromDeclaredSchema(params);
		if (typedFields.length > 0) return typedFields;
		return columnsFromDeclaredJsonSchema(params);
	})();
	if (declaredColumns.length > 0) {
		return {
			ok: true,
			output: withSourceProperties({ mode: 'table', columns: declaredColumns }, params, 'declared')
		};
	}
	const artifactColumns = columnsFromArtifact(params);
	if (artifactColumns.length > 0) {
		return {
			ok: true,
			output: withSourceProperties({ mode: 'table', columns: artifactColumns }, params, 'artifact')
		};
	}
	const primingColumns = columnsFromPriming(params);
	if (primingColumns.length > 0) {
		return {
			ok: true,
			output: withSourceProperties({ mode: 'table', columns: primingColumns }, params, 'sample')
		};
	}
	return {
		ok: true,
		output: withSourceProperties({
			...OPAQUE_SCHEMA,
			note: 'Run source node to infer schema.'
		}, params, 'opaque')
	};
};
