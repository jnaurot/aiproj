import type { SchemaFunction, SchemaPlaneColumn, SchemaPlaneOutput } from '$lib/flow/types/schemaPlane';
import { OPAQUE_SCHEMA } from '$lib/flow/schema/schemaRegistry';

function firstInput(inputs: readonly SchemaPlaneOutput[]) {
	return inputs[0];
}

function missingInput() {
	return {
		ok: false as const,
		error: {
			code: 'MISSING_REQUIRED_INPUT' as const,
			message: 'Transform node requires at least one input',
			handles: ['in']
		}
	};
}

function cloneColumns(columns: SchemaPlaneColumn[]): SchemaPlaneColumn[] {
	return columns.map((column) => ({ ...column, properties: { ...(column.properties ?? {}) } }));
}

function findColumn(input: SchemaPlaneOutput, name: string): SchemaPlaneColumn | undefined {
	return (input.columns ?? []).find((column) => String(column.name) === String(name));
}

function numericType(type: string): boolean {
	return type === 'number';
}

const passthroughOps = new Set([
	'filter',
	'json_filter',
	'sort',
	'dedupe',
	'limit',
	'null_policy',
	'outlier_policy',
	'text_clean',
	'nlp_normalize',
	'tokenize_chunk',
	'dataset_split',
	'class_imbalance',
	'numeric_scale',
	'feature_selection',
	'leakage_detect',
	'quality_profile',
	'drift_compare',
	'determinism_profile',
	'fit_state_registry',
	'pii_guard',
	'inference_parity',
	'split',
	'quality_gate',
	'ml_contract',
	'sql'
]);

export const schemaFn_transform: SchemaFunction = (inputs, params) => {
	const input = firstInput(inputs);
	if (!input) return missingInput();

	const op = String(params?.op ?? '').trim();
	if (!op) {
		return {
			ok: false,
			error: {
				code: 'TYPE_MISMATCH',
				message: 'Transform op is required for schema propagation',
				handles: ['in']
			}
		};
	}

	if (passthroughOps.has(op)) return { ok: true, output: input };

	if (op === 'select') {
		const select = ((params as any).select ?? {}) as Record<string, unknown>;
		const columns = Array.isArray(select.columns) ? (select.columns as unknown[]) : [];
		const out: SchemaPlaneColumn[] = [];
		for (const name of columns) {
			const col = findColumn(input, String(name));
			if (!col) {
				return {
					ok: false,
					error: {
						code: 'SHAPE_MISMATCH',
						message: `Column '${String(name)}' not found in input schema`,
						handles: ['in']
					}
				};
			}
			out.push({ ...col, properties: { ...(col.properties ?? {}) } });
		}
		return { ok: true, output: { ...input, columns: out } };
	}

	if (op === 'rename') {
		const map = ((params as any).rename?.map ?? {}) as Record<string, string>;
		const renamed = cloneColumns(input.columns ?? []).map((column) => ({
			...column,
			name: map[column.name] ?? column.name
		}));
		return { ok: true, output: { ...input, columns: renamed } };
	}

	if (op === 'derive') {
		const derive = ((params as any).derive ?? {}) as Record<string, unknown>;
		const rules = Array.isArray(derive.rules) ? derive.rules : [];
		const columns = cloneColumns(input.columns ?? []);
		for (const rule of rules as any[]) {
			const name = String(rule?.name ?? '').trim();
			if (!name) continue;
			columns.push({
				name,
				type: 'number',
				nullable: true,
				properties: {}
			});
		}
		return { ok: true, output: { ...input, columns } };
	}

	if (op === 'aggregate') {
		const aggregate = ((params as any).aggregate ?? {}) as Record<string, unknown>;
		const groupBy = Array.isArray(aggregate.groupBy) ? aggregate.groupBy : [];
		const metrics = Array.isArray(aggregate.metrics) ? aggregate.metrics : [];
		const columns: SchemaPlaneColumn[] = [];
		for (const groupCol of groupBy as string[]) {
			const found = findColumn(input, groupCol);
			if (!found) {
				return {
					ok: false,
					error: { code: 'SHAPE_MISMATCH', message: `groupBy column '${groupCol}' not found`, handles: ['in'] }
				};
			}
			columns.push({ ...found, properties: { ...(found.properties ?? {}) } });
		}
		for (const metric of metrics as any[]) {
			const name = String(metric?.name ?? '').trim();
			if (!name) continue;
			const opName = String(metric?.op ?? '');
			const colName = String(metric?.column ?? '').trim();
			if (['sum', 'mean', 'min', 'max'].includes(opName)) {
				const found = findColumn(input, colName);
				if (!found || !numericType(found.type)) {
					return {
						ok: false,
						error: {
							code: 'TYPE_MISMATCH',
							message: `Aggregate '${opName}' requires numeric column '${colName}'`,
							handles: ['in']
						}
					};
				}
			}
			columns.push({
				name,
				type: 'number',
				nullable: true,
				properties: {}
			});
		}
		return { ok: true, output: { mode: 'table', columns } };
	}

	if (op === 'join') {
		const right = inputs[1];
		if (!right) {
			return {
				ok: false,
				error: {
					code: 'MISSING_REQUIRED_INPUT',
					message: 'Join requires two inputs',
					handles: ['left', 'right']
				}
			};
		}
		const join = ((params as any).join ?? {}) as Record<string, unknown>;
		const clauses = Array.isArray(join.clauses) ? join.clauses : [];
		for (const clause of clauses as any[]) {
			const leftCol = findColumn(input, String(clause?.leftCol ?? ''));
			const rightCol = findColumn(right, String(clause?.rightCol ?? ''));
			if (!leftCol || !rightCol) {
				return {
					ok: false,
					error: { code: 'SHAPE_MISMATCH', message: 'Join key columns must exist on both inputs', handles: ['left', 'right'] }
				};
			}
			if (leftCol.type !== rightCol.type) {
				return {
					ok: false,
					error: { code: 'TYPE_MISMATCH', message: 'Join key column types must match', handles: ['left', 'right'] }
				};
			}
		}
		const rightPrefix = String((join as any).right_prefix ?? '').trim();
		const merged = cloneColumns(input.columns ?? []);
		for (const column of right.columns ?? []) {
			merged.push({
				...column,
				name: rightPrefix ? `${rightPrefix}${column.name}` : column.name,
				properties: { ...(column.properties ?? {}) }
			});
		}
		return { ok: true, output: { mode: 'table', columns: merged } };
	}

	if (op === 'embedding') {
		const embedding = ((params as any).embedding ?? {}) as Record<string, unknown>;
		const dims = Number(embedding.dimensions ?? 0);
		const outputColumn = String(embedding.outputColumn ?? 'embedding').trim() || 'embedding';
		const textCols = new Set<string>(Array.isArray(embedding.columns) ? (embedding.columns as string[]) : []);
		const remaining = (input.columns ?? []).filter((column) => !textCols.has(column.name));
		return {
			ok: true,
			output: {
				mode: 'table',
				columns: [
					...cloneColumns(remaining),
					{
						name: outputColumn,
						type: 'tensor',
						nullable: false,
						properties: { dtype: 'float32' }
					}
				],
				shape: dims > 0 ? [dims] : undefined
			}
		};
	}

	if (op === 'pivot' || op === 'json_to_table' || op === 'text_to_table' || op === 'table_to_json') {
		return { ok: true, output: OPAQUE_SCHEMA };
	}

	return { ok: true, output: OPAQUE_SCHEMA };
};

