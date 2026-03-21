import type { InputSchemaView } from '$lib/flow/components/editors/TransformEditor/inputSchema';

export type ModelPreviewDiff = {
	inputType: string;
	outputType: string;
	inputColumns: string[];
	outputColumns: string[];
	sampleInput: Record<string, unknown> | string | null;
	sampleOutput: Record<string, unknown> | string | null;
	notes: string[];
};

function schemaType(view: InputSchemaView | undefined): string {
	const kind = String(view?.schema?.kind ?? '').trim().toLowerCase();
	if (!kind) return 'unknown';
	if (kind === 'string') return 'text';
	return kind;
}

function inferInputColumns(view: InputSchemaView | undefined): string[] {
	if (!view || view.schema?.kind !== 'table') return [];
	const fields = Array.isArray(view.schema.required_fields) ? view.schema.required_fields : [];
	return fields.map((f) => String(f.name ?? '').trim()).filter((name) => name.length > 0);
}

function inferOutputColumns(params: Record<string, unknown>): string[] {
	const output = (params.output && typeof params.output === 'object' ? params.output : {}) as Record<string, unknown>;
	const mode = String(output.mode ?? 'text').trim().toLowerCase();
	if (mode !== 'json') return [];
	const jsonSchema = output.jsonSchema && typeof output.jsonSchema === 'object' ? (output.jsonSchema as Record<string, unknown>) : {};
	const props =
		jsonSchema.properties && typeof jsonSchema.properties === 'object'
			? (jsonSchema.properties as Record<string, unknown>)
			: {};
	return Object.keys(props);
}

export function buildModelPreviewDiff(input: {
	params: Record<string, unknown>;
	inputSchemas: InputSchemaView[];
	sampleRows: Array<Record<string, unknown>>;
}): ModelPreviewDiff {
	const primarySchema = input.inputSchemas?.[0];
	const inputType = schemaType(primarySchema);
	const inputColumns = inferInputColumns(primarySchema);
	const output = (input.params.output && typeof input.params.output === 'object'
		? input.params.output
		: {}) as Record<string, unknown>;
	const outputType = String(output.mode ?? 'text').trim().toLowerCase() || 'text';
	const outputColumns = inferOutputColumns(input.params);

	let sampleInput: Record<string, unknown> | string | null = null;
	if (inputType === 'table') {
		sampleInput = input.sampleRows?.[0] ?? null;
	} else if (inputType === 'text') {
		const row = input.sampleRows?.[0] ?? null;
		sampleInput = row ? String(Object.values(row)[0] ?? '') : null;
	}

	let sampleOutput: Record<string, unknown> | string | null = null;
	if (outputType === 'text') sampleOutput = 'text response';
	if (outputType === 'json') {
		sampleOutput = Object.fromEntries(outputColumns.map((k) => [k, `<${k}>`]));
	}
	if (outputType === 'embeddings') {
		const embedding = output.embedding && typeof output.embedding === 'object' ? (output.embedding as Record<string, unknown>) : {};
		const dims = Number(embedding.dims ?? 3);
		sampleOutput = { vector: Array.from({ length: Math.min(Math.max(dims, 1), 6) }, () => 0.0) };
	}

	const notes: string[] = [];
	if (!sampleInput) notes.push('No upstream sample available yet. Run upstream for richer preview.');
	if (outputType === 'json' && outputColumns.length === 0) notes.push('JSON output mode is set without explicit schema properties.');

	return {
		inputType,
		outputType,
		inputColumns,
		outputColumns,
		sampleInput,
		sampleOutput,
		notes
	};
}
