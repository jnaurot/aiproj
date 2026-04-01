import type { Node } from '@xyflow/svelte';
import type { PipelineNodeData } from '$lib/flow/types';
import type { InputSchemaView } from './inputSchema';
import { uniqueStrings } from '$lib/flow/components/editors/shared';

export type SchemaColumn = { name: string; type?: string };

export function buildSchemaTypeMap(columns: SchemaColumn[]): Map<string, string> {
	const out = new Map<string, string>();
	for (const col of columns) {
		const name = String(col?.name ?? '').trim();
		if (!name) continue;
		const nextType = String(col?.type ?? 'unknown').trim() || 'unknown';
		const prevType = out.get(name);
		if (!prevType || prevType === 'unknown' || prevType.length === 0) {
			out.set(name, nextType);
			continue;
		}
		if (nextType !== 'unknown' && nextType.length > 0) out.set(name, nextType);
	}
	return out;
}

export function resolveExpectedInputColumns(node: Node<PipelineNodeData> | undefined): string[] {
	const schema = (node?.data as any)?.schema;
	const expectedInputSchemas =
		schema?.expectedInputSchemas && typeof schema.expectedInputSchemas === 'object'
			? (schema.expectedInputSchemas as Record<string, unknown>)
			: {};
	const out: string[] = [];
	for (const [handle, envelope] of Object.entries(expectedInputSchemas)) {
		const handleName = String(handle ?? '').trim().toLowerCase();
		if (!handleName || handleName.startsWith('param') || handleName.startsWith('control') || handleName.startsWith('ctl')) {
			continue;
		}
		const typedSchema =
			envelope && typeof envelope === 'object'
				? ((envelope as any).typedSchema as Record<string, unknown> | undefined)
				: undefined;
		const fields = Array.isArray((typedSchema as any)?.fields) ? ((typedSchema as any).fields as unknown[]) : [];
		for (const field of fields) {
			if (field && typeof field === 'object') {
				const name = String((field as Record<string, unknown>).name ?? '').trim();
				if (name) out.push(name);
				continue;
			}
			const name = String(field ?? '').trim();
			if (name) out.push(name);
		}
	}
	return uniqueStrings(out);
}

export function buildJsonFilterColumns(params: {
	selectedNode: Node<PipelineNodeData> | undefined;
	inputColumns?: string[];
	inputSchemaColumns?: SchemaColumn[];
	inputSchemas?: InputSchemaView[];
}): Array<{ name: string; type: string }> {
	const schemaTypeByName = buildSchemaTypeMap(
		(params.inputSchemaColumns?.length ?? 0) > 0
			? (params.inputSchemaColumns ?? [])
			: (params.inputSchemas ?? []).flatMap((schema) => schema.columns ?? [])
	);
	const expectedInputColumns = resolveExpectedInputColumns(params.selectedNode);
	const columnNames = uniqueStrings(
		[...(params.inputColumns ?? []), ...Array.from(schemaTypeByName.keys()), ...expectedInputColumns]
			.map((c) => String(c).trim())
			.filter(Boolean)
	).sort((a, b) => a.localeCompare(b));
	return columnNames.map((name) => ({ name, type: schemaTypeByName.get(name) ?? 'unknown' }));
}
