import type { TransformKind } from '$lib/flow/schema/transform';
import type { InputSchemaView } from './inputSchema';

export type InputSchemaColumn = {
	name: string;
	type: string;
};

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

export function toTransformInputColumns(inputSchemas: InputSchemaView[]): string[] {
	return uniqueStrings(
		(inputSchemas ?? []).flatMap((schema) =>
			(schema?.columns ?? []).map((c) => String(c?.name ?? ''))
		)
	);
}

export function toTransformInputSchemaColumns(inputSchemas: InputSchemaView[]): InputSchemaColumn[] {
	const merged = new Map<string, string>();
	for (const schema of inputSchemas ?? []) {
		for (const col of schema?.columns ?? []) {
			const name = String(col?.name ?? '').trim();
			if (!name) continue;
			const nextType = String(col?.type ?? 'unknown').trim() || 'unknown';
			const prevType = merged.get(name);
			// Prefer first non-unknown type when duplicate names appear across inputs.
			if (!prevType || prevType === 'unknown') {
				merged.set(name, nextType);
			}
		}
	}
	return Array.from(merged.entries())
		.map(([name, type]) => ({ name, type }))
		.sort((a, b) => a.name.localeCompare(b.name));
}

export function buildTransformSchemaProps(transformKind: TransformKind, inputSchemas: InputSchemaView[]) {
	const inputColumns = toTransformInputColumns(inputSchemas);
	const inputSchemaColumns = toTransformInputSchemaColumns(inputSchemas);
	if (transformKind === 'join') {
		return { inputColumns, inputSchemaColumns, inputSchemas };
	}
	return {
		inputColumns,
		inputSchemaColumns,
		inputSchemas
	};
}
