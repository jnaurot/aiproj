import type { ComponentApiPort } from '$lib/flow/client/components';

export const RESERVED_COMPONENT_OUTPUT_NAMES = new Set([
	'component',
	'meta',
	'outputs',
	'input_refs'
]);

export type ComponentOutputBinding = {
	nodeId?: string;
	artifact?: 'current' | 'last';
};

export type ComponentOutputValidation = {
	outputErrors: Record<number, string[]>;
	bindingErrors: Record<string, string[]>;
	hasErrors: boolean;
};

function isFieldCollectionRequired(typeRaw: unknown): boolean {
	const t = String(typeRaw ?? '').trim().toLowerCase();
	return t === 'table' || t === 'json';
}

export function validateComponentOutputs(
	outputs: ComponentApiPort[],
	bindingsOutputs: Record<string, ComponentOutputBinding>
): ComponentOutputValidation {
	const outputErrors: Record<number, string[]> = {};
	const bindingErrors: Record<string, string[]> = {};
	const seenByName = new Map<string, number>();
	for (let i = 0; i < outputs.length; i += 1) {
		const out = outputs[i] as ComponentApiPort;
		const issues: string[] = [];
		const name = String(out?.name ?? '').trim();
		const key = name.toLowerCase();
		if (!name) issues.push('Output name is required.');
		if (name.startsWith('__')) issues.push('Output names starting with "__" are reserved.');
		if (RESERVED_COMPONENT_OUTPUT_NAMES.has(key)) issues.push(`Output name "${name}" is reserved.`);
		if (key) {
			if (seenByName.has(key)) {
				issues.push(`Output name "${name}" duplicates another output.`);
			} else {
				seenByName.set(key, i);
			}
		}

		const typedSchema = (out?.typedSchema ?? {}) as { type?: string; fields?: Array<{ name?: string; type?: string }> };
		const schemaType = String(typedSchema?.type ?? '').trim();
		if (!schemaType) issues.push('typedSchema.type is required.');
		const fields = Array.isArray(typedSchema?.fields) ? typedSchema.fields : [];
		if (isFieldCollectionRequired(schemaType) && fields.length === 0) {
			issues.push(`typedSchema.fields must include at least one field for ${schemaType}.`);
		}
		const seenFieldNames = new Set<string>();
		for (const field of fields) {
			const fieldName = String(field?.name ?? '').trim();
			const fieldType = String(field?.type ?? '').trim();
			if (!fieldName) issues.push('typedSchema.fields[].name is required.');
			if (!fieldType) issues.push(`typedSchema.fields.${fieldName || '(unnamed)'}.type is required.`);
			const fieldKey = fieldName.toLowerCase();
			if (fieldKey) {
				if (seenFieldNames.has(fieldKey)) {
					issues.push(`typedSchema.fields has duplicate "${fieldName}".`);
				} else {
					seenFieldNames.add(fieldKey);
				}
			}
		}
		if (issues.length > 0) outputErrors[i] = issues;

		if (name && Boolean(out?.required ?? true)) {
			const binding = bindingsOutputs[name] ?? {};
			if (!String(binding?.nodeId ?? '').trim()) {
				bindingErrors[name] = ['bindings.outputs.<name>.nodeId is required for required outputs.'];
			}
		}
	}
	return {
		outputErrors,
		bindingErrors,
		hasErrors: Object.keys(outputErrors).length > 0 || Object.keys(bindingErrors).length > 0
	};
}

export function syncOutputBindings(
	outputs: ComponentApiPort[],
	current: Record<string, ComponentOutputBinding>,
	internalNodeIds: string[]
): { next: Record<string, ComponentOutputBinding>; changed: boolean } {
	const source = current ?? {};
	const next: Record<string, ComponentOutputBinding> = {};
	const fallbackNodeId = String(internalNodeIds[0] ?? '').trim();
	for (const output of outputs) {
		const outName = String(output?.name ?? '').trim();
		if (!outName) continue;
		const existing = source[outName] ?? {};
		const nodeId = String(existing?.nodeId ?? '').trim() || fallbackNodeId || undefined;
		next[outName] = {
			nodeId,
			artifact: existing?.artifact === 'last' ? 'last' : 'current'
		};
	}
	const prevKey = JSON.stringify(source);
	const nextKey = JSON.stringify(next);
	return { next, changed: prevKey !== nextKey };
}
