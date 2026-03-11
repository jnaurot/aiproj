import type { ComponentApiPort } from '$lib/flow/client/components';
import type { PayloadType } from '$lib/flow/types';

export const RESERVED_COMPONENT_OUTPUT_NAMES = new Set([
	'component',
	'meta',
	'outputs',
	'input_refs'
]);

export type ComponentOutputBinding = {
	outputRef?: string;
	artifact?: 'current' | 'last';
};

export type ComponentOutputValidation = {
	outputErrors: Record<number, string[]>;
	bindingErrors: Record<string, string[]>;
	hasErrors: boolean;
};

export type ValidateComponentOutputsOptions = {
	availableOutputRefs?: string[];
};

const PAYLOAD_TYPE_OPTIONS: PayloadType[] = ['table', 'text', 'json', 'binary', 'embeddings'];

const OUTPUT_NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

function normalizePayloadType(value: unknown): PayloadType {
	const v = String(value ?? '').trim().toLowerCase();
	if (v === 'string') return 'text';
	return PAYLOAD_TYPE_OPTIONS.includes(v as PayloadType) ? (v as PayloadType) : 'json';
}

export function deriveOutputPayloadType(
	output: ComponentApiPort,
	bindingsOutputs: Record<string, ComponentOutputBinding>,
	internalNodeMetaByRef: Record<string, { outPayloadType?: PayloadType }>
): PayloadType {
	const outName = String(output?.name ?? '').trim();
	const boundOutputRef = String(bindingsOutputs?.[outName]?.outputRef ?? '').trim();
	const inferred = internalNodeMetaByRef?.[boundOutputRef]?.outPayloadType;
	return normalizePayloadType(inferred ?? output?.typedSchema?.type ?? 'json');
}

export function applyDerivedOutputPayloadTypes(
	outputs: ComponentApiPort[],
	bindingsOutputs: Record<string, ComponentOutputBinding>,
	internalNodeMetaByRef: Record<string, { outPayloadType?: PayloadType }>
): ComponentApiPort[] {
	return outputs.map((output) => {
		const outputType = deriveOutputPayloadType(output, bindingsOutputs, internalNodeMetaByRef);
		const typedSchema = (output?.typedSchema ?? {}) as { fields?: unknown[] };
		return {
			...output,
			typedSchema: {
				type: outputType,
				fields: Array.isArray(typedSchema?.fields) ? typedSchema.fields : []
			}
		};
	});
}

export function validateComponentOutputs(
	outputs: ComponentApiPort[],
	bindingsOutputs: Record<string, ComponentOutputBinding>,
	options: ValidateComponentOutputsOptions = {}
): ComponentOutputValidation {
	const outputErrors: Record<number, string[]> = {};
	const bindingErrors: Record<string, string[]> = {};
	const seenByName = new Map<string, number>();
	const availableOutputRefs = new Set(
		(options.availableOutputRefs ?? [])
			.map((ref) => String(ref ?? '').trim())
			.filter((ref) => ref.length > 0)
	);
	for (let i = 0; i < outputs.length; i += 1) {
		const out = outputs[i] as ComponentApiPort;
		const issues: string[] = [];
		const name = String(out?.name ?? '').trim();
		const key = name.toLowerCase();
		if (!name) issues.push('Output name is required.');
		if (name.startsWith('__')) issues.push('Output names starting with "__" are reserved.');
		if (RESERVED_COMPONENT_OUTPUT_NAMES.has(key)) issues.push(`Output name "${name}" is reserved.`);
		if (name && !OUTPUT_NAME_PATTERN.test(name)) {
			issues.push('Output names must match [A-Za-z_][A-Za-z0-9_]*.');
		}
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

		if (name) {
			const binding = bindingsOutputs[name] ?? {};
			const boundOutputRef = String(binding?.outputRef ?? '').trim();
			const bindingIssues: string[] = [];
			if (!boundOutputRef) {
				bindingIssues.push('bindings.outputs.<name>.outputRef is required.');
			}
			if (boundOutputRef && availableOutputRefs.size > 0 && !availableOutputRefs.has(boundOutputRef)) {
				bindingIssues.push('bindings.outputs.<name>.outputRef must reference an exposed internal output.');
			}
			const artifactMode = String(binding?.artifact ?? 'current').trim();
			if (artifactMode !== 'current' && artifactMode !== 'last') {
				bindingIssues.push('bindings.outputs.<name>.artifact must be "current" or "last".');
			}
			if (bindingIssues.length > 0) {
				bindingErrors[name] = bindingIssues;
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
	availableOutputRefs: string[]
): { next: Record<string, ComponentOutputBinding>; changed: boolean } {
	const source = current ?? {};
	const next: Record<string, ComponentOutputBinding> = {};
	const fallbackOutputRef = String(availableOutputRefs[0] ?? '').trim();
	const existingFallbackOutputRef = Object.values(source)
		.map((value) => String((value as any)?.outputRef ?? '').trim())
		.find((value) => value.length > 0);
	for (const output of outputs) {
		const outName = String(output?.name ?? '').trim();
		if (!outName) continue;
		const existing = source[outName] ?? {};
		const outputRef =
			String(existing?.outputRef ?? '').trim() ||
			fallbackOutputRef ||
			existingFallbackOutputRef ||
			undefined;
		next[outName] = {
			outputRef,
			artifact: existing?.artifact === 'last' ? 'last' : 'current'
		};
	}
	const prevKey = JSON.stringify(source);
	const nextKey = JSON.stringify(next);
	return { next, changed: prevKey !== nextKey };
}
