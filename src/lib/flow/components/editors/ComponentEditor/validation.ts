import type { ComponentApiPort } from '$lib/flow/client/components';
import type { PortType } from '$lib/flow/types';

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

export type ValidateComponentOutputsOptions = {
	internalNodeIds?: string[];
};

const PORT_TYPE_OPTIONS: PortType[] = ['table', 'text', 'json', 'binary', 'embeddings'];

const OUTPUT_NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

function normalizePortType(value: unknown): PortType {
	const v = String(value ?? '').trim().toLowerCase();
	return PORT_TYPE_OPTIONS.includes(v as PortType) ? (v as PortType) : 'json';
}

export function deriveOutputPortType(
	output: ComponentApiPort,
	bindingsOutputs: Record<string, ComponentOutputBinding>,
	internalNodeMetaById: Record<string, { outPortType?: PortType }>
): PortType {
	const outName = String(output?.name ?? '').trim();
	const boundNodeId = String(bindingsOutputs?.[outName]?.nodeId ?? '').trim();
	const inferred = internalNodeMetaById?.[boundNodeId]?.outPortType;
	return normalizePortType(inferred ?? output?.portType ?? 'json');
}

export function applyDerivedOutputPortTypes(
	outputs: ComponentApiPort[],
	bindingsOutputs: Record<string, ComponentOutputBinding>,
	internalNodeMetaById: Record<string, { outPortType?: PortType }>
): ComponentApiPort[] {
	return outputs.map((output) => {
		const portType = deriveOutputPortType(output, bindingsOutputs, internalNodeMetaById);
		const typedSchema = (output?.typedSchema ?? {}) as { fields?: unknown[] };
		return {
			...output,
			portType,
			typedSchema: {
				type: portType,
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
	const internalNodeIds = new Set(
		(options.internalNodeIds ?? [])
			.map((id) => String(id ?? '').trim())
			.filter((id) => id.length > 0)
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
		if (schemaType && String(out?.portType ?? '').trim() && schemaType !== String(out?.portType ?? '').trim()) {
			issues.push('typedSchema.type must match portType.');
		}
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
			const boundNodeId = String(binding?.nodeId ?? '').trim();
			const bindingIssues: string[] = [];
			if (!boundNodeId) {
				bindingIssues.push('bindings.outputs.<name>.nodeId is required.');
			}
			if (boundNodeId && internalNodeIds.size > 0 && !internalNodeIds.has(boundNodeId)) {
				bindingIssues.push('bindings.outputs.<name>.nodeId must reference an internal node in the selected revision.');
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
	internalNodeIds: string[]
): { next: Record<string, ComponentOutputBinding>; changed: boolean } {
	const source = current ?? {};
	const next: Record<string, ComponentOutputBinding> = {};
	const fallbackNodeId = String(internalNodeIds[0] ?? '').trim();
	const existingFallbackNodeId = Object.values(source)
		.map((value) => String((value as any)?.nodeId ?? '').trim())
		.find((value) => value.length > 0);
	for (const output of outputs) {
		const outName = String(output?.name ?? '').trim();
		if (!outName) continue;
		const existing = source[outName] ?? {};
		const nodeId =
			String(existing?.nodeId ?? '').trim() ||
			fallbackNodeId ||
			existingFallbackNodeId ||
			undefined;
		next[outName] = {
			nodeId,
			artifact: existing?.artifact === 'last' ? 'last' : 'current'
		};
	}
	const prevKey = JSON.stringify(source);
	const nextKey = JSON.stringify(next);
	return { next, changed: prevKey !== nextKey };
}
