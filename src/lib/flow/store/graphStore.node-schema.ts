// src/lib/flow/store/graphStore.node-schema.ts
//
// Pure functions for node schema canonicalisation, edge schema constraints/diagnostics,
// port affinity utilities, and IO-derivation helpers.
// No Svelte store dependency — safe to import from any downstream module.

import type { Node, Edge } from '@xyflow/svelte';
import type {
	NodeStatus,
	NodeKind,
	PipelineNodeData,
	PipelineEdgeData,
	PayloadType
} from '$lib/flow/types';
import { isPayloadType } from '$lib/flow/types/base';
import { evaluateSchemaCoercion } from '$lib/flow/schema/coercionPolicy';
import {
	NodeSchemaEnvelopeSchema,
	NodeSchemaObservationSchema,
	type NodeSchemaObservation
} from '$lib/flow/schema/schemaContract';
import type { KnownRunEvent } from '$lib/flow/types/run';
import type { ComponentApiContract } from '$lib/flow/client/components';
import type {
	GraphState,
	AdapterTransformKind,
	SchemaCompatibility,
	EdgeCheck,
	EdgeSchemaConstraint,
	EdgeSchemaDiagnostic,
	NodeSchemaContractEdge,
	NodeSchemaContractSnapshot,
} from './graphStore.types';

export function normalizeComponentPayloadType(value: unknown): PayloadType | null {
	const t = String(value ?? '').trim().toLowerCase();
	if (t === 'string') return 'text';
	if (t === 'table' || t === 'text' || t === 'json' || t === 'binary' || t === 'embeddings') {
		return t as PayloadType;
	}
	return null;
}

export function deriveIoFromComponentApi(api: unknown): { in?: PayloadType | null; out?: PayloadType | null } {
	const contract = (api ?? {}) as ComponentApiContract;
	const inputs = Array.isArray(contract?.inputs) ? contract.inputs : [];
	const inTyped = (inputs[0] as any)?.typedSchema?.type;
	const inputType = normalizeComponentPayloadType(inTyped ?? null);
	// Component output routing/type comes from API outputs + sourceHandle.
	return { in: inputType, out: null };
}

export function deriveSourceOutPort(data: PipelineNodeData): PayloadType {
	const schema = (data as any)?.schema ?? {};
	const expectedType = normalizeTypedSchemaPrimitive(schema?.expectedSchema?.typedSchema?.type);
	if (isPayloadType(expectedType)) return expectedType;
	const params = (((data as any)?.params ?? {}) as Record<string, any>);
	const outputMode = String(
		(params?.output && typeof params.output === 'object' ? (params.output as any)?.mode : undefined) ??
			params?.output_mode ??
			''
	)
		.trim()
		.toLowerCase();
	if (outputMode === 'json') return 'json';
	if (outputMode === 'text') return 'text';
	if (outputMode === 'table') return 'table';
	if (outputMode === 'binary') return 'binary';
	const inferredType = normalizeTypedSchemaPrimitive(schema?.inferredSchema?.typedSchema?.type);
	if (isPayloadType(inferredType)) return inferredType;
	const observedType = normalizeTypedSchemaPrimitive(schema?.observedSchema?.typedSchema?.type);
	if (isPayloadType(observedType)) return observedType;
	const sourceKind = String((data as any)?.sourceKind ?? '').trim().toLowerCase();
	if (sourceKind === 'api') return 'json';
	if (sourceKind === 'database' || sourceKind === 'warehouse') return 'table';
	if (sourceKind === 'file' || sourceKind === 'object_store') {
		const fileFormat = String(params?.file_format ?? '').trim().toLowerCase();
		if (fileFormat === 'json') return 'json';
		if (fileFormat === 'txt' || fileFormat === 'pdf') return 'text';
		if (
			fileFormat === 'csv' ||
			fileFormat === 'tsv' ||
			fileFormat === 'parquet' ||
			fileFormat === 'excel'
		) {
			return 'table';
		}
		if (
			fileFormat === 'jpg' ||
			fileFormat === 'jpeg' ||
			fileFormat === 'png' ||
			fileFormat === 'webp' ||
			fileFormat === 'gif' ||
			fileFormat === 'svg' ||
			fileFormat === 'tif' ||
			fileFormat === 'tiff' ||
			fileFormat === 'mp3' ||
			fileFormat === 'wav' ||
			fileFormat === 'flac' ||
			fileFormat === 'ogg' ||
			fileFormat === 'm4a' ||
			fileFormat === 'aac' ||
			fileFormat === 'mp4' ||
			fileFormat === 'mov' ||
			fileFormat === 'webm'
		) {
			// Source contract parity remains binary for media payloads.
			return 'binary';
		}
		return 'binary';
	}
	return 'text';
}

export function deriveDeclaredOutPort(data: PipelineNodeData): PayloadType | null {
	const schema = (data as any)?.schema ?? {};
	const expectedType = normalizeTypedSchemaPrimitive(schema?.expectedSchema?.typedSchema?.type);
	return isPayloadType(expectedType) ? expectedType : null;
}

export function deriveLlmOutPort(data: PipelineNodeData, params: Record<string, any>): PayloadType {
	const declaredOut = deriveDeclaredOutPort(data);
	if (declaredOut) return declaredOut;
	const output = params?.output && typeof params.output === 'object' ? params.output : {};
	const mode = String((output as any)?.mode ?? '').trim().toLowerCase();
	if (mode === 'json') return 'json';
	if (mode === 'binary') return 'binary';
	if (mode === 'embeddings') return 'embeddings';
	return 'text';
}

export function deriveModelInPort(data: PipelineNodeData): PayloadType {
	const modelKind = String((data as any)?.modelKind ?? 'llm').trim().toLowerCase();
	if (modelKind === 'vision') return 'image';
	if (modelKind === 'audio') return 'audio';
	return 'text';
}

export function deriveTransformIo(params: Record<string, any>, transformKindRaw: unknown): { in: PayloadType; out: PayloadType } {
	const op = String(params?.op ?? transformKindRaw ?? '').trim().toLowerCase();
	if (op === 'json_to_table') return { in: 'json', out: 'table' };
	if (op === 'json_filter') return { in: 'json', out: 'json' };
	if (op === 'text_to_table') return { in: 'text', out: 'table' };
	if (op === 'table_to_json') return { in: 'table', out: 'json' };
	return { in: 'table', out: 'table' };
}

export function deriveToolIo(_params: Record<string, any>): { in: PayloadType; out: PayloadType } {
	return { in: 'json', out: 'json' };
}

export function deriveNodeIoForData(data: PipelineNodeData): { in: PayloadType | null; out: PayloadType | null } {
	if (data.kind === 'source') {
		return { in: null, out: deriveSourceOutPort(data) };
	}
	const params = ((data as any)?.params ?? {}) as Record<string, any>;
	if (data.kind === 'llm') {
		return { in: 'text', out: deriveLlmOutPort(data, params) };
	}
	if (data.kind === 'model') {
		return { in: deriveModelInPort(data), out: deriveLlmOutPort(data, params) };
	}
	if (data.kind === 'transform') {
		return deriveTransformIo(params, (data as any)?.transformKind);
	}
	if (data.kind === 'tool') {
		return deriveToolIo(params);
	}
	if (data.kind === 'component') {
		const componentIo = deriveIoFromComponentApi((params as any)?.api);
		return {
			in: (componentIo.in ?? null) as PayloadType | null,
			out: (componentIo.out ?? null) as PayloadType | null
		};
	}
	return { in: null, out: null };
}

export function canonicalizeNodeSchemas(nodes: Node<PipelineNodeData>[]): Node<PipelineNodeData>[] {
	const normalizeMode = (raw: unknown): 'once' | 'single_item' | 'batch' => {
		const mode = String(raw ?? 'once').trim().toLowerCase();
		if (mode === 'single_item' || mode === 'continuous') return 'single_item';
		if (mode === 'batch') return 'batch';
		return 'once';
	};
	const normalizePlane = (raw: unknown): 'work' | 'param' | 'control' => {
		const plane = String(raw ?? 'work').trim().toLowerCase();
		if (plane === 'config') return 'param';
		if (plane === 'param' || plane === 'control') return plane;
		return 'work';
	};
	const normalizeCardinality = (raw: unknown): 'one' | 'many' => {
		const cardinality = String(raw ?? 'many').trim().toLowerCase();
		return cardinality === 'one' ? 'one' : 'many';
	};
	const normalizePortDeclarations = (
		raw: unknown
	): { in: Record<string, any>; out: Record<string, any> } => {
		const value = raw && typeof raw === 'object' ? (raw as Record<string, any>) : {};
		const out: { in: Record<string, any>; out: Record<string, any> } = { in: {}, out: {} };
		for (const direction of ['in', 'out'] as const) {
			const byDir = value[direction];
			if (!byDir || typeof byDir !== 'object') continue;
			for (const [handle, decl] of Object.entries(byDir as Record<string, any>)) {
				const key = String(handle ?? '').trim();
				if (!key || !decl || typeof decl !== 'object') continue;
				const plane = normalizePlane((decl as any).plane ?? (decl as any).affinity);
				const normalized: Record<string, any> = {
					plane,
					affinity: plane,
					required: Boolean((decl as any).required ?? false),
					cardinality: normalizeCardinality((decl as any).cardinality)
				};
				if (direction === 'in') {
					normalized.behavior = normalizeMode((decl as any).behavior ?? (decl as any).consume_mode);
				}
				out[direction][key] = normalized;
			}
		}
		return out;
	};
	const portDeclarationsFromPortContracts = (
		raw: unknown
	): { in: Record<string, any>; out: Record<string, any> } | null => {
		if (!raw || typeof raw !== 'object') return null;
		const value = raw as Record<string, any>;
		const out: { in: Record<string, any>; out: Record<string, any> } = { in: {}, out: {} };
		for (const direction of ['in', 'out'] as const) {
			const byDir = value[direction];
			if (!byDir || typeof byDir !== 'object') continue;
			for (const [handle, contract] of Object.entries(byDir as Record<string, any>)) {
				const key = String(handle ?? '').trim();
				if (!key || !contract || typeof contract !== 'object') continue;
				const plane = normalizePlane((contract as any).affinity);
				const normalized: Record<string, any> = {
					plane,
					affinity: plane,
					required: false,
					cardinality: 'many'
				};
				if (direction === 'in') {
					normalized.behavior = normalizeMode((contract as any).behavior);
				}
				out[direction][key] = normalized;
			}
		}
		const hasAny = Object.keys(out.in).length > 0 || Object.keys(out.out).length > 0;
		return hasAny ? out : null;
	};
	const defaultPortDeclarationsForKind = (
		kindRaw: unknown
	): { in: Record<string, any>; out: Record<string, any> } => {
		const kind = String(kindRaw ?? '').trim().toLowerCase();
		const out: { in: Record<string, any>; out: Record<string, any> } = {
			in: {},
			out: {
				out: { plane: 'work', affinity: 'work', required: false, cardinality: 'many' }
			}
		};
		if (kind === 'source') return out;
		out.in.in = {
			plane: 'work',
			affinity: 'work',
			required: false,
			cardinality: 'many',
			behavior: 'single_item'
		};
		if (kind === 'model' || kind === 'llm') {
			out.in.param_filters = {
				plane: 'param',
				affinity: 'param',
				required: false,
				cardinality: 'many',
				behavior: 'once'
			};
			out.in.param_context = {
				plane: 'param',
				affinity: 'param',
				required: false,
				cardinality: 'many',
				behavior: 'once'
			};
		} else {
			out.in.param_config = {
				plane: 'param',
				affinity: 'param',
				required: false,
				cardinality: 'many',
				behavior: 'once'
			};
		}
		out.in.control_in = {
			plane: 'control',
			affinity: 'control',
			required: false,
			cardinality: 'many',
			behavior: 'single_item'
		};
		return out;
	};
	return nodes.map((node) => {
		const inferredSchema = deriveInferredSchemaObservationForNode({
			...node,
			data: {
				...node.data
			}
		} as Node<PipelineNodeData>);
		const existingSchema =
			node.data?.schema && typeof node.data.schema === 'object'
				? (node.data.schema as Record<string, unknown>)
				: {};
		const migratedSchema = { ...existingSchema } as Record<string, unknown>;
		const legacyExpectedInputSchema =
			migratedSchema.expectedInputSchema && typeof migratedSchema.expectedInputSchema === 'object'
				? (migratedSchema.expectedInputSchema as Record<string, unknown>)
				: null;
		const splitExpectedInputSchemas =
			migratedSchema.expectedInputSchemas && typeof migratedSchema.expectedInputSchemas === 'object'
				? (migratedSchema.expectedInputSchemas as Record<string, unknown>)
				: null;
		if (legacyExpectedInputSchema && !splitExpectedInputSchemas) {
			migratedSchema.expectedInputSchemas = { in: legacyExpectedInputSchema };
		}
		if (splitExpectedInputSchemas) {
			const repairedExpectedInputSchemas: Record<string, Record<string, unknown>> = {};
			for (const [rawHandle, rawEnvelope] of Object.entries(splitExpectedInputSchemas)) {
				const handle = String(rawHandle ?? '').trim();
				if (!handle || !rawEnvelope || typeof rawEnvelope !== 'object') continue;
				const parsed = NodeSchemaObservationSchema.safeParse(rawEnvelope);
				if (parsed.success) {
					repairedExpectedInputSchemas[handle] = parsed.data as unknown as Record<string, unknown>;
					continue;
				}
				const typed = (rawEnvelope as any)?.typedSchema;
				const typedType = String((typed as any)?.type ?? '').trim().toLowerCase();
				const repairedType: PayloadType =
					typedType === 'json' || typedType === 'table' || typedType === 'binary' || typedType === 'embeddings' || typedType === 'image' || typedType === 'audio' || typedType === 'video'
						? (typedType as PayloadType)
						: 'text';
				const typedFields = Array.isArray((typed as any)?.fields) ? (typed as any).fields : [];
				repairedExpectedInputSchemas[handle] = {
					typedSchema: {
						type: repairedType,
						fields: typedFields
					},
					source: String((rawEnvelope as any)?.source ?? 'declared'),
					state: String((rawEnvelope as any)?.state ?? 'fresh')
				};
			}
			migratedSchema.expectedInputSchemas = repairedExpectedInputSchemas;
		}
		delete (migratedSchema as any).expectedInputSchema;
		const nextSchemaRaw = {
			...migratedSchema,
			...(inferredSchema ? { inferredSchema } : {})
		};
		const parsedSchema = NodeSchemaEnvelopeSchema.safeParse(nextSchemaRaw);
		const nextSchema =
			Object.keys(nextSchemaRaw).length > 0 && parsedSchema.success ? parsedSchema.data : node.data.schema;
		const processingPolicyRaw =
			(node.data as any)?.processingPolicy && typeof (node.data as any)?.processingPolicy === 'object'
				? ((node.data as any).processingPolicy as Record<string, any>)
				: {};
		const inputHandlesRaw =
			processingPolicyRaw?.input_handles && typeof processingPolicyRaw.input_handles === 'object'
				? (processingPolicyRaw.input_handles as Record<string, any>)
				: {};
		const normalizedInputHandles = Object.fromEntries(
			Object.entries(inputHandlesRaw)
				.map(([handle, policy]) => {
					if (!policy || typeof policy !== 'object') return null;
					return [
						String(handle ?? '').trim(),
						{
							consume_mode: normalizeMode((policy as any).consume_mode ?? (policy as any).consumeMode),
							batch_size: Math.max(
								1,
								Number((policy as any).batch_size ?? (policy as any).batchSize ?? 1)
							),
							max_inflight: Math.max(
								1,
								Number((policy as any).max_inflight ?? (policy as any).maxInflight ?? 1)
							),
							read_once: Boolean(
								(policy as any).read_once ??
									(policy as any).readOnce ??
									['once', 'read_once'].includes(
										String((policy as any).consume_mode ?? (policy as any).consumeMode ?? '')
											.trim()
											.toLowerCase()
									)
							)
						}
					];
				})
				.filter(
					(entry): entry is [string, { consume_mode: 'once' | 'single_item' | 'batch'; batch_size: number; max_inflight: number; read_once: boolean }] =>
						Array.isArray(entry) && String(entry[0] ?? '').trim().length > 0
				)
		);
		const normalizedProcessingPolicy = {
			consume_mode: normalizeMode(processingPolicyRaw.consume_mode ?? processingPolicyRaw.consumeMode),
			batch_size: Math.max(1, Number(processingPolicyRaw.batch_size ?? processingPolicyRaw.batchSize ?? 1)),
			max_inflight: Math.max(
				1,
				Number(processingPolicyRaw.max_inflight ?? processingPolicyRaw.maxInflight ?? 1)
			),
			read_once: Boolean(
				processingPolicyRaw.read_once ??
					processingPolicyRaw.readOnce ??
					['once', 'read_once'].includes(
						String(processingPolicyRaw.consume_mode ?? processingPolicyRaw.consumeMode ?? '')
							.trim()
							.toLowerCase()
					)
			),
			...(String(processingPolicyRaw.on_error ?? processingPolicyRaw.onError ?? '')
				.trim()
				.toLowerCase() === 'skip_failed'
				? { on_error: 'skip_failed' as const }
				: String(processingPolicyRaw.on_error ?? processingPolicyRaw.onError ?? '')
						.trim()
						.toLowerCase() === 'fail_fast'
					? { on_error: 'fail_fast' as const }
					: {}),
			input_handles: normalizedInputHandles
		};
		const rawPortDeclarations =
			(node.data as any)?.portDeclarations && typeof (node.data as any).portDeclarations === 'object'
				? ((node.data as any).portDeclarations as Record<string, unknown>)
				: null;
		const rawPortContracts =
			(node.data as any)?.portContracts && typeof (node.data as any).portContracts === 'object'
				? ((node.data as any).portContracts as Record<string, unknown>)
				: null;
		const normalizedPortDeclarations = rawPortDeclarations
			? normalizePortDeclarations(rawPortDeclarations)
			: portDeclarationsFromPortContracts(rawPortContracts) ??
				defaultPortDeclarationsForKind((node.data as any)?.kind);
		const portContractsFromDeclarations: Record<string, Record<string, any>> = { in: {}, out: {} };
		if (normalizedPortDeclarations) {
			for (const direction of ['in', 'out'] as const) {
				for (const [handle, decl] of Object.entries(normalizedPortDeclarations[direction])) {
					const entry: Record<string, any> = { affinity: String((decl as any).plane ?? 'work') };
					if (direction === 'in') entry.behavior = String((decl as any).behavior ?? 'single_item');
					portContractsFromDeclarations[direction][handle] = entry;
				}
			}
		}
		return {
			...node,
			data: {
				...node.data,
				processingPolicy: normalizedProcessingPolicy,
				...(normalizedPortDeclarations ? { portDeclarations: normalizedPortDeclarations } : {}),
				portContracts: normalizedPortDeclarations
					? {
							...(((node.data as any)?.portContracts as Record<string, unknown>) ?? {}),
							...portContractsFromDeclarations
						}
					: ((node.data as any)?.portContracts as any),
				...(nextSchema ? { schema: nextSchema } : {})
			}
		};
	});
}

export function getPayloadType(
	nodes: Node<PipelineNodeData>[],
	sourceId: string,
	whichPort: 'in' | 'out'
): PayloadType | null {
	const n = nodes.find((x) => x.id === sourceId);
	if (!n) return null;
	const derived = deriveNodeIoForData(n.data);
	return (whichPort === 'in' ? derived.in : derived.out) as PayloadType | null;
}

export function componentApiOutputPayloadType(
	node: Node<PipelineNodeData>,
	sourceHandle: string
): PayloadType | null {
	if (node.data.kind !== 'component') return null;
	const params = ((node.data as any)?.params ?? {}) as Record<string, any>;
	const publishedProfile = Array.isArray(params?.published_profile) ? (params.published_profile as any[]) : [];
	if (publishedProfile.length > 0) {
		const publishedOutputs = publishedProfile.filter(
			(item) => String(item?.kind ?? '').trim().toLowerCase() === 'data_output'
		);
		const handle = String(sourceHandle ?? '').trim();
		if (!handle || handle === 'out') {
			if (publishedOutputs.length === 1) {
				return normalizeComponentPayloadType((publishedOutputs[0] as any)?.native_contract?.type ?? null);
			}
			return null;
		}
		const decl = publishedOutputs.find(
			(item) =>
				String((item as any)?.alias ?? '').trim() === handle ||
				String((item as any)?.handle_id ?? '').trim() === handle
		);
		return normalizeComponentPayloadType((decl as any)?.native_contract?.type ?? null);
	}
	const outputs = Array.isArray((node.data as any)?.params?.api?.outputs)
		? ((node.data as any).params.api.outputs as any[])
		: [];
	const handle = String(sourceHandle ?? '').trim();
	if (!handle || handle === 'out') {
		if (outputs.length === 1) {
			return normalizeComponentPayloadType((outputs[0] as any)?.typedSchema?.type ?? null);
		}
		return null;
	}
	const decl = outputs.find((o) => String((o as any)?.name ?? '').trim() === handle);
	return normalizeComponentPayloadType((decl as any)?.typedSchema?.type ?? null);
}

export function sourcePayloadTypeForEdge(
	nodes: Node<PipelineNodeData>[],
	edge: Edge<PipelineEdgeData>
): PayloadType | null {
	const node = nodes.find((x) => x.id === edge.source);
	if (!node) return null;
	if (node.data.kind === 'component') {
		return componentApiOutputPayloadType(node, String((edge as any).sourceHandle ?? 'out'));
	}
	return deriveNodeIoForData(node.data).out;
}

export function sourcePayloadHint(
	node: Node<PipelineNodeData>,
	whichPort: 'in' | 'out',
	handleId: string = 'out',
	opts?: { preferNodeSchema?: boolean }
) {
	const preferNodeSchema = opts?.preferNodeSchema !== false;
	if (preferNodeSchema) {
		const expectedHint = typedSchemaToPayloadHint((node.data as any)?.schema?.expectedSchema?.typedSchema);
		if (expectedHint) return expectedHint;
		const inferredHint = typedSchemaToPayloadHint((node.data as any)?.schema?.inferredSchema?.typedSchema);
		if (inferredHint) return inferredHint;
		const observedHint = typedSchemaToPayloadHint((node.data as any)?.schema?.observedSchema?.typedSchema);
		if (observedHint) return observedHint;
	}
	if (node.data.kind === 'component' && whichPort === 'out') {
		const params = ((node.data as any)?.params ?? {}) as Record<string, any>;
		const publishedProfile = Array.isArray(params?.published_profile) ? (params.published_profile as any[]) : [];
		const publishedOutputs = publishedProfile.filter(
			(item) => String(item?.kind ?? '').trim().toLowerCase() === 'data_output'
		);
		const outputs =
			publishedOutputs.length > 0
				? publishedOutputs.map((item) => ({
						name: String((item as any)?.alias ?? (item as any)?.handle_id ?? '').trim(),
						typedSchema: (item as any)?.native_contract ?? null
					}))
				: Array.isArray((node.data as any)?.params?.api?.outputs)
					? ((node.data as any).params.api.outputs as any[])
					: [];
		const handle = String(handleId ?? '').trim();
		const decl = handle && handle !== 'out'
			? outputs.find((o) => String((o as any)?.name ?? '').trim() === handle)
			: outputs.length === 1
				? outputs[0]
				: null;
		const typed = (decl as any)?.typedSchema ?? null;
		const typedType = String((typed as any)?.type ?? '').trim().toLowerCase();
		if (typedType === 'table') {
			const fields = normalizeSchemaFields((typed as any)?.fields);
			const columns = schemaFieldNames(fields);
			return columns.length > 0 ? { type: 'table', fields, columns } : { type: 'table' };
		}
		if (typedType === 'json') return { type: 'json' };
		if (typedType === 'text') return { type: 'string' };
		if (typedType === 'binary') return { type: 'binary' };
		// Schema-first: component edge hints come from typedSchema only.
		return { type: 'unknown' };
	}
	const derived = deriveNodeIoForData(node.data);
	const fallbackType = normalizeHintType(whichPort === 'in' ? derived.in ?? 'unknown' : derived.out ?? 'unknown');
	const baseHint =
		fallbackType === 'table'
			? { type: 'table' }
			: fallbackType === 'json'
				? { type: 'json' }
				: fallbackType === 'text'
					? { type: 'string' }
					: fallbackType === 'binary'
						? { type: 'binary' }
						: fallbackType === 'embeddings'
							? { type: 'embeddings' }
							: { type: 'unknown' };
	if (whichPort === 'in' && (node.data.kind === 'llm' || node.data.kind === 'model')) {
		const policy = String(((node.data as any)?.params ?? {})?.coercion_policy ?? 'strict')
			.trim()
			.toLowerCase();
		return {
			...baseHint,
			coercion_policy: policy || 'strict'
		};
	}
	if (fallbackType === 'table') return { type: 'table' };
	if (fallbackType === 'json') return { type: 'json' };
	if (fallbackType === 'text') return { type: 'string' };
	if (fallbackType === 'binary') return { type: 'binary' };
	if (fallbackType === 'embeddings') return { type: 'embeddings' };
	return { type: 'unknown' };
}

export function targetPayloadHint(node: Node<PipelineNodeData>) {
	if (node.data.kind !== 'transform') return sourcePayloadHint(node, 'in');

	const params: any = node.data.params ?? {};
	const op = params?.op ?? node.data.transformKind;
	if (op === 'select') {
		const cols = params?.select?.columns;
		if (Array.isArray(cols) && cols.length > 0) {
			const requiredFields = makeSchemaFieldsFromColumns(cols);
			return {
				type: 'table',
				required_fields: requiredFields,
				required_columns: schemaFieldNames(requiredFields)
			};
		}
	}
	if (op === 'split') {
		const sourceColumn = String(params?.split?.sourceColumn ?? '').trim();
		if (sourceColumn) {
			const requiredFields = makeSchemaFieldsFromColumns([sourceColumn]);
			return {
				type: 'table',
				required_fields: requiredFields,
				required_columns: schemaFieldNames(requiredFields)
			};
		}
	}
	if (op === 'quality_gate') {
		const checks = Array.isArray(params?.quality_gate?.checks) ? params.quality_gate.checks : [];
		const required = new Set<string>();
		for (const check of checks) {
			const kind = String(check?.kind ?? '').trim().toLowerCase();
			if (kind === 'leakage') {
				const feature = String(check?.featureColumn ?? '').trim();
				const target = String(check?.targetColumn ?? '').trim();
				if (feature) required.add(feature);
				if (target) required.add(target);
				continue;
			}
			const column = String(check?.column ?? '').trim();
			if (column) required.add(column);
		}
		if (required.size > 0) {
			const requiredFields = makeSchemaFieldsFromColumns(Array.from(required));
			return {
				type: 'table',
				required_fields: requiredFields,
				required_columns: schemaFieldNames(requiredFields)
			};
		}
	}
	if (op === 'null_policy') {
		const cols = Array.isArray(params?.null_policy?.columns) ? params.null_policy.columns : [];
		const required = cols
			.map((c: unknown) => String(c ?? '').trim())
			.filter((c: string) => c.length > 0);
		if (required.length > 0) {
			const requiredFields = makeSchemaFieldsFromColumns(required);
			return {
				type: 'table',
				required_fields: requiredFields,
				required_columns: schemaFieldNames(requiredFields)
			};
		}
	}
	if (op === 'outlier_policy') {
		const cols = Array.isArray(params?.outlier_policy?.columns) ? params.outlier_policy.columns : [];
		const required = cols
			.map((c: unknown) => String(c ?? '').trim())
			.filter((c: string) => c.length > 0);
		if (required.length > 0) {
			const requiredFields = makeSchemaFieldsFromColumns(required);
			return {
				type: 'table',
				required_fields: requiredFields,
				required_columns: schemaFieldNames(requiredFields)
			};
		}
	}
	if (op === 'text_clean') {
		const cols = Array.isArray(params?.text_clean?.columns) ? params.text_clean.columns : [];
		const required = cols
			.map((c: unknown) => String(c ?? '').trim())
			.filter((c: string) => c.length > 0);
		if (required.length > 0) {
			const requiredFields = makeSchemaFieldsFromColumns(required);
			return {
				type: 'table',
				required_fields: requiredFields,
				required_columns: schemaFieldNames(requiredFields)
			};
		}
	}
	if (op === 'nlp_normalize') {
		const cols = Array.isArray(params?.nlp_normalize?.columns) ? params.nlp_normalize.columns : [];
		const required = cols
			.map((c: unknown) => String(c ?? '').trim())
			.filter((c: string) => c.length > 0);
		if (required.length > 0) {
			const requiredFields = makeSchemaFieldsFromColumns(required);
			return {
				type: 'table',
				required_fields: requiredFields,
				required_columns: schemaFieldNames(requiredFields)
			};
		}
	}
	if (op === 'tokenize_chunk') {
		const cols = Array.isArray(params?.tokenize_chunk?.columns) ? params.tokenize_chunk.columns : [];
		const required = cols
			.map((c: unknown) => String(c ?? '').trim())
			.filter((c: string) => c.length > 0);
		if (required.length > 0) {
			const requiredFields = makeSchemaFieldsFromColumns(required);
			return {
				type: 'table',
				required_fields: requiredFields,
				required_columns: schemaFieldNames(requiredFields)
			};
		}
	}
	if (op === 'dataset_split') {
		const spec = params?.dataset_split ?? {};
		const strategy = String(spec?.strategy ?? '').trim().toLowerCase();
		const required: string[] = [];
		if (strategy === 'stratified') required.push(String(spec?.stratifyColumn ?? '').trim());
		if (strategy === 'group') required.push(String(spec?.groupColumn ?? '').trim());
		if (strategy === 'time') required.push(String(spec?.timeColumn ?? '').trim());
		const cols = required.filter((c) => c.length > 0);
		if (cols.length > 0) {
			const requiredFields = makeSchemaFieldsFromColumns(cols);
			return {
				type: 'table',
				required_fields: requiredFields,
				required_columns: schemaFieldNames(requiredFields)
			};
		}
	}
	if (op === 'class_imbalance') {
		const label = String(params?.class_imbalance?.labelColumn ?? '').trim();
		if (label) {
			const requiredFields = makeSchemaFieldsFromColumns([label]);
			return {
				type: 'table',
				required_fields: requiredFields,
				required_columns: schemaFieldNames(requiredFields)
			};
		}
	}
	if (op === 'categorical_encode') {
		const cols = Array.isArray(params?.categorical_encode?.columns) ? params.categorical_encode.columns : [];
		const required = cols.map((c: unknown) => String(c ?? '').trim()).filter((c: string) => c.length > 0);
		if (required.length > 0) {
			const requiredFields = makeSchemaFieldsFromColumns(required);
			return {
				type: 'table',
				required_fields: requiredFields,
				required_columns: schemaFieldNames(requiredFields)
			};
		}
	}
	if (op === 'numeric_scale') {
		const cols = Array.isArray(params?.numeric_scale?.columns) ? params.numeric_scale.columns : [];
		const required = cols.map((c: unknown) => String(c ?? '').trim()).filter((c: string) => c.length > 0);
		if (required.length > 0) {
			const requiredFields = makeSchemaFieldsFromColumns(required);
			return {
				type: 'table',
				required_fields: requiredFields,
				required_columns: schemaFieldNames(requiredFields)
			};
		}
	}
	if (op === 'embedding') {
		const cols = Array.isArray(params?.embedding?.columns) ? params.embedding.columns : [];
		const required = cols.map((c: unknown) => String(c ?? '').trim()).filter((c: string) => c.length > 0);
		if (required.length > 0) {
			const requiredFields = makeSchemaFieldsFromColumns(required);
			return {
				type: 'table',
				required_fields: requiredFields,
				required_columns: schemaFieldNames(requiredFields)
			};
		}
	}
	if (op === 'feature_selection') {
		const spec = params?.feature_selection ?? {};
		const required = new Set<string>();
		const cols = Array.isArray(spec?.columns) ? spec.columns : [];
		for (const c of cols) {
			const col = String(c ?? '').trim();
			if (col) required.add(col);
		}
		const selected = Array.isArray(spec?.selectedColumns) ? spec.selectedColumns : [];
		for (const c of selected) {
			const col = String(c ?? '').trim();
			if (col) required.add(col);
		}
		if (required.size > 0) {
			const requiredFields = makeSchemaFieldsFromColumns(Array.from(required));
			return {
				type: 'table',
				required_fields: requiredFields,
				required_columns: schemaFieldNames(requiredFields)
			};
		}
	}
	if (op === 'leakage_detect') {
		const spec = params?.leakage_detect ?? {};
		const required = new Set<string>();
		const splitCol = String(spec?.splitColumn ?? '').trim();
		if (splitCol) required.add(splitCol);
		const keys = Array.isArray(spec?.keyColumns) ? spec.keyColumns : [];
		for (const k of keys) {
			const col = String(k ?? '').trim();
			if (col) required.add(col);
		}
		const label = String(spec?.labelColumn ?? '').trim();
		if (label) required.add(label);
		if (required.size > 0) {
			const requiredFields = makeSchemaFieldsFromColumns(Array.from(required));
			return {
				type: 'table',
				required_fields: requiredFields,
				required_columns: schemaFieldNames(requiredFields)
			};
		}
	}
	if (op === 'quality_profile' || op === 'drift_compare' || op === 'determinism_profile' || op === 'fit_state_registry' || op === 'pii_guard' || op === 'inference_parity') {
		const fieldCandidates: string[] = [];
		if (op === 'quality_profile') {
			const cols = Array.isArray(params?.quality_profile?.columns) ? params.quality_profile.columns : [];
			fieldCandidates.push(...cols.map((c: unknown) => String(c ?? '').trim()));
		}
		if (op === 'drift_compare') {
			const cols = Array.isArray(params?.drift_compare?.compareColumns) ? params.drift_compare.compareColumns : [];
			fieldCandidates.push(...cols.map((c: unknown) => String(c ?? '').trim()));
		}
		if (op === 'fit_state_registry') {
			const cols = Array.isArray(params?.fit_state_registry?.includeColumns) ? params.fit_state_registry.includeColumns : [];
			fieldCandidates.push(...cols.map((c: unknown) => String(c ?? '').trim()));
		}
		if (op === 'pii_guard') {
			const cols = Array.isArray(params?.pii_guard?.columns) ? params.pii_guard.columns : [];
			fieldCandidates.push(...cols.map((c: unknown) => String(c ?? '').trim()));
		}
		const required = fieldCandidates.filter((c) => c.length > 0);
		if (required.length > 0) {
			const requiredFields = makeSchemaFieldsFromColumns(required);
			return {
				type: 'table',
				required_fields: requiredFields,
				required_columns: schemaFieldNames(requiredFields)
			};
		}
	}
	if (op === 'tokenize_chunk') {
		const cols = Array.isArray(params?.tokenize_chunk?.columns) ? params.tokenize_chunk.columns : [];
		const required = cols
			.map((c: unknown) => String(c ?? '').trim())
			.filter((c: string) => c.length > 0);
		if (required.length > 0) {
			const requiredFields = makeSchemaFieldsFromColumns(required);
			return {
				type: 'table',
				required_fields: requiredFields,
				required_columns: schemaFieldNames(requiredFields)
			};
		}
	}
	if (op === 'ml_contract') {
		const spec = params?.ml_contract ?? {};
		const required = new Set<string>();
		const label = String(spec?.labelColumn ?? '').trim();
		if (label) required.add(label);
		const features = Array.isArray(spec?.featureColumns) ? spec.featureColumns : [];
		for (const feature of features) {
			const col = String(feature ?? '').trim();
			if (col) required.add(col);
		}
		const id = String(spec?.idColumn ?? '').trim();
		if (id) required.add(id);
		const ts = String(spec?.timestampColumn ?? '').trim();
		if (ts) required.add(ts);
		if (required.size > 0) {
			const requiredFields = makeSchemaFieldsFromColumns(Array.from(required));
			return {
				type: 'table',
				required_fields: requiredFields,
				required_columns: schemaFieldNames(requiredFields)
			};
		}
	}
	return sourcePayloadHint(node, 'in');
}

export function normalizeHintType(raw: unknown): string {
	const value = String(raw ?? '').trim().toLowerCase();
	if (value === 'string') return 'text';
	return value;
}

type SchemaField = {
	name: string;
	type: string;
	nullable: boolean;
	constraints?: Record<string, unknown>;
};

export function normalizeSchemaField(raw: unknown): SchemaField | null {
	if (typeof raw === 'string') {
		const name = String(raw).trim();
		if (!name) return null;
		return { name, type: 'unknown', nullable: true };
	}
	if (!raw || typeof raw !== 'object') return null;
	const name = String((raw as any).name ?? '').trim();
	if (!name) return null;
	const typeRaw = String((raw as any).type ?? 'unknown').trim().toLowerCase();
	const type = typeRaw.length > 0 ? typeRaw : 'unknown';
	const nullable =
		typeof (raw as any).nullable === 'boolean'
			? Boolean((raw as any).nullable)
			: String(type).toLowerCase() === 'unknown';
	const constraints =
		(raw as any).constraints && typeof (raw as any).constraints === 'object'
			? ({ ...(raw as any).constraints } as Record<string, unknown>)
			: undefined;
	return { name, type, nullable, constraints };
}

export function normalizeSchemaFields(raw: unknown): SchemaField[] {
	if (!Array.isArray(raw)) return [];
	const preferred = new Map<string, SchemaField>();
	for (const item of raw) {
		const field = normalizeSchemaField(item);
		if (!field) continue;
		const key = field.name.toLowerCase();
		const existing = preferred.get(key);
		if (!existing) {
			preferred.set(key, field);
			continue;
		}
		const existingUnknown = existing.type === 'unknown';
		const nextUnknown = field.type === 'unknown';
		if (existingUnknown && !nextUnknown) {
			preferred.set(key, field);
			continue;
		}
		if (existing.type === field.type && existing.nullable && !field.nullable) {
			preferred.set(key, field);
		}
	}
	return Array.from(preferred.values());
}

export function schemaFieldNames(fields: SchemaField[]): string[] {
	return fields.map((field) => String(field.name ?? '').trim()).filter((name) => name.length > 0);
}

export function makeSchemaFieldsFromColumns(columns: unknown, fallbackType = 'unknown'): SchemaField[] {
	if (!Array.isArray(columns)) return [];
	return normalizeSchemaFields(
		columns
			.map((col) => String(col ?? '').trim())
			.filter((name) => name.length > 0)
			.map((name) => ({ name, type: fallbackType, nullable: true }))
	);
}

type TypedSchemaPrimitive =
	| 'table'
	| 'json'
	| 'text'
	| 'binary'
	| 'embeddings'
	| 'image'
	| 'audio'
	| 'video'
	| 'unknown';

export function normalizeTypedSchemaPrimitive(raw: unknown): TypedSchemaPrimitive {
	const value = String(raw ?? '').trim().toLowerCase();
	if (value === 'table') return 'table';
	if (value === 'json' || value === 'object' || value === 'array') return 'json';
	if (value === 'text' || value === 'string') return 'text';
	if (value === 'binary' || value === 'bytes') return 'binary';
	if (value === 'embeddings' || value === 'embedding') return 'embeddings';
	if (value === 'image') return 'image';
	if (value === 'audio') return 'audio';
	if (value === 'video') return 'video';
	return 'unknown';
}

export function schemaFieldTypeToTypedPrimitive(raw: unknown): TypedSchemaPrimitive {
	const normalized = normalizeTypedSchemaPrimitive(raw);
	if (normalized !== 'unknown') return normalized;
	return 'unknown';
}

export function normalizeTypedSchemaFields(raw: unknown): Array<{
	name: string;
	type: TypedSchemaPrimitive;
	nullable?: boolean;
}> {
	const fields = normalizeSchemaFields(raw);
	return fields.map((field) => ({
		name: field.name,
		type: schemaFieldTypeToTypedPrimitive(field.type),
		nullable: field.nullable
	}));
}

export function payloadHintToTypedSchema(payloadHint: unknown): { type: TypedSchemaPrimitive; fields: Array<{ name: string; type: TypedSchemaPrimitive; nullable?: boolean }> } | null {
	if (!payloadHint || typeof payloadHint !== 'object') return null;
	const hint = payloadHint as Record<string, unknown>;
	const type = normalizeTypedSchemaPrimitive(hint.type);
	if (type === 'unknown') return null;
	const rawFields = Array.isArray(hint.fields)
		? hint.fields
		: (Array.isArray(hint.required_fields) ? hint.required_fields : []);
	if (type !== 'table') {
		// Preserve declared JSON keys for authoring-time helpers (for example Transform JSON filter key pickers).
		// Non-table schemas may still omit fields entirely.
		const normalizedFields = type === 'json' ? normalizeTypedSchemaFields(rawFields) : [];
		return { type, fields: normalizedFields };
	}
	return { type: 'table', fields: normalizeTypedSchemaFields(rawFields) };
}

export function typedSchemaToPayloadHint(typedSchemaRaw: unknown): Record<string, unknown> | null {
	if (!typedSchemaRaw || typeof typedSchemaRaw !== 'object') return null;
	const typedSchema = typedSchemaRaw as Record<string, unknown>;
	const type = normalizeTypedSchemaPrimitive(typedSchema.type);
	if (type === 'unknown') return null;
	if (type === 'table') {
		const fields = normalizeTypedSchemaFields(Array.isArray(typedSchema.fields) ? typedSchema.fields : []);
		return {
			type: 'table',
			fields,
			columns: schemaFieldNames(fields as any)
		};
	}
	if (type === 'json') {
		const fields = normalizeTypedSchemaFields(Array.isArray(typedSchema.fields) ? typedSchema.fields : []);
		if (fields.length > 0) {
			return { type: 'json', fields };
		}
	}
	if (type === 'text') return { type: 'string' };
	return { type };
}

export function fingerprintTypedSchema(typedSchemaRaw: unknown): string | undefined {
	const typed = typedSchemaRaw && typeof typedSchemaRaw === 'object' ? (typedSchemaRaw as Record<string, unknown>) : null;
	if (!typed) return undefined;
	const normalized = {
		type: normalizeTypedSchemaPrimitive(typed.type),
		fields: normalizeTypedSchemaFields(Array.isArray(typed.fields) ? typed.fields : [])
	};
	return JSON.stringify(normalized);
}

export function hasSchemaEnvelopeContent(raw: unknown): boolean {
	if (!raw || typeof raw !== 'object') return false;
	const env = raw as Record<string, unknown>;
	return Boolean(
		env.inferredSchema ||
			env.expectedInputSchemas ||
			env.expectedSchema ||
			env.observedSchema
	);
}

export function deriveInferredSchemaObservationForNode(node: Node<PipelineNodeData>): NodeSchemaObservation | null {
	if (node.data.kind === 'source') {
		const params = ((node.data as any)?.params ?? {}) as Record<string, unknown>;
		const sourceKind = String((node.data as any)?.sourceKind ?? '').trim().toLowerCase();
		if (sourceKind === 'file') {
			const fileFormat = String(params?.file_format ?? '').trim().toLowerCase();
			let type: TypedSchemaPrimitive = 'unknown';
			if (fileFormat === 'txt' || fileFormat === 'pdf') type = 'text';
			else if (fileFormat === 'json') type = 'json';
			else if (fileFormat === 'csv' || fileFormat === 'tsv' || fileFormat === 'parquet' || fileFormat === 'excel') type = 'table';
			else if (fileFormat) type = 'binary';
			if (type !== 'unknown') {
				const typed =
					type === 'table'
						? {
								type: 'table' as const,
								fields: normalizeTypedSchemaFields(
									((node.data as any)?.schema?.inferredSchema?.typedSchema?.fields as any[]) ?? []
								)
							}
						: { type, fields: [] as Array<{ name: string; type: TypedSchemaPrimitive; nullable?: boolean }> };
				return {
					typedSchema: typed,
					source: 'sample',
					state: 'fresh',
					schemaFingerprint: fingerprintTypedSchema(typed),
					updatedAt: String(
						(node.data as any)?.meta?.updatedAt ?? (node.data as any)?.meta?.createdAt ?? new Date().toISOString()
					)
				};
			}
		}
	}
	const hint = sourcePayloadHint(node, 'out', 'out', { preferNodeSchema: false });
	const typed = payloadHintToTypedSchema(hint);
	if (!typed) return null;
	const source = node.data.kind === 'component' ? 'component_contract' : 'sample';
	const updatedAt = String(
		(node.data as any)?.meta?.updatedAt ?? (node.data as any)?.meta?.createdAt ?? new Date().toISOString()
	);
	return {
		typedSchema: typed,
		source,
		state: typed.type === 'unknown' ? 'partial' : 'fresh',
		schemaFingerprint: fingerprintTypedSchema(typed),
		updatedAt
	};
}

export function deriveObservedSchemaObservationFromNodeOutput(
	evt: Extract<KnownRunEvent, { type: 'node_output' }>,
	node: Node<PipelineNodeData> | undefined
): NodeSchemaObservation | null {
	const observedType = normalizeTypedSchemaPrimitive((evt as any)?.payloadType ?? '');
	if (observedType === 'unknown') return null;
	const sourceObs = (evt as any)?.sourceObservability;
	const sourceObsColumns = Array.isArray(sourceObs?.table_columns)
		? normalizeTypedSchemaFields(
				(sourceObs.table_columns as any[]).map((col) => ({
					name: col?.name,
					type: col?.type
				}))
			)
		: [];
	const primingInferred = ((evt as any)?.primingArtifact?.inferred_schema ?? null) as Record<string, any> | null;
	const primingFields = Array.isArray(primingInferred?.fields)
		? normalizeTypedSchemaFields((primingInferred?.fields as any[]).map((f) => ({ name: f?.name, type: f?.type })))
		: [];
	const inferredFields =
		(node?.data as any)?.schema?.inferredSchema?.typedSchema?.fields &&
		Array.isArray((node?.data as any)?.schema?.inferredSchema?.typedSchema?.fields)
			? normalizeTypedSchemaFields((node?.data as any)?.schema?.inferredSchema?.typedSchema?.fields)
			: [];
	const resolvedFields =
		sourceObsColumns.length > 0
			? sourceObsColumns
			: primingFields.length > 0
				? primingFields
				: inferredFields;
	const typedSchema =
		observedType === 'table'
			? { type: 'table' as const, fields: resolvedFields }
			: { type: observedType, fields: [] };
	return {
		typedSchema,
		source: 'runtime',
		state: 'fresh',
		schemaFingerprint: fingerprintTypedSchema(typedSchema),
		updatedAt: String((evt as any)?.at ?? new Date().toISOString())
	};
}

export function computeSchemaDriftSummary(
	expectedRaw: unknown,
	observedRaw: unknown
): {
	hasDrift: boolean;
	typeMismatch: boolean;
	missingColumns: string[];
	mismatchedColumns: string[];
} {
	const expected = expectedRaw && typeof expectedRaw === 'object' ? (expectedRaw as Record<string, unknown>) : null;
	const observed = observedRaw && typeof observedRaw === 'object' ? (observedRaw as Record<string, unknown>) : null;
	if (!expected || !observed) {
		return { hasDrift: false, typeMismatch: false, missingColumns: [], mismatchedColumns: [] };
	}
	const expectedType = normalizeTypedSchemaPrimitive(expected.type);
	const observedType = normalizeTypedSchemaPrimitive(observed.type);
	const typeMismatch =
		expectedType !== 'unknown' &&
		observedType !== 'unknown' &&
		expectedType !== observedType;
	if (expectedType !== 'table' || observedType !== 'table') {
		return { hasDrift: typeMismatch, typeMismatch, missingColumns: [], mismatchedColumns: [] };
	}
	const expectedFields = normalizeSchemaFields(expected.fields);
	const observedFields = normalizeSchemaFields(observed.fields);
	const observedByName = new Map<string, SchemaField>();
	for (const field of observedFields) {
		const key = String(field.name ?? '').trim().toLowerCase();
		if (!key) continue;
		observedByName.set(key, field);
	}
	const missingColumns: string[] = [];
	const mismatchedColumns: string[] = [];
	for (const field of expectedFields) {
		const name = String(field.name ?? '').trim();
		if (!name) continue;
		const observedField = observedByName.get(name.toLowerCase());
		if (!observedField) {
			missingColumns.push(name);
			continue;
		}
		const expectedTypeForField = String(field.type ?? 'unknown').trim().toLowerCase() || 'unknown';
		const observedTypeForField = String(observedField.type ?? 'unknown').trim().toLowerCase() || 'unknown';
		if (
			expectedTypeForField !== 'unknown' &&
			observedTypeForField !== 'unknown' &&
			expectedTypeForField !== observedTypeForField
		) {
			mismatchedColumns.push(name);
		}
	}
	return {
		hasDrift: typeMismatch || missingColumns.length > 0 || mismatchedColumns.length > 0,
		typeMismatch,
		missingColumns,
		mismatchedColumns
	};
}

export function __normalizeSchemaFieldsForTest(raw: unknown): SchemaField[] {
	return normalizeSchemaFields(raw);
}

export function adapterKindForTypes(providedType: string, requiredType: string): AdapterTransformKind | null {
	const key = `${providedType}->${requiredType}`;
	if (key === 'text->table') return 'text_to_table';
	if (key === 'json->table') return 'json_to_table';
	if (key === 'table->json') return 'table_to_json';
	return null;
}

export function adapterSuggestionForTypes(providedType: string, requiredType: string): string | null {
	const adapterKind = adapterKindForTypes(providedType, requiredType);
	if (adapterKind === 'text_to_table') return "Insert Transform adapter: op='text_to_table'.";
	if (adapterKind === 'json_to_table') return "Insert Transform adapter: op='json_to_table'.";
	if (adapterKind === 'table_to_json') return "Insert Transform adapter: op='table_to_json'.";
	return null;
}

export function isSchemaCompatible(
	providedSchema: Record<string, any> | undefined,
	requiredSchema: Record<string, any> | undefined,
	edgeModeRaw: unknown = 'work'
): SchemaCompatibility {
	const edgeMode = String(edgeModeRaw ?? 'work').trim().toLowerCase() || 'work';
	if (edgeMode !== 'work') {
		return { ok: true };
	}
	const providedType = normalizeHintType(providedSchema?.type ?? 'unknown');
	const requiredType = normalizeHintType(requiredSchema?.type ?? 'unknown');
	if (requiredType === 'unknown') {
		return { ok: true };
	}
	if (providedType === 'unknown') {
		return {
			ok: false,
			reason: 'missing_typed_schema',
			missingColumns: []
		};
	}
	const coercionPolicy = String(requiredSchema?.coercion_policy ?? 'safe_widening')
		.trim()
		.toLowerCase();
	const coercion = evaluateSchemaCoercion(providedType, requiredType, coercionPolicy);
	if (!coercion.allowed) {
		const adapterKind = adapterKindForTypes(providedType, requiredType);
		return {
			ok: false,
			reason: 'type_mismatch',
			suggestion: adapterSuggestionForTypes(providedType, requiredType),
			adapterKind
		};
	}
	const providedFields = normalizeSchemaFields(providedSchema?.fields);
	const providedColumns =
		providedFields.length > 0
			? schemaFieldNames(providedFields)
			: Array.isArray(providedSchema?.columns)
				? providedSchema.columns
						.map((c: unknown) => String(c ?? '').trim())
						.filter((c: string) => c.length > 0)
				: [];
	const requiredFields = normalizeSchemaFields(requiredSchema?.required_fields);
	const requiredColumns =
		requiredFields.length > 0
			? schemaFieldNames(requiredFields)
			: Array.isArray(requiredSchema?.required_columns)
				? requiredSchema.required_columns
						.map((c: unknown) => String(c ?? '').trim())
						.filter((c: string) => c.length > 0)
				: [];
	if (requiredColumns.length > 0 && providedColumns.length === 0) {
		return { ok: false, reason: 'missing_typed_schema', missingColumns: requiredColumns };
	}
	if (requiredColumns.length > 0 && providedColumns.length > 0) {
		const missing = requiredColumns.filter((c) => !providedColumns.includes(c));
		if (missing.length > 0) {
			return { ok: false, reason: 'missing_required_columns', missingColumns: missing };
		}
	}
	if (coercion.lossy) {
		const adapterKind = adapterKindForTypes(providedType, requiredType);
		return {
			ok: true,
			warning: 'lossy_coercion',
			suggestion: adapterSuggestionForTypes(providedType, requiredType),
			adapterKind
		};
	}
	return { ok: true };
}

export function inferredTransformOutputHint(node: Node<PipelineNodeData>): Record<string, any> | undefined {
	if (node.data.kind !== 'transform') return undefined;
	const params: any = node.data.params ?? {};
	const op = String(params?.op ?? node.data.transformKind ?? '').trim().toLowerCase();
	if (op === 'table_to_json') return { type: 'json' };
	if (op === 'json_filter') return { type: 'json' };
	if (op === 'json_to_table' || op === 'text_to_table') return { type: 'table' };
	if (op === 'split') {
		const outColumn = String(params?.split?.outColumn ?? 'part').trim() || 'part';
		const fields: SchemaField[] = [{ name: outColumn, type: 'string', nullable: true }];
		if (Boolean(params?.split?.emitIndex ?? true)) {
			fields.push({ name: 'index', type: 'integer', nullable: false });
		}
		if (Boolean(params?.split?.emitSourceRow ?? true)) {
			fields.push({ name: 'source_row', type: 'integer', nullable: false });
		}
		return { type: 'table', fields, columns: schemaFieldNames(fields) };
	}
	if (op === 'select') {
		const mode = String(params?.select?.mode ?? 'include').trim().toLowerCase();
		const fields = makeSchemaFieldsFromColumns(params?.select?.columns);
		if (mode === 'include' && fields.length > 0) {
			return { type: 'table', fields, columns: schemaFieldNames(fields) };
		}
	}
	return undefined;
}

export function buildProvidedSchema(
	node: Node<PipelineNodeData>,
	sourceHandle: string
): Record<string, any> {
	return (
		inferredTransformOutputHint(node) ??
		(sourcePayloadHint(node as any, 'out', sourceHandle) as Record<string, any> | undefined) ??
		{ type: 'unknown' }
	);
}

export function expectedInputTypedSchemaForHandle(
	node: Node<PipelineNodeData>,
	targetHandleRaw?: string | null
): Record<string, unknown> | null {
	const schemaEnv = ((node.data as any)?.schema ?? {}) as Record<string, any>;
	const expectedByHandle =
		schemaEnv?.expectedInputSchemas && typeof schemaEnv.expectedInputSchemas === 'object'
			? (schemaEnv.expectedInputSchemas as Record<string, any>)
			: null;
	const targetHandle = String(targetHandleRaw ?? 'in').trim() || 'in';
	const handleEnvelope =
		expectedByHandle && typeof expectedByHandle[targetHandle] === 'object'
			? (expectedByHandle[targetHandle] as Record<string, any>)
			: expectedByHandle && typeof expectedByHandle.in === 'object'
				? (expectedByHandle.in as Record<string, any>)
				: null;
	if (handleEnvelope && typeof handleEnvelope.typedSchema === 'object') {
		return handleEnvelope.typedSchema as Record<string, unknown>;
	}
	return null;
}

export function buildRequiredSchema(
	node: Node<PipelineNodeData>,
	targetHandleRaw?: string | null
): Record<string, any> {
	const explicitInputHint = typedSchemaToPayloadHint(
		expectedInputTypedSchemaForHandle(node, targetHandleRaw)
	);
	const payload =
		(explicitInputHint as Record<string, any> | undefined) ??
		((targetPayloadHint(node as any) as Record<string, any> | undefined) ?? { type: 'unknown' });
	const params = ((node.data as any)?.params ?? {}) as Record<string, any>;
	const policyRaw =
		params?.coercion_policy ??
		params?.coercionPolicy ??
		(params?.coercion && typeof params.coercion === 'object' ? (params.coercion as any).policy : undefined) ??
		'safe_widening';
	const policy =
		String(policyRaw ?? 'safe_widening').trim().toLowerCase() === 'allow_lossy'
			? 'allow_lossy'
			: String(policyRaw ?? 'safe_widening').trim().toLowerCase() === 'strict' ||
				  String(policyRaw ?? 'safe_widening').trim().toLowerCase() === 'forbid'
				? 'strict'
				: 'safe_widening';
	return {
		...payload,
		coercion_policy: policy
	};
}

export function computeEdgeSchemaConstraintsInternal(
	nodes: Node<PipelineNodeData>[],
	edges: Edge<PipelineEdgeData>[]
): Record<string, EdgeSchemaConstraint> {
	const byNodeId = new Map(nodes.map((n) => [n.id, n]));
	const out: Record<string, EdgeSchemaConstraint> = {};
	for (const edge of edges) {
		const edgeId = String(edge.id ?? '');
		if (!edgeId) continue;
		const sourceNodeId = String(edge.source ?? '');
		const targetNodeId = String(edge.target ?? '');
		const sourceNode = byNodeId.get(sourceNodeId);
		const targetNode = byNodeId.get(targetNodeId);
		const sourceHandle = String((edge as any)?.sourceHandle ?? 'out').trim() || 'out';
		const targetHandle = String((edge as any)?.targetHandle ?? 'in').trim() || 'in';
		const mode = normalizeEdgeMode(edge as Edge<PipelineEdgeData>);
		if (!sourceNode || !targetNode) continue;
		const sourceAffinity = nodePortAffinity(sourceNode as any, 'out', sourceHandle);
		const targetAffinity = nodePortAffinity(targetNode as any, 'in', targetHandle);
		const providedSchema = buildProvidedSchema(sourceNode as any, sourceHandle);
		const requiredSchema = buildRequiredSchema(targetNode as any, targetHandle);
		const existingContract = ((edge.data ?? {}) as Record<string, any>)?.contract ?? {};
		const snapshot =
			existingContract && typeof existingContract.snapshot === 'object'
				? (existingContract.snapshot as Record<string, any>)
				: {};
		const snapshotSourceSchemaFingerprint = String(snapshot?.sourceSchemaFingerprint ?? '').trim() || undefined;
		const snapshotTargetSchemaFingerprint = String(snapshot?.targetSchemaFingerprint ?? '').trim() || undefined;
		const currentSourceSchemaFingerprint = stableSchemaSignature(providedSchema ?? null);
		const currentTargetSchemaFingerprint = stableSchemaSignature(requiredSchema ?? null);
		const snapshotDrift = Boolean(
			snapshotSourceSchemaFingerprint &&
				snapshotTargetSchemaFingerprint &&
				(snapshotSourceSchemaFingerprint !== currentSourceSchemaFingerprint ||
					snapshotTargetSchemaFingerprint !== currentTargetSchemaFingerprint)
		);
		const check = isSchemaCompatible(providedSchema, requiredSchema, mode);
		out[edgeId] = {
			edgeId,
			mode,
			sourceNodeId,
			targetNodeId,
			sourceHandle,
			targetHandle,
			sourceAffinity,
			targetAffinity,
			providedSchema,
			requiredSchema,
			compatible: check.ok,
			warning: check.ok ? check.warning : undefined,
			adapterKind: check.adapterKind ?? null,
			reason: check.ok ? undefined : check.reason,
			missingColumns: check.ok ? undefined : check.missingColumns,
			snapshotSourceSchemaFingerprint,
			snapshotTargetSchemaFingerprint,
			currentSourceSchemaFingerprint,
			currentTargetSchemaFingerprint,
			snapshotDrift,
			suggestions:
				check.ok
					? check.warning && check.suggestion
						? [check.suggestion]
						: []
					: check.reason === 'type_mismatch'
						? check.suggestion
							? [check.suggestion]
							: []
						: []
		};
	}
	return out;
}

export function __computeEdgeSchemaConstraintsForTest(
	nodes: Node<PipelineNodeData>[],
	edges: Edge<PipelineEdgeData>[]
): Record<string, EdgeSchemaConstraint> {
	return computeEdgeSchemaConstraintsInternal(nodes, edges);
}

export function computeEdgeSchemaDiagnosticsInternal(
	constraints: Record<string, EdgeSchemaConstraint>
): Record<string, EdgeSchemaDiagnostic | null> {
	const out: Record<string, EdgeSchemaDiagnostic | null> = {};
	for (const [edgeId, constraint] of Object.entries(constraints ?? {})) {
		const modeLabel =
			constraint.mode === 'param'
				? 'Param shape mismatch'
				: constraint.mode === 'control'
					? 'Control contract mismatch'
					: 'Work payload mismatch';
		if (constraint.compatible) {
			if (constraint.warning === 'lossy_coercion') {
				out[edgeId] = {
					edgeId,
					code: 'TYPE_MISMATCH',
					severity: 'warning',
					message: `${modeLabel}: lossy coercion ${String(constraint.providedSchema?.type ?? 'unknown')} -> ${String(constraint.requiredSchema?.type ?? 'unknown')}`,
					details: {
						providedSchema: constraint.providedSchema,
						requiredSchema: constraint.requiredSchema,
						targetHandle: constraint.targetHandle,
						sourceHandle: constraint.sourceHandle,
						sourceNodeId: constraint.sourceNodeId,
						targetNodeId: constraint.targetNodeId,
						mode: constraint.mode,
						sourceAffinity: constraint.sourceAffinity,
						targetAffinity: constraint.targetAffinity,
						snapshotSourceSchemaFingerprint: constraint.snapshotSourceSchemaFingerprint,
						snapshotTargetSchemaFingerprint: constraint.snapshotTargetSchemaFingerprint,
						currentSourceSchemaFingerprint: constraint.currentSourceSchemaFingerprint,
						currentTargetSchemaFingerprint: constraint.currentTargetSchemaFingerprint,
						snapshotDrift: constraint.snapshotDrift
					},
					suggestions: constraint.suggestions ?? []
				};
				continue;
			}
			if (constraint.snapshotDrift) {
				out[edgeId] = {
					edgeId,
					code: 'TYPE_MISMATCH',
					severity: 'warning',
					message: `${modeLabel}: edge contract snapshot drift detected (current schema no longer matches persisted snapshot)`,
					details: {
						providedSchema: constraint.providedSchema,
						requiredSchema: constraint.requiredSchema,
						targetHandle: constraint.targetHandle,
						sourceHandle: constraint.sourceHandle,
						sourceNodeId: constraint.sourceNodeId,
						targetNodeId: constraint.targetNodeId,
						mode: constraint.mode,
						sourceAffinity: constraint.sourceAffinity,
						targetAffinity: constraint.targetAffinity,
						snapshotSourceSchemaFingerprint: constraint.snapshotSourceSchemaFingerprint,
						snapshotTargetSchemaFingerprint: constraint.snapshotTargetSchemaFingerprint,
						currentSourceSchemaFingerprint: constraint.currentSourceSchemaFingerprint,
						currentTargetSchemaFingerprint: constraint.currentTargetSchemaFingerprint,
						snapshotDrift: true
					},
					suggestions: [
						'Refresh edge contract snapshot from current schemas.',
						'If this drift is intentional, accept coercion or insert an adapter before target.',
						'If drift is accidental, restore expected input/output schemas and rebind the edge.'
					]
				};
				continue;
			}
			out[edgeId] = null;
			continue;
		}
		if (constraint.reason === 'missing_required_columns') {
			out[edgeId] = {
				edgeId,
				code: 'PAYLOAD_SCHEMA_MISMATCH',
				severity: 'error',
				message: `${modeLabel}: missing required columns ${(constraint.missingColumns ?? []).join(', ') || '(unknown)'}`,
				details: {
					providedSchema: constraint.providedSchema,
					requiredSchema: constraint.requiredSchema,
					missingColumns: constraint.missingColumns,
					targetHandle: constraint.targetHandle,
					sourceHandle: constraint.sourceHandle,
					sourceNodeId: constraint.sourceNodeId,
					targetNodeId: constraint.targetNodeId,
					mode: constraint.mode,
					sourceAffinity: constraint.sourceAffinity,
					targetAffinity: constraint.targetAffinity,
					snapshotSourceSchemaFingerprint: constraint.snapshotSourceSchemaFingerprint,
					snapshotTargetSchemaFingerprint: constraint.snapshotTargetSchemaFingerprint,
					currentSourceSchemaFingerprint: constraint.currentSourceSchemaFingerprint,
					currentTargetSchemaFingerprint: constraint.currentTargetSchemaFingerprint,
					snapshotDrift: constraint.snapshotDrift
				},
				suggestions: constraint.suggestions ?? []
			};
			continue;
		}
		if (constraint.reason === 'missing_typed_schema') {
			out[edgeId] = {
				edgeId,
				code: 'PAYLOAD_SCHEMA_MISMATCH',
				severity: 'error',
				message: `${modeLabel}: required typed schema coverage is missing. Required columns: ${(constraint.missingColumns ?? []).join(', ') || '(unknown)'}`,
				details: {
					providedSchema: constraint.providedSchema,
					requiredSchema: constraint.requiredSchema,
					missingColumns: constraint.missingColumns,
					targetHandle: constraint.targetHandle,
					sourceHandle: constraint.sourceHandle,
					sourceNodeId: constraint.sourceNodeId,
					targetNodeId: constraint.targetNodeId,
					mode: constraint.mode,
					sourceAffinity: constraint.sourceAffinity,
					targetAffinity: constraint.targetAffinity,
					snapshotSourceSchemaFingerprint: constraint.snapshotSourceSchemaFingerprint,
					snapshotTargetSchemaFingerprint: constraint.snapshotTargetSchemaFingerprint,
					currentSourceSchemaFingerprint: constraint.currentSourceSchemaFingerprint,
					currentTargetSchemaFingerprint: constraint.currentTargetSchemaFingerprint,
					snapshotDrift: constraint.snapshotDrift
				},
				suggestions: constraint.suggestions ?? []
			};
			continue;
		}
		out[edgeId] = {
			edgeId,
			code: 'TYPE_MISMATCH',
			severity: 'error',
			message: `${modeLabel}: incompatible schema types ${String(constraint.providedSchema?.type ?? 'unknown')} -> ${String(constraint.requiredSchema?.type ?? 'unknown')}`,
			details: {
				providedSchema: constraint.providedSchema,
				requiredSchema: constraint.requiredSchema,
				targetHandle: constraint.targetHandle,
				sourceHandle: constraint.sourceHandle,
				sourceNodeId: constraint.sourceNodeId,
				targetNodeId: constraint.targetNodeId,
				mode: constraint.mode,
				sourceAffinity: constraint.sourceAffinity,
				targetAffinity: constraint.targetAffinity,
				snapshotSourceSchemaFingerprint: constraint.snapshotSourceSchemaFingerprint,
				snapshotTargetSchemaFingerprint: constraint.snapshotTargetSchemaFingerprint,
				currentSourceSchemaFingerprint: constraint.currentSourceSchemaFingerprint,
				currentTargetSchemaFingerprint: constraint.currentTargetSchemaFingerprint,
				snapshotDrift: constraint.snapshotDrift
			},
			suggestions: constraint.suggestions ?? []
		};
	}
	return out;
}

export function __computeEdgeSchemaDiagnosticsForTest(
	constraints: Record<string, EdgeSchemaConstraint>
): Record<string, EdgeSchemaDiagnostic | null> {
	return computeEdgeSchemaDiagnosticsInternal(constraints);
}

export function normalizeHintPayloadType(raw: unknown): PayloadType | undefined {
	const t = normalizeHintType(raw);
	return isPayloadType(t) ? t : undefined;
}

export function inferPortAffinityFromHandle(handle: unknown, direction: 'in' | 'out'): 'work' | 'param' | 'control' {
	const raw = String(handle ?? (direction === 'in' ? 'in' : 'out'))
		.trim()
		.toLowerCase();
	if (raw.startsWith('param')) return 'param';
	if (raw.startsWith('control') || raw.startsWith('ctl')) return 'control';
	return 'work';
}

export function nodePortAffinity(
	node: Node<PipelineNodeData>,
	direction: 'in' | 'out',
	handle: unknown
): 'work' | 'param' | 'control' {
	const inferred = inferPortAffinityFromHandle(handle, direction);
	if (inferred !== 'work') return inferred;
	const data = (node.data ?? {}) as any;
	const declared = data?.portDeclarations?.[direction];
	const ports = declared && typeof declared === 'object' ? declared : data?.portContracts?.[direction];
	const key = String(handle ?? 'default').trim() || 'default';
	const exact = ports && typeof ports === 'object' ? ports[key] : undefined;
	const fallback = ports && typeof ports === 'object' ? ports.default : undefined;
	const affinity = String((exact ?? fallback ?? {}).plane ?? (exact ?? fallback ?? {}).affinity ?? '')
		.trim()
		.toLowerCase();
	if (affinity === 'work' || affinity === 'param' || affinity === 'control') {
		return affinity;
	}
	return inferred;
}

export function declaredPortHandles(
	node: Node<PipelineNodeData>,
	direction: 'in' | 'out'
): string[] {
	const data = (node.data ?? {}) as any;
	const declared =
		data?.portDeclarations?.[direction] && typeof data?.portDeclarations?.[direction] === 'object'
			? data.portDeclarations[direction]
			: null;
	const ports = declared ?? data?.portContracts?.[direction];
	if (!ports || typeof ports !== 'object') return [];
	const raw = Object.keys(ports as Record<string, unknown>)
		.map((value) => String(value ?? '').trim())
		.filter((value) => value.length > 0 && value !== 'default');
	if (direction === 'in') {
		const expectedInputSchemas =
			data?.schema?.expectedInputSchemas && typeof data.schema.expectedInputSchemas === 'object'
				? (data.schema.expectedInputSchemas as Record<string, unknown>)
				: {};
		for (const handle of Object.keys(expectedInputSchemas)) {
			const key = String(handle ?? '').trim();
			if (key.length > 0 && !raw.includes(key)) raw.push(key);
		}
	}
	if (declared && typeof declared === 'object' && Object.prototype.hasOwnProperty.call(declared, 'default')) {
		const implicit = direction === 'in' ? 'in' : 'out';
		if (!raw.includes(implicit)) raw.unshift(implicit);
	}
	return raw;
}

export function hasPortHandle(
	node: Node<PipelineNodeData>,
	direction: 'in' | 'out',
	handle: string
): boolean {
	const data = (node.data ?? {}) as any;
	const declaredPorts =
		data?.portDeclarations?.[direction] && typeof data?.portDeclarations?.[direction] === 'object'
			? data.portDeclarations[direction]
			: null;
	if (declaredPorts && typeof declaredPorts === 'object') {
		const normalized = String(handle ?? '').trim();
		const record = declaredPorts as Record<string, unknown>;
		if (direction === 'in') {
			const expectedInputSchemas =
				data?.schema?.expectedInputSchemas && typeof data.schema.expectedInputSchemas === 'object'
					? (data.schema.expectedInputSchemas as Record<string, unknown>)
					: {};
			if (normalized.length > 0 && Object.prototype.hasOwnProperty.call(expectedInputSchemas, normalized)) {
				return true;
			}
		}
		if (normalized.length === 0) return (direction === 'in' ? 'in' : 'out') in record;
		return normalized in record;
	}
	const ports = data?.portContracts?.[direction];
	if (!ports || typeof ports !== 'object') return true;
	const normalized = String(handle ?? '').trim();
	const record = ports as Record<string, unknown>;
	if (normalized.length === 0) return 'default' in record || 'in' in record || 'out' in record;
	return normalized in record || 'default' in record;
}

export function portCardinality(
	node: Node<PipelineNodeData>,
	direction: 'in' | 'out',
	handle: string
): 'one' | 'many' {
	const data = (node.data ?? {}) as any;
	const ports =
		data?.portDeclarations?.[direction] && typeof data?.portDeclarations?.[direction] === 'object'
			? data.portDeclarations[direction]
			: null;
	if (!ports || typeof ports !== 'object') return 'many';
	const key = String(handle ?? '').trim() || (direction === 'in' ? 'in' : 'out');
	const exact = (ports as Record<string, any>)[key];
	const fallback = (ports as Record<string, any>).default;
	const cardinality = String((exact ?? fallback ?? {}).cardinality ?? 'many').trim().toLowerCase();
	return cardinality === 'one' ? 'one' : 'many';
}

export function edgeModeCompatible(
	mode: string,
	sourceAffinity: 'work' | 'param' | 'control',
	targetAffinity: 'work' | 'param' | 'control'
): boolean {
	const m = String(mode || 'work')
		.trim()
		.toLowerCase();
	if (m === 'work') return sourceAffinity === 'work' && targetAffinity === 'work';
	if (m === 'param') return (sourceAffinity === 'work' || sourceAffinity === 'param') && targetAffinity === 'param';
	if (m === 'control') return sourceAffinity === 'control' && targetAffinity === 'control';
	return false;
}

export function inferEdgeModeFromHandles(edge: Edge<PipelineEdgeData>): 'work' | 'param' | 'control' {
	const sourceHandle = String((edge as any)?.sourceHandle ?? '').trim().toLowerCase();
	const targetHandle = String((edge as any)?.targetHandle ?? '').trim().toLowerCase();
	if (
		sourceHandle.startsWith('control') ||
		sourceHandle.startsWith('ctl') ||
		targetHandle.startsWith('control') ||
		targetHandle.startsWith('ctl')
	) {
		return 'control';
	}
	if (sourceHandle.startsWith('param') || targetHandle.startsWith('param')) {
		return 'param';
	}
	return 'work';
}

export function normalizeEdgeLinkKind(edge: Edge<PipelineEdgeData>): 'data_link' | 'control_link' {
	const rawKind = String((edge.data as any)?.linkKind ?? (edge.data as any)?.link_kind ?? '')
		.trim()
		.toLowerCase();
	return rawKind === 'control_link' ? 'control_link' : 'data_link';
}

export function normalizeEdgeMode(edge: Edge<PipelineEdgeData>): 'work' | 'param' | 'control' {
	if (normalizeEdgeLinkKind(edge) === 'control_link') return 'control';
	const rawMode = String((edge.data as any)?.mode ?? '').trim().toLowerCase();
	if (rawMode === 'work' || rawMode === 'param' || rawMode === 'control') {
		return rawMode;
	}
	return inferEdgeModeFromHandles(edge);
}

export function stableSchemaSignature(value: unknown): string {
	const normalize = (input: unknown): unknown => {
		if (Array.isArray(input)) return input.map((item) => normalize(item));
		if (input && typeof input === 'object') {
			const out: Record<string, unknown> = {};
			for (const key of Object.keys(input as Record<string, unknown>).sort()) {
				out[key] = normalize((input as Record<string, unknown>)[key]);
			}
			return out;
		}
		return input;
	};
	try {
		return JSON.stringify(normalize(value ?? null));
	} catch {
		return '';
	}
}

export function edgeContractSnapshotFromSchemas(
	providedSchema: Record<string, any> | undefined,
	requiredSchema: Record<string, any> | undefined,
	compatibility: SchemaCompatibility,
	edgeMode: 'work' | 'param' | 'control'
): Record<string, any> {
	const providedType = normalizeHintType(providedSchema?.type ?? 'unknown');
	const requiredType = normalizeHintType(requiredSchema?.type ?? 'unknown');
	const policy = String(requiredSchema?.coercion_policy ?? 'safe_widening').trim().toLowerCase();
	const coercion = evaluateSchemaCoercion(providedType, requiredType, policy);
	let decision: 'native' | 'coerced' | 'adapter' | 'incompatible' = 'incompatible';
	if (compatibility.ok) {
		decision = coercion.mode === 'native' ? 'native' : 'coerced';
		if (compatibility.adapterKind) decision = 'adapter';
	}
	return {
		edgeMode,
		sourceSchemaFingerprint: stableSchemaSignature(providedSchema ?? null),
		targetSchemaFingerprint: stableSchemaSignature(requiredSchema ?? null),
		compatible: compatibility.ok,
		decision,
		coercion: {
			allowed: coercion.allowed,
			lossy: coercion.lossy,
			mode: coercion.mode
		},
		updatedAt: new Date().toISOString()
	};
}

export function sameHandleProvidedSchemaConflict(
	nodes: Node<PipelineNodeData>[],
	edges: Edge<PipelineEdgeData>[],
	candidate: Edge<PipelineEdgeData>
): { conflict: true; targetNodeId: string; targetHandle: string; edgeIds: string[] } | { conflict: false } {
	const combined = [...edges, candidate];
	const byNodeId = new Map(nodes.map((node) => [node.id, node]));
	const signaturesByHandle = new Map<string, Map<string, string[]>>();
	for (const edge of combined) {
		if (normalizeEdgeMode(edge) !== 'work') continue;
		const sourceNode = byNodeId.get(String(edge.source ?? ''));
		const targetNode = byNodeId.get(String(edge.target ?? ''));
		if (!sourceNode || !targetNode) continue;
		const sourceHandle = String((edge as any)?.sourceHandle ?? 'out').trim() || 'out';
		const targetHandle = String((edge as any)?.targetHandle ?? 'in').trim() || 'in';
		const key = `${String(edge.target ?? '')}::${targetHandle}`;
		const payload = buildProvidedSchema(sourceNode as any, sourceHandle);
		const signature = stableSchemaSignature(payload);
		if (!signaturesByHandle.has(key)) signaturesByHandle.set(key, new Map());
		const bySignature = signaturesByHandle.get(key)!;
		if (!bySignature.has(signature)) bySignature.set(signature, []);
		bySignature.get(signature)!.push(String(edge.id ?? ''));
	}
	for (const [key, bySignature] of signaturesByHandle.entries()) {
		if (bySignature.size <= 1) continue;
		const [targetNodeId, targetHandle = 'in'] = key.split('::');
		const edgeIds = Array.from(bySignature.values()).flat().filter((value) => value.length > 0);
		return { conflict: true, targetNodeId, targetHandle, edgeIds };
	}
	return { conflict: false };
}

export function isEdgeStillValid(nodes: Node<PipelineNodeData>[], e: Edge<PipelineEdgeData>): EdgeCheck {
	const sourceNode = nodes.find((n) => n.id === e.source);
	const targetNode = nodes.find((n) => n.id === e.target);
	if (!sourceNode || !targetNode) {
		return { ok: false, reason: 'typed_schema_missing' };
	}
	const mode = normalizeEdgeMode(e);
	const sourceHandle = String((e as any).sourceHandle ?? 'out');
	const targetHandle = String((e as any).targetHandle ?? 'in');
	if (!hasPortHandle(targetNode, 'in', targetHandle)) {
		return { ok: false, reason: 'typed_schema_missing' };
	}
	const sourceAffinity = nodePortAffinity(sourceNode, 'out', sourceHandle);
	const targetAffinity = nodePortAffinity(targetNode, 'in', targetHandle);
	if (!edgeModeCompatible(mode, sourceAffinity, targetAffinity)) {
		return { ok: false, reason: 'mode_mismatch' };
	}
	const sourcePayload = sourceNode ? buildProvidedSchema(sourceNode as any, sourceHandle) : undefined;
	const targetPayload = targetNode ? buildRequiredSchema(targetNode as any, targetHandle) : undefined;
	if (!sourcePayload || !targetPayload) {
		return { ok: false, reason: 'typed_schema_missing' };
	}
	const schemaCheck = isSchemaCompatible(sourcePayload as any, targetPayload as any, mode);
	if (!schemaCheck.ok) {
		if (schemaCheck.reason === 'missing_typed_schema') {
			return {
				ok: false,
				reason: 'typed_schema_missing',
				missingColumns: schemaCheck.missingColumns
			};
		}
		if (schemaCheck.reason === 'missing_required_columns') {
			return {
				ok: false,
				reason: 'schema_mismatch',
				missingColumns: schemaCheck.missingColumns
			};
		}
		return {
			ok: false,
			reason: 'type_mismatch',
			suggestion: schemaCheck.suggestion,
			adapterKind: schemaCheck.adapterKind
		};
	}

	return {
		ok: true,
		out: normalizeHintPayloadType((sourcePayload as any)?.type ?? null) ?? undefined,
		in: normalizeHintPayloadType((targetPayload as any)?.type ?? null) ?? undefined
	};
}
export function buildNodeSchemaContractSnapshotInternal(
	state: GraphState,
	nodeIdRaw: string
): NodeSchemaContractSnapshot {
	const nodeId = String(nodeIdRaw ?? '').trim();
	if (!nodeId) return { nodeId: '', status: 'clean', edges: [] };
	const constraints = computeEdgeSchemaConstraintsInternal(state.nodes as any, state.edges as any);
	const diagnostics = computeEdgeSchemaDiagnosticsInternal(constraints as any);
	const edges: NodeSchemaContractEdge[] = [];
	for (const edge of state.edges ?? []) {
		const edgeId = String(edge.id ?? '');
		if (!edgeId) continue;
		if (String(edge.source ?? '') !== nodeId && String(edge.target ?? '') !== nodeId) continue;
		const constraint = constraints[edgeId];
		if (!constraint) continue;
		const diag = diagnostics[edgeId];
		const severity: 'clean' | 'warning' | 'error' =
			diag?.severity === 'error' ? 'error' : diag?.severity === 'warning' ? 'warning' : 'clean';
		edges.push({
			edgeId,
			mode: constraint.mode,
			direction: String(edge.target ?? '') === nodeId ? 'incoming' : 'outgoing',
			sourceNodeId: String(edge.source ?? ''),
			targetNodeId: String(edge.target ?? ''),
			sourceHandle: String((edge as any).sourceHandle ?? '').trim() || null,
			targetHandle: String((edge as any).targetHandle ?? '').trim() || null,
			providedSchema: constraint.providedSchema,
			requiredSchema: constraint.requiredSchema,
			severity,
			snapshotDrift: constraint.snapshotDrift,
			snapshotSourceSchemaFingerprint: constraint.snapshotSourceSchemaFingerprint,
			snapshotTargetSchemaFingerprint: constraint.snapshotTargetSchemaFingerprint,
			currentSourceSchemaFingerprint: constraint.currentSourceSchemaFingerprint,
			currentTargetSchemaFingerprint: constraint.currentTargetSchemaFingerprint,
			suggestions: constraint.suggestions ?? [],
			adapterKind: constraint.adapterKind ?? null
		});
	}
	const status: 'clean' | 'warning' | 'error' = edges.some((edge) => edge.severity === 'error')
		? 'error'
		: edges.some((edge) => edge.severity === 'warning')
			? 'warning'
			: 'clean';
	return { nodeId, status, edges };
}

export function __buildNodeSchemaContractSnapshotForTest(
	state: GraphState,
	nodeId: string
): NodeSchemaContractSnapshot {
	return buildNodeSchemaContractSnapshotInternal(state, nodeId);
}
