// src/lib/flow/store/graphStore.inspector.ts
//
// Inspector / node-config editing helpers.
// Pure helpers are module-level exports; store-aware operations live inside
// the createInspectorManager factory so they can be tested in isolation.

import type { Node, Edge } from '@xyflow/svelte';
import type { PipelineNodeData, PipelineEdgeData } from '$lib/flow/types';
import { isPayloadType } from '$lib/flow/types/base';
import { updateNodeParamsValidated } from './graph';
import {
	getLlmEditorCommitMode,
	getSourceEditorCommitMode,
	getToolEditorCommitMode,
	getTransformEditorCommitMode
} from '$lib/flow/editorCommitPolicy';
import { NodeSchemaEnvelopeSchema } from '$lib/flow/schema/schemaContract';
import { stableJson, logPush } from './graphStore.audit';
import {
	normalizeComponentPayloadType,
	deriveNodeIoForData,
	payloadHintToTypedSchema,
	fingerprintTypedSchema,
	hasSchemaEnvelopeContent
} from './graphStore.node-schema';
import type {
	GraphState,
	AuditContext,
	ApiEditorUiState,
	InspectorDraftAcceptValidation,
	InspectorDraftPatchIntent,
	SavePreflightDiagnostic,
	NormalizedNodeBinding,
	NodeOutputInfo
} from './graphStore.types';

// ---------------------------------------------------------------------------
// Module-level pure helpers
// ---------------------------------------------------------------------------

export function hasAnyKeys(value: unknown): boolean {
	return Boolean(value && typeof value === 'object' && Object.keys(value as Record<string, unknown>).length > 0);
}

export function defaultApiEditorUiState(params?: Record<string, any>): ApiEditorUiState {
	const authType = String(params?.auth_type ?? 'none');
	const bodyMode = String(params?.bodyMode ?? params?.body_mode ?? 'none');
	return {
		requestOpen: true,
		authOpen: authType !== 'none',
		transportOpen: false,
		executionOpen: false,
		debugOpen: false,
		queryOpen: hasAnyKeys(params?.query),
		headersOpen: hasAnyKeys(params?.headers),
		bodyOpen: bodyMode !== 'none'
	};
}

export function sanitizeComponentDraftParams(params: Record<string, any>): Record<string, any> {
	const api = params?.api;
	const exposureRegistry = Array.isArray(params?.exposureRegistry) ? (params.exposureRegistry as any[]) : null;
	if (!api || typeof api !== 'object' || !Array.isArray(exposureRegistry)) {
		return params;
	}
	const outputs = Array.isArray((api as any).outputs) ? ((api as any).outputs as any[]) : [];
	const validOutputNames = new Set(
		outputs
			.map((out) => String((out as any)?.name ?? '').trim())
			.filter((name) => name.length > 0)
	);
	const nextExposureRegistry = exposureRegistry.filter((rec) => {
		if (!rec || typeof rec !== 'object') return false;
		if (String((rec as any).kind ?? '').trim().toLowerCase() !== 'data_output') return true;
		const alias = String((rec as any).alias ?? '').trim();
		const handleId = String((rec as any).handle_id ?? '').trim();
		if (alias && validOutputNames.has(alias)) return true;
		if (handleId.startsWith('data_out::')) {
			const outName = handleId.slice('data_out::'.length).trim();
			return outName.length > 0 && validOutputNames.has(outName);
		}
		return false;
	});
	return {
		...params,
		exposureRegistry: nextExposureRegistry
	};
}

export function validateComponentDraftForAccept(params: Record<string, any>): { ok: true } | { ok: false; errors: string[] } {
	const api = params?.api;
	const outputs = Array.isArray(api?.outputs) ? (api.outputs as any[]) : [];
	const exposureRegistry = Array.isArray(params?.exposureRegistry) ? (params.exposureRegistry as any[]) : [];
	const errors: string[] = [];
	const seenOutputNames = new Set<string>();
	for (const output of outputs) {
		const outputName = String(output?.name ?? '').trim();
		if (!outputName) {
			errors.push('Component output name is required before Accept.');
			continue;
		}
		const outputNameKey = outputName.toLowerCase();
		if (seenOutputNames.has(outputNameKey)) {
			errors.push(`Component output "${outputName}" duplicates another declared output.`);
			continue;
		}
		seenOutputNames.add(outputNameKey);
		const exposure = exposureRegistry.find(
			(rec) =>
				rec &&
				typeof rec === 'object' &&
				String((rec as any).kind ?? '').trim().toLowerCase() === 'data_output' &&
				(
					String((rec as any).alias ?? '').trim() === outputName ||
					String((rec as any).handle_id ?? '').trim() === `data_out::${outputName}`
				)
		);
		const internalSourcePath = String((exposure as any)?.internal_source_path ?? '').trim();
		const isRequired = Boolean((output as any)?.required ?? true);
		if (isRequired && !internalSourcePath) {
			errors.push(`Component output "${outputName}" requires an API Contract output source before Accept.`);
		}
		const typedSchemaType = normalizeComponentPayloadType(output?.typedSchema?.type);
		if (typedSchemaType == null) {
			errors.push(`Component output "${outputName}" must declare typedSchema.type.`);
		}
	}
	if (errors.length > 0) return { ok: false, errors };
	return { ok: true };
}

export function validateInspectorDraftForAccept(state: GraphState): InspectorDraftAcceptValidation {
	const nodeId = state.inspector.nodeId;
	if (!nodeId) return { ok: false, errors: ['No node selected.'] };
	const node = state.nodes.find((n) => n.id === nodeId);
	if (!node) return { ok: false, errors: ['Selected node no longer exists.'] };
	if (node.data.kind !== 'component') return { ok: true, errors: [] };
	const paramsForCommit = sanitizeComponentDraftParams(
		(state.inspector.draftParams ?? {}) as Record<string, any>
	);
	const validation = validateComponentDraftForAccept(paramsForCommit);
	if (!validation.ok) return { ok: false, errors: validation.errors };
	return { ok: true, errors: [] };
}

export function listComponentOutputNames(node: Node<PipelineNodeData>): string[] {
	if (node.data.kind !== 'component') return [];
	const outputs = Array.isArray((node.data as any)?.params?.api?.outputs)
		? ((node.data as any).params.api.outputs as any[])
		: [];
	return outputs
		.map((o) => String((o as any)?.name ?? '').trim())
		.filter((name): name is string => name.length > 0);
}

export function normalizeHandleId(handleId: string | null | undefined, fallback: 'in' | 'out'): string {
	const v = String(handleId ?? '').trim();
	return v ? v : fallback;
}

export function canonicalComponentSourceHandleForEdge(
	nodes: Node<PipelineNodeData>[],
	edge: Edge<PipelineEdgeData>
): string | null {
	const sourceNode = nodes.find((n) => n.id === edge.source);
	if (!sourceNode || sourceNode.data.kind !== 'component') {
		return normalizeHandleId((edge as any).sourceHandle, 'out');
	}
	const outputNames = listComponentOutputNames(sourceNode);
	if (outputNames.length === 0) return null;
	const raw = String((edge as any).sourceHandle ?? '').trim();
	if (!raw || raw === 'out') {
		return outputNames.length === 1 ? outputNames[0] : null;
	}
	return outputNames.includes(raw) ? raw : null;
}

export function dedupeEdgesBySignature(
	edges: Edge<PipelineEdgeData>[]
): { edges: Edge<PipelineEdgeData>[]; removedIds: string[] } {
	const seen = new Set<string>();
	const next: Edge<PipelineEdgeData>[] = [];
	const removedIds: string[] = [];
	for (const edge of edges) {
		const key = [
			String(edge.source ?? ''),
			String((edge as any).sourceHandle ?? 'out'),
			String(edge.target ?? ''),
			String((edge as any).targetHandle ?? 'in')
		].join('|');
		if (seen.has(key)) {
			removedIds.push(String(edge.id ?? ''));
			continue;
		}
		seen.add(key);
		next.push(edge);
	}
	return { edges: next, removedIds };
}

export function reconcileComponentOutgoingEdges(
	nodeId: string,
	nextNode: Node<PipelineNodeData>,
	edges: Edge<PipelineEdgeData>[],
	previousOutputNames: string[]
): { edges: Edge<PipelineEdgeData>[]; removedIds: string[] } {
	if (nextNode.data.kind !== 'component') return { edges, removedIds: [] };
	const nextOutputNames = listComponentOutputNames(nextNode);
	const nextSet = new Set(nextOutputNames);
	const renameMap = new Map<string, string>();
	for (let i = 0; i < Math.min(previousOutputNames.length, nextOutputNames.length); i += 1) {
		const prevName = String(previousOutputNames[i] ?? '').trim();
		const nextName = String(nextOutputNames[i] ?? '').trim();
		if (!prevName || !nextName || prevName === nextName) continue;
		if (nextSet.has(prevName)) continue;
		renameMap.set(prevName, nextName);
	}

	const rewritten = edges
		.map((edge) => {
			if (edge.source !== nodeId) return edge;
			const rawHandle = String((edge as any).sourceHandle ?? '').trim();
			if (!rawHandle || rawHandle === 'out') {
				if (nextOutputNames.length === 1) {
					return { ...edge, sourceHandle: nextOutputNames[0] };
				}
				return edge;
			}
			if (nextSet.has(rawHandle)) return edge;
			const mapped = renameMap.get(rawHandle);
			if (mapped && nextSet.has(mapped)) {
				return { ...edge, sourceHandle: mapped };
			}
			if (nextOutputNames.length === 1) {
				return { ...edge, sourceHandle: nextOutputNames[0] };
			}
			return null;
		})
		.filter((edge): edge is Edge<PipelineEdgeData> => Boolean(edge));

	const removedIds = edges
		.filter((edge) => edge.source === nodeId)
		.filter((edge) => !rewritten.some((candidate) => candidate.id === edge.id))
		.map((edge) => String(edge.id ?? ''));

	const deduped = dedupeEdgesBySignature(rewritten);
	return { edges: deduped.edges, removedIds: [...removedIds, ...deduped.removedIds] };
}

export function effectiveExecParamsForNode(node: Node<PipelineNodeData> | undefined): Record<string, unknown> {
	const raw = { ...(node?.data?.params ?? {}) } as Record<string, unknown>;
	for (const key of [
		'recentSnapshotIds',
		'recent_snapshot_ids',
		'snapshotMetadata',
		'snapshot_metadata',
		'recentSnapshots',
		'snapshotHistory'
	]) {
		delete raw[key];
	}
	return raw;
}

export function committedNodeParamsForNode(
	state: GraphState,
	nodeId: string
): Record<string, any> {
	const node = state.nodes.find((x) => x.id === nodeId);
	return { ...((node?.data?.params ?? {}) as Record<string, any>) };
}

export function nodeFreezeMode(
	node: Node<PipelineNodeData & Record<string, unknown>> | undefined | null
): 'per_run' | 'sticky' | null {
	const freeze = (node?.data as any)?.meta?.freeze;
	if (!freeze || typeof freeze !== 'object') return null;
	if (freeze.enabled !== true) return null;
	const mode = String(freeze.mode ?? '').trim().toLowerCase();
	if (mode === 'per_run' || mode === 'sticky') return mode;
	return null;
}

export function pendingInspectorDraftSaveDiagnostic(state: GraphState): SavePreflightDiagnostic | null {
	if (!Boolean(state?.inspector?.dirty)) return null;
	const inspectorNodeId = String(state?.inspector?.nodeId ?? '').trim();
	if (!inspectorNodeId) return null;
	const node = state.nodes.find((n) => String(n?.id ?? '') === inspectorNodeId);
	if (!node) return null;
	const draftParams = (state.inspector?.draftParams ?? {}) as Record<string, any>;
	const nodeKind = node.data.kind;
	let editorMode: 'draft' | 'immediate' = 'immediate';
	let detail = '';
	if (nodeKind === 'transform') {
		const transformKind = String(
			draftParams?.transformKind ??
				(node?.data as any)?.transformKind ??
				draftParams?.op ??
				(node?.data as any)?.params?.op ??
				'select'
		).trim();
		editorMode = getTransformEditorCommitMode(transformKind);
		detail = `"${transformKind}"`;
	} else if (nodeKind === 'source') {
		const sourceKind = String(draftParams?.sourceKind ?? (node?.data as any)?.sourceKind ?? 'file').trim();
		editorMode = getSourceEditorCommitMode(sourceKind);
		detail = `"${sourceKind}"`;
	} else if (nodeKind === 'llm' || nodeKind === 'model') {
		const llmKind = String(draftParams?.llmKind ?? (node?.data as any)?.llmKind ?? 'ollama').trim();
		editorMode = getLlmEditorCommitMode(llmKind);
		detail = `"${llmKind}"`;
	} else if (nodeKind === 'tool') {
		const provider = String(draftParams?.provider ?? (node?.data as any)?.params?.provider ?? 'mcp').trim();
		editorMode = getToolEditorCommitMode(provider);
		detail = `"${provider}"`;
	} else {
		return null;
	}
	if (editorMode !== 'draft') return null;
	return {
		code: 'INSPECTOR_DRAFT_PENDING_ACCEPT',
		path: `nodes.${inspectorNodeId}.inspector.draftParams`,
		message: `Unsaved ${nodeKind} draft changes detected for ${detail}. Click Accept before saving the graph.`,
		severity: 'error'
	};
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

type PruneResult =
	| { ok: true; edges: Edge<PipelineEdgeData>[]; prunedIds: string[] }
	| { ok: false; error: string };

type InspectorDeps = {
	update: (fn: (s: GraphState) => GraphState, ctx?: AuditContext) => void;
	getState: () => GraphState;
	persist: (state: GraphState) => void;
	applyLocalStaleInvalidation: (nodeId: string, reason?: string) => void;
	syncAcceptParamsForNode: (
		nodeId: string,
		params: Record<string, any>,
		beforeExecParams: Record<string, unknown>
	) => Promise<void>;
	pruneAndRecontractEdgesStrict: (
		nodes: Node<PipelineNodeData>[],
		edges: Edge<PipelineEdgeData>[]
	) => PruneResult;
};

type SchemaEnvelopeChannel = 'expectedSchema' | 'expectedInputSchemas';

export function createInspectorManager(deps: InspectorDeps) {
	const { update, getState, persist } = deps;

	function updateNodeConfigImpl(
		nodeId: string,
		config: { params?: unknown; schema?: Record<string, unknown> },
		opts?: { allowComponentContractMutation?: boolean; enforceComponentContractBoundary?: boolean }
	) {
		let out: { ok: boolean; error?: string; removedEdgeIds?: string[] } = { ok: true };
		const allowComponentContractMutation = Boolean(opts?.allowComponentContractMutation ?? false);
		const enforceComponentContractBoundary = Boolean(opts?.enforceComponentContractBoundary ?? false);
		const componentContractMutationError =
			'Component API contract can only be edited in component authoring mode.';

		const hasContractMutationInPatch = (currentParamsRaw: unknown, patchRaw: unknown): boolean => {
			const currentParams =
				currentParamsRaw && typeof currentParamsRaw === 'object'
					? (currentParamsRaw as Record<string, unknown>)
					: {};
			const patchParams =
				patchRaw && typeof patchRaw === 'object' ? (patchRaw as Record<string, unknown>) : null;
			if (!patchParams) return false;
			const keyChanged = (key: string): boolean =>
				Object.prototype.hasOwnProperty.call(patchParams, key) &&
				stableJson(patchParams[key]) !== stableJson(currentParams[key]);
			if (keyChanged('api')) return true;
			if (keyChanged('exposureRegistry')) return true;
			if (keyChanged('published_profile')) return true;
			if (keyChanged('debug_profile')) return true;
			if (Object.prototype.hasOwnProperty.call(patchParams, 'bindings')) {
				const patchBindings =
					patchParams.bindings && typeof patchParams.bindings === 'object'
						? (patchParams.bindings as Record<string, unknown>)
						: null;
				if (patchBindings && Object.prototype.hasOwnProperty.call(patchBindings, 'outputs')) {
					const currentBindings =
						currentParams.bindings && typeof currentParams.bindings === 'object'
							? (currentParams.bindings as Record<string, unknown>)
							: {};
					const currentOutputs =
						currentBindings.outputs && typeof currentBindings.outputs === 'object'
							? (currentBindings.outputs as Record<string, unknown>)
							: null;
					const patchOutputs =
						patchBindings.outputs && typeof patchBindings.outputs === 'object'
							? (patchBindings.outputs as Record<string, unknown>)
							: null;
					if (stableJson(patchOutputs) !== stableJson(currentOutputs)) return true;
				}
			}
			return false;
		};

		update((s) => {
			let nodes = s.nodes;
			let edges = s.edges;
			let removedEdgeIds: string[] = [];
			let autoUnpinned = false;
			let pinAutoClearNotice: string | null = null;

			// 0) Ensure node exists
			const node = nodes.find((n) => n.id === nodeId);
			if (!node) {
				out = { ok: false, error: 'Node not found' };
				return logPush(s, 'warn', out.error!, nodeId);
			}
			if (
				config.params !== undefined &&
				node.data.kind === 'component' &&
				enforceComponentContractBoundary &&
				s.editingContext === 'graph' &&
				!allowComponentContractMutation &&
				hasContractMutationInPatch((node.data as any)?.params, config.params)
			) {
				out = { ok: false, error: componentContractMutationError };
				const inspector =
					String(s.inspector?.nodeId ?? '') === nodeId
						? {
								...s.inspector,
								systemNotice: componentContractMutationError
							}
						: s.inspector;
				return logPush({ ...s, inspector }, 'warn', componentContractMutationError, nodeId);
			}
			const beforeExecParams = effectiveExecParamsForNode(node as Node<PipelineNodeData>);
			const wasPinnedBeforeParams = nodeFreezeMode(node as any) !== null;
			const previousComponentOutputNames =
				node.data.kind === 'component' ? listComponentOutputNames(node as Node<PipelineNodeData>) : [];

			// ---- 1) params (must be valid to commit) ----
			if (config.params !== undefined) {
				const res = updateNodeParamsValidated(nodes, nodeId, config.params);
				if (res.error) {
					out = { ok: false, error: res.error };
					return logPush(s, 'error', res.error, nodeId);
				}
				nodes = res.nodes;
			}
			if (config.schema !== undefined) {
				const parsed = NodeSchemaEnvelopeSchema.safeParse(config.schema ?? {});
				if (!parsed.success) {
					out = { ok: false, error: 'Invalid schema envelope payload.' };
					return logPush(s, 'error', out.error, nodeId);
				}
				nodes = nodes.map((n) =>
					n.id === nodeId
						? {
								...n,
								data: {
									...n.data,
									schema: parsed.data
								}
							}
						: n
				);
			}

			const currentNode = nodes.find((n) => n.id === nodeId) ?? node;
			const afterExecParams = effectiveExecParamsForNode(currentNode as Node<PipelineNodeData>);
			const execParamsChanged = stableJson(beforeExecParams) !== stableJson(afterExecParams);
			if (config.params !== undefined && wasPinnedBeforeParams && execParamsChanged) {
				nodes = nodes.map((n) => {
					if (n.id !== nodeId) return n;
					const nextMeta = { ...(((n.data as any)?.meta ?? {}) as Record<string, unknown>) };
					delete (nextMeta as any).freeze;
					return {
						...n,
						data: {
							...(n.data as any),
							meta: nextMeta
						}
					} as Node<PipelineNodeData>;
				});
				autoUnpinned = true;
				pinAutoClearNotice =
					'[Pin cleared] Parameters changed, so this node was automatically unpinned to keep execution integrity.';
			}
			const effectiveIo = deriveNodeIoForData(currentNode.data);
			const { in: inputType, out: outputType } = effectiveIo;
			if (inputType !== null && !isPayloadType(inputType)) {
				out = { ok: false, error: `Invalid derived input payload type: ${String(inputType)}` };
				return logPush(s, 'warn', out.error!, nodeId);
			}
			if (outputType !== null && !isPayloadType(outputType)) {
				out = { ok: false, error: `Invalid derived output payload type: ${String(outputType)}` };
				return logPush(s, 'warn', out.error!, nodeId);
			}

			nodes = nodes.map((n) => {
				if (n.id !== nodeId) return n;
				return {
					...n,
					data: {
						...n.data,
						meta: { ...(n.data.meta ?? {}), updatedAt: new Date().toISOString() }
					}
				};
			});

			const updatedNode = nodes.find((n) => n.id === nodeId)!;

			if (updatedNode.data.kind === 'component') {
				const reconciled = reconcileComponentOutgoingEdges(
					nodeId,
					updatedNode as Node<PipelineNodeData>,
					edges,
					previousComponentOutputNames
				);
				edges = reconciled.edges;
				if (reconciled.removedIds.length) {
					removedEdgeIds = [...removedEdgeIds, ...reconciled.removedIds];
				}
			}

			const pr = deps.pruneAndRecontractEdgesStrict(nodes, edges);
			if (pr.ok === false) {
				out = { ok: false, error: pr.error };
				return logPush(s, 'warn', pr.error, nodeId);
			}
			edges = pr.edges;
			if (pr.prunedIds?.length) {
				removedEdgeIds = [...removedEdgeIds, ...pr.prunedIds];
			}
			if (removedEdgeIds.length) {
				const uniq = Array.from(new Set(removedEdgeIds.filter((id) => id.length > 0)));
				out.removedEdgeIds = uniq;
			}

			const nextInspector =
				autoUnpinned && String(s.inspector?.nodeId ?? '') === nodeId
					? {
							...s.inspector,
							systemNotice: pinAutoClearNotice
						}
					: s.inspector;
			const next = logPush({ ...s, nodes, edges, inspector: nextInspector }, 'info', 'Node config updated', nodeId);
			persist(next);
			return next;
		});

		return out;
	}

	function setNodeSchemaObservationImpl(
		nodeId: string,
		channel: SchemaEnvelopeChannel,
		typedSchema: Record<string, unknown> | null,
		inputHandleRaw?: string
	): { ok: boolean; error?: string } {
		let result: { ok: boolean; error?: string } = { ok: true };
		update((s) => {
			const node = s.nodes.find((n) => n.id === nodeId);
			if (!node) {
				result = { ok: false, error: 'Node not found' };
				return s;
			}
			const existingSchema =
				node.data?.schema && typeof node.data.schema === 'object'
					? ({ ...(node.data.schema as Record<string, unknown>) } as Record<string, unknown>)
					: {};
			if (typedSchema == null) {
				if (channel === 'expectedInputSchemas') {
					const handle = String(inputHandleRaw ?? 'in').trim() || 'in';
					const current =
						(existingSchema as any).expectedInputSchemas &&
						typeof (existingSchema as any).expectedInputSchemas === 'object'
							? ({ ...((existingSchema as any).expectedInputSchemas as Record<string, unknown>) } as Record<
									string,
									unknown
								>)
							: {};
					delete current[handle];
					if (Object.keys(current).length === 0) {
						delete (existingSchema as any).expectedInputSchemas;
					} else {
						(existingSchema as any).expectedInputSchemas = current;
					}
				} else {
					delete (existingSchema as any)[channel];
				}
			} else {
				const normalizedTyped = payloadHintToTypedSchema(typedSchema);
				if (!normalizedTyped) {
					result = { ok: false, error: 'Expected schema must include a valid typed schema type.' };
					return logPush(s, 'warn', result.error, nodeId);
				}
				const observation = {
					typedSchema: normalizedTyped,
					source: 'declared',
					state: 'fresh',
					schemaFingerprint: fingerprintTypedSchema(normalizedTyped),
					updatedAt: new Date().toISOString()
				};
				if (channel === 'expectedInputSchemas') {
					const handle = String(inputHandleRaw ?? 'in').trim() || 'in';
					const current =
						(existingSchema as any).expectedInputSchemas &&
						typeof (existingSchema as any).expectedInputSchemas === 'object'
							? ({ ...((existingSchema as any).expectedInputSchemas as Record<string, unknown>) } as Record<
									string,
									unknown
								>)
							: {};
					current[handle] = observation;
					(existingSchema as any).expectedInputSchemas = current;
				} else {
					(existingSchema as any)[channel] = observation;
				}
			}
			const parsed = NodeSchemaEnvelopeSchema.safeParse(existingSchema);
			if (!parsed.success) {
				result = { ok: false, error: 'Expected schema is invalid.' };
				return logPush(s, 'warn', result.error, nodeId);
			}
			const nodes = s.nodes.map((n) => {
				if (n.id !== nodeId) return n;
				const schema = parsed.data;
				const nextData: Record<string, unknown> = {
					...(n.data as Record<string, unknown>),
					meta: { ...(((n.data as any)?.meta ?? {}) as Record<string, unknown>), updatedAt: new Date().toISOString() }
				};
				if (hasSchemaEnvelopeContent(schema)) {
					nextData.schema = schema;
				} else {
					delete nextData.schema;
				}
				return {
					...n,
					data: nextData as PipelineNodeData & Record<string, unknown>
				};
			});
			const rechecked = deps.pruneAndRecontractEdgesStrict(nodes, s.edges);
			if (!rechecked.ok) {
				result = { ok: false, error: rechecked.error };
				return logPush(s, 'warn', rechecked.error, nodeId);
			}
			const next = logPush(
				{ ...s, nodes, edges: rechecked.edges },
				'info',
				channel === 'expectedSchema'
					? 'Expected schema updated'
					: channel === 'expectedInputSchemas'
						? `Expected input schema updated for handle ${String(inputHandleRaw ?? 'in')}`
						: 'Expected schema updated',
				nodeId
			);
			persist(next);
			return next;
		});
		if (result.ok) {
			deps.applyLocalStaleInvalidation(nodeId, 'PARAMS_CHANGED');
		}
		return result;
	}

	function setNodeExpectedSchemaImpl(
		nodeId: string,
		typedSchema: Record<string, unknown> | null
	): { ok: boolean; error?: string } {
		return setNodeSchemaObservationImpl(nodeId, 'expectedSchema', typedSchema);
	}

	function setNodeExpectedInputSchemaImpl(
		nodeId: string,
		typedSchema: Record<string, unknown> | null
	): { ok: boolean; error?: string } {
		return setNodeSchemaObservationImpl(nodeId, 'expectedInputSchemas', typedSchema, 'in');
	}

	function setNodeExpectedInputSchemaForHandleImpl(
		nodeId: string,
		inputHandle: string,
		typedSchema: Record<string, unknown> | null
	): { ok: boolean; error?: string } {
		return setNodeSchemaObservationImpl(nodeId, 'expectedInputSchemas', typedSchema, inputHandle);
	}

	function canonicalInspectorDraftForNode(
		node: Node<PipelineNodeData & Record<string, unknown>> | undefined,
		params: Record<string, any>
	): Record<string, any> {
		if (!node) return params;
		if (node.data?.kind === 'component') {
			return sanitizeComponentDraftParams(params);
		}
		return params;
	}

	function patchInspectorDraft(
		patch: Record<string, any>,
		opts?: { intent?: InspectorDraftPatchIntent; notice?: string | null }
	) {
		update((s) => {
			if (!s.inspector.nodeId) return s;
			const node = s.nodes.find((n) => n.id === s.inspector.nodeId);
			const nextDraftParams = { ...s.inspector.draftParams, ...patch };
			const intent: InspectorDraftPatchIntent = opts?.intent ?? 'user_edit';
			const baselineCanonical = canonicalInspectorDraftForNode(
				node as any,
				structuredClone((node?.data?.params ?? {}) as Record<string, any>)
			);
			const nextCanonical = canonicalInspectorDraftForNode(node as any, structuredClone(nextDraftParams));
			const changedVsBaseline = JSON.stringify(nextCanonical) !== JSON.stringify(baselineCanonical);
			const changedVsCurrent = JSON.stringify(nextDraftParams) !== JSON.stringify(s.inspector.draftParams ?? {});
			const nextDirty = intent === 'system_canonicalize' ? Boolean(s.inspector.dirty) : changedVsBaseline;
			const nextSystemNotice =
				intent === 'system_canonicalize' && changedVsCurrent
					? String(opts?.notice ?? 'Bindings normalized automatically.')
					: intent === 'user_edit'
						? null
						: s.inspector.systemNotice ?? null;
			return {
				...s,
				inspector: {
					...s.inspector,
					draftParams: nextDraftParams,
					dirty: nextDirty,
					systemNotice: nextSystemNotice
				}
			};
		});
	}

	// optional: dropdown commit (keeps draft consistent + commits)
	async function commitInspectorImmediate(patch: Record<string, any>) {
		const s = getState();
		const nodeId = s.inspector.nodeId;
		if (!nodeId) return { ok: false, error: 'No node selected' };
		const targetNode = s.nodes.find((x) => x.id === nodeId);
		const commitPatch =
			targetNode?.data?.kind === 'component'
				? sanitizeComponentDraftParams(patch)
				: patch;
		if (patch?.op === 'dedupe' || patch?.dedupe || s.inspector.draftParams?.op === 'dedupe') {
			console.log('[dedupe-store] commitInspectorImmediate:patch', {
				nodeId,
				patch: commitPatch,
				draftParams: s.inspector.draftParams
			});
		}
		const beforeNode = targetNode;
		const beforeExecParams = effectiveExecParamsForNode(beforeNode);

		// 2) commit patch (validated/stripped)
		const result = updateNodeConfigImpl(nodeId, { params: commitPatch }, { enforceComponentContractBoundary: true });
		if (!result.ok) return result;

		const afterState = getState();
		const paramsForSubmit = committedNodeParamsForNode(afterState, nodeId);
		if (paramsForSubmit?.op === 'dedupe') {
			console.log('[dedupe-store] commitInspectorImmediate:paramsForSubmit', {
				nodeId,
				paramsForSubmit
			});
		}
		update((cur) => {
			if (cur.inspector.nodeId !== nodeId) return cur;
			return {
				...cur,
				inspector: {
					...cur.inspector,
					draftParams: structuredClone(paramsForSubmit),
					dirty: false
				}
			};
		});

		await deps.syncAcceptParamsForNode(nodeId, paramsForSubmit, beforeExecParams);
		return result;
	}

	async function commitSnapshotSelection(patch: Record<string, any>) {
		const s = getState();
		const nodeId = s.inspector.nodeId;
		if (!nodeId) return { ok: false, error: 'No node selected' };
		const beforeNode = s.nodes.find((x) => x.id === nodeId);
		const beforeExecParams = effectiveExecParamsForNode(beforeNode);

		// Commit only the provided snapshot-related patch; do not merge with pending draft.
		const result = updateNodeConfigImpl(nodeId, { params: patch });
		if (!result.ok) return result;

		const afterState = getState();
		const paramsForSubmit = committedNodeParamsForNode(afterState, nodeId);
		update((cur) => {
			if (cur.inspector.nodeId !== nodeId) return cur;
			return {
				...cur,
				inspector: {
					...cur.inspector,
					draftParams: structuredClone(paramsForSubmit),
					dirty: false
				}
			};
		});

		await deps.syncAcceptParamsForNode(nodeId, paramsForSubmit, beforeExecParams);
		return result;
	}

	async function applyInspectorDraft() {
		const s = getState();
		const nodeId = s.inspector.nodeId;
		if (!nodeId) return { ok: false, error: 'No node selected' };
		const beforeNode = s.nodes.find((x) => x.id === nodeId);
		const beforeExecParams = effectiveExecParamsForNode(beforeNode);
		const paramsForCommit =
			beforeNode?.data?.kind === 'component'
				? sanitizeComponentDraftParams(s.inspector.draftParams as Record<string, any>)
				: (s.inspector.draftParams as Record<string, any>);
		if (beforeNode?.data?.kind === 'component') {
			const validation = validateComponentDraftForAccept(paramsForCommit);
			if (!validation.ok) {
				update((cur) => {
					let next = cur;
					for (const issue of validation.errors) {
						next = logPush(next, 'warn', issue, nodeId);
					}
					return next;
				});
				return {
					ok: false,
					reason: 'component_accept_blocked',
					error: validation.errors[0] ?? 'Component output bindings are invalid.',
					details: validation.errors
				} as const;
			}
		}

		const r = updateNodeConfigImpl(nodeId, { params: paramsForCommit }, { enforceComponentContractBoundary: true });

		if (!r.ok) {
			if (String(r.error ?? '').toLowerCase().includes('component authoring mode')) {
				return {
					ok: false,
					reason: 'component_contract_readonly' as const,
					error: r.error
				};
			}
			return r;
		}

		// only clear dirty if commit succeeded (fail-closed keeps draft)
		if (r.ok) {
			update((st) => {
				const n = st.nodes.find((x) => x.id === nodeId);
				return {
					...st,
					inspector: {
						nodeId,
						draftParams: structuredClone((n?.data.params ?? {}) as any),
						dirty: false,
						uiByNodeId: st.inspector.uiByNodeId
					}
				};
			});

			await deps.syncAcceptParamsForNode(nodeId, paramsForCommit, beforeExecParams);
		}
		return r;
	}

	function revertInspectorDraft() {
		update((s) => {
			const nodeId = s.inspector.nodeId;
			if (!nodeId) return s;
			const n = s.nodes.find((x) => x.id === nodeId);
			return {
				...s,
				inspector: {
					nodeId,
					draftParams: structuredClone((n?.data.params ?? {}) as any),
					dirty: false,
					uiByNodeId: s.inspector.uiByNodeId
				}
			};
		});
	}

	function getInspectorUi(nodeId: string, paramsHint?: Record<string, any>): ApiEditorUiState {
		const state = getState();
		const existing = state.inspector.uiByNodeId?.[nodeId];
		if (existing) return existing;
		const node = state.nodes.find((n) => n.id === nodeId);
		const params = paramsHint ?? ((node?.data?.params ?? {}) as Record<string, any>);
		return defaultApiEditorUiState(params);
	}

	function setInspectorUi(nodeId: string, patch: Partial<ApiEditorUiState>): void {
		update((s) => {
			const node = s.nodes.find((n) => n.id === nodeId);
			const base =
				s.inspector.uiByNodeId?.[nodeId] ??
				defaultApiEditorUiState((node?.data?.params ?? {}) as Record<string, any>);
			return {
				...s,
				inspector: {
					...s.inspector,
					uiByNodeId: {
						...(s.inspector.uiByNodeId ?? {}),
						[nodeId]: { ...base, ...patch }
					}
				}
			};
		});
	}

	return {
		actions: {
			patchInspectorDraft,
			commitInspectorImmediate,
			commitSnapshotSelection,
			applyInspectorDraft,
			revertInspectorDraft,
			getInspectorDraftAcceptValidation(stateOverride?: GraphState): InspectorDraftAcceptValidation {
				const state = stateOverride ?? getState();
				return validateInspectorDraftForAccept(state);
			},
			getInspectorUi,
			setInspectorUi,
			updateNodeConfig: updateNodeConfigImpl,
			setNodeExpectedSchema(nodeId: string, typedSchema: Record<string, unknown> | null) {
				return setNodeExpectedSchemaImpl(nodeId, typedSchema);
			},
			setNodeExpectedInputSchema(nodeId: string, typedSchema: Record<string, unknown> | null) {
				return setNodeExpectedInputSchemaImpl(nodeId, typedSchema);
			},
			setNodeExpectedInputSchemaForHandle(
				nodeId: string,
				inputHandle: string,
				typedSchema: Record<string, unknown> | null
			) {
				return setNodeExpectedInputSchemaForHandleImpl(nodeId, inputHandle, typedSchema);
			},
		}
	};
}
