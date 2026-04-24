// src/lib/flow/store/graphStore.persistence.ts
import type { Node, Edge } from '@xyflow/svelte';
import type { PipelineNodeData, PipelineEdgeData, PipelineGraphDTO } from '$lib/flow/types';
import { TOOL_BUILTIN_PROFILE_IDS } from '$lib/flow/schema/toolBuiltinProfiles';
import { validateCustomPackageDraft } from '$lib/flow/schema/toolBuiltinCustomPackages';
import {
	getLatestGraphRevision,
	getGraphRevision,
	listGraphRevisions,
	createGraphRevision,
	listGraphs as listGraphsClient,
	deleteGraph as deleteGraphClient,
	deleteGraphRevision as deleteGraphRevisionClient
} from '$lib/flow/client/graphs';
import {
	getComponentRevision,
	listComponentRevisions,
	listComponents,
	createComponentRevision,
	renameComponent as renameComponentClient,
	deleteComponent as deleteComponentClient,
	deleteComponentRevision as deleteComponentRevisionClient,
	type ComponentApiContract
} from '$lib/flow/client/components';
import { comparePublishedProfiles, materializeExposureProfiles, normalizeExposureRegistry } from '$lib/flow/components/exposureProfiles';
import { stableJson, logPush as _logPush, nextLogId, ensureNormalizedBindingsForNodes, withGraphMeta } from './graphStore.audit';
import { pendingInspectorDraftSaveDiagnostic, sanitizeComponentDraftParams, normalizeHandleId, canonicalComponentSourceHandleForEdge } from './graphStore.inspector';
import { displayStatusFromBinding } from './runScope';
import {
	NodeDocExplanationModeSchema,
	NodeDocTrainingModeSchema,
	sanitizeNodeDocGeneratedExplanation,
	type NodeDocExplanationMode,
	type NodeDocGeneratedExplanation,
	type NodeDocTrainingMode
} from '$lib/flow/schema/nodeDocs';
import { createMemoizedNodeDocResolver, type NodeDocResolved } from '$lib/flow/components/nodeDocsViewModel';
import {
	stripToDTO,
	buildHardResetState as _buildHardResetState,
	captureComponentEditSnapshot,
	normalizeGraphForComponentMigration,
	pruneAndRecontractEdgesStrict,
	canonicalizeComponentEdgeSourceHandles,
} from './graphStore.graph-edit';
import {
	isEdgeStillValid,
	payloadHintToTypedSchema,
	normalizeComponentPayloadType,
} from './graphStore.node-schema';
import { findDuplicateNodeNames } from './nodeNameUniqueness';
import { clearGraphDraft } from './persist';
import type {
	GraphState,
	AuditContext,
	SavePreflightDiagnostic,
	SavePreflightResult,
	SaveConsistencyEntity,
	SaveConsistencyMismatch,
	InspectorDraftPatchIntent,
	ComponentEditSessionSnapshot,
	EditorContext,
	InputResolution,
	NormalizedNodeBinding,
} from './graphStore.types';
import { INITIAL_INSPECTOR } from './graphStore.types';
import { buildPromotedCheckpointKey, parsePromotedCheckpointKey, type CheckpointRecord } from '$lib/flow/types/checkpoint';

// Suppress unused import warning — _buildHardResetState kept for potential future use
void (_buildHardResetState as unknown);
void (_logPush as unknown);

const allowedPorts = new Set(['table', 'text', 'json', 'binary', 'embeddings', 'image', 'audio', 'video']);
const allowedBuiltinProfileIds = new Set<string>(TOOL_BUILTIN_PROFILE_IDS);
const COMPONENT_DRAFT_GRAPH_KEY = '__graphDraft';
const COMPONENT_DRAFT_LAST_COMMITTED_CHECKPOINTS_KEY = '__lastCommittedCheckpointRegistry';

function readComponentDraftGraph(
	value: unknown
): { nodes: unknown[]; edges: unknown[]; checkpointRegistry: Record<string, unknown> } | null {
	if (!value || typeof value !== 'object') return null;
	const graph = (value as Record<string, unknown>)[COMPONENT_DRAFT_GRAPH_KEY];
	if (!graph || typeof graph !== 'object') return null;
	const nodes = Array.isArray((graph as any).nodes) ? ((graph as any).nodes as unknown[]) : null;
	const edges = Array.isArray((graph as any).edges) ? ((graph as any).edges as unknown[]) : null;
	if (!nodes || !edges) return null;
	const checkpointRegistry =
		(graph as any).checkpointRegistry && typeof (graph as any).checkpointRegistry === 'object'
			? structuredClone((graph as any).checkpointRegistry as Record<string, unknown>)
			: {};
	return {
		nodes: structuredClone(nodes),
		edges: structuredClone(edges),
		checkpointRegistry
	};
}

// ── PART A: Top-level pure function exports ──────────────────────────────────

export function buildPersistableGraphStrict(
	nodes: Node<PipelineNodeData>[],
	edges: Edge<PipelineEdgeData>[],
	graphId?: string,
	checkpointRegistry?: Record<string, unknown>
): { ok: true; graph: PipelineGraphDTO } | { ok: false; error: string } {
	const normalized = normalizeGraphForComponentMigration(nodes, edges);
	const canonicalized = canonicalizeComponentEdgeSourceHandles(normalized.nodes, normalized.edges, 'strict');
	if (!canonicalized.ok) return { ok: false, error: canonicalized.error };
	const rechecked = pruneAndRecontractEdgesStrict(normalized.nodes, canonicalized.edges);
	if (!rechecked.ok) return { ok: false, error: rechecked.error };
	return {
		ok: true,
		graph: stripToDTO(normalized.nodes, rechecked.edges, graphId, (checkpointRegistry as any) ?? {})
	};
}

function nodeLabelForSaveCompare(node: unknown): string {
	if (!node || typeof node !== 'object') return '-';
	const data = (node as any).data;
	const label = String((data as any)?.label ?? '').trim();
	if (label) return label;
	const kind = String((data as any)?.kind ?? '').trim();
	const subtype = String(
		(data as any)?.transformKind ??
			(data as any)?.sourceKind ??
			(data as any)?.llmKind ??
			((data as any)?.params?.provider ?? '')
	).trim();
	if (kind && subtype) return `${kind}:${subtype}`;
	if (kind) return kind;
	return '-';
}

function edgeLabelForSaveCompare(edge: unknown): string {
	if (!edge || typeof edge !== 'object') return '-';
	const source = String((edge as any).source ?? '').trim();
	const target = String((edge as any).target ?? '').trim();
	const sourceHandle = String((edge as any).sourceHandle ?? 'out').trim() || 'out';
	const targetHandle = String((edge as any).targetHandle ?? 'in').trim() || 'in';
	return `${source}:${sourceHandle} -> ${target}:${targetHandle}`;
}

function sanitizeNodeForSaveCompare(node: unknown): Record<string, unknown> {
	const next = structuredClone((node ?? {}) as Record<string, unknown>);
	delete (next as any).selected;
	delete (next as any).dragging;
	delete (next as any).positionAbsolute;
	delete (next as any).resizing;
	delete (next as any).measured;
	return next;
}

function sanitizeEdgeForSaveCompare(edge: unknown): Record<string, unknown> {
	const next = structuredClone((edge ?? {}) as Record<string, unknown>);
	delete (next as any).selected;
	return next;
}

function asSaveEntity(id: string, label: string): SaveConsistencyEntity {
	return { id, label: label || '-' };
}

export function computeSaveConsistencyMismatch(
	canvasGraph: PipelineGraphDTO,
	persistedGraph: PipelineGraphDTO
): SaveConsistencyMismatch | null {
	const canvasNodes = Array.isArray(canvasGraph?.nodes) ? (canvasGraph.nodes as any[]) : [];
	const persistedNodes = Array.isArray(persistedGraph?.nodes) ? (persistedGraph.nodes as any[]) : [];
	const canvasEdges = Array.isArray(canvasGraph?.edges) ? (canvasGraph.edges as any[]) : [];
	const persistedEdges = Array.isArray(persistedGraph?.edges) ? (persistedGraph.edges as any[]) : [];

	const canvasNodeMap = new Map<string, any>();
	for (const node of canvasNodes) {
		const id = String((node as any)?.id ?? '').trim();
		if (!id) continue;
		canvasNodeMap.set(id, node);
	}
	const persistedNodeMap = new Map<string, any>();
	for (const node of persistedNodes) {
		const id = String((node as any)?.id ?? '').trim();
		if (!id) continue;
		persistedNodeMap.set(id, node);
	}

	const canvasEdgeMap = new Map<string, any>();
	for (const edge of canvasEdges) {
		const id = String((edge as any)?.id ?? '').trim();
		if (!id) continue;
		canvasEdgeMap.set(id, edge);
	}
	const persistedEdgeMap = new Map<string, any>();
	for (const edge of persistedEdges) {
		const id = String((edge as any)?.id ?? '').trim();
		if (!id) continue;
		persistedEdgeMap.set(id, edge);
	}

	const missingNodes: SaveConsistencyEntity[] = [];
	const addedNodes: SaveConsistencyEntity[] = [];
	const changedNodes: SaveConsistencyEntity[] = [];
	const missingEdges: SaveConsistencyEntity[] = [];
	const addedEdges: SaveConsistencyEntity[] = [];
	const changedEdges: SaveConsistencyEntity[] = [];

	for (const [id, node] of canvasNodeMap.entries()) {
		if (!persistedNodeMap.has(id)) {
			missingNodes.push(asSaveEntity(id, nodeLabelForSaveCompare(node)));
			continue;
		}
		const persisted = persistedNodeMap.get(id);
		if (stableJson(sanitizeNodeForSaveCompare(node)) !== stableJson(sanitizeNodeForSaveCompare(persisted))) {
			changedNodes.push(asSaveEntity(id, nodeLabelForSaveCompare(node)));
		}
	}
	for (const [id, node] of persistedNodeMap.entries()) {
		if (canvasNodeMap.has(id)) continue;
		addedNodes.push(asSaveEntity(id, nodeLabelForSaveCompare(node)));
	}

	for (const [id, edge] of canvasEdgeMap.entries()) {
		if (!persistedEdgeMap.has(id)) {
			missingEdges.push(asSaveEntity(id, edgeLabelForSaveCompare(edge)));
			continue;
		}
		const persisted = persistedEdgeMap.get(id);
		if (stableJson(sanitizeEdgeForSaveCompare(edge)) !== stableJson(sanitizeEdgeForSaveCompare(persisted))) {
			changedEdges.push(asSaveEntity(id, edgeLabelForSaveCompare(edge)));
		}
	}
	for (const [id, edge] of persistedEdgeMap.entries()) {
		if (canvasEdgeMap.has(id)) continue;
		addedEdges.push(asSaveEntity(id, edgeLabelForSaveCompare(edge)));
	}

	const hasStructuralMismatch =
		missingNodes.length > 0 ||
		addedNodes.length > 0 ||
		missingEdges.length > 0 ||
		addedEdges.length > 0 ||
		canvasNodes.length !== persistedNodes.length ||
		canvasEdges.length !== persistedEdges.length;
	if (!hasStructuralMismatch) {
		// Strict canonicalization may rewrite node/edge payload details without changing graph structure.
		// Do not block save on changed-only deltas (changedNodes/changedEdges) to avoid false positives.
		return null;
	}
	return {
		canvasNodeCount: canvasNodes.length,
		persistedNodeCount: persistedNodes.length,
		canvasEdgeCount: canvasEdges.length,
		persistedEdgeCount: persistedEdges.length,
		missingNodes,
		addedNodes,
		changedNodes,
		missingEdges,
		addedEdges,
		changedEdges
	};
}

export function __computeSaveConsistencyMismatchForTest(
	canvasGraph: PipelineGraphDTO,
	persistedGraph: PipelineGraphDTO
): SaveConsistencyMismatch | null {
	return computeSaveConsistencyMismatch(canvasGraph, persistedGraph);
}

export function toolBuiltinPreflightDiagnostics(node: Node<PipelineNodeData>): SavePreflightDiagnostic[] {
	if (node.data.kind !== 'tool') return [];
	const params = ((node.data as any)?.params ?? {}) as Record<string, any>;
	const provider = String(params?.provider ?? '').trim().toLowerCase();
	const builtin =
		params?.builtin && typeof params.builtin === 'object' ? (params.builtin as Record<string, any>) : null;
	if (!builtin && provider !== 'builtin') return [];
	const profileId = String((builtin?.profileId ?? 'core') ?? 'core').trim() || 'core';
	if (!allowedBuiltinProfileIds.has(profileId)) {
		return [
			{
				code: 'ENV_PROFILE_INVALID',
				path: `nodes.${String(node.id)}.params.builtin.profileId`,
				message: `Tool builtin profile "${profileId}" is invalid.`,
				severity: 'error'
			}
		];
	}
	if (profileId !== 'custom') return [];
	const customPackagesRaw = Array.isArray(builtin?.customPackages) ? (builtin?.customPackages as string[]) : [];
	if (customPackagesRaw.length === 0) {
		return [
			{
				code: 'ENV_PROFILE_MISSING',
				path: `nodes.${String(node.id)}.params.builtin.customPackages`,
				message: "Custom builtin profile requires at least one package before save.",
				severity: 'error'
			}
		];
	}
	const parsed = validateCustomPackageDraft(customPackagesRaw.join('\n'));
	const diagnostics: SavePreflightDiagnostic[] = [];
	if (parsed.blocked.length > 0) {
		diagnostics.push({
			code: 'ENV_PROFILE_PACKAGE_BLOCKED',
			path: `nodes.${String(node.id)}.params.builtin.customPackages`,
			message: `Custom builtin profile includes blocked package(s): ${parsed.blocked.join(', ')}`,
			severity: 'error'
		});
	}
	if (parsed.errors.length > 0) {
		diagnostics.push({
			code: 'ENV_PROFILE_INVALID',
			path: `nodes.${String(node.id)}.params.builtin.customPackages`,
			message: `Custom builtin profile has invalid package entries: ${parsed.errors.join('; ')}`,
			severity: 'error'
		});
	}
	return diagnostics;
}

export function buildSavePreflightDiagnostics(
	nodes: Node<PipelineNodeData>[],
	edges: Edge<PipelineEdgeData>[]
): SavePreflightResult {
	const normalized = normalizeGraphForComponentMigration(nodes, edges);
	const workingNodes = normalized.nodes;
	const workingEdges = normalized.edges;
	const diagnostics: SavePreflightDiagnostic[] = [];
	for (const node of nodes) {
		const schemaEnv =
			(node.data as any)?.schema && typeof (node.data as any).schema === 'object'
				? ((node.data as any).schema as Record<string, unknown>)
				: {};
		if (schemaEnv.expectedInputSchema && typeof schemaEnv.expectedInputSchema === 'object') {
			diagnostics.push({
				code: 'LEGACY_EXPECTED_INPUT_SCHEMA_DEPRECATED',
				path: `nodes.${String(node.id)}.data.schema.expectedInputSchema`,
				message:
					'Legacy data.schema.expectedInputSchema is deprecated; use data.schema.expectedInputSchemas.<handle> before 2026-06-30.',
				severity: 'warning'
			});
		}
		const portDeclarations = (node.data as any)?.portDeclarations;
		const portContracts = (node.data as any)?.portContracts;
		if (
			portContracts &&
			typeof portContracts === 'object' &&
			Object.keys(portContracts as Record<string, unknown>).length > 0 &&
			(!portDeclarations || typeof portDeclarations !== 'object')
		) {
			diagnostics.push({
				code: 'LEGACY_PORT_CONTRACTS_DEPRECATED',
				path: `nodes.${String(node.id)}.data.portContracts`,
				message:
					'Legacy data.portContracts is deprecated as the primary port model; declare data.portDeclarations before 2026-06-30.',
				severity: 'warning'
			});
		}
	}
	for (const edge of edges) {
		const queuePolicy = String((edge as any)?.data?.queue?.policy ?? 'fifo')
			.trim()
			.toLowerCase();
		if (queuePolicy === 'round_robin') {
			diagnostics.push({
				code: 'EDGE_QUEUE_POLICY_PREVIEW',
				path: `edges.${String(edge.id ?? '')}.data.queue.policy`,
				message: 'queue.policy=round_robin is preview-only; default fifo remains the stable policy.',
				severity: 'warning'
			});
		}
	}
	for (const edge of workingEdges) {
		const sourceHandle = String((edge as any)?.sourceHandle ?? 'out').trim() || 'out';
		const sourceNode = workingNodes.find((n) => n.id === edge.source);
		if (sourceNode?.data?.kind === 'component') {
			const canonicalHandle = canonicalComponentSourceHandleForEdge(workingNodes, edge);
			if (canonicalHandle == null) {
				diagnostics.push({
					code: 'COMPONENT_OUTPUT_HANDLE_UNRESOLVED',
					path: `edges.${String(edge.id ?? '')}.sourceHandle`,
					message: `Component edge sourceHandle "${sourceHandle}" is not declared in API outputs.`,
					severity: 'error'
				});
			}
		}
		const edgeCheck = isEdgeStillValid(workingNodes, edge);
		if (!edgeCheck.ok) {
			if (edgeCheck.reason === 'type_mismatch') {
				diagnostics.push({
					code: 'CONTRACT_EDGE_TYPE_MISMATCH',
					path: `edges.${String(edge.id ?? '')}.data.contract`,
					message: `Edge has incompatible schemas (source=${String(edge.source ?? '')}:${sourceHandle} target=${String((edge as any)?.target ?? '')}:${String((edge as any)?.targetHandle ?? 'in')})${edgeCheck.suggestion ? ` ${edgeCheck.suggestion}` : ''}.`,
					severity: 'error'
				});
			} else if (edgeCheck.reason === 'typed_schema_missing') {
				diagnostics.push({
					code: 'CONTRACT_EDGE_TYPED_SCHEMA_MISSING',
					path: `edges.${String(edge.id ?? '')}.data.contract.payload.source`,
					message: `Edge is missing required typed schema coverage. Required columns: ${(edgeCheck.missingColumns ?? []).join(', ') || '(unknown)'}.`,
					severity: 'error'
				});
			} else if (edgeCheck.reason === 'schema_mismatch') {
				diagnostics.push({
					code: 'CONTRACT_EDGE_SCHEMA_MISMATCH',
					path: `edges.${String(edge.id ?? '')}.data.contract`,
					message: `Edge is missing required columns: ${(edgeCheck.missingColumns ?? []).join(', ') || '(unknown)'}.`,
					severity: 'error'
				});
			} else {
				diagnostics.push({
					code: 'CONTRACT_EDGE_SCHEMA_UNRESOLVED',
					path: `edges.${String(edge.id ?? '')}.data.contract`,
					message: `Edge has unresolved schema compatibility (source=${String(edge.source ?? '')}:${sourceHandle} target=${String((edge as any)?.target ?? '')}:${String((edge as any)?.targetHandle ?? 'in')}).`,
					severity: 'error'
				});
			}
		}
	}

	for (const node of workingNodes) {
		diagnostics.push(...toolBuiltinPreflightDiagnostics(node));
		const expectedSchema = (node.data as any)?.schema?.expectedSchema;
		if (expectedSchema != null) {
			const expectedTypedRaw =
				typeof (expectedSchema as any)?.typedSchema === 'object'
					? ((expectedSchema as any).typedSchema as Record<string, unknown>)
					: null;
			const expectedTyped = payloadHintToTypedSchema(expectedTypedRaw);
			if (!expectedTyped) {
				diagnostics.push({
					code: 'EXPECTED_SCHEMA_INVALID',
					path: `nodes.${String(node.id)}.data.schema.expectedSchema.typedSchema`,
					message: 'Expected schema must define a valid typedSchema.type.',
					severity: 'error'
				});
			}
		}
		const expectedInputSchemas = (node.data as any)?.schema?.expectedInputSchemas;
		if (expectedInputSchemas != null) {
			if (typeof expectedInputSchemas !== 'object' || Array.isArray(expectedInputSchemas)) {
				diagnostics.push({
					code: 'EXPECTED_INPUT_SCHEMA_INVALID',
					path: `nodes.${String(node.id)}.data.schema.expectedInputSchemas`,
					message: 'Expected input schemas must be an object keyed by input handle.',
					severity: 'error'
				});
			} else {
				for (const [handle, envelope] of Object.entries(expectedInputSchemas as Record<string, any>)) {
					const expectedInputTypedRaw =
						typeof envelope?.typedSchema === 'object'
							? (envelope.typedSchema as Record<string, unknown>)
							: null;
					const expectedInputTyped = payloadHintToTypedSchema(expectedInputTypedRaw);
					if (!expectedInputTyped) {
						diagnostics.push({
							code: 'EXPECTED_INPUT_SCHEMA_INVALID',
							path: `nodes.${String(node.id)}.data.schema.expectedInputSchemas.${String(handle)}.typedSchema`,
							message: `Expected input schema for handle "${String(handle)}" must define a valid typedSchema.type.`,
							severity: 'error'
						});
					}
				}
			}
		}
		if (node.data.kind !== 'component') continue;
		const componentParams = ((node.data as any)?.params ?? {}) as Record<string, any>;
		const apiOutputs = Array.isArray(componentParams?.api?.outputs)
			? (componentParams.api.outputs as any[])
			: [];
		const exposureRegistry = Array.isArray(componentParams?.exposureRegistry)
			? (componentParams.exposureRegistry as any[])
			: [];
		for (let i = 0; i < apiOutputs.length; i += 1) {
			const out = apiOutputs[i] ?? {};
			const outputName = String(out?.name ?? '').trim();
			const pathBase = `nodes.${String(node.id)}.params.api.outputs[${i}]`;
			if (!outputName) {
				diagnostics.push({
					code: 'COMPONENT_OUTPUT_NAME_REQUIRED',
					path: `${pathBase}.name`,
					message: 'Component output name is required.',
					severity: 'error'
				});
				continue;
			}
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
			const isRequired = Boolean((out as any)?.required ?? true);
			if (isRequired && !internalSourcePath) {
				diagnostics.push({
					code: 'COMPONENT_OUTPUT_SOURCE_MISSING',
					path: `nodes.${String(node.id)}.params.exposureRegistry`,
					message: `Component output "${outputName}" requires API Contract internal_source_path.`,
					severity: 'error'
				});
			}
			const typedSchemaType = normalizeComponentPayloadType(out?.typedSchema?.type);
			if (typedSchemaType == null) {
				diagnostics.push({
					code: 'COMPONENT_OUTPUT_TYPED_SCHEMA_MISSING',
					path: `${pathBase}.typedSchema.type`,
					message: `Component output "${outputName}" must declare typedSchema.type.`,
					severity: 'error'
				});
			}
		}
	}
	for (const duplicate of findDuplicateNodeNames(workingNodes)) {
		diagnostics.push({
			code: 'NODE_NAME_DUPLICATE',
			path: `nodes.${duplicate.nodeIds.join(',')}.data.label`,
			message: `Duplicate node name "${duplicate.displayName}" in scope "${duplicate.scopeLabel || duplicate.scopeKey}" (case-insensitive, trimmed match).`,
			severity: 'error'
		});
	}

	return {
		ok: !diagnostics.some((d) => d.severity === 'error'),
		diagnostics
	};
}

export function summarizeSavePreflightError(diagnostics: SavePreflightDiagnostic[]): string {
	const errors = diagnostics.filter((d) => d.severity === 'error');
	if (errors.length === 0) return 'Graph preflight failed.';
	return errors
		.slice(0, 5)
		.map((d, i) => `${i + 1}. [${d.code}] (${d.path}) ${d.message}`)
		.join('\n');
}

// ── Node doc localStorage helpers ────────────────────────────────────────────

const NODE_DOC_EXPLANATION_MODE_STORAGE_KEY = 'flow.nodeDocExplanationMode.v1';
const NODE_DOC_TRAINING_MODE_STORAGE_KEY = 'flow.nodeDocTrainingMode.v1';

function loadNodeDocExplanationMode(): NodeDocExplanationMode {
	if (typeof window === 'undefined' || typeof window.localStorage === 'undefined') return 'default';
	try {
		const raw = String(window.localStorage.getItem(NODE_DOC_EXPLANATION_MODE_STORAGE_KEY) ?? '').trim();
		const parsed = NodeDocExplanationModeSchema.safeParse(raw);
		return parsed.success ? parsed.data : 'default';
	} catch {
		return 'default';
	}
}

function persistNodeDocExplanationMode(mode: NodeDocExplanationMode): void {
	if (typeof window === 'undefined' || typeof window.localStorage === 'undefined') return;
	try {
		window.localStorage.setItem(NODE_DOC_EXPLANATION_MODE_STORAGE_KEY, mode);
	} catch {
		// no-op
	}
}

function loadNodeDocTrainingMode(): NodeDocTrainingMode {
	if (typeof window === 'undefined' || typeof window.localStorage === 'undefined') return 'off';
	try {
		const raw = String(window.localStorage.getItem(NODE_DOC_TRAINING_MODE_STORAGE_KEY) ?? '').trim();
		const parsed = NodeDocTrainingModeSchema.safeParse(raw);
		return parsed.success ? parsed.data : 'off';
	} catch {
		return 'off';
	}
}

function persistNodeDocTrainingMode(mode: NodeDocTrainingMode): void {
	if (typeof window === 'undefined' || typeof window.localStorage === 'undefined') return;
	try {
		window.localStorage.setItem(NODE_DOC_TRAINING_MODE_STORAGE_KEY, mode);
	} catch {
		// no-op
	}
}

// ── resolveNodeInputsFromState (pure export) ──────────────────────────────────

function isFailedBindingStatus(binding: NormalizedNodeBinding | undefined): boolean {
	const display = displayStatusFromBinding(binding as any);
	const raw = String(binding?.status ?? '').toLowerCase();
	return display === 'failed' || raw.startsWith('failed');
}

function resolveUpstreamArtifact(
	state: GraphState,
	upstreamBinding: NormalizedNodeBinding | undefined
): { artifactId?: string; artifactSource?: 'active_run' | 'bound' } {
	const currentArtifactId =
		upstreamBinding?.current?.artifactId ?? upstreamBinding?.currentArtifactId ?? null;
	const lastArtifactId = upstreamBinding?.last?.artifactId ?? upstreamBinding?.lastArtifactId ?? null;
	const activeRunId = state.activeRunId;
	if (
		activeRunId &&
		upstreamBinding?.currentRunId === activeRunId &&
		typeof currentArtifactId === 'string' &&
		currentArtifactId.length > 0
	) {
		return { artifactId: currentArtifactId, artifactSource: 'active_run' };
	}
	if (typeof currentArtifactId === 'string' && currentArtifactId.length > 0) {
		return { artifactId: currentArtifactId, artifactSource: 'bound' };
	}
	if (typeof lastArtifactId === 'string' && lastArtifactId.length > 0) {
		return { artifactId: lastArtifactId, artifactSource: 'bound' };
	}
	return {};
}

export function resolveNodeInputsFromState(state: GraphState, nodeId: string): InputResolution[] {
	const node = state.nodes.find((n) => n.id === nodeId);
	if (!node) return [];
	const incoming = (state.edges ?? [])
		.filter((e) => e.target === nodeId)
		.slice()
		.sort((a, b) => String(a.id ?? '').localeCompare(String(b.id ?? '')));
	const inputHandles = new Set<string>();
	if (incoming.length === 0) inputHandles.add('in');
	for (const e of incoming) inputHandles.add(normalizeHandleId((e as any).targetHandle, 'in'));
	const orderedInputHandles = Array.from(inputHandles).sort((a, b) => a.localeCompare(b));
	const resolutions: InputResolution[] = [];
	for (const inputHandle of orderedInputHandles) {
		const edge = incoming.find((e) => normalizeHandleId((e as any).targetHandle, 'in') === inputHandle) ?? null;
		if (!edge) {
			resolutions.push({
				inputHandle,
				edge: null,
				status: 'missing',
				reason: 'DISCONNECTED',
				upstream: { nodeId: '', sourceHandle: '' }
			});
			continue;
		}
		const fromNodeId = String(edge.source ?? '');
		const sourceHandle = normalizeHandleId((edge as any).sourceHandle, 'out');
		const upstreamBinding = state.nodeBindings?.[fromNodeId];
		const upstreamOut = state.nodeOutputs?.[fromNodeId];
		const resolved = resolveUpstreamArtifact(state, upstreamBinding);
		if (resolved.artifactId) {
			resolutions.push({
				inputHandle,
				edge: { fromNodeId, sourceHandle },
				status: 'resolved',
				artifactId: resolved.artifactId,
				artifactSource: resolved.artifactSource,
				upstream: {
					nodeId: fromNodeId,
					sourceHandle,
					status: displayStatusFromBinding(upstreamBinding as any),
					isUpToDate: upstreamBinding?.isUpToDate,
					staleReason: upstreamBinding?.staleReason ?? null
				},
				artifactSummary: {
					mimeType: upstreamOut?.mimeType,
					schemaFingerprint: upstreamOut?.actualContractFingerprint,
					contract: upstreamOut?.payloadType
				}
			});
			continue;
		}
		resolutions.push({
			inputHandle,
			edge: { fromNodeId, sourceHandle },
			status: 'missing',
			reason: isFailedBindingStatus(upstreamBinding) ? 'UPSTREAM_FAILED' : 'UPSTREAM_NO_ARTIFACT',
			upstream: {
				nodeId: fromNodeId,
				sourceHandle,
				status: displayStatusFromBinding(upstreamBinding as any),
				isUpToDate: upstreamBinding?.isUpToDate,
				staleReason: upstreamBinding?.staleReason ?? null
			}
		});
	}
	return resolutions;
}

export { loadNodeDocExplanationMode, loadNodeDocTrainingMode };

// ── PART B: Factory ──────────────────────────────────────────────────────────

type PersistenceDeps = {
	update: (fn: (s: GraphState) => GraphState, ctx?: AuditContext) => void;
	getState: () => GraphState;
	persist: (state: GraphState) => void;
	applyGraphDocument: (
		graph: { nodes: unknown[]; edges: unknown[]; checkpointRegistry?: Record<string, unknown> },
		graphIdOverride?: string | null
	) => { ok: boolean; reason?: string };
	updateNodeConfig: (nodeId: string, config: any, opts?: any) => { ok: boolean; error?: string };
};

export function createPersistenceManager(deps: PersistenceDeps) {
	const { update, getState, persist, applyGraphDocument, updateNodeConfig } = deps;

	const resolveNodeDocMemoized = createMemoizedNodeDocResolver();

	function selectNode(nodeId: string | null) {
		update((s) => {
			if (!nodeId) {
				return {
					...s,
					selectedNodeId: null,
					inspector: { ...INITIAL_INSPECTOR, uiByNodeId: s.inspector.uiByNodeId }
				};
			}
			const n = s.nodes.find((x) => x.id === nodeId);
			return {
				...s,
				selectedNodeId: nodeId,
				inspector: {
					nodeId,
					draftParams: structuredClone((n?.data.params ?? {}) as any),
					dirty: false,
					uiByNodeId: s.inspector.uiByNodeId
				}
			};
		});
	}

	function getSavePreflight(stateOverride?: GraphState): import('./graphStore.types').SavePreflightResult {
		const state = stateOverride ?? getState();
		return buildSavePreflightDiagnostics(state.nodes as any, state.edges as any);
	}

	function resolveNodeInputs(nodeId: string): InputResolution[] {
		const s = getState();
		return resolveNodeInputsFromState(s, nodeId);
	}

	function getNodeDocResolved(nodeId: string): NodeDocResolved | null {
		const s = getState();
		return resolveNodeDocMemoized(s, nodeId);
	}

	function getNodeDocExplanationMode(): NodeDocExplanationMode {
		const s = getState();
		return s.nodeDocExplanationMode ?? 'default';
	}

	function setNodeDocExplanationMode(modeRaw: NodeDocExplanationMode): void {
		const parsed = NodeDocExplanationModeSchema.safeParse(modeRaw);
		const mode = parsed.success ? parsed.data : 'default';
		update((s) => ({ ...s, nodeDocExplanationMode: mode }));
		persistNodeDocExplanationMode(mode);
	}

	function getNodeDocTrainingMode(): NodeDocTrainingMode {
		const s = getState();
		const parsed = NodeDocTrainingModeSchema.safeParse((s as any)?.nodeDocTrainingMode);
		return parsed.success ? parsed.data : 'off';
	}

	function setNodeDocTrainingMode(modeRaw: NodeDocTrainingMode): void {
		const parsed = NodeDocTrainingModeSchema.safeParse(modeRaw);
		const mode = parsed.success ? parsed.data : 'off';
		update((s) => ({ ...s, nodeDocTrainingMode: mode }));
		persistNodeDocTrainingMode(mode);
	}

	function setNodeDocRuntimeConfig(config: Partial<{
		tooltipEnabled: boolean;
		tooltipOpenDelayMs: number;
		planesExpansionEnabled: boolean;
		planesExpansionDelayMs: number;
		explainModel: string;
		explainTemperature: number;
		explainTopP: number;
		explainMaxTokens: number;
	}>): void {
		update((s) => {
			const tooltipEnabled = typeof config?.tooltipEnabled === 'boolean' ? config.tooltipEnabled : s.nodeDocTooltipEnabled;
			const tooltipOpenDelayMsRaw = Number(config?.tooltipOpenDelayMs);
			const tooltipOpenDelayMs = Number.isFinite(tooltipOpenDelayMsRaw)
				? Math.max(0, Math.min(10000, Math.round(tooltipOpenDelayMsRaw)))
				: s.nodeDocTooltipOpenDelayMs;
			const planesExpansionEnabled =
				typeof config?.planesExpansionEnabled === 'boolean'
					? config.planesExpansionEnabled
					: s.nodeDocPlanesExpansionEnabled;
			const planesExpansionDelayMsRaw = Number(config?.planesExpansionDelayMs);
			const planesExpansionDelayMs = Number.isFinite(planesExpansionDelayMsRaw)
				? Math.max(0, Math.min(15000, Math.round(planesExpansionDelayMsRaw)))
				: s.nodeDocPlanesExpansionDelayMs;
			const explainModel = String(config?.explainModel ?? s.nodeDocExplainModel).trim() || s.nodeDocExplainModel;
			const explainTemperatureRaw = Number(config?.explainTemperature);
			const explainTemperature = Number.isFinite(explainTemperatureRaw)
				? Math.max(0, Math.min(2, explainTemperatureRaw))
				: s.nodeDocExplainTemperature;
			const explainTopPRaw = Number(config?.explainTopP);
			const explainTopP = Number.isFinite(explainTopPRaw)
				? Math.max(0, Math.min(1, explainTopPRaw))
				: s.nodeDocExplainTopP;
			const explainMaxTokensRaw = Number(config?.explainMaxTokens);
			const explainMaxTokens = Number.isFinite(explainMaxTokensRaw)
				? Math.max(1, Math.min(4096, Math.round(explainMaxTokensRaw)))
				: s.nodeDocExplainMaxTokens;
			if (
				tooltipEnabled === s.nodeDocTooltipEnabled &&
				tooltipOpenDelayMs === s.nodeDocTooltipOpenDelayMs &&
				planesExpansionEnabled === s.nodeDocPlanesExpansionEnabled &&
				planesExpansionDelayMs === s.nodeDocPlanesExpansionDelayMs &&
				explainModel === s.nodeDocExplainModel &&
				explainTemperature === s.nodeDocExplainTemperature &&
				explainTopP === s.nodeDocExplainTopP &&
				explainMaxTokens === s.nodeDocExplainMaxTokens
			) {
				return s;
			}
			return {
				...s,
				nodeDocTooltipEnabled: tooltipEnabled,
				nodeDocTooltipOpenDelayMs: tooltipOpenDelayMs,
				nodeDocPlanesExpansionEnabled: planesExpansionEnabled,
				nodeDocPlanesExpansionDelayMs: planesExpansionDelayMs,
				nodeDocExplainModel: explainModel,
				nodeDocExplainTemperature: explainTemperature,
				nodeDocExplainTopP: explainTopP,
				nodeDocExplainMaxTokens: explainMaxTokens
			};
		});
	}

	function setNodeDocGeneratedExplanation(nodeIdRaw: string, generatedRaw: unknown): { ok: boolean; reason?: string } {
		const nodeId = String(nodeIdRaw ?? '').trim();
		if (!nodeId) return { ok: false, reason: 'missing_node_id' };
		const generated = sanitizeNodeDocGeneratedExplanation(generatedRaw);
		if (!generated) return { ok: false, reason: 'invalid_generated_explanation' };
		let changed = false;
		update((s) => {
			const nodes = (s.nodes as any[]).map((node) => {
				if (String(node?.id ?? '') !== nodeId) return node;
				const meta = ((node?.data as any)?.meta ?? {}) as Record<string, unknown>;
				const nodeDoc = ((meta as any)?.nodeDoc ?? {}) as Record<string, unknown>;
				const current = sanitizeNodeDocGeneratedExplanation((nodeDoc as any)?.generated);
				if (
					current &&
					current.signature_key === generated.signature_key &&
					current.summary === generated.summary
				) {
					return node;
				}
				changed = true;
				return {
					...node,
					data: {
						...(node.data ?? {}),
						meta: {
							...meta,
							updatedAt: new Date().toISOString(),
							nodeDoc: {
								...nodeDoc,
								generated: generated as NodeDocGeneratedExplanation
							}
						}
					}
				};
			});
			if (!changed) return s;
			const next = { ...s, nodes };
			persist(next);
			return next;
		});
		return changed ? { ok: true } : { ok: false, reason: 'no_change' };
	}

	function clearNodeDocGeneratedExplanation(nodeIdRaw: string): { ok: boolean; reason?: string } {
		const nodeId = String(nodeIdRaw ?? '').trim();
		if (!nodeId) return { ok: false, reason: 'missing_node_id' };
		let changed = false;
		update((s) => {
			const nodes = (s.nodes as any[]).map((node) => {
				if (String(node?.id ?? '') !== nodeId) return node;
				const meta = ((node?.data as any)?.meta ?? {}) as Record<string, unknown>;
				const nodeDoc = ((meta as any)?.nodeDoc ?? {}) as Record<string, unknown>;
				if (!((nodeDoc as any)?.generated && typeof (nodeDoc as any)?.generated === 'object')) return node;
				changed = true;
				const nextNodeDoc = { ...nodeDoc } as Record<string, unknown>;
				delete (nextNodeDoc as any).generated;
				return {
					...node,
					data: {
						...(node.data ?? {}),
						meta: {
							...meta,
							updatedAt: new Date().toISOString(),
							nodeDoc: nextNodeDoc
						}
					}
				};
			});
			if (!changed) return s;
			const next = { ...s, nodes };
			persist(next);
			return next;
		});
		return changed ? { ok: true } : { ok: false, reason: 'no_change' };
	}

	function clearDraft() {
		clearGraphDraft();
	}

	async function saveGraph(message?: string, opts?: { graphName?: string }) {
		const current = getState();
		if (current.editingContext === 'component') {
			return {
				ok: false,
				reason: 'in_component_edit' as const,
				error:
					'Cannot save graph while in component edit mode. Exit component edit, or use saveComponentRevision.'
			};
		}
		const graphId = String(current.graphId ?? '').trim();
		if (!graphId) return { ok: false, reason: 'missing_graph_id' as const };
		const pendingDraftDiagnostic = pendingInspectorDraftSaveDiagnostic(current);
		if (pendingDraftDiagnostic) {
			return {
				ok: false,
				reason: 'preflight_failed' as const,
				error: summarizeSavePreflightError([pendingDraftDiagnostic]),
				diagnostics: [pendingDraftDiagnostic]
			};
		}
		const preflight = buildSavePreflightDiagnostics(current.nodes as any, current.edges as any);
		if (!preflight.ok) {
			return {
				ok: false,
				reason: 'preflight_failed' as const,
				error: summarizeSavePreflightError(preflight.diagnostics),
				diagnostics: preflight.diagnostics
			};
		}
		const strictGraph = buildPersistableGraphStrict(
			current.nodes as any,
			current.edges as any,
			graphId,
			current.checkpointRegistry ?? {}
		);
		if (!strictGraph.ok) return { ok: false, reason: 'invalid_graph' as const, error: strictGraph.error };
		const graph = strictGraph.graph;
		const canvasGraph = stripToDTO(
			current.nodes as any,
			current.edges as any,
			graphId,
			current.checkpointRegistry ?? {}
		);
		const strictCanvasGraph = buildPersistableGraphStrict(
			canvasGraph.nodes as any,
			canvasGraph.edges as any,
			graphId,
			current.checkpointRegistry ?? {}
		);
		if (!strictCanvasGraph.ok) {
			return { ok: false, reason: 'invalid_graph' as const, error: strictCanvasGraph.error };
		}
		const consistencyMismatch = computeSaveConsistencyMismatch(strictCanvasGraph.graph, graph);
		if (consistencyMismatch) {
			const diag: SavePreflightDiagnostic = {
				code: 'SAVE_CONSISTENCY_MISMATCH',
				path: 'graph',
				message: 'Save blocked: persisted payload is inconsistent with current canvas graph.',
				severity: 'error'
			};
			return {
				ok: false,
				reason: 'consistency_mismatch' as const,
				error: summarizeSavePreflightError([diag]),
				diagnostics: [diag],
				consistency: consistencyMismatch
			};
		}
		try {
			const created = await createGraphRevision({
				graphId,
				graphName: String(opts?.graphName ?? '').trim() || undefined,
				revisionKind: 'save_graph',
				message: String(message ?? '').trim() || undefined,
				graph
			});
			return {
				ok: true,
				graphId: String(created.graphId),
				graphName: created.graphName ?? null,
				revisionId: String(created.revisionId),
				createdAt: String(created.createdAt),
				diagnostics: preflight.diagnostics
			};
		} catch (error) {
			return { ok: false, reason: 'save_failed' as const, error: String(error) };
		}
	}

	async function saveGraphVersion(versionName: string, message?: string) {
		const current = getState();
		if (current.editingContext === 'component') {
			return {
				ok: false,
				reason: 'in_component_edit' as const,
				error:
					'Cannot save graph while in component edit mode. Exit component edit, or use saveComponentRevision.'
			};
		}
		const graphId = String(current.graphId ?? '').trim();
		const nextVersionName = String(versionName ?? '').trim();
		if (!graphId) return { ok: false, reason: 'missing_graph_id' as const };
		if (!nextVersionName) return { ok: false, reason: 'missing_version_name' as const };
		const pendingDraftDiagnostic = pendingInspectorDraftSaveDiagnostic(current);
		if (pendingDraftDiagnostic) {
			return {
				ok: false,
				reason: 'preflight_failed' as const,
				error: summarizeSavePreflightError([pendingDraftDiagnostic]),
				diagnostics: [pendingDraftDiagnostic]
			};
		}
		const preflight = buildSavePreflightDiagnostics(current.nodes as any, current.edges as any);
		if (!preflight.ok) {
			return {
				ok: false,
				reason: 'preflight_failed' as const,
				error: summarizeSavePreflightError(preflight.diagnostics),
				diagnostics: preflight.diagnostics
			};
		}
		const strictGraph = buildPersistableGraphStrict(
			current.nodes as any,
			current.edges as any,
			graphId,
			current.checkpointRegistry ?? {}
		);
		if (!strictGraph.ok) return { ok: false, reason: 'invalid_graph' as const, error: strictGraph.error };
		const graph = strictGraph.graph;
		const canvasGraph = stripToDTO(
			current.nodes as any,
			current.edges as any,
			graphId,
			current.checkpointRegistry ?? {}
		);
		const strictCanvasGraph = buildPersistableGraphStrict(
			canvasGraph.nodes as any,
			canvasGraph.edges as any,
			graphId,
			current.checkpointRegistry ?? {}
		);
		if (!strictCanvasGraph.ok) {
			return { ok: false, reason: 'invalid_graph' as const, error: strictCanvasGraph.error };
		}
		const consistencyMismatch = computeSaveConsistencyMismatch(strictCanvasGraph.graph, graph);
		if (consistencyMismatch) {
			const diag: SavePreflightDiagnostic = {
				code: 'SAVE_CONSISTENCY_MISMATCH',
				path: 'graph',
				message: 'Save blocked: persisted payload is inconsistent with current canvas graph.',
				severity: 'error'
			};
			return {
				ok: false,
				reason: 'consistency_mismatch' as const,
				error: summarizeSavePreflightError([diag]),
				diagnostics: [diag],
				consistency: consistencyMismatch
			};
		}
		try {
			const created = await createGraphRevision({
				graphId,
				versionName: nextVersionName,
				revisionKind: 'save_version',
				message: String(message ?? '').trim() || undefined,
				graph
			});
			return {
				ok: true,
				graphId: String(created.graphId),
				revisionId: String(created.revisionId),
				versionName: created.versionName ?? null,
				createdAt: String(created.createdAt),
				diagnostics: preflight.diagnostics
			};
		} catch (error) {
			return { ok: false, reason: 'save_failed' as const, error: String(error) };
		}
	}

	async function saveGraphAs(graphName: string, message?: string, versionName?: string) {
		const nextGraphName = String(graphName ?? '').trim();
		if (!nextGraphName) return { ok: false, reason: 'missing_graph_name' as const };
		const current = getState();
		if (current.editingContext === 'component') {
			return {
				ok: false,
				reason: 'in_component_edit' as const,
				error:
					'Cannot save graph while in component edit mode. Exit component edit, or use saveComponentRevision.'
			};
		}
		const pendingDraftDiagnostic = pendingInspectorDraftSaveDiagnostic(current);
		if (pendingDraftDiagnostic) {
			return {
				ok: false,
				reason: 'preflight_failed' as const,
				error: summarizeSavePreflightError([pendingDraftDiagnostic]),
				diagnostics: [pendingDraftDiagnostic]
			};
		}
		const preflight = buildSavePreflightDiagnostics(current.nodes as any, current.edges as any);
		if (!preflight.ok) {
			return {
				ok: false,
				reason: 'preflight_failed' as const,
				error: summarizeSavePreflightError(preflight.diagnostics),
				diagnostics: preflight.diagnostics
			};
		}
		const strictGraph = buildPersistableGraphStrict(
			current.nodes as any,
			current.edges as any,
			current.graphId,
			current.checkpointRegistry ?? {}
		);
		if (!strictGraph.ok) return { ok: false, reason: 'invalid_graph' as const, error: strictGraph.error };
		const graph = strictGraph.graph;
		const canvasGraph = stripToDTO(
			current.nodes as any,
			current.edges as any,
			current.graphId,
			current.checkpointRegistry ?? {}
		);
		const strictCanvasGraph = buildPersistableGraphStrict(
			canvasGraph.nodes as any,
			canvasGraph.edges as any,
			current.graphId,
			current.checkpointRegistry ?? {}
		);
		if (!strictCanvasGraph.ok) {
			return { ok: false, reason: 'invalid_graph' as const, error: strictCanvasGraph.error };
		}
		const consistencyMismatch = computeSaveConsistencyMismatch(strictCanvasGraph.graph, graph);
		if (consistencyMismatch) {
			const diag: SavePreflightDiagnostic = {
				code: 'SAVE_CONSISTENCY_MISMATCH',
				path: 'graph',
				message: 'Save blocked: persisted payload is inconsistent with current canvas graph.',
				severity: 'error'
			};
			return {
				ok: false,
				reason: 'consistency_mismatch' as const,
				error: summarizeSavePreflightError([diag]),
				diagnostics: [diag],
				consistency: consistencyMismatch
			};
		}
		try {
			const created = await createGraphRevision({
				graphName: nextGraphName,
				versionName: String(versionName ?? '').trim() || undefined,
				revisionKind: 'save_graph_as',
				message: String(message ?? '').trim() || undefined,
				graph
			});
			update((s) => {
				const next = { ...s, graphId: String(created.graphId) };
				persist(next);
				return next;
			});
			return {
				ok: true,
				graphId: String(created.graphId),
				graphName: created.graphName ?? null,
				revisionId: String(created.revisionId),
				createdAt: String(created.createdAt),
				diagnostics: preflight.diagnostics
			};
		} catch (error) {
			return { ok: false, reason: 'save_failed' as const, error: String(error) };
		}
	}

	async function saveComponentRevision(opts?: { message?: string }) {
		const s = getState();
		const session = s.componentEditSession;
		if (!session || s.editingContext !== 'component') {
			return { ok: false, reason: 'not_in_component_edit' as const };
		}
		const cid = String(session.componentId ?? '').trim();
		const rid = String(session.revisionId ?? '').trim();
		if (!cid || !rid) return { ok: false, reason: 'missing_component_ref' as const };

		const cleanNodes = (s.nodes as any[]).map((node) => {
			const freeze = (node?.data as any)?.meta?.freeze;
			if ((freeze as any)?.mode !== 'per_run') return node;
			const nextMeta = { ...(((node?.data as any)?.meta ?? {}) as Record<string, unknown>) };
			delete (nextMeta as any).freeze;
			delete (nextMeta as any).freezeLineage;
			return {
				...node,
				data: {
					...(node.data ?? {}),
					meta: nextMeta
				}
			};
		});
		const internalNodeIds = new Set(
			(cleanNodes as any[]).map((node) => String((node as any)?.id ?? '').trim()).filter(Boolean)
		);
		const internalCheckpointRegistry = Object.fromEntries(
			Object.entries((s.checkpointRegistry ?? {}) as Record<string, unknown>).filter(([nodeId]) =>
				internalNodeIds.has(String(nodeId ?? '').trim())
			)
		) as Record<string, unknown>;
		const strictGraph = buildPersistableGraphStrict(
			cleanNodes as any,
			s.edges as any,
			undefined,
			internalCheckpointRegistry
		);
		if (!strictGraph.ok) return { ok: false, reason: 'invalid_graph' as const, error: strictGraph.error };

		try {
			const existingDetail = await getComponentRevision(cid, rid);
			const created = await createComponentRevision({
				componentId: cid,
				parentRevisionId: rid,
				message: String(opts?.message ?? '').trim() || 'save_component',
				schemaVersion: Number((existingDetail as any)?.schemaVersion ?? 1) || 1,
				graph: {
					nodes: strictGraph.graph.nodes,
					edges: strictGraph.graph.edges,
					checkpointRegistry: structuredClone(
						(strictGraph.graph as any).checkpointRegistry ?? internalCheckpointRegistry
					)
				},
				api: ((existingDetail?.definition?.api ?? { inputs: [], outputs: [] }) as ComponentApiContract),
				configSchema: structuredClone(
					(existingDetail?.definition?.configSchema ?? {}) as Record<string, unknown>
				),
				exposureRegistry: structuredClone(
					Array.isArray((existingDetail?.definition as any)?.exposureRegistry)
						? ((existingDetail?.definition as any).exposureRegistry as unknown[])
						: []
				)
			});
			const nextRid = String((created as any)?.revisionId ?? '').trim();
			if (!nextRid) return { ok: false, reason: 'save_failed' as const, error: 'missing_revision_id' };
			const updateResult = updateComponentEditSessionRevision(nextRid);
			if (!(updateResult as any)?.ok) {
				return {
					ok: false,
					reason: 'save_failed' as const,
					error: String((updateResult as any)?.reason ?? 'revision_update_failed')
				};
			}
			const oldCacheKey = `${cid}@${rid}`;
			const newCacheKey = `${cid}@${nextRid}`;
			update((state) => {
				const prevCache = (state.componentContractDraftCache ?? {}) as Record<string, unknown>;
				const oldEntry =
					oldCacheKey && oldCacheKey in prevCache ? (prevCache[oldCacheKey] as Record<string, unknown>) : null;
				const nextCache = Object.fromEntries(
					Object.entries(prevCache).filter(([k]) => k !== oldCacheKey)
				) as Record<string, unknown>;
				if (oldEntry && nextCache[newCacheKey] == null) {
					nextCache[newCacheKey] = oldEntry;
				}
				const nextState = {
					...state,
					componentContractDraftCache: nextCache
				};
				persist(nextState);
				return nextState;
			});
			return { ok: true, revisionId: nextRid };
		} catch (error) {
			return { ok: false, reason: 'save_failed' as const, error: String(error) };
		}
	}

	async function listGraphs(limit = 50, offset = 0) {
		try {
			const listed = await listGraphsClient(limit, offset);
			return {
				ok: true,
				graphs: Array.isArray(listed.graphs) ? listed.graphs : []
			};
		} catch (error) {
			return { ok: false, reason: 'list_failed' as const, error: String(error) };
		}
	}

	async function listGraphRevisionHistory(limit = 30, offset = 0) {
		const current = getState();
		const graphId = String(current.graphId ?? '').trim();
		if (!graphId) return { ok: false, reason: 'missing_graph_id' as const };
		try {
			const listed = await listGraphRevisions(graphId, limit, offset);
			return {
				ok: true,
				graphId,
				revisions: Array.isArray(listed.revisions) ? listed.revisions : []
			};
		} catch (error) {
			return { ok: false, reason: 'list_failed' as const, error: String(error) };
		}
	}

	async function listGraphRevisionHistoryForGraph(graphId: string, limit = 30, offset = 0) {
		const gid = String(graphId ?? '').trim();
		if (!gid) return { ok: false, reason: 'missing_graph_id' as const };
		try {
			const listed = await listGraphRevisions(gid, limit, offset);
			return {
				ok: true,
				graphId: gid,
				revisions: Array.isArray(listed.revisions) ? listed.revisions : []
			};
		} catch (error) {
			return { ok: false, reason: 'list_failed' as const, error: String(error) };
		}
	}

	async function restoreGraphRevision(revisionId: string) {
		const current = getState();
		const graphId = String(current.graphId ?? '').trim();
		const rid = String(revisionId ?? '').trim();
		if (!graphId) return { ok: false, reason: 'missing_graph_id' as const };
		if (!rid) return { ok: false, reason: 'missing_revision_id' as const };
		try {
			const restored = await getGraphRevision(graphId, rid);
			const graph = (restored?.graph ?? {}) as any;
			const applied = applyGraphDocument(graph, restored.graphId);
			if (!applied.ok) return { ok: false, reason: 'invalid_payload' as const };
			return {
				ok: true,
				graphId: String(restored.graphId),
				revisionId: String(restored.revisionId)
			};
		} catch (error) {
			return { ok: false, reason: 'restore_failed' as const, error: String(error) };
		}
	}

	async function loadGraphRevision(graphId: string, revisionId: string) {
		const gid = String(graphId ?? '').trim();
		const rid = String(revisionId ?? '').trim();
		if (!gid) return { ok: false, reason: 'missing_graph_id' as const };
		if (!rid) return { ok: false, reason: 'missing_revision_id' as const };
		try {
			const restored = await getGraphRevision(gid, rid);
			const graph = (restored?.graph ?? {}) as any;
			const applied = applyGraphDocument(graph, restored.graphId);
			if (!applied.ok) return { ok: false, reason: 'invalid_payload' as const };
			return {
				ok: true,
				graphId: String(restored.graphId),
				graphName: restored.graphName ?? null,
				revisionId: String(restored.revisionId)
			};
		} catch (error) {
			return { ok: false, reason: 'restore_failed' as const, error: String(error) };
		}
	}

	async function deleteGraph(graphId: string) {
		const gid = String(graphId ?? '').trim();
		if (!gid) return { ok: false, reason: 'missing_graph_id' as const };
		try {
			const deleted = await deleteGraphClient(gid);
			return { ok: true, deleted };
		} catch (error) {
			return { ok: false, reason: 'delete_failed' as const, error: String(error) };
		}
	}

	async function deleteGraphRevision(graphId: string, revisionId: string) {
		const gid = String(graphId ?? '').trim();
		const rid = String(revisionId ?? '').trim();
		if (!gid) return { ok: false, reason: 'missing_graph_id' as const };
		if (!rid) return { ok: false, reason: 'missing_revision_id' as const };
		try {
			const deleted = await deleteGraphRevisionClient(gid, rid);
			return { ok: true, deleted };
		} catch (error) {
			return { ok: false, reason: 'delete_failed' as const, error: String(error) };
		}
	}

	async function hydrateLatestGraphFromBackend() {
		if (typeof window === 'undefined') return { ok: false, reason: 'non_browser' as const };
		try {
			const current = getState();
			const graphId = String(current.graphId ?? '').trim();
			if (!graphId) return { ok: false, reason: 'missing_graph_id' as const };
			const latest = await getLatestGraphRevision(graphId);
			const graph = (latest?.graph ?? {}) as any;
			const applied = applyGraphDocument(graph, latest.graphId);
			if (!applied.ok) return { ok: false, reason: 'invalid_payload' as const };
			return {
				ok: true,
				graphId: String(latest.graphId),
				graphName: latest.graphName ?? null,
				revisionId: String(latest.revisionId)
			};
		} catch (error) {
			return { ok: false, reason: 'read_failed' as const, error: String(error) };
		}
	}

	async function listComponentCatalog(limit = 100, offset = 0) {
		try {
			const components = await listComponents(limit, offset);
			return { ok: true, components };
		} catch (error) {
			return { ok: false, reason: 'list_components_failed' as const, error: String(error) };
		}
	}

	async function listComponentRevisionHistory(componentId: string, limit = 100, offset = 0) {
		try {
			const revisions = await listComponentRevisions(componentId, limit, offset);
			return { ok: true, revisions };
		} catch (error) {
			return { ok: false, reason: 'list_revisions_failed' as const, error: String(error) };
		}
	}

	async function getComponentRevisionDetail(componentId: string, revisionId: string) {
		try {
			const detail = await getComponentRevision(componentId, revisionId);
			return { ok: true, detail };
		} catch (error) {
			return { ok: false, reason: 'get_revision_failed' as const, error: String(error) };
		}
	}

	async function openComponentRevisionForEditing(componentId: string, revisionId: string, entryNodeId?: string | null) {
		const cid = String(componentId ?? '').trim();
		const rid = String(revisionId ?? '').trim();
		if (!cid || !rid) return { ok: false, reason: 'missing_component_ref' as const };
		try {
			const before = getState();
			const snapshot = captureComponentEditSnapshot(before);
			const parentSession = before.componentEditSession
				? structuredClone(before.componentEditSession)
				: null;
			const detail = await getComponentRevision(cid, rid);
			const draftCacheKey = `${cid}@${rid}`;
			const cachedDraftRaw =
				before.componentContractDraftCache &&
				typeof before.componentContractDraftCache === 'object'
					? (before.componentContractDraftCache[draftCacheKey] as Record<string, any> | undefined)
					: undefined;
			const detailApi = ((detail?.definition?.api ?? { inputs: [], outputs: [] }) as ComponentApiContract);
			const entryId = String(entryNodeId ?? '').trim() || null;
			const entryNode =
				entryId && entryId.length > 0
					? before.nodes.find(
							(n) => String(n.id ?? '') === entryId && String((n.data as any)?.kind ?? '') === 'component'
						)
					: null;
			const entryRef = (((entryNode?.data as any)?.params ?? {})?.componentRef ?? {}) as Record<string, any>;
			const entryComponentId = String(entryRef?.componentId ?? '').trim();
			const entryRevisionId = String(entryRef?.revisionId ?? '').trim();
			const useEntryDraftParams = Boolean(entryNode && entryComponentId === cid && entryRevisionId === rid);
			const entryParams = ((entryNode?.data as any)?.params ?? {}) as Record<string, any>;
			const draftApi =
				cachedDraftRaw && cachedDraftRaw?.api && typeof cachedDraftRaw.api === 'object'
					? (cachedDraftRaw.api as ComponentApiContract)
					: useEntryDraftParams && entryParams?.api && typeof entryParams.api === 'object'
						? (entryParams.api as ComponentApiContract)
						: detailApi;
			const cachedDraftGraph = readComponentDraftGraph(cachedDraftRaw);
			const draftExposureRegistry = normalizeExposureRegistry(
				cachedDraftRaw
					? cachedDraftRaw?.exposureRegistry
					: useEntryDraftParams
						? entryParams?.exposureRegistry
						: (detail?.definition as any)?.exposureRegistry,
				draftApi
			);
			const draftProfiles = materializeExposureProfiles(draftExposureRegistry);
			const contractDraftParams = sanitizeComponentDraftParams({
				componentRef: {
					componentId: cid,
					revisionId: rid,
					apiVersion: 'v1'
				},
				api: draftApi,
				exposureRegistry: draftExposureRegistry,
				published_profile: draftProfiles.published_profile,
				debug_profile: draftProfiles.debug_profile,
				config: {}
			});
			const graph = (
				cachedDraftGraph ?? ((detail?.definition?.graph ?? {}) as { nodes?: unknown[]; edges?: unknown[] })
			) as {
				nodes?: unknown[];
				edges?: unknown[];
				checkpointRegistry?: Record<string, unknown>;
			};
			const revisionCheckpointRegistry =
				(detail?.definition?.graph as any)?.checkpointRegistry &&
				typeof (detail?.definition?.graph as any)?.checkpointRegistry === 'object'
					? structuredClone((detail?.definition?.graph as any).checkpointRegistry)
					: {};
			const draftCheckpointRegistry =
				cachedDraftGraph?.checkpointRegistry && typeof cachedDraftGraph.checkpointRegistry === 'object'
					? structuredClone(cachedDraftGraph.checkpointRegistry)
					: {};
			const mergedCheckpointRegistry = {
				...revisionCheckpointRegistry,
				...draftCheckpointRegistry
			};
			const applied = applyGraphDocument(
				{
					nodes: Array.isArray(graph?.nodes) ? graph.nodes : [],
					edges: Array.isArray(graph?.edges) ? graph.edges : [],
					checkpointRegistry: mergedCheckpointRegistry
				},
				null
			);
			if (!applied.ok) {
				return { ok: false, reason: 'invalid_payload' as const, error: String(applied.reason ?? 'invalid_payload') };
			}
			update((s) => {
				// Strip promoted cmp: entries that belong to the component being opened —
				// they'll be represented natively inside the component edit session.
				const entryPrefix = entryId ? `cmp:${entryId}:` : '';
				const filteredCheckpointRegistry = entryPrefix
					? Object.fromEntries(
							Object.entries((s.checkpointRegistry ?? {}) as Record<string, unknown>).filter(
								([key]) => !key.startsWith(entryPrefix)
							)
						)
					: (s.checkpointRegistry ?? {});
				const next = {
					...s,
					checkpointRegistry: filteredCheckpointRegistry,
					editingContext: 'component' as const,
					componentEditSession: {
						componentId: cid,
						revisionId: rid,
						entryNodeId: entryId,
						contractDraftParams,
						snapshot,
						parentSession
					},
					componentContractDraftCache: {
						...(s.componentContractDraftCache ?? {}),
						[draftCacheKey]: {
							...((cachedDraftRaw ?? {}) as Record<string, unknown>),
							...contractDraftParams,
							[COMPONENT_DRAFT_LAST_COMMITTED_CHECKPOINTS_KEY]:
								structuredClone(revisionCheckpointRegistry)
						}
					},
					lastRunStatus: 'never_run' as const,
					logs: [
						...(Array.isArray(s.logs) ? s.logs : []),
						{
							id: nextLogId(),
							ts: new Date().toLocaleTimeString(),
							level: 'info' as const,
							message: `[component-edit] Loaded internals: ${cid}@${rid}${cachedDraftGraph ? ' (draft graph)' : ''}`
						}
					]
				};
				persist(next);
				return next;
			});
			return { ok: true, detail };
		} catch (error) {
			return { ok: false, reason: 'open_component_failed' as const, error: String(error) };
		}
	}

	function hasUnsavedCheckpointChanges(componentNodeId: string): boolean {
		const nodeId = String(componentNodeId ?? '').trim();
		if (!nodeId) return false;
		const state = getState();
		const node = (state.nodes ?? []).find((candidate) => String((candidate as any)?.id ?? '').trim() === nodeId);
		if (!node || String((node as any)?.data?.kind ?? '').trim().toLowerCase() !== 'component') return false;
		const ref = (((node as any)?.data?.params ?? {}) as any)?.componentRef ?? {};
		const componentId = String(ref?.componentId ?? '').trim();
		const revisionId = String(ref?.revisionId ?? '').trim();
		if (!componentId || !revisionId) return false;
		const cacheKey = `${componentId}@${revisionId}`;
		const cacheEntryRaw =
			state.componentContractDraftCache && typeof state.componentContractDraftCache === 'object'
				? (state.componentContractDraftCache[cacheKey] as Record<string, unknown> | undefined)
				: undefined;
		if (!cacheEntryRaw || typeof cacheEntryRaw !== 'object') return false;
		const draftGraph = readComponentDraftGraph(cacheEntryRaw);
		if (!draftGraph) return false;
		const draftCheckpointRegistry =
			draftGraph.checkpointRegistry && typeof draftGraph.checkpointRegistry === 'object'
				? (draftGraph.checkpointRegistry as Record<string, unknown>)
				: {};
		const lastCommittedRaw = (cacheEntryRaw as Record<string, unknown>)[
			COMPONENT_DRAFT_LAST_COMMITTED_CHECKPOINTS_KEY
		];
		const lastCommitted =
			lastCommittedRaw && typeof lastCommittedRaw === 'object'
				? (lastCommittedRaw as Record<string, unknown>)
				: {};
		return stableJson(lastCommitted) !== stableJson(draftCheckpointRegistry);
	}

	function returnFromComponentEditSession() {
		const state = getState();
		const session = state.componentEditSession;
		if (!session) return { ok: false as const, reason: 'no_component_edit_session' as const };
		const snapshot = session.snapshot;
		const parentSession = session.parentSession ? structuredClone(session.parentSession) : null;
		const cacheKey = `${String(session.componentId ?? '').trim()}@${String(session.revisionId ?? '').trim()}`;
		update((s) => {
			const internalNodeIds = new Set(
				(s.nodes ?? []).map((node) => String((node as any)?.id ?? '').trim()).filter(Boolean)
			);
			const internalCheckpoints = Object.fromEntries(
				Object.entries((s.checkpointRegistry ?? {}) as Record<string, unknown>).filter(([nodeId]) =>
					internalNodeIds.has(String(nodeId ?? '').trim())
				)
			) as Record<string, unknown>;
			const entryNodeId = String(session.entryNodeId ?? '').trim();
			const promotedCheckpoints: Record<string, CheckpointRecord> = {};
			for (const [innerNodeId, rawCheckpoint] of Object.entries(internalCheckpoints)) {
				const checkpoint = rawCheckpoint as CheckpointRecord;
				const promotedKey = entryNodeId ? buildPromotedCheckpointKey(entryNodeId, innerNodeId) : innerNodeId;
				promotedCheckpoints[promotedKey] = {
					...checkpoint,
					nodeId: promotedKey
				};
			}
			const nextEditingContext: EditorContext = parentSession ? 'component' : 'graph';
			const existingDraftCacheEntry =
				cacheKey && s.componentContractDraftCache && typeof s.componentContractDraftCache === 'object'
					? ((s.componentContractDraftCache[cacheKey] as Record<string, any> | undefined) ?? {})
					: {};
			const nextDraftCacheEntry = {
				...(existingDraftCacheEntry ?? {}),
				...(session.contractDraftParams ?? {}),
				[COMPONENT_DRAFT_GRAPH_KEY]: {
					nodes: structuredClone(s.nodes),
					edges: structuredClone(s.edges),
					checkpointRegistry: structuredClone(internalCheckpoints)
				}
			};
			const checkpointRows = Object.values(internalCheckpoints ?? {}) as Array<Record<string, unknown>>;
			const checkpointTotal = checkpointRows.length;
			const checkpointValid = checkpointRows.filter(
				(checkpoint) => String((checkpoint as any)?.staleness ?? '').trim().toLowerCase() === 'valid'
			).length;
			const checkpointStale = checkpointRows.filter((checkpoint) => {
				const staleness = String((checkpoint as any)?.staleness ?? '').trim().toLowerCase();
				return staleness === 'stale' || staleness === 'artifact_missing';
			}).length;
			const nextSnapshotNodes = structuredClone(snapshot.nodes).map((node) => {
				if (String((node as any)?.id ?? '').trim() !== String(session.entryNodeId ?? '').trim()) return node;
				const meta = { ...((((node as any)?.data ?? {}) as any)?.meta ?? {}) } as Record<string, unknown>;
				if (checkpointTotal > 0) {
					(meta as any).checkpointSummary = {
						total: checkpointTotal,
						valid: checkpointValid,
						stale: checkpointStale
					};
				} else {
					delete (meta as any).checkpointSummary;
				}
				return {
					...node,
					data: {
						...((node as any)?.data ?? {}),
						meta
					}
				};
			});
			const next: GraphState = {
				...s,
				graphId: snapshot.graphId,
				nodes: nextSnapshotNodes,
				edges: structuredClone(snapshot.edges),
				selectedNodeId: snapshot.selectedNodeId,
				inspector: structuredClone(snapshot.inspector),
				logs: [
					...structuredClone(snapshot.logs),
					{
						id: nextLogId(),
						ts: new Date().toLocaleTimeString(),
						level: 'info',
						message: `[component-edit] Returned to graph context from ${session.componentId}@${session.revisionId}`
					}
				],
				runStatus: snapshot.runStatus,
				lastRunStatus: snapshot.lastRunStatus,
				freshness: snapshot.freshness,
				staleNodeCount: snapshot.staleNodeCount,
				activeRunMode: snapshot.activeRunMode,
				activeRunFrom: snapshot.activeRunFrom,
				activeRunNodeSet: new Set(Array.from(snapshot.activeRunNodeSet ?? [])),
				nodeOutputs: structuredClone(snapshot.nodeOutputs),
				nodeBindings: ensureNormalizedBindingsForNodes(
					structuredClone(snapshot.nodes) as any,
					structuredClone(snapshot.nodeBindings) as any
				),
				activeRunId: snapshot.activeRunId,
				checkpointRegistry: {
						...structuredClone(snapshot.checkpointRegistry ?? {}),
						...promotedCheckpoints
					},
				editingContext: nextEditingContext,
				componentEditSession: parentSession,
				componentContractDraftCache: cacheKey
					? {
							...(s.componentContractDraftCache ?? {}),
							[cacheKey]: nextDraftCacheEntry
						}
					: (s.componentContractDraftCache ?? {})
			};
			persist(next);
			return withGraphMeta(next);
		}, { source: 'graph_edit' });
		return { ok: true as const, hasParentSession: Boolean(parentSession) };
	}

	function updateComponentEditSessionRevision(revisionId: string) {
		const rid = String(revisionId ?? '').trim();
		if (!rid) return { ok: false as const, reason: 'missing_revision_id' as const };
		let updated = false;
		update((s) => {
			const session = s.componentEditSession;
			if (!session) return s;
			if (String(session.revisionId ?? '').trim() === rid) return s;
			updated = true;
			const next: GraphState = {
				...s,
				componentEditSession: {
					...session,
					revisionId: rid,
					contractDraftParams: sanitizeComponentDraftParams({
						...(session.contractDraftParams ?? {}),
						componentRef: {
							...(((session.contractDraftParams ?? {}) as Record<string, any>).componentRef ?? {}),
							componentId: String(session.componentId ?? '').trim(),
							revisionId: rid
						},

					})
				},
				logs: [
					...(Array.isArray(s.logs) ? s.logs : []),
					{
						id: nextLogId(),
						ts: new Date().toLocaleTimeString(),
						level: 'info',
						message: `[component-edit] Active revision updated: ${session.componentId}@${rid}`
					}
				]
			};
			persist(next);
			return next;
		});
		if (!updated) return { ok: false as const, reason: 'no_component_edit_session' as const };
		return { ok: true as const, revisionId: rid };
	}

	function patchComponentEditContractDraft(
		patch: Record<string, any>,
		opts?: { intent?: InspectorDraftPatchIntent; notice?: string | null }
	) {
		const intent: InspectorDraftPatchIntent = opts?.intent ?? 'user_edit';
		let updated = false;
		update((s) => {
			const session = s.componentEditSession;
			if (!session) return s;
			const nextDraftRaw = {
				...(session.contractDraftParams ?? {}),
				...(patch ?? {})
			};
			const nextDraft = sanitizeComponentDraftParams(nextDraftRaw);
			const baseline = sanitizeComponentDraftParams(session.contractDraftParams ?? {});
			const changed = stableJson(nextDraft) !== stableJson(baseline);
			const cacheKey = `${String(session.componentId ?? '').trim()}@${String(session.revisionId ?? '').trim()}`;
			const existingCacheEntry =
				cacheKey && s.componentContractDraftCache && typeof s.componentContractDraftCache === 'object'
					? ((s.componentContractDraftCache[cacheKey] as Record<string, any> | undefined) ?? {})
					: {};
			const nextNotice =
				intent === 'system_canonicalize' && changed
					? String(opts?.notice ?? 'Component contract normalized automatically.')
					: null;
			const nextCacheEntry =
				COMPONENT_DRAFT_GRAPH_KEY in (existingCacheEntry ?? {})
					? {
							...nextDraft,
							[COMPONENT_DRAFT_GRAPH_KEY]:
								(existingCacheEntry as Record<string, unknown>)[COMPONENT_DRAFT_GRAPH_KEY]
						}
					: nextDraft;
			updated = true;
			return {
				...s,
				componentEditSession: {
					...session,
					contractDraftParams: nextDraft
				},
				componentContractDraftCache: cacheKey
					? {
							...(s.componentContractDraftCache ?? {}),
							[cacheKey]: nextCacheEntry
						}
					: (s.componentContractDraftCache ?? {}),
				inspector: {
					...s.inspector,
					systemNotice: nextNotice
				}
			};
		});
		if (!updated) return { ok: false as const, reason: 'no_component_edit_session' as const };
		return { ok: true as const };
	}

	function applySavedComponentRevisionToReturnGraph(
		componentId: string,
		fromRevisionId: string,
		toRevisionId: string,
		scope: 'none' | 'one' | 'all'
	) {
		const cid = String(componentId ?? '').trim();
		const fromRid = String(fromRevisionId ?? '').trim();
		const toRid = String(toRevisionId ?? '').trim();
		const mode = scope === 'all' || scope === 'none' ? scope : 'one';
		if (!cid || !fromRid || !toRid) return { ok: false as const, reason: 'missing_revision_context' as const };
		let applied = false;
		let matchedCount = 0;
		let updatedCount = 0;
		let entryMatched = false;
		update((s) => {
			const session = s.componentEditSession;
			if (!session) return s;
			const snapshot = session.snapshot;
			const draftParams = ((session.contractDraftParams ?? {}) as Record<string, any>);
			const draftApi = draftParams?.api && typeof draftParams.api === 'object'
				? structuredClone(draftParams.api)
				: null;
			const draftExposureRegistry = Array.isArray(draftParams?.exposureRegistry)
				? structuredClone(draftParams.exposureRegistry)
				: null;
			const draftPublishedProfile = Array.isArray(draftParams?.published_profile)
				? structuredClone(draftParams.published_profile)
				: null;
			const draftDebugProfile = Array.isArray(draftParams?.debug_profile)
				? structuredClone(draftParams.debug_profile)
				: null;
			const matchingNodeIds = (snapshot.nodes ?? [])
				.filter((n) => {
					if (n.data?.kind !== 'component') return false;
					const ref = (((n.data as any)?.params ?? {}) as any)?.componentRef ?? {};
					const nodeComponentId = String(ref?.componentId ?? '').trim();
					const nodeRevisionId = String(ref?.revisionId ?? '').trim();
					return nodeComponentId === cid && nodeRevisionId === fromRid;
				})
				.map((n) => String(n.id));
			matchedCount = matchingNodeIds.length;
			const targetIds = new Set<string>();
			if (mode === 'all') {
				for (const id of matchingNodeIds) targetIds.add(id);
			} else if (mode === 'one') {
				const entryNodeId = String(session.entryNodeId ?? '').trim();
				if (entryNodeId && matchingNodeIds.includes(entryNodeId)) {
					entryMatched = true;
					targetIds.add(entryNodeId);
				}
			}
			const nextSnapshotNodes = (snapshot.nodes ?? []).map((n) => {
				if (!targetIds.has(String(n.id))) return n;
				updatedCount += 1;
				const params = structuredClone(((n.data as any)?.params ?? {}) as Record<string, unknown>);
				const existingRef = ((params as any)?.componentRef ?? {}) as Record<string, unknown>;
				return {
					...n,
					data: {
						...n.data,
						params: {
							...params,
							...(draftApi ? { api: draftApi } : {}),
							...(draftExposureRegistry ? { exposureRegistry: draftExposureRegistry } : {}),
							...(draftPublishedProfile ? { published_profile: draftPublishedProfile } : {}),
							...(draftDebugProfile ? { debug_profile: draftDebugProfile } : {}),
							componentRef: {
								...existingRef,
								componentId: cid,
								revisionId: toRid
							}
						},
						meta: {
							...(n.data?.meta ?? {}),
							componentLatestRevisionId: toRid,
							componentHasUpdate: false,
							updatedAt: new Date().toISOString()
						}
					}
				};
			});
			const nextSnapshotInspector = (() => {
				const currentInspector = snapshot.inspector ?? INITIAL_INSPECTOR;
				const inspectorNodeId = String((currentInspector as any)?.nodeId ?? '').trim();
				if (!inspectorNodeId) return currentInspector;
				if (!targetIds.has(inspectorNodeId)) return currentInspector;
				const refreshedNode = nextSnapshotNodes.find((n) => String((n as any)?.id ?? '') === inspectorNodeId);
				if (!refreshedNode) return currentInspector;
				return {
					...currentInspector,
					draftParams: structuredClone((((refreshedNode as any)?.data ?? {})?.params ?? {}) as Record<string, any>),
					dirty: false
				};
			})();
			const modeLabel = mode === 'all' ? 'all' : mode === 'none' ? 'none' : 'one';
			const nextLogMessage = `[component-edit] Save apply scope=${modeLabel} updated=${updatedCount}/${matchedCount} ${cid}@${fromRid} -> ${cid}@${toRid}`;
			const nextSnapshotLogs = [
				...(Array.isArray(snapshot.logs) ? structuredClone(snapshot.logs) : []),
				{
					id: nextLogId(),
					ts: new Date().toLocaleTimeString(),
					level: 'info' as const,
					message: nextLogMessage
				}
			];
			const next: GraphState = {
				...s,
				componentEditSession: {
					...session,
					revisionId: toRid,
					contractDraftParams: sanitizeComponentDraftParams({
						...(session.contractDraftParams ?? {}),
						componentRef: {
							...(((session.contractDraftParams ?? {}) as Record<string, any>).componentRef ?? {}),
							componentId: cid,
							revisionId: toRid
						}
					}),
					snapshot: {
						...snapshot,
						nodes: nextSnapshotNodes,
						inspector: nextSnapshotInspector,
						logs: nextSnapshotLogs
					}
				},
				componentContractDraftCache: (() => {
					const prevCache = (s.componentContractDraftCache ?? {}) as Record<string, unknown>;
					const oldKey = `${cid}@${fromRid}`;
					const newKey = `${cid}@${toRid}`;
					const oldEntry = oldKey in prevCache ? (prevCache[oldKey] as Record<string, unknown>) : null;
					// Migrate the old cache entry (including __lastCommittedCheckpointRegistry)
					// to the new key so the committed baseline is not lost when the revision changes.
					if (oldEntry && !(newKey in prevCache)) {
						const nextCache = { ...prevCache, [newKey]: oldEntry };
						delete (nextCache as Record<string, unknown>)[oldKey];
						return nextCache;
					}
					return prevCache;
				})(),
				logs: [
					...(Array.isArray(s.logs) ? s.logs : []),
					{
						id: nextLogId(),
						ts: new Date().toLocaleTimeString(),
						level: 'info',
						message: nextLogMessage
					}
				]
			};
			applied = true;
			persist(next);
			return next;
		});
		if (!applied) return { ok: false as const, reason: 'no_component_edit_session' as const };
		return { ok: true as const, scope: mode, matchedCount, updatedCount, entryMatched };
	}

	async function forkComponentRevisionToNode(
		nodeId: string,
		fromComponentId: string,
		fromRevisionId: string,
		nextComponentId: string,
		opts?: { revisionId?: string; message?: string }
	) {
		const sourceComponentId = String(fromComponentId ?? '').trim();
		const sourceRevisionId = String(fromRevisionId ?? '').trim();
		const targetComponentId = String(nextComponentId ?? '').trim();
		const targetRevisionId = String(opts?.revisionId ?? '').trim();
		const message = String(opts?.message ?? '').trim() || `fork:${sourceComponentId}@${sourceRevisionId}`;
		if (!nodeId) return { ok: false, reason: 'missing_node_id' as const };
		if (!sourceComponentId || !sourceRevisionId) {
			return { ok: false, reason: 'missing_source_ref' as const };
		}
		if (!targetComponentId) return { ok: false, reason: 'missing_target_component_id' as const };
		try {
			const source = await getComponentRevision(sourceComponentId, sourceRevisionId);
			const created = await createComponentRevision({
				componentId: targetComponentId,
				revisionId: targetRevisionId || undefined,
				parentRevisionId: undefined,
				message,
				schemaVersion: Number(source?.schemaVersion ?? 1) || 1,
				graph: {
					nodes: structuredClone(((source?.definition?.graph as any)?.nodes ?? []) as unknown[]),
					edges: structuredClone(((source?.definition?.graph as any)?.edges ?? []) as unknown[])
				},
				api: structuredClone(
					((source?.definition?.api as ComponentApiContract | undefined) ?? {
						inputs: [],
						outputs: []
					}) as ComponentApiContract
				),
				configSchema: structuredClone((source?.definition?.configSchema ?? {}) as Record<string, unknown>),
				exposureRegistry: structuredClone(
					(Array.isArray((source?.definition as any)?.exposureRegistry)
						? ((source?.definition as any).exposureRegistry as unknown[])
						: []) as any
				)
			});
			const apply = await applyComponentRevisionToNode(
				nodeId,
				String(created.componentId ?? targetComponentId),
				String(created.revisionId ?? '')
			);
			if (!(apply as any)?.ok) {
				return {
					ok: false,
					reason: 'fork_apply_failed' as const,
					error: String((apply as any)?.error ?? (apply as any)?.reason ?? 'unknown')
				};
			}
			return { ok: true, created, applied: apply };
		} catch (error) {
			return { ok: false, reason: 'fork_failed' as const, error: String(error) };
		}
	}

	async function renameComponent(componentId: string, nextComponentId: string) {
		try {
			const renamed = await renameComponentClient(componentId, nextComponentId);
			const fromId = String(componentId ?? '').trim();
			const toId = String((renamed as any)?.componentId ?? nextComponentId ?? '').trim();
			if (fromId && toId && fromId !== toId) {
				const state = getState();
				const componentNodeIds = state.nodes
					.filter((n) => {
						if (n.data.kind !== 'component') return false;
						const currentId = String(((n.data.params as any)?.componentRef?.componentId ?? '')).trim();
						return currentId === fromId;
					})
					.map((n) => n.id);
				for (const nodeId of componentNodeIds) {
					const node = getState().nodes.find((n) => n.id === nodeId);
					const existingRef = ((node?.data?.params as any)?.componentRef ?? {}) as Record<string, unknown>;
					const patch = {
						componentRef: {
							...existingRef,
							componentId: toId
						}
					};
					const result = updateNodeConfig(nodeId, { params: patch });
					if (!result.ok) {
						return {
							ok: false,
							reason: 'rename_component_failed' as const,
							error: String(result.error ?? 'Failed to update component node reference')
						};
					}
				}
				update((s) => {
					const draftComponentRef = ((s.inspector.draftParams ?? {}) as Record<string, any>)
						.componentRef as Record<string, any> | undefined;
					if (String(draftComponentRef?.componentId ?? '').trim() !== fromId) return s;
					return {
						...s,
						inspector: {
							...s.inspector,
							draftParams: {
								...(s.inspector.draftParams ?? {}),
								componentRef: {
									...(draftComponentRef ?? {}),
									componentId: toId
								}
							}
						}
					};
				});
			}
			return { ok: true, renamed };
		} catch (error) {
			return { ok: false, reason: 'rename_component_failed' as const, error: String(error) };
		}
	}

	async function deleteComponent(componentId: string) {
		try {
			const deleted = await deleteComponentClient(componentId);
			return { ok: true, deleted };
		} catch (error) {
			return { ok: false, reason: 'delete_component_failed' as const, error: String(error) };
		}
	}

	async function deleteComponentRevision(componentId: string, revisionId: string) {
		try {
			const deleted = await deleteComponentRevisionClient(componentId, revisionId);
			return { ok: true, deleted };
		} catch (error) {
			return { ok: false, reason: 'delete_component_revision_failed' as const, error: String(error) };
		}
	}

	async function applyComponentRevisionToNode(nodeId: string, componentId: string, revisionId: string) {
		const cid = String(componentId ?? '').trim();
		const rid = String(revisionId ?? '').trim();
		if (!cid || !rid) return { ok: false, reason: 'missing_component_ref' as const };
		const node = getState().nodes.find((n) => n.id === nodeId);
		if (!node || node.data.kind !== 'component') return { ok: false, reason: 'node_not_component' as const };
		try {
			const detail = await getComponentRevision(cid, rid);
			const api = (detail.definition?.api ?? { inputs: [], outputs: [] }) as ComponentApiContract;
			const baseExposureRegistry = normalizeExposureRegistry(
				(detail.definition as any)?.exposureRegistry,
				api
			);
			const nextProfiles = materializeExposureProfiles(baseExposureRegistry);
			const prevApi = (((node.data as any)?.params ?? {})?.api ?? { inputs: [], outputs: [] }) as ComponentApiContract;
			const prevExposureRegistry = normalizeExposureRegistry(
				(((node.data as any)?.params ?? {}) as any)?.exposureRegistry,
				prevApi
			);
			const prevProfiles = materializeExposureProfiles(prevExposureRegistry);
			const compatibilityMapping = (
				((detail.definition as any)?.compatibilityMapping &&
				typeof (detail.definition as any)?.compatibilityMapping === 'object'
					? ((detail.definition as any).compatibilityMapping as Record<string, string>)
					: {}) as Record<string, string>
			);
			const publishedDiff = comparePublishedProfiles(
				prevProfiles.published_profile,
				nextProfiles.published_profile
			);
			if (publishedDiff.breaking) {
				const mapped = new Set(
					Object.keys(compatibilityMapping).filter(
						(fromHandle) =>
							String(fromHandle).trim() &&
							String(compatibilityMapping[fromHandle] ?? '').trim()
					)
				);
				const retypedHandles = new Set(publishedDiff.retyped.map((item) => item.handle_id));
				const removedCovered = publishedDiff.removed.every((handleId) => mapped.has(handleId));
				const retypedCovered = [...retypedHandles].every((handleId) => mapped.has(handleId));
				if (!removedCovered || !retypedCovered) {
					return {
						ok: false,
						reason: 'breaking_component_contract' as const,
						error: `Published handle contract changed without compatibility mapping (removed=${publishedDiff.removed.length}, retyped=${publishedDiff.retyped.length})`,
						details: publishedDiff
					};
				}
			}
			const internalGraph = (detail.definition?.graph ?? { nodes: [], edges: [] }) as {
				nodes?: Array<{ id?: string }>;
				edges?: Array<{ source?: string; target?: string }>;
			};
			const internalNodes = Array.isArray(internalGraph.nodes) ? internalGraph.nodes : [];
			const internalEdges = Array.isArray(internalGraph.edges) ? internalGraph.edges : [];
			const nodeIds = new Set(
				internalNodes
					.map((n) => String(n?.id ?? '').trim())
					.filter((id) => id.length > 0)
			);
			const outDegree = new Map<string, number>();
			for (const id of nodeIds) outDegree.set(id, 0);
			for (const e of internalEdges) {
				const src = String(e?.source ?? '').trim();
				if (nodeIds.has(src)) outDegree.set(src, (outDegree.get(src) ?? 0) + 1);
			}
			const leafNodeId =
				Array.from(nodeIds).find((id) => (outDegree.get(id) ?? 0) === 0) ?? '';
			const firstNodeId = Array.from(nodeIds)[0] ?? '';
			const nodeById = new Map(
				internalNodes
					.map((n) => [String(n?.id ?? '').trim(), n] as const)
					.filter(([id]) => id.length > 0)
			);
			const outputRefForNodeId = (candidateNodeId: string): string | undefined => {
				const rawId = String(candidateNodeId ?? '').trim();
				if (!rawId) return undefined;
				const entry = nodeById.get(rawId) as any;
				if (!entry) return undefined;
				const kind = String(entry?.data?.kind ?? 'node').trim().toLowerCase() || 'node';
				const name = String(entry?.data?.label ?? rawId).trim() || rawId;
				const baseRef = `${kind}:${name}`;
				const outputs = Array.isArray(entry?.data?.params?.api?.outputs)
					? (entry.data.params.api.outputs as any[])
					: [];
				if (kind === 'component' && outputs.length > 0) {
					const outName = String(outputs[0]?.name ?? '').trim();
					return outName ? `${baseRef}|${outName}` : baseRef;
				}
				return baseRef;
			};

			const nextExposureRegistry = [...nextProfiles.published_profile];
			const apiOutputs = Array.isArray(api.outputs) ? api.outputs : [];
			for (const out of apiOutputs) {
				const outName = String((out as any)?.name ?? '').trim();
				if (!outName) continue;
				const idx = nextExposureRegistry.findIndex(
					(rec: any) =>
						String(rec?.kind ?? '').trim().toLowerCase() === 'data_output' &&
						(
							String(rec?.alias ?? '').trim() === outName ||
							String(rec?.handle_id ?? '').trim() === `data_out::${outName}`
						)
				);
				const sourceRef =
					outputRefForNodeId(leafNodeId) ||
					outputRefForNodeId(firstNodeId) ||
					`out:${outName}`;
				if (idx >= 0) {
					const existingSource = String((nextExposureRegistry[idx] as any)?.internal_source_path ?? '').trim();
					const shouldReplaceLegacySource =
						!existingSource ||
						existingSource === `out:${outName}` ||
						existingSource === outName;
					nextExposureRegistry[idx] = {
						...nextExposureRegistry[idx],
						handle_id: `data_out::${outName}`,
						alias: outName,
						internal_source_path: shouldReplaceLegacySource ? sourceRef : existingSource,
						kind: 'data_output',
						published: true,
						exposed: true
					};
				} else {
					nextExposureRegistry.push({
						handle_id: `data_out::${outName}`,
						alias: outName,
						internal_source_path: sourceRef,
						kind: 'data_output',
						native_contract: (out as any)?.typedSchema ?? { type: 'json', fields: [] },
						published: true,
						exposed: true,
						debug_visible: false
					} as any);
				}
			}
			const nextProfilesNormalized = materializeExposureProfiles(nextExposureRegistry as any);
			const paramsPatch = {
				componentRef: {
					componentId: cid,
					revisionId: rid,
					apiVersion: String((node.data.params as any)?.componentRef?.apiVersion ?? 'v1')
				},
				api,
				exposureRegistry: nextExposureRegistry,
				published_profile: nextProfilesNormalized.published_profile,
				debug_profile: nextProfilesNormalized.debug_profile
			};
			const result = updateNodeConfig(nodeId, {
				params: paramsPatch
			}, { allowComponentContractMutation: true });
			if (!result.ok) return { ok: false, reason: 'update_failed' as const, error: result.error };
			const revisions = await listComponentRevisions(cid, 20, 0);
			const latestRevisionId = String(revisions?.[0]?.revisionId ?? '').trim() || null;
			update((s) => {
				const target = s.nodes.find((n) => n.id === nodeId);
				if (!target) return s;
				const refreshedParams = structuredClone((target.data.params ?? {}) as Record<string, unknown>);
				const nodes = s.nodes.map((n) =>
					n.id === nodeId
						? {
								...n,
								data: {
									...n.data,
									params: refreshedParams,
									meta: {
										...(n.data.meta ?? {}),
										componentLatestRevisionId: latestRevisionId,
										componentHasUpdate: Boolean(latestRevisionId && latestRevisionId !== rid),
										updatedAt: new Date().toISOString()
									}
								}
							}
						: n
				);
				const next = { ...s, nodes };
				persist(next);
				return next;
			});
			const stateAfter = getState();
			const refreshedNode = stateAfter.nodes.find((n) => n.id === nodeId);
			const refreshedParams = structuredClone((refreshedNode?.data.params ?? {}) as Record<string, unknown>);
			update((s) => {
				if (s.inspector.nodeId !== nodeId) return s;
				return {
					...s,
					inspector: {
						...s.inspector,
						draftParams: refreshedParams,
						dirty: false
					}
				};
			});
			return {
				ok: true,
				detail,
				latestRevisionId,
				hasUpdate: Boolean(latestRevisionId && latestRevisionId !== rid)
			};
		} catch (error) {
			return { ok: false, reason: 'apply_revision_failed' as const, error: String(error) };
		}
	}

	// suppress unused variable warnings for sets used only inside toolBuiltinPreflightDiagnostics (module-level)
	void allowedPorts;

	return {
		actions: {
			selectNode,
			getSavePreflight,
			resolveNodeInputs,
			getNodeDocResolved,
			getNodeDocExplanationMode,
			setNodeDocExplanationMode,
			getNodeDocTrainingMode,
			setNodeDocTrainingMode,
			setNodeDocRuntimeConfig,
			setNodeDocGeneratedExplanation,
			clearNodeDocGeneratedExplanation,
			clearDraft,
			saveGraph,
			saveGraphVersion,
			saveGraphAs,
			saveComponentRevision,
			listGraphs,
			listGraphRevisionHistory,
			listGraphRevisionHistoryForGraph,
			restoreGraphRevision,
			loadGraphRevision,
			deleteGraph,
			deleteGraphRevision,
			hydrateLatestGraphFromBackend,
			listComponentCatalog,
			listComponentRevisionHistory,
			getComponentRevisionDetail,
			openComponentRevisionForEditing,
			returnFromComponentEditSession,
			updateComponentEditSessionRevision,
			patchComponentEditContractDraft,
			applySavedComponentRevisionToReturnGraph,
			forkComponentRevisionToNode,
			renameComponent,
			deleteComponent,
			deleteComponentRevision,
			applyComponentRevisionToNode,
			hasUnsavedCheckpointChanges,
		}
	};
}
