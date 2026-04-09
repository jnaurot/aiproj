// src/lib/flow/store/graphStore.ts
import { writable, get, derived } from 'svelte/store';
import type { Node, Edge } from '@xyflow/svelte';

import type {
	NodeStatus,
	NodeKind,
	PipelineNodeData,
	PipelineEdgeData,
	PipelineGraphDTO,
	PayloadType
} from '$lib/flow/types';
import { isPayloadType } from '$lib/flow/types/base';
import { defaultSourceParamsByKind } from '$lib/flow/schema/sourceDefaults';
import { defaultLlmParamsByKind } from '$lib/flow/schema/llmDefaults';
import { defaultTransformParamsByKind } from '$lib/flow/schema/transformDefaults';
import { defaultToolParamsByProvider, type ToolProvider } from '$lib/flow/schema/toolDefaults';
import { evaluateSchemaCoercion } from '$lib/flow/schema/coercionPolicy';
import type { SchemaDiagnosticCode } from '$lib/flow/schema/diagnosticsContract';
import { TOOL_BUILTIN_PROFILE_IDS } from '$lib/flow/schema/toolBuiltinProfiles';
import { validateCustomPackageDraft } from '$lib/flow/schema/toolBuiltinCustomPackages';
import {
	getLlmEditorCommitMode,
	getSourceEditorCommitMode,
	getToolEditorCommitMode,
	getTransformEditorCommitMode
} from '$lib/flow/editorCommitPolicy';
import { NodeSchemaEnvelopeSchema } from '$lib/flow/schema/schemaContract';
import {
	createInspectorManager,
	sanitizeComponentDraftParams,
	validateInspectorDraftForAccept,
	pendingInspectorDraftSaveDiagnostic,
	canonicalComponentSourceHandleForEdge,
	normalizeHandleId,
	dedupeEdgesBySignature,
	reconcileComponentOutgoingEdges,
	effectiveExecParamsForNode,
	nodeFreezeMode,
	listComponentOutputNames,
} from './graphStore.inspector';
import {
	normalizeComponentPayloadType,
	deriveNodeIoForData,
	canonicalizeNodeSchemas,
	deriveObservedSchemaObservationFromNodeOutput,
	computeSchemaDriftSummary,
	isEdgeStillValid,
	normalizeEdgeMode,
	buildProvidedSchema,
	buildRequiredSchema,
	isSchemaCompatible,
	edgeContractSnapshotFromSchemas,
	computeEdgeSchemaConstraintsInternal,
	computeEdgeSchemaDiagnosticsInternal,
	payloadHintToTypedSchema,
	fingerprintTypedSchema,
	hasSchemaEnvelopeContent,
	declaredPortHandles,
	sameHandleProvidedSchemaConflict,
	normalizeEdgeLinkKind,
	nodePortAffinity,
	portCardinality,
	edgeModeCompatible,
	normalizeHintType,
	hasPortHandle,
	sourcePayloadHint,
	targetPayloadHint,
	inferEdgeModeFromHandles,
	adapterKindForTypes,
	adapterSuggestionForTypes,
} from './graphStore.node-schema';
export {
	__buildNodeSchemaContractSnapshotForTest,
	deriveNodeIoForData,
	__computeEdgeSchemaConstraintsForTest,
	__computeEdgeSchemaDiagnosticsForTest,
	__normalizeSchemaFieldsForTest,
} from './graphStore.node-schema';
import { defaultNodeData } from '$lib/flow/schema/defaults';
import { updateNodeParamsValidated } from './graph';
import {
	findDuplicateNodeNames,
	findNodeIdByName,
	normalizeNodeName,
	resolveUniqueNodeName
} from './nodeNameUniqueness';
import { saveGraphToLocalStorage, loadGraphFromLocalStorage, emptyGraph, clearGraphDraft } from './persist';
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
	renameComponent,
	deleteComponent,
	deleteComponentRevision,
	type ComponentApiContract
} from '$lib/flow/client/components';
import {
	comparePublishedProfiles,
	materializeExposureProfiles,
	normalizeExposureRegistry
} from '$lib/flow/components/exposureProfiles';
import {
	createMemoizedNodeDocResolver,
	resolveNodeDocForState,
	type NodeDocResolved
} from '$lib/flow/components/nodeDocsViewModel';
import {
	NodeDocExplanationModeSchema,
	sanitizeNodeDocGeneratedExplanation,
	NodeDocTrainingModeSchema,
	type NodeDocExplanationMode,
	type NodeDocGeneratedExplanation,
	type NodeDocTrainingMode
} from '$lib/flow/schema/nodeDocs';
import {
	acceptNodeParams,
	cancelAllRuns,
	createEventBatcher,
	createRun,
	getRun,
	pauseRun,
	resolveSourceNode,
	resumeRun,
	streamRunEvents
} from '$lib/flow/client/runs';
import type { KnownRunEvent } from '$lib/flow/types/run';
import type { SourceKind, LlmKind, TransformKind } from '$lib/flow/types/paramsMap';
import {
	buildRunCreateRequest,
	computeGraphFreshness,
	computePlannedNodeSet,
	planRunConnectedComponents,
	displayStatusFromBinding,
	getStaleFlipNodeIds,
	isBindingStale,
	mergeBindingsSticky,
	type ActiveRunMode,
	type GraphFreshness as ScopeFreshness
} from './runScope';

import type { BindingPair } from './graphStore.bindings';
import type {
	NodeOutputInfo,
	NodeExecutionError,
	NodeBindingInfo,
	NormalizedNodeBinding,
	RunSnapshotLike,
	AuditContext,
	RunLog,
	RunStatus,
	GraphLastRunStatus,
	EdgeExec,
	LogLevel,
	ApiEditorUiState,
	InspectorState,
	InspectorDraftAcceptValidation,
	InspectorDraftPatchIntent,
	SavePreflightSeverity,
	SavePreflightDiagnostic,
	SavePreflightResult,
	SaveConsistencyEntity,
	SaveConsistencyMismatch,
	EditorContext,
	ComponentEditSessionSnapshot,
	ComponentEditSession,
	SchemaCompatibility,
	EdgeCheck,
	EdgeInvalidReason,
	AdapterTransformKind,
	EdgeSchemaConstraint,
	EdgeSchemaDiagnostic,
	NodeSchemaContractEdge,
	NodeSchemaContractSnapshot,
	InputResolution,
	GraphState,
	QueueRuntime,
} from './graphStore.types';
export type {
	NodeOutputInfo,
	NodeExecutionError,
	NodeBindingInfo,
	NormalizedNodeBinding,
	RunSnapshotLike,
	AuditContext,
	RunLog,
	RunStatus,
	GraphLastRunStatus,
	EdgeExec,
	LogLevel,
	ApiEditorUiState,
	InspectorState,
	InspectorDraftAcceptValidation,
	InspectorDraftPatchIntent,
	SavePreflightSeverity,
	SavePreflightDiagnostic,
	SavePreflightResult,
	SaveConsistencyEntity,
	SaveConsistencyMismatch,
	EditorContext,
	ComponentEditSessionSnapshot,
	ComponentEditSession,
	SchemaCompatibility,
	EdgeCheck,
	EdgeInvalidReason,
	AdapterTransformKind,
	EdgeSchemaConstraint,
	EdgeSchemaDiagnostic,
	NodeSchemaContractEdge,
	NodeSchemaContractSnapshot,
	InputResolution,
	GraphState,
	QueueRuntime,
} from './graphStore.types';
export {
	RUN_IDLE,
	NODE_STATUS_IDLE,
	NODE_STATUS_SUCCEEDED,
	INITIAL_INSPECTOR,
} from './graphStore.types';
import { RUN_IDLE, NODE_STATUS_IDLE, NODE_STATUS_SUCCEEDED, INITIAL_INSPECTOR } from './graphStore.types';
import { createHistoryManager, runInHistoryTransaction } from './graphStore.history';
import {
	auditStateTransition,
	withGraphMeta,
	logPush,
	stableJson,
	DEV_MODE,
	nextLogId,
	ensureNormalizedBindingsForNodes,
	pruneNodeOutputsForNodes,
	_normalizeBinding,
	_withPair,
	_pairFromLegacy,
	_assertBindingPairInvariant,
	__assertBindingPairForTest as __assertBindingPairForTestFromAudit,
	__normalizeBindingForTest as __normalizeBindingForTestFromAudit,
} from './graphStore.audit';
import {
	applyRunEventState,
	hydrateFromRunSnapshotState,
	applyLlmHolderToNodes,
	reduceRunEventState,
	reconcileModelLeaseRunningInvariant,
	resetRunUiState,
	resetEdgesExec,
	clearNodeCacheUi,
	clearNodeCacheUiForNodes,
	collectPinnedNodeIds,
	collectPinnedArtifactsByNode,
	clearPerRunPinsOnNodes,
	isNodeStateFromActiveRunAndFresh,
	validatePinEligibility,
	downstreamNodeIds,
	createRunManager,
	__setPauseResumeTraceEnabledForTest as __setPauseResumeTraceEnabledForTestFromRun,
	getPauseResumeTraceEnabled,
	__applyRunEventForTest,
	__hydrateFromRunSnapshotForTest,
	__resetRunUiStateForTest,
	__collectPinnedArtifactsByNodeForTest,
	__markStaleFromNodeForTest as __markStaleFromNodeForTestFromRun,
	__validatePinEligibilityForTest as __validatePinEligibilityForTestFromRun,
} from './graphStore.run';
const allowedPorts = new Set(['table', 'text', 'json', 'binary', 'embeddings', 'image', 'audio', 'video']);
const allowedBuiltinProfileIds = new Set<string>(TOOL_BUILTIN_PROFILE_IDS);

// re-export test hooks that moved to graphStore.audit
export const __assertBindingPairForTest = __assertBindingPairForTestFromAudit;
export const __normalizeBindingForTest = __normalizeBindingForTestFromAudit;

// re-export test hooks that moved to graphStore.run
export { __applyRunEventForTest, __hydrateFromRunSnapshotForTest, __resetRunUiStateForTest, __collectPinnedArtifactsByNodeForTest } from './graphStore.run';
export const __setPauseResumeTraceEnabledForTest = __setPauseResumeTraceEnabledForTestFromRun;
export const __markStaleFromNodeForTest = __markStaleFromNodeForTestFromRun;
export const __validatePinEligibilityForTest = __validatePinEligibilityForTestFromRun;

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

function mintGraphId(): string {
	try {
		return `graph_${crypto.randomUUID()}`;
	} catch {
		return `graph_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
	}
}


function buildHardResetState(freshGraphId: string): GraphState {
	return {
		graphId: freshGraphId,
		nodes: [],
		edges: [],
		selectedNodeId: null,
		inspector: INITIAL_INSPECTOR,
		logs: [],
		runStatus: RUN_IDLE,
		lastRunStatus: 'never_run',
		freshness: 'never_run',
		staleNodeCount: 0,
		activeRunMode: 'from_start',
		activeRunFrom: null,
		activeRunNodeSet: new Set<string>(),
		nodeOutputs: {},
		nodeBindings: {},
		activeRunId: null,
		editingContext: 'graph',
		componentEditSession: null,
		componentContractDraftCache: {}
	};
}

function captureComponentEditSnapshot(state: GraphState): ComponentEditSessionSnapshot {
	return {
		graphId: state.graphId,
		nodes: structuredClone(state.nodes),
		edges: structuredClone(state.edges),
		selectedNodeId: state.selectedNodeId,
		inspector: structuredClone(state.inspector),
		logs: structuredClone(state.logs),
		runStatus: state.runStatus,
		lastRunStatus: state.lastRunStatus,
		freshness: state.freshness,
		staleNodeCount: state.staleNodeCount,
		activeRunMode: state.activeRunMode,
		activeRunFrom: state.activeRunFrom,
		activeRunNodeSet: new Set(Array.from(state.activeRunNodeSet ?? [])),
		nodeOutputs: structuredClone(state.nodeOutputs),
		nodeBindings: structuredClone(state.nodeBindings),
		activeRunId: state.activeRunId
	};
}

export function __hardResetGraphForTest(_state: GraphState, freshGraphId = 'graph_test_reset'): GraphState {
	return buildHardResetState(freshGraphId);
}

function stripToDTO(
	nodes: Node<PipelineNodeData>[],
	edges: Edge<PipelineEdgeData>[],
	graphId?: string
): PipelineGraphDTO {
	const persistedNodes = nodes.map((node) => {
		const data = (node as any)?.data;
		if (!data || typeof data !== 'object') return node;
		const nextData = { ...data } as Record<string, unknown>;
		return {
			...node,
			data: nextData as PipelineNodeData & Record<string, unknown>
		};
	});
	const dto: PipelineGraphDTO = {
		version: 1,
		nodes: persistedNodes as any,
		edges: recomputeEdgeContractsBestEffort(nodes, edges)
	};
	if (graphId) {
		dto.meta = { ...(dto.meta ?? {}), graphId } as any;
	}
	return dto;
}

export function __stripToDTOForTest(
	nodes: Node<PipelineNodeData>[],
	edges: Edge<PipelineEdgeData>[],
	graphId?: string
): PipelineGraphDTO {
	return stripToDTO(nodes, edges, graphId);
}

function edgeStructuralSignature(edge: Edge<PipelineEdgeData>): string {
	const contract = (edge?.data as any)?.contract ?? {};
	const payload = (contract as any)?.payload ?? {};
	return [
		String(edge?.id ?? ''),
		String(edge?.source ?? ''),
		String((edge as any)?.sourceHandle ?? ''),
		String(edge?.target ?? ''),
		String((edge as any)?.targetHandle ?? ''),
		String((edge?.data as any)?.exec ?? ''),
		String((contract as any)?.out ?? ''),
		String((contract as any)?.in ?? ''),
		JSON.stringify((payload as any)?.source ?? null),
		JSON.stringify((payload as any)?.target ?? null)
	].join('|');
}

function shouldPreserveStoreEdgesOnCanvasSync(
	storeEdges: Edge<PipelineEdgeData>[],
	canvasEdges: Edge<PipelineEdgeData>[]
): boolean {
	if (canvasEdges.length >= storeEdges.length) return false;
	if (storeEdges.length === 0) return false;
	const storeById = new Map<string, Edge<PipelineEdgeData>>();
	for (const edge of storeEdges) {
		storeById.set(String(edge.id ?? ''), edge);
	}
	for (const edge of canvasEdges) {
		const id = String(edge.id ?? '');
		const existing = storeById.get(id);
		if (!existing) return false;
		// If the edge shape changed, this is not a stale node-drag sync.
		if (edgeStructuralSignature(edge) !== edgeStructuralSignature(existing)) return false;
	}
	return true;
}

function normalizeComponentPayloadTypeOrDefault(value: unknown, fallback: PayloadType = 'json'): PayloadType {
	const normalized = normalizeComponentPayloadType(value);
	return normalized ?? fallback;
}

function normalizeComponentNodeForMigration(
	node: Node<PipelineNodeData>
): { node: Node<PipelineNodeData>; outputNames: string[]; outputByName: Map<string, PayloadType>; bindingNames: string[] } {
	if (node.data.kind !== 'component') {
		return { node, outputNames: [], outputByName: new Map(), bindingNames: [] };
	}
	const params = (((node.data as any)?.params ?? {}) as Record<string, any>) || {};
	const api = (params.api ?? {}) as Record<string, any>;
	const outputsRaw = Array.isArray(api.outputs) ? (api.outputs as any[]) : [];
	const normalizedOutputs = outputsRaw
		.filter((out) => Boolean(out) && typeof out === 'object')
		.map((out) => {
			const outName = String((out as any)?.name ?? '').trim();
			const outputType = normalizeComponentPayloadTypeOrDefault((out as any)?.typedSchema?.type, 'json');
			const typedSchemaRaw =
				(out as any)?.typedSchema && typeof (out as any).typedSchema === 'object'
					? ((out as any).typedSchema as Record<string, any>)
					: {};
			const fieldsRaw = Array.isArray(typedSchemaRaw.fields) ? (typedSchemaRaw.fields as any[]) : [];
			const normalizedFields =
				outputType === 'table' || outputType === 'json'
					? fieldsRaw
					: [];
			return {
				...(out as any),
				name: outName,
				typedSchema: {
					type: outputType,
					fields: normalizedFields
				}
			};
		})
		.filter((out) => String((out as any)?.name ?? '').trim().length > 0);

	const outputNames = normalizedOutputs.map((out) => String((out as any)?.name ?? '').trim());
	const outputSet = new Set(outputNames);
	const outputByName = new Map<string, PayloadType>();
	for (const out of normalizedOutputs) {
		const name = String((out as any)?.name ?? '').trim();
		const outputType = normalizeComponentPayloadTypeOrDefault((out as any)?.typedSchema?.type, 'json');
		outputByName.set(name, outputType);
	}

	const exposureRegistryRaw = Array.isArray(params.exposureRegistry) ? (params.exposureRegistry as any[]) : [];
	const exposureRegistry = exposureRegistryRaw.filter((rec) => {
		if (!rec || typeof rec !== 'object') return false;
		if (String((rec as any).kind ?? '').trim().toLowerCase() !== 'data_output') return true;
		const alias = String((rec as any).alias ?? '').trim();
		const handleId = String((rec as any).handle_id ?? '').trim();
		if (alias && outputSet.has(alias)) return true;
		if (handleId.startsWith('data_out::')) {
			const outName = handleId.slice('data_out::'.length).trim();
			return outName.length > 0 && outputSet.has(outName);
		}
		return false;
	});

		const nextNode: Node<PipelineNodeData> = {
			...node,
			data: {
				...node.data,
				params: {
					...params,
				api: {
					...(api as Record<string, any>),
					outputs: normalizedOutputs
				},
				exposureRegistry
			}
		}
	};
	const bindingNames = outputNames.filter((name) =>
		exposureRegistry.some(
			(rec: any) =>
				String(rec?.kind ?? '').trim().toLowerCase() === 'data_output' &&
				(
					String(rec?.alias ?? '').trim() === name ||
					String(rec?.handle_id ?? '').trim() === `data_out::${name}`
				)
		)
	);
	return { node: nextNode, outputNames, outputByName, bindingNames };
}

function normalizeGraphForComponentMigration(
	nodes: Node<PipelineNodeData>[],
	edges: Edge<PipelineEdgeData>[]
): { nodes: Node<PipelineNodeData>[]; edges: Edge<PipelineEdgeData>[] } {
	const nodeInfo = new Map<
		string,
		{ outputNames: string[]; outputByName: Map<string, PayloadType>; bindingNames: string[] }
	>();
	const normalizedNodes = nodes.map((node) => {
		if (node.data.kind === 'tool') {
			const params = ((node.data as any)?.params ?? {}) as Record<string, any>;
			if (String(params?.provider ?? '').trim().toLowerCase() === 'builtin') {
				const builtin = (params?.builtin && typeof params.builtin === 'object'
					? params.builtin
					: {}) as Record<string, any>;
				const profileId = String(builtin.profileId ?? '').trim() || 'core';
				const customPackages = Array.isArray(builtin.customPackages)
					? builtin.customPackages
							.filter((pkg: unknown) => typeof pkg === 'string')
							.map((pkg: string) => pkg.trim())
							.filter((pkg: string) => pkg.length > 0)
					: [];
				const locked = typeof builtin.locked === 'string' ? builtin.locked.trim() : '';
				const nextBuiltin: Record<string, any> = {
					...builtin,
					profileId,
					customPackages
				};
				if (locked) nextBuiltin.locked = locked;
				else delete nextBuiltin.locked;
				node = {
					...node,
					data: {
						...node.data,
						params: {
							...params,
							builtin: nextBuiltin
						}
					}
				};
			}
		}
		const normalized = normalizeComponentNodeForMigration(node);
		if (node.data.kind === 'component') {
			nodeInfo.set(String(node.id), {
				outputNames: normalized.outputNames,
				outputByName: normalized.outputByName,
				bindingNames: normalized.bindingNames
			});
		}
		return normalized.node;
	});

	const normalizedEdges = edges.map((edge) => {
		const srcInfo = nodeInfo.get(String(edge.source));
		if (!srcInfo) return edge;
		const outputNames = srcInfo.outputNames;
		if (outputNames.length === 0) return edge;
		const sourceHandle = String((edge as any)?.sourceHandle ?? 'out').trim() || 'out';
		const outputSet = new Set(outputNames);
		const bindingNames = srcInfo.bindingNames;
		const edgeDataContract =
			(edge as any)?.data && typeof (edge as any).data === 'object'
				? ((edge as any).data?.contract as Record<string, any> | undefined)
				: undefined;
		const contractOut = normalizeComponentPayloadType(edgeDataContract?.out ?? null);
		let canonicalHandle = sourceHandle;
		if (canonicalHandle === 'out') {
			if (outputNames.length === 1) {
				canonicalHandle = outputNames[0];
			} else if (bindingNames.length === 1) {
				canonicalHandle = bindingNames[0];
			} else if (contractOut) {
				const candidates = outputNames.filter(
					(name) => srcInfo.outputByName.get(name) === contractOut
				);
				if (candidates.length === 1) canonicalHandle = candidates[0];
			}
		} else if (!outputSet.has(canonicalHandle)) {
			if (outputNames.length === 1) {
				canonicalHandle = outputNames[0];
			} else if (bindingNames.length === 1) {
				canonicalHandle = bindingNames[0];
			} else if (contractOut) {
				const candidates = outputNames.filter(
					(name) => srcInfo.outputByName.get(name) === contractOut
				);
				if (candidates.length === 1) canonicalHandle = candidates[0];
			}
		}
		if (canonicalHandle === sourceHandle) return edge;
		return {
			...edge,
			sourceHandle: canonicalHandle
		};
	});
	const canonicalNodes = canonicalizeNodeSchemas(normalizedNodes);

	return {
		nodes: canonicalNodes,
		edges: recomputeEdgeContractsBestEffort(canonicalNodes, normalizedEdges)
	};
}

function buildPersistableGraphStrict(
	nodes: Node<PipelineNodeData>[],
	edges: Edge<PipelineEdgeData>[],
	graphId?: string
): { ok: true; graph: PipelineGraphDTO } | { ok: false; error: string } {
	const normalized = normalizeGraphForComponentMigration(nodes, edges);
	const canonicalized = canonicalizeComponentEdgeSourceHandles(normalized.nodes, normalized.edges, 'strict');
	if (!canonicalized.ok) return { ok: false, error: canonicalized.error };
	const rechecked = pruneAndRecontractEdgesStrict(normalized.nodes, canonicalized.edges);
	if (!rechecked.ok) return { ok: false, error: rechecked.error };
	return { ok: true, graph: stripToDTO(normalized.nodes, rechecked.edges, graphId) };
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

function computeSaveConsistencyMismatch(
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

function toolBuiltinPreflightDiagnostics(node: Node<PipelineNodeData>): SavePreflightDiagnostic[] {
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

function buildSavePreflightDiagnostics(
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
			message: `Duplicate node name "${duplicate.displayName}" (case-insensitive, trimmed match).`,
			severity: 'error'
		});
	}

	return {
		ok: !diagnostics.some((d) => d.severity === 'error'),
		diagnostics
	};
}

function summarizeSavePreflightError(diagnostics: SavePreflightDiagnostic[]): string {
	const errors = diagnostics.filter((d) => d.severity === 'error');
	if (errors.length === 0) return 'Graph preflight failed.';
	return errors
		.slice(0, 5)
		.map((d, i) => `${i + 1}. [${d.code}] (${d.path}) ${d.message}`)
		.join('\n');
}


function setEdgeExec(
	edges: Edge<PipelineEdgeData>[],
	edgeId: string,
	exec: 'idle' | 'active' | 'done'
) {
	return edges.map((e) => (e.id === edgeId ? { ...e, data: { ...e.data, exec: exec } } : e));
}

function downstreamIds(startId: string, edges: Edge<PipelineEdgeData>[]) {
	const adj = new Map<string, string[]>();
	for (const e of edges) adj.set(e.source, [...(adj.get(e.source) ?? []), e.target]);

	const seen = new Set<string>();
	const q = [startId];
	while (q.length) {
		const cur = q.shift()!;
		for (const nxt of adj.get(cur) ?? []) {
			if (!seen.has(nxt)) {
				seen.add(nxt);
				q.push(nxt);
			}
		}
	}
	return seen;
}

function pruneAndRecontractEdgesStrict(
	nodes: Node<PipelineNodeData>[],
	edges: Edge<PipelineEdgeData>[]
):
	| { ok: true; edges: Edge<PipelineEdgeData>[]; prunedIds: string[] }
	| { ok: false; error: string } {
	const next: Edge<PipelineEdgeData>[] = [];
	const prunedIds: string[] = [];

	for (const e of edges) {
		const chk = isEdgeStillValid(nodes, e);

		if (chk.ok === false) {
			if (chk.reason === 'type_mismatch' || chk.reason === 'schema_mismatch') {
				// allowed prune
				prunedIds.push(e.id);
				continue;
			}

			// NOT allowed to silently prune: graph invariants broken
			return {
				ok: false,
				error: `Edge ${e.id} has unresolved schema compatibility (source=${e.source}:${e.sourceHandle ?? 'out'} target=${e.target}:${e.targetHandle ?? 'in'})`
			};
		}

		next.push({
			...e,
			data: {
				...(e.data ?? {}),
				exec: e.data?.exec ?? 'idle',
				mode: normalizeEdgeMode(e),
				contract: (() => {
					const sourceHandle = String((e as any).sourceHandle ?? 'out').trim() || 'out';
					const targetHandle = String((e as any).targetHandle ?? 'in').trim() || 'in';
					const sourceNode = nodes.find((n) => n.id === e.source)!;
					const targetNode = nodes.find((n) => n.id === e.target)!;
					const payloadSource = buildProvidedSchema(sourceNode as any, sourceHandle) as Record<string, any>;
					const payloadTarget = buildRequiredSchema(targetNode as any, targetHandle) as Record<string, any>;
					const compatibility = isSchemaCompatible(
						payloadSource ?? { type: 'unknown' },
						payloadTarget ?? { type: 'unknown' },
						normalizeEdgeMode(e)
					);
					return {
						out: chk.out,
						in: chk.in,
						payload: {
							source: payloadSource,
							target: payloadTarget
						},
						snapshot: edgeContractSnapshotFromSchemas(
							payloadSource,
							payloadTarget,
							compatibility,
							normalizeEdgeMode(e)
						)
					};
				})()
			}
		});
	}

	return { ok: true, edges: next, prunedIds };
}

function canonicalizeComponentEdgeSourceHandles(
	nodes: Node<PipelineNodeData>[],
	edges: Edge<PipelineEdgeData>[],
	mode: 'strict' | 'best_effort'
):
	| { ok: true; edges: Edge<PipelineEdgeData>[] }
	| { ok: false; error: string } {
	const next: Edge<PipelineEdgeData>[] = [];
	for (const edge of edges) {
		const sourceNode = nodes.find((n) => n.id === edge.source);
		if (!sourceNode || sourceNode.data.kind !== 'component') {
			next.push(edge);
			continue;
		}
		const canonicalSourceHandle = canonicalComponentSourceHandleForEdge(nodes, edge);
		if (canonicalSourceHandle == null) {
			if (mode === 'strict') {
				return {
					ok: false,
					error: `Edge ${String(edge.id ?? '')} has unresolved component source handle (source=${String(edge.source ?? '')}:${String((edge as any).sourceHandle ?? 'out')})`
				};
			}
			next.push(edge);
			continue;
		}
		next.push({
			...edge,
			sourceHandle: canonicalSourceHandle
		});
	}
	return { ok: true, edges: next };
}

function recomputeEdgeContractsBestEffort(
	nodes: Node<PipelineNodeData>[],
	edges: Edge<PipelineEdgeData>[]
): Edge<PipelineEdgeData>[] {
	const canonicalized = canonicalizeComponentEdgeSourceHandles(nodes, edges, 'best_effort');
	const working = canonicalized.ok ? canonicalized.edges : edges;
	return working.map((edge) => {
		const sourceNode = nodes.find((n) => n.id === edge.source);
		const targetNode = nodes.find((n) => n.id === edge.target);
		if (!sourceNode || !targetNode) return edge;
		const chk = isEdgeStillValid(nodes, edge);
		const existingContract = ((edge.data ?? {}) as any).contract ?? {};
		const sourceHandle = String((edge as any).sourceHandle ?? 'out').trim() || 'out';
		const targetHandle = String((edge as any).targetHandle ?? 'in').trim() || 'in';
		const payload = {
			source: buildProvidedSchema(sourceNode as any, sourceHandle),
			target: buildRequiredSchema(targetNode as any, targetHandle)
		};
		const edgeMode = normalizeEdgeMode(edge);
		const snapshotCompatibility: SchemaCompatibility = chk.ok
			? { ok: true }
			: {
					ok: false,
					reason:
						chk.reason === 'schema_mismatch'
							? 'missing_required_columns'
							: chk.reason === 'typed_schema_missing'
								? 'missing_typed_schema'
								: 'type_mismatch',
					missingColumns: chk.missingColumns,
					suggestion: chk.suggestion ?? null,
					adapterKind: chk.adapterKind ?? null
				};
		const snapshot = edgeContractSnapshotFromSchemas(
			payload.source as any,
			payload.target as any,
			snapshotCompatibility,
			edgeMode
		);
		if (chk.ok) {
			return {
				...edge,
				data: {
					...(edge.data ?? {}),
					contract: {
						out: chk.out,
						in: chk.in,
						payload,
						snapshot
					}
				}
			};
		}
		return {
			...edge,
			data: {
				...(edge.data ?? {}),
				contract: {
					out: existingContract?.out,
					in: existingContract?.in,
					payload,
					snapshot
				}
			}
		};
	});
}

function topoFrom(
	nodes: Node<PipelineNodeData>[],
	edges: Edge<PipelineEdgeData>[],
	startId: string | null
) {
	const inDeg = new Map<string, number>();
	const adj = new Map<string, string[]>();

	for (const n of nodes) {
		inDeg.set(n.id, 0);
		adj.set(n.id, []);
	}

	for (const e of edges) {
		adj.get(e.source)!.push(e.target);
		inDeg.set(e.target, (inDeg.get(e.target) ?? 0) + 1);
	}

	const startSet = new Set<string>();
	if (startId) {
		startSet.add(startId);
		for (const d of downstreamIds(startId, edges)) startSet.add(d);
	} else {
		for (const [id, deg] of inDeg.entries()) if (deg === 0) startSet.add(id);
		const roots = [...startSet];
		for (const r of roots) for (const d of downstreamIds(r, edges)) startSet.add(d);
	}

	const inDeg2 = new Map<string, number>();
	for (const id of startSet) inDeg2.set(id, 0);
	for (const e of edges) {
		if (startSet.has(e.source) && startSet.has(e.target)) {
			inDeg2.set(e.target, (inDeg2.get(e.target) ?? 0) + 1);
		}
	}

	const q: string[] = [];
	for (const [id, deg] of inDeg2.entries()) if (deg === 0) q.push(id);

	const order: string[] = [];
	while (q.length) {
		const cur = q.shift()!;
		order.push(cur);
		for (const nxt of adj.get(cur) ?? []) {
			if (!startSet.has(nxt)) continue;
			const nd = (inDeg2.get(nxt) ?? 0) - 1;
			inDeg2.set(nxt, nd);
			if (nd === 0) q.push(nxt);
		}
	}

	if (order.length !== startSet.size) return [...startSet].sort();
	return order;
}

const loaded = loadGraphFromLocalStorage(emptyGraph);
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

const loadedNodes = Array.isArray((loaded as any)?.nodes)
	? ((loaded as any).nodes as Node<PipelineNodeData>[])
	: [];
const loadedEdgesRaw = Array.isArray((loaded as any)?.edges)
	? ((loaded as any).edges as Edge<PipelineEdgeData>[])
	: [];
const loadedNormalized = normalizeGraphForComponentMigration(loadedNodes, loadedEdgesRaw);
const loadedCanonicalized = canonicalizeComponentEdgeSourceHandles(
	loadedNormalized.nodes,
	loadedNormalized.edges,
	'best_effort'
);
const loadedEdges = recomputeEdgeContractsBestEffort(
	loadedNormalized.nodes,
	loadedCanonicalized.ok ? loadedCanonicalized.edges : loadedNormalized.edges
);

const initialState: GraphState = {
	graphId: String((loaded as any)?.meta?.graphId ?? mintGraphId()),
	nodeDocExplanationMode: loadNodeDocExplanationMode(),
	nodeDocTrainingMode: loadNodeDocTrainingMode(),
	nodeDocTooltipEnabled: true,
	nodeDocTooltipOpenDelayMs: 500,
	nodeDocPlanesExpansionEnabled: true,
	nodeDocPlanesExpansionDelayMs: 1200,
	nodeDocExplainModel: 'glm-4.7-flash:latest',
	nodeDocExplainTemperature: 0.2,
	nodeDocExplainTopP: 1.0,
	nodeDocExplainMaxTokens: 512,
	nodes: loadedNormalized.nodes,
	edges: loadedEdges,
	selectedNodeId: null,
	inspector: INITIAL_INSPECTOR,
	logs: [],
	runStatus: RUN_IDLE,
	lastRunStatus: 'never_run',
	freshness: 'never_run',
	staleNodeCount: 0,
	activeRunMode: 'from_start',
	activeRunFrom: null,
	activeRunNodeSet: new Set<string>(),
	nodeOutputs: {},
	nodeBindings: ensureNormalizedBindingsForNodes(loadedNormalized.nodes, {}),
	activeRunId: null,
	editingContext: 'graph',
	componentEditSession: null,
	componentContractDraftCache: {}
};

export const graphStore = (() => {
	const { subscribe, set, update: rawUpdate } = writable<GraphState>(initialState);
	const resolveNodeDocMemoized = createMemoizedNodeDocResolver();

	// ── history ──────────────────────────────────────────────────────────
	const history = createHistoryManager({
		getState: () => get({ subscribe } as any) as GraphState,
		applyDocument: (graph, graphId) => {
			return applyGraphDocument(graph, graphId).ok;
		},
		snapshotFromState: (s) => stripToDTO(s.nodes as any, s.edges as any, s.graphId),
	});

	// ── audited update ───────────────────────────────────────────────────
	const update = history.wrapUpdate(
		rawUpdate,
		auditStateTransition,
		(s) => stripToDTO(s.nodes as any, s.edges as any, s.graphId),
	);
function applyLocalStaleInvalidation(nodeId: string, rootReason: string = 'PARAMS_CHANGED'): void {
		update((cur) => {
			const pinnedNodeIds = new Set<string>(collectPinnedNodeIds(cur.nodes as any));
			const candidateIds = downstreamNodeIds(cur.edges, nodeId, pinnedNodeIds);
			const nodeBindings = { ...cur.nodeBindings };
			let nodeOutputs = { ...cur.nodeOutputs };
			let changed = false;
			for (const affectedId of candidateIds) {
				if (affectedId !== nodeId && pinnedNodeIds.has(affectedId)) continue;
				const prev = _normalizeBinding(nodeBindings[affectedId], affectedId);
				const hadArtifact = Boolean(prev.current?.artifactId || prev.last?.artifactId);
				if (!hadArtifact && affectedId !== nodeId) continue;
				if (isNodeStateFromActiveRunAndFresh(cur, prev)) continue;
				let next = {
					...prev,
					status: 'stale',
					isUpToDate: false,
					cacheValid: false,
					currentRunId: null,
					staleReason: affectedId === nodeId ? rootReason : 'UPSTREAM_CHANGED'
				};
				next = _withPair(next, 'current', { execKey: null, artifactId: null });
				_assertBindingPairInvariant(next, affectedId, 'applyLocalStaleInvalidation');
				nodeBindings[affectedId] = next;
				nodeOutputs = clearNodeCacheUi(nodeOutputs, affectedId);
				changed = true;
			}
			if (!changed) return cur;
			// Keep existing previews while stale so users can compare last known outputs.
			return withGraphMeta({ ...cur, nodeBindings, nodeOutputs });
		}, { source: 'accept_params', expectedDirtyTransition: true });
	}

function applyBackendAffectedStale(affectedNodeIds: string[], rootNodeId: string): void {
		if (!Array.isArray(affectedNodeIds) || affectedNodeIds.length === 0) return;
		update((cur) => {
			const pinnedNodeIds = new Set<string>(collectPinnedNodeIds(cur.nodes as any));
			const nodeBindings = { ...cur.nodeBindings };
			const touchedIds: string[] = [];
			for (const affectedId of affectedNodeIds) {
				if (affectedId !== rootNodeId && pinnedNodeIds.has(affectedId)) continue;
				const prev = _normalizeBinding(nodeBindings[affectedId], affectedId);
				if (isNodeStateFromActiveRunAndFresh(cur, prev)) continue;
				let next = {
					...prev,
					status: 'stale',
					isUpToDate: false,
					cacheValid: false,
					currentRunId: null,
					staleReason: affectedId === rootNodeId ? 'PARAMS_CHANGED' : 'UPSTREAM_CHANGED'
				};
				next = _withPair(next, 'current', { execKey: null, artifactId: null });
				_assertBindingPairInvariant(next, affectedId, 'applyBackendAffectedStale');
				nodeBindings[affectedId] = next;
				touchedIds.push(affectedId);
			}
			const nodeOutputs =
				touchedIds.length > 0
					? clearNodeCacheUiForNodes({ ...cur.nodeOutputs }, touchedIds)
					: cur.nodeOutputs;
			return withGraphMeta({ ...cur, nodeBindings, nodeOutputs });
		}, { source: 'accept_params', allowedNodeIds: new Set(affectedNodeIds), expectedDirtyTransition: true });
	}

	function applySourceRehydration(nodeId: string, resolved: {
		execKey: string;
		artifactId: string | null;
		artifact?: { mimeType?: string; payloadType?: string };
	}): void {
		if (!resolved.artifactId) return;
		update((cur) => {
			const prevBinding = _normalizeBinding(cur.nodeBindings?.[nodeId], nodeId);
			let nextBinding: NormalizedNodeBinding = {
				...prevBinding,
				status: 'succeeded_up_to_date',
				cacheValid: true,
				isUpToDate: true,
				staleReason: null
			};
			nextBinding = _withPair(nextBinding, 'current', {
				execKey: resolved.execKey,
				artifactId: resolved.artifactId
			});
			nextBinding = _withPair(nextBinding, 'last', {
				execKey: resolved.execKey,
				artifactId: resolved.artifactId
			});
			_assertBindingPairInvariant(nextBinding, nodeId, 'applySourceRehydration');
			const nodeBindings = {
				...cur.nodeBindings,
				[nodeId]: nextBinding
			};
			const prevOut: NodeOutputInfo | undefined = cur.nodeOutputs?.[nodeId];
			const nodeOutputs = {
				...cur.nodeOutputs,
				[nodeId]: {
					...prevOut,
					mimeType: resolved.artifact?.mimeType ?? prevOut.mimeType,
					payloadType: resolved.artifact?.payloadType ?? prevOut.payloadType,
					preview: undefined,
					cached: true,
					cacheDecision: 'cache_hit' as const
				}
			};
			return withGraphMeta({ ...cur, nodeBindings, nodeOutputs });
		}, { source: 'accept_params', expectedDirtyTransition: true, allowedNodeIds: new Set([nodeId]) });
	}

	async function syncAcceptParamsForNode(
		nodeId: string,
		paramsForSubmit: Record<string, any>,
		beforeExecParams: Record<string, unknown>
	): Promise<void> {
		if (paramsForSubmit?.op === 'dedupe') {
			console.log('[dedupe-store] syncAcceptParamsForNode:begin', {
				nodeId,
				paramsForSubmit
			});
		}
		const st = get({ subscribe } as any) as GraphState;
		const afterNode = st.nodes.find((x) => x.id === nodeId);
		const afterExecParams = effectiveExecParamsForNode(afterNode);
		const execInputsChanged = stableJson(beforeExecParams) !== stableJson(afterExecParams);
		if (!execInputsChanged) return;
		const isSourceFile =
			String((afterNode as any)?.data?.kind ?? '') === 'source' &&
			String((afterNode as any)?.data?.sourceKind ?? 'file') === 'file';

		// Even when no active backend run handle exists, keep local UI and previews honest.
		if (!st.activeRunId) {
			applyLocalStaleInvalidation(nodeId);
			if (isSourceFile) {
				try {
					const resolved = await resolveSourceNode({
						graphId: st.graphId,
						graph: { version: 1, nodes: st.nodes, edges: st.edges },
						nodeId,
						params: paramsForSubmit
					});
					applySourceRehydration(nodeId, resolved);
				} catch {
					// keep stale state on resolve failure
				}
			}
			return;
		}

		try {
			const resp = await acceptNodeParams({
				runId: st.activeRunId,
				nodeId,
				graph: { version: 1, nodes: st.nodes, edges: st.edges },
				params: paramsForSubmit
			});
			applyBackendAffectedStale(resp.affectedNodeIds ?? [], nodeId);
			const snap = await getRun(st.activeRunId);
			update((cur) => hydrateFromRunSnapshotState(cur, snap), {
				source: 'hydrate_snapshot',
				snapshotNodeIds: new Set(Object.keys(snap.nodeBindings ?? {}))
			});
			if (isSourceFile) {
				try {
					const resolved = await resolveSourceNode({
						graphId: st.graphId,
						graph: { version: 1, nodes: st.nodes, edges: st.edges },
						nodeId,
						params: paramsForSubmit
					});
					applySourceRehydration(nodeId, resolved);
				} catch {
					// keep stale state on resolve failure
				}
			}
		} catch (e) {
			// Backend sync failed; still keep local UX in stale state for changed effective inputs.
			applyLocalStaleInvalidation(nodeId);
			update((cur) => logPush(cur, 'warn', `accept-params sync failed: ${String(e)}`, nodeId));
		}
	}


	function persist(state: GraphState) {
		saveGraphToLocalStorage(stripToDTO(state.nodes, state.edges, state.graphId));
	}

	// ── inspector manager ────────────────────────────────────────────────────
	const inspector = createInspectorManager({
		update,
		getState: () => get({ subscribe } as any) as GraphState,
		persist,
		applyLocalStaleInvalidation,
		syncAcceptParamsForNode,
		pruneAndRecontractEdgesStrict,
	});
	const updateNodeConfigImpl = inspector.actions.updateNodeConfig;

	// ── run manager ─────────────────────────────────────────────────────────────
	const runManager = createRunManager({
		update,
		getState: () => get({ subscribe } as any) as GraphState,
		persist,
		applyLocalStaleInvalidation,
		applyBackendAffectedStale,
		applySourceRehydration,
		syncAcceptParamsForNode,
		hydrateFromRunSnapshot: hydrateFromRunSnapshotState,
	});


	function applyGraphDocument(
		graph: { nodes: unknown[]; edges: unknown[] },
		graphIdOverride?: string | null
	): { ok: boolean; reason?: string } {
		const nextNodes = Array.isArray(graph?.nodes) ? (graph.nodes as Node<PipelineNodeData>[]) : null;
		const nextEdges = Array.isArray(graph?.edges) ? (graph.edges as Edge<PipelineEdgeData>[]) : null;
		if (!nextNodes || !nextEdges) return { ok: false, reason: 'invalid_payload' };
		const normalized = normalizeGraphForComponentMigration(nextNodes, nextEdges);
		const canonicalized = canonicalizeComponentEdgeSourceHandles(normalized.nodes, normalized.edges, 'strict');
		if (!canonicalized.ok) return { ok: false, reason: canonicalized.error };
		const rechecked = pruneAndRecontractEdgesStrict(normalized.nodes, canonicalized.edges);
		if (!rechecked.ok) return { ok: false, reason: rechecked.error };
		update((s) => {
			const nextState = withGraphMeta({
				...s,
				graphId: String(graphIdOverride || s.graphId),
				nodes: normalized.nodes,
				edges: rechecked.edges,
				selectedNodeId: null,
				inspector: { ...INITIAL_INSPECTOR, uiByNodeId: s.inspector.uiByNodeId },
				logs: [],
				runStatus: RUN_IDLE,
				lastRunStatus: 'never_run',
				freshness: 'never_run',
				staleNodeCount: 0,
				activeRunMode: 'from_start',
				activeRunFrom: null,
				activeRunNodeSet: new Set<string>(),
				nodeOutputs: {},
				nodeBindings: ensureNormalizedBindingsForNodes(normalized.nodes as any, {}),
				activeRunId: null,
				editingContext: 'graph',
				componentEditSession: null
			});
			persist(nextState);
			return nextState;
		}, { source: 'graph_edit' });
		if (!history.isApplying()) {
			history.resetToSnapshot(stripToDTO(
				(get({ subscribe } as any) as GraphState).nodes as any,
				(get({ subscribe } as any) as GraphState).edges as any,
				(get({ subscribe } as any) as GraphState).graphId
			));
		}
		return { ok: true };
	}

	function applySemanticSubtypeReset(
		nodeId: string,
		payload: Record<string, unknown>
	): void {
		applyLocalStaleInvalidation(nodeId, 'KIND_CHANGED');
		if (DEV_MODE) {
			const st = get({ subscribe } as any) as GraphState;
			const b = st.nodeBindings?.[nodeId];
			const o = st.nodeOutputs?.[nodeId];
			console.debug('[graphStore][subtype-switch] post-invalidate', {
				nodeId,
				...payload,
				status: b?.status,
				isUpToDate: b?.isUpToDate,
				cacheDecision: o?.cacheDecision,
				cached: o?.cached,
				currentArtifactId: b?.current?.artifactId ?? b?.currentArtifactId ?? null,
				currentExecKey: b?.current?.execKey ?? b?.currentExecKey ?? null,
				lastArtifactId: b?.last?.artifactId ?? b?.lastArtifactId ?? null
			});
		}
	}

	return {
		subscribe,
		...history.actions,
		...inspector.actions,
		setPauseResumeTraceLoggingEnabled(enabled: boolean) {
			__setPauseResumeTraceEnabledForTestFromRun(enabled);
		},
		getPauseResumeTraceLoggingEnabled() {
			return getPauseResumeTraceEnabled();
		},
		getSavePreflight(stateOverride?: GraphState): SavePreflightResult {
			const state = stateOverride ?? (get({ subscribe } as any) as GraphState);
			return buildSavePreflightDiagnostics(state.nodes as any, state.edges as any);
		},
		resolveNodeInputs(nodeId: string): InputResolution[] {
			const s = get({ subscribe } as any) as GraphState;
			return resolveNodeInputsFromState(s, nodeId);
		},
		getNodeDocResolved(nodeId: string): NodeDocResolved | null {
			const s = get({ subscribe } as any) as GraphState;
			return resolveNodeDocMemoized(s, nodeId);
		},
		getNodeDocExplanationMode(): NodeDocExplanationMode {
			const s = get({ subscribe } as any) as GraphState;
			return s.nodeDocExplanationMode ?? 'default';
		},
		setNodeDocExplanationMode(modeRaw: NodeDocExplanationMode): void {
			const parsed = NodeDocExplanationModeSchema.safeParse(modeRaw);
			const mode = parsed.success ? parsed.data : 'default';
			update((s) => ({ ...s, nodeDocExplanationMode: mode }));
			persistNodeDocExplanationMode(mode);
		},
		getNodeDocTrainingMode(): NodeDocTrainingMode {
			const s = get({ subscribe } as any) as GraphState;
			const parsed = NodeDocTrainingModeSchema.safeParse((s as any)?.nodeDocTrainingMode);
			return parsed.success ? parsed.data : 'off';
		},
		setNodeDocTrainingMode(modeRaw: NodeDocTrainingMode): void {
			const parsed = NodeDocTrainingModeSchema.safeParse(modeRaw);
			const mode = parsed.success ? parsed.data : 'off';
			update((s) => ({ ...s, nodeDocTrainingMode: mode }));
			persistNodeDocTrainingMode(mode);
		},
		setNodeDocRuntimeConfig(config: Partial<{
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
		},
		setNodeDocGeneratedExplanation(nodeIdRaw: string, generatedRaw: unknown): { ok: boolean; reason?: string } {
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
		},
		clearNodeDocGeneratedExplanation(nodeIdRaw: string): { ok: boolean; reason?: string } {
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
		},

		setSourceKind(nodeId: string, nextKind: SourceKind) {
			return runInHistoryTransaction(history, () => {
				const nextParams = structuredClone(defaultSourceParamsByKind[nextKind]);

				// 1) update structural subtype on the node
				update((s) => {
					const node = s.nodes.find((n) => n.id === nodeId);
					if (!node) return logPush(s, 'warn', 'Node not found', nodeId);

					const nodes = s.nodes.map((n) =>
						n.id === nodeId
							? {
								...n,
								data: {
									...n.data,
									sourceKind: nextKind, // ✅ structural
									meta: { ...(n.data.meta ?? {}), updatedAt: new Date().toISOString() }
								}
							}
							: n
					);

					const next = { ...s, nodes };
					persist(next);
					return next;
				});

				// 2) replace params via your validated path (schema stripping happens here)
				const r = updateNodeConfigImpl(nodeId, { params: nextParams });
				if (r.ok) {
					applySemanticSubtypeReset(nodeId, { kind: 'source', sourceKind: nextKind });
				}

				// 3) ensure inspector draft matches immediately after type switch
				if (r.ok) {
					update((s) => {
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
				return r;
			});
		},

		// graphStore.ts (inside your graphStore object)
		setLlmKind(nodeId: string, nextKind: LlmKind) {
			return runInHistoryTransaction(history, () => {
				const nextParams = structuredClone(defaultLlmParamsByKind[nextKind]);

				// 1) update structural subtype on the node
				update((s) => {
					const node = s.nodes.find((n) => n.id === nodeId);
					if (!node) return logPush(s, 'warn', 'Node not found', nodeId);

					const nodes = s.nodes.map((n) =>
						n.id === nodeId
							? {
								...n,
								data: {
									...n.data,
									llmKind: nextKind, // ✅ structural
									meta: { ...(n.data.meta ?? {}), updatedAt: new Date().toISOString() }
								}
							}
							: n
					);

					const next = { ...s, nodes };
					persist(next);
					return next;
				});

				// 2) replace params via your validated path (schema stripping happens here)
				const r = updateNodeConfigImpl(nodeId, { params: nextParams });
				if (r.ok) {
					const node = get({ subscribe } as any).nodes.find((n: any) => n.id === nodeId);
					applySemanticSubtypeReset(nodeId, { kind: node?.data?.kind ?? 'model', llmKind: nextKind });
				}

				// 3) ensure inspector draft matches immediately after type switch
				if (r.ok) {
					update((s) => {
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

				return r;
			});
		},

		// graphStore.ts (inside your graphStore object)
		setTransformKind(nodeId: string, nextKind: TransformKind) {
			return runInHistoryTransaction(history, () => {
				const nextParams = structuredClone(defaultTransformParamsByKind[nextKind]);

				// 1) update structural subtype on the node
				update((s) => {
					const node = s.nodes.find((n) => n.id === nodeId);
					if (!node) return logPush(s, 'warn', 'Node not found', nodeId);

					const nodes = s.nodes.map((n) =>
						n.id === nodeId
							? {
								...n,
								data: {
									...n.data,
									transformKind: nextKind, // ✅ structural
									meta: { ...(n.data.meta ?? {}), updatedAt: new Date().toISOString() }
								}
							}
							: n
					);

					const next = { ...s, nodes };
					persist(next);
					return next;
				});

				// 2) replace params via your validated path (schema stripping happens here)
				const r = updateNodeConfigImpl(nodeId, { params: nextParams });
				if (r.ok) {
					applySemanticSubtypeReset(nodeId, { kind: 'transform', transformKind: nextKind });
				}

				// 3) ensure inspector draft matches immediately after type switch
				if (r.ok) {
					update((s) => {
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

				return r;
			});
		},

		setToolProvider(nodeId: string, nextProvider: ToolProvider) {
			return runInHistoryTransaction(history, () => {
				const nextParams = structuredClone(defaultToolParamsByProvider[nextProvider]);
				const r = updateNodeConfigImpl(nodeId, { params: nextParams });
				if (r.ok) {
					applySemanticSubtypeReset(nodeId, { kind: 'tool', provider: nextProvider });
				}

				if (r.ok) {
					update((s) => {
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

				return r;
			});
		},

		setToolKind(nodeId: string, nextProvider: ToolProvider) {
			return this.setToolProvider(nodeId, nextProvider);
		},

		// ----- sync entrypoints (because SvelteFlow uses bind:nodes/bind:edges) -----
		syncFromCanvas(nodes: Node<PipelineNodeData>[], edges: Edge<PipelineEdgeData>[]) {
			update((s) => {
				const nextEdges = shouldPreserveStoreEdgesOnCanvasSync(s.edges, edges) ? s.edges : edges;
				const normalized = normalizeGraphForComponentMigration(nodes, nextEdges);
				// avoid needless churn if same references
				if (s.nodes === normalized.nodes && s.edges === normalized.edges) return s;
				const next = {
					...s,
					nodes: normalized.nodes,
					edges: normalized.edges,
					nodeBindings: ensureNormalizedBindingsForNodes(normalized.nodes, s.nodeBindings ?? {})
				};
				persist(next);
				return next;
			});
		},

		// ----- selection -----
		selectNode(nodeId: string | null) {
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
		},

		// ----- node CRUD -----
		addNode(kind: NodeKind, position: { x: number; y: number }, opts?: { label?: string }) {
			const id = `n_${crypto.randomUUID()}`;
			const baseNode: Node<PipelineNodeData> = {
				id,
				type: kind,
				position,
				data: defaultNodeData(kind)
			};
			const node = canonicalizeNodeSchemas([baseNode])[0] as Node<PipelineNodeData>;
			if ((node.data as any)?.schema) {
				delete (node.data as any).schema;
			}

			update((s) => {
				const requestedLabel =
					typeof opts?.label === 'string' && String(opts.label).trim().length > 0
						? String(opts.label).trim()
						: String((node.data as any)?.label ?? '').trim();
				const uniqueLabel = resolveUniqueNodeName(s.nodes as Node<PipelineNodeData>[], requestedLabel);
				if (uniqueLabel) {
					(node.data as any).label = uniqueLabel;
				}
				const nodeBindings = {
					...s.nodeBindings,
					[id]: _normalizeBinding(s.nodeBindings?.[id], id)
				};
				const next = logPush(
					{ ...s, nodes: [...s.nodes, node], selectedNodeId: id, nodeBindings },
					'info',
					`Added node ${id} (${kind})`,
					id
				);
				persist(next);
				return next;
			});

			return id;
		},

		deleteNode(nodeId: string) {
			update((s) => {
				const nodes = s.nodes.filter((n) => n.id !== nodeId);
				const edges = s.edges.filter((e) => e.source !== nodeId && e.target !== nodeId);
				const selectedNodeId = s.selectedNodeId === nodeId ? null : s.selectedNodeId;
				const { [nodeId]: _dropBinding, ...nodeBindings } = s.nodeBindings;
				const { [nodeId]: _dropOutput, ...nodeOutputs } = s.nodeOutputs;

				const next = logPush(
					{ ...s, nodes, edges, selectedNodeId, nodeBindings, nodeOutputs },
					'info',
					`Deleted node ${nodeId}`,
					nodeId
				);
				const withMeta = withGraphMeta(next);
				persist(withMeta);
				return withMeta;
			});
		},

		// ----- edge CRUD -----
		deleteEdge(edgeId: string) {
			update((s) => {
				const edges = s.edges.filter((e) => e.id !== edgeId);
				const next = logPush({ ...s, edges }, 'info', `Deleted edge ${edgeId}`);
				persist(next);
				return next;
			});
		},

		updateEdgeConfig(
			edgeId: string,
			patch: {
				mode?: 'work' | 'param' | 'control';
				fatal?: boolean;
				queue?: { max?: number; overflow?: 'block' | 'spill' | 'error'; policy?: 'fifo' | 'round_robin' };
				work?: { item_mode?: 'artifact' | 'json_items' | 'table_rows'; max_items?: number };
			}
		) {
			let out: { ok: boolean; error?: string } = { ok: true };
			update((s) => {
				const idx = s.edges.findIndex((e) => e.id === edgeId);
				if (idx < 0) {
					out = { ok: false, error: 'Edge not found' };
					return s;
				}
				const edge = s.edges[idx];
				const nextMode = String(patch.mode ?? normalizeEdgeMode(edge)).trim().toLowerCase();
				if (!['work', 'param', 'control'].includes(nextMode)) {
					out = { ok: false, error: 'Invalid edge mode' };
					return s;
				}
				const nextQueue = {
					max: Math.max(1, Number(patch.queue?.max ?? (edge.data as any)?.queue?.max ?? 1000)),
					overflow: String(
						patch.queue?.overflow ?? (edge.data as any)?.queue?.overflow ?? 'block'
					).toLowerCase() as 'block' | 'spill' | 'error',
					policy: String(
						patch.queue?.policy ?? (edge.data as any)?.queue?.policy ?? 'fifo'
					).toLowerCase() as 'fifo' | 'round_robin'
				};
				if (!['block', 'spill', 'error'].includes(nextQueue.overflow)) {
					out = { ok: false, error: 'Invalid queue overflow policy' };
					return s;
				}
				if (!['fifo', 'round_robin'].includes(nextQueue.policy)) {
					out = { ok: false, error: 'Invalid queue arbitration policy' };
					return s;
				}
				const nextWork = {
					item_mode: String(
						patch.work?.item_mode ??
							(edge.data as any)?.work?.item_mode ??
							(edge.data as any)?.work?.itemMode ??
							'artifact'
					).toLowerCase() as 'artifact' | 'json_items' | 'table_rows',
					max_items: Math.max(
						1,
						Number(patch.work?.max_items ?? (edge.data as any)?.work?.max_items ?? (edge.data as any)?.work?.maxItems ?? 256)
					)
				};
				if (!['artifact', 'json_items', 'table_rows'].includes(nextWork.item_mode)) {
					out = { ok: false, error: 'Invalid work item mode' };
					return s;
				}
				const nextEdge: Edge<PipelineEdgeData> = {
					...edge,
					data: {
						...(edge.data ?? { exec: 'idle' as const }),
						exec: edge.data?.exec ?? 'idle',
						linkKind: normalizeEdgeLinkKind(edge),
						mode: nextMode as any,
						fatal: Boolean(patch.fatal ?? (edge.data as any)?.fatal ?? false),
						queue: nextQueue,
						work: nextWork
					}
				};
				const chk = isEdgeStillValid(s.nodes, nextEdge);
				if (!chk.ok) {
					out = { ok: false, error: `Edge incompatible after config update (${chk.reason})` };
					return s;
				}
				const edges = [...s.edges];
				edges[idx] = nextEdge;
				const next = logPush({ ...s, edges }, 'info', `Updated edge ${edgeId} config`);
				persist(next);
				return next;
			});
			return out;
		},

		preflightConnection(input: {
			source: string;
			target: string;
			sourceHandle?: string | null;
			targetHandle?: string | null;
			mode?: 'work' | 'param' | 'control' | null;
		}) {
			const source = String(input?.source ?? '').trim();
			const target = String(input?.target ?? '').trim();
			if (!source || !target) {
				return { ok: false as const, error: 'Missing source or target' };
			}
			if (source === target) {
				return { ok: false as const, error: 'Cannot connect node to itself' };
			}
			const state = get({ subscribe } as any) as GraphState;
			const sourceNode = state.nodes.find((node) => node.id === source);
			const targetNode = state.nodes.find((node) => node.id === target);
			if (!sourceNode || !targetNode) {
				return { ok: false as const, error: 'Source or target node not found' };
			}
			const sourceHandle = String(input?.sourceHandle ?? 'out').trim() || 'out';
			const targetHandleRaw = String(input?.targetHandle ?? '').trim();
			const modeRaw = String(input?.mode ?? '').trim().toLowerCase();
			const inferredMode = inferEdgeModeFromHandles({
				sourceHandle,
				targetHandle: targetHandleRaw || undefined
			} as any);
			const mode =
				modeRaw === 'work' || modeRaw === 'param' || modeRaw === 'control'
					? (modeRaw as 'work' | 'param' | 'control')
					: inferredMode;
			const sourceAffinity = nodePortAffinity(sourceNode, 'out', sourceHandle);
			const detailsBase = {
				mode,
				sourceHandle,
				sourceAffinity,
				targetHandle: targetHandleRaw || null
			};

			if (!targetHandleRaw) {
				const declared = declaredPortHandles(targetNode, 'in');
				const candidateHandles = declared.length > 0 ? [...declared] : ['in'];
				if (hasPortHandle(targetNode, 'in', 'in') && !candidateHandles.includes('in')) {
					candidateHandles.unshift('in');
				}
				const compatible = candidateHandles.some((candidate) =>
					edgeModeCompatible(mode, sourceAffinity, nodePortAffinity(targetNode, 'in', candidate))
				);
				if (!compatible) {
					return {
						ok: false as const,
						error: 'No compatible target input handle for this edge mode',
						details: {
							...detailsBase,
							candidateHandles
						}
					};
				}
				return {
					ok: true as const,
					deferred: true as const,
					details: {
						...detailsBase,
						candidateHandles
					}
				};
			}

			if (!hasPortHandle(targetNode, 'in', targetHandleRaw)) {
				return {
					ok: false as const,
					error: `Target handle '${targetHandleRaw}' is not declared for this node`,
					details: detailsBase
				};
			}
			if (portCardinality(targetNode, 'in', targetHandleRaw) === 'one') {
				const existingInbound = state.edges.filter(
					(edge) =>
						String((edge as any).target ?? '') === target &&
						(String((edge as any).targetHandle ?? 'in').trim() || 'in') === targetHandleRaw
				);
				if (existingInbound.length >= 1) {
					return {
						ok: false as const,
						error: `Target handle '${targetHandleRaw}' allows only one inbound edge`,
						details: detailsBase
					};
				}
			}
			const targetAffinity = nodePortAffinity(targetNode, 'in', targetHandleRaw);
			if (!edgeModeCompatible(mode, sourceAffinity, targetAffinity)) {
				return {
					ok: false as const,
					error: 'Edge mode is incompatible with source/target port affinities',
					details: {
						...detailsBase,
						targetHandle: targetHandleRaw,
						targetAffinity
					}
				};
			}
			const edgeCandidate = {
				id: '__preflight__',
				source,
				target,
				sourceHandle,
				targetHandle: targetHandleRaw,
				data: { exec: 'idle', mode }
			} as any;
			const schemaCheck = isEdgeStillValid(state.nodes, edgeCandidate);
			if (!schemaCheck.ok) {
				return {
					ok: false as const,
					error:
						schemaCheck.reason === 'mode_mismatch'
							? 'Edge mode is incompatible with source/target port affinities'
							: schemaCheck.reason === 'type_mismatch'
								? `Incompatible schemas${schemaCheck.suggestion ? `. ${schemaCheck.suggestion}` : ''}`
								: schemaCheck.reason === 'typed_schema_missing'
									? `Missing required typed schema coverage: ${(schemaCheck.missingColumns ?? []).join(', ') || '(unknown)'}`
									: schemaCheck.reason === 'schema_mismatch'
										? `Missing required columns: ${(schemaCheck.missingColumns ?? []).join(', ') || '(unknown)'}`
										: 'Cannot resolve schema compatibility for this connection',
					suggestion: schemaCheck.suggestion ?? null,
					adapterKind: schemaCheck.adapterKind ?? null,
					details: {
						...detailsBase,
						targetHandle: targetHandleRaw,
						targetAffinity
					}
				};
			}
			if (schemaCheck.warning === 'lossy_coercion' || schemaCheck.adapterKind || schemaCheck.suggestion) {
				return {
					ok: true as const,
					deferred: false as const,
					warning: schemaCheck.warning ?? null,
					suggestion: schemaCheck.suggestion ?? null,
					adapterKind: schemaCheck.adapterKind ?? null
				};
			}
			const preflightEdgeId = '__preflight__';
			const sameHandleConflict = sameHandleProvidedSchemaConflict(
				state.nodes as any,
				state.edges as any,
				{
					id: preflightEdgeId,
					source,
					target,
					sourceHandle,
					targetHandle: targetHandleRaw,
					data: { exec: 'idle', mode }
				} as any
			);
			if (sameHandleConflict.conflict) {
				return {
					ok: false as const,
					error:
						'Multiple inbound work edges on the same target handle must provide identical schemas',
					details: {
						...detailsBase,
						targetHandle: targetHandleRaw,
						targetAffinity
					}
				};
			}
			return {
				ok: true as const,
				deferred: false as const,
				details: {
					...detailsBase,
					targetHandle: targetHandleRaw,
					targetAffinity
				}
			};
		},

		addEdge(edge: Edge<PipelineEdgeData>) {
			let out: {
				ok: boolean;
				id?: string;
				error?: string;
				suggestion?: string | null;
				adapterKind?: AdapterTransformKind | null;
			} = { ok: true };
			update((s) => {
				// basic sanity checks
				const sourceExists = s.nodes.some((n) => n.id === edge.source);
				const targetExists = s.nodes.some((n) => n.id === edge.target);
				if (!sourceExists || !targetExists) {
					out = { ok: false, error: 'Source or target node not found' };
					return s;
				}

				// default id if absent
				const id = edge.id ?? `e_${crypto.randomUUID()}`;

				// duplicate id?
				if (s.edges.some((ee) => ee.id === id)) {
					out = { ok: false, error: 'Edge id already exists' };
					return s;
				}

				// no self-connection
				if (edge.source === edge.target) {
					out = { ok: false, error: 'Cannot connect node to itself' };
					return s;
				}

				// basic cycle prevention: if target reaches source already, adding would create cycle
				const adj = new Map<string, string[]>();
				for (const ee of s.edges) adj.set(ee.source, [...(adj.get(ee.source) ?? []), ee.target]);

				const seen = new Set<string>();
				const q = [edge.target];
				let createsCycle = false;
				while (q.length) {
					const cur = q.shift()!;
					if (cur === edge.source) {
						createsCycle = true;
						break;
					}
					for (const nxt of adj.get(cur) ?? []) {
						if (!seen.has(nxt)) {
							seen.add(nxt);
							q.push(nxt);
						}
					}
				}
				if (createsCycle) {
					out = { ok: false, error: 'Connection would create a cycle' };
					return s;
				}

				const canonicalSourceHandle = canonicalComponentSourceHandleForEdge(
					s.nodes,
					{ ...edge, id } as Edge<PipelineEdgeData>
				);
				if (canonicalSourceHandle == null) {
					out = {
						ok: false,
						error: 'Component output handle is required and must match a declared output.'
					};
					return s;
				}
				const edgeForValidation: Edge<PipelineEdgeData> = {
					...edge,
					id,
					sourceHandle: canonicalSourceHandle
				};
				const targetNodeForCardinality = s.nodes.find((n) => n.id === edgeForValidation.target);
				const targetHandle = String((edgeForValidation as any).targetHandle ?? 'in').trim() || 'in';
				if (targetNodeForCardinality && portCardinality(targetNodeForCardinality as any, 'in', targetHandle) === 'one') {
					const existingInbound = s.edges.filter(
						(existing) =>
							String((existing as any).target ?? '') === String(edgeForValidation.target ?? '') &&
							(String((existing as any).targetHandle ?? 'in').trim() || 'in') === targetHandle
					);
					if (existingInbound.length >= 1) {
						out = {
							ok: false,
							error: `Target handle '${targetHandle}' allows only one inbound edge`
						};
						return s;
					}
				}

				// Validate schema compatibility and refresh edge contract metadata.
				const chk = isEdgeStillValid(s.nodes, edgeForValidation);
				if (chk.ok === false) {
					out = {
						ok: false,
						suggestion: chk.suggestion,
						adapterKind: chk.adapterKind,
						error:
							chk.reason === 'mode_mismatch'
								? 'Edge mode is incompatible with source/target port affinities'
							: chk.reason === 'type_mismatch'
								? `Incompatible schemas${chk.suggestion ? `. ${chk.suggestion}` : ''}`
								: chk.reason === 'typed_schema_missing'
									? `Missing required typed schema coverage: ${(chk.missingColumns ?? []).join(', ') || '(unknown)'}`
								: chk.reason === 'schema_mismatch'
									? `Missing required columns: ${(chk.missingColumns ?? []).join(', ') || '(unknown)'}`
								: 'Cannot resolve schema compatibility for this connection'
					};
					return logPush(
						s,
						'info',
						`[schema-edge-checks-v2] decision=block edge=${id} reason=${chk.reason}`,
						edge.source
					);
				}
				const sameHandleConflict = sameHandleProvidedSchemaConflict(
					s.nodes as any,
					s.edges as any,
					edgeForValidation
				);
				if (sameHandleConflict.conflict) {
					out = {
						ok: false,
						error:
							'Multiple inbound work edges on the same target handle must provide identical schemas'
					};
					return logPush(
						s,
						'info',
						`[schema-edge-checks-v2] decision=block edge=${id} reason=multi_edge_same_handle_schema_mismatch target=${sameHandleConflict.targetNodeId}:${sameHandleConflict.targetHandle}`,
						edge.source
					);
				}
				const sourceNode = s.nodes.find((n) => n.id === edgeForValidation.source)!;
				const targetNode = s.nodes.find((n) => n.id === edgeForValidation.target)!;
				const sourceHint = sourcePayloadHint(
					sourceNode as any,
					'out',
					String((edgeForValidation as any).sourceHandle ?? 'out')
				);
				const targetHint = targetPayloadHint(targetNode as any);
				const constraintProvidedSchema = buildProvidedSchema(
					sourceNode as any,
					String((edgeForValidation as any).sourceHandle ?? 'out')
				);
				const constraintRequiredSchema = buildRequiredSchema(
					targetNode as any,
					String((edgeForValidation as any).targetHandle ?? 'in')
				);
				const providedType = normalizeHintType(sourceHint?.type ?? chk.out ?? 'unknown');
				const requiredType = normalizeHintType(targetHint?.type ?? chk.in ?? 'unknown');
				const coercion = evaluateSchemaCoercion(providedType, requiredType);
				const adapterKind = adapterKindForTypes(providedType, requiredType);
				if (adapterKind) {
					out.adapterKind = adapterKind;
					out.suggestion = adapterSuggestionForTypes(providedType, requiredType);
				}
				const edgeMode = normalizeEdgeMode(edgeForValidation as any);
				const explicitItemModeRaw = String(
					(edge.data as any)?.work?.item_mode ?? (edge.data as any)?.work?.itemMode ?? ''
				)
					.trim()
					.toLowerCase();
				const explicitItemMode =
					explicitItemModeRaw === 'artifact' ||
					explicitItemModeRaw === 'json_items' ||
					explicitItemModeRaw === 'table_rows'
						? (explicitItemModeRaw as 'artifact' | 'json_items' | 'table_rows')
						: null;
				const inferredDefaultItemMode: 'artifact' | 'json_items' | 'table_rows' =
					providedType === 'table'
						? 'table_rows'
						: providedType === 'json'
							? 'json_items'
							: 'artifact';
				const nextItemMode: 'artifact' | 'json_items' | 'table_rows' =
					explicitItemMode ?? (edgeMode === 'work' ? inferredDefaultItemMode : 'artifact');

				const nextEdge: Edge<PipelineEdgeData> = {
					...edgeForValidation,
					id,
					data: {
						...(edge.data ?? {}),
						exec: edge.data?.exec ?? 'idle',
						linkKind: normalizeEdgeLinkKind(edgeForValidation as any),
						mode: edgeMode,
						fatal: Boolean((edge.data as any)?.fatal ?? false),
						queue: {
							max: Math.max(1, Number((edge.data as any)?.queue?.max ?? 1000)),
							overflow: String((edge.data as any)?.queue?.overflow ?? 'block').trim().toLowerCase() as
								| 'block'
								| 'spill'
								| 'error',
							policy: (
								(() => {
									const raw = String((edge.data as any)?.queue?.policy ?? 'fifo').trim().toLowerCase();
									return raw === 'round_robin' ? 'round_robin' : 'fifo';
								})()
							) as 'fifo' | 'round_robin'
						},
						work: {
							item_mode: nextItemMode,
							max_items: Math.max(1, Number((edge.data as any)?.work?.max_items ?? (edge.data as any)?.work?.maxItems ?? 256))
						},
						contract: {
							out: chk.out,
							in: chk.in,
							payload: {
								source: constraintProvidedSchema,
								target: constraintRequiredSchema
							},
							snapshot: edgeContractSnapshotFromSchemas(
								constraintProvidedSchema as Record<string, any>,
								constraintRequiredSchema as Record<string, any>,
								{ ok: true },
								normalizeEdgeMode(edgeForValidation as any)
							)
						}
					}
				};

				const decision = adapterKind ? 'adapter' : coercion.mode === 'native' ? 'native' : 'coerced';
				let nextState: GraphState = { ...s, edges: [...s.edges, nextEdge] };
				nextState = logPush(
					nextState,
					'info',
					`[schema-edge-checks-v2] decision=${decision} edge=${id} source=${providedType} target=${requiredType}`
				);
				const next = logPush(nextState, 'info', `Added edge ${id}`);
				persist(next);
				out.id = id;
				return next;
			});

			return out;
		},

		insertSchemaAdapterForEdgeConnection(input: {
			source: string;
			target: string;
			sourceHandle?: string | null;
			targetHandle?: string | null;
			adapterKind?: AdapterTransformKind | null;
		}) {
			const source = String(input?.source ?? '').trim();
			const target = String(input?.target ?? '').trim();
			if (!source || !target) {
				return { ok: false as const, error: 'Missing source or target for adapter insertion' };
			}

			const state = get({ subscribe } as any) as GraphState;
			const sourceNode = state.nodes.find((n) => n.id === source);
			const targetNode = state.nodes.find((n) => n.id === target);
			if (!sourceNode || !targetNode) {
				return { ok: false as const, error: 'Source or target node not found' };
			}

			const sourceHandleRaw = String(input?.sourceHandle ?? '').trim();
			const sourceHandle = sourceHandleRaw.length > 0 ? sourceHandleRaw : undefined;
			const targetHandleRaw = String(input?.targetHandle ?? '').trim();
			const targetHandle = targetHandleRaw.length > 0 ? targetHandleRaw : undefined;
			const sourceHint = sourcePayloadHint(sourceNode as any, 'out', sourceHandle ?? 'out');
			const targetHint = targetPayloadHint(targetNode as any);
			const providedType = normalizeHintType(sourceHint?.type ?? 'unknown');
			const requiredType = normalizeHintType(targetHint?.type ?? 'unknown');
			const adapterKind = (input?.adapterKind ?? adapterKindForTypes(providedType, requiredType)) as
				| AdapterTransformKind
				| null;
			if (!adapterKind) {
				return {
					ok: false as const,
					error: `No adapter available for ${providedType}->${requiredType}`
				};
			}

			const midX = (Number(sourceNode.position?.x ?? 0) + Number(targetNode.position?.x ?? 0)) / 2;
			const midY = (Number(sourceNode.position?.y ?? 0) + Number(targetNode.position?.y ?? 0)) / 2;
			const adapterNodeId = this.addNode('transform', { x: midX, y: midY });
			const subtypeRes = this.setTransformKind(adapterNodeId, adapterKind);
			if (!subtypeRes.ok) {
				this.deleteNode(adapterNodeId);
				return {
					ok: false as const,
					error: String(subtypeRes.error ?? 'Failed to configure adapter node')
				};
			}

			const incomingRes = this.addEdge({
				id: `e_${crypto.randomUUID()}`,
				source,
				target: adapterNodeId,
				sourceHandle,
				targetHandle: 'in',
				data: { exec: 'idle', linkKind: 'data_link', mode: 'work' as any }
			} as Edge<PipelineEdgeData>);
			if (!incomingRes.ok) {
				this.deleteNode(adapterNodeId);
				return {
					ok: false as const,
					error: String(incomingRes.error ?? 'Failed to connect source to adapter')
				};
			}

			const outgoingRes = this.addEdge({
				id: `e_${crypto.randomUUID()}`,
				source: adapterNodeId,
				target,
				sourceHandle: 'out',
				targetHandle,
				data: { exec: 'idle', linkKind: 'data_link', mode: 'work' as any }
			} as Edge<PipelineEdgeData>);
			if (!outgoingRes.ok) {
				if (incomingRes.id) this.deleteEdge(incomingRes.id);
				this.deleteNode(adapterNodeId);
				return {
					ok: false as const,
					error: String(outgoingRes.error ?? 'Failed to connect adapter to target')
				};
			}

			return {
				ok: true as const,
				adapterKind,
				adapterNodeId,
				incomingEdgeId: incomingRes.id ?? null,
				outgoingEdgeId: outgoingRes.id ?? null
			};
		},

		updateNodeTitle(nodeId: string, label: string) {
			const state = get({ subscribe } as any) as GraphState;
			const cleaned = String(label ?? '').trim();
			const normalized = normalizeNodeName(cleaned);
			if (!normalized) {
				return { ok: false as const, error: 'Node name cannot be empty.' };
			}
			const duplicateNodeId = findNodeIdByName(state.nodes as Node<PipelineNodeData>[], cleaned, {
				excludeNodeId: nodeId
			});
			if (duplicateNodeId) {
				return {
					ok: false as const,
					error: `Node name "${cleaned}" already exists in this graph.`,
					reason: 'duplicate_name_in_scope' as const,
					existingNodeId: duplicateNodeId
				};
			}
			update((s) => {
				const nodes = s.nodes.map((n) =>
					n.id === nodeId ? { ...n, data: { ...n.data, label: cleaned } } : n
				);
				const next = { ...s, nodes };
				persist(next);
				return next;
			});
			return { ok: true as const };
		},

		validateNodeName(name: string, opts?: { excludeNodeId?: string | null }) {
			const state = get({ subscribe } as any) as GraphState;
			const cleaned = String(name ?? '').trim();
			const normalized = normalizeNodeName(cleaned);
			if (!normalized) {
				return { ok: false as const, error: 'Node name cannot be empty.' };
			}
			const duplicateNodeId = findNodeIdByName(state.nodes as Node<PipelineNodeData>[], cleaned, {
				excludeNodeId: String(opts?.excludeNodeId ?? '').trim() || null
			});
			if (duplicateNodeId) {
				return {
					ok: false as const,
					error: `Node name "${cleaned}" already exists in this graph.`,
					reason: 'duplicate_name_in_scope' as const,
					existingNodeId: duplicateNodeId
				};
			}
			return { ok: true as const, cleanedName: cleaned };
		},

		setNodeFreezeMode(nodeId: string, mode: 'per_run' | 'sticky' | null) {
			let out: { ok: boolean; error?: string } = { ok: true };
			update((s) => {
				const targetNode = s.nodes.find((n) => n.id === nodeId) as
					| Node<PipelineNodeData & Record<string, unknown>>
					| undefined;
				if (!targetNode) {
					out = { ok: false, error: 'Node not found.' };
					return s;
				}
				if (mode !== null) {
					const eligibility = validatePinEligibility(
						targetNode,
						_normalizeBinding(s.nodeBindings?.[nodeId], nodeId)
					);
					if (!eligibility.ok) {
						out = { ok: false, error: eligibility.error };
						const inspector =
							String(s.inspector?.nodeId ?? '') === nodeId
								? { ...s.inspector, systemNotice: eligibility.error }
								: s.inspector;
						return logPush({ ...s, inspector }, 'warn', eligibility.error, nodeId);
					}
				}
				const nodes = s.nodes.map((n) => {
					if (n.id !== nodeId) return n;
					const nextMeta = { ...(((n.data as any)?.meta ?? {}) as Record<string, unknown>) };
					if (mode === null) {
						delete (nextMeta as any).freeze;
					} else {
						(nextMeta as any).freeze = { enabled: true, mode };
					}
					nextMeta.updatedAt = new Date().toISOString();
					return {
						...n,
						data: {
							...(n.data as any),
							meta: nextMeta
						}
					};
				});
				const recomputedActiveRunNodeSet = computePlannedNodeSet(
					nodes as any,
					s.edges as any,
					s.activeRunFrom,
					s.activeRunMode
				);
				const next = withGraphMeta({
					...s,
					nodes,
					activeRunNodeSet: recomputedActiveRunNodeSet
				});
				persist(next);
				return next;
			});
			return out;
		},

		setSelectedNodeFreezeMode(mode: 'per_run' | 'sticky' | null) {
			const cur = get({ subscribe } as any) as GraphState;
			const nodeId = String(cur.selectedNodeId ?? '').trim();
			if (!nodeId) return { ok: false as const, error: 'No node selected' };
			return this.setNodeFreezeMode(nodeId, mode);
		},

		updateNodeProcessingPolicy(
			nodeId: string,
			patch: {
				consume_mode?: 'once' | 'single_item' | 'batch';
				batch_size?: number;
				max_inflight?: number;
				read_once?: boolean;
				on_error?: 'fail_fast' | 'skip_failed';
			}
		) {
			let out: { ok: boolean; error?: string } = { ok: true };
			update((s) => {
				const node = s.nodes.find((n) => n.id === nodeId);
				if (!node) {
					out = { ok: false, error: 'Node not found' };
					return s;
				}
				const existing = ((node.data as any)?.processingPolicy ?? {}) as Record<string, any>;
				const nextMode = String(patch.consume_mode ?? existing.consume_mode ?? 'once').trim().toLowerCase();
				if (!['once', 'single_item', 'batch'].includes(nextMode)) {
					out = { ok: false, error: 'Invalid consume mode' };
					return s;
				}
				const nextPolicy = {
					...(existing as Record<string, any>),
					consume_mode: nextMode as 'once' | 'single_item' | 'batch',
					batch_size: Math.max(1, Number(patch.batch_size ?? existing.batch_size ?? 1)),
					max_inflight: Math.max(1, Number(patch.max_inflight ?? existing.max_inflight ?? 1)),
					read_once: nextMode === 'once',
					...(patch.on_error ? { on_error: patch.on_error } : {})
				};
				const nodes = s.nodes.map((n) =>
					n.id === nodeId
						? {
								...n,
								data: {
									...n.data,
									processingPolicy: nextPolicy
								}
							}
						: n
				);
				const next = logPush({ ...s, nodes }, 'info', `Updated node ${nodeId} processing policy`);
				persist(next);
				return next;
			});
			return out;
		},

		updateNodeInputHandleProcessingPolicy(
			nodeId: string,
			inputHandle: string,
			patch: {
				consume_mode?: 'once' | 'single_item' | 'batch';
				batch_size?: number;
				max_inflight?: number;
				read_once?: boolean;
			}
		) {
			let out: { ok: boolean; error?: string } = { ok: true };
			update((s) => {
				const node = s.nodes.find((n) => n.id === nodeId);
				if (!node) {
					out = { ok: false, error: 'Node not found' };
					return s;
				}
				const handle = String(inputHandle ?? '').trim() || 'in';
				const existing = ((node.data as any)?.processingPolicy ?? {}) as Record<string, any>;
				const existingByHandle =
					existing.input_handles && typeof existing.input_handles === 'object'
						? (existing.input_handles as Record<string, any>)
						: {};
				const existingHandle = (existingByHandle[handle] ?? {}) as Record<string, any>;
				const nextMode = String(patch.consume_mode ?? existingHandle.consume_mode ?? 'once').trim().toLowerCase();
				if (!['once', 'single_item', 'batch'].includes(nextMode)) {
					out = { ok: false, error: 'Invalid consume mode' };
					return s;
				}
				const nextHandlePolicy = {
					consume_mode: nextMode as 'once' | 'single_item' | 'batch',
					batch_size: Math.max(1, Number(patch.batch_size ?? existingHandle.batch_size ?? existing.batch_size ?? 1)),
					max_inflight: Math.max(
						1,
						Number(patch.max_inflight ?? existingHandle.max_inflight ?? existing.max_inflight ?? 1)
					),
					read_once: nextMode === 'once'
				};
				const nextPolicy = {
					...(existing as Record<string, any>),
					input_handles: {
						...existingByHandle,
						[handle]: nextHandlePolicy
					}
				};
				const nodes = s.nodes.map((n) =>
					n.id === nodeId
						? {
								...n,
								data: {
									...n.data,
									processingPolicy: nextPolicy
								}
							}
						: n
				);
				const next = logPush(
					{ ...s, nodes },
					'info',
					`Updated node ${nodeId} processing policy for input handle ${handle}`
				);
				persist(next);
				return next;
			});
			return out;
		},

		updateNodePortDeclaration(
			nodeId: string,
			direction: 'in' | 'out',
			handle: string,
			patch: {
				plane?: 'work' | 'param' | 'control';
				required?: boolean;
				cardinality?: 'one' | 'many';
				behavior?: 'once' | 'single_item' | 'batch';
			}
		) {
			let out: { ok: boolean; error?: string } = { ok: true };
			update((s) => {
				const node = s.nodes.find((n) => n.id === nodeId);
				if (!node) {
					out = { ok: false, error: 'Node not found' };
					return s;
				}
				const dir = direction === 'out' ? 'out' : 'in';
				const key = String(handle ?? '').trim();
				if (!key) {
					out = { ok: false, error: 'Handle is required' };
					return s;
				}
				const data = (node.data ?? {}) as Record<string, any>;
				const existingDecls =
					data.portDeclarations && typeof data.portDeclarations === 'object'
						? (data.portDeclarations as Record<string, any>)
						: {};
				const byDir =
					existingDecls[dir] && typeof existingDecls[dir] === 'object'
						? (existingDecls[dir] as Record<string, any>)
						: {};
				const existing = (byDir[key] ?? {}) as Record<string, any>;
				const nextPlane = String(patch.plane ?? existing.plane ?? existing.affinity ?? 'work')
					.trim()
					.toLowerCase();
				if (!['work', 'param', 'control'].includes(nextPlane)) {
					out = { ok: false, error: 'Invalid plane' };
					return s;
				}
				const nextCardinality = String(patch.cardinality ?? existing.cardinality ?? 'many')
					.trim()
					.toLowerCase();
				if (!['one', 'many'].includes(nextCardinality)) {
					out = { ok: false, error: 'Invalid cardinality' };
					return s;
				}
				const nextDecl: Record<string, any> = {
					plane: nextPlane,
					affinity: nextPlane,
					required: Boolean(patch.required ?? existing.required ?? false),
					cardinality: nextCardinality
				};
				if (dir === 'in') {
					const nextBehavior = String(patch.behavior ?? existing.behavior ?? 'single_item')
						.trim()
						.toLowerCase();
					if (!['once', 'single_item', 'batch'].includes(nextBehavior)) {
						out = { ok: false, error: 'Invalid behavior' };
						return s;
					}
					nextDecl.behavior = nextBehavior;
				}
				const nextPortDeclarations: Record<string, any> = {
					...existingDecls,
					[dir]: {
						...byDir,
						[key]: nextDecl
					}
				};
				const nextPortContractsByDir: Record<string, any> = {
					...((data.portContracts && typeof data.portContracts === 'object'
						? data.portContracts
						: {}) as Record<string, any>),
					[dir]: {
						...(((data.portContracts as any)?.[dir] ?? {}) as Record<string, any>),
						[key]: {
							affinity: nextPlane,
							...(dir === 'in' ? { behavior: String(nextDecl.behavior ?? 'single_item') } : {})
						}
					}
				};
				const nodes = s.nodes.map((n) =>
					n.id === nodeId
						? {
								...n,
								data: {
									...n.data,
									portDeclarations: nextPortDeclarations,
									portContracts: nextPortContractsByDir
								}
							}
						: n
				);
				const next = logPush(
					{ ...s, nodes },
					'info',
					`Updated node ${nodeId} ${dir} port declaration ${key}`
				);
				persist(next);
				return next;
			});
			return out;
		},

		removeNodePortDeclaration(nodeId: string, direction: 'in' | 'out', handle: string) {
			let out: { ok: boolean; error?: string } = { ok: true };
			update((s) => {
				const node = s.nodes.find((n) => n.id === nodeId);
				if (!node) {
					out = { ok: false, error: 'Node not found' };
					return s;
				}
				const dir = direction === 'out' ? 'out' : 'in';
				const key = String(handle ?? '').trim();
				if (!key) {
					out = { ok: false, error: 'Handle is required' };
					return s;
				}
				const data = (node.data ?? {}) as Record<string, any>;
				const existingDecls =
					data.portDeclarations && typeof data.portDeclarations === 'object'
						? (data.portDeclarations as Record<string, any>)
						: {};
				const byDir =
					existingDecls[dir] && typeof existingDecls[dir] === 'object'
						? ({ ...(existingDecls[dir] as Record<string, any>) } as Record<string, any>)
						: {};
				if (!Object.prototype.hasOwnProperty.call(byDir, key)) {
					return s;
				}
				const previousHandles = declaredPortHandles(node as Node<PipelineNodeData>, dir);
				const previousIndex = previousHandles.indexOf(key);
				delete byDir[key];
				const nextPortDeclarations = {
					...existingDecls,
					[dir]: byDir
				};
				const existingContracts =
					data.portContracts && typeof data.portContracts === 'object'
						? ({ ...(data.portContracts as Record<string, any>) } as Record<string, any>)
						: {};
				const nextContractsByDir =
					existingContracts[dir] && typeof existingContracts[dir] === 'object'
						? ({ ...(existingContracts[dir] as Record<string, any>) } as Record<string, any>)
						: {};
				delete nextContractsByDir[key];
				existingContracts[dir] = nextContractsByDir;
				const nodes = s.nodes.map((n) =>
					n.id === nodeId
						? {
								...n,
								data: {
									...n.data,
									portDeclarations: nextPortDeclarations,
									portContracts: existingContracts
								}
							}
						: n
				);
				const updatedNode = nodes.find((n) => n.id === nodeId) as Node<PipelineNodeData> | undefined;
				const remainingHandles = updatedNode ? declaredPortHandles(updatedNode, dir) : [];
				const nextHandleForRemoved = (() => {
					if (remainingHandles.length === 0) return null;
					if (previousIndex < 0) return remainingHandles[0];
					const idx = Math.max(0, Math.min(previousIndex, remainingHandles.length - 1));
					return remainingHandles[idx];
				})();
				let droppedEdges = 0;
				const edges = s.edges.flatMap((edge) => {
					const touchesNode =
						dir === 'in' ? String(edge.target ?? '') === nodeId : String(edge.source ?? '') === nodeId;
					if (!touchesNode) return [edge];
					const edgeHandle =
						dir === 'in'
							? String((edge as any).targetHandle ?? 'in').trim() || 'in'
							: String((edge as any).sourceHandle ?? 'out').trim() || 'out';
					if (edgeHandle !== key) {
						// Re-clone to force edge anchor refresh when handle layout changes.
						return [{ ...edge }];
					}
					if (!nextHandleForRemoved) {
						droppedEdges += 1;
						return [];
					}
					if (dir === 'in') {
						return [{ ...edge, targetHandle: nextHandleForRemoved }];
					}
					return [{ ...edge, sourceHandle: nextHandleForRemoved }];
				});
				const next = logPush(
					{ ...s, nodes, edges },
					'info',
					`Removed node ${nodeId} ${dir} port declaration ${key}${droppedEdges > 0 ? ` (dropped ${droppedEdges} edge${droppedEdges === 1 ? '' : 's'})` : ''}`
				);
				persist(next);
				return next;
			});
			return out;
		},

		setNodeMeta(nodeId: string, patch: Record<string, unknown>) {
			update((s) => {
				const node = s.nodes.find((n) => n.id === nodeId);
				if (!node) return s;
				const nodes = s.nodes.map((n) =>
					n.id === nodeId
						? {
								...n,
								data: {
									...n.data,
									meta: {
										...(n.data.meta ?? {}),
										...patch,
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
		},

		...runManager.actions,
		hardResetGraph() {
			const freshGraphId = mintGraphId();
			const next = buildHardResetState(freshGraphId);
			persist(next);
			set(next);
			history.resetToSnapshot(stripToDTO(next.nodes as any, next.edges as any, next.graphId));
		},

		clearDraft() {
			clearGraphDraft();
		},

		loadGraphDocument(graph: { nodes: unknown[]; edges: unknown[] }, graphIdOverride?: string | null) {
			const applied = applyGraphDocument(graph, graphIdOverride);
			if (!applied.ok) return { ok: false, reason: 'invalid_payload' as const };
			return { ok: true };
		},

		async saveGraph(message?: string, opts?: { graphName?: string }) {
			const current = get({ subscribe } as any) as GraphState;
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
			const strictGraph = buildPersistableGraphStrict(current.nodes as any, current.edges as any, graphId);
			if (!strictGraph.ok) return { ok: false, reason: 'invalid_graph' as const, error: strictGraph.error };
			const graph = strictGraph.graph;
			const canvasGraph = stripToDTO(current.nodes as any, current.edges as any, graphId);
			const strictCanvasGraph = buildPersistableGraphStrict(
				canvasGraph.nodes as any,
				canvasGraph.edges as any,
				graphId
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
		},

		async saveGraphVersion(versionName: string, message?: string) {
			const current = get({ subscribe } as any) as GraphState;
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
			const strictGraph = buildPersistableGraphStrict(current.nodes as any, current.edges as any, graphId);
			if (!strictGraph.ok) return { ok: false, reason: 'invalid_graph' as const, error: strictGraph.error };
			const graph = strictGraph.graph;
			const canvasGraph = stripToDTO(current.nodes as any, current.edges as any, graphId);
			const strictCanvasGraph = buildPersistableGraphStrict(
				canvasGraph.nodes as any,
				canvasGraph.edges as any,
				graphId
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
		},

		async saveGraphAs(graphName: string, message?: string, versionName?: string) {
			const nextGraphName = String(graphName ?? '').trim();
			if (!nextGraphName) return { ok: false, reason: 'missing_graph_name' as const };
			const current = get({ subscribe } as any) as GraphState;
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
				current.graphId
			);
			if (!strictGraph.ok) return { ok: false, reason: 'invalid_graph' as const, error: strictGraph.error };
			const graph = strictGraph.graph;
			const canvasGraph = stripToDTO(current.nodes as any, current.edges as any, current.graphId);
			const strictCanvasGraph = buildPersistableGraphStrict(
				canvasGraph.nodes as any,
				canvasGraph.edges as any,
				current.graphId
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
		},

		async listGraphs(limit = 50, offset = 0) {
			try {
				const listed = await listGraphsClient(limit, offset);
				return {
					ok: true,
					graphs: Array.isArray(listed.graphs) ? listed.graphs : []
				};
			} catch (error) {
				return { ok: false, reason: 'list_failed' as const, error: String(error) };
			}
		},

		async listGraphRevisionHistory(limit = 30, offset = 0) {
			const current = get({ subscribe } as any) as GraphState;
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
		},

		async listGraphRevisionHistoryForGraph(graphId: string, limit = 30, offset = 0) {
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
		},

		async restoreGraphRevision(revisionId: string) {
			const current = get({ subscribe } as any) as GraphState;
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
		},

		async loadGraphRevision(graphId: string, revisionId: string) {
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
		},

		async deleteGraph(graphId: string) {
			const gid = String(graphId ?? '').trim();
			if (!gid) return { ok: false, reason: 'missing_graph_id' as const };
			try {
				const deleted = await deleteGraphClient(gid);
				return { ok: true, deleted };
			} catch (error) {
				return { ok: false, reason: 'delete_failed' as const, error: String(error) };
			}
		},

		async deleteGraphRevision(graphId: string, revisionId: string) {
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
		},

		async hydrateLatestGraphFromBackend() {
			if (typeof window === 'undefined') return { ok: false, reason: 'non_browser' as const };
			try {
				const current = get({ subscribe } as any) as GraphState;
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
		},

		async listComponentCatalog(limit = 100, offset = 0) {
			try {
				const components = await listComponents(limit, offset);
				return { ok: true, components };
			} catch (error) {
				return { ok: false, reason: 'list_components_failed' as const, error: String(error) };
			}
		},

		async listComponentRevisionHistory(componentId: string, limit = 100, offset = 0) {
			try {
				const revisions = await listComponentRevisions(componentId, limit, offset);
				return { ok: true, revisions };
			} catch (error) {
				return { ok: false, reason: 'list_revisions_failed' as const, error: String(error) };
			}
		},

		async getComponentRevisionDetail(componentId: string, revisionId: string) {
			try {
				const detail = await getComponentRevision(componentId, revisionId);
				return { ok: true, detail };
			} catch (error) {
				return { ok: false, reason: 'get_revision_failed' as const, error: String(error) };
			}
		},

		async openComponentRevisionForEditing(componentId: string, revisionId: string, entryNodeId?: string | null) {
			const cid = String(componentId ?? '').trim();
			const rid = String(revisionId ?? '').trim();
			if (!cid || !rid) return { ok: false, reason: 'missing_component_ref' as const };
			try {
				const before = get({ subscribe } as any) as GraphState;
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
				const graph = (detail?.definition?.graph ?? {}) as { nodes?: unknown[]; edges?: unknown[] };
				const applied = applyGraphDocument(
					{
						nodes: Array.isArray(graph?.nodes) ? graph.nodes : [],
						edges: Array.isArray(graph?.edges) ? graph.edges : []
					},
					null
				);
				if (!applied.ok) {
					return { ok: false, reason: 'invalid_payload' as const, error: String(applied.reason ?? 'invalid_payload') };
				}
					update((s) => {
						const next = {
							...s,
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
								[draftCacheKey]: contractDraftParams
							},
							lastRunStatus: 'never_run' as const,
						logs: [
							...(Array.isArray(s.logs) ? s.logs : []),
							{
								id: nextLogId(),
								ts: new Date().toLocaleTimeString(),
								level: 'info' as const,
								message: `[component-edit] Loaded internals: ${cid}@${rid}`
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
		},

		returnFromComponentEditSession() {
			const state = get({ subscribe } as any) as GraphState;
			const session = state.componentEditSession;
			if (!session) return { ok: false as const, reason: 'no_component_edit_session' as const };
			const snapshot = session.snapshot;
			const parentSession = session.parentSession ? structuredClone(session.parentSession) : null;
			update((s) => {
				const nextEditingContext: EditorContext = parentSession ? 'component' : 'graph';
				const next: GraphState = {
					...s,
					graphId: snapshot.graphId,
					nodes: structuredClone(snapshot.nodes),
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
					editingContext: nextEditingContext,
					componentEditSession: parentSession
				};
				persist(next);
				return withGraphMeta(next);
			}, { source: 'graph_edit' });
			return { ok: true as const, hasParentSession: Boolean(parentSession) };
		},

		updateComponentEditSessionRevision(revisionId: string) {
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
							}
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
		},

		patchComponentEditContractDraft(
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
				const nextNotice =
					intent === 'system_canonicalize' && changed
						? String(opts?.notice ?? 'Component contract normalized automatically.')
						: null;
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
								[cacheKey]: nextDraft
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
		},

		applySavedComponentRevisionToReturnGraph(
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
		},

		async forkComponentRevisionToNode(
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
				const apply = await this.applyComponentRevisionToNode(
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
		},

		async renameComponent(componentId: string, nextComponentId: string) {
			try {
				const renamed = await renameComponent(componentId, nextComponentId);
				const fromId = String(componentId ?? '').trim();
				const toId = String((renamed as any)?.componentId ?? nextComponentId ?? '').trim();
				if (fromId && toId && fromId !== toId) {
					const state = get({ subscribe } as any) as GraphState;
					const componentNodeIds = state.nodes
						.filter((n) => {
							if (n.data.kind !== 'component') return false;
							const currentId = String(((n.data.params as any)?.componentRef?.componentId ?? '')).trim();
							return currentId === fromId;
						})
						.map((n) => n.id);
					for (const nodeId of componentNodeIds) {
						const node = (get({ subscribe } as any) as GraphState).nodes.find((n) => n.id === nodeId);
						const existingRef = ((node?.data?.params as any)?.componentRef ?? {}) as Record<string, unknown>;
						const patch = {
							componentRef: {
								...existingRef,
								componentId: toId
							}
						};
						const result = updateNodeConfigImpl(nodeId, { params: patch });
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
		},

		async deleteComponent(componentId: string) {
			try {
				const deleted = await deleteComponent(componentId);
				return { ok: true, deleted };
			} catch (error) {
				return { ok: false, reason: 'delete_component_failed' as const, error: String(error) };
			}
		},

		async deleteComponentRevision(componentId: string, revisionId: string) {
			try {
				const deleted = await deleteComponentRevision(componentId, revisionId);
				return { ok: true, deleted };
			} catch (error) {
				return { ok: false, reason: 'delete_component_revision_failed' as const, error: String(error) };
			}
		},

		async applyComponentRevisionToNode(nodeId: string, componentId: string, revisionId: string) {
			const cid = String(componentId ?? '').trim();
			const rid = String(revisionId ?? '').trim();
			if (!cid || !rid) return { ok: false, reason: 'missing_component_ref' as const };
			const node = (get({ subscribe } as any) as GraphState).nodes.find((n) => n.id === nodeId);
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
				const result = updateNodeConfigImpl(nodeId, {
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
				const stateAfter = get({ subscribe } as any) as GraphState;
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
		},

		};
})();

export const selectedNode = derived(graphStore, ($s) =>
	$s.selectedNodeId ? ($s.nodes.find((n) => n.id === $s.selectedNodeId) ?? null) : null
);

export const edgeSchemaConstraints = derived(graphStore, ($s) =>
	computeEdgeSchemaConstraintsInternal($s.nodes as any, $s.edges as any)
);

export const edgeSchemaDiagnostics = derived(edgeSchemaConstraints, ($constraints) =>
	computeEdgeSchemaDiagnosticsInternal($constraints as any)
);


export function getNodeDocResolvedFromState(state: GraphState, nodeId: string): NodeDocResolved | null {
	return resolveNodeDocForState(state, nodeId);
}

export function getNodeDocExplanationModeFromState(state: GraphState): NodeDocExplanationMode {
	const parsed = NodeDocExplanationModeSchema.safeParse((state as any)?.nodeDocExplanationMode);
	return parsed.success ? parsed.data : 'default';
}

export function getNodeDocTrainingModeFromState(state: GraphState): NodeDocTrainingMode {
	const parsed = NodeDocTrainingModeSchema.safeParse((state as any)?.nodeDocTrainingMode);
	return parsed.success ? parsed.data : 'off';
}

export function getNodeDocTooltipEnabledFromState(state: GraphState): boolean {
	return Boolean((state as any)?.nodeDocTooltipEnabled ?? true);
}

export function getNodeDocTooltipOpenDelayMsFromState(state: GraphState): number {
	const raw = Number((state as any)?.nodeDocTooltipOpenDelayMs ?? 500);
	if (!Number.isFinite(raw)) return 500;
	return Math.max(0, Math.min(10000, Math.round(raw)));
}

export function getNodeDocPlanesExpansionEnabledFromState(state: GraphState): boolean {
	return Boolean((state as any)?.nodeDocPlanesExpansionEnabled ?? true);
}

export function getNodeDocPlanesExpansionDelayMsFromState(state: GraphState): number {
	const raw = Number((state as any)?.nodeDocPlanesExpansionDelayMs ?? 1200);
	if (!Number.isFinite(raw)) return 1200;
	return Math.max(0, Math.min(15000, Math.round(raw)));
}

