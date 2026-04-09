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
import { TOOL_BUILTIN_PROFILE_IDS } from '$lib/flow/schema/toolBuiltinProfiles';
import { validateCustomPackageDraft } from '$lib/flow/schema/toolBuiltinCustomPackages';
import { evaluateSchemaCoercion } from '$lib/flow/schema/coercionPolicy';
import type { SchemaDiagnosticCode } from '$lib/flow/schema/diagnosticsContract';
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
	deleteComponentRevision
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
	mintGraphId,
	buildHardResetState,
	captureComponentEditSnapshot,
	stripToDTO,
	edgeStructuralSignature,
	shouldPreserveStoreEdgesOnCanvasSync,
	normalizeComponentPayloadTypeOrDefault,
	normalizeComponentNodeForMigration,
	normalizeGraphForComponentMigration,
	setEdgeExec,
	downstreamIds,
	pruneAndRecontractEdgesStrict,
	canonicalizeComponentEdgeSourceHandles,
	recomputeEdgeContractsBestEffort,
	topoFrom,
	createGraphEditManager,
} from './graphStore.graph-edit';
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
import {
	createPersistenceManager,
	loadNodeDocExplanationMode,
	loadNodeDocTrainingMode,
} from './graphStore.persistence';
export { __computeSaveConsistencyMismatchForTest } from './graphStore.persistence';
export { resolveNodeInputsFromState } from './graphStore.persistence';
export { __stripToDTOForTest, __hardResetGraphForTest } from './graphStore.graph-edit';

// re-export test hooks that moved to graphStore.audit
export const __assertBindingPairForTest = __assertBindingPairForTestFromAudit;
export const __normalizeBindingForTest = __normalizeBindingForTestFromAudit;

// re-export test hooks that moved to graphStore.run
export { __applyRunEventForTest, __hydrateFromRunSnapshotForTest, __resetRunUiStateForTest, __collectPinnedArtifactsByNodeForTest } from './graphStore.run';
export const __setPauseResumeTraceEnabledForTest = __setPauseResumeTraceEnabledForTestFromRun;
export const __markStaleFromNodeForTest = __markStaleFromNodeForTestFromRun;
export const __validatePinEligibilityForTest = __validatePinEligibilityForTestFromRun;

// isFailedBindingStatus, resolveUpstreamArtifact, resolveNodeInputsFromState,
// buildPersistableGraphStrict, computeSaveConsistencyMismatch, buildSavePreflightDiagnostics,
// summarizeSavePreflightError, toolBuiltinPreflightDiagnostics
// — all moved to graphStore.persistence.ts

// mintGraphId, buildHardResetState, captureComponentEditSnapshot, stripToDTO,
// __hardResetGraphForTest, __stripToDTOForTest, edgeStructuralSignature,
// shouldPreserveStoreEdgesOnCanvasSync, normalizeComponentPayloadTypeOrDefault,
// normalizeComponentNodeForMigration, normalizeGraphForComponentMigration
// — all moved to graphStore.graph-edit.ts

// buildPersistableGraphStrict — moved to graphStore.persistence.ts


// computeSaveConsistencyMismatch, toolBuiltinPreflightDiagnostics, buildSavePreflightDiagnostics,
// summarizeSavePreflightError — all moved to graphStore.persistence.ts



// setEdgeExec, downstreamIds, pruneAndRecontractEdgesStrict,
// canonicalizeComponentEdgeSourceHandles, recomputeEdgeContractsBestEffort, topoFrom
// — all moved to graphStore.graph-edit.ts

const loaded = loadGraphFromLocalStorage(emptyGraph);
// loadNodeDocExplanationMode, persistNodeDocExplanationMode, loadNodeDocTrainingMode,
// persistNodeDocTrainingMode — all moved to graphStore.persistence.ts


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

	// ── history ──────────────────────────────────────────────────────────
	// graphEdit is declared before history so the closure captures it by reference.
	// It will be assigned after createGraphEditManager() is called below.
	let graphEdit: ReturnType<typeof createGraphEditManager>;
	const history = createHistoryManager({
		getState: () => get({ subscribe } as any) as GraphState,
		applyDocument: (graph, graphId) => {
			// graphEdit is guaranteed to be assigned before undo/redo can be triggered
			return graphEdit.actions.applyGraphDocument(graph, graphId).ok;
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


	// ── graph-edit manager ──────────────────────────────────────────────────────
	graphEdit = createGraphEditManager({
		update,
		set,
		getState: () => get({ subscribe } as any) as GraphState,
		history,
		persist,
		applyLocalStaleInvalidation,
		updateNodeConfig: updateNodeConfigImpl,
	});


	// ── persistence manager ──────────────────────────────────────────────────
	const persistence = createPersistenceManager({
		update,
		getState: () => get({ subscribe } as any) as GraphState,
		persist,
		applyGraphDocument: graphEdit.actions.applyGraphDocument,
		updateNodeConfig: updateNodeConfigImpl,
	});

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
		...runManager.actions,
		...graphEdit.actions,
		...persistence.actions,
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

