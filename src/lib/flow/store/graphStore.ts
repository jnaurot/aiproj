// src/lib/flow/store/graphStore.ts
import { writable, get, derived } from 'svelte/store';
import type { Node, Edge } from '@xyflow/svelte';

import type {
	PipelineNodeData,
	PipelineEdgeData,
} from '$lib/flow/types';
import {
	createInspectorManager,
	effectiveExecParamsForNode,
} from './graphStore.inspector';
import {
	computeEdgeSchemaConstraintsInternal,
	computeEdgeSchemaDiagnosticsInternal,
} from './graphStore.node-schema';
export {
	__buildNodeSchemaContractSnapshotForTest,
	deriveNodeIoForData,
	__computeEdgeSchemaConstraintsForTest,
	__computeEdgeSchemaDiagnosticsForTest,
	__normalizeSchemaFieldsForTest,
} from './graphStore.node-schema';
import {
	saveGraphToLocalStorage,
	loadGraphFromLocalStorage,
	emptyGraph,
	saveComponentDraftCache,
	loadComponentDraftCache
} from './persist';
import {
	acceptNodeParams,
	getRun,
	resolveSourceNode,
} from '$lib/flow/client/runs';
import {
	resolveNodeDocForState,
	type NodeDocResolved
} from '$lib/flow/components/nodeDocsViewModel';
import {
	NodeDocExplanationModeSchema,
	NodeDocTrainingModeSchema,
	type NodeDocExplanationMode,
	type NodeDocTrainingMode
} from '$lib/flow/schema/nodeDocs';
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
	EdgeDiagnosticSnapshot,
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
	EdgeDiagnosticSnapshot,
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
import { createHistoryManager } from './graphStore.history';
import {
	mintGraphId,
	stripToDTO,
	normalizeGraphForComponentMigration,
	canonicalizeComponentEdgeSourceHandles,
	recomputeEdgeContractsBestEffort,
	pruneAndRecontractEdgesStrict,
	createGraphEditManager,
} from './graphStore.graph-edit';
export { __stripToDTOForTest, __hardResetGraphForTest } from './graphStore.graph-edit';
import {
	auditStateTransition,
	withGraphMeta,
	logPush,
	stableJson,
	ensureNormalizedBindingsForNodes,
	_normalizeBinding,
	_withPair,
	_assertBindingPairInvariant,
	__assertBindingPairForTest as __assertBindingPairForTestFromAudit,
	__normalizeBindingForTest as __normalizeBindingForTestFromAudit,
	__normalizeBindingForLegacyMigrationForTest as __normalizeBindingForLegacyMigrationForTestFromAudit,
	__normalizeBindingStrictForTest as __normalizeBindingStrictForTestFromAudit,
} from './graphStore.audit';
import {
	isNodeStateFromActiveRunAndFresh,
	hydrateFromRunSnapshotState,
	clearNodeCacheUi,
	clearNodeCacheUiForNodes,
	downstreamNodeIds,
	createRunManager,
	__setPauseResumeTraceEnabledForTest as __setPauseResumeTraceEnabledForTestFromRun,
	getPauseResumeTraceEnabled,
	__applyRunEventForTest,
	__hydrateFromRunSnapshotForTest,
	__resetRunUiStateForTest,
	__markStaleFromNodeForTest as __markStaleFromNodeForTestFromRun,
	__buildPlannerScopeTraceForTest as __buildPlannerScopeTraceForTestFromRun,
} from './graphStore.run';
import {
	createPersistenceManager,
	loadNodeDocExplanationMode,
	loadNodeDocTrainingMode,
} from './graphStore.persistence';
import { createSchemaPlaneManager, emptySchemaPlaneState, recomputeSchemaPlane } from './graphStore.schemaPlane';
import { registerAllBuiltinSchemaFunctions } from '$lib/flow/schema/schemaRegistry';
export { __computeSaveConsistencyMismatchForTest } from './graphStore.persistence';
export { resolveNodeInputsFromState } from './graphStore.persistence';

// re-export test hooks that moved to graphStore.audit
export const __assertBindingPairForTest = __assertBindingPairForTestFromAudit;
export const __normalizeBindingForTest = __normalizeBindingForTestFromAudit;
export const __normalizeBindingForLegacyMigrationForTest = __normalizeBindingForLegacyMigrationForTestFromAudit;
export const __normalizeBindingStrictForTest = __normalizeBindingStrictForTestFromAudit;

// re-export test hooks that moved to graphStore.run
export { __applyRunEventForTest, __hydrateFromRunSnapshotForTest, __resetRunUiStateForTest } from './graphStore.run';
export const __setPauseResumeTraceEnabledForTest = __setPauseResumeTraceEnabledForTestFromRun;
export const __markStaleFromNodeForTest = __markStaleFromNodeForTestFromRun;
export const __buildPlannerScopeTraceForTest = __buildPlannerScopeTraceForTestFromRun;

registerAllBuiltinSchemaFunctions();
function normalizeSchemaOpaqueUpstreamPolicy(raw: unknown): 'warn' | 'none' {
	const value = String(raw ?? '')
		.trim()
		.toLowerCase();
	if (value === 'none' || value === 'off' || value === 'ignore') return 'none';
	return 'warn';
}

const defaultSchemaOpaqueUpstreamPolicy = normalizeSchemaOpaqueUpstreamPolicy(
	import.meta.env.VITE_SCHEMA_OPAQUE_UPSTREAM_POLICY ?? 'warn'
);

const loaded = loadGraphFromLocalStorage(emptyGraph);

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
	runBlockedReason: null,
	viewMode: 'execution',
	schemaOpaqueUpstreamPolicy: defaultSchemaOpaqueUpstreamPolicy,
	schemaWarningDismissCount: 0,
	nodeOutputs: {},
	nodeBindings: ensureNormalizedBindingsForNodes(loadedNormalized.nodes, {}),
	activeRunId: null,
	editingContext: 'graph',
	componentEditSession: null,
	componentContractDraftCache: loadComponentDraftCache() as Record<string, any>,
	checkpointRegistry:
		(loaded as any)?.checkpointRegistry && typeof (loaded as any).checkpointRegistry === 'object'
			? structuredClone((loaded as any).checkpointRegistry)
			: {},
	schemaPlane: emptySchemaPlaneState(),
	schemaEdgeInspectorEdgeId: null
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
		snapshotFromState: (s) => stripToDTO(s.nodes as any, s.edges as any, s.graphId, s.checkpointRegistry ?? {}),
	});

	// ── audited update ───────────────────────────────────────────────────
	const updateWithAudit = history.wrapUpdate(
		rawUpdate,
		auditStateTransition,
		(s) => stripToDTO(s.nodes as any, s.edges as any, s.graphId, s.checkpointRegistry ?? {}),
	);
	const update = (
		fn: (s: GraphState) => GraphState,
		ctx?: AuditContext
	) => updateWithAudit((s) => withGraphMeta(fn(s)), ctx);
function applyLocalStaleInvalidation(nodeId: string, rootReason: string = 'PARAMS_CHANGED'): void {
		update((cur) => {
			const checkpointBoundaryNodeIds = new Set<string>(Object.keys(cur.checkpointRegistry ?? {}));
			const candidateIds = downstreamNodeIds(cur.edges, nodeId, checkpointBoundaryNodeIds);
			const nodeBindings = { ...cur.nodeBindings };
			let nodeOutputs = { ...cur.nodeOutputs };
			let changed = false;
			for (const affectedId of candidateIds) {
				if (affectedId !== nodeId && checkpointBoundaryNodeIds.has(affectedId)) continue;
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
			const checkpointBoundaryNodeIds = new Set<string>(Object.keys(cur.checkpointRegistry ?? {}));
			const nodeBindings = { ...cur.nodeBindings };
			const touchedIds: string[] = [];
			for (const affectedId of affectedNodeIds) {
				if (affectedId !== rootNodeId && checkpointBoundaryNodeIds.has(affectedId)) continue;
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
		if (state.editingContext !== 'component') {
			saveGraphToLocalStorage(
				stripToDTO(state.nodes, state.edges, state.graphId, state.checkpointRegistry ?? {})
			);
		}
		saveComponentDraftCache((state.componentContractDraftCache ?? {}) as Record<string, unknown>);
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
	const schemaPlane = createSchemaPlaneManager({
		getState: () => get({ subscribe } as any) as GraphState
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
		getNodeSchemaResult: schemaPlane.getNodeSchemaResult,
		getEdgeSchemaResult: schemaPlane.getEdgeSchema,
		getEdgeSchemaValidationState: schemaPlane.getEdgeValidationState,
		getEdgeDiagnosticSnapshot(edgeId: string) {
			return getEdgeDiagnosticSnapshotFromState(get({ subscribe } as any) as GraphState, edgeId);
		},
		getSchemaErrors: schemaPlane.getSchemaErrors,
		hasSchemaErrors: schemaPlane.hasSchemaErrors,
		getSchemaConfigurationHints: schemaPlane.getConfigurationHints,
		setViewMode(mode: 'execution' | 'schema') {
			update((s) => withGraphMeta({ ...s, viewMode: mode }));
		},
		toggleSchemaView() {
			update((s) =>
				withGraphMeta({
					...s,
					viewMode: s.viewMode === 'schema' ? 'execution' : 'schema'
				})
			);
		},
		setSchemaOpaqueUpstreamPolicy(policyRaw: 'warn' | 'none' | string) {
			const normalized = normalizeSchemaOpaqueUpstreamPolicy(policyRaw);
			update((s) => {
				if ((s as any).schemaOpaqueUpstreamPolicy === normalized) return s;
				return withGraphMeta({
					...s,
					schemaOpaqueUpstreamPolicy: normalized
				});
			});
		},

		// ── Schema edge inspector ─────────────────────────────────────────────
		setSchemaEdgeInspectorEdgeId(edgeId: string | null) {
			update((s) => ({ ...s, schemaEdgeInspectorEdgeId: edgeId ?? null }));
		},

		/**
		 * Update the schema on a node handle and trigger schema re-validation.
		 * Wrapped in a history transaction so the mutation + schemaPlane recompute
		 * produce exactly one undo entry.
		 *
		 * direction='input'  → updates node.data.schema.expectedInputSchemas[handleId]
		 * direction='output' → updates node.data.schema.expectedSchema (output schema)
		 */
		updateNodeSchema(
			nodeId: string,
			handleId: string,
			direction: 'input' | 'output',
			schema: Record<string, unknown> | null
		): { ok: boolean; error?: string } {
			history.actions.beginHistoryTransaction();
			let result: { ok: boolean; error?: string } = { ok: true };
			try {
				if (direction === 'input') {
					result = inspector.actions.setNodeExpectedInputSchemaForHandle(nodeId, handleId, schema);
				} else {
					result = inspector.actions.setNodeExpectedSchema(nodeId, schema);
				}
				// Trigger schema plane re-computation so live validation updates
				update((s) => ({
					...s,
					schemaPlane: recomputeSchemaPlane(s)
				}));
			} finally {
				history.actions.endHistoryTransaction();
			}
			return result;
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

export function getEdgeDiagnosticSnapshotFromState(
	state: GraphState,
	edgeIdRaw: string
): EdgeDiagnosticSnapshot | null {
	const edgeId = String(edgeIdRaw ?? '').trim();
	if (!edgeId) return null;
	const edge = (state.edges ?? []).find((candidate) => String(candidate?.id ?? '') === edgeId);
	if (!edge) return null;
	const constraints = computeEdgeSchemaConstraintsInternal(state.nodes as any, state.edges as any);
	const diagnostics = computeEdgeSchemaDiagnosticsInternal(constraints as any);
	const diag = diagnostics[edgeId];
	const contractSeverity: 'clean' | 'warning' | 'error' =
		diag?.severity === 'error' ? 'error' : diag?.severity === 'warning' ? 'warning' : 'clean';
	const hasSchemaPlane =
		Boolean(state.schemaPlane) &&
		typeof state.schemaPlane === 'object' &&
		state.schemaPlane.nodeSchemas &&
		state.schemaPlane.edgeSchemas;
	const stateWithSchema = hasSchemaPlane
		? state
		: ({
				...state,
				schemaPlane: recomputeSchemaPlane(state)
			} as GraphState);
	const schemaValidation = createSchemaPlaneManager({ getState: () => stateWithSchema }).getEdgeValidationState(edgeId);
	const schemaPlaneState =
		schemaValidation?.state === 'error'
			? 'error'
			: schemaValidation?.state === 'warning'
				? 'warning'
				: schemaValidation?.state === 'valid'
					? 'valid'
					: 'neutral';
	const schemaPlaneCode = String(schemaValidation?.code ?? '').trim().toUpperCase();
	const edgeExec = String((edge.data as any)?.exec ?? 'idle').trim().toLowerCase();
	const targetNodeId = String(edge.target ?? '').trim();
	const blockedByNode =
		(state.queueRuntime?.blockedByNode && typeof state.queueRuntime.blockedByNode === 'object'
			? state.queueRuntime.blockedByNode
			: {}) ?? {};
	const blockedEntry = targetNodeId ? (blockedByNode as any)[targetNodeId] : null;
	let runtimeState: EdgeDiagnosticSnapshot['runtimeState'] = 'inactive';
	if (edgeExec === 'active') runtimeState = 'running';
	else if (edgeExec === 'done') runtimeState = 'settled';
	else if (blockedEntry && Array.isArray(blockedEntry.missingEdgeIds) && blockedEntry.missingEdgeIds.includes(edgeId)) {
		runtimeState = 'blocked';
	} else if (blockedEntry && String(blockedEntry.reasonCode ?? '').trim() === 'NO_READY_WORK') {
		runtimeState = 'waiting';
	}
	// Merge contract severity and schema-plane state into a single effectiveSeverity.
	// Rules:
	//   • contract 'error'/'warning' always takes precedence (it's the declared contract check).
	//   • schemaPlane 'error' (hard propagation failure, e.g. SHAPE_MISMATCH / column not found)
	//     also surfaces as an error — the edge leads into a broken node.
	//   • schemaPlane 'warning' (opaque / unverifiable) is informational only and does NOT
	//     escalate effectiveSeverity; the existing edgeSchemaAuthority tests codify this.
	const effectiveSeverity: 'clean' | 'warning' | 'error' =
		contractSeverity === 'error' || schemaPlaneState === 'error'
			? 'error'
			: contractSeverity === 'warning' ||
				  (schemaPlaneState === 'warning' && schemaPlaneCode === 'SHAPE_MISMATCH_OPAQUE')
				? 'warning'
				: 'clean';
	return {
		edgeId,
		contractSeverity,
		schemaPlaneState,
		runtimeState,
		effectiveSeverity,
		contractMessage: diag?.message ? String(diag.message) : undefined,
		schemaPlaneMessage: schemaValidation?.message ? String(schemaValidation.message) : undefined
	};
}


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
