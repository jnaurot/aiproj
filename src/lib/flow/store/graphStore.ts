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
const allowedPorts = new Set(['table', 'text', 'json', 'binary', 'embeddings', 'image', 'audio', 'video']);
const allowedBuiltinProfileIds = new Set<string>(TOOL_BUILTIN_PROFILE_IDS);

// re-export test hooks that moved to graphStore.audit
export const __assertBindingPairForTest = __assertBindingPairForTestFromAudit;
export const __normalizeBindingForTest = __normalizeBindingForTestFromAudit;

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


function applyLlmHolderToNodes(
	nodes: Node<PipelineNodeData>[],
	holderNodeId: string | null | Iterable<string>
): Node<PipelineNodeData>[] {
	const active = new Set<string>();
	if (typeof holderNodeId === 'string') {
		const holder = String(holderNodeId ?? '').trim();
		if (holder) active.add(holder);
	} else if (holderNodeId && Symbol.iterator in Object(holderNodeId)) {
		for (const candidate of holderNodeId as Iterable<string>) {
			const holder = String(candidate ?? '').trim();
			if (holder) active.add(holder);
		}
	}
	let changed = false;
	const nextNodes = nodes.map((node) => {
		const nodeId = String(node.id ?? '');
		const meta = { ...((node.data as any)?.meta ?? {}) };
		const currentlyAllocated = Boolean((meta as any).llmAllocated);
		const shouldAllocate = active.has(nodeId);
		if (currentlyAllocated === shouldAllocate) return node;
		changed = true;
		if (shouldAllocate) {
			(meta as any).llmAllocated = true;
		} else {
			delete (meta as any).llmAllocated;
		}
		return {
			...node,
			data: {
				...node.data,
				meta
			}
		};
	});
	return changed ? nextNodes : nodes;
}

function reconcileModelLeaseRunningInvariant(state: GraphState): GraphState {
	const runState = String(state.runStatus ?? '').trim().toLowerCase();
	const runActive = runState === 'running' || runState === 'pausing' || runState === 'resuming';
	if (!runActive) return state;

	const leaseActive = Array.isArray((state.queueRuntime?.llmLease as any)?.activeNodeIds)
		? (((state.queueRuntime?.llmLease as any)?.activeNodeIds as unknown[]) ?? [])
				.map((item) => String(item ?? '').trim())
				.filter(Boolean)
		: [];
	const leaseActiveSet = new Set<string>(leaseActive);

	const nextNodes = applyLlmHolderToNodes(state.nodes, leaseActiveSet);
	let nodesChanged = nextNodes !== state.nodes;

	let nextBindings = state.nodeBindings ?? {};
	let bindingsChanged = false;

	for (const node of nextNodes) {
		const nodeId = String((node as any)?.id ?? '').trim();
		if (!nodeId) continue;
		const kind = String(((node as any)?.data?.kind ?? '')).trim().toLowerCase();
		if (kind !== 'model' && kind !== 'llm') continue;
		const hasLease = leaseActiveSet.has(nodeId);
		const prevBinding = _normalizeBinding((nextBindings as any)?.[nodeId], nodeId);

		if (hasLease && prevBinding.status !== 'running') {
			nextBindings = {
				...nextBindings,
				[nodeId]: {
					...prevBinding,
					status: 'running',
					currentRunId: prevBinding.currentRunId ?? state.activeRunId
				}
			};
			bindingsChanged = true;
			continue;
		}

		if (!hasLease && prevBinding.status === 'running') {
			nextBindings = {
				...nextBindings,
				[nodeId]: {
					...prevBinding,
					status: 'busy',
					currentRunId: prevBinding.currentRunId ?? state.activeRunId
				}
			};
			bindingsChanged = true;
		}
	}

	if (!nodesChanged && !bindingsChanged) return state;
	return {
		...state,
		nodes: nextNodes,
		nodeBindings: nextBindings
	};
}

function applyRunEventState(state: GraphState, evt: KnownRunEvent, runId: string): GraphState {
	const reduced = reduceRunEventState(state, evt, runId);
	const reconciled = reconcileModelLeaseRunningInvariant(reduced);
	if (!shouldTracePauseResumeEvent(state, reconciled, evt)) return reconciled;
	const details = buildPauseResumeTraceDetails(evt);
	return logPush(
		reconciled,
		'info',
		`[trace][pause-resume] evt=${evt.type} runStatus=${String(state.runStatus ?? 'idle')}->${String(reconciled.runStatus ?? 'idle')}${details ? ` ${details}` : ''}`,
		(evt as any)?.nodeId,
		(evt as any)?.componentPath,
		(evt as any)?.edgeId
	);
}

let pauseResumeTraceEnabled = false;

const PAUSE_RESUME_RUN_STATUS_TRACE = new Set<RunStatus>(['pausing', 'paused', 'resuming']);
const PAUSE_RESUME_FORCE_TRACE_EVENTS = new Set<string>([
	'run_pause_requested',
	'run_pausing',
	'run_paused',
	'run_resume_requested',
	'run_resuming',
	'run_resumed',
	'run_resume_failed'
]);

function shouldTracePauseResumeEvent(
	prev: GraphState,
	next: GraphState,
	evt: KnownRunEvent
): boolean {
	if (!pauseResumeTraceEnabled) return false;
	if (PAUSE_RESUME_FORCE_TRACE_EVENTS.has(String(evt.type ?? ''))) return true;
	if (PAUSE_RESUME_RUN_STATUS_TRACE.has(prev.runStatus)) return true;
	if (PAUSE_RESUME_RUN_STATUS_TRACE.has(next.runStatus)) return true;
	return false;
}

export function __setPauseResumeTraceEnabledForTest(enabled: boolean): void {
	pauseResumeTraceEnabled = Boolean(enabled);
}

function buildPauseResumeTraceDetails(evt: KnownRunEvent): string {
	const e = evt as any;
	const summary: Record<string, unknown> = {
		runId: typeof e.runId === 'string' ? e.runId : undefined,
		nodeId: typeof e.nodeId === 'string' ? e.nodeId : undefined,
		edgeId: typeof e.edgeId === 'string' ? e.edgeId : undefined,
		status: typeof e.status === 'string' ? e.status : undefined,
		signal: typeof e.signal === 'string' ? e.signal : undefined,
		decision: typeof e.decision === 'string' ? e.decision : undefined,
		exec: typeof e.exec === 'string' ? e.exec : undefined,
		handle: typeof e.handle === 'string' ? e.handle : undefined,
		at: typeof e.at === 'string' ? e.at : undefined,
		plannedNodeCount: Array.isArray(e.plannedNodeIds) ? e.plannedNodeIds.length : undefined
	};
	if (e.snapshot && typeof e.snapshot === 'object') {
		const nodes = (e.snapshot as any)?.frontierValidationBasis?.nodes;
		if (nodes && typeof nodes === 'object') {
			summary.snapshotFrontierNodeCount = Object.keys(nodes).length;
		}
	}
	const compact = Object.fromEntries(
		Object.entries(summary).filter(([, value]) => value !== undefined && value !== null)
	);
	if (Object.keys(compact).length === 0) return '';
	return stableJson(compact);
}

function clearActiveWorkIncomingEdgesForNode(
	edges: Edge<PipelineEdgeData & Record<string, unknown>>[],
	nodeId: string
): Edge<PipelineEdgeData & Record<string, unknown>>[] {
	let changed = false;
	const targetId = String(nodeId ?? '').trim();
	if (!targetId) return edges;
	const nextEdges = edges.map((edge) => {
		if (String(edge?.target ?? '') !== targetId) return edge;
		const mode = String((edge?.data as any)?.mode ?? 'work').trim().toLowerCase();
		if (mode !== 'work') return edge;
		const exec = String((edge?.data as any)?.exec ?? 'idle').trim().toLowerCase();
		if (exec !== 'active') return edge;
		changed = true;
		return {
			...edge,
			data: {
				...(edge.data ?? {}),
				exec: 'done'
			}
		};
	});
	return changed ? nextEdges : edges;
}

function canApplyNodeEvent(state: GraphState, nodeId: string, evtRunId?: string): boolean {
	if (!nodeId) return false;
	if (!state.activeRunId) return true;
	if (evtRunId && evtRunId !== state.activeRunId) return false;
	// Event streams are run-scoped by runId; do not drop valid per-node events based on
	// planned sets because scheduler/runtime may execute additional upstream nodes.
	return true;
}

function applyPauseSnapshotFrontierBindings(
	state: GraphState,
	snapshot: Record<string, any> | null | undefined
): GraphState {
	const basisNodes =
		snapshot &&
		typeof snapshot === 'object' &&
		typeof (snapshot as any).frontierValidationBasis === 'object' &&
		(snapshot as any).frontierValidationBasis &&
		typeof (snapshot as any).frontierValidationBasis.nodes === 'object'
			? ((snapshot as any).frontierValidationBasis.nodes as Record<string, any>)
			: {};
	const nodeBindings = { ...(state.nodeBindings ?? {}) } as Record<string, NormalizedNodeBinding>;
	let mutated = false;
	const applyPair = (nodeId: string, pair: Record<string, any> | null | undefined) => {
		const execKey = String(pair?.currentExecKey ?? '').trim();
		const artifactId = String(pair?.currentArtifactId ?? '').trim();
		if (!execKey || !artifactId) return;
		const prev = _normalizeBinding(nodeBindings[nodeId], nodeId);
		let next: NormalizedNodeBinding = {
			...prev,
			status:
				prev.status === 'failed' || prev.status === 'canceled'
					? prev.status
					: 'succeeded_up_to_date',
			cacheValid: true,
			isUpToDate: true,
			staleReason: null
		};
		next = _withPair(next, 'current', { execKey, artifactId });
		next = _withPair(next, 'last', { execKey, artifactId });
		if (
			prev.currentExecKey !== next.currentExecKey ||
			prev.currentArtifactId !== next.currentArtifactId ||
			prev.lastExecKey !== next.lastExecKey ||
			prev.lastArtifactId !== next.lastArtifactId ||
			prev.cacheValid !== next.cacheValid ||
			prev.isUpToDate !== next.isUpToDate ||
			prev.staleReason !== next.staleReason
		) {
			nodeBindings[nodeId] = next;
			mutated = true;
		}
	};
	for (const [nodeId, rawNode] of Object.entries(basisNodes)) {
		if (!nodeId || !rawNode || typeof rawNode !== 'object') continue;
		applyPair(nodeId, (rawNode as any).binding);
		const upstream =
			(rawNode as any).upstreamBindings && typeof (rawNode as any).upstreamBindings === 'object'
				? ((rawNode as any).upstreamBindings as Record<string, any>)
				: {};
		for (const [upstreamNodeId, upstreamPair] of Object.entries(upstream)) {
			applyPair(upstreamNodeId, upstreamPair as Record<string, any>);
		}
	}
	if (!mutated) return state;
	return { ...state, nodeBindings };
}

function isNodeStateFromActiveRunAndFresh(cur: GraphState, binding: NormalizedNodeBinding): boolean {
	// Guard only during an active in-flight run; completed runs must not block invalidation.
	if (cur.runStatus !== 'running') return false;
	if (!cur.activeRunId) return false;
	if (binding.currentRunId !== cur.activeRunId) return false;
	const status = String(binding.status ?? '').toLowerCase();
	return (
		status === 'running' ||
		status.startsWith('succeeded') ||
		binding.isUpToDate === true
	);
}

function changedBindingNodeIds(
	prev: Record<string, NormalizedNodeBinding>,
	next: Record<string, NormalizedNodeBinding>
): string[] {
	const ids = new Set([...Object.keys(prev ?? {}), ...Object.keys(next ?? {})]);
	const changed: string[] = [];
	for (const id of ids) {
		const a = prev?.[id] ?? null;
		const b = next?.[id] ?? null;
		if (a === b) continue;
		if (!a || !b) {
			changed.push(id);
			continue;
		}
		const keys = new Set([...Object.keys(a), ...Object.keys(b)]);
		let mutated = false;
		for (const k of keys) {
			if ((a as any)[k] !== (b as any)[k]) {
				mutated = true;
				break;
			}
		}
		if (mutated) changed.push(id);
	}
	return changed;
}

function debugLogOutOfScopeBindingMutation(prev: GraphState, next: GraphState, context: string): void {
	if (!DEV_MODE) return;
	if (!next.activeRunId || !next.activeRunNodeSet || next.activeRunNodeSet.size === 0) return;
	const changed = changedBindingNodeIds(prev.nodeBindings ?? {}, next.nodeBindings ?? {});
	const outside = changed.filter((id) => !next.activeRunNodeSet.has(id));
	if (outside.length === 0) return;
	console.warn('[graphStore] out-of-scope nodeBindings mutation', {
		context,
		outsideNodeIds: outside,
		activeRunId: next.activeRunId,
		activeRunNodeSet: Array.from(next.activeRunNodeSet)
	});
}

function debugLogStaleFlips(prev: GraphState, next: GraphState, context: string): void {
	if (!DEV_MODE) return;
	const flips = getStaleFlipNodeIds(prev.nodeBindings ?? {}, next.nodeBindings ?? {});
	if (flips.length === 0) return;
	console.warn('[graphStore] stale flip detected', {
		context,
		nodeIds: flips,
		activeRunId: next.activeRunId,
		activeRunNodeSet: next.activeRunNodeSet ? Array.from(next.activeRunNodeSet) : []
	});
}


function downstreamNodeIds(
	edges: Edge<PipelineEdgeData>[],
	nodeId: string,
	pinnedNodeIds: Set<string> = new Set<string>()
): Set<string> {
	const out = new Set<string>([nodeId]);
	const q = [nodeId];
	while (q.length > 0) {
		const cur = q.shift()!;
		for (const e of edges) {
			if (e.source !== cur) continue;
			const nxt = String(e.target ?? '');
			if (!nxt || out.has(nxt)) continue;
			// Pinned nodes are execution/staleness boundaries for upstream changes.
			if (nxt !== nodeId && pinnedNodeIds.has(nxt)) continue;
			out.add(nxt);
			q.push(nxt);
		}
	}
	return out;
}

export function __markStaleFromNodeForTest(state: GraphState, nodeId: string): GraphState {
	const pinnedNodeIds = new Set<string>(collectPinnedNodeIds(state.nodes as any));
	const candidateIds = downstreamNodeIds(state.edges, nodeId, pinnedNodeIds);
	const nodeBindings = { ...state.nodeBindings };
	let changed = false;
	for (const affectedId of candidateIds) {
		if (affectedId !== nodeId && pinnedNodeIds.has(affectedId)) continue;
		const prev = _normalizeBinding(nodeBindings[affectedId], affectedId);
		const hadArtifact = Boolean(prev.current?.artifactId || prev.last?.artifactId);
		if (!hadArtifact) continue;
		if (isNodeStateFromActiveRunAndFresh(state, prev)) continue;
		let next = {
			...prev,
			status: 'stale',
			isUpToDate: false,
			cacheValid: false,
			currentRunId: null,
			staleReason: affectedId === nodeId ? 'PARAMS_CHANGED' : 'UPSTREAM_CHANGED'
		};
		next = _withPair(next, 'current', { execKey: null, artifactId: null });
		nodeBindings[affectedId] = next;
		changed = true;
	}
	if (!changed) return state;
	return withGraphMeta({ ...state, nodeBindings });
}

function clearNodeCacheUi(
	nodeOutputs: Record<string, NodeOutputInfo>,
	nodeId: string
): Record<string, NodeOutputInfo> {
	const prev = nodeOutputs?.[nodeId];
	if (!prev) return nodeOutputs;
	return {
		...nodeOutputs,
		[nodeId]: {
			...prev,
			cached: false,
			cacheDecision: undefined,
			expectedContractFingerprint: undefined,
			actualContractFingerprint: undefined,
			mismatchKind: undefined
		}
	};
}

function clearNodeCacheUiForNodes(
	nodeOutputs: Record<string, NodeOutputInfo>,
	nodeIds: Iterable<string>
): Record<string, NodeOutputInfo> {
	let next = nodeOutputs;
	for (const nodeId of nodeIds) {
		next = clearNodeCacheUi(next, nodeId);
	}
	return next;
}

function hasCurrentBoundArtifact(binding: NormalizedNodeBinding | undefined | null): boolean {
	const execKey = String(binding?.current?.execKey ?? '').trim();
	const artifactId = String(binding?.current?.artifactId ?? '').trim();
	return execKey.length > 0 && artifactId.length > 0;
}

function validatePinEligibility(
	node: Node<PipelineNodeData & Record<string, unknown>> | undefined | null,
	binding: NormalizedNodeBinding | undefined | null
): { ok: true } | { ok: false; error: string } {
	if (!node) return { ok: false, error: 'Node not found.' };
	const display = displayStatusFromBinding(binding as any);
	if (display !== 'succeeded') {
		return { ok: false, error: 'Pin is only allowed when node status is succeeded.' };
	}
	if (!hasCurrentBoundArtifact(binding)) {
		return {
			ok: false,
			error: 'Pin requires a current bound artifact. Run the node successfully first.'
		};
	}
	return { ok: true };
}

export function __validatePinEligibilityForTest(
	node: Node<PipelineNodeData & Record<string, unknown>> | undefined | null,
	binding: NodeBindingInfo | NormalizedNodeBinding | undefined | null
): { ok: true } | { ok: false; error: string } {
	const normalized =
		binding == null ? undefined : _normalizeBinding(binding as NodeBindingInfo, String(node?.id ?? 'test'));
	return validatePinEligibility(node, normalized);
}

function collectPinnedNodeIds(nodes: Node<PipelineNodeData & Record<string, unknown>>[]): string[] {
	return nodes
		.filter((node) => nodeFreezeMode(node) !== null)
		.map((node) => String(node.id ?? '').trim())
		.filter(Boolean);
}

function collectPinnedArtifactsByNode(
	nodes: Node<PipelineNodeData & Record<string, unknown>>[],
	nodeBindings: Record<string, NodeBindingInfo | NormalizedNodeBinding | undefined>
): Record<string, { artifactId: string; execKey?: string; outputs?: Record<string, { artifactId: string; execKey?: string }> }> {
	const out: Record<
		string,
		{ artifactId: string; execKey?: string; outputs?: Record<string, { artifactId: string; execKey?: string }> }
	> = {};
	for (const node of nodes) {
		if (nodeFreezeMode(node) === null) continue;
		const nodeId = String(node.id ?? '').trim();
		if (!nodeId) continue;
		const binding = _normalizeBinding(nodeBindings?.[nodeId], nodeId);
		const lineage = binding.last?.artifactId || binding.last?.execKey ? binding.last : binding.current;
		const artifactId = String(lineage?.artifactId ?? '').trim();
		const execKey = String(lineage?.execKey ?? '').trim();
		if (!artifactId || !execKey) continue;
		const pinned: { artifactId: string; execKey?: string; outputs?: Record<string, { artifactId: string; execKey?: string }> } = {
			artifactId,
			execKey
		};
		if (node.data.kind === 'component') {
			const rawOutputLineage =
				((binding as any)?.outputLineage && typeof (binding as any).outputLineage === 'object'
					? ((binding as any).outputLineage as Record<string, any>)
					: null) ?? null;
			if (rawOutputLineage) {
				const outputs: Record<string, { artifactId: string; execKey?: string }> = {};
				for (const [rawHandle, rawPair] of Object.entries(rawOutputLineage)) {
					const handle = String(rawHandle ?? '').trim();
					if (!handle || !rawPair || typeof rawPair !== 'object') continue;
					const outputArtifactId = String((rawPair as any).artifactId ?? '').trim();
					const outputExecKey = String((rawPair as any).execKey ?? '').trim();
					if (!outputArtifactId) continue;
					outputs[handle] = outputExecKey
						? { artifactId: outputArtifactId, execKey: outputExecKey }
						: { artifactId: outputArtifactId };
				}
				if (Object.keys(outputs).length > 0) pinned.outputs = outputs;
			}
		}
		out[nodeId] = pinned;
	}
	return out;
}

function clearPerRunPinsOnNodes(
	nodes: Node<PipelineNodeData & Record<string, unknown>>[]
): Node<PipelineNodeData & Record<string, unknown>>[] {
	let changed = false;
	const next = nodes.map((node) => {
		if (nodeFreezeMode(node) !== 'per_run') return node;
		const meta = { ...(((node.data as any)?.meta ?? {}) as Record<string, unknown>) };
		delete (meta as any).freeze;
		changed = true;
		return {
			...node,
			data: {
				...(node.data as any),
				meta: {
					...meta,
					updatedAt: new Date().toISOString()
				}
			}
		} as Node<PipelineNodeData & Record<string, unknown>>;
	});
	return changed ? next : nodes;
}

function assertNoOutOfScopeStaleFlips(prev: GraphState, next: GraphState, context: string): void {
	if (!DEV_MODE) return;
	if (!next.activeRunId || !next.activeRunNodeSet || next.activeRunNodeSet.size === 0) return;
	const flips = getStaleFlipNodeIds(prev.nodeBindings ?? {}, next.nodeBindings ?? {});
	const outOfScope = flips.filter((id) => !next.activeRunNodeSet.has(id));
	for (const nodeId of outOfScope) {
		console.error('[graphStore] out-of-scope stale flip', {
			context,
			nodeId,
			prevBinding: prev.nodeBindings?.[nodeId] ?? null,
			nextBinding: next.nodeBindings?.[nodeId] ?? null,
			activeRunId: next.activeRunId,
			runMode: next.activeRunMode,
			runFrom: next.activeRunFrom,
			activeRunNodeSet: Array.from(next.activeRunNodeSet)
		});
	}
}

function assertRunStartedBindingTouchInScope(prev: GraphState, next: GraphState): void {
	if (!DEV_MODE) return;
	const changed = changedBindingNodeIds(prev.nodeBindings ?? {}, next.nodeBindings ?? {});
	if (changed.length === 0) return;
	const outOfScope = changed.filter((id) => !next.activeRunNodeSet?.has(id));
	if (outOfScope.length === 0) return;
	console.error('[graphStore] run_started mutated out-of-scope nodeBindings', {
		changedNodeIds: changed,
		outOfScopeNodeIds: outOfScope,
		activeRunId: next.activeRunId,
		runMode: next.activeRunMode,
		runFrom: next.activeRunFrom,
		activeRunNodeSet: next.activeRunNodeSet ? Array.from(next.activeRunNodeSet) : [],
		bindingsBefore: outOfScope.reduce(
			(acc, id) => ({ ...acc, [id]: prev.nodeBindings?.[id] ?? null }),
			{} as Record<string, NormalizedNodeBinding | null>
		),
		bindingsAfter: outOfScope.reduce(
			(acc, id) => ({ ...acc, [id]: next.nodeBindings?.[id] ?? null }),
			{} as Record<string, NormalizedNodeBinding | null>
		)
	});
}

const RUN_MONITOR_HISTORY_LIMIT = 20;

const CONTROL_SIGNAL_ALLOWED = new Set([
	'ready',
	'busy',
	'drain',
	'pause',
	'blocked',
	'resume',
	'llm_acquired',
	'llm_released',
	'upstream_opened',
	'item_enqueued',
	'input_drained',
	'upstream_closed',
	'input_ready',
	'input_blocked',
	'node_active',
	'node_quiescent',
	'node_terminal'
]);

function normalizeControlSignal(evt: Extract<KnownRunEvent, { type: 'control_signal' }>): string | null {
	const v1 = String((evt as any)?.control_signal?.signalType ?? '')
		.trim()
		.toLowerCase();
	const raw = String((evt as any)?.signal ?? '')
		.trim()
		.toLowerCase();
	const normalized = v1 || raw;
	if (!normalized) return null;
	return CONTROL_SIGNAL_ALLOWED.has(normalized) ? normalized : null;
}

function applyControlPlaneEdgeState(
	prevQueueRuntime: NonNullable<GraphState['queueRuntime']>,
	evt: Extract<KnownRunEvent, { type: 'control_signal' }>,
	signal: string
): NonNullable<GraphState['queueRuntime']> {
	const edgeId = String((evt as any)?.edgeId ?? '').trim();
	if (!edgeId) return prevQueueRuntime;
	const priorMap =
		prevQueueRuntime.controlPlaneEdgeState && typeof prevQueueRuntime.controlPlaneEdgeState === 'object'
			? prevQueueRuntime.controlPlaneEdgeState
			: {};
	const prior =
		(priorMap as Record<string, any>)[edgeId] ??
		({
			edgeId,
			open: false,
			closed: false,
			depth: 0,
			blocked: false,
			lastSeq: 0
		} as any);
	const next = {
		edgeId,
		open: Boolean(prior.open),
		closed: Boolean(prior.closed),
		depth: Math.max(0, Number(prior.depth ?? 0)),
		blocked: Boolean(prior.blocked),
		lastSeq: Math.max(0, Number((evt as any)?.seq ?? prior.lastSeq ?? 0)),
		updatedAt: String((evt as any)?.at ?? '')
	};
	const incomingSeq = Math.max(0, Number((evt as any)?.seq ?? 0));
	const priorSeq = Math.max(0, Number(prior.lastSeq ?? 0));
	if (incomingSeq > 0 && priorSeq > 0 && incomingSeq <= priorSeq) {
		return prevQueueRuntime;
	}
	if (signal === 'upstream_opened') {
		next.open = true;
		next.closed = false;
	}
	if (signal === 'item_enqueued') {
		next.open = true;
		next.depth = Math.max(0, next.depth + 1);
	}
	if (signal === 'input_drained') {
		next.depth = 0;
	}
	if (signal === 'upstream_closed') {
		next.open = false;
		next.closed = true;
	}
	if (signal === 'input_blocked') {
		next.blocked = true;
	}
	if (signal === 'input_ready') {
		next.blocked = false;
	}
	return {
		...prevQueueRuntime,
		controlPlaneEdgeState: {
			...(priorMap as Record<string, any>),
			[edgeId]: next
		}
	};
}

function reduceRunEventState(state: GraphState, evt: KnownRunEvent, runId: string): GraphState {
	const evtGraphId = (evt as any)?.graphId;
	if (typeof evtGraphId === 'string' && evtGraphId && evtGraphId !== state.graphId) {
		return state;
	}
	switch (evt.type) {
		case 'node_output': {
			if (!canApplyNodeEvent(state, evt.nodeId, evt.runId)) return state;
			const prevBinding = _normalizeBinding(state.nodeBindings?.[evt.nodeId], evt.nodeId);
			let nextForNode: NormalizedNodeBinding = {
				...prevBinding,
				currentRunId: runId,
				lastRunId: runId,
				isUpToDate: prevBinding.isUpToDate
			};
			const currentPair = _pairFromLegacy(prevBinding, 'current');
			const boundExecKey = currentPair.execKey ?? _pairFromLegacy(prevBinding, 'last').execKey ?? evt.artifactId;
			nextForNode = _withPair(nextForNode, 'current', { execKey: boundExecKey, artifactId: evt.artifactId });
			nextForNode = _withPair(nextForNode, 'last', { execKey: boundExecKey, artifactId: evt.artifactId });
			_assertBindingPairInvariant(nextForNode, evt.nodeId, 'node_output');
			const nodeBindings = {
				...state.nodeBindings,
				[evt.nodeId]: nextForNode
			};
			const prevCacheDecision = state.nodeOutputs?.[evt.nodeId]?.cacheDecision;
			const nextCacheDecision =
				evt.cached === true
					? (prevCacheDecision ?? 'cache_hit')
					: 'cache_miss';
			const nodeOutputs = {
				...state.nodeOutputs,
				[evt.nodeId]: {
					mimeType: evt.mimeType,
					payloadType: evt.payloadType,
					preview: evt.preview ?? undefined,
					sourceObservability:
						evt.sourceObservability && typeof evt.sourceObservability === 'object'
							? (evt.sourceObservability as Record<string, unknown>)
							: undefined,
					primingArtifact:
						(evt as any).primingArtifact && typeof (evt as any).primingArtifact === 'object'
							? ((evt as any).primingArtifact as Record<string, unknown>)
							: undefined,
					cached: evt.cached ?? false,
					cacheDecision: nextCacheDecision
				}
			};
			let driftLogMessage: string | null = null;
			const nodes = state.nodes.map((node) => {
				if (String(node.id ?? '') !== evt.nodeId) return node;
				const observedSchema = deriveObservedSchemaObservationFromNodeOutput(
					evt as Extract<KnownRunEvent, { type: 'node_output' }>,
					node as Node<PipelineNodeData>
				);
				if (!observedSchema) return node;
				const existingSchema =
					node.data?.schema && typeof node.data.schema === 'object'
						? (node.data.schema as Record<string, unknown>)
						: {};
				const parsedSchema = NodeSchemaEnvelopeSchema.safeParse({
					...existingSchema,
					observedSchema
				});
				if (!parsedSchema.success) return node;
				const drift = computeSchemaDriftSummary(
					(parsedSchema.data as any)?.expectedSchema?.typedSchema,
					(parsedSchema.data as any)?.observedSchema?.typedSchema
				);
				if (drift.hasDrift) {
					const details: string[] = [];
					if (drift.typeMismatch) details.push('type mismatch');
					if (drift.missingColumns.length > 0) {
						details.push(`missing columns: ${drift.missingColumns.join(', ')}`);
					}
					if (drift.mismatchedColumns.length > 0) {
						details.push(`column type drift: ${drift.mismatchedColumns.join(', ')}`);
					}
					driftLogMessage = `[schema-drift] expected vs observed drift detected (${details.join('; ') || 'unknown'})`;
				}
				return {
					...node,
					data: {
						...node.data,
						schema: parsedSchema.data
					}
				};
			});
			const baseNext = withGraphMeta({
				...state,
				nodes,
				nodeOutputs,
				nodeBindings
			});
			if (driftLogMessage) {
				return logPush(baseNext, 'warn', driftLogMessage, evt.nodeId);
			}
			return baseNext;
		}
		case 'cache_decision': {
			if (!canApplyNodeEvent(state, evt.nodeId, evt.runId)) return state;
			const prevBinding = _normalizeBinding(state.nodeBindings?.[evt.nodeId], evt.nodeId);
			const nextIsUpToDate =
				evt.decision === 'cache_hit'
					? true
					: evt.decision === 'cache_hit_contract_mismatch'
						? false
						: (typeof prevBinding.isUpToDate === 'boolean' ? prevBinding.isUpToDate : true);
			let nextBinding: NormalizedNodeBinding = {
				...prevBinding,
				cacheValid: evt.decision === 'cache_hit',
				isUpToDate: nextIsUpToDate,
				status: evt.decision === 'cache_hit_contract_mismatch' ? 'stale' : prevBinding.status,
				staleReason: evt.decision === 'cache_hit_contract_mismatch' ? 'CONTRACT_MISMATCH' : prevBinding.staleReason
			};
			if (evt.decision === 'cache_hit_contract_mismatch' && isNodeStateFromActiveRunAndFresh(state, prevBinding)) {
				nextBinding = {
					...prevBinding,
					cacheValid: false
				};
			}
			if (evt.decision === 'cache_hit') {
				const aid = evt.artifactId ?? prevBinding.current?.artifactId ?? prevBinding.last?.artifactId ?? null;
				nextBinding = _withPair(nextBinding, 'current', { execKey: evt.execKey, artifactId: aid });
			} else {
				nextBinding = _withPair(nextBinding, 'current', { execKey: null, artifactId: null });
			}
			_assertBindingPairInvariant(nextBinding, evt.nodeId, 'cache_decision');
			const nodeBindings = {
				...state.nodeBindings,
				[evt.nodeId]: nextBinding
			};
			const prev = state.nodeOutputs?.[evt.nodeId];
			const nextForNode: NodeOutputInfo = {
				mimeType: prev?.mimeType,
				payloadType: prev?.payloadType,
				preview: prev?.preview,
				cached: evt.decision === 'cache_hit',
				cacheDecision: evt.decision,
				expectedContractFingerprint:
					evt.decision === 'cache_hit_contract_mismatch'
						? String((evt as any).expectedContractFingerprint ?? '')
						: prev?.expectedContractFingerprint,
				actualContractFingerprint:
					evt.decision === 'cache_hit_contract_mismatch'
						? String((evt as any).actualContractFingerprint ?? '')
						: prev?.actualContractFingerprint,
				mismatchKind:
					evt.decision === 'cache_hit_contract_mismatch'
						? String((evt as any).mismatchKind ?? '')
						: prev?.mismatchKind
			};
			return withGraphMeta({
				...state,
				nodeBindings,
				nodeOutputs: {
					...state.nodeOutputs,
					[evt.nodeId]: nextForNode
				}
			});
		}
		case 'run_started': {
			const evtMode = ((evt as any).runMode ?? state.activeRunMode) as ActiveRunMode;
			const evtPlanned = Array.isArray((evt as any).plannedNodeIds)
				? new Set<string>((evt as any).plannedNodeIds as string[])
				: computePlannedNodeSet(
					state.nodes,
					state.edges,
					evt.runFrom ?? null,
					evtMode ?? (evt.runFrom ? 'from_selected_onward' : 'from_start')
				);
			const evtPinned = Array.isArray((evt as any).pinnedNodeIds)
				? new Set<string>((evt as any).pinnedNodeIds as string[])
				: new Set<string>();
			const nodeBindings = { ...state.nodeBindings };
			for (const nodeId of evtPlanned) {
				if (evtPinned.has(nodeId)) continue;
				const prevBinding = _normalizeBinding(nodeBindings[nodeId], nodeId);
				const hasArtifact = Boolean(
					prevBinding.current?.artifactId ??
					prevBinding.currentArtifactId ??
					prevBinding.last?.artifactId ??
					prevBinding.lastArtifactId
				);
				if (!hasArtifact) continue;
				if (isNodeStateFromActiveRunAndFresh(state, prevBinding)) continue;
				nodeBindings[nodeId] = {
					...prevBinding,
					status: 'stale',
					isUpToDate: false,
					cacheValid: false,
					currentRunId: null,
					staleReason: 'RUN_PENDING'
				};
			}
			const nodeOutputs = clearNodeCacheUiForNodes(state.nodeOutputs, evtPlanned);
			const nodes = applyLlmHolderToNodes(state.nodes, null);
			return withGraphMeta(
				logPush(
					{
						...state,
						nodes,
						activeRunId: evt.runId ?? state.activeRunId,
						activeRunMode: evtMode,
						activeRunFrom: evt.runFrom ?? state.activeRunFrom,
						activeRunNodeSet: evtPlanned,
						nodeBindings,
						nodeOutputs,
						queueRuntime: {
							...(state.queueRuntime ?? {}),
							metrics: {},
							nodeMetrics: {},
							runtimeItemMetrics: {},
							runScoped: undefined,
							schedulerSnapshot: undefined,
							llmLease: undefined,
							adaptiveDecisions: [],
							currentRunSummary: {
								runId: String(evt.runId ?? runId),
								maxPendingQueueDepth: 0,
								hadStalledSnapshot: false,
								blockedEvents: 0
							},
							blockedByNode: {},
							softFailByNode: {}
							,
							controlPlaneEdgeState: {},
							controlPlaneNodeState: {},
							appliedControlSeq: 0
						}
					},
					'info',
					`Run started ${evt.runFrom ? `(from ${evt.runFrom})` : '(from start)'}`
				)
			);
		}
		case 'run_pause_requested': {
			return withGraphMeta(logPush({ ...state, runStatus: 'pausing' }, 'info', 'Pause requested'));
		}
		case 'run_pausing': {
			return withGraphMeta(logPush({ ...state, runStatus: 'pausing' }, 'info', 'Run pausing'));
		}
		case 'run_paused': {
			const nodes = applyLlmHolderToNodes(state.nodes, null);
			const edges = (state.edges ?? []).map((edge) => {
				const mode = String((edge.data as any)?.mode ?? 'work').trim().toLowerCase();
				if (mode !== 'work') return edge;
				const exec = String((edge.data as any)?.exec ?? 'idle').trim().toLowerCase();
				if (exec !== 'active') return edge;
				return {
					...edge,
					data: {
						...(edge.data ?? {}),
						exec: 'idle'
					}
				};
			});
			const snapshot = ((evt as any)?.snapshot ?? null) as Record<string, any> | null;
			const withFrontierBindings = applyPauseSnapshotFrontierBindings(
				{
					...state,
					nodes,
					edges,
					runStatus: 'paused'
				},
				snapshot
			);
			return withGraphMeta(
				logPush(
					withFrontierBindings,
					'info',
					'Run paused'
				)
			);
		}
		case 'run_resume_requested':
		case 'run_resuming': {
			return withGraphMeta(logPush({ ...state, runStatus: 'resuming' }, 'info', 'Run resuming'));
		}
		case 'run_resumed': {
			const evtMode = ((evt as any).runMode ?? state.activeRunMode) as ActiveRunMode;
			const evtPlanned = Array.isArray((evt as any).plannedNodeIds)
				? new Set<string>((evt as any).plannedNodeIds as string[])
				: state.activeRunNodeSet;
			return withGraphMeta(
				logPush(
					{
						...state,
						runStatus: 'running',
						activeRunId: evt.runId ?? state.activeRunId,
						activeRunMode: evtMode,
						activeRunFrom: (evt as any).runFrom ?? state.activeRunFrom,
						activeRunNodeSet: evtPlanned
					},
					'info',
					'Run resumed'
				)
			);
		}
		case 'run_resume_failed': {
			const code = String((evt as any)?.errorCode ?? '').trim();
			const msg = String((evt as any)?.error ?? '').trim();
			const details = ((evt as any)?.details ?? null) as Record<string, unknown> | null;
			const reasonCodes = Array.isArray(details?.reasonCodes)
				? (details?.reasonCodes as unknown[]).map((value) => String(value ?? '').trim()).filter(Boolean)
				: [];
			const nodeIds = Array.isArray(details?.nodeIds)
				? (details?.nodeIds as unknown[]).map((value) => String(value ?? '').trim()).filter(Boolean)
				: [];
			const diagSuffixParts: string[] = [];
			if (reasonCodes.length > 0) diagSuffixParts.push(`reasons=${reasonCodes.join(',')}`);
			if (nodeIds.length > 0) diagSuffixParts.push(`nodes=${nodeIds.join(',')}`);
			const diagSuffix = diagSuffixParts.length > 0 ? ` [${diagSuffixParts.join(' ')}]` : '';
			return withGraphMeta(
				logPush(
					{ ...state, runStatus: 'paused' },
					'error',
					`Run resume failed${code ? ` (${code})` : ''}${msg ? `: ${msg}` : ''}${diagSuffix}`
				)
			);
		}
		case 'run_telemetry': {
			const previousSummary =
				(state.queueRuntime?.currentRunSummary &&
				typeof state.queueRuntime.currentRunSummary === 'object'
					? state.queueRuntime.currentRunSummary
					: null) ?? null;
			if (!previousSummary) return state;
			return {
				...state,
				queueRuntime: {
					...(state.queueRuntime ?? {}),
					currentRunSummary: {
						...previousSummary,
						runtimeMs: Math.max(0, Number((evt as any)?.runtime_ms ?? 0)),
						peakConcurrency: Math.max(0, Number((evt as any)?.peak_concurrency ?? 0))
					}
				}
			};
		}
		case 'node_started': {
			if (!canApplyNodeEvent(state, evt.nodeId, evt.runId)) return state;
			const prevBinding = _normalizeBinding(state.nodeBindings?.[evt.nodeId], evt.nodeId);
			if (
				prevBinding.currentRunId === (evt.runId ?? runId) &&
				(prevBinding.status.startsWith('succeeded') || prevBinding.isUpToDate === true)
			) {
				return state;
			}
			const nodeBindings = {
				...state.nodeBindings,
				[evt.nodeId]: {
					...prevBinding,
					status: 'running',
					currentRunId: evt.runId ?? runId
				}
			};
			const nodeOutputs = {
				...state.nodeOutputs,
				[evt.nodeId]: {
					...(state.nodeOutputs?.[evt.nodeId] ?? {}),
					lastError: null
				}
			};
			const blockedByNode =
				state.queueRuntime?.blockedByNode && typeof state.queueRuntime.blockedByNode === 'object'
					? { ...(state.queueRuntime.blockedByNode as Record<string, unknown>) }
					: {};
			if (Object.prototype.hasOwnProperty.call(blockedByNode, evt.nodeId)) {
				delete (blockedByNode as Record<string, unknown>)[evt.nodeId];
			}
			return withGraphMeta(
				logPush(
					{
						...state,
						nodeBindings,
						nodeOutputs,
						queueRuntime: {
							...(state.queueRuntime ?? {}),
							blockedByNode: blockedByNode as any
						}
					},
					'info',
					'Node started',
					evt.nodeId
				)
			);
		}
		case 'component_started': {
			if (!canApplyNodeEvent(state, evt.nodeId, evt.runId)) return state;
			const prevBinding = _normalizeBinding(state.nodeBindings?.[evt.nodeId], evt.nodeId);
			const nodeBindings = {
				...state.nodeBindings,
				[evt.nodeId]: {
					...prevBinding,
					status: 'running',
					currentRunId: evt.runId ?? runId,
					staleReason: null
				}
			};
			const nodeOutputs = {
				...state.nodeOutputs,
				[evt.nodeId]: {
					...(state.nodeOutputs?.[evt.nodeId] ?? {}),
					lastError: null
				}
			};
			return withGraphMeta(
				logPush({ ...state, nodeBindings, nodeOutputs }, 'info', 'Component started', evt.nodeId)
			);
		}
		case 'component_finished': {
			if (!canApplyNodeEvent(state, evt.nodeId, evt.runId)) return state;
			const prevBinding = _normalizeBinding(state.nodeBindings?.[evt.nodeId], evt.nodeId);
			// Keep running until wrapper component node itself emits node_finished.
			const nodeBindings = {
				...state.nodeBindings,
				[evt.nodeId]: {
					...prevBinding,
					status: 'running',
					currentRunId: evt.runId ?? runId
				}
			};
			return withGraphMeta(logPush({ ...state, nodeBindings }, 'info', 'Component internals finished', evt.nodeId));
		}
		case 'component_failed': {
			if (!canApplyNodeEvent(state, evt.nodeId, evt.runId)) return state;
			const prevBinding = _normalizeBinding(state.nodeBindings?.[evt.nodeId], evt.nodeId);
			const nodeBindings = {
				...state.nodeBindings,
				[evt.nodeId]: {
					...prevBinding,
					status: 'failed',
					isUpToDate: false,
					cacheValid: false,
					currentRunId: evt.runId ?? runId,
					staleReason: 'COMPONENT_FAILED'
				}
			};
			const nodeOutputs = {
				...state.nodeOutputs,
				[evt.nodeId]: {
					...(state.nodeOutputs?.[evt.nodeId] ?? {}),
					lastError: {
						message: String((evt as any).error ?? 'Component failed'),
						errorCode: 'COMPONENT_FAILED'
					}
				}
			};
			return withGraphMeta(logPush({ ...state, nodeBindings, nodeOutputs }, 'error', 'Component failed', evt.nodeId));
		}
		case 'edge_exec': {
			const edges = state.edges.map((e) => {
				if (e.id !== evt.edgeId) return e;
				const mode = String((e.data as any)?.mode ?? 'work').trim().toLowerCase();
				// Running visuals are only valid for active work-plane execution.
				if (mode !== 'work' && evt.exec === 'active') return e;
				return { ...e, data: { ...(e.data ?? {}), exec: evt.exec } };
			});
			return { ...state, edges };
		}
		case 'control_signal': {
			const signal = normalizeControlSignal(evt);
			if (!signal) {
				return logPush(state, 'warn', `[control] ignored unknown signal`, evt.nodeId);
			}
			const incomingSeq = Math.max(0, Number((evt as any)?.seq ?? 0));
			const appliedSeq = Math.max(0, Number((state.queueRuntime as any)?.appliedControlSeq ?? 0));
			if (incomingSeq > 0 && incomingSeq <= appliedSeq) {
				return state;
			}
			const nextAppliedSeq = Math.max(appliedSeq, incomingSeq);
			if (evt.nodeId && signal === 'llm_acquired') {
				// LLM star ownership is driven by llm_lease holder events (single source of truth).
				return logPush(
					{
						...state,
						queueRuntime: {
							...(state.queueRuntime ?? {}),
							appliedControlSeq: nextAppliedSeq
						}
					},
					'info',
					`[control] ${signal} node=${evt.nodeId}`,
					evt.nodeId
				);
			}
			if (evt.nodeId && signal === 'llm_released') {
				const nodeId = String(evt.nodeId ?? '').trim();
				const prevBinding = _normalizeBinding(state.nodeBindings?.[nodeId], nodeId);
				const nextBinding =
					prevBinding.status === 'running' && prevBinding.currentRunId === (evt.runId ?? runId)
						? {
								...prevBinding,
								status: 'busy',
								currentRunId: evt.runId ?? runId
							}
						: prevBinding;
				const nextState = {
					...state,
					edges: clearActiveWorkIncomingEdgesForNode(state.edges, nodeId),
					nodeBindings: {
						...state.nodeBindings,
						[nodeId]: nextBinding
					},
					queueRuntime: {
						...(state.queueRuntime ?? {}),
						appliedControlSeq: nextAppliedSeq
					}
				};
				return logPush(nextState, 'info', `[control] ${signal} node=${nodeId}`, nodeId);
			}
			const nodePart = evt.nodeId ? ` node=${evt.nodeId}` : '';
			const handle = String((evt as any)?.handle ?? '').trim();
			const handlePart = handle ? ` handle=${handle}` : '';
			const prevQueueRuntime =
				state.queueRuntime && typeof state.queueRuntime === 'object' ? state.queueRuntime : {};
			const queueRuntimeWithControlState = applyControlPlaneEdgeState(prevQueueRuntime as any, evt, signal);
			const prevNodeStateMap =
				prevQueueRuntime.controlPlaneNodeState && typeof prevQueueRuntime.controlPlaneNodeState === 'object'
					? (prevQueueRuntime.controlPlaneNodeState as Record<string, unknown>)
					: {};
			let nextNodeStateMap = prevNodeStateMap;
			const controlNodeId = String((evt as any)?.nodeId ?? '').trim();
			if (
				controlNodeId &&
				(signal === 'node_active' || signal === 'node_quiescent' || signal === 'node_terminal')
			) {
				const prior =
					prevNodeStateMap[controlNodeId] && typeof prevNodeStateMap[controlNodeId] === 'object'
						? (prevNodeStateMap[controlNodeId] as Record<string, unknown>)
						: {};
				nextNodeStateMap = {
					...prevNodeStateMap,
					[controlNodeId]: {
						nodeId: controlNodeId,
						lastSignal: signal,
						terminalReasonCode:
							signal === 'node_terminal'
								? (String((evt as any)?.reasonCode ?? '').trim() || undefined)
								: (String(prior.terminalReasonCode ?? '').trim() || undefined),
						lastSeq: nextAppliedSeq,
						updatedAt: String((evt as any)?.at ?? '')
					}
				};
			}
			if (!evt.nodeId || !handle) {
				return logPush(
					{
						...state,
						queueRuntime: {
						...queueRuntimeWithControlState
						,
						controlPlaneNodeState: nextNodeStateMap as any,
						appliedControlSeq: nextAppliedSeq
					}
				},
					'info',
					`[control] ${signal}${nodePart}${handlePart}`,
					evt.nodeId
				);
			}
			const key = `${evt.nodeId}:${handle}`;
			const prevHandleStates =
				prevQueueRuntime.handleStates && typeof prevQueueRuntime.handleStates === 'object'
					? prevQueueRuntime.handleStates
					: {};
			const prevTimeline = Array.isArray(prevQueueRuntime.handleTimeline) ? prevQueueRuntime.handleTimeline : [];
			const nextState = {
				...state,
				queueRuntime: {
					...queueRuntimeWithControlState,
					controlPlaneNodeState: nextNodeStateMap as any,
					handleStates: {
						...prevHandleStates,
						[key]: {
							state: signal,
							updatedAt: String((evt as any)?.at ?? '')
						}
					},
					handleTimeline: [
						...prevTimeline.slice(Math.max(0, prevTimeline.length - 199)),
						{
							nodeId: String(evt.nodeId),
							handle,
							signal,
							at: String((evt as any)?.at ?? '')
						}
					],
					appliedControlSeq: nextAppliedSeq
				}
			};
			return logPush(nextState, 'info', `[control] ${signal}${nodePart}${handlePart}`, evt.nodeId);
		}
		case 'branch_cascade': {
			const originNodeId = String((evt as any)?.originNodeId ?? '').trim();
			const blockedNodeIds = Array.isArray((evt as any)?.blockedNodeIds)
				? ((evt as any).blockedNodeIds as unknown[]).map((item) => String(item ?? '').trim()).filter(Boolean)
				: [];
			const reasonCode = String((evt as any)?.reasonCode ?? '').trim();
			const previous = Array.isArray((state.queueRuntime as any)?.branchCascade)
				? (((state.queueRuntime as any).branchCascade as any[]) ?? [])
				: [];
			const entry = {
				originNodeId,
				blockedNodeIds,
				reasonCode,
				at: String((evt as any)?.at ?? '')
			};
			const nextState = {
				...state,
				queueRuntime: {
					...(state.queueRuntime ?? {}),
					branchCascade: [...previous.slice(Math.max(0, previous.length - 99)), entry]
				}
			};
			return logPush(
				nextState,
				'warn',
				`[cascade] origin=${originNodeId || '(unknown)'} blocked=${blockedNodeIds.join(',') || '(none)'}${reasonCode ? ` reason=${reasonCode}` : ''}`,
				originNodeId || undefined
			);
		}
		case 'node_handle_satisfaction': {
			const nodeId = String((evt as any)?.nodeId ?? '').trim();
			const handle = String((evt as any)?.handle ?? '').trim();
			const statusRaw = String((evt as any)?.status ?? '').trim().toLowerCase();
			const status = statusRaw === 'partial' || statusRaw === 'none' ? statusRaw : 'all';
			if (!nodeId || !handle) return state;
			const key = `${nodeId}:${handle}`;
			const previous =
				(state.queueRuntime?.handleSatisfaction && typeof state.queueRuntime.handleSatisfaction === 'object'
					? state.queueRuntime.handleSatisfaction
					: {}) ?? {};
			const nextState = {
				...state,
				queueRuntime: {
					...(state.queueRuntime ?? {}),
					handleSatisfaction: {
						...previous,
						[key]: {
							nodeId,
							handle,
							status,
							connectedEdges: Number((evt as any)?.connectedEdges ?? 0),
							providedEdges: Number((evt as any)?.providedEdges ?? 0),
							updatedAt: String((evt as any)?.at ?? '')
						}
					}
				}
			};
			const level = status === 'all' ? 'info' : status === 'partial' ? 'warn' : 'error';
			return logPush(
				nextState,
				level,
				`[handle] ${handle} status=${status} provided=${Number((evt as any)?.providedEdges ?? 0)}/${Number((evt as any)?.connectedEdges ?? 0)}`,
				nodeId
			);
		}
		case 'node_input_warning': {
			const nodeId = String((evt as any)?.nodeId ?? '').trim();
			const handle = String((evt as any)?.handle ?? '').trim();
			const edgeId = String((evt as any)?.edgeId ?? '').trim();
			const planeRaw = String((evt as any)?.plane ?? '').trim().toLowerCase();
			const plane: 'param' | 'control' = planeRaw === 'control' ? 'control' : 'param';
			if (!nodeId || !handle || !edgeId) return state;
			const key = `${nodeId}:${handle}:${edgeId}:${plane}`;
			const previous =
				(state.queueRuntime?.paramControlWarnings &&
				typeof state.queueRuntime.paramControlWarnings === 'object'
					? state.queueRuntime.paramControlWarnings
					: {}) ?? {};
			const codeRaw = String((evt as any)?.code ?? '').trim();
			const code: 'PARAM_CONTROL_EMPTY_INPUT' =
				codeRaw === 'PARAM_CONTROL_EMPTY_INPUT' ? 'PARAM_CONTROL_EMPTY_INPUT' : 'PARAM_CONTROL_EMPTY_INPUT';
			const reasonCode = String((evt as any)?.reasonCode ?? '').trim();
			const upstreamNodeId = String((evt as any)?.upstreamNodeId ?? '').trim();
			const nextState = {
				...state,
				queueRuntime: {
					...(state.queueRuntime ?? {}),
					paramControlWarnings: {
						...previous,
						[key]: {
							nodeId,
							handle,
							edgeId,
							plane,
							code,
							reasonCode: reasonCode || undefined,
							upstreamNodeId: upstreamNodeId || undefined,
							updatedAt: String((evt as any)?.at ?? '')
						}
					}
				}
			};
			return logPush(
				nextState,
				'warn',
				`[input-warning] plane=${plane} handle=${handle} edge=${edgeId}${reasonCode ? ` reason=${reasonCode}` : ''}`,
				nodeId
			);
		}
		case 'node_warning_summary': {
			const warningKey = String((evt as any)?.warningKey ?? '').trim();
			const nodeId = String((evt as any)?.nodeId ?? '').trim();
			const handle = String((evt as any)?.handle ?? '').trim();
			const code = String((evt as any)?.code ?? '').trim();
			const count = Math.max(0, Number((evt as any)?.count ?? 0));
			if (!warningKey || !nodeId || !handle || !code || count <= 0) return state;
			const previous =
				(state.queueRuntime?.warningSummary &&
				typeof state.queueRuntime.warningSummary === 'object'
					? state.queueRuntime.warningSummary
					: {}) ?? {};
			const plane = String((evt as any)?.plane ?? '').trim();
			const edgeId = String((evt as any)?.edgeId ?? '').trim();
			const reasonCode = String((evt as any)?.reasonCode ?? '').trim();
			const upstreamNodeId = String((evt as any)?.upstreamNodeId ?? '').trim();
			const nextState = {
				...state,
				queueRuntime: {
					...(state.queueRuntime ?? {}),
					warningSummary: {
						...previous,
						[warningKey]: {
							warningKey,
							nodeId,
							handle,
							code,
							plane: plane || undefined,
							edgeId: edgeId || undefined,
							reasonCode: reasonCode || undefined,
							upstreamNodeId: upstreamNodeId || undefined,
							count,
							firstAt: String((evt as any)?.firstAt ?? '').trim() || undefined,
							updatedAt: String((evt as any)?.at ?? '')
						}
					}
				}
			};
			if (count <= 1) return nextState;
			return logPush(
				nextState,
				'info',
				`[warning-summary] code=${code} handle=${handle} count=${count}${reasonCode ? ` reason=${reasonCode}` : ''}`,
				nodeId
			);
		}
		case 'node_blocked': {
			const nodeId = String((evt as any)?.nodeId ?? '').trim();
			if (!nodeId) return state;
			const reasonCode = String((evt as any)?.reasonCode ?? '').trim() || 'NO_READY_WORK';
			const handle = String((evt as any)?.handle ?? '').trim();
			const planeRaw = String((evt as any)?.plane ?? '').trim().toLowerCase();
			const plane =
				planeRaw === 'param' || planeRaw === 'control' || planeRaw === 'work'
					? planeRaw
					: planeRaw === 'config'
						? 'param'
						: undefined;
			const missingEdgeIds = Array.isArray((evt as any)?.missingEdgeIds)
				? ((evt as any).missingEdgeIds as unknown[]).map((item) => String(item ?? '').trim()).filter(Boolean)
				: [];
			const waitingOnNodeIds = Array.isArray((evt as any)?.waitingOnNodeIds)
				? ((evt as any).waitingOnNodeIds as unknown[]).map((item) => String(item ?? '').trim()).filter(Boolean)
				: [];
			const details =
				(evt as any)?.details && typeof (evt as any).details === 'object'
					? ({ ...((evt as any).details as Record<string, unknown>) } as Record<string, unknown>)
					: undefined;
			const previous =
				(state.queueRuntime?.blockedByNode && typeof state.queueRuntime.blockedByNode === 'object'
					? state.queueRuntime.blockedByNode
					: {}) ?? {};
			const previousSummary =
				(state.queueRuntime?.currentRunSummary &&
				typeof state.queueRuntime.currentRunSummary === 'object'
					? state.queueRuntime.currentRunSummary
					: null) ?? null;
			const nextState = {
				...state,
				queueRuntime: {
					...(state.queueRuntime ?? {}),
					currentRunSummary: previousSummary
						? {
								...previousSummary,
								blockedEvents: Math.max(0, Number(previousSummary.blockedEvents ?? 0)) + 1
							}
						: undefined,
					blockedByNode: {
						...previous,
						[nodeId]: {
							nodeId,
							reasonCode,
							handle: handle || undefined,
							plane: plane as any,
							missingEdgeIds,
							waitingOnNodeIds,
							details,
							updatedAt: String((evt as any)?.at ?? ''),
						}
					}
				}
			};
			return logPush(
				nextState,
				'info',
				`[blocked] node=${nodeId} reason=${reasonCode}${handle ? ` handle=${handle}` : ''}`,
				nodeId
			);
		}
		case 'control_gate_state': {
			const nodeId = String((evt as any)?.nodeId ?? '').trim();
			const gateState = String((evt as any)?.state ?? '').trim().toLowerCase() || 'blocked';
			const reasonCode = String((evt as any)?.reasonCode ?? '').trim();
			const handle = String((evt as any)?.handle ?? '').trim();
			const previousGates =
				(state.queueRuntime && typeof state.queueRuntime === 'object'
					? ((state.queueRuntime as any).controlGatesByNode ?? {})
					: {}) ?? {};
			const gateRecord =
				nodeId
					? {
							nodeId,
							state: gateState,
							reasonCode: reasonCode || undefined,
							handle: handle || undefined,
							updatedAt: String((evt as any)?.at ?? '')
						}
					: undefined;
			const nextState = gateRecord
				? {
						...state,
						queueRuntime: {
							...(state.queueRuntime ?? {}),
							controlGatesByNode: {
								...previousGates,
								[nodeId]: gateRecord
							}
						}
					}
				: state;
			return logPush(
				nextState,
				gateState === 'open' ? 'info' : 'warn',
				`[control-gate] node=${nodeId || '(unknown)'} state=${gateState}${reasonCode ? ` reason=${reasonCode}` : ''}${handle ? ` handle=${handle}` : ''}`,
				nodeId || undefined
			);
		}
		case 'queue_metrics': {
			const globalDepth = Number((evt as any)?.metrics?.globalDepth ?? 0);
			const perEdgeMax = Number((evt as any)?.metrics?.perEdgeMax ?? 0);
			const rawItemStats = (evt as any)?.runtimeItemMetrics ?? {};
			const rawByPlane = (rawItemStats?.byPlane ?? {}) as Record<string, any>;
			const normalizeMetricPlane = (raw: unknown): 'work' | 'param' | 'control' => {
				const value = String(raw ?? '').trim().toLowerCase();
				if (value === 'config') return 'param';
				if (value === 'param' || value === 'control') return value;
				return 'work';
			};
			const normalizedByPlane: Record<string, any> = {
				work: { itemsEnqueued: 0, itemsDequeued: 0, itemsAccepted: 0, itemsRejected: 0 },
				param: { itemsEnqueued: 0, itemsDequeued: 0, itemsAccepted: 0, itemsRejected: 0 },
				control: { itemsEnqueued: 0, itemsDequeued: 0, itemsAccepted: 0, itemsRejected: 0 }
			};
			for (const [rawPlaneKey, rawBucket] of Object.entries(rawByPlane)) {
				const planeNorm = normalizeMetricPlane(rawPlaneKey);
				const bucket = rawBucket && typeof rawBucket === 'object' ? (rawBucket as Record<string, unknown>) : {};
				normalizedByPlane[planeNorm] = {
					itemsEnqueued:
						Number(normalizedByPlane[planeNorm]?.itemsEnqueued ?? 0) + Number(bucket.itemsEnqueued ?? 0),
					itemsDequeued:
						Number(normalizedByPlane[planeNorm]?.itemsDequeued ?? 0) + Number(bucket.itemsDequeued ?? 0),
					itemsAccepted:
						Number(normalizedByPlane[planeNorm]?.itemsAccepted ?? 0) + Number(bucket.itemsAccepted ?? 0),
					itemsRejected:
						Number(normalizedByPlane[planeNorm]?.itemsRejected ?? 0) + Number(bucket.itemsRejected ?? 0)
				};
			}
			const rawByHandle = (rawItemStats?.byHandle ?? {}) as Record<string, any>;
			const normalizedByHandle: Record<string, any> = {};
			for (const [handleKey, rawHandleMetrics] of Object.entries(rawByHandle)) {
				const metrics =
					rawHandleMetrics && typeof rawHandleMetrics === 'object'
						? ({ ...(rawHandleMetrics as Record<string, unknown>) } as Record<string, unknown>)
						: {};
				metrics.plane = normalizeMetricPlane(metrics.plane);
				normalizedByHandle[handleKey] = metrics;
			}
			const itemStats = {
				...(rawItemStats && typeof rawItemStats === 'object' ? rawItemStats : {}),
				byPlane: normalizedByPlane,
				byHandle: normalizedByHandle
			};
			const enq = Number(itemStats?.itemsEnqueued ?? 0);
			const deq = Number(itemStats?.itemsDequeued ?? 0);
			const rej = Number(itemStats?.itemsRejected ?? 0);
			const planeStats = (itemStats?.byPlane ?? {}) as Record<string, any>;
			const workEnq = Number(planeStats?.work?.itemsEnqueued ?? 0);
			const paramEnq = Number(planeStats?.param?.itemsEnqueued ?? 0);
			const controlEnq = Number(planeStats?.control?.itemsEnqueued ?? 0);
			const prevAggregate = ((state.queueRuntime as any)?.aggregateDiagnostics ?? {}) as Record<string, unknown>;
			const nextAggregate = {
				queueMetricEvents: Number(prevAggregate.queueMetricEvents ?? 0) + 1,
				itemsEnqueued: Number(prevAggregate.itemsEnqueued ?? 0) + enq,
				itemsDequeued: Number(prevAggregate.itemsDequeued ?? 0) + deq,
				itemsAccepted: Number(prevAggregate.itemsAccepted ?? 0) + Number(itemStats?.itemsAccepted ?? 0),
				itemsRejected: Number(prevAggregate.itemsRejected ?? 0) + rej
			};
			const nextState = {
				...state,
				queueRuntime: {
					metrics: (evt as any)?.metrics ?? {},
					nodeMetrics: (evt as any)?.nodeMetrics ?? {},
					runtimeItemMetrics: itemStats ?? {},
					runScoped: {
						runId: String((evt as any)?.runId ?? ''),
						scope: String((evt as any)?.scope ?? 'run'),
						metrics: (evt as any)?.metrics ?? {},
						nodeMetrics: (evt as any)?.nodeMetrics ?? {},
						runtimeItemMetrics: itemStats ?? {}
					},
					aggregateDiagnostics: nextAggregate,
					handleStates:
						(state.queueRuntime?.handleStates && typeof state.queueRuntime.handleStates === 'object'
							? state.queueRuntime.handleStates
							: {}) ?? {},
					handleTimeline:
						Array.isArray(state.queueRuntime?.handleTimeline) ? state.queueRuntime.handleTimeline : []
					,
					branchCascade:
						Array.isArray((state.queueRuntime as any)?.branchCascade)
							? ((state.queueRuntime as any).branchCascade as any[])
							: [],
					handleSatisfaction:
						(state.queueRuntime?.handleSatisfaction &&
						typeof state.queueRuntime.handleSatisfaction === 'object'
							? state.queueRuntime.handleSatisfaction
							: {}) ?? {},
					paramControlWarnings:
						(state.queueRuntime?.paramControlWarnings &&
						typeof state.queueRuntime.paramControlWarnings === 'object'
							? state.queueRuntime.paramControlWarnings
							: {}) ?? {},
					warningSummary:
						(state.queueRuntime?.warningSummary &&
						typeof state.queueRuntime.warningSummary === 'object'
							? state.queueRuntime.warningSummary
							: {}) ?? {},
					schedulerSnapshot:
						(state.queueRuntime?.schedulerSnapshot &&
						typeof state.queueRuntime.schedulerSnapshot === 'object'
							? state.queueRuntime.schedulerSnapshot
							: undefined),
					llmLease:
						(state.queueRuntime?.llmLease &&
						typeof state.queueRuntime.llmLease === 'object'
							? state.queueRuntime.llmLease
							: undefined),
					adaptiveDecisions: Array.isArray((state.queueRuntime as any)?.adaptiveDecisions)
						? (((state.queueRuntime as any).adaptiveDecisions as unknown[]) ?? [])
						: [],
					currentRunSummary:
						(state.queueRuntime?.currentRunSummary &&
						typeof state.queueRuntime.currentRunSummary === 'object'
							? state.queueRuntime.currentRunSummary
							: undefined),
					runHistory: Array.isArray(state.queueRuntime?.runHistory)
						? state.queueRuntime?.runHistory
						: [],
					blockedByNode:
						(state.queueRuntime?.blockedByNode &&
						typeof state.queueRuntime.blockedByNode === 'object'
							? state.queueRuntime.blockedByNode
							: {}) ?? {},
					controlPlaneNodeState:
						((evt as any)?.controlPlaneNodeState &&
						typeof (evt as any).controlPlaneNodeState === 'object'
							? ((evt as any).controlPlaneNodeState as Record<string, unknown>)
							: (state.queueRuntime?.controlPlaneNodeState &&
								typeof state.queueRuntime.controlPlaneNodeState === 'object'
									? state.queueRuntime.controlPlaneNodeState
									: {})) ?? {},
					controlPlaneEdgeState:
						((evt as any)?.controlPlaneEdgeState &&
						typeof (evt as any).controlPlaneEdgeState === 'object'
							? ((evt as any).controlPlaneEdgeState as Record<string, unknown>)
							: state.queueRuntime?.controlPlaneEdgeState) ?? {}
				}
			};
			return logPush(
				nextState,
				'info',
				`[queue] scope=run depth=${globalDepth} per_edge_max=${perEdgeMax} enq=${enq} deq=${deq} rejected=${rej} by_plane(work=${workEnq},param=${paramEnq},control=${controlEnq})`
			);
		}
		case 'scheduler_snapshot': {
			const readyCount = Math.max(0, Number((evt as any)?.readyCount ?? 0));
			const inflightCount = Math.max(0, Number((evt as any)?.inflightCount ?? 0));
			const pendingQueueDepth = Math.max(0, Number((evt as any)?.pendingQueueDepth ?? 0));
			const runnableNodeCount = Math.max(0, Number((evt as any)?.runnableNodeCount ?? 0));
			const lastControlSeq = Math.max(0, Number((evt as any)?.lastControlSeq ?? 0));
			const appliedControlSeq = Math.max(0, Number((state.queueRuntime as any)?.appliedControlSeq ?? 0));
			const nextAppliedControlSeq = Math.max(appliedControlSeq, lastControlSeq);
			const stalled = Boolean((evt as any)?.stalled ?? false);
			const perNodeRaw = Array.isArray((evt as any)?.perNode) ? ((evt as any).perNode as unknown[]) : [];
			const perNode = perNodeRaw
				.map((item) => {
					const row = item as Record<string, unknown>;
					const nodeId = String(row?.nodeId ?? '').trim();
					if (!nodeId) return null;
					const readyWork = Boolean(row?.readyWork ?? false);
					const inflight = Math.max(0, Number(row?.inflight ?? 0));
					const pendingInputCount = Math.max(0, Number(row?.pendingInputCount ?? 0));
					const lastBlockedReasonCode = String(row?.lastBlockedReasonCode ?? '').trim();
					return {
						nodeId,
						readyWork,
						inflight,
						pendingInputCount,
					lastBlockedReasonCode: lastBlockedReasonCode || undefined
				};
			})
			.filter(Boolean) as Array<{
					nodeId: string;
					readyWork: boolean;
					inflight: number;
					pendingInputCount: number;
					lastBlockedReasonCode?: string;
				}>;
			const previousSummary =
				(state.queueRuntime?.currentRunSummary &&
				typeof state.queueRuntime.currentRunSummary === 'object'
					? state.queueRuntime.currentRunSummary
					: null) ?? null;
			const nextState = {
				...state,
				queueRuntime: {
					...(state.queueRuntime ?? {}),
					currentRunSummary: previousSummary
						? {
								...previousSummary,
								maxPendingQueueDepth: Math.max(
									Math.max(0, Number(previousSummary.maxPendingQueueDepth ?? 0)),
									pendingQueueDepth
								),
								hadStalledSnapshot: Boolean(previousSummary.hadStalledSnapshot ?? false) || stalled
							}
						: undefined,
					schedulerSnapshot: {
						readyCount,
						inflightCount,
						pendingQueueDepth,
						runnableNodeCount,
						stalled,
						perNode,
						updatedAt: String((evt as any)?.at ?? '')
					},
					controlPlaneEdgeState:
						((evt as any)?.controlPlaneEdgeState &&
						typeof (evt as any).controlPlaneEdgeState === 'object'
							? ((evt as any).controlPlaneEdgeState as Record<string, unknown>)
							: state.queueRuntime?.controlPlaneEdgeState) ?? {},
					appliedControlSeq: nextAppliedControlSeq
				}
			};
			return logPush(
				nextState,
				stalled ? 'warn' : 'info',
				`[scheduler-snapshot] ready=${readyCount} inflight=${inflightCount} pending=${pendingQueueDepth} runnable=${runnableNodeCount} stalled=${String(stalled).toLowerCase()}`
			);
		}
		case 'scheduler_adaptive_decision': {
			const at = String((evt as any)?.at ?? '').trim();
			const runIdForEvent = String((evt as any)?.runId ?? runId).trim();
			const mode = String((evt as any)?.mode ?? 'off').trim() || 'off';
			const enforced = Boolean((evt as any)?.enforced ?? false);
			const reasons = Array.isArray((evt as any)?.reasons)
				? ((evt as any).reasons as unknown[]).map((value) => String(value ?? '').trim()).filter(Boolean)
				: [];
			const toNumberRecord = (value: unknown): Record<string, number> => {
				if (!value || typeof value !== 'object') return {};
				const out: Record<string, number> = {};
				for (const [key, raw] of Object.entries(value as Record<string, unknown>)) {
					const n = Number(raw ?? 0);
					if (!Number.isFinite(n)) continue;
					out[String(key)] = n;
				}
				return out;
			};
			const toChangedCaps = (value: unknown): Record<string, { from: number; to: number }> => {
				if (!value || typeof value !== 'object') return {};
				const out: Record<string, { from: number; to: number }> = {};
				for (const [key, raw] of Object.entries(value as Record<string, unknown>)) {
					if (!raw || typeof raw !== 'object') continue;
					const from = Number((raw as Record<string, unknown>).from ?? 0);
					const to = Number((raw as Record<string, unknown>).to ?? 0);
					if (!Number.isFinite(from) || !Number.isFinite(to)) continue;
					out[String(key)] = { from, to };
				}
				return out;
			};
			const prior = Array.isArray((state.queueRuntime as any)?.adaptiveDecisions)
				? (((state.queueRuntime as any).adaptiveDecisions as unknown[]) ?? [])
				: [];
			const entry = {
				at,
				runId: runIdForEvent,
				mode,
				enforced,
				inputs:
					(evt as any)?.inputs && typeof (evt as any).inputs === 'object'
						? ({ ...((evt as any).inputs as Record<string, unknown>) } as Record<string, unknown>)
						: {},
				reasons,
				hardCaps: toNumberRecord((evt as any)?.hardCaps),
				minCaps: toNumberRecord((evt as any)?.minCaps),
				proposedCaps: toNumberRecord((evt as any)?.proposedCaps),
				effectiveCaps: toNumberRecord((evt as any)?.effectiveCaps),
				changedCaps: toChangedCaps((evt as any)?.changedCaps)
			};
			const nextState = {
				...state,
				queueRuntime: {
					...(state.queueRuntime ?? {}),
					adaptiveDecisions: [...prior.slice(Math.max(0, prior.length - 199)), entry]
				}
			};
			const changedKeys = Object.keys(entry.changedCaps);
			const reasonSuffix = reasons.length > 0 ? ` reasons=${reasons.join(',')}` : '';
			const changedSuffix =
				changedKeys.length > 0
					? ` changed=${changedKeys.map((key) => `${key}:${entry.changedCaps[key].from}->${entry.changedCaps[key].to}`).join(',')}`
					: ' changed=none';
			return logPush(
				nextState,
				'info',
				`[adaptive] mode=${mode} enforced=${String(enforced)}${changedSuffix}${reasonSuffix}`
			);
		}
		case 'llm_lease': {
			const stateRaw = String((evt as any)?.state ?? '').trim().toLowerCase();
			const leaseState = stateRaw === 'waiting' || stateRaw === 'acquired' || stateRaw === 'released' ? stateRaw : 'released';
			const nodeId = String((evt as any)?.nodeId ?? '').trim();
			const holderRaw = (evt as any)?.holderNodeId;
			const holderNodeId =
				holderRaw === null || holderRaw === undefined ? null : (String(holderRaw ?? '').trim() || null);
			const waitQueueLength = Math.max(0, Number((evt as any)?.waitQueueLength ?? 0));
			const waitingNodeIds = Array.isArray((evt as any)?.waitingNodeIds)
				? ((evt as any).waitingNodeIds as unknown[]).map((item) => String(item ?? '').trim()).filter(Boolean)
				: [];
			const prevActive = Array.isArray((state.queueRuntime?.llmLease as any)?.activeNodeIds)
				? ((state.queueRuntime?.llmLease as any).activeNodeIds as unknown[])
						.map((item) => String(item ?? '').trim())
						.filter(Boolean)
				: [];
			const activeNodeIds = new Set<string>(prevActive);
			if (leaseState === 'acquired' && nodeId) {
				activeNodeIds.add(nodeId);
			} else if (leaseState === 'released') {
				if (nodeId) activeNodeIds.delete(nodeId);
				if (holderNodeId) activeNodeIds.delete(String(holderNodeId ?? '').trim());
			}
			if (state.runStatus !== 'running') {
				activeNodeIds.clear();
			}
			const nodes = applyLlmHolderToNodes(state.nodes, activeNodeIds);
			const nextState = {
				...state,
				nodes,
				queueRuntime: {
					...(state.queueRuntime ?? {}),
					llmLease: {
						state: leaseState as 'waiting' | 'acquired' | 'released',
						nodeId: nodeId || undefined,
						holderNodeId,
						activeNodeIds: Array.from(activeNodeIds),
						waitQueueLength,
						waitingNodeIds,
						updatedAt: String((evt as any)?.at ?? '')
					}
				}
			};
			return logPush(
				nextState,
				'info',
				`[llm-lease] state=${leaseState} holder=${holderNodeId ?? '(none)'} queue=${waitQueueLength}`,
				nodeId || undefined
			);
		}
		case 'contract_drift': {
			const edgeId = String((evt as any)?.edgeId ?? '').trim();
			const targetNodeId = String((evt as any)?.targetNodeId ?? '').trim();
			const srcFp = String((evt as any)?.snapshotSourceSchemaFingerprint ?? '').trim().slice(0, 12);
			const curFp = String((evt as any)?.currentSourceSchemaFingerprint ?? '').trim().slice(0, 12);
			return logPush(
				state,
				'warn',
				`[contract-drift] edge=${edgeId || '(unknown)'} snapshot=${srcFp || '(missing)'} current=${curFp || '(missing)'}`,
				targetNodeId || undefined
			);
		}
		case 'node_decision': {
			const count = Number((evt as any)?.count ?? 1);
			const reason = String((evt as any)?.reasonCode ?? '').trim();
			const suffix = reason ? ` reason=${reason}` : '';
			return logPush(state, evt.decision === 'reject' ? 'warn' : 'info', `[decision] ${evt.decision} x${count}${suffix}`, evt.nodeId);
		}
		case 'node_reject': {
			const count = Number((evt as any)?.count ?? 1);
			const reason = String((evt as any)?.reasonCode ?? '').trim();
			const plane = String((evt as any)?.plane ?? 'work').trim();
			return logPush(
				state,
				'warn',
				`[reject] plane=${plane} x${count}${reason ? ` reason=${reason}` : ''}`,
				(evt as any)?.nodeId
			);
		}
		case 'log': {
			const message = String(evt.message ?? '');
			const edgeId = String((evt as any)?.edgeId ?? '').trim() || undefined;
			const softFailMatch = message.match(/\[scheduler\]\s+soft-fail skip node=([^\s]+).*items=(\d+)/i);
			if (softFailMatch) {
				const nodeId = String(evt.nodeId ?? softFailMatch[1] ?? '').trim();
				const itemsRejected = Math.max(1, Number(softFailMatch[2] ?? 1));
				const previous =
					(state.queueRuntime?.softFailByNode &&
					typeof state.queueRuntime.softFailByNode === 'object'
						? state.queueRuntime.softFailByNode
						: {}) ?? {};
				const prevNode = previous[nodeId] ?? { count: 0, itemsRejected: 0 };
				const nextState = {
					...state,
					queueRuntime: {
						...(state.queueRuntime ?? {}),
						softFailByNode: {
							...previous,
							[nodeId]: {
								count: Number(prevNode.count ?? 0) + 1,
								itemsRejected: Number(prevNode.itemsRejected ?? 0) + itemsRejected,
								lastAt: String((evt as any)?.at ?? '')
							}
						}
					}
				};
				return logPush(nextState, evt.level, message, evt.nodeId, (evt as any).componentPath, edgeId);
			}
			return logPush(state, evt.level, message, evt.nodeId, (evt as any).componentPath, edgeId);
		}
		case 'node_finished': {
			if (!canApplyNodeEvent(state, evt.nodeId, evt.runId)) return state;
			const prevBinding = _normalizeBinding(state.nodeBindings?.[evt.nodeId], evt.nodeId);
			const softFailByNode =
				(state.queueRuntime?.softFailByNode &&
				typeof state.queueRuntime.softFailByNode === 'object'
					? state.queueRuntime.softFailByNode
					: {}) ?? {};
			const softFailNode = softFailByNode[String(evt.nodeId ?? '')] ?? null;
			const softFailSucceeded =
				evt.status === 'stale' &&
				Boolean(softFailNode && Number((softFailNode as any)?.count ?? 0) > 0);
			const succeeded = evt.status === 'succeeded' || softFailSucceeded;
			const rawError = typeof evt.error === 'string' ? String(evt.error) : '';
			let parsedErrorPayload: Record<string, unknown> | null = null;
			if (rawError.startsWith('{') && rawError.endsWith('}')) {
				try {
					const parsed = JSON.parse(rawError);
					if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
						parsedErrorPayload = parsed as Record<string, unknown>;
					}
				} catch {
					parsedErrorPayload = null;
				}
			}
			const errorDetails =
				((evt as any).errorDetails as Record<string, unknown> | undefined) ??
				((parsedErrorPayload?.details as Record<string, unknown> | undefined) ?? undefined);
			const errorCodeValue =
				typeof (evt as any).errorCode === 'string'
					? String((evt as any).errorCode)
					: typeof parsedErrorPayload?.errorCode === 'string'
						? String(parsedErrorPayload.errorCode)
						: typeof parsedErrorPayload?.code === 'string'
							? String(parsedErrorPayload.code)
							: undefined;
			const errorMessageValue =
				typeof parsedErrorPayload?.message === 'string'
					? String(parsedErrorPayload.message)
					: rawError || undefined;
			const errorPayload: NodeExecutionError | null = succeeded
				? null
				: {
					message: errorMessageValue,
					errorCode: errorCodeValue,
					op: typeof errorDetails?.op === 'string' ? String(errorDetails.op) : undefined,
					paramPath: typeof errorDetails?.paramPath === 'string' ? String(errorDetails.paramPath) : undefined,
					missingColumns: Array.isArray(errorDetails?.missingColumns)
						? errorDetails?.missingColumns.map((c) => String(c))
						: undefined,
					availableColumns: Array.isArray(errorDetails?.availableColumns)
						? errorDetails?.availableColumns.map((c) => String(c))
						: undefined,
					availableColumnsSource:
						typeof errorDetails?.availableColumnsSource === 'string'
							? (errorDetails.availableColumnsSource as any)
							: undefined
				};
			let nextBinding: NormalizedNodeBinding = {
				...prevBinding,
				status: succeeded ? 'succeeded_up_to_date' : evt.status,
				currentRunId: evt.runId ?? runId,
				isUpToDate: succeeded ? true : false,
				cacheValid: succeeded ? true : false,
				staleReason: succeeded ? null : prevBinding.staleReason
			};
			if (succeeded) {
				const current = _pairFromLegacy(nextBinding, 'current');
				if (current.execKey && current.artifactId) {
					nextBinding = _withPair(nextBinding, 'last', current);
				}
			}
			_assertBindingPairInvariant(nextBinding, evt.nodeId, 'node_finished');
			const nodeBindings = {
				...state.nodeBindings,
				[evt.nodeId]: nextBinding
			};
			const prevOut = state.nodeOutputs?.[evt.nodeId];
			const nodeOutputs = {
				...state.nodeOutputs,
				[evt.nodeId]: {
					...prevOut,
					cacheDecision: prevOut?.cacheDecision ?? (succeeded ? 'cache_miss' : prevOut?.cacheDecision),
					lastError: errorPayload
				}
			};
			const nodes = state.nodes.map((node) => {
				if (String(node.id) !== String(evt.nodeId)) return node;
				const meta = { ...((node.data as any)?.meta ?? {}) };
				delete (meta as any).llmAllocated;
				return {
					...node,
					data: {
						...node.data,
						meta
					}
				};
			});
			return withGraphMeta(
				logPush(
					{ ...state, nodeBindings, nodeOutputs, nodes },
					'info',
					`Node finished (${displayStatusFromBinding(nextBinding)})`,
					evt.nodeId
				)
			);
		}
		case 'run_finished': {
			const previousSummary =
				(state.queueRuntime?.currentRunSummary &&
				typeof state.queueRuntime.currentRunSummary === 'object'
					? state.queueRuntime.currentRunSummary
					: null) ?? null;
			const priorHistory = Array.isArray(state.queueRuntime?.runHistory)
				? (state.queueRuntime?.runHistory as Array<Record<string, unknown>>)
				: [];
			const historyRow = {
				runId: String(evt.runId ?? runId),
				finishedAt: String(evt.at ?? ''),
				status: (evt.status ?? 'succeeded') as RunStatus,
				runtimeMs: Math.max(0, Number((previousSummary as any)?.runtimeMs ?? 0)),
				peakConcurrency: Math.max(0, Number((previousSummary as any)?.peakConcurrency ?? 0)),
				maxPendingQueueDepth: Math.max(
					0,
					Number((previousSummary as any)?.maxPendingQueueDepth ?? 0)
				),
				hadStalledSnapshot: Boolean((previousSummary as any)?.hadStalledSnapshot ?? false),
				blockedEvents: Math.max(0, Number((previousSummary as any)?.blockedEvents ?? 0))
			};
			const nextHistory = [...priorHistory, historyRow].slice(-RUN_MONITOR_HISTORY_LIMIT);
			const nodes = state.nodes.map((node) => {
				const meta = { ...((node.data as any)?.meta ?? {}) };
				if (!Object.prototype.hasOwnProperty.call(meta, 'llmAllocated')) return node;
				delete (meta as any).llmAllocated;
				return {
					...node,
					data: {
						...node.data,
						meta
					}
				};
			});
			const nextEdges = (state.edges ?? []).map((edge) => {
				const exec = String((edge.data as any)?.exec ?? 'idle').trim().toLowerCase();
				if (exec !== 'active') return edge;
				return {
					...edge,
					data: {
						...(edge.data ?? {}),
						exec: evt.status === 'succeeded' ? 'done' : 'idle'
					}
				};
			});
			let nodeBindings = state.nodeBindings;
			if (evt.status === 'succeeded') {
				const componentNodeIds = new Set(
					(state.nodes ?? [])
						.filter((node) => String((node as any)?.data?.kind ?? '').trim().toLowerCase() === 'component')
						.map((node) => String(node.id ?? '').trim())
						.filter(Boolean)
				);
				if (componentNodeIds.size > 0) {
					const patched: Record<string, NormalizedNodeBinding> = {
						...(state.nodeBindings ?? {})
					} as Record<string, NormalizedNodeBinding>;
					let changed = false;
					for (const nodeId of componentNodeIds) {
						const prevBinding = _normalizeBinding((state.nodeBindings ?? {})[nodeId], nodeId);
						const hasArtifact = Boolean(
							prevBinding.currentArtifactId ??
								prevBinding.current?.artifactId ??
								prevBinding.lastArtifactId ??
								prevBinding.last?.artifactId
						);
						if (!hasArtifact) continue;
						const canonicalPair = (() => {
							const currentPair = _pairFromLegacy(prevBinding, 'current');
							if (currentPair.execKey && currentPair.artifactId) return currentPair;
							const lastPair = _pairFromLegacy(prevBinding, 'last');
							if (lastPair.execKey && lastPair.artifactId) return lastPair;
							return null;
						})();
						let nextBinding: NormalizedNodeBinding = {
							...prevBinding,
							status:
								prevBinding.status === 'failed' || prevBinding.status === 'canceled'
									? prevBinding.status
									: 'succeeded_up_to_date',
							isUpToDate: true,
							cacheValid: true,
							staleReason: null
						};
						if (canonicalPair) {
							nextBinding = _withPair(nextBinding, 'current', canonicalPair);
							nextBinding = _withPair(nextBinding, 'last', canonicalPair);
						}
						if (
							nextBinding.status !== prevBinding.status ||
							nextBinding.currentExecKey !== prevBinding.currentExecKey ||
							nextBinding.currentArtifactId !== prevBinding.currentArtifactId ||
							nextBinding.lastExecKey !== prevBinding.lastExecKey ||
							nextBinding.lastArtifactId !== prevBinding.lastArtifactId ||
							nextBinding.isUpToDate !== prevBinding.isUpToDate ||
							nextBinding.cacheValid !== prevBinding.cacheValid ||
							nextBinding.staleReason !== prevBinding.staleReason
						) {
							patched[nodeId] = nextBinding;
							changed = true;
						}
					}
					if (changed) {
						nodeBindings = patched;
					}
				}
			}
			return withGraphMeta(
				logPush(
					{
						...state,
						runStatus: evt.status,
						edges: nextEdges,
						nodes,
						nodeBindings,
						queueRuntime: {
							...(state.queueRuntime ?? {}),
							currentRunSummary: undefined,
							runHistory: nextHistory as any
						}
					},
					'info',
					`Run finished (${evt.status})`
				)
			);
		}
		default:
			return state;
	}
}

function hydrateFromRunSnapshotState(state: GraphState, snap: RunSnapshotLike): GraphState {
	if (typeof snap.graphId === 'string' && snap.graphId && snap.graphId !== state.graphId) {
		return state;
	}
	const nodeBindingsPatch: Record<string, NormalizedNodeBinding> = {};
	const nodeOutputs: Record<string, NodeOutputInfo> = { ...(state.nodeOutputs ?? {}) };
	const componentNodeIds = new Set(
		(state.nodes ?? [])
			.filter((node) => String((node as any)?.data?.kind ?? '').trim().toLowerCase() === 'component')
			.map((node) => String(node.id ?? '').trim())
			.filter(Boolean)
	);
	const hasPair = (binding: NormalizedNodeBinding | undefined | null, which: 'current' | 'last'): boolean => {
		const pair = which === 'current' ? binding?.current : binding?.last;
		return Boolean(pair?.execKey && pair?.artifactId);
	};
	const pairEquals = (
		left: { execKey?: string | null; artifactId?: string | null } | null | undefined,
		right: { execKey?: string | null; artifactId?: string | null } | null | undefined
	): boolean =>
		String(left?.execKey ?? '') === String(right?.execKey ?? '') &&
		String(left?.artifactId ?? '') === String(right?.artifactId ?? '');
	const isFailureLike = (status: string): boolean =>
		status === 'failed' || status === 'canceled' || status === 'skipped';
	const isExplicitInvalidation = (binding: NormalizedNodeBinding): boolean =>
		String(binding.status ?? '').trim().toLowerCase() === 'stale' &&
		Boolean(String(binding.staleReason ?? '').trim()) &&
		String(binding.staleReason ?? '').trim().toUpperCase() !== 'RUN_PENDING';
	for (const [nodeId, raw] of Object.entries(snap.nodeBindings ?? {})) {
		const incoming = _normalizeBinding(raw as NodeBindingInfo, nodeId);
		const prev = _normalizeBinding((state.nodeBindings ?? {})[nodeId], nodeId);
		const incomingStatus = String(incoming.status ?? '').trim().toLowerCase();
		const prevStatus = String(prev.status ?? '').trim().toLowerCase();
		const isComponentBoundary = componentNodeIds.has(String(nodeId ?? '').trim());
		const incomingStaleReason = String(incoming.staleReason ?? '').trim().toUpperCase();
		const incomingRunPendingStale = incomingStatus === 'stale' && incomingStaleReason === 'RUN_PENDING';
		const incomingNonAuthoritativeFreshness =
			(incomingStatus === 'stale' && !isExplicitInvalidation(incoming)) ||
			(incomingStatus === 'idle' && incoming.isUpToDate !== true) ||
			!hasPair(incoming, 'current');
		const prevCanonicalPair = hasPair(prev, 'current') ? prev.current : prev.last;
		const incomingCurrentPair = incoming.current;
		const incomingMatchesPrevPair =
			Boolean(incomingCurrentPair?.execKey && incomingCurrentPair?.artifactId) &&
			pairEquals(incomingCurrentPair, prevCanonicalPair);
		const prevFromActiveRun = Boolean(
			state.activeRunId &&
			prev.currentRunId &&
			String(prev.currentRunId) === String(state.activeRunId)
		);
		const prevLooksFresh = prevStatus === 'running' || prevStatus.startsWith('succeeded') || prev.isUpToDate === true;
		const incomingLooksWeaker =
			(incomingStatus === 'idle' || incomingStatus === 'stale') &&
			incoming.isUpToDate !== true &&
			(!hasPair(incoming, 'current') || incomingMatchesPrevPair);
		const preserveActiveRunFreshness =
			prevFromActiveRun &&
			prevLooksFresh &&
			incomingLooksWeaker &&
			!isFailureLike(incomingStatus) &&
			(!isExplicitInvalidation(incoming) || incomingMatchesPrevPair);
		if (preserveActiveRunFreshness) {
			const canonicalPair = prevCanonicalPair;
			let next: NormalizedNodeBinding = {
				...incoming,
				status: prev.status,
				isUpToDate: true,
				cacheValid: prev.cacheValid ?? true,
				staleReason: null
			};
			next = _withPair(next, 'current', canonicalPair);
			next = _withPair(next, 'last', canonicalPair);
			_assertBindingPairInvariant(next, nodeId, 'hydrate_snapshot_preserve_active_run_freshness');
			nodeBindingsPatch[nodeId] = next;
			continue;
		}
		const preserveFreshSuccess =
			isComponentBoundary &&
			(prevStatus.startsWith('succeeded') || prev.isUpToDate === true) &&
			(hasPair(prev, 'current') || hasPair(prev, 'last')) &&
			(incomingRunPendingStale || incomingNonAuthoritativeFreshness || incomingMatchesPrevPair) &&
			!isFailureLike(incomingStatus) &&
			(!isExplicitInvalidation(incoming) || incomingMatchesPrevPair);
		if (preserveFreshSuccess) {
			const canonicalPair = prevCanonicalPair;
			let next: NormalizedNodeBinding = {
				...incoming,
				status: prev.status,
				isUpToDate: true,
				cacheValid: prev.cacheValid ?? true,
				staleReason: null
			};
			next = _withPair(next, 'current', canonicalPair);
			next = _withPair(next, 'last', canonicalPair);
			_assertBindingPairInvariant(next, nodeId, 'hydrate_snapshot_preserve_fresh_success');
			nodeBindingsPatch[nodeId] = next;
			continue;
		}
		nodeBindingsPatch[nodeId] = incoming;
	}
	const nodeBindings = mergeBindingsSticky(state.nodeBindings ?? {}, nodeBindingsPatch);
	const runStatus = (snap.status as RunStatus) || state.runStatus;
	const runMode = snap.runMode ?? state.activeRunMode;
	const activeRunNodeSet = Array.isArray(snap.plannedNodeIds)
		? new Set<string>(snap.plannedNodeIds)
		: state.activeRunNodeSet;
	return withGraphMeta({
		...state,
		runStatus,
		nodeBindings,
		nodeOutputs,
		activeRunMode: runMode,
		activeRunFrom: state.activeRunFrom,
		activeRunNodeSet
	});
}

export function __applyRunEventForTest(state: GraphState, evt: KnownRunEvent, runId: string): GraphState {
	return applyRunEventState(state, evt, runId);
}

export function __hydrateFromRunSnapshotForTest(
	state: GraphState,
	snap: RunSnapshotLike
): GraphState {
	return hydrateFromRunSnapshotState(state, snap);
}

function buildHardResetState(freshGraphId: string): GraphState {
	return {
		graphId: freshGraphId,
		nodes: [],
		edges: [],
		selectedNodeId: null,
		inspector: INITIAL_INSPECTOR,
		logs: [],
		runStatus: RUN_IDLE,//change or fix 
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

export function __resetRunUiStateForTest(state: GraphState): GraphState {
	return resetRunUiState(state);
}

export function __collectPinnedArtifactsByNodeForTest(
	nodes: Node<PipelineNodeData & Record<string, unknown>>[],
	nodeBindings: Record<string, NodeBindingInfo | NormalizedNodeBinding | undefined>
): Record<string, { artifactId: string; execKey?: string; outputs?: Record<string, { artifactId: string; execKey?: string }> }> {
	return collectPinnedArtifactsByNode(nodes, nodeBindings);
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


function resetEdgesExec(edges: Edge<PipelineEdgeData>[]): Edge<PipelineEdgeData>[] {
	return edges.map((e) => ({ ...e, data: { ...e.data, exec: 'idle' as EdgeExec } }));
}

function resetRunUiState(state: GraphState): GraphState {
	const edges = resetEdgesExec(state.edges);
	const nodes = applyLlmHolderToNodes(state.nodes, null);
	const normalizedBindings = ensureNormalizedBindingsForNodes(nodes as any, state.nodeBindings ?? {});
	const nodeBindings: Record<string, NormalizedNodeBinding> = {};
	for (const [nodeId, binding] of Object.entries(normalizedBindings)) {
		const nodeIdNorm = String(nodeId ?? '').trim();
		if (!nodeIdNorm) continue;
		const lineage = binding.last?.artifactId || binding.last?.execKey ? binding.last : binding.current;
		nodeBindings[nodeIdNorm] = {
			...binding,
			status: 'idle',
			isUpToDate: false,
			cacheValid: false,
			currentRunId: null,
			lastRunId: null,
			staleReason: null,
			current: {
				execKey: null,
				artifactId: null
			},
			last: {
				execKey: lineage?.execKey ?? null,
				artifactId: lineage?.artifactId ?? null
			},
			currentExecKey: null,
			currentArtifactId: null,
			lastExecKey: lineage?.execKey ?? null,
			lastArtifactId: lineage?.artifactId ?? null
		};
	}
	return withGraphMeta({
		...state,
		nodes,
		edges,
		nodeBindings,
		logs: [],
		runStatus: RUN_IDLE,
		activeRunId: null,
		activeRunMode: 'from_start',
		activeRunFrom: null,
		activeRunNodeSet: new Set<string>()
	});
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
	let activeRunStreamHandle: { runId: string; close: () => void } | null = null;
	let resumeFallbackPollTimer: ReturnType<typeof setTimeout> | null = null;

	function clearResumeFallbackPollTimer(): void {
		if (!resumeFallbackPollTimer) return;
		clearTimeout(resumeFallbackPollTimer);
		resumeFallbackPollTimer = null;
	}

	function attachActiveRunEventStream(runId: string): void {
		const rid = String(runId ?? '').trim();
		if (!rid) return;
		if (activeRunStreamHandle) {
			try {
				activeRunStreamHandle.close();
			} catch {
				// no-op
			}
			activeRunStreamHandle = null;
		}
		clearResumeFallbackPollTimer();

		let settled = false;
		let terminalReconciling = false;
		let subHandle: { close: () => void } | null = null;
		const settle = () => {
			if (settled) return;
			settled = true;
			clearResumeFallbackPollTimer();
			if (activeRunStreamHandle?.runId === rid) {
				activeRunStreamHandle = null;
			}
			try {
				subHandle?.close();
			} catch {
				// no-op
			}
		};
		const applyEventBatch = (events: KnownRunEvent[]) => {
			let sawTerminal = false;
			for (const evt of events) {
				const cur = get({ subscribe } as any) as GraphState;
				const evtGraphId = (evt as any)?.graphId;
				if (typeof evtGraphId === 'string' && evtGraphId && evtGraphId !== cur.graphId) {
					continue;
				}
				const auditCtx: AuditContext =
					evt.type === 'run_started'
						? {
								source: 'event',
								evt,
								expectedDirtyTransition: true,
								allowedNodeIds: new Set<string>(
									Array.isArray((evt as any).plannedNodeIds)
										? ((evt as any).plannedNodeIds as string[])
										: []
								)
							}
						: { source: 'event', evt };
				update((s) => applyRunEventState(s, evt, rid), auditCtx);
				if (evt.type === 'run_finished' || evt.type === 'run_paused') {
					sawTerminal = true;
				}
			}
			if (!sawTerminal || terminalReconciling || settled) return;
			terminalReconciling = true;
			const current = get({ subscribe } as any) as GraphState;
			persist(current);
			void getRun(rid)
				.then((snap) => {
					const latest = get({ subscribe } as any) as GraphState;
					if (
						typeof snap.graphId === 'string' &&
						snap.graphId &&
						snap.graphId !== latest.graphId
					) {
						return;
					}
					update((s) => hydrateFromRunSnapshot(s, snap), {
						source: 'hydrate_snapshot',
						snapshotNodeIds: new Set(Object.keys(snap.nodeBindings ?? {}))
					});
				})
				.catch(() => {})
				.finally(() => {
					settle();
				});
		};
		const batcher = createEventBatcher<KnownRunEvent>(applyEventBatch, {
			maxBatchSize: 48,
			maxDelayMs: 16
		});
		subHandle = streamRunEvents(
			rid,
			(evt: KnownRunEvent) => {
				batcher.push(evt);
			},
			() => {
				if (settled) return;
				batcher.flush();
				update((s) => withGraphMeta(logPush({ ...s }, 'warn', 'Event stream error; reconciling run status')));
				void getRun(rid)
					.then((snap) => {
						const latest = get({ subscribe } as any) as GraphState;
						if (
							typeof snap.graphId === 'string' &&
							snap.graphId &&
							snap.graphId !== latest.graphId
						) {
							settle();
							return;
						}
						update((s) => hydrateFromRunSnapshot(s, snap), {
							source: 'hydrate_snapshot',
							snapshotNodeIds: new Set(Object.keys((snap as any)?.nodeBindings ?? {}))
						});
						const status = String((snap as any)?.status ?? '').toLowerCase();
						if (
							status === 'succeeded' ||
							status === 'failed' ||
							status === 'canceled' ||
							status === 'paused'
						) {
							update((s) =>
								withGraphMeta(logPush({ ...s }, 'info', `Run reconciled via poll (${status})`))
							);
							settle();
							return;
						}
						// Transitional status (running/resuming/pausing): keep reconciling until terminal.
						const poll = () => {
							if (settled) return;
							void getRun(rid)
								.then((nextSnap) => {
									const currentState = get({ subscribe } as any) as GraphState;
									if (
										typeof nextSnap.graphId === 'string' &&
										nextSnap.graphId &&
										nextSnap.graphId !== currentState.graphId
									) {
										settle();
										return;
									}
									update((s) => hydrateFromRunSnapshot(s, nextSnap), {
										source: 'hydrate_snapshot',
										snapshotNodeIds: new Set(Object.keys((nextSnap as any)?.nodeBindings ?? {}))
									});
									const nextStatus = String((nextSnap as any)?.status ?? '').toLowerCase();
									if (
										nextStatus === 'succeeded' ||
										nextStatus === 'failed' ||
										nextStatus === 'canceled' ||
										nextStatus === 'paused'
									) {
										update((s) =>
											withGraphMeta(logPush({ ...s }, 'info', `Run finished via polling (${nextStatus})`))
										);
										settle();
										return;
									}
									resumeFallbackPollTimer = setTimeout(poll, 2000);
								})
								.catch(() => {
									resumeFallbackPollTimer = setTimeout(poll, 3000);
								});
						};
						resumeFallbackPollTimer = setTimeout(poll, 1500);
					})
					.catch(() => {
						// Keep trying with poll loop when immediate reconciliation fetch fails.
						const poll = () => {
							if (settled) return;
							void getRun(rid)
								.then((snap) => {
									update((s) => hydrateFromRunSnapshot(s, snap), {
										source: 'hydrate_snapshot',
										snapshotNodeIds: new Set(Object.keys((snap as any)?.nodeBindings ?? {}))
									});
									const status = String((snap as any)?.status ?? '').toLowerCase();
									if (
										status === 'succeeded' ||
										status === 'failed' ||
										status === 'canceled' ||
										status === 'paused'
									) {
										settle();
										return;
									}
									resumeFallbackPollTimer = setTimeout(poll, 2000);
								})
								.catch(() => {
									resumeFallbackPollTimer = setTimeout(poll, 3000);
								});
						};
						resumeFallbackPollTimer = setTimeout(poll, 1500);
					});
			}
		);
		activeRunStreamHandle = { runId: rid, close: () => subHandle?.close() };
	}

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
			update((cur) => hydrateFromRunSnapshot(cur, snap), {
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

	function hydrateFromRunSnapshot(
		state: GraphState,
		snap: RunSnapshotLike
	): GraphState {
		return hydrateFromRunSnapshotState(state, snap);
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
			pauseResumeTraceEnabled = Boolean(enabled);
		},
		getPauseResumeTraceLoggingEnabled() {
			return pauseResumeTraceEnabled;
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

		//before extensive renovations

		async hardCancelActiveRuns() {
			clearResumeFallbackPollTimer();
			try {
				await cancelAllRuns({ hard: true });
			} catch (error) {
				update((s) => logPush(s, 'warn', `Cancel runs failed: ${String(error)}`));
			}
			if (activeRunStreamHandle) {
				try {
					activeRunStreamHandle.close();
				} catch {
					// no-op
				}
				activeRunStreamHandle = null;
			}
		},

		async pauseActiveRun() {
			const current = get({ subscribe } as any) as GraphState;
			const runId = String(current.activeRunId ?? '').trim();
			if (!runId) return { ok: false, reason: 'missing_run_id' as const };
			if (current.runStatus !== 'running' && current.runStatus !== 'pausing') {
				return { ok: false, reason: 'run_not_running' as const };
			}
			try {
				await pauseRun(runId);
				update((s) => withGraphMeta({ ...s, runStatus: 'pausing' }));
				return { ok: true as const };
			} catch (error) {
				update((s) => logPush(s, 'error', `Pause run failed: ${String(error)}`));
				return { ok: false as const, reason: 'pause_failed' as const, error: String(error) };
			}
		},

		async resumeActiveRun() {
			const current = get({ subscribe } as any) as GraphState;
			const runId = String(current.activeRunId ?? '').trim();
			if (!runId) return { ok: false, reason: 'missing_run_id' as const };
			if (current.runStatus !== 'paused' && current.runStatus !== 'resuming') {
				return { ok: false, reason: 'run_not_paused' as const };
			}
			try {
				await resumeRun(runId);
				update((s) => withGraphMeta({ ...s, runStatus: 'resuming' }));
				attachActiveRunEventStream(runId);
				return { ok: true as const };
			} catch (error) {
				update((s) => logPush(s, 'error', `Resume run failed: ${String(error)}`));
				return { ok: false as const, reason: 'resume_failed' as const, error: String(error) };
			}
		},

		// ----- clear edges of prior run's status (uses edge highlighting) -----
		resetRunUi() {
			update((s) => {
				clearResumeFallbackPollTimer();
				const next = resetRunUiState(s);
				persist(next);
				return next;
			}, { source: 'graph_edit' });
		},

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

		async runRemote(
			runFrom: string | null,
			runMode?: ActiveRunMode,
			cacheMode?: 'default_on' | 'force_off' | 'force_on',
			adaptiveMode?: 'off' | 'observe' | 'enforce' | null
		) {
			// prevent concurrent runs
			const s0 = get({ subscribe } as any) as GraphState;
			if (s0.runStatus === 'running' || s0.runStatus === 'pausing' || s0.runStatus === 'resuming') return;

			// reset UI
			this.resetRunUi();
			update((s) => withGraphMeta({ ...s, runStatus: 'running' }));

			// snapshot graph DTO
			const s1 = get({ subscribe } as any) as GraphState;
			const effectiveRunMode: ActiveRunMode = runMode ?? (runFrom ? 'from_selected_onward' : 'from_start');
			const dirtyNodeIds =
				effectiveRunMode === 'from_start'
					? Object.entries(s1.nodeBindings ?? {})
							.filter(([, binding]) => isBindingStale(binding))
							.map(([nodeId]) => nodeId)
					: [];
			const pinnedNodeIds = collectPinnedNodeIds(s1.nodes);
			const pinnedArtifacts = collectPinnedArtifactsByNode(
				s1.nodes,
				(s1.nodeBindings ?? {}) as Record<string, NodeBindingInfo | NormalizedNodeBinding | undefined>
			);
			const clearPerRunPinsIfAny = () => {
				update((s) => {
					const nextNodes = clearPerRunPinsOnNodes(s.nodes);
					if (nextNodes === s.nodes) return s;
					const next = withGraphMeta({ ...s, nodes: nextNodes });
					persist(next);
					return next;
				});
			};
			const sseRuntimeStats = {
				sseTerminalCount: 0,
				fallbackTerminalCount: 0,
				fallbackPollAttempts: 0
			};
			const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
			const waitForTerminalSnapshotWithFallback = async (
				runId: string,
				options?: { intervalMs?: number; maxAttempts?: number }
			): Promise<any> => {
				const intervalMs = Math.max(500, Number(options?.intervalMs ?? 3000));
				const maxAttempts = Math.max(1, Number(options?.maxAttempts ?? 120));
				for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
					sseRuntimeStats.fallbackPollAttempts += 1;
					const snap = await getRun(runId);
					const status = String((snap as any)?.status ?? '').toLowerCase();
					if (
						status === 'succeeded' ||
						status === 'failed' ||
						status === 'canceled' ||
						status === 'paused'
					) {
						return snap;
					}
					await sleep(intervalMs);
				}
				throw new Error(`Run terminal snapshot timeout: ${runId}`);
			};
			const waitForTerminalViaSse = async (
				runId: string,
				options?: {
					fallbackIntervalMs?: number;
					fallbackMaxAttempts?: number;
					maxReconnectAttempts?: number;
					reconnectBackoffMs?: number;
				}
			): Promise<{ snap: any; completionSource: 'sse' | 'fallback_poll' }> => {
				return await new Promise((resolve, reject) => {
					let settled = false;
					let subHandle: { close: () => void } | null = null;
					const maxReconnectAttempts = Math.max(0, Number(options?.maxReconnectAttempts ?? 2));
					const reconnectBackoffMs = Math.max(100, Number(options?.reconnectBackoffMs ?? 500));
					let reconnectAttempts = 0;
					const settleResolve = (value: { snap: any; completionSource: 'sse' | 'fallback_poll' }) => {
						if (settled) return;
						settled = true;
						subHandle?.close();
						resolve(value);
					};
					const settleReject = (error: unknown) => {
						if (settled) return;
						settled = true;
						subHandle?.close();
						reject(error);
					};
					const startStream = () => {
						if (settled) return;
						subHandle?.close();
						subHandle = streamRunEvents(
							runId,
							(evt: KnownRunEvent) => {
								if (evt.type !== 'run_finished' && evt.type !== 'run_paused') return;
								void getRun(runId)
									.then((snap) => {
										sseRuntimeStats.sseTerminalCount += 1;
										settleResolve({ snap, completionSource: 'sse' });
									})
									.catch((error) => settleReject(error));
							},
							() => {
								if (settled) return;
								if (reconnectAttempts < maxReconnectAttempts) {
									const delay = reconnectBackoffMs * Math.pow(2, reconnectAttempts);
									reconnectAttempts += 1;
									void sleep(delay).then(() => startStream());
									return;
								}
								void waitForTerminalSnapshotWithFallback(runId, {
									intervalMs: options?.fallbackIntervalMs ?? 3000,
									maxAttempts: options?.fallbackMaxAttempts ?? 120
								})
									.then((snap) => {
										sseRuntimeStats.fallbackTerminalCount += 1;
										settleResolve({ snap, completionSource: 'fallback_poll' });
									})
									.catch((error) => settleReject(error));
							}
						);
					};
					startStream();
				});
			};
			const runSingleWithStream = async (
				payload: ReturnType<typeof buildRunCreateRequest>,
				plannedNodeSet: Set<string>
			): Promise<void> => {
				let runId: string;
				try {
					const created = await createRun(payload);
					runId = created.runId;
					update((s) =>
						withGraphMeta({
							...s,
							graphId: created.graphId || s.graphId,
							activeRunId: runId,
							activeRunMode: effectiveRunMode,
							activeRunFrom: runFrom,
							activeRunNodeSet: plannedNodeSet
						})
					);
					try {
						const snap = await getRun(runId);
						update((s) => hydrateFromRunSnapshot(s, snap), {
							source: 'hydrate_snapshot',
							snapshotNodeIds: new Set(Object.keys(snap.nodeBindings ?? {}))
						});
					} catch {
						// non-fatal: stream events can still drive updates
					}
				} catch (e) {
					update((s) =>
						withGraphMeta(
							logPush({ ...s, runStatus: 'failed' }, 'error', `Run create failed: ${String(e)}`)
						)
					);
					return;
				}

				await new Promise<void>((resolve) => {
					let subHandle: { close: () => void } | null = null;
					let settled = false;
					let terminalReconciling = false;
					const settle = () => {
						if (settled) return;
						settled = true;
						if (activeRunStreamHandle?.runId === runId) {
							activeRunStreamHandle = null;
						}
						resolve();
					};
					const applyEventBatch = (events: KnownRunEvent[]) => {
						let sawTerminal = false;
						for (const evt of events) {
							const cur = get({ subscribe } as any) as GraphState;
							const evtGraphId = (evt as any)?.graphId;
							if (typeof evtGraphId === 'string' && evtGraphId && evtGraphId !== cur.graphId) {
								continue;
							}
							const auditCtx: AuditContext =
								evt.type === 'run_started'
									? {
											source: 'event',
											evt,
											expectedDirtyTransition: true,
											allowedNodeIds: new Set<string>(
												Array.isArray((evt as any).plannedNodeIds)
													? ((evt as any).plannedNodeIds as string[])
													: []
											)
										}
									: { source: 'event', evt };
							update((s) => {
								const nextState = applyRunEventState(s, evt, runId);
								debugLogOutOfScopeBindingMutation(s, nextState, evt.type);
								debugLogStaleFlips(s, nextState, evt.type);
								assertNoOutOfScopeStaleFlips(s, nextState, evt.type);
								if (evt.type === 'run_started') {
									assertRunStartedBindingTouchInScope(s, nextState);
								}
								return nextState;
							}, auditCtx);

							if (evt.type === 'run_finished' || evt.type === 'run_paused') {
								sawTerminal = true;
							}
						}
						if (!sawTerminal || terminalReconciling || settled) return;
						terminalReconciling = true;
						const current = get({ subscribe } as any) as GraphState;
						persist(current);
						void getRun(runId)
							.then((snap) => {
								const latest = get({ subscribe } as any) as GraphState;
								if (
									typeof snap.graphId === 'string' &&
									snap.graphId &&
									snap.graphId !== latest.graphId
								) {
									return;
								}
								update((s) => hydrateFromRunSnapshot(s, snap), {
									source: 'hydrate_snapshot',
									snapshotNodeIds: new Set(Object.keys(snap.nodeBindings ?? {}))
								});
							})
							.catch(() => {})
							.finally(() => {
								subHandle?.close();
								settle();
							});
					};
					const batcher = createEventBatcher<KnownRunEvent>(applyEventBatch, {
						maxBatchSize: 48,
						maxDelayMs: 16
					});
					subHandle = streamRunEvents(
						runId,
						(evt: KnownRunEvent) => {
							batcher.push(evt);
						},
						() => {
							const cur = get({ subscribe } as any) as GraphState;
							const isTerminalForThisRun =
								cur.activeRunId !== runId ||
								cur.runStatus === 'succeeded' ||
								cur.runStatus === 'failed' ||
								cur.runStatus === 'canceled' ||
								cur.runStatus === 'paused';
							if (isTerminalForThisRun) {
								settle();
								return;
							}
							batcher.flush();
							update((s) =>
								withGraphMeta(logPush({ ...s }, 'warn', 'Event stream error; reconciling run status'))
							);
							void getRun(runId)
								.then((snap) => {
									const status = String((snap as any)?.status ?? '').toLowerCase();
									if (
										status === 'succeeded' ||
										status === 'failed' ||
										status === 'canceled' ||
										status === 'paused'
									) {
										update((s) => hydrateFromRunSnapshot(s, snap), {
											source: 'hydrate_snapshot',
											snapshotNodeIds: new Set(Object.keys((snap as any)?.nodeBindings ?? {}))
										});
										update((s) =>
											withGraphMeta(
												logPush(
													{ ...s },
													'info',
													`Run reconciled via immediate poll (${status})`
												)
											)
										);
										settle();
										return;
									}
									update((s) =>
										withGraphMeta(logPush({ ...s }, 'warn', 'Event stream error; switching to fallback polling'))
									);
									void waitForTerminalViaSse(runId, {
										fallbackIntervalMs: 3000,
										fallbackMaxAttempts: 120,
										maxReconnectAttempts: 2,
										reconnectBackoffMs: 500
									})
										.then((snap) => {
											update((s) => hydrateFromRunSnapshot(s, snap.snap), {
												source: 'hydrate_snapshot',
												snapshotNodeIds: new Set(Object.keys((snap as any)?.snap?.nodeBindings ?? {}))
											});
											update((s) =>
												withGraphMeta(
													logPush(
														{ ...s },
														'info',
														`Run finished via ${snap.completionSource} (${String((snap as any)?.snap?.status ?? 'unknown')}) polls=${sseRuntimeStats.fallbackPollAttempts}`
													)
												)
											);
										})
										.catch((error) => {
											update((s) =>
												withGraphMeta(
													logPush(
														{ ...s, runStatus: 'failed' },
														'error',
														`Event stream error and fallback polling failed: ${String(error)}`
													)
												)
											);
										})
										.finally(() => {
											settle();
										});
								})
								.catch(() => {
									update((s) =>
										withGraphMeta(logPush({ ...s }, 'warn', 'Event stream error; switching to fallback polling'))
									);
									void waitForTerminalViaSse(runId, {
										fallbackIntervalMs: 3000,
										fallbackMaxAttempts: 120,
										maxReconnectAttempts: 2,
										reconnectBackoffMs: 500
									})
										.then((snap) => {
											update((s) => hydrateFromRunSnapshot(s, snap.snap), {
												source: 'hydrate_snapshot',
												snapshotNodeIds: new Set(Object.keys((snap as any)?.snap?.nodeBindings ?? {}))
											});
											update((s) =>
												withGraphMeta(
													logPush(
														{ ...s },
														'info',
														`Run finished via ${snap.completionSource} (${String((snap as any)?.snap?.status ?? 'unknown')}) polls=${sseRuntimeStats.fallbackPollAttempts}`
													)
												)
											);
										})
										.catch((error) => {
											update((s) =>
												withGraphMeta(
													logPush(
														{ ...s, runStatus: 'failed' },
														'error',
														`Event stream error and fallback polling failed: ${String(error)}`
													)
												)
											);
										})
										.finally(() => {
											settle();
										});
								});
						}
					);
					activeRunStreamHandle = { runId, close: () => subHandle?.close() };
				});
			};

			const componentPlans = planRunConnectedComponents(s1.nodes, s1.edges, runFrom, effectiveRunMode);
			const shouldRunAsSubgraphs =
				componentPlans.length > 1 ||
				(effectiveRunMode !== 'from_start' &&
					componentPlans.length === 1 &&
					componentPlans[0].size > 0 &&
					componentPlans[0].size < s1.nodes.length);

			if (!shouldRunAsSubgraphs) {
				const payload = buildRunCreateRequest(
					{ version: 1, nodes: s1.nodes, edges: s1.edges },
					s1.graphId,
					runFrom,
					effectiveRunMode,
					dirtyNodeIds,
					pinnedNodeIds,
					pinnedArtifacts,
					cacheMode,
					adaptiveMode ?? null
				);
				const plannedNodeSet = computePlannedNodeSet(s1.nodes, s1.edges, runFrom, effectiveRunMode);
				await runSingleWithStream(payload, plannedNodeSet);
				clearPerRunPinsIfAny();
				return;
			}

			const allPlannedNodeSet = new Set<string>();
			for (const componentSet of componentPlans) {
				for (const nodeId of componentSet) allPlannedNodeSet.add(nodeId);
			}
			update((s) =>
				withGraphMeta({
					...s,
					activeRunId: null,
					activeRunMode: effectiveRunMode,
					activeRunFrom: runFrom,
					activeRunNodeSet: allPlannedNodeSet
				})
			);

			const plannedByNodeId = new Map(
				s1.nodes.map((node) => [node.id, node]).filter(([id]) => Boolean(String(id ?? '').trim()))
			);
			const runPromises = componentPlans.map(async (componentSet, index) => {
				const componentTag = `sg${index + 1}/${componentPlans.length}`;
				const componentNodeIds = Array.from(componentSet);
				const componentNodes = componentNodeIds
					.map((id) => plannedByNodeId.get(id))
					.filter((node): node is Node<PipelineNodeData & Record<string, unknown>> => Boolean(node));
				const componentIdSet = new Set(componentNodes.map((node) => node.id));
				const componentEdges = s1.edges.filter(
					(edge) => componentIdSet.has(String(edge.source ?? '')) && componentIdSet.has(String(edge.target ?? ''))
				);
				const componentDirtyNodeIds = dirtyNodeIds.filter((nodeId) => componentIdSet.has(nodeId));
				update((s) =>
					withGraphMeta(
						logPush(
							{ ...s },
							'info',
							`[subgraph ${componentTag}] Run started (nodes=${componentNodes.length})`
						)
					)
				);
				try {
					const payload = buildRunCreateRequest(
						{ version: 1, nodes: componentNodes, edges: componentEdges },
						s1.graphId,
						null,
						'from_start',
						componentDirtyNodeIds,
						pinnedNodeIds,
						pinnedArtifacts,
						cacheMode,
						adaptiveMode ?? null
					);
					const created = await createRun(payload);
					const { snap, completionSource } = await waitForTerminalViaSse(created.runId, {
						fallbackIntervalMs: 3000,
						fallbackMaxAttempts: 120
					});
					update(
						(s) => hydrateFromRunSnapshot(s, snap),
						{
							source: 'hydrate_snapshot',
							snapshotNodeIds: new Set(Object.keys((snap as any)?.nodeBindings ?? {}))
						}
					);
					const status = String((snap as any)?.status ?? 'failed').toLowerCase();
					update((s) =>
						withGraphMeta(
							logPush(
								{ ...s },
								status === 'failed' ? 'error' : 'info',
								`[subgraph ${componentTag}] Run finished (${status}) via ${completionSource}`
							)
						)
					);
					return status;
				} catch (error) {
					update((s) =>
						withGraphMeta(
							logPush(
								{ ...s },
								'error',
								`[subgraph ${componentTag}] Run failed: ${String(error)}`
							)
						)
					);
					return 'failed';
				}
			});

			const statuses = await Promise.all(runPromises);
			const succeeded = statuses.filter((status) => status === 'succeeded').length;
			const failed = statuses.filter((status) => status === 'failed').length;
			const canceled = statuses.filter((status) => status === 'canceled').length;
			const aggregateStatus: RunStatus = failed > 0 ? 'failed' : canceled > 0 ? 'canceled' : 'succeeded';
			update((s) =>
				withGraphMeta(
					logPush(
						{ ...s, runStatus: aggregateStatus, activeRunId: null },
						failed > 0 ? 'error' : 'info',
						`[subgraph summary] total=${componentPlans.length} succeeded=${succeeded} failed=${failed} canceled=${canceled} completion_source(sse=${sseRuntimeStats.sseTerminalCount},fallback=${sseRuntimeStats.fallbackTerminalCount}) polls=${sseRuntimeStats.fallbackPollAttempts}`
					)
				)
			);
			clearPerRunPinsIfAny();
			const current = get({ subscribe } as any) as GraphState;
			persist(current);
		}
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

