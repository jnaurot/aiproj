// src/lib/flow/store/graphStore.run.ts
// Run lifecycle module extracted from graphStore.ts (Step 6 of refactor).
// Contains: pure run-event reducers, run-state helpers, pin/cache utilities,
//           and the createRunManager factory for closure-scoped run actions.

import type { Node, Edge } from '@xyflow/svelte';
import type { PipelineNodeData, PipelineEdgeData } from '$lib/flow/types';
import type { KnownRunEvent } from '$lib/flow/types/run';
import type { ActiveRunMode } from './runScope';
import {
	computePlannedNodeSet,
	planRunConnectedComponents,
	buildRunCreateRequest,
	displayStatusFromBinding,
	isBindingStale,
	mergeBindingsSticky,
	getStaleFlipNodeIds,
} from './runScope';
import {
	acceptNodeParams,
	cancelAllRuns,
	createEventBatcher,
	createRun,
	getRun,
	pauseRun,
	resolveSourceNode,
	resumeRun,
	streamRunEvents,
} from '$lib/flow/client/runs';
import type {
	NodeOutputInfo,
	NodeExecutionError,
	NodeBindingInfo,
	NormalizedNodeBinding,
	RunSnapshotLike,
	AuditContext,
	RunStatus,
	EdgeExec,
	GraphState,
} from './graphStore.types';
import { RUN_IDLE } from './graphStore.types';
import {
	logPush,
	stableJson,
	DEV_MODE,
	ensureNormalizedBindingsForNodes,
	withGraphMeta,
	_normalizeBinding,
	_withPair,
	_pairFromLegacy,
	_assertBindingPairInvariant,
} from './graphStore.audit';
import { effectiveExecParamsForNode } from './graphStore.inspector';
import { deriveObservedSchemaObservationFromNodeOutput, computeSchemaDriftSummary } from './graphStore.node-schema';
import { NodeSchemaEnvelopeSchema } from '$lib/flow/schema/schemaContract';
import type { CheckpointStaleness } from '$lib/flow/types/checkpoint';
import type { SchemaPlaneResult } from '$lib/flow/types/schemaPlane';


export function applyLlmHolderToNodes(
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

export function reconcileModelLeaseRunningInvariant(state: GraphState): GraphState {
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

export function applyRunEventState(state: GraphState, evt: KnownRunEvent, runId: string): GraphState {
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

export function getPauseResumeTraceEnabled(): boolean {
	return pauseResumeTraceEnabled;
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

export function isNodeStateFromActiveRunAndFresh(cur: GraphState, binding: NormalizedNodeBinding): boolean {
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


export function downstreamNodeIds(
	edges: Edge<PipelineEdgeData>[],
	nodeId: string,
	checkpointBoundaryNodeIds: Set<string> = new Set<string>()
): Set<string> {
	const out = new Set<string>([nodeId]);
	const q = [nodeId];
	while (q.length > 0) {
		const cur = q.shift()!;
		for (const e of edges) {
			if (e.source !== cur) continue;
			const nxt = String(e.target ?? '');
			if (!nxt || out.has(nxt)) continue;
			// Checkpointed nodes are execution/staleness boundaries for upstream changes.
			if (nxt !== nodeId && checkpointBoundaryNodeIds.has(nxt)) continue;
			out.add(nxt);
			q.push(nxt);
		}
	}
	return out;
}

export function __markStaleFromNodeForTest(state: GraphState, nodeId: string): GraphState {
	const checkpointBoundaryNodeIds = new Set<string>(Object.keys(state.checkpointRegistry ?? {}));
	const candidateIds = downstreamNodeIds(state.edges, nodeId, checkpointBoundaryNodeIds);
	const nodeBindings = { ...state.nodeBindings };
	let changed = false;
	for (const affectedId of candidateIds) {
		if (affectedId !== nodeId && checkpointBoundaryNodeIds.has(affectedId)) continue;
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

export function clearNodeCacheUi(
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

export function clearNodeCacheUiForNodes(
	nodeOutputs: Record<string, NodeOutputInfo>,
	nodeIds: Iterable<string>
): Record<string, NodeOutputInfo> {
	let next = nodeOutputs;
	for (const nodeId of nodeIds) {
		next = clearNodeCacheUi(next, nodeId);
	}
	return next;
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

type MemoDecision = 'reuse' | 'compute' | 'skip_non_memoizable';

function parseMemoDecisionFromTraceLog(message: string): { decision: MemoDecision; memoKey?: string } | null {
	const trimmed = String(message ?? '').trim();
	if (!trimmed.startsWith('[trace][memo.execute_decision]')) return null;
	const jsonStart = trimmed.indexOf('{');
	if (jsonStart < 0) return null;
	let payload: Record<string, unknown> | null = null;
	try {
		const parsed = JSON.parse(trimmed.slice(jsonStart));
		if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
			payload = parsed as Record<string, unknown>;
		}
	} catch {
		payload = null;
	}
	if (!payload) return null;
	const decision = String(payload.decision ?? '').trim() as MemoDecision;
	if (decision !== 'reuse' && decision !== 'compute' && decision !== 'skip_non_memoizable') return null;
	const memoKeyRaw = String((payload.memoKey ?? payload.memo_key ?? '') ?? '').trim();
	return {
		decision,
		memoKey: memoKeyRaw || undefined
	};
}

export function reduceRunEventState(state: GraphState, evt: KnownRunEvent, runId: string): GraphState {
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
			const nodeBindings = { ...state.nodeBindings };
			for (const [nodeId, rawBinding] of Object.entries(nodeBindings)) {
				const prevBinding = _normalizeBinding(rawBinding, nodeId);
				if (prevBinding.memoState == null && prevBinding.checkpointable !== true) continue;
				// Only reset memo/checkpoint state for nodes that are actually being
				// re-executed in this run.  Non-planned nodes (e.g. siblings that were
				// resolved via cache in an earlier run) must keep their memoState so
				// that the inspector can still offer the "Save checkpoint" action.
				if (!evtPlanned.has(nodeId)) continue;
				nodeBindings[nodeId] = {
					...prevBinding,
					memoState: undefined,
					checkpointable: false
				};
			}
			const evtRunId = String((evt as any).runId ?? runId ?? '').trim();
			for (const nodeId of evtPlanned) {
				const prevBinding = _normalizeBinding(nodeBindings[nodeId], nodeId);
				const hasArtifact = Boolean(
					prevBinding.current?.artifactId ??
					prevBinding.currentArtifactId ??
					prevBinding.last?.artifactId ??
					prevBinding.lastArtifactId
				);
				if (!hasArtifact) continue;
				if (isNodeStateFromActiveRunAndFresh(state, prevBinding)) continue;
				// If the binding already reflects the completion of THIS exact run (i.e. the
				// initial getRun snapshot arrived and was applied before the event stream
				// delivered run_started), skip the stale transition.  The node's final state
				// is already correct; clobbering it with 'stale' would cause a transient
				// disappearance of the "Save checkpoint" action and an unnecessary UI flash.
				if (
					evtRunId &&
					String(prevBinding.currentRunId ?? '').trim() === evtRunId &&
					String(prevBinding.status ?? '').toLowerCase().startsWith('succeeded')
				) continue;
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
			const memoDecision = parseMemoDecisionFromTraceLog(message);
			let memoStatePatched = state;
			if (memoDecision && evt.nodeId) {
				const nodeId = String(evt.nodeId ?? '').trim();
				const prevBinding = _normalizeBinding(state.nodeBindings?.[nodeId], nodeId);
				memoStatePatched = {
					...state,
					nodeBindings: {
						...state.nodeBindings,
						[nodeId]: {
							...prevBinding,
							memoState: {
								decision: memoDecision.decision,
								memoKey: memoDecision.memoKey,
								resolvedAt: new Date().toISOString()
							}
						}
					}
				};
			}
			const softFailMatch = message.match(/\[scheduler\]\s+soft-fail skip node=([^\s]+).*items=(\d+)/i);
			if (softFailMatch) {
				const nodeId = String(evt.nodeId ?? softFailMatch[1] ?? '').trim();
				const itemsRejected = Math.max(1, Number(softFailMatch[2] ?? 1));
				const previous =
					(memoStatePatched.queueRuntime?.softFailByNode &&
					typeof memoStatePatched.queueRuntime.softFailByNode === 'object'
						? memoStatePatched.queueRuntime.softFailByNode
						: {}) ?? {};
				const prevNode = previous[nodeId] ?? { count: 0, itemsRejected: 0 };
				const nextState = {
					...memoStatePatched,
					queueRuntime: {
						...(memoStatePatched.queueRuntime ?? {}),
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
			return logPush(memoStatePatched, evt.level, message, evt.nodeId, (evt as any).componentPath, edgeId);
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
				staleReason: succeeded ? null : prevBinding.staleReason,
				checkpointable: succeeded && !state.checkpointRegistry?.[evt.nodeId]
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

export function hydrateFromRunSnapshotState(state: GraphState, snap: RunSnapshotLike): GraphState {
	if (typeof snap.graphId === 'string' && snap.graphId && snap.graphId !== state.graphId) {
		return state;
	}
	const rawCheckpointOutcomes = (() => {
		const snake = (snap as any)?.checkpoint_outcomes;
		if (snake && typeof snake === 'object') return snake as Record<string, string>;
		const camel = (snap as any)?.checkpointOutcomes;
		if (camel && typeof camel === 'object') return camel as Record<string, string>;
		return null;
	})();
	const checkpointOutcomes = rawCheckpointOutcomes
		? Object.entries(rawCheckpointOutcomes).reduce<Record<string, CheckpointStaleness>>((acc, [nodeId, raw]) => {
				const key = String(nodeId ?? '').trim();
				const status = String(raw ?? '').trim().toLowerCase();
				if (!key) return acc;
				if (
					status === 'valid' ||
					status === 'stale' ||
					status === 'artifact_missing' ||
					status === 'unknown'
				) {
					acc[key] = status as CheckpointStaleness;
				}
				return acc;
			}, {})
		: {};
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
	const checkpointRegistry = { ...(state.checkpointRegistry ?? {}) };
	let checkpointRegistryChanged = false;
	if (Object.keys(checkpointOutcomes).length > 0) {
		for (const [nodeId, staleness] of Object.entries(checkpointOutcomes)) {
			const existing = checkpointRegistry[nodeId];
			if (!existing) continue;
			if (String(existing.staleness ?? '') === String(staleness)) continue;
			checkpointRegistry[nodeId] = {
				...existing,
				staleness
			};
			checkpointRegistryChanged = true;
		}
	}
	return withGraphMeta({
		...state,
		runStatus,
		nodeBindings,
		nodeOutputs,
		activeRunMode: runMode,
		activeRunFrom: state.activeRunFrom,
		activeRunNodeSet,
		checkpointRegistry: checkpointRegistryChanged ? checkpointRegistry : state.checkpointRegistry
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

export function __resetRunUiStateForTest(state: GraphState): GraphState {
	return resetRunUiState(state);
}

export function resetEdgesExec(edges: Edge<PipelineEdgeData>[]): Edge<PipelineEdgeData>[] {
	return edges.map((e) => ({ ...e, data: { ...e.data, exec: 'idle' as EdgeExec } }));
}

export function resetRunUiState(state: GraphState): GraphState {
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
		activeRunNodeSet: new Set<string>(),
		runBlockedReason: null
	});
}



// RunManager factory

export type RunDeps = {
	update: (fn: (s: GraphState) => GraphState, ctx?: AuditContext) => void;
	getState: () => GraphState;
	persist: (state: GraphState) => void;
	applyLocalStaleInvalidation: (nodeId: string, reason?: string) => void;
	applyBackendAffectedStale: (affectedNodeIds: string[], rootNodeId: string) => void;
	applySourceRehydration: (nodeId: string, resolved: { execKey: string; artifactId: string | null; artifact?: { mimeType?: string; payloadType?: string } }) => void;
	syncAcceptParamsForNode: (nodeId: string, params: Record<string, any>, beforeExecParams: Record<string, unknown>) => Promise<void>;
	hydrateFromRunSnapshot: (state: GraphState, snap: RunSnapshotLike) => GraphState;
};

type SchemaGuardAssessment =
	| { kind: 'none' }
	| { kind: 'outside_run_path'; count: number }
	| {
			kind: 'in_run_path';
			errors: Array<{ nodeId: string; code?: string; message: string }>;
	  };

export function createRunManager(deps: RunDeps) {
	let activeRunStreamHandle: { runId: string; close: () => void } | null = null;
	let resumeFallbackPollTimer: ReturnType<typeof setTimeout> | null = null;
	const componentDraftGraphKey = '__graphDraft';
	const componentDraftLastCommittedCheckpointsKey = '__lastCommittedCheckpointRegistry';

	function draftCheckpointRegistryFromCacheEntry(entry: unknown): Record<string, unknown> {
		if (!entry || typeof entry !== 'object') return {};
		const graph = (entry as Record<string, unknown>)[componentDraftGraphKey];
		if (!graph || typeof graph !== 'object') return {};
		const checkpoints = (graph as Record<string, unknown>).checkpointRegistry;
		if (!checkpoints || typeof checkpoints !== 'object') return {};
		return checkpoints as Record<string, unknown>;
	}

	function committedCheckpointRegistryFromCacheEntry(entry: unknown): Record<string, unknown> {
		if (!entry || typeof entry !== 'object') return {};
		const checkpoints = (entry as Record<string, unknown>)[componentDraftLastCommittedCheckpointsKey];
		if (!checkpoints || typeof checkpoints !== 'object') return {};
		return checkpoints as Record<string, unknown>;
	}

	function collectComponentNodesWithUnsavedCheckpointChanges(state: GraphState): string[] {
		const out: string[] = [];
		const draftCache = (state.componentContractDraftCache ?? {}) as Record<string, unknown>;
		for (const node of state.nodes ?? []) {
			if (String((node as any)?.data?.kind ?? '').trim().toLowerCase() !== 'component') continue;
			const nodeId = String((node as any)?.id ?? '').trim();
			if (!nodeId) continue;
			const ref = (((node as any)?.data?.params ?? {}) as any)?.componentRef ?? {};
			const componentId = String(ref?.componentId ?? '').trim();
			const revisionId = String(ref?.revisionId ?? '').trim();
			if (!componentId || !revisionId) continue;
			const cacheKey = `${componentId}@${revisionId}`;
			const cacheEntry = draftCache[cacheKey];
			if (!cacheEntry || typeof cacheEntry !== 'object') continue;
			const draftRegistry = draftCheckpointRegistryFromCacheEntry(cacheEntry);
			const committedRegistry = committedCheckpointRegistryFromCacheEntry(cacheEntry);
			if (stableJson(draftRegistry) !== stableJson(committedRegistry)) {
				out.push(nodeId);
			}
		}
		return out;
	}

	function assessSchemaGuard(
		state: GraphState,
		runFrom: string | null,
		runMode: ActiveRunMode
	): SchemaGuardAssessment {
		const nodeSchemas = state.schemaPlane?.nodeSchemas ?? {};
		const allErrors = Object.entries(nodeSchemas)
			.filter((e): e is [string, Extract<SchemaPlaneResult, { ok: false }>] => {
				const r = e[1];
				return !!r && r.ok === false;
			})
			.map(([nodeId, result]) => ({
				nodeId,
				code: result.error.code,
				message: result.error.message
			}));
		if (allErrors.length <= 0) return { kind: 'none' };
		const plannedSet = computePlannedNodeSet(state.nodes, state.edges, runFrom, runMode);
		const inPath = allErrors.filter((item) => plannedSet.has(item.nodeId));
		if (inPath.length <= 0) return { kind: 'outside_run_path', count: allErrors.length };
		return { kind: 'in_run_path', errors: inPath };
	}

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
				const cur = deps.getState() as GraphState;
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
				deps.update((s) => applyRunEventState(s, evt, rid), auditCtx);
				if (evt.type === 'run_finished' || evt.type === 'run_paused') {
					sawTerminal = true;
				}
			}
			if (!sawTerminal || terminalReconciling || settled) return;
			terminalReconciling = true;
			const current = deps.getState() as GraphState;
			deps.persist(current);
			void getRun(rid)
				.then((snap) => {
					const latest = deps.getState() as GraphState;
					if (
						typeof snap.graphId === 'string' &&
						snap.graphId &&
						snap.graphId !== latest.graphId
					) {
						return;
					}
					deps.update((s) => deps.hydrateFromRunSnapshot(s, snap), {
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
				deps.update((s) => withGraphMeta(logPush({ ...s }, 'warn', 'Event stream error; reconciling run status')));
				void getRun(rid)
					.then((snap) => {
						const latest = deps.getState() as GraphState;
						if (
							typeof snap.graphId === 'string' &&
							snap.graphId &&
							snap.graphId !== latest.graphId
						) {
							settle();
							return;
						}
						deps.update((s) => deps.hydrateFromRunSnapshot(s, snap), {
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
							deps.update((s) =>
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
									const currentState = deps.getState() as GraphState;
									if (
										typeof nextSnap.graphId === 'string' &&
										nextSnap.graphId &&
										nextSnap.graphId !== currentState.graphId
									) {
										settle();
										return;
									}
									deps.update((s) => deps.hydrateFromRunSnapshot(s, nextSnap), {
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
										deps.update((s) =>
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
									deps.update((s) => deps.hydrateFromRunSnapshot(s, snap), {
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

	async function runRemote(
		runFrom: string | null,
		runMode?: ActiveRunMode,
		cacheMode?: 'default_on' | 'force_off' | 'force_on',
		adaptiveMode?: 'off' | 'observe' | 'enforce' | null,
		opts?: { allowUnsavedCheckpointChanges?: boolean; allowSchemaErrors?: boolean }
	) {
			// prevent concurrent runs
			const s0 = deps.getState() as GraphState;
			if (s0.runStatus === 'running' || s0.runStatus === 'pausing' || s0.runStatus === 'resuming') {
				return { ok: false as const, reason: 'run_already_active' as const };
			}

			const allowUnsavedCheckpointChanges = Boolean(opts?.allowUnsavedCheckpointChanges ?? false);
			const allowSchemaErrors = Boolean(opts?.allowSchemaErrors ?? false);
			const effectiveRunMode: ActiveRunMode = runMode ?? (runFrom ? 'from_selected_onward' : 'from_start');

			const schemaGuard = assessSchemaGuard(s0, runFrom, effectiveRunMode);
			if (schemaGuard.kind === 'outside_run_path') {
				deps.update((s) =>
					withGraphMeta(
						logPush(
							{ ...s },
							'warn',
							`Schema warning: ${schemaGuard.count} schema error${schemaGuard.count === 1 ? '' : 's'} exist outside the active run path.`
						)
					)
				);
			}
			if (schemaGuard.kind === 'in_run_path' && !allowSchemaErrors && s0.schemaWarningDismissCount < 3) {
				const message = `Schema validation found ${schemaGuard.errors.length} issue${schemaGuard.errors.length === 1 ? '' : 's'} in the active run path.`;
				deps.update((s) =>
					withGraphMeta(
						logPush(
							{
								...s,
								runBlockedReason: {
									type: 'schema_errors_in_run_path',
									nodeIds: schemaGuard.errors.map((item) => item.nodeId),
									message,
									errors: schemaGuard.errors
								}
							},
							'warn',
							message
						)
					)
				);
				return {
					ok: false as const,
					reason: 'schema_errors_in_run_path' as const,
					nodeIds: schemaGuard.errors.map((item) => item.nodeId),
					errors: schemaGuard.errors,
					message
				};
			}
			if (schemaGuard.kind === 'in_run_path' && allowSchemaErrors) {
				deps.update((s) =>
					withGraphMeta({
						...s,
						runBlockedReason: null,
						schemaWarningDismissCount: Math.min(3, Number(s.schemaWarningDismissCount ?? 0) + 1)
					})
				);
			}

			if (!allowUnsavedCheckpointChanges) {
				const blockedComponentNodeIds = collectComponentNodesWithUnsavedCheckpointChanges(s0);
				if (blockedComponentNodeIds.length > 0) {
					const message = `Unsaved checkpoint changes detected in ${blockedComponentNodeIds.length} component node${blockedComponentNodeIds.length === 1 ? '' : 's'}.`;
					deps.update((s) =>
						withGraphMeta(
							logPush(
								{
									...s,
									runBlockedReason: {
										type: 'unsaved_checkpoint_changes',
										componentNodeIds: blockedComponentNodeIds,
										message
									}
								},
								'warn',
								message
							)
						)
					);
					return {
						ok: false as const,
						reason: 'unsaved_checkpoint_changes' as const,
						componentNodeIds: blockedComponentNodeIds,
						message
					};
				}
			}

			// reset UI
			deps.update((s) => {
				const next = resetRunUiState(s);
				deps.persist(next);
				return next;
			}, { source: 'graph_edit' });
			deps.update((s) => withGraphMeta({ ...s, runStatus: 'running' }));

			// snapshot graph DTO
			const s1 = deps.getState() as GraphState;
			const dirtyNodeIds =
				effectiveRunMode === 'from_start'
					? Object.entries(s1.nodeBindings ?? {})
							.filter(([, binding]) => isBindingStale(binding))
							.map(([nodeId]) => nodeId)
					: [];
			const checkpoints = s1.checkpointRegistry;
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
					deps.update((s) =>
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
						deps.update((s) => deps.hydrateFromRunSnapshot(s, snap), {
							source: 'hydrate_snapshot',
							snapshotNodeIds: new Set(Object.keys(snap.nodeBindings ?? {}))
						});
					} catch {
						// non-fatal: stream events can still drive updates
					}
				} catch (e) {
					deps.update((s) =>
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
							const cur = deps.getState() as GraphState;
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
							deps.update((s) => {
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
						const current = deps.getState() as GraphState;
						deps.persist(current);
						void getRun(runId)
							.then((snap) => {
								const latest = deps.getState() as GraphState;
								if (
									typeof snap.graphId === 'string' &&
									snap.graphId &&
									snap.graphId !== latest.graphId
								) {
									return;
								}
								deps.update((s) => deps.hydrateFromRunSnapshot(s, snap), {
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
							const cur = deps.getState() as GraphState;
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
							deps.update((s) =>
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
										deps.update((s) => deps.hydrateFromRunSnapshot(s, snap), {
											source: 'hydrate_snapshot',
											snapshotNodeIds: new Set(Object.keys((snap as any)?.nodeBindings ?? {}))
										});
										deps.update((s) =>
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
									deps.update((s) =>
										withGraphMeta(logPush({ ...s }, 'warn', 'Event stream error; switching to fallback polling'))
									);
									void waitForTerminalViaSse(runId, {
										fallbackIntervalMs: 3000,
										fallbackMaxAttempts: 120,
										maxReconnectAttempts: 2,
										reconnectBackoffMs: 500
									})
										.then((snap) => {
											deps.update((s) => deps.hydrateFromRunSnapshot(s, snap.snap), {
												source: 'hydrate_snapshot',
												snapshotNodeIds: new Set(Object.keys((snap as any)?.snap?.nodeBindings ?? {}))
											});
											deps.update((s) =>
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
											deps.update((s) =>
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
									deps.update((s) =>
										withGraphMeta(logPush({ ...s }, 'warn', 'Event stream error; switching to fallback polling'))
									);
									void waitForTerminalViaSse(runId, {
										fallbackIntervalMs: 3000,
										fallbackMaxAttempts: 120,
										maxReconnectAttempts: 2,
										reconnectBackoffMs: 500
									})
										.then((snap) => {
											deps.update((s) => deps.hydrateFromRunSnapshot(s, snap.snap), {
												source: 'hydrate_snapshot',
												snapshotNodeIds: new Set(Object.keys((snap as any)?.snap?.nodeBindings ?? {}))
											});
											deps.update((s) =>
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
											deps.update((s) =>
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
					cacheMode,
					adaptiveMode ?? null,
					checkpoints
				);
				const plannedNodeSet = computePlannedNodeSet(s1.nodes, s1.edges, runFrom, effectiveRunMode);
				await runSingleWithStream(payload, plannedNodeSet);
				return { ok: true as const };
			}

			const allPlannedNodeSet = new Set<string>();
			for (const componentSet of componentPlans) {
				for (const nodeId of componentSet) allPlannedNodeSet.add(nodeId);
			}
			deps.update((s) =>
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
				deps.update((s) =>
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
						cacheMode,
						adaptiveMode ?? null,
						checkpoints
					);
					const created = await createRun(payload);
					const { snap, completionSource } = await waitForTerminalViaSse(created.runId, {
						fallbackIntervalMs: 3000,
						fallbackMaxAttempts: 120
					});
					deps.update(
						(s) => deps.hydrateFromRunSnapshot(s, snap),
						{
							source: 'hydrate_snapshot',
							snapshotNodeIds: new Set(Object.keys((snap as any)?.nodeBindings ?? {}))
						}
					);
					const status = String((snap as any)?.status ?? 'failed').toLowerCase();
					deps.update((s) =>
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
					deps.update((s) =>
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
			deps.update((s) =>
				withGraphMeta(
					logPush(
						{ ...s, runStatus: aggregateStatus, activeRunId: null },
						failed > 0 ? 'error' : 'info',
						`[subgraph summary] total=${componentPlans.length} succeeded=${succeeded} failed=${failed} canceled=${canceled} completion_source(sse=${sseRuntimeStats.sseTerminalCount},fallback=${sseRuntimeStats.fallbackTerminalCount}) polls=${sseRuntimeStats.fallbackPollAttempts}`
					)
				)
			);
			const current = deps.getState() as GraphState;
			deps.persist(current);
			return { ok: true as const };
	}

	return {
		actions: {
			runRemote,
			hardCancelActiveRuns: async function() {
				clearResumeFallbackPollTimer();
				try {
					await cancelAllRuns({ hard: true });
				} catch (error) {
					deps.update((s) => logPush(s, 'warn', `Cancel runs failed: ${String(error)}`));
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
			pauseActiveRun: async function() {
				const current = deps.getState();
				const runId = String(current.activeRunId ?? '').trim();
				if (!runId) return { ok: false, reason: 'missing_run_id' as const };
				if (current.runStatus !== 'running' && current.runStatus !== 'pausing') {
					return { ok: false, reason: 'run_not_running' as const };
				}
				try {
					await pauseRun(runId);
					deps.update((s) => withGraphMeta({ ...s, runStatus: 'pausing' }));
					return { ok: true as const };
				} catch (error) {
					deps.update((s) => logPush(s, 'error', `Pause run failed: ${String(error)}`));
					return { ok: false as const, reason: 'pause_failed' as const, error: String(error) };
				}
			},
			resumeActiveRun: async function() {
				const current = deps.getState();
				const runId = String(current.activeRunId ?? '').trim();
				if (!runId) return { ok: false, reason: 'missing_run_id' as const };
				if (current.runStatus !== 'paused' && current.runStatus !== 'resuming') {
					return { ok: false, reason: 'run_not_paused' as const };
				}
				try {
					await resumeRun(runId);
					deps.update((s) => withGraphMeta({ ...s, runStatus: 'resuming' }));
					attachActiveRunEventStream(runId);
					return { ok: true as const };
				} catch (error) {
					deps.update((s) => logPush(s, 'error', `Resume run failed: ${String(error)}`));
					return { ok: false as const, reason: 'resume_failed' as const, error: String(error) };
				}
			},
			resetRunUi() {
				deps.update((s) => {
					clearResumeFallbackPollTimer();
					const next = resetRunUiState(s);
					deps.persist(next);
					return next;
				}, { source: 'graph_edit' });
			},
			clearRunBlockedReason() {
				deps.update((s) => {
					if (!s.runBlockedReason) return s;
					return withGraphMeta({ ...s, runBlockedReason: null });
				});
			},
		},
	};
}
