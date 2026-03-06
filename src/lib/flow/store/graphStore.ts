// src/lib/flow/store/graphStore.ts
import { writable, get, derived } from 'svelte/store';
import type { Node, Edge } from '@xyflow/svelte';

import type {
	NodeStatus,
	NodeKind,
	PipelineNodeData,
	PipelineEdgeData,
	PipelineGraphDTO,
	PortType
} from '$lib/flow/types';
import { isPortType } from '$lib/flow/types/base';
import { defaultSourceParamsByKind } from '$lib/flow/schema/sourceDefaults';
import { defaultLlmParamsByKind } from '$lib/flow/schema/llmDefaults';
import { defaultTransformParamsByKind } from '$lib/flow/schema/transformDefaults';
import { defaultToolParamsByProvider, type ToolProvider } from '$lib/flow/schema/toolDefaults';
import { defaultNodeData } from '$lib/flow/schema/defaults';
import { updateNodeParamsValidated } from './graph';
import { saveGraphToLocalStorage, loadGraphFromLocalStorage, emptyGraph } from './persist';
import { acceptNodeParams, createRun, getRun, resolveSourceNode, streamRunEvents } from '$lib/flow/client/runs';
import type { KnownRunEvent } from '$lib/flow/types/run';
import type { SourceKind, LlmKind, TransformKind } from '$lib/flow/types/paramsMap';
import { getAllowedPortsForNode } from '$lib/flow/portCapabilities';
import {
	buildRunCreateRequest,
	computeGraphFreshness,
	computePlannedNodeSet,
	displayStatusFromBinding,
	getStaleFlipNodeIds,
	mergeBindingsSticky,
	type ActiveRunMode,
	type GraphFreshness as ScopeFreshness
} from './runScope';

type NodeOutputInfo = {
	mimeType?: string;
	portType?: string;
	preview?: string;
	cached?: boolean;
	cacheDecision?: 'cache_hit' | 'cache_miss' | 'cache_hit_contract_mismatch';
	expectedContractFingerprint?: string;
	actualContractFingerprint?: string;
	mismatchKind?: string;
	lastError?: NodeExecutionError | null;
};

export type NodeExecutionError = {
	message?: string;
	errorCode?: string;
	op?: string;
	paramPath?: string;
	missingColumns?: string[];
	availableColumns?: string[];
	availableColumnsSource?: 'schema' | 'inferred' | string;
};
type NodeBindingInfo = {
	status?: string;
	current?: { execKey?: string | null; artifactId?: string | null } | null;
	last?: { execKey?: string | null; artifactId?: string | null } | null;
	lastArtifactId?: string | null; // legacy
	lastRunId?: string | null;
	lastExecKey?: string | null; // legacy
	currentExecKey?: string | null; // legacy
	currentArtifactId?: string | null; // legacy
	currentRunId?: string | null;
	isUpToDate?: boolean;
	cacheValid?: boolean;
	staleReason?: string | null;
};
type BindingPair = { execKey: string | null; artifactId: string | null };
export type NormalizedNodeBinding = NodeBindingInfo & {
	status: string;
	isUpToDate: boolean;
	cacheValid: boolean;
	currentRunId: string | null;
	staleReason: string | null;
	current: BindingPair;
	last: BindingPair;
};
type EdgeExec = 'idle' | 'active' | 'done';
type LogLevel = 'info' | 'warn' | 'error';
type RunLog = {
	id: number; // âœ… ADD
	ts: string;
	level: LogLevel;
	message: string;
	nodeId?: string;
};
const RUN_IDLE = "idle"
type RunStatus = typeof RUN_IDLE | 'running' | 'succeeded' | 'failed' | 'canceled' | 'cancelled';
type GraphLastRunStatus = 'succeeded' | 'failed' | 'cancelled' | 'never_run';
type AuditContext = {
	source: 'event' | 'accept_params' | 'hydrate_snapshot' | 'graph_edit' | 'unknown';
	evt?: KnownRunEvent;
	allowedNodeIds?: Set<string>;
	snapshotNodeIds?: Set<string>;
	expectedDirtyTransition?: boolean;
};
type InspectorState = {
	nodeId: string | null;
	draftParams: Record<string, any>;
	dirty: boolean;
	uiByNodeId: Record<string, ApiEditorUiState>;
};

export type ApiEditorUiState = {
	requestOpen: boolean;
	authOpen: boolean;
	transportOpen: boolean;
	executionOpen: boolean;
	debugOpen: boolean;
	queryOpen: boolean;
	headersOpen: boolean;
	bodyOpen: boolean;
};

const IDLE: NodeStatus = 'idle';
const SUCCEEDED: NodeStatus = 'succeeded';
const allowedPorts = new Set(['table', 'text', 'json', 'binary', 'embeddings']);
const initialInspector: InspectorState = {
	nodeId: null,
	draftParams: {},
	dirty: false,
	uiByNodeId: {}
};

function hasAnyKeys(value: unknown): boolean {
	return Boolean(value && typeof value === 'object' && Object.keys(value as Record<string, unknown>).length > 0);
}

function defaultApiEditorUiState(params?: Record<string, any>): ApiEditorUiState {
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

let logSeq = 0;
const statusRegressionLogThrottle = new Map<string, number>();
const debugLastStatusChange = new Map<
	string,
	{
		ts: string;
		eventType: string;
		stack: string;
		prevDisplay: NodeStatus;
		nextDisplay: NodeStatus;
		prevNodeStatus?: NodeStatus;
		nextNodeStatus?: NodeStatus;
	}
>();
const DEV_MODE = (() => {
	try {
		return Boolean((import.meta as any)?.env?.DEV);
	} catch {
		return false;
	}
})();

function _pairFromLegacy(binding: NodeBindingInfo | undefined, which: 'current' | 'last') {
	const b = binding ?? {};
	if (which === 'current') {
		const hasStructured = Boolean(b.current && typeof b.current === 'object');
		let execKey = (b.current?.execKey ?? b.currentExecKey) ?? null;
		let artifactId = (b.current?.artifactId ?? b.currentArtifactId) ?? null;
		return {
			execKey: hasStructured ? execKey : (execKey ?? artifactId),
			artifactId: hasStructured ? artifactId : (artifactId ?? execKey)
		};
	}
	const hasStructured = Boolean(b.last && typeof b.last === 'object');
	let execKey = (b.last?.execKey ?? b.lastExecKey) ?? null;
	let artifactId = (b.last?.artifactId ?? b.lastArtifactId) ?? null;
	return {
		execKey: hasStructured ? execKey : (execKey ?? artifactId),
		artifactId: hasStructured ? artifactId : (artifactId ?? execKey)
	};
}

function _withPair(binding: NormalizedNodeBinding, which: 'current' | 'last', pair: BindingPair): NormalizedNodeBinding {
	const next: NormalizedNodeBinding = { ...binding };
	if (which === 'current') {
		next.current = {
			execKey: pair.execKey,
			artifactId: pair.artifactId
		};
		next.currentExecKey = pair.execKey;
		next.currentArtifactId = pair.artifactId;
	} else {
		next.last = {
			execKey: pair.execKey,
			artifactId: pair.artifactId
		};
		next.lastExecKey = pair.execKey;
		next.lastArtifactId = pair.artifactId;
	}
	return next;
}

function _assertBindingPairInvariant(
	binding: NodeBindingInfo | undefined,
	nodeId: string,
	context: string,
	force = false
): void {
	if ((!DEV_MODE && !force) || !binding) return;
	for (const which of ['current', 'last'] as const) {
		const pair = _pairFromLegacy(binding, which);
		const hasExec = Boolean(pair.execKey);
		const hasArt = Boolean(pair.artifactId);
		if (hasExec !== hasArt) {
			throw new Error(`[graphStore] INVALID_BINDING_PAIR ${context} node=${nodeId} pair=${which}`);
		}
	}
}

export function __assertBindingPairForTest(binding: NodeBindingInfo, nodeId = 'test', context = 'test'): void {
	_assertBindingPairInvariant(binding, nodeId, context, true);
}

function _normalizeBinding(binding: NodeBindingInfo | undefined, nodeId?: string): NormalizedNodeBinding {
	const b = { ...(binding ?? {}) };
	const hasCurrentFields =
		Object.prototype.hasOwnProperty.call(b, 'current') ||
		Object.prototype.hasOwnProperty.call(b, 'currentExecKey') ||
		Object.prototype.hasOwnProperty.call(b, 'currentArtifactId');
	const hasLastFields =
		Object.prototype.hasOwnProperty.call(b, 'last') ||
		Object.prototype.hasOwnProperty.call(b, 'lastExecKey') ||
		Object.prototype.hasOwnProperty.call(b, 'lastArtifactId');
	if (hasCurrentFields) b.current = _pairFromLegacy(b, 'current');
	if (hasLastFields) b.last = _pairFromLegacy(b, 'last');
	const current = (b.current as BindingPair | undefined) ?? { execKey: null, artifactId: null };
	const last = (b.last as BindingPair | undefined) ?? { execKey: null, artifactId: null };
	const normalized: NormalizedNodeBinding = {
		...b,
		status: String(b.status ?? 'idle'),
		isUpToDate: Boolean(b.isUpToDate ?? false),
		cacheValid: Boolean(b.cacheValid ?? false),
		currentRunId: (b.currentRunId ?? null) as string | null,
		staleReason: (b.staleReason ?? null) as string | null,
		current: {
			execKey: current.execKey ?? null,
			artifactId: current.artifactId ?? null
		},
		last: {
			execKey: last.execKey ?? null,
			artifactId: last.artifactId ?? null
		}
	};
	normalized.currentExecKey = normalized.current.execKey;
	normalized.currentArtifactId = normalized.current.artifactId;
	normalized.lastExecKey = normalized.last.execKey;
	normalized.lastArtifactId = normalized.last.artifactId;
	if (nodeId) _assertBindingPairInvariant(normalized, nodeId, 'normalize');
	return normalized;
}

export function __normalizeBindingForTest(
	binding: NodeBindingInfo | undefined,
	nodeId = 'test'
): NormalizedNodeBinding {
	return _normalizeBinding(binding, nodeId);
}

export type GraphState = {
	graphId: string;
	nodes: Node<PipelineNodeData & Record<string, unknown>>[];
	edges: Edge<PipelineEdgeData & Record<string, unknown>>[];
	selectedNodeId: string | null;
	inspector: InspectorState; // âœ… add this
	logs: RunLog[];
	runStatus: RunStatus;
	lastRunStatus: GraphLastRunStatus;
	freshness: ScopeFreshness;
	staleNodeCount: number;
	activeRunMode: ActiveRunMode;
	activeRunFrom: string | null;
	activeRunNodeSet: Set<string>;
	nodeOutputs: Record<string, NodeOutputInfo>;
	nodeBindings: Record<string, NormalizedNodeBinding>;
	activeRunId: string | null;
};

export type InputResolution = {
	inPort: string;
	edge: { fromNodeId: string; fromPort: string } | null;
	status: 'resolved' | 'missing';
	reason?: 'DISCONNECTED' | 'UPSTREAM_NO_ARTIFACT' | 'UPSTREAM_FAILED' | 'UNKNOWN';
	artifactId?: string;
	artifactSource?: 'active_run' | 'bound';
	upstream: {
		nodeId: string;
		port: string;
		status?: string;
		isUpToDate?: boolean;
		staleReason?: string | null;
	};
	artifactSummary?: {
		mimeType?: string;
		schemaFingerprint?: string;
		contract?: string;
	};
};

function normalizeHandleId(handleId: string | null | undefined, fallback: 'in' | 'out'): string {
	const v = String(handleId ?? '').trim();
	return v ? v : fallback;
}

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
	const inPortType = node.data?.ports?.in ?? null;
	const hasInputPort = inPortType !== null && inPortType !== undefined;
	const incoming = (state.edges ?? [])
		.filter((e) => e.target === nodeId)
		.slice()
		.sort((a, b) => String(a.id ?? '').localeCompare(String(b.id ?? '')));
	const inPorts = new Set<string>();
	if (hasInputPort) inPorts.add('in');
	for (const e of incoming) inPorts.add(normalizeHandleId((e as any).targetHandle, 'in'));
	const orderedInPorts = Array.from(inPorts).sort((a, b) => a.localeCompare(b));
	const resolutions: InputResolution[] = [];
	for (const inPort of orderedInPorts) {
		const edge = incoming.find((e) => normalizeHandleId((e as any).targetHandle, 'in') === inPort) ?? null;
		if (!edge) {
			resolutions.push({
				inPort,
				edge: null,
				status: 'missing',
				reason: 'DISCONNECTED',
				upstream: { nodeId: '', port: '' }
			});
			continue;
		}
		const fromNodeId = String(edge.source ?? '');
		const fromPort = normalizeHandleId((edge as any).sourceHandle, 'out');
		const upstreamBinding = state.nodeBindings?.[fromNodeId];
		const upstreamOut = state.nodeOutputs?.[fromNodeId];
		const resolved = resolveUpstreamArtifact(state, upstreamBinding);
		if (resolved.artifactId) {
			resolutions.push({
				inPort,
				edge: { fromNodeId, fromPort },
				status: 'resolved',
				artifactId: resolved.artifactId,
				artifactSource: resolved.artifactSource,
				upstream: {
					nodeId: fromNodeId,
					port: fromPort,
					status: displayStatusFromBinding(upstreamBinding as any),
					isUpToDate: upstreamBinding?.isUpToDate,
					staleReason: upstreamBinding?.staleReason ?? null
				},
				artifactSummary: {
					mimeType: upstreamOut?.mimeType,
					schemaFingerprint: upstreamOut?.actualContractFingerprint,
					contract: upstreamOut?.portType
				}
			});
			continue;
		}
		resolutions.push({
			inPort,
			edge: { fromNodeId, fromPort },
			status: 'missing',
			reason: isFailedBindingStatus(upstreamBinding) ? 'UPSTREAM_FAILED' : 'UPSTREAM_NO_ARTIFACT',
			upstream: {
				nodeId: fromNodeId,
				port: fromPort,
				status: displayStatusFromBinding(upstreamBinding as any),
				isUpToDate: upstreamBinding?.isUpToDate,
				staleReason: upstreamBinding?.staleReason ?? null
			}
		});
	}
	return resolutions;
}

function ensureNormalizedBindingsForNodes(
	nodes: Node<PipelineNodeData & Record<string, unknown>>[],
	nodeBindings: Record<string, NormalizedNodeBinding>
): Record<string, NormalizedNodeBinding> {
	const liveNodeIds = new Set((nodes ?? []).map((n) => n.id).filter(Boolean));
	const next: Record<string, NormalizedNodeBinding> = {};
	for (const [nodeId, binding] of Object.entries(nodeBindings ?? {})) {
		if (!liveNodeIds.has(nodeId)) continue;
		next[nodeId] = binding;
	}
	for (const node of nodes ?? []) {
		if (!node?.id) continue;
		next[node.id] = _normalizeBinding(next[node.id], node.id);
	}
	return next;
}

function pruneNodeOutputsForNodes(
	nodes: Node<PipelineNodeData & Record<string, unknown>>[],
	nodeOutputs: Record<string, NodeOutputInfo>
): Record<string, NodeOutputInfo> {
	const liveNodeIds = new Set((nodes ?? []).map((n) => n.id).filter(Boolean));
	const next: Record<string, NodeOutputInfo> = {};
	for (const [nodeId, output] of Object.entries(nodeOutputs ?? {})) {
		if (!liveNodeIds.has(nodeId)) continue;
		next[nodeId] = output;
	}
	return next;
}

function mintGraphId(): string {
	try {
		return `graph_${crypto.randomUUID()}`;
	} catch {
		return `graph_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
	}
}

function nowTs() {
	return new Date().toLocaleTimeString();
}

function logPush(state: GraphState, level: LogLevel, message: string, nodeId?: string) {
	logSeq += 1;
	return {
		...state,
		logs: [...state.logs, { id: logSeq, ts: nowTs(), level, message, nodeId }]
	};
}

function captureStack(label: string): string {
	try {
		return new Error(label).stack ?? '';
	} catch {
		return '';
	}
}

function isAllowedSucceededRegression(nodeId: string, ctx: AuditContext): boolean {
	if (ctx.source === 'accept_params') {
		if (ctx.expectedDirtyTransition) return true;
		return Boolean(ctx.allowedNodeIds?.has(nodeId));
	}
	if (ctx.source === 'hydrate_snapshot') return false;
	if (ctx.source === 'graph_edit') return true;
	if (ctx.source !== 'event' || !ctx.evt) return false;
	const evt = ctx.evt;
	if (
		(evt.type === 'node_started' || evt.type === 'node_finished' || evt.type === 'cache_decision') &&
		evt.nodeId === nodeId
	) {
		if (evt.type === 'cache_decision') return evt.decision !== 'cache_hit';
		return true;
	}
	return false;
}

function logSucceededRegression(
	channel: 'binding' | 'node_data',
	nodeId: string,
	prev: GraphState,
	next: GraphState,
	ctx: AuditContext,
	prevDisplay: NodeStatus,
	nextDisplay: NodeStatus,
	prevNodeStatus?: NodeStatus,
	nextNodeStatus?: NodeStatus
): void {
	const eventType = ctx.evt?.type ?? ctx.source;
	const stack = captureStack(`[graphStore] ${channel} succeeded regression ${nodeId}`);
	const payload = {
		channel,
		eventType,
		event: ctx.evt ?? null,
		nodeId,
		prevDisplay,
		nextDisplay,
		prevNodeStatus,
		nextNodeStatus,
		prevBinding: prev.nodeBindings?.[nodeId] ?? null,
		nextBinding: next.nodeBindings?.[nodeId] ?? null,
		prevOutput: prev.nodeOutputs?.[nodeId] ?? null,
		nextOutput: next.nodeOutputs?.[nodeId] ?? null,
		activeRunId: next.activeRunId,
		activeRunMode: next.activeRunMode,
		activeRunFrom: next.activeRunFrom,
		activeRunNodeSetSize: next.activeRunNodeSet?.size ?? 0,
		stack
	};

	const key = `${channel}:${eventType}:${nodeId}`;
	const now = Date.now();
	const last = statusRegressionLogThrottle.get(key) ?? 0;
	if (DEV_MODE || now - last > 2000) {
		console.error('[graphStore] SUCCEEDED_REGRESSION', payload);
		statusRegressionLogThrottle.set(key, now);
	}
	debugLastStatusChange.set(nodeId, {
		ts: new Date().toISOString(),
		eventType,
		stack,
		prevDisplay,
		nextDisplay,
		prevNodeStatus,
		nextNodeStatus
	});
}

function auditSucceededRegressions(prev: GraphState, next: GraphState, ctx: AuditContext): void {
	if (ctx.source === 'hydrate_snapshot') return;
	const ids = new Set([...Object.keys(prev.nodeBindings ?? {}), ...Object.keys(next.nodeBindings ?? {})]);
	for (const nodeId of ids) {
		const prevDisplay = displayStatusFromBinding(prev.nodeBindings?.[nodeId]);
		const nextDisplay = displayStatusFromBinding(next.nodeBindings?.[nodeId]);
		if (prevDisplay !== SUCCEEDED || nextDisplay === SUCCEEDED) continue;
		if (nextDisplay === 'running') continue;
		if (nextDisplay === 'idle' && !next.nodeBindings?.[nodeId]) continue;
		const allowed = isAllowedSucceededRegression(nodeId, ctx);
		if (allowed) {
			if (DEV_MODE) {
				console.debug('[graphStore] EXPECTED_DIRTY_TRANSITION', {
					nodeId,
					source: ctx.source,
					eventType: ctx.evt?.type ?? ctx.source,
					prevDisplay,
					nextDisplay
				});
			}
			continue;
		}
		logSucceededRegression('binding', nodeId, prev, next, ctx, prevDisplay, nextDisplay);
		if (DEV_MODE && !allowed) {
			throw new Error(`SUCCEEDED_REGRESSION(binding): node=${nodeId}, source=${ctx.source}`);
		}
	}
}

function assertHydrationBindingInvariants(prev: GraphState, next: GraphState, ctx: AuditContext): void {
	if (!DEV_MODE || ctx.source !== 'hydrate_snapshot') return;
	const prevBindings = prev.nodeBindings ?? {};
	const nextBindings = next.nodeBindings ?? {};
	const patchIds = ctx.snapshotNodeIds ?? new Set<string>();
	const dropped = Object.keys(prevBindings).filter((id) => !nextBindings[id]);
	if (dropped.length > 0) {
		console.error('[graphStore] BINDING_DROPPED_DURING_HYDRATION', {
			droppedNodeIds: dropped,
			patchNodeIds: Array.from(patchIds)
		});
		throw new Error(`BINDING_DROPPED_DURING_HYDRATION: ${dropped.join(',')}`);
	}
	for (const [id, prevBinding] of Object.entries(prevBindings)) {
		if (patchIds.has(id)) continue;
		const nextBinding = nextBindings[id];
		const same = JSON.stringify(prevBinding) === JSON.stringify(nextBinding);
		if (!same) {
			console.error('[graphStore] OUT_OF_SCOPE_BINDING_MUTATED_DURING_HYDRATION', {
				nodeId: id,
				prevBinding,
				nextBinding,
				patchNodeIds: Array.from(patchIds)
			});
			throw new Error(`OUT_OF_SCOPE_BINDING_MUTATED_DURING_HYDRATION: ${id}`);
		}
	}
}

function auditStateTransition(prev: GraphState, next: GraphState, ctx: AuditContext): void {
	assertHydrationBindingInvariants(prev, next, ctx);
	auditSucceededRegressions(prev, next, ctx);
}

function withGraphMeta(state: GraphState): GraphState {
	const normalizedBindings = ensureNormalizedBindingsForNodes(state.nodes, state.nodeBindings ?? {});
	const normalizedOutputs = pruneNodeOutputsForNodes(state.nodes, state.nodeOutputs ?? {});
	const { freshness, staleNodeCount } = computeGraphFreshness(normalizedBindings ?? {});
	let lastRunStatus = state.lastRunStatus;
	if (state.runStatus === 'succeeded') lastRunStatus = 'succeeded';
	if (state.runStatus === 'failed') lastRunStatus = 'failed';
	if (state.runStatus === 'canceled' || state.runStatus === 'cancelled') lastRunStatus = 'cancelled';
	if (freshness === 'never_run') lastRunStatus = 'never_run';
	return {
		...state,
		freshness,
		staleNodeCount,
		lastRunStatus,
		nodeBindings: normalizedBindings,
		nodeOutputs: normalizedOutputs
	};
}

function canApplyNodeEvent(state: GraphState, nodeId: string, evtRunId?: string): boolean {
	if (!nodeId) return false;
	if (!state.activeRunId) return true;
	if (evtRunId && evtRunId !== state.activeRunId) return false;
	// Event streams are run-scoped by runId; do not drop valid per-node events based on
	// planned sets because scheduler/runtime may execute additional upstream nodes.
	return true;
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

function stableJson(value: unknown): string {
	try {
		return JSON.stringify(value ?? null);
	} catch {
		return '';
	}
}

function downstreamNodeIds(edges: Edge<PipelineEdgeData>[], nodeId: string): Set<string> {
	const out = new Set<string>([nodeId]);
	const q = [nodeId];
	while (q.length > 0) {
		const cur = q.shift()!;
		for (const e of edges) {
			if (e.source !== cur) continue;
			const nxt = String(e.target ?? '');
			if (!nxt || out.has(nxt)) continue;
			out.add(nxt);
			q.push(nxt);
		}
	}
	return out;
}

export function __markStaleFromNodeForTest(state: GraphState, nodeId: string): GraphState {
	const candidateIds = downstreamNodeIds(state.edges, nodeId);
	const nodeBindings = { ...state.nodeBindings };
	let changed = false;
	for (const affectedId of candidateIds) {
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

function effectiveExecParamsForNode(node: Node<PipelineNodeData> | undefined): Record<string, unknown> {
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

function committedNodeParamsForNode(
	state: GraphState,
	nodeId: string
): Record<string, any> {
	const node = state.nodes.find((x) => x.id === nodeId);
	return { ...((node?.data?.params ?? {}) as Record<string, any>) };
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

function assertNoRunStartedStaleFlips(prev: GraphState, next: GraphState): void {
	if (!DEV_MODE) return;
	const flips = getStaleFlipNodeIds(prev.nodeBindings ?? {}, next.nodeBindings ?? {});
	if (flips.length === 0) return;
	console.error('[graphStore] stale flip detected on run_started', {
		nodeIds: flips,
		activeRunId: next.activeRunId,
		runMode: next.activeRunMode,
		runFrom: next.activeRunFrom,
		activeRunNodeSet: next.activeRunNodeSet ? Array.from(next.activeRunNodeSet) : []
	});
}

function assertRunStartedNoBindingTouch(prev: GraphState, next: GraphState): void {
	if (!DEV_MODE) return;
	const changed = changedBindingNodeIds(prev.nodeBindings ?? {}, next.nodeBindings ?? {});
	if (changed.length === 0) return;
	const outOfScope = changed.filter((id) => !next.activeRunNodeSet?.has(id));
	console.error('[graphStore] run_started mutated nodeBindings (forbidden)', {
		changedNodeIds: changed,
		outOfScopeNodeIds: outOfScope,
		activeRunId: next.activeRunId,
		runMode: next.activeRunMode,
		runFrom: next.activeRunFrom,
		activeRunNodeSet: next.activeRunNodeSet ? Array.from(next.activeRunNodeSet) : [],
		bindingsBefore: changed.reduce(
			(acc, id) => ({ ...acc, [id]: prev.nodeBindings?.[id] ?? null }),
			{} as Record<string, NormalizedNodeBinding | null>
		),
		bindingsAfter: changed.reduce(
			(acc, id) => ({ ...acc, [id]: next.nodeBindings?.[id] ?? null }),
			{} as Record<string, NormalizedNodeBinding | null>
		)
	});
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
					portType: evt.portType,
					preview: evt.preview ?? undefined,
					cached: evt.cached ?? false,
					cacheDecision: nextCacheDecision
				}
			};
			return withGraphMeta({
				...state,
				nodeOutputs,
				nodeBindings
			});
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
				portType: prev?.portType,
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
			return withGraphMeta(
				logPush(
					{
						...state,
						activeRunId: evt.runId ?? state.activeRunId,
						activeRunMode: evtMode,
						activeRunFrom: evt.runFrom ?? state.activeRunFrom,
						activeRunNodeSet: evtPlanned
					},
					'info',
					`Run started ${evt.runFrom ? `(from ${evt.runFrom})` : '(from start)'}`
				)
			);
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
			return withGraphMeta(logPush({ ...state, nodeBindings, nodeOutputs }, 'info', 'Node started', evt.nodeId));
		}
		case 'edge_exec': {
			const edges = state.edges.map((e) =>
				e.id === evt.edgeId ? { ...e, data: { ...(e.data ?? {}), exec: evt.exec } } : e
			);
			return { ...state, edges };
		}
		case 'log':
			return logPush(state, evt.level, evt.message, evt.nodeId);
		case 'node_finished': {
			if (!canApplyNodeEvent(state, evt.nodeId, evt.runId)) return state;
			const prevBinding = _normalizeBinding(state.nodeBindings?.[evt.nodeId], evt.nodeId);
			const succeeded = evt.status === 'succeeded';
			const errorDetails = (evt as any).errorDetails as Record<string, unknown> | undefined;
			const errorPayload: NodeExecutionError | null = succeeded
				? null
				: {
					message: evt.error ? String(evt.error) : undefined,
					errorCode: typeof (evt as any).errorCode === 'string' ? String((evt as any).errorCode) : undefined,
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
			return withGraphMeta(
				logPush(
					{ ...state, nodeBindings, nodeOutputs },
					'info',
					`Node finished (${displayStatusFromBinding(nextBinding)})`,
					evt.nodeId
				)
			);
		}
		case 'run_finished': {
			return withGraphMeta(
				logPush({ ...state, runStatus: evt.status }, 'info', `Run finished (${evt.status})`)
			);
		}
		default:
			return state;
	}
}

type RunSnapshotLike = {
	graphId?: string;
	status?: string;
	runMode?: ActiveRunMode;
	plannedNodeIds?: string[];
	nodeStatus?: Record<string, string>;
	nodeOutputs?: Record<string, string>;
	nodeBindings?: Record<string, Record<string, unknown>>;
};

function hydrateFromRunSnapshotState(state: GraphState, snap: RunSnapshotLike): GraphState {
	if (typeof snap.graphId === 'string' && snap.graphId && snap.graphId !== state.graphId) {
		return state;
	}
	const nodeBindingsPatch: Record<string, NormalizedNodeBinding> = {};
	const nodeOutputs: Record<string, NodeOutputInfo> = { ...(state.nodeOutputs ?? {}) };
	for (const [nodeId, raw] of Object.entries(snap.nodeBindings ?? {})) {
		const b = _normalizeBinding(raw as NodeBindingInfo, nodeId);
		nodeBindingsPatch[nodeId] = b;
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
	return reduceRunEventState(state, evt, runId);
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
		inspector: initialInspector,
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
		activeRunId: null
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
	const dto: PipelineGraphDTO = { version: 1, nodes, edges };
	if (graphId) {
		dto.meta = { ...(dto.meta ?? {}), graphId } as any;
	}
	return dto;
}

function getPortType(
	nodes: Node<PipelineNodeData>[],
	sourceId: string,
	whichPort: 'in' | 'out'
): PortType | null {
	const n = nodes.find((x) => x.id === sourceId);
	if (!n) return null;
	return (n.data.ports?.[whichPort as 'in' | 'out'] ?? null) as PortType | null;
}

function sourcePayloadHint(node: Node<PipelineNodeData>, whichPort: 'in' | 'out') {
	const port = node.data.ports?.[whichPort] ?? null;
	if (port === 'table') {
		let columns: string[] | undefined;
		if (node.data.kind === 'source') {
			const params: any = node.data.params ?? {};
			const sourceKind = node.data.sourceKind;
			if (sourceKind === 'file') {
				const fileFormat = String(params?.file_format ?? 'csv').toLowerCase();
				if (fileFormat === 'txt') columns = ['text'];
				if (fileFormat === 'pdf') {
					columns = ['page_number', 'text', 'has_tables', 'table_count', 'tables'];
				}
			}
		}
		return columns ? { type: 'table', columns } : { type: 'table' };
	}
	if (port === 'json') return { type: 'json' };
	if (port === 'text') return { type: 'string' };
	if (port === 'binary') return { type: 'binary' };
	return undefined;
}

function targetPayloadHint(node: Node<PipelineNodeData>) {
	if (node.data.kind !== 'transform') return sourcePayloadHint(node, 'in');

	const params: any = node.data.params ?? {};
	const op = params?.op ?? node.data.transformKind;
	if (op !== 'select') return sourcePayloadHint(node, 'in');

	const cols = params?.select?.columns;
	if (!Array.isArray(cols) || cols.length === 0) return sourcePayloadHint(node, 'in');

	return { type: 'table', required_columns: cols };
}

type EdgeInvalidReason =
	| 'missing_port_type' // couldn't resolve out/in
	| 'type_mismatch'; // outType !== in
type EdgeCheck =
	| { ok: true; out?: PortType; in?: PortType }
	| { ok: false; reason: EdgeInvalidReason };

function isEdgeStillValid(nodes: Node<PipelineNodeData>[], e: Edge<PipelineEdgeData>): EdgeCheck {
	const outPort = getPortType(nodes, e.source, 'out');
	const inPort = getPortType(nodes, e.target, 'in');

	if (outPort == null || inPort == null) {
		return { ok: false, reason: 'missing_port_type' };
	}

	if (outPort !== inPort) {
		return { ok: false, reason: 'type_mismatch' };
	}

	return { ok: true, out: outPort, in: inPort };
}

function resetEdgesExec(edges: Edge<PipelineEdgeData>[]): Edge<PipelineEdgeData>[] {
	return edges.map((e) => ({ ...e, data: { ...e.data, exec: 'idle' as EdgeExec } }));
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
			if (chk.reason === 'type_mismatch') {
				// allowed prune
				prunedIds.push(e.id);
				continue;
			}

			// NOT allowed to silently prune: graph invariants broken
			return {
				ok: false,
				error: `Edge ${e.id} has unresolved port types (source=${e.source}:${e.sourceHandle ?? 'out'} target=${e.target}:${e.targetHandle ?? 'in'})`
			};
		}

		next.push({
			...e,
			data: {
				...(e.data ?? {}),
				exec: e.data?.exec ?? 'idle',
				contract: {
					out: chk.out,
					in: chk.in,
					payload: {
						source: sourcePayloadHint(nodes.find((n) => n.id === e.source)! as any, 'out'),
						target: targetPayloadHint(nodes.find((n) => n.id === e.target)! as any)
					}
				}
			}
		});
	}

	return { ok: true, edges: next, prunedIds };
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

const initialState: GraphState = {
	graphId: String((loaded as any)?.meta?.graphId ?? mintGraphId()),
	nodes: loaded.nodes,
	edges: loaded.edges,
	selectedNodeId: null,
	inspector: initialInspector,
	logs: [],
	runStatus: IDLE,
	lastRunStatus: 'never_run',
	freshness: 'never_run',
	staleNodeCount: 0,
	activeRunMode: 'from_start',
	activeRunFrom: null,
	activeRunNodeSet: new Set<string>(),
	nodeOutputs: {},
	nodeBindings: ensureNormalizedBindingsForNodes(loaded.nodes, {}),
	activeRunId: null
};

export const graphStore = (() => {
	const { subscribe, set, update: rawUpdate } = writable<GraphState>(initialState);

	const update = (
		recipe: (state: GraphState) => GraphState,
		ctx: AuditContext = { source: 'unknown' }
	) =>
		rawUpdate((state) => {
			const next = recipe(state);
			auditStateTransition(state, next, ctx);
			return next;
		});

	function updateNodeConfigImpl(
		nodeId: string,
		config: { params?: unknown; ports?: { in?: PortType | null; out?: PortType | null } }
	) {
		let out: { ok: boolean; error?: string; removedEdgeIds?: string[] } = { ok: true };

		update((s) => {
			let nodes = s.nodes;
			let edges = s.edges;

			// 0) Ensure node exists
			const node = nodes.find((n) => n.id === nodeId);
			if (!node) {
				out = { ok: false, error: 'Node not found' };
				return logPush(s, 'warn', out.error!, nodeId);
			}

			// ---- 1) params (must be valid to commit) ----
			if (config.params !== undefined) {
				const res = updateNodeParamsValidated(nodes, nodeId, config.params);
				if (res.error) {
					out = { ok: false, error: res.error };
					return logPush(s, 'error', res.error, nodeId);
				}
				nodes = res.nodes;
			}

			// ---- 2) ports (must be valid to commit) ----
			if (config.ports) {
				const { in: inPort, out: outPort } = config.ports;

				if (inPort !== undefined && inPort !== null && !isPortType(inPort)) {
					out = { ok: false, error: `Invalid input port type: ${String(inPort)}` };
					return logPush(s, 'warn', out.error!, nodeId);
				}
				if (outPort !== undefined && outPort !== null && !isPortType(outPort)) {
					out = { ok: false, error: `Invalid output port type: ${String(outPort)}` };
					return logPush(s, 'warn', out.error!, nodeId);
				}

				const allowedIn = getAllowedPortsForNode(node as any, 'in');
				const allowedOut = getAllowedPortsForNode(node as any, 'out');
				if (inPort !== undefined && inPort !== null && !allowedIn.includes(inPort)) {
					out = {
						ok: false,
						error: `${node.data.kind} input port '${String(inPort)}' is not supported`
					};
					return logPush(s, 'warn', out.error!, nodeId);
				}
				if (outPort !== undefined && outPort !== null && !allowedOut.includes(outPort)) {
					out = {
						ok: false,
						error: `${node.data.kind} output port '${String(outPort)}' is not supported`
					};
					return logPush(s, 'warn', out.error!, nodeId);
				}

				// capture connectivity BEFORE changes
				const incoming = edges.filter((e) => e.target === nodeId);
				const outgoing = edges.filter((e) => e.source === nodeId);

				nodes = nodes.map((n) => {
					if (n.id !== nodeId) return n;

					const curPorts = n.data.ports ?? {};
					const mergedPorts = {
						in: inPort !== undefined ? inPort : (curPorts.in ?? null),
						out: outPort !== undefined ? outPort : (curPorts.out ?? null)
					};

					return {
						...n,
						data: {
							...n.data,
							ports: mergedPorts,
							meta: { ...(n.data.meta ?? {}), updatedAt: new Date().toISOString() }
						}
					};
				});

				// NOW read what we actually set
				const updatedNode = nodes.find((n) => n.id === nodeId)!;
				const pin = updatedNode.data.ports?.in ?? null;
				const pout = updatedNode.data.ports?.out ?? null;

				// Invariant: cannot null a port that is currently used by edges
				if (incoming.length > 0 && pin == null) {
					out = {
						ok: false,
						error: 'Cannot set input port to null while node has incoming edges.'
					};
					return logPush(s, 'warn', out.error!, nodeId);
				}
				if (outgoing.length > 0 && pout == null) {
					out = {
						ok: false,
						error: 'Cannot set output port to null while node has outgoing edges.'
					};
					return logPush(s, 'warn', out.error!, nodeId);
				}

				const pr = pruneAndRecontractEdgesStrict(nodes, edges);
				if (pr.ok === false) {
					out = { ok: false, error: pr.error };
					return logPush(s, 'warn', pr.error, nodeId);
				}
				edges = pr.edges;
				if (pr.prunedIds?.length) out.removedEdgeIds = pr.prunedIds;
			}

			const next = logPush({ ...s, nodes, edges }, 'info', 'Node config updated', nodeId);
			persist(next);
			return next;
		});

		return out;
	}

	type UpdateNodeConfig = {
		params?: unknown;
		ports?: {
			in?: PortType | null; // apply to all input handles
			out?: PortType | null; // apply to all output handles
		};
	};

	type PreviewUpdateResult =
		| {
			ok: true;
			prunedEdgeIds: string[];
			nextNodes: Node<PipelineNodeData>[];
			nextEdges: Edge<PipelineEdgeData>[];
		}
		| { ok: false; error: string };

	//BEGIN
	function patchInspectorDraft(patch: Record<string, any>) {
		update((s) => {
			if (!s.inspector.nodeId) return s;
			return {
				...s,
				inspector: {
					...s.inspector,
					draftParams: { ...s.inspector.draftParams, ...patch },
					dirty: true
				}
			};
		});
	}

	// optional: dropdown commit (keeps draft consistent + commits)
	async function commitInspectorImmediate(patch: Record<string, any>) {
		const s = get({ subscribe } as any) as GraphState;
		const nodeId = s.inspector.nodeId;
		if (!nodeId) return { ok: false, error: 'No node selected' };
		if (patch?.op === 'dedupe' || patch?.dedupe || s.inspector.draftParams?.op === 'dedupe') {
			console.log('[dedupe-store] commitInspectorImmediate:patch', {
				nodeId,
				patch,
				draftParams: s.inspector.draftParams
			});
		}
		const beforeNode = s.nodes.find((x) => x.id === nodeId);
		const beforeExecParams = effectiveExecParamsForNode(beforeNode);

		// 2) commit patch (validated/stripped)
		const result = updateNodeConfigImpl(nodeId, { params: patch });
		if (!result.ok) return result;

		const afterState = get({ subscribe } as any) as GraphState;
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

		await syncAcceptParamsForNode(nodeId, paramsForSubmit, beforeExecParams);
		return result;
	}

	async function commitSnapshotSelection(patch: Record<string, any>) {
		const s = get({ subscribe } as any) as GraphState;
		const nodeId = s.inspector.nodeId;
		if (!nodeId) return { ok: false, error: 'No node selected' };
		const beforeNode = s.nodes.find((x) => x.id === nodeId);
		const beforeExecParams = effectiveExecParamsForNode(beforeNode);

		// Commit only the provided snapshot-related patch; do not merge with pending draft.
		const result = updateNodeConfigImpl(nodeId, { params: patch });
		if (!result.ok) return result;

		const afterState = get({ subscribe } as any) as GraphState;
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

		await syncAcceptParamsForNode(nodeId, paramsForSubmit, beforeExecParams);
		return result;
	}

function applyLocalStaleInvalidation(nodeId: string, rootReason: string = 'PARAMS_CHANGED'): void {
		update((cur) => {
			const candidateIds = downstreamNodeIds(cur.edges, nodeId);
			const nodeBindings = { ...cur.nodeBindings };
			let nodeOutputs = { ...cur.nodeOutputs };
			let changed = false;
			for (const affectedId of candidateIds) {
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
			const nodeBindings = { ...cur.nodeBindings };
			const touchedIds: string[] = [];
			for (const affectedId of affectedNodeIds) {
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
		artifact?: { mimeType?: string; portType?: string };
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
					portType: resolved.artifact?.portType ?? prevOut.portType,
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

	async function applyInspectorDraft() {
		const s = get({ subscribe } as any) as GraphState;
		const nodeId = s.inspector.nodeId;
		if (!nodeId) return { ok: false, error: 'No node selected' };
		const beforeNode = s.nodes.find((x) => x.id === nodeId);
		const beforeExecParams = effectiveExecParamsForNode(beforeNode);

		const r = updateNodeConfigImpl(nodeId, { params: s.inspector.draftParams });

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

			await syncAcceptParamsForNode(nodeId, s.inspector.draftParams, beforeExecParams);
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

	//END

	function persist(state: GraphState) {
		saveGraphToLocalStorage(stripToDTO(state.nodes, state.edges, state.graphId));
	}

	function hydrateFromRunSnapshot(
		state: GraphState,
		snap: RunSnapshotLike
	): GraphState {
		return hydrateFromRunSnapshotState(state, snap);
	}

	function getInspectorUi(nodeId: string, paramsHint?: Record<string, any>): ApiEditorUiState {
		const state = get({ subscribe } as any) as GraphState;
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
		patchInspectorDraft,
		commitInspectorImmediate,
		commitSnapshotSelection,
		applyInspectorDraft,
		revertInspectorDraft,
		getInspectorUi,
		setInspectorUi,
		resolveNodeInputs(nodeId: string): InputResolution[] {
			const s = get({ subscribe } as any) as GraphState;
			return resolveNodeInputsFromState(s, nodeId);
		},
		updateNodeConfig: updateNodeConfigImpl,

		setSourceKind(nodeId: string, nextKind: SourceKind) {
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
								sourceKind: nextKind, // âœ… structural
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
		},

		// graphStore.ts (inside your graphStore object)
		setLlmKind(nodeId: string, nextKind: LlmKind) {
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
								llmKind: nextKind, // âœ… structural
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
				applySemanticSubtypeReset(nodeId, { kind: 'llm', llmKind: nextKind });
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
		},

		// graphStore.ts (inside your graphStore object)
		setTransformKind(nodeId: string, nextKind: TransformKind) {
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
								transformKind: nextKind, // âœ… structural
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
		},

		setToolProvider(nodeId: string, nextProvider: ToolProvider) {
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
		},

		setToolKind(nodeId: string, nextProvider: ToolProvider) {
			return this.setToolProvider(nodeId, nextProvider);
		},

		// ----- sync entrypoints (because SvelteFlow uses bind:nodes/bind:edges) -----
		syncFromCanvas(nodes: Node<PipelineNodeData>[], edges: Edge<PipelineEdgeData>[]) {
			update((s) => {
				// avoid needless churn if same references
				if (s.nodes === nodes && s.edges === edges) return s;
				const next = {
					...s,
					nodes,
					edges,
					nodeBindings: ensureNormalizedBindingsForNodes(nodes, s.nodeBindings ?? {})
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
						inspector: { ...initialInspector, uiByNodeId: s.inspector.uiByNodeId }
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
		addNode(kind: NodeKind, position: { x: number; y: number }) {
			const id = `n_${crypto.randomUUID()}`;
			const node: Node<PipelineNodeData> = {
				id,
				type: kind,
				position,
				data: defaultNodeData(kind)
			};

			update((s) => {
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

		addEdge(edge: Edge<PipelineEdgeData>) {
			let out: { ok: boolean; id?: string; error?: string } = { ok: true };
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

				// validate port types + refresh contract
				const chk = isEdgeStillValid(s.nodes, { ...edge, id } as Edge<PipelineEdgeData>);
				if (chk.ok === false) {
					out = {
						ok: false,
						error:
							chk.reason === 'type_mismatch'
								? 'Incompatible port types'
								: 'Cannot resolve port ttypes for this connection'
					};
					return s;
				}
				const sourceNode = s.nodes.find((n) => n.id === edge.source)!;
				const targetNode = s.nodes.find((n) => n.id === edge.target)!;

				const nextEdge: Edge<PipelineEdgeData> = {
					...edge,
					id,
					data: {
						...(edge.data ?? {}),
						exec: edge.data?.exec ?? 'idle',
						contract: {
							out: chk.out,
							in: chk.in,
							payload: {
								source: sourcePayloadHint(sourceNode as any, 'out'),
								target: targetPayloadHint(targetNode as any)
							}
						}
					}
				};

				const next = logPush({ ...s, edges: [...s.edges, nextEdge] }, 'info', `Added edge ${id}`);
				persist(next);
				out.id = id;
				return next;
			});

			return out;
		},

		updateNodeTitle(nodeId: string, label: string) {
			update((s) => {
				const nodes = s.nodes.map((n) =>
					n.id === nodeId ? { ...n, data: { ...n.data, label } } : n
				);
				const next = { ...s, nodes };
				persist(next);
				return next;
			});
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

		// ----- clear edges of prior run's status (uses edge highlighting) -----
		resetRunUi() {
			update((s) => {
				const edges = resetEdgesExec(s.edges);
				const next = withGraphMeta({ ...s, edges, logs: [], runStatus: IDLE });
				persist(next);
				return next;
			});
		},

		hardResetGraph() {
			const freshGraphId = mintGraphId();
			const next = buildHardResetState(freshGraphId);
			persist(next);
			set(next);
		},

		async runRemote(runFrom: string | null, runMode?: ActiveRunMode) {
			// prevent concurrent runs
			const s0 = get({ subscribe } as any) as GraphState;
			if (s0.runStatus === 'running') return;

			// reset UI
			this.resetRunUi();
			update((s) => withGraphMeta({ ...s, runStatus: 'running' }));

			// snapshot graph DTO
			const s1 = get({ subscribe } as any) as GraphState;
			const effectiveRunMode: ActiveRunMode = runMode ?? (runFrom ? 'from_selected_onward' : 'from_start');
			const payload = buildRunCreateRequest(
				{ version: 1, nodes: s1.nodes, edges: s1.edges },
				s1.graphId,
				runFrom,
				effectiveRunMode
			);
			const plannedNodeSet = computePlannedNodeSet(s1.nodes, s1.edges, runFrom, effectiveRunMode);

			// create run
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
				const sub = streamRunEvents(
					runId,
					(evt: KnownRunEvent) => {
						const cur = get({ subscribe } as any) as GraphState;
						const evtGraphId = (evt as any)?.graphId;
						if (
							typeof evtGraphId === 'string' &&
							evtGraphId &&
							evtGraphId !== cur.graphId
						) {
							return;
						}
						update((s) => {
							const nextState = reduceRunEventState(s, evt, runId);
							debugLogOutOfScopeBindingMutation(s, nextState, evt.type);
							debugLogStaleFlips(s, nextState, evt.type);
							assertNoOutOfScopeStaleFlips(s, nextState, evt.type);
							if (evt.type === 'run_started') {
								assertRunStartedNoBindingTouch(s, nextState);
								assertNoRunStartedStaleFlips(s, nextState);
							}
							return nextState;
						}, { source: 'event', evt });

						if (evt.type === 'run_finished') {
							const cur = get({ subscribe } as any) as GraphState;
							persist(cur);
							void getRun(runId)
								.then((snap) => {
									const current = get({ subscribe } as any) as GraphState;
									if (
										typeof snap.graphId === 'string' &&
										snap.graphId &&
										snap.graphId !== current.graphId
									) {
										return;
									}
									update((s) => hydrateFromRunSnapshot(s, snap), {
										source: 'hydrate_snapshot',
										snapshotNodeIds: new Set(Object.keys(snap.nodeBindings ?? {}))
									});
								})
								.catch(() => { });
							sub.close();
							resolve();
						}
					},
					() => {
						update((s) =>
							withGraphMeta(logPush({ ...s, runStatus: 'failed' }, 'error', 'Event stream error'))
						);
						resolve();
					}
				);
			});
		}
	};
})();

export const selectedNode = derived(graphStore, ($s) =>
	$s.selectedNodeId ? ($s.nodes.find((n) => n.id === $s.selectedNodeId) ?? null) : null
);
