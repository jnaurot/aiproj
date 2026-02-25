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
import { acceptNodeParams, createRun, getRun, streamRunEvents } from '$lib/flow/client/runs';
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
	shouldUpdateBinding,
	type ActiveRunMode,
	type GraphFreshness as ScopeFreshness
} from './runScope';

type NodeOutputInfo = {
	artifactId: string;
	mimeType?: string;
	portType?: string;
	preview?: string;
	cached?: boolean;
	cacheDecision?: 'cache_hit' | 'cache_miss' | 'cache_hit_contract_mismatch';
};
type NodeBindingInfo = {
	status?: string;
	lastArtifactId?: string | null;
	lastRunId?: string | null;
	lastExecKey?: string | null;
	currentExecKey?: string | null;
	currentArtifactId?: string | null;
	currentRunId?: string | null;
	isUpToDate?: boolean;
	cacheValid?: boolean;
	staleReason?: string | null;
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
type RunStatus = 'idle' | 'running' | 'succeeded' | 'failed' | 'canceled' | 'cancelled';
type GraphLastRunStatus = 'succeeded' | 'failed' | 'cancelled' | 'never_run';
type AuditContext = {
	source: 'event' | 'accept_params' | 'hydrate_snapshot' | 'graph_edit' | 'unknown';
	evt?: KnownRunEvent;
	allowedNodeIds?: Set<string>;
	snapshotNodeIds?: Set<string>;
};
type InspectorState = {
	nodeId: string | null;
	draftParams: Record<string, any>;
	dirty: boolean;
};

const IDLE: NodeStatus = 'idle';
const SUCCEEDED: NodeStatus = 'succeeded';
const allowedPorts = new Set(['table', 'text', 'json', 'binary', 'embeddings']);
const initialInspector: InspectorState = {
	nodeId: null,
	draftParams: {},
	dirty: false
};

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

export type GraphState = {
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
	nodeBindings: Record<string, NodeBindingInfo>;
	activeRunId: string | null;
};

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

function nodeStatusById(nodes: Node<PipelineNodeData & Record<string, unknown>>[]): Record<string, NodeStatus> {
	const out: Record<string, NodeStatus> = {};
	for (const n of nodes) {
		out[n.id] = n.data.status as NodeStatus;
	}
	return out;
}

function isAllowedSucceededRegression(nodeId: string, ctx: AuditContext): boolean {
	if (ctx.source === 'accept_params') {
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
		logSucceededRegression('binding', nodeId, prev, next, ctx, prevDisplay, nextDisplay);
		if (DEV_MODE && !allowed) {
			throw new Error(`SUCCEEDED_REGRESSION(binding): node=${nodeId}, source=${ctx.source}`);
		}
	}
}

function auditNodeDataStatusRegressions(prev: GraphState, next: GraphState, ctx: AuditContext): void {
	if (ctx.source === 'hydrate_snapshot') return;
	const prevById = nodeStatusById(prev.nodes);
	const nextById = nodeStatusById(next.nodes);
	const ids = new Set([...Object.keys(prevById), ...Object.keys(nextById)]);
	for (const nodeId of ids) {
		const prevStatus = prevById[nodeId];
		const nextStatus = nextById[nodeId];
		if (prevStatus !== SUCCEEDED || !nextStatus || nextStatus === SUCCEEDED) continue;
		if (nextStatus === 'running') continue;
		const allowed = isAllowedSucceededRegression(nodeId, ctx);
		const prevDisplay = displayStatusFromBinding(prev.nodeBindings?.[nodeId]);
		const nextDisplay = displayStatusFromBinding(next.nodeBindings?.[nodeId]);
		logSucceededRegression(
			'node_data',
			nodeId,
			prev,
			next,
			ctx,
			prevDisplay,
			nextDisplay,
			prevStatus,
			nextStatus
		);
		if (DEV_MODE && !allowed) {
			throw new Error(`SUCCEEDED_REGRESSION(node_data): node=${nodeId}, source=${ctx.source}`);
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
	auditNodeDataStatusRegressions(prev, next, ctx);
}

function withGraphMeta(state: GraphState): GraphState {
	const { freshness, staleNodeCount } = computeGraphFreshness(state.nodeBindings ?? {});
	let lastRunStatus = state.lastRunStatus;
	if (state.runStatus === 'succeeded') lastRunStatus = 'succeeded';
	if (state.runStatus === 'failed') lastRunStatus = 'failed';
	if (state.runStatus === 'canceled' || state.runStatus === 'cancelled') lastRunStatus = 'cancelled';
	if (freshness === 'never_run') lastRunStatus = 'never_run';
	return { ...state, freshness, staleNodeCount, lastRunStatus };
}

function canApplyNodeEvent(state: GraphState, nodeId: string): boolean {
	return shouldUpdateBinding(state.activeRunId, state.activeRunNodeSet, nodeId);
}

function changedBindingNodeIds(
	prev: Record<string, NodeBindingInfo>,
	next: Record<string, NodeBindingInfo>
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
			{} as Record<string, NodeBindingInfo | null>
		),
		bindingsAfter: changed.reduce(
			(acc, id) => ({ ...acc, [id]: next.nodeBindings?.[id] ?? null }),
			{} as Record<string, NodeBindingInfo | null>
		)
	});
}

function reduceRunEventState(state: GraphState, evt: KnownRunEvent, runId: string): GraphState {
	switch (evt.type) {
		case 'node_output': {
			if (!canApplyNodeEvent(state, evt.nodeId)) return state;
			const prevBinding = state.nodeBindings?.[evt.nodeId] ?? {};
			const nodeBindings = {
				...state.nodeBindings,
				[evt.nodeId]: {
					...prevBinding,
					currentArtifactId: evt.artifactId,
					lastArtifactId: evt.artifactId,
					currentRunId: runId,
					lastRunId: runId,
					isUpToDate: typeof prevBinding.isUpToDate === 'boolean' ? prevBinding.isUpToDate : true
				}
			};
			const nodeOutputs = {
				...state.nodeOutputs,
				[evt.nodeId]: {
					artifactId: evt.artifactId,
					mimeType: evt.mimeType,
					portType: evt.portType,
					preview: evt.preview ?? undefined,
					cached: evt.cached ?? false,
					cacheDecision: state.nodeOutputs?.[evt.nodeId]?.cacheDecision
				}
			};
			return withGraphMeta({
				...state,
				nodeOutputs,
				nodeBindings
			});
		}
		case 'cache_decision': {
			if (!canApplyNodeEvent(state, evt.nodeId)) return state;
			const prevBinding = state.nodeBindings?.[evt.nodeId] ?? {};
			const nextIsUpToDate =
				evt.decision === 'cache_hit'
					? true
					: evt.decision === 'cache_hit_contract_mismatch'
						? false
						: (typeof prevBinding.isUpToDate === 'boolean' ? prevBinding.isUpToDate : true);
			const nodeBindings = {
				...state.nodeBindings,
				[evt.nodeId]: {
					...prevBinding,
					currentExecKey: evt.execKey,
					cacheValid: evt.decision === 'cache_hit',
					isUpToDate: nextIsUpToDate,
					currentArtifactId:
						evt.decision === 'cache_hit'
							? (evt.artifactId ?? prevBinding.currentArtifactId ?? prevBinding.lastArtifactId)
							: null
				}
			};
			const prev = state.nodeOutputs?.[evt.nodeId];
			const nextForNode: NodeOutputInfo = {
				artifactId: evt.artifactId ?? prev?.artifactId ?? '',
				mimeType: prev?.mimeType,
				portType: prev?.portType,
				preview: prev?.preview,
				cached:
					evt.decision === 'cache_hit' || evt.decision === 'cache_hit_contract_mismatch'
						? true
						: (prev?.cached ?? false),
				cacheDecision: evt.decision
			};
			if (!nextForNode.artifactId) {
				return withGraphMeta({ ...state, nodeBindings });
			}
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
			if (!canApplyNodeEvent(state, evt.nodeId)) return state;
			const nodeBindings = {
				...state.nodeBindings,
				[evt.nodeId]: {
					...(state.nodeBindings?.[evt.nodeId] ?? {}),
					status: 'running'
				}
			};
			return withGraphMeta(logPush({ ...state, nodeBindings }, 'info', 'Node started', evt.nodeId));
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
			if (!canApplyNodeEvent(state, evt.nodeId)) return state;
			const prevBinding = state.nodeBindings?.[evt.nodeId] ?? {};
			const nextBinding: NodeBindingInfo = {
				...prevBinding,
				status: evt.status === 'succeeded' ? 'succeeded_up_to_date' : evt.status
			};
			const nodeBindings = {
				...state.nodeBindings,
				[evt.nodeId]: nextBinding
			};
			return withGraphMeta(
				logPush(
					{ ...state, nodeBindings },
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
	status?: string;
	runMode?: ActiveRunMode;
	plannedNodeIds?: string[];
	nodeStatus?: Record<string, string>;
	nodeOutputs?: Record<string, string>;
	nodeBindings?: Record<string, Record<string, unknown>>;
};

function hydrateFromRunSnapshotState(state: GraphState, snap: RunSnapshotLike): GraphState {
	const nodeBindingsPatch: Record<string, NodeBindingInfo> = {};
	const nodeOutputs: Record<string, NodeOutputInfo> = { ...(state.nodeOutputs ?? {}) };
	for (const [nodeId, raw] of Object.entries(snap.nodeBindings ?? {})) {
		const b = raw as NodeBindingInfo;
		nodeBindingsPatch[nodeId] = b;
		const aid = (b.currentArtifactId ?? b.lastArtifactId) as string | null | undefined;
		if (aid) {
			nodeOutputs[nodeId] = { ...(nodeOutputs[nodeId] ?? {}), artifactId: aid };
		}
	}
	for (const [nodeId, aid] of Object.entries(snap.nodeOutputs ?? {})) {
		if (!aid) continue;
		nodeOutputs[nodeId] = { ...(nodeOutputs[nodeId] ?? {}), artifactId: aid };
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

function stripToDTO(
	nodes: Node<PipelineNodeData>[],
	edges: Edge<PipelineEdgeData>[]
): PipelineGraphDTO {
	return { version: 1, nodes, edges };
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
	nodeBindings: {},
	activeRunId: null
};

const statusOrIdle = (s: NodeStatus): NodeStatus => s;

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
	function commitInspectorImmediate(patch: Record<string, any>) {
		const s = get({ subscribe } as any) as GraphState;
		const nodeId = s.inspector.nodeId;
		if (!nodeId) return { ok: false, error: 'No node selected' };

		// 1) update draft (so Apply is non-detrimental)
		patchInspectorDraft(patch);

		// 2) commit patch (validated/stripped)
		return updateNodeConfigImpl(nodeId, { params: patch });
	}

	async function applyInspectorDraft() {
		const s = get({ subscribe } as any) as GraphState;
		const nodeId = s.inspector.nodeId;
		if (!nodeId) return { ok: false, error: 'No node selected' };

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
						dirty: false
					}
				};
			});

			const st = get({ subscribe } as any) as GraphState;
			if (st.activeRunId) {
				try {
					const resp = await acceptNodeParams({
						runId: st.activeRunId,
						nodeId,
						graph: { version: 1, nodes: st.nodes, edges: st.edges },
						params: s.inspector.draftParams
					});
					if (Array.isArray(resp.affectedNodeIds) && resp.affectedNodeIds.length > 0) {
						update((cur) => {
							const nodeBindings = { ...cur.nodeBindings };
							for (const affectedId of resp.affectedNodeIds) {
								const prev = nodeBindings[affectedId] ?? {};
								nodeBindings[affectedId] = {
									...prev,
									status: 'stale',
									isUpToDate: false,
									currentArtifactId: null,
									currentRunId: null,
									currentExecKey: null
								};
							}
							return withGraphMeta({ ...cur, nodeBindings });
						}, { source: 'accept_params', allowedNodeIds: new Set(resp.affectedNodeIds) });
					}
					const snap = await getRun(st.activeRunId);
					update((cur) => hydrateFromRunSnapshot(cur, snap), {
						source: 'hydrate_snapshot',
						snapshotNodeIds: new Set(Object.keys(snap.nodeBindings ?? {}))
					});
				} catch (e) {
					update((cur) =>
						logPush(cur, 'warn', `accept-params sync failed: ${String(e)}`, nodeId)
					);
				}
			}
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
					dirty: false
				}
			};
		});
	}

	//END

	function persist(state: GraphState) {
		saveGraphToLocalStorage(stripToDTO(state.nodes, state.edges));
	}

	function hydrateFromRunSnapshot(
		state: GraphState,
		snap: RunSnapshotLike
	): GraphState {
		return hydrateFromRunSnapshotState(state, snap);
	}

	return {
		subscribe,
		patchInspectorDraft,
		commitInspectorImmediate,
		applyInspectorDraft,
		revertInspectorDraft,
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

			// 3) ensure inspector draft matches immediately after type switch
			if (r.ok) {
				update((s) => {
					const n = s.nodes.find((x) => x.id === nodeId);
					return {
						...s,
						inspector: {
							nodeId,
							draftParams: structuredClone((n?.data.params ?? {}) as any),
							dirty: false
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

			// 3) ensure inspector draft matches immediately after type switch
			if (r.ok) {
				update((s) => {
					const n = s.nodes.find((x) => x.id === nodeId);
					return {
						...s,
						inspector: {
							nodeId,
							draftParams: structuredClone((n?.data.params ?? {}) as any),
							dirty: false
						}
					};
				});
			}

			return r;
		},

		// graphStore.ts (inside your graphStore object)
		setTransformKind(nodeId: string, nextKind: TransformKind) {
			if (nextKind === 'python') {
				return { ok: false, error: 'Transform op "python" is disabled. Use Tool node instead.' };
			}
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

			// 3) ensure inspector draft matches immediately after type switch
			if (r.ok) {
				update((s) => {
					const n = s.nodes.find((x) => x.id === nodeId);
					return {
						...s,
						inspector: {
							nodeId,
							draftParams: structuredClone((n?.data.params ?? {}) as any),
							dirty: false
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
				update((s) => {
					const n = s.nodes.find((x) => x.id === nodeId);
					return {
						...s,
						inspector: {
							nodeId,
							draftParams: structuredClone((n?.data.params ?? {}) as any),
							dirty: false
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
				const next = { ...s, nodes, edges };
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
						inspector: initialInspector
					};
				}

				const n = s.nodes.find((x) => x.id === nodeId);
				return {
					...s,
					selectedNodeId: nodeId,
					inspector: {
						nodeId,
						draftParams: structuredClone((n?.data.params ?? {}) as any),
						dirty: false
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
				const next = logPush(
					{ ...s, nodes: [...s.nodes, node], selectedNodeId: id },
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

				const next = logPush(
					{ ...s, nodes, edges, selectedNodeId },
					'info',
					`Deleted node ${nodeId}`,
					nodeId
				);
				persist(next);
				return next;
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
				runFrom,
				effectiveRunMode
			);
			const plannedNodeSet = computePlannedNodeSet(s1.nodes, s1.edges, runFrom, effectiveRunMode);

			// create run
			let runId: string;

			try {
				({ runId } = await createRun(payload));
				update((s) =>
					withGraphMeta({
						...s,
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
