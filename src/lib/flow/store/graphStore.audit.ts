// src/lib/flow/store/graphStore.audit.ts
//
// State-transition auditing, logging helpers, and the withGraphMeta utility.
// All functions here are pure (no side effects beyond console.error/debug in dev).
//
// Exports used by later modules:
//   auditStateTransition  – wired into history.wrapUpdate
//   withGraphMeta         – called after every state mutation to recompute derived fields
//   logPush               – append a run log entry to state
//   stableJson            – stable JSON serialisation used as a cheap equality key

import type { Node } from '@xyflow/svelte';
import type { NodeStatus, PipelineNodeData } from '$lib/flow/types';
import type {
	AuditContext,
	GraphState,
	NodeBindingInfo,
	NodeOutputInfo,
	NormalizedNodeBinding,
	LogLevel,
} from './graphStore.types';
import type { BindingPair } from './graphStore.bindings';
import { NODE_STATUS_SUCCEEDED } from './graphStore.types';
import { displayStatusFromBinding, computeGraphFreshness } from './runScope';
import { recomputeSchemaPlane } from './graphStore.schemaPlane';

// ---------------------------------------------------------------------------
// Module-level state (moved from graphStore.ts)
// ---------------------------------------------------------------------------

export let logSeq = 0;
export function nextLogId(): number { return ++logSeq; }
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
export const DEV_MODE = (() => {
	try {
		return Boolean((import.meta as any)?.env?.DEV);
	} catch {
		return false;
	}
})();

// ---------------------------------------------------------------------------
// Binding normalization helpers (needed by withGraphMeta)
// ---------------------------------------------------------------------------

export function _pairFromLegacy(binding: NodeBindingInfo | undefined, which: 'current' | 'last') {
	const b = binding ?? {};
	if (which === 'current') {
		const hasStructured = Boolean(b.current && typeof b.current === 'object');
		const execKey = (b.current?.execKey ?? b.currentExecKey) ?? null;
		const artifactId = (b.current?.artifactId ?? b.currentArtifactId) ?? null;
		return {
			execKey: hasStructured ? execKey : execKey,
			artifactId: hasStructured ? artifactId : artifactId
		};
	}
	const hasStructured = Boolean(b.last && typeof b.last === 'object');
	const execKey = (b.last?.execKey ?? b.lastExecKey) ?? null;
	const artifactId = (b.last?.artifactId ?? b.lastArtifactId) ?? null;
	return {
		execKey: hasStructured ? execKey : execKey,
		artifactId: hasStructured ? artifactId : artifactId
	};
}

function _pairFromLegacyForMigration(binding: NodeBindingInfo | undefined, which: 'current' | 'last') {
	const b = binding ?? {};
	if (which === 'current') {
		const hasStructured = Boolean(b.current && typeof b.current === 'object');
		const execKey = (b.current?.execKey ?? b.currentExecKey) ?? null;
		const artifactId = (b.current?.artifactId ?? b.currentArtifactId) ?? null;
		return {
			execKey: hasStructured ? execKey : (execKey ?? artifactId),
			artifactId: hasStructured ? artifactId : (artifactId ?? execKey)
		};
	}
	const hasStructured = Boolean(b.last && typeof b.last === 'object');
	const execKey = (b.last?.execKey ?? b.lastExecKey) ?? null;
	const artifactId = (b.last?.artifactId ?? b.lastArtifactId) ?? null;
	return {
		execKey: hasStructured ? execKey : (execKey ?? artifactId),
		artifactId: hasStructured ? artifactId : (artifactId ?? execKey)
	};
}

export function _assertBindingPairInvariant(
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

export function _withPair(binding: NormalizedNodeBinding, which: 'current' | 'last', pair: BindingPair): NormalizedNodeBinding {
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

export function __assertBindingPairForTest(binding: NodeBindingInfo, nodeId = 'test', context = 'test'): void {
	_assertBindingPairInvariant(binding, nodeId, context, true);
}

export function __normalizeBindingForTest(
	binding: NodeBindingInfo | undefined,
	nodeId = 'test'
): NormalizedNodeBinding {
	return __normalizeBindingForLegacyMigrationForTest(binding, nodeId);
}

export function __normalizeBindingForLegacyMigrationForTest(
	binding: NodeBindingInfo | undefined,
	nodeId = 'test'
): NormalizedNodeBinding {
	const b = { ...(binding ?? {}) };
	const hasCurrentFields =
		Object.prototype.hasOwnProperty.call(b, 'current') ||
		Object.prototype.hasOwnProperty.call(b, 'currentExecKey') ||
		Object.prototype.hasOwnProperty.call(b, 'currentArtifactId');
	const hasLastFields =
		Object.prototype.hasOwnProperty.call(b, 'last') ||
		Object.prototype.hasOwnProperty.call(b, 'lastExecKey') ||
		Object.prototype.hasOwnProperty.call(b, 'lastArtifactId');
	if (hasCurrentFields) b.current = _pairFromLegacyForMigration(b, 'current');
	if (hasLastFields) b.last = _pairFromLegacyForMigration(b, 'last');
	return _normalizeBinding(b, nodeId);
}

export function __normalizeBindingStrictForTest(
	binding: NodeBindingInfo | undefined,
	nodeId = 'test'
): NormalizedNodeBinding {
	return _normalizeBinding(binding, nodeId);
}

export function _normalizeBinding(binding: NodeBindingInfo | undefined, nodeId?: string): NormalizedNodeBinding {
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

export function ensureNormalizedBindingsForNodes(
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

export function pruneNodeOutputsForNodes(
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

// ---------------------------------------------------------------------------
// Timestamp and logging helpers
// ---------------------------------------------------------------------------

function nowTs() {
	return new Date().toLocaleTimeString();
}

function _componentPathFromNodeId(state: GraphState, nodeId?: string): string[] | undefined {
	const raw = String(nodeId ?? '').trim();
	if (!raw.startsWith('cmp:')) return undefined;
	const componentInstanceIds: string[] = [];
	let cursor = raw;
	let guard = 0;
	while (cursor.startsWith('cmp:') && guard < 32) {
		guard += 1;
		const rest = cursor.slice(4);
		const sep = rest.indexOf(':');
		if (sep <= 0) break;
		const instanceId = rest.slice(0, sep).trim();
		if (!instanceId) break;
		componentInstanceIds.push(instanceId);
		cursor = rest.slice(sep + 1);
	}
	if (!componentInstanceIds.length) return undefined;
	const names = componentInstanceIds.map((instanceId) => {
		const node = state.nodes.find((n) => n.id === instanceId);
		const data = (node?.data ?? {}) as Record<string, any>;
		const ref = (data.params as Record<string, any> | undefined)?.componentRef as
			| Record<string, unknown>
			| undefined;
		const componentId = String(ref?.componentId ?? '').trim();
		return componentId || instanceId;
	});
	return names.length ? names : undefined;
}

function formatEnvProfileRunLogMessage(message: string): string {
	const raw = String(message ?? '').trim();
	if (!raw) return raw;
	const envCodePrefix = raw.match(/^(ENV_PROFILE_[A-Z_]+)\s*:\s*(.*)$/);
	if (envCodePrefix) {
		const code = String(envCodePrefix[1] ?? '').trim();
		const rest = String(envCodePrefix[2] ?? '').trim();
		if (code === 'ENV_PROFILE_MISSING' && !/Install profile:/i.test(rest)) {
			return `${code}: ${rest} Install profile: POST /env/profiles/install.`;
		}
		return raw;
	}
	if (!raw.startsWith('{') || !raw.endsWith('}')) return raw;
	try {
		const parsed = JSON.parse(raw) as Record<string, unknown>;
		const code = String(parsed?.errorCode ?? parsed?.code ?? '').trim().toUpperCase();
		if (!code.startsWith('ENV_PROFILE_')) return raw;
		const profileId = String(parsed?.profileId ?? '').trim() || 'core';
		const missingPackages = Array.isArray(parsed?.missingPackages)
			? (parsed.missingPackages as unknown[]).map((v) => String(v)).filter((v) => v.trim().length > 0)
			: [];
		const installHint = String(parsed?.installHint ?? '').trim() || 'POST /env/profiles/install';
		if (code === 'ENV_PROFILE_MISSING') {
			const suffix =
				missingPackages.length > 0
					? `missing packages: ${missingPackages.join(', ')}.`
					: 'is not installed.';
			return `${code}: profile '${profileId}' ${suffix} Install profile: ${installHint} (profileId='${profileId}').`;
		}
		if (code === 'ENV_PROFILE_INVALID') {
			return `${code}: profile '${profileId}' is invalid. Update profile selection in the tool editor.`;
		}
		if (code === 'ENV_PROFILE_PACKAGE_BLOCKED') {
			return `${code}: profile '${profileId}' has blocked package entries.`;
		}
		if (code === 'ENV_PROFILE_INSTALL_FAILED') {
			return `${code}: profile '${profileId}' install failed. Retry install via ${installHint} (profileId='${profileId}').`;
		}
		return raw;
	} catch {
		return raw;
	}
}

export function logPush(
	state: GraphState,
	level: LogLevel,
	message: string,
	nodeId?: string,
	componentPath?: string[],
	edgeId?: string
) {
	logSeq += 1;
	const resolvedComponentPath = componentPath?.length ? componentPath : _componentPathFromNodeId(state, nodeId);
	const normalizedMessage = formatEnvProfileRunLogMessage(message);
	return {
		...state,
		logs: [
			...state.logs,
			{
				id: logSeq,
				ts: nowTs(),
				level,
				message: normalizedMessage,
				nodeId,
				edgeId: edgeId ? String(edgeId) : undefined,
				componentPath: resolvedComponentPath
			}
		]
	};
}

// ---------------------------------------------------------------------------
// Audit helpers
// ---------------------------------------------------------------------------

function captureStack(label: string): string {
	try {
		return new Error(label).stack ?? '';
	} catch {
		return '';
	}
}

function isAllowedSucceededRegression(nodeId: string, ctx: AuditContext): boolean {
	if (ctx.expectedDirtyTransition && ctx.allowedNodeIds?.has(nodeId)) {
		return true;
	}
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
	if (evt.type === 'run_started') {
		return Boolean(ctx.allowedNodeIds?.has(nodeId));
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
		if (prevDisplay !== NODE_STATUS_SUCCEEDED || nextDisplay === NODE_STATUS_SUCCEEDED) continue;
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

export function auditStateTransition(prev: GraphState, next: GraphState, ctx: AuditContext): void {
	assertHydrationBindingInvariants(prev, next, ctx);
	auditSucceededRegressions(prev, next, ctx);
}

// ---------------------------------------------------------------------------
// withGraphMeta — recomputes derived fields after every state mutation
// ---------------------------------------------------------------------------

export function withGraphMeta(state: GraphState): GraphState {
	const normalizedBindings = ensureNormalizedBindingsForNodes(state.nodes, state.nodeBindings ?? {});
	const normalizedOutputs = pruneNodeOutputsForNodes(state.nodes, state.nodeOutputs ?? {});
	const schemaPlane = recomputeSchemaPlane({
		...state,
		nodeBindings: normalizedBindings,
		nodeOutputs: normalizedOutputs
	});
	const { freshness, staleNodeCount } = computeGraphFreshness(normalizedBindings ?? {});
	let lastRunStatus = state.lastRunStatus;
	if (state.runStatus === 'succeeded') lastRunStatus = 'succeeded';
	if (state.runStatus === 'failed') lastRunStatus = 'failed';
	if (state.runStatus === 'canceled') lastRunStatus = 'canceled';
	if (freshness === 'never_run') lastRunStatus = 'never_run';
	return {
		...state,
		freshness,
		staleNodeCount,
		lastRunStatus,
		nodeBindings: normalizedBindings,
		nodeOutputs: normalizedOutputs,
		schemaPlane
	};
}

// ---------------------------------------------------------------------------
// stableJson — stable serialisation used as a cheap equality key
// ---------------------------------------------------------------------------

export function stableJson(value: unknown): string {
	try {
		return JSON.stringify(value ?? null);
	} catch {
		return '';
	}
}
