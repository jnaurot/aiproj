import type { Edge, Node } from '@xyflow/svelte';

import type { PipelineEdgeData, PipelineNodeData } from '$lib/flow/types';
import {
	projectEdgeStatus,
	projectNodeStatus,
	reconcileLifecycleForActiveRun,
	type EdgeLifecycleStatus,
	type RunActivityStatus,
	type NodeExecutionStatus,
	type NodeFreshnessStatus,
	type NodeLifecycleStatus
} from '$lib/flow/store/statusModel';

type QueueMetric = {
	depth?: unknown;
	blocked?: unknown;
	full?: unknown;
	oldestAgeSec?: unknown;
};

type SchedulerPerNode = {
	nodeId: string;
	readyWork: boolean;
	inflight: number;
	pendingInputCount: number;
	lastBlockedReasonCode?: string;
};

type SchedulerSnapshot = {
	stalled?: unknown;
	perNode?: unknown;
};

type LlmLease = {
	state?: unknown;
	nodeId?: unknown;
	holderNodeId?: unknown;
	activeNodeIds?: unknown;
	waitQueueLength?: unknown;
	waitingNodeIds?: unknown;
};

type ControlPlaneNodeState = Record<
	string,
	{
		nodeId?: string;
		lastSignal?: string;
		terminalReasonCode?: string;
		lastSeq?: number;
		updatedAt?: string;
	}
>;

type BlockedByNode = Record<
	string,
	{
		nodeId: string;
		reasonCode: string;
		handle?: string;
		plane?: 'work' | 'param' | 'control';
		updatedAt?: string;
		history?: Array<{ code?: string; at?: string; action?: 'set' | 'cleared' }>;
	}
>;


export const BLOCKER_HISTORY_MAX = 10 as const;

export type PhaseCode =
	| 'AWAITING_INPUT'
	| 'AWAITING_DISPATCH'
	| 'AWAITING_LEASE'
	| 'AWAITING_PROVIDER_RESPONSE'
	| 'POSTPROCESSING'
	| 'WRITING_OUTPUT'
	| 'TERMINAL';

export type BlockerCode =
	| 'MAX_INFLIGHT_REACHED:global'
	| 'MAX_INFLIGHT_REACHED:provider'
	| 'MAX_INFLIGHT_REACHED:model'
	| 'MAX_INFLIGHT_REACHED:node'
	| 'WAITING_REQUIRED_INPUT'
	| 'DEPENDENCY_NOT_READY'
	| 'LEASE_UNAVAILABLE';

export type BlockerDetail = {
	source?: 'global' | 'provider' | 'model' | 'node';
	limit?: number;
	inflight?: number;
	holderNodeId?: string;
	provider?: string;
	model?: string;
	queueDepth?: number;
};

export type MonitorBlocker = {
	code: BlockerCode;
	detail?: BlockerDetail;
	since?: string;
};

export type MonitorLastBlocker = {
	code: BlockerCode;
	detail?: BlockerDetail;
	clearedAt?: string;
};

export type MonitorBlockerHistoryEntry = {
	code: string;
	at: string;
	action: 'set' | 'cleared';
};

export type RunMonitorNodeRow = {
	nodeId: string;
	label: string;
	status: string;
	lifecycle: NodeLifecycleStatus;
	execution: NodeExecutionStatus;
	freshness: NodeFreshnessStatus;
	consumeMode: 'once' | 'single_item' | 'batch';
	acceptedCount: number;
	rejectedCount: number;
	totalProcessed: number;
	pendingInputCount: number;
	inflight: number;
	inboundDepth: number;
	readyWork: boolean;
	blockedReasonCode: string | null;
	blockedHandle: string | null;
	blockedPlane: 'work' | 'param' | 'control' | null;
	updatedAt: string | null;
	terminalReasonCode: string | null;
	isBlocked: boolean;
	isWaiting: boolean;
	isLlmHolder: boolean;
	isLlmWaiting: boolean;
	phase: PhaseCode | null;
	phaseSince: string | null;
	blocker: MonitorBlocker | null;
	lastBlocker: MonitorLastBlocker | null;
	blockerHistory: MonitorBlockerHistoryEntry[];
	/** Human-visible reason why this node is not making progress. Empty string
	 *  when the node is running or done — never the literal string "-". */
	displayReason: string;
	statusParityMismatch: boolean;
};

export type RunMonitorEdgeRow = {
	edgeId: string;
	handle: string;
	sourceNodeId: string;
	sourceLabel: string;
	targetNodeId: string;
	targetLabel: string;
	lifecycle: EdgeLifecycleStatus;
	exec: 'idle' | 'active' | 'done';
	depth: number;
	blocked: boolean;
	full: boolean;
	oldestAgeSec: number | null;
};

export type RunMonitorAdaptiveDecisionRow = {
	at: string;
	runId: string;
	mode: string;
	modeSource: string;
	enforced: boolean;
	inputs: Record<string, unknown>;
	reasons: string[];
	changedCaps: Record<string, { from: number; to: number }>;
	hardCaps: Record<string, number>;
	minCaps: Record<string, number>;
	proposedCaps: Record<string, number>;
	effectiveCaps: Record<string, number>;
	explanation: {
		score: number;
		severity: 'low' | 'medium' | 'high';
		signals: string[];
		components: Array<{ label: string; delta: number }>;
	};
	diffFromPrevious?: {
		modeChanged: boolean;
		scoreDelta: number;
		capDelta: Record<string, { from: number; to: number }>;
		reasonsAdded: string[];
		reasonsRemoved: string[];
	};
};

export type RunMonitorAdaptiveComponentBreakdownItem = {
	label: string;
	delta: number;
	absDelta: number;
	percentOfMax: number;
	direction: 'up' | 'down';
};

export type RunMonitorAdaptiveDecisionSummary = {
	total: number;
	enforced: number;
	byMode: Record<string, number>;
	bySeverity: Record<'low' | 'medium' | 'high', number>;
};

export type RunMonitorTrendSparkline = {
	path: string;
	width: number;
	height: number;
	points: Array<{
		x: number;
		y: number;
		value: number;
		createdAt: string;
	}>;
	baselines: {
		firstValueY: number;
		meanValueY: number;
	};
	minValue: number;
	maxValue: number;
	lastValue: number;
	deltaValue: number;
	deltaPct: number | null;
	pointsCount: number;
};

export type RunMonitorAdaptiveModeFilter = 'all' | 'off' | 'observe' | 'enforce';
export type RunMonitorAdaptiveSeverityFilter = 'all' | 'low' | 'medium' | 'high';
export type RunMonitorRegressionPair = { runId: string; baselineRunId: string };
export type RunMonitorTransitionFilter = 'all' | 'run' | 'node' | 'violations';

export type RunMonitorTransitionRow = {
	id: number;
	runId: string;
	type: string;
	at: string;
	entity: string;
	entityId: string;
	source: string;
	target: string;
	reasonCode: string;
	isViolation: boolean;
};

export type RunMonitorFilter = 'all' | 'blocked' | 'waiting' | 'stalled';
export type RunMonitorSort = 'pending_desc' | 'pending_asc' | 'depth_desc' | 'depth_asc' | 'label_asc';
export type RunMonitorEdgeFilter = 'inactive' | 'waiting' | 'running' | 'done' | 'active' | 'blocked' | 'full';
export type MonitorGroupKey = 'active' | 'waiting' | 'pending' | 'done';

export type MonitorNodeGroup = {
	key: MonitorGroupKey;
	label: string;
	rows: RunMonitorNodeRow[];
	totalCount: number;
	runningCount: number;
	throttledCount: number;
	waitingCount: number;
	pausedCount: number;
	notStartedCount: number;
	completedCount: number;
	failedCount: number;
	canceledCount: number;
};

export type MonitorGroupedNodes = {
	groups: MonitorNodeGroup[];
	totalNodeCount: number;
	hasFailures: boolean;
	activeGroupIndex: number;
	waitingGroupIndex: number;
	pendingGroupIndex: number;
	doneGroupIndex: number;
};

type RunMonitorProjectionInput = {
	nodes: Node<PipelineNodeData & Record<string, unknown>>[];
	edges: Edge<PipelineEdgeData & Record<string, unknown>>[];
	nodeBindings?: Record<string, unknown>;
	queueRuntime?: {
		metrics?: Record<string, unknown>;
		schedulerSnapshot?: SchedulerSnapshot;
		llmLease?: LlmLease;
		blockedByNode?: BlockedByNode;
		controlPlaneNodeState?: ControlPlaneNodeState;
	};
	runStatus?: RunActivityStatus;
};

function asRecord(value: unknown): Record<string, unknown> {
	return value && typeof value === 'object' ? (value as Record<string, unknown>) : {};
}

function asArray<T>(value: unknown): T[] {
	return Array.isArray(value) ? (value as T[]) : [];
}

function nodeLabel(node: Node<PipelineNodeData & Record<string, unknown>>): string {
	const raw = String((node?.data as any)?.label ?? '').trim();
	return raw || String(node?.id ?? '').trim() || '(unknown node)';
}

function parseSchedulerPerNode(snapshot: SchedulerSnapshot | undefined): Map<string, SchedulerPerNode> {
	const out = new Map<string, SchedulerPerNode>();
	for (const raw of asArray<Record<string, unknown>>(snapshot?.perNode)) {
		const nodeId = String(raw?.nodeId ?? '').trim();
		if (!nodeId) continue;
		out.set(nodeId, {
			nodeId,
			readyWork: Boolean(raw?.readyWork ?? false),
			inflight: Math.max(0, Number(raw?.inflight ?? 0)),
			pendingInputCount: Math.max(0, Number(raw?.pendingInputCount ?? 0)),
			lastBlockedReasonCode: String(raw?.lastBlockedReasonCode ?? '').trim() || undefined
		});
	}
	return out;
}

type RuntimeNodeCounter = {
	accepted: number;
	rejected: number;
};

function parseRuntimeNodeCounters(queueRuntime: Record<string, unknown>): Map<string, RuntimeNodeCounter> {
	const pickMetrics = (container: unknown): Record<string, unknown> =>
		asRecord(asRecord(container).runtimeItemMetrics);
	const runScopedMetrics = pickMetrics(queueRuntime.runScoped);
	const rootMetrics = pickMetrics(queueRuntime);
	const nodeCounters = asRecord(runScopedMetrics.nodeCounters);
	const fallbackCounters = asRecord(rootMetrics.nodeCounters);
	const byHandle = asRecord(runScopedMetrics.byHandle);
	const fallbackByHandle = asRecord(rootMetrics.byHandle);
	const out = new Map<string, RuntimeNodeCounter>();

	const upsert = (nodeId: string, acceptedRaw: unknown, rejectedRaw: unknown): void => {
		const accepted = Math.max(0, Number(acceptedRaw ?? 0));
		const rejected = Math.max(0, Number(rejectedRaw ?? 0));
		const existing = out.get(nodeId);
		if (existing) {
			existing.accepted = Math.max(existing.accepted, accepted);
			existing.rejected = Math.max(existing.rejected, rejected);
			return;
		}
		out.set(nodeId, { accepted, rejected });
	};

	for (const [nodeIdRaw, value] of Object.entries(nodeCounters)) {
		const nodeId = String(nodeIdRaw ?? '').trim();
		if (!nodeId) continue;
		const entry = asRecord(value);
		upsert(nodeId, entry.accepted, entry.rejected);
	}
	for (const [nodeIdRaw, value] of Object.entries(fallbackCounters)) {
		const nodeId = String(nodeIdRaw ?? '').trim();
		if (!nodeId || out.has(nodeId)) continue;
		const entry = asRecord(value);
		upsert(nodeId, entry.accepted, entry.rejected);
	}

	const ingestByHandle = (bucket: Record<string, unknown>): void => {
		for (const value of Object.values(bucket)) {
			const entry = asRecord(value);
			const nodeId = String(entry.nodeId ?? '').trim();
			if (!nodeId) continue;
			const plane = String(entry.plane ?? 'work').trim().toLowerCase();
			if (plane !== 'work') continue;
			const accepted = Math.max(0, Number(entry.itemsAccepted ?? 0));
			const rejected = Math.max(0, Number(entry.itemsRejected ?? 0));
			const existing = out.get(nodeId);
			if (existing) {
				existing.accepted += accepted;
				existing.rejected += rejected;
			} else {
				out.set(nodeId, { accepted, rejected });
			}
		}
	};

	if (out.size === 0) {
		ingestByHandle(byHandle);
		if (out.size === 0) ingestByHandle(fallbackByHandle);
	}

	return out;
}

function parseEdgeMetric(edgeMetrics: Record<string, unknown>, metricKey: string): QueueMetric {
	return asRecord(edgeMetrics[metricKey]) as QueueMetric;
}

function buildInboundDepthByNode(
	edges: Edge<PipelineEdgeData & Record<string, unknown>>[],
	edgeMetrics: Record<string, unknown>
): Map<string, number> {
	const out = new Map<string, number>();
	for (const edge of edges) {
		const edgeId = String(edge?.id ?? '').trim();
		if (!edgeId) continue;
		const targetNodeId = String((edge as any)?.target ?? '').trim();
		if (!targetNodeId) continue;
		const handle = String((edge as any)?.targetHandle ?? 'in').trim() || 'in';
		const metric = parseEdgeMetric(edgeMetrics, `${edgeId}:${handle}`);
		const depth = Math.max(0, Number(metric.depth ?? 0));
		const current = out.get(targetNodeId) ?? 0;
		if (depth > current) out.set(targetNodeId, depth);
	}
	return out;
}

function normalizeMaxInflightSource(raw: string): BlockerCode {
	const source = raw.trim().toLowerCase();
	if (source === 'global') return 'MAX_INFLIGHT_REACHED:global';
	if (source === 'provider') return 'MAX_INFLIGHT_REACHED:provider';
	if (source === 'model') return 'MAX_INFLIGHT_REACHED:model';
	return 'MAX_INFLIGHT_REACHED:node';
}

function blockerCodeFromReason(reasonCodeRaw: string): BlockerCode | null {
	const reasonCode = reasonCodeRaw.trim();
	if (!reasonCode) return null;
	if (reasonCode.startsWith('MAX_INFLIGHT_REACHED:')) return normalizeMaxInflightSource(reasonCode.split(':', 2)[1] ?? 'node');
	if (reasonCode === 'MAX_INFLIGHT_REACHED') return 'MAX_INFLIGHT_REACHED:node';
	if (reasonCode === 'WAITING_REQUIRED_INPUT') return 'WAITING_REQUIRED_INPUT';
	if (reasonCode === 'DEPENDENCY_NOT_READY') return 'DEPENDENCY_NOT_READY';
	if (reasonCode === 'LEASE_UNAVAILABLE') return 'LEASE_UNAVAILABLE';
	if (reasonCode.startsWith('WAITING_REQUIRED_') || reasonCode.startsWith('HANDLE_INPUT_')) return 'WAITING_REQUIRED_INPUT';
	return null;
}

function phaseFromContext(input: {
	lifecycle: NodeLifecycleStatus;
	isLlmHolder: boolean;
	isLlmWaiting: boolean;
	hasBlocker: boolean;
	readyWork: boolean;
	pendingInputCount: number;
}): PhaseCode | null {
	if (input.lifecycle === 'completed' || input.lifecycle === 'failed' || input.lifecycle === 'canceled' || input.lifecycle === 'skipped') {
		return 'TERMINAL';
	}
	if (input.isLlmHolder) return 'AWAITING_PROVIDER_RESPONSE';
	if (input.isLlmWaiting) return 'AWAITING_LEASE';
	if (input.pendingInputCount > 0 || input.lifecycle === 'waiting') return 'AWAITING_INPUT';
	if (input.hasBlocker || input.readyWork || input.lifecycle === 'running') return 'AWAITING_DISPATCH';
	return null;
}

export function buildRunMonitorNodeRows(input: RunMonitorProjectionInput): RunMonitorNodeRow[] {
	const nodes = input?.nodes ?? [];
	const edges = input?.edges ?? [];
	const nodeBindings = asRecord(input?.nodeBindings);
	const queueRuntime = asRecord(input?.queueRuntime);
	const snapshot = asRecord(queueRuntime.schedulerSnapshot) as SchedulerSnapshot;
	const blockedByNode = asRecord(queueRuntime.blockedByNode) as BlockedByNode;
	const controlPlaneNodeState = asRecord(queueRuntime.controlPlaneNodeState) as ControlPlaneNodeState;
	const controlPlaneEdgeState = asRecord(queueRuntime.controlPlaneEdgeState);
	const llmLease = asRecord(queueRuntime.llmLease) as LlmLease;
	const runtimeNodeCounters = parseRuntimeNodeCounters(queueRuntime);
	const perNodeMap = parseSchedulerPerNode(snapshot);
	const edgeMetrics = asRecord(asRecord(queueRuntime.metrics).edges);
	const inboundDepthByNode = buildInboundDepthByNode(edges, edgeMetrics);

	const llmState = String(llmLease?.state ?? '').trim().toLowerCase();
	const llmHolderNodeId = String(llmLease?.holderNodeId ?? '').trim();
	const runIsActive = (input?.runStatus ?? 'idle') === 'running';
	const llmActiveNodeIds = new Set(
		asArray<unknown>(llmLease?.activeNodeIds)
			.map((value) => String(value ?? '').trim())
			.filter(Boolean)
	);
	const llmActorNodeId = String(llmLease?.nodeId ?? '').trim();
	const llmWaitingNodeIds = new Set(
		asArray<unknown>(llmLease?.waitingNodeIds)
			.map((value) => String(value ?? '').trim())
			.filter(Boolean)
	);

	return nodes.map((node) => {
		const nodeId = String(node?.id ?? '').trim();
		const schedulerRow = perNodeMap.get(nodeId);
		const projection = projectNodeStatus(nodeBindings[nodeId] as any);
		const blockedRow = asRecord(blockedByNode[nodeId]);
		const controlNodeState = asRecord(controlPlaneNodeState[nodeId]);
		const blockedReasonCode = String(blockedRow.reasonCode ?? '').trim();
		const terminalReasonCode = String(controlNodeState.terminalReasonCode ?? '').trim();
		const isTerminalized = terminalReasonCode.length > 0;
		const blockedHandle = String(blockedRow.handle ?? '').trim();
		const blockedPlaneRaw = String(blockedRow.plane ?? '').trim().toLowerCase();
		const blockedPlane =
			blockedPlaneRaw === 'work' || blockedPlaneRaw === 'param' || blockedPlaneRaw === 'control'
				? (blockedPlaneRaw as 'work' | 'param' | 'control')
				: null;
		const pendingInputCount = Math.max(0, Number(schedulerRow?.pendingInputCount ?? 0));
		const inflight = Math.max(0, Number(schedulerRow?.inflight ?? 0));
		const processingPolicy = asRecord((node?.data as any)?.processingPolicy);
		const consumeModeRaw = String(processingPolicy.consume_mode ?? 'once').trim().toLowerCase();
		const consumeMode: 'once' | 'single_item' | 'batch' =
			consumeModeRaw === 'single_item' || consumeModeRaw === 'batch' ? consumeModeRaw : 'once';
		const lifecycle = reconcileLifecycleForActiveRun({
			lifecycle: projection.lifecycle,
			consumeMode,
			runStatus: input?.runStatus ?? 'idle',
			inflight,
			pendingInputCount,
			readyWork: Boolean(schedulerRow?.readyWork ?? false),
			blockedReasonCode,
			isTerminalized
		});
		const nodeCounter = runtimeNodeCounters.get(nodeId);
		const acceptedCount = Math.max(0, Number(nodeCounter?.accepted ?? 0));
		const rejectedCount = Math.max(0, Number(nodeCounter?.rejected ?? 0));
		const canReflectLeaseTelemetry =
			runIsActive &&
			(projection.execution === 'running' || projection.lifecycle === 'running' || inflight > 0);
		const isLlmHolder =
			canReflectLeaseTelemetry &&
			nodeId.length > 0 &&
			(llmActiveNodeIds.has(nodeId) || (llmState !== 'released' && llmHolderNodeId === nodeId));
		const isLlmWaiting =
			canReflectLeaseTelemetry &&
			!isLlmHolder &&
			(llmWaitingNodeIds.has(nodeId) ||
				(llmState === 'waiting' && nodeId.length > 0 && llmActorNodeId === nodeId));
		const schedulerBlockedReasonCode = String(schedulerRow?.lastBlockedReasonCode ?? '').trim();
		const canUseSchedulerBlockedReason =
			!blockedReasonCode &&
			inflight === 0 &&
			(lifecycle === 'waiting' || lifecycle === 'blocked' || pendingInputCount > 0) &&
			!isLlmHolder;
		const effectiveBlockedCodeForCurrent = isTerminalized
			? ''
			: isLlmHolder
			? ''
			: blockedReasonCode || (canUseSchedulerBlockedReason ? schedulerBlockedReasonCode : '');
		const blockerCode =
			blockerCodeFromReason(effectiveBlockedCodeForCurrent) ??
			(isLlmWaiting ? 'LEASE_UNAVAILABLE' : null);
		const blocker: MonitorBlocker | null = blockerCode
			? {
					code: blockerCode,
					detail: blockerCode.startsWith('MAX_INFLIGHT_REACHED:')
						? { source: blockerCode.split(':', 2)[1] as BlockerDetail['source'] }
						: undefined,
					// Use only the per-node blockedRow timestamp — never the shared
					// scheduler snapshot timestamp, which would reset all unblocked nodes
					// simultaneously on every scheduler tick (Issue 2).
					since: String(blockedRow.updatedAt ?? '').trim() || undefined
			  }
			: null;
		const lastBlockerCode = blockerCodeFromReason(schedulerBlockedReasonCode);
		const lastBlocker: MonitorLastBlocker | null =
			!blocker && lastBlockerCode
				? {
						code: lastBlockerCode,
						detail: lastBlockerCode.startsWith('MAX_INFLIGHT_REACHED:')
							? { source: lastBlockerCode.split(':', 2)[1] as BlockerDetail['source'] }
							: undefined,
						// clearedAt must NOT use snapshot?.updatedAt — that global clock
						// updates on every scheduler tick and makes every previously-blocked
						// node appear to have just cleared (Issue 3).
						// blockedRow.updatedAt is absent once the entry is deleted, so we
						// leave clearedAt undefined when a precise timestamp is unavailable.
						clearedAt: String(blockedRow.updatedAt ?? '').trim() || undefined
				  }
				: null;
		const phase = phaseFromContext({
			lifecycle,
			isLlmHolder,
			isLlmWaiting,
			hasBlocker: Boolean(blocker),
			readyWork: Boolean(schedulerRow?.readyWork ?? false),
			pendingInputCount
		});
		// phaseSince: use the most precise available per-node timestamp.
		// For LLM phases use the lease event timestamp; for blocked phases use the
		// blocked-entry timestamp. Never fall back to snapshot?.updatedAt — that is
		// a shared global clock that would reset all nodes simultaneously (Issue 2).
		const phaseSince = (() => {
			if (blockedRow.updatedAt) return String(blockedRow.updatedAt).trim() || null;
			if (isLlmHolder || isLlmWaiting) {
				return String((llmLease as any)?.updatedAt ?? '').trim() || null;
			}
			return null;
		})();
		const blockerHistory = (() => {
			const historyRaw = asArray<Record<string, unknown>>((blockedRow as any)?.history);
			if (historyRaw.length > 0) {
				return historyRaw
					.map((entry) => ({
						code: String(entry.code ?? '').trim(),
						at: String(entry.at ?? '').trim(),
						action: String(entry.action ?? '').trim() === 'cleared' ? 'cleared' : 'set'
					}))
					.filter((entry) => entry.code && entry.at)
					.slice(-BLOCKER_HISTORY_MAX) as MonitorBlockerHistoryEntry[];
			}
			const synthetic: MonitorBlockerHistoryEntry[] = [];
			if (blocker?.code) synthetic.push({ code: blocker.code, at: blocker.since ?? new Date().toISOString(), action: 'set' });
			if (lastBlocker?.code) synthetic.push({ code: lastBlocker.code, at: lastBlocker.clearedAt ?? new Date().toISOString(), action: 'cleared' });
			return synthetic.slice(-BLOCKER_HISTORY_MAX);
		})();

		// Priority: blocked reason > llm-wait > llm-hold > stale (when idle/waiting) > ""
		// Never emit the literal "-" — an empty string means "nothing to explain".
		const effectiveBlockedCode = effectiveBlockedCodeForCurrent;
		let displayReason = '';
		if (effectiveBlockedCode) {
			displayReason = effectiveBlockedCode;
		} else if (isLlmWaiting) {
			displayReason = 'llm-wait';
		} else if (isLlmHolder) {
			displayReason = 'llm-hold';
		} else if (
			projection.freshness === 'stale' &&
			lifecycle !== 'running' &&
			lifecycle !== 'failed' &&
			lifecycle !== 'canceled' &&
			lifecycle !== 'skipped'
		) {
			displayReason = 'stale';
		}

		const projectedLifecycle = projection.lifecycle;
		const inboundWorkEdges = edges.filter((edge) => {
			const targetId = String(edge?.target ?? '').trim();
			if (targetId !== nodeId) return false;
			const mode = String((edge.data as any)?.mode ?? 'work').trim().toLowerCase();
			return mode === 'work';
		});
		const allImmediateInboundWorkEdgesClosed =
			inboundWorkEdges.length === 0 ||
			inboundWorkEdges.every((edge) => {
				const edgeId = String(edge?.id ?? '').trim();
				if (!edgeId) return false;
				const controlState = asRecord(controlPlaneEdgeState[edgeId]);
				return Boolean(controlState.closed);
			});
		const terminalEligibleWaitingState =
			lifecycle === 'waiting' &&
			pendingInputCount === 0 &&
			inflight === 0 &&
			!isLlmHolder &&
			allImmediateInboundWorkEdgesClosed;
		const statusParityMismatch =
			(projectedLifecycle === 'completed' && terminalEligibleWaitingState) ||
			(projectedLifecycle === 'waiting' && lifecycle === 'completed') ||
			(projectedLifecycle === 'running' && lifecycle !== 'running');

		return {
			nodeId,
			label: nodeLabel(node),
			status: projection.display,
			lifecycle,
			execution: projection.execution,
			freshness: projection.freshness,
			consumeMode,
			acceptedCount,
			rejectedCount,
			totalProcessed: acceptedCount + rejectedCount,
			pendingInputCount,
			inflight,
			inboundDepth: Math.max(0, Number(inboundDepthByNode.get(nodeId) ?? 0)),
			readyWork: Boolean(schedulerRow?.readyWork ?? false),
			// Use effectiveBlockedCodeForCurrent (not the raw blockedByNode entry) so
			// that scheduler-only reasons (e.g. WAITING_REQUIRED_PARAM carried solely in
			// schedulerSnapshot.perNode[].lastBlockedReasonCode) are surfaced when the
			// node is genuinely waiting.  canUseSchedulerBlockedReason already guards
			// against Issue 5 (running nodes): it requires inflight === 0 and a matching
			// lifecycle/pendingInputCount, so inflight > 0 nodes are never affected.
			blockedReasonCode: effectiveBlockedCodeForCurrent || null,
			blockedHandle: blockedHandle || null,
			blockedPlane,
			updatedAt: String(blockedRow.updatedAt ?? '').trim() || null,
			terminalReasonCode: terminalReasonCode || null,
			isBlocked: Boolean(effectiveBlockedCodeForCurrent),
			isWaiting: pendingInputCount > 0 && inflight === 0,
			isLlmHolder,
			isLlmWaiting,
			phase,
			phaseSince,
			blocker,
			lastBlocker,
			blockerHistory,
			displayReason,
			statusParityMismatch
		};
	});
}

function nodeLabelByIdMap(nodes: Node<PipelineNodeData & Record<string, unknown>>[]): Map<string, string> {
	const out = new Map<string, string>();
	for (const node of nodes) {
		const nodeId = String(node?.id ?? '').trim();
		if (!nodeId) continue;
		out.set(nodeId, nodeLabel(node));
	}
	return out;
}

export function buildRunMonitorEdgeRows(input: RunMonitorProjectionInput): RunMonitorEdgeRow[] {
	const edges = input?.edges ?? [];
	const queueRuntime = asRecord(input?.queueRuntime);
	const edgeMetrics = asRecord(asRecord(queueRuntime.metrics).edges);
	const labelById = nodeLabelByIdMap(input?.nodes ?? []);
	const out: RunMonitorEdgeRow[] = [];
	for (const edge of edges) {
		const edgeId = String(edge?.id ?? '').trim();
		if (!edgeId) continue;
		const sourceNodeId = String((edge as any)?.source ?? '').trim();
		const targetNodeId = String((edge as any)?.target ?? '').trim();
		const handle = String((edge as any)?.targetHandle ?? 'in').trim() || 'in';
		const metric = parseEdgeMetric(edgeMetrics, `${edgeId}:${handle}`);
		const edgeStatus = projectEdgeStatus({
			exec: (edge?.data as any)?.exec,
			mode: (edge?.data as any)?.mode,
			depth: metric.depth,
			blocked: metric.blocked,
			full: metric.full
		});
		const depth = edgeStatus.diagnostics.depth;
		const blocked = edgeStatus.diagnostics.blocked;
		const full = edgeStatus.diagnostics.full;
		const rawAge = metric.oldestAgeSec;
		const oldestAgeSec = Number.isFinite(Number(rawAge)) ? Math.max(0, Number(rawAge)) : null;
		out.push({
			edgeId,
			handle,
			sourceNodeId,
			sourceLabel: String((labelById.get(sourceNodeId) ?? sourceNodeId) || '(unknown node)'),
			targetNodeId,
			targetLabel: String((labelById.get(targetNodeId) ?? targetNodeId) || '(unknown node)'),
			lifecycle: edgeStatus.lifecycle,
			exec: edgeStatus.exec,
			depth,
			blocked,
			full,
			oldestAgeSec
		});
	}
	out.sort((a, b) => b.depth - a.depth || a.edgeId.localeCompare(b.edgeId));
	return out;
}

export function filterAndSortRunMonitorNodes(
	rows: RunMonitorNodeRow[],
	filter: RunMonitorFilter,
	sort: RunMonitorSort,
	globalStalled: boolean
): RunMonitorNodeRow[] {
	let next = rows.slice();
	if (filter === 'blocked') {
		next = next.filter((row) => row.isBlocked);
	} else if (filter === 'waiting') {
		next = next.filter((row) => row.isWaiting);
	} else if (filter === 'stalled') {
		next = globalStalled ? next.filter((row) => row.isBlocked || row.isWaiting) : [];
	}

	const withIndex = next.map((row, index) => ({ row, index }));
	withIndex.sort((left, right) => {
		if (sort === 'pending_desc') {
			return (
				right.row.pendingInputCount - left.row.pendingInputCount ||
				right.row.inboundDepth - left.row.inboundDepth ||
				left.row.label.localeCompare(right.row.label) ||
				left.index - right.index
			);
		}
		if (sort === 'pending_asc') {
			return (
				left.row.pendingInputCount - right.row.pendingInputCount ||
				right.row.inboundDepth - left.row.inboundDepth ||
				left.row.label.localeCompare(right.row.label) ||
				left.index - right.index
			);
		}
		if (sort === 'depth_asc') {
			return (
				left.row.inboundDepth - right.row.inboundDepth ||
				right.row.pendingInputCount - left.row.pendingInputCount ||
				left.row.label.localeCompare(right.row.label) ||
				left.index - right.index
			);
		}
		if (sort === 'label_asc') {
			return (
				left.row.label.localeCompare(right.row.label) ||
				right.row.pendingInputCount - left.row.pendingInputCount ||
				right.row.inboundDepth - left.row.inboundDepth ||
				left.index - right.index
			);
		}
		return (
			right.row.inboundDepth - left.row.inboundDepth ||
			right.row.pendingInputCount - left.row.pendingInputCount ||
			left.row.label.localeCompare(right.row.label) ||
			left.index - right.index
		);
	});
	return withIndex.map((entry) => entry.row);
}

export function classifyNodeToGroup(row: RunMonitorNodeRow): MonitorGroupKey {
	const lifecycle = String(row?.lifecycle ?? '').trim().toLowerCase();
	const phase = String(row?.phase ?? '').trim().toUpperCase();
	if (lifecycle === 'completed' || lifecycle === 'failed' || lifecycle === 'canceled' || lifecycle === 'skipped') {
		return 'done';
	}
	if (row?.isLlmHolder || row?.isLlmWaiting || Number(row?.inflight ?? 0) > 0) {
		return 'active';
	}
	if (lifecycle === 'running' || lifecycle === 'active') return 'active';
	if (
		phase === 'AWAITING_DISPATCH' ||
		phase === 'AWAITING_LEASE' ||
		phase === 'AWAITING_PROVIDER_RESPONSE' ||
		phase === 'POSTPROCESSING' ||
		phase === 'WRITING_OUTPUT'
	) {
		return 'active';
	}
	if (lifecycle === 'waiting' || lifecycle === 'blocked' || lifecycle === 'paused') return 'waiting';
	if (row?.isBlocked || row?.isWaiting) return 'waiting';
	if (phase === 'AWAITING_INPUT') return 'waiting';
	return 'pending';
}

function emptyNodeGroup(key: MonitorGroupKey, label: string): MonitorNodeGroup {
	return {
		key,
		label,
		rows: [],
		totalCount: 0,
		runningCount: 0,
		throttledCount: 0,
		waitingCount: 0,
		pausedCount: 0,
		notStartedCount: 0,
		completedCount: 0,
		failedCount: 0,
		canceledCount: 0
	};
}

function summarizeGroup(rows: RunMonitorNodeRow[], key: MonitorGroupKey, label: string): MonitorNodeGroup {
	const group = emptyNodeGroup(key, label);
	group.rows = rows;
	group.totalCount = rows.length;
	for (const row of rows) {
		const lifecycle = String(row.lifecycle ?? '').trim().toLowerCase();
		if (key === 'active') {
			if (Number(row.inflight ?? 0) > 0 || row.isLlmHolder) group.runningCount += 1;
			if (String(row.phase ?? '').trim().toUpperCase() === 'AWAITING_DISPATCH' && row.blocker) {
				group.throttledCount += 1;
			}
		}
		if (key === 'waiting') {
			if (lifecycle === 'paused') group.pausedCount += 1;
			else group.waitingCount += 1;
		}
		if (key === 'pending') {
			group.notStartedCount += 1;
		}
		if (key === 'done') {
			if (lifecycle === 'failed') group.failedCount += 1;
			else if (lifecycle === 'canceled') group.canceledCount += 1;
			else group.completedCount += 1;
		}
	}
	return group;
}

export function groupMonitorNodeRows(
	rows: RunMonitorNodeRow[],
	filter: RunMonitorFilter,
	sort: RunMonitorSort,
	globalStalled: boolean
): MonitorGroupedNodes {
	const allRows = Array.isArray(rows) ? rows : [];
	const activeRows: RunMonitorNodeRow[] = [];
	const waitingRows: RunMonitorNodeRow[] = [];
	const pendingRows: RunMonitorNodeRow[] = [];
	const doneRows: RunMonitorNodeRow[] = [];
	for (const row of allRows) {
		const groupKey = classifyNodeToGroup(row);
		if (groupKey === 'active') activeRows.push(row);
		else if (groupKey === 'waiting') waitingRows.push(row);
		else if (groupKey === 'pending') pendingRows.push(row);
		else doneRows.push(row);
	}
	const active = summarizeGroup(
		filterAndSortRunMonitorNodes(activeRows, filter, sort, globalStalled),
		'active',
		'Active'
	);
	const waiting = summarizeGroup(
		filterAndSortRunMonitorNodes(waitingRows, filter, sort, globalStalled),
		'waiting',
		'Waiting'
	);
	const pending = summarizeGroup(
		filterAndSortRunMonitorNodes(pendingRows, filter, sort, globalStalled),
		'pending',
		'Pending'
	);
	const done = summarizeGroup(filterAndSortRunMonitorNodes(doneRows, filter, sort, globalStalled), 'done', 'Done');
	const groups: MonitorNodeGroup[] = [active, waiting, pending, done];
	return {
		groups,
		totalNodeCount: groups.reduce((sum, group) => sum + group.totalCount, 0),
		hasFailures: groups.some((group) => group.failedCount > 0),
		activeGroupIndex: 0,
		waitingGroupIndex: 1,
		pendingGroupIndex: 2,
		doneGroupIndex: 3
	};
}

export function headerSummary(group: MonitorNodeGroup): string {
	if (!group || group.totalCount <= 0) return '0';
	if (group.key === 'active') {
		const parts: string[] = [];
		if (group.runningCount > 0) parts.push(`${group.runningCount} running`);
		if (group.throttledCount > 0) parts.push(`${group.throttledCount} throttled`);
		return parts.join(' | ') || `${group.totalCount} active`;
	}
	if (group.key === 'waiting') {
		const parts: string[] = [];
		const waitingOnly = Math.max(0, group.waitingCount);
		if (waitingOnly > 0) parts.push(`${waitingOnly} waiting`);
		if (group.pausedCount > 0) parts.push(`${group.pausedCount} paused`);
		return parts.join(' | ') || `${group.totalCount} waiting`;
	}
	if (group.key === 'pending') {
		return `${group.totalCount} not started`;
	}
	const parts: string[] = [];
	if (group.completedCount > 0) parts.push(`${group.completedCount} completed`);
	if (group.failedCount > 0) parts.push(`${group.failedCount} failed !`);
	if (group.canceledCount > 0) parts.push(`${group.canceledCount} canceled`);
	return parts.join(' | ') || `${group.totalCount} done`;
}

export function preferredMonitorEdgeFocusNodeId(sourceNodeId: string, targetNodeId: string): string {
	const target = String(targetNodeId ?? '').trim();
	if (target) return target;
	return String(sourceNodeId ?? '').trim();
}

export function edgeStatusesForFilter(row: RunMonitorEdgeRow): RunMonitorEdgeFilter[] {
	const statuses: RunMonitorEdgeFilter[] = [];
	if (row.lifecycle === 'running') {
		statuses.push('running', 'active');
	} else if (row.lifecycle === 'waiting') {
		statuses.push('waiting');
	} else if (row.lifecycle === 'done') {
		statuses.push('done');
	} else {
		statuses.push('inactive');
	}
	if (row.blocked) statuses.push('blocked');
	if (row.full) statuses.push('full');
	return Array.from(new Set(statuses));
}

export function filterRunMonitorEdgeRows(
	rows: RunMonitorEdgeRow[],
	filters: RunMonitorEdgeFilter[]
): RunMonitorEdgeRow[] {
	const normalizedRows = Array.isArray(rows) ? rows : [];
	const normalizedFilters = (Array.isArray(filters) ? filters : []).filter(Boolean);
	if (normalizedFilters.length === 0) return normalizedRows;
	return normalizedRows.filter((row) => {
		const statuses = edgeStatusesForFilter(row);
		return statuses.some((status) => normalizedFilters.includes(status));
	});
}

export function buildRunMonitorAdaptiveDecisionRows(
	queueRuntime: unknown
): RunMonitorAdaptiveDecisionRow[] {
	const runtime = asRecord(queueRuntime);
	const rows = Array.isArray((runtime as any)?.adaptiveDecisions)
		? (((runtime as any).adaptiveDecisions as unknown[]) ?? [])
		: [];
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
	const toCaps = (value: unknown): Record<string, number> => {
		if (!value || typeof value !== 'object') return {};
		const out: Record<string, number> = {};
		for (const [key, raw] of Object.entries(value as Record<string, unknown>)) {
			const n = Number(raw ?? 0);
			if (!Number.isFinite(n)) continue;
			out[String(key)] = n;
		}
		return out;
	};
	const projected = rows
		.map((raw) => {
			const row = asRecord(raw);
			const inputs =
				row.inputs && typeof row.inputs === 'object'
					? ({ ...(row.inputs as Record<string, unknown>) } as Record<string, unknown>)
					: {};
			const reasons = asArray<unknown>(row.reasons)
				.map((value) => String(value ?? '').trim())
				.filter(Boolean);
			const changedCaps = toChangedCaps(row.changedCaps);
			const effectiveCaps = toCaps(row.effectiveCaps);
			const explanation = explainAdaptiveDecision({
				enforced: Boolean(row.enforced ?? false),
				reasons,
				changedCaps,
				inputs,
				effectiveCaps
			});
			return {
				at: String(row.at ?? '').trim(),
				runId: String(row.runId ?? '').trim(),
				mode: String(row.mode ?? '').trim() || 'off',
				modeSource: String(row.modeSource ?? '').trim() || 'env',
				enforced: Boolean(row.enforced ?? false),
				inputs,
				reasons,
				changedCaps,
				hardCaps: toCaps(row.hardCaps),
				minCaps: toCaps(row.minCaps),
				proposedCaps: toCaps(row.proposedCaps),
				effectiveCaps,
				explanation
			} as RunMonitorAdaptiveDecisionRow;
		})
		.filter((row) => row.runId.length > 0)
		.sort((a, b) => b.at.localeCompare(a.at));
	for (let index = 0; index < projected.length; index += 1) {
		const current = projected[index];
		const previous = projected[index + 1];
		if (!previous) continue;
		const capDelta: Record<string, { from: number; to: number }> = {};
		for (const key of Array.from(
			new Set([...Object.keys(current.effectiveCaps), ...Object.keys(previous.effectiveCaps)])
		)) {
			const from = Number(previous.effectiveCaps[key] ?? NaN);
			const to = Number(current.effectiveCaps[key] ?? NaN);
			if (!Number.isFinite(from) || !Number.isFinite(to) || from === to) continue;
			capDelta[key] = { from, to };
		}
		const prevReasons = new Set(previous.reasons.map((reason) => reason.toLowerCase()));
		const currentReasons = new Set(current.reasons.map((reason) => reason.toLowerCase()));
		const reasonsAdded = Array.from(currentReasons).filter((reason) => !prevReasons.has(reason));
		const reasonsRemoved = Array.from(prevReasons).filter((reason) => !currentReasons.has(reason));
		current.diffFromPrevious = {
			modeChanged: String(current.mode ?? '') !== String(previous.mode ?? ''),
			scoreDelta: Number(current.explanation.score ?? 0) - Number(previous.explanation.score ?? 0),
			capDelta,
			reasonsAdded,
			reasonsRemoved
		};
	}
	return projected;
}

export function explainAdaptiveDecision(input: {
	enforced: boolean;
	reasons: string[];
	changedCaps: Record<string, { from: number; to: number }>;
	inputs: Record<string, unknown>;
	effectiveCaps: Record<string, number>;
}): {
	score: number;
	severity: 'low' | 'medium' | 'high';
	signals: string[];
	components: Array<{ label: string; delta: number }>;
} {
	const signals: string[] = [];
	const components: Array<{ label: string; delta: number }> = [];
	let score = 0;
	const addComponent = (label: string, deltaRaw: number): void => {
		const delta = Number(deltaRaw ?? 0);
		if (!Number.isFinite(delta) || delta === 0) return;
		score += delta;
		components.push({ label: String(label ?? ''), delta: Number(delta.toFixed(2)) });
	};
	if (input.enforced) {
		addComponent('enforced_mode', 20);
		signals.push('enforced_mode');
	}
	const changedEntries = Object.entries(input.changedCaps ?? {});
	if (changedEntries.length > 0) {
		addComponent('changed_caps', Math.min(24, changedEntries.length * 8));
		signals.push(`changed_caps=${changedEntries.length}`);
	}
	let totalDelta = 0;
	for (const [, delta] of changedEntries) {
		const from = Number(delta?.from ?? 0);
		const to = Number(delta?.to ?? 0);
		if (!Number.isFinite(from) || !Number.isFinite(to)) continue;
		totalDelta += Math.abs(from - to);
	}
	if (totalDelta > 0) {
		addComponent('cap_delta_sum', Math.min(20, totalDelta * 3));
		signals.push(`delta_sum=${totalDelta}`);
	}
	const reasonWeights: Record<string, number> = {
		queue_depth_high: 15,
		queue_pressure: 12,
		failure_rate_high: 20,
		error_rate_high: 20,
		lease_wait_high: 10,
		recovery: 6
	};
	for (const reasonRaw of input.reasons ?? []) {
		const reason = String(reasonRaw ?? '').trim().toLowerCase();
		if (!reason) continue;
		const weighted = Number(reasonWeights[reason] ?? 5);
		addComponent(`reason:${reason}`, weighted);
		signals.push(`reason:${reason}`);
	}
	const queueDepth = Number((input.inputs ?? {}).queueDepth ?? (input.inputs ?? {}).pendingQueueDepth ?? 0);
	if (Number.isFinite(queueDepth) && queueDepth > 0) {
		let queueDelta = 0;
		if (queueDepth >= 20) queueDelta = 10;
		else if (queueDepth >= 10) queueDelta = 6;
		else if (queueDepth >= 5) queueDelta = 3;
		addComponent('queue_depth', queueDelta);
		signals.push(`queue_depth=${queueDepth}`);
	}
	const failureRate = Number((input.inputs ?? {}).failureRate ?? (input.inputs ?? {}).errorRate ?? 0);
	if (Number.isFinite(failureRate) && failureRate > 0) {
		let failureDelta = 0;
		if (failureRate >= 0.2) failureDelta = 15;
		else if (failureRate >= 0.1) failureDelta = 10;
		else if (failureRate >= 0.05) failureDelta = 5;
		addComponent('failure_rate', failureDelta);
		signals.push(`failure_rate=${failureRate.toFixed(3)}`);
	}
	const leaseWaitMs = Number((input.inputs ?? {}).leaseWaitMs ?? 0);
	if (Number.isFinite(leaseWaitMs) && leaseWaitMs > 0) {
		let leaseDelta = 0;
		if (leaseWaitMs >= 5000) leaseDelta = 8;
		else if (leaseWaitMs >= 2000) leaseDelta = 5;
		else if (leaseWaitMs >= 1000) leaseDelta = 3;
		addComponent('lease_wait_ms', leaseDelta);
		signals.push(`lease_wait_ms=${leaseWaitMs}`);
	}
	const bounded = Math.max(0, Math.min(100, Math.round(score)));
	const severity: 'low' | 'medium' | 'high' =
		bounded >= 70 ? 'high' : bounded >= 40 ? 'medium' : 'low';
	return { score: bounded, severity, signals, components };
}

export function buildAdaptiveComponentBreakdown(
	components: Array<{ label?: string; delta?: number }>
): RunMonitorAdaptiveComponentBreakdownItem[] {
	const normalized = (Array.isArray(components) ? components : [])
		.map((item) => {
			const label = String(item?.label ?? '').trim();
			const delta = Number(item?.delta ?? NaN);
			if (!label || !Number.isFinite(delta) || delta === 0) return null;
			return { label, delta };
		})
		.filter((item): item is { label: string; delta: number } => item !== null)
		.sort((a, b) => Math.abs(b.delta) - Math.abs(a.delta) || a.label.localeCompare(b.label));
	if (normalized.length === 0) return [];
	const maxAbs = Math.max(...normalized.map((item) => Math.abs(item.delta)), 1e-9);
	return normalized.map((item) => {
		const absDelta = Math.abs(item.delta);
		const percentOfMax = Math.max(0, Math.min(100, (absDelta / maxAbs) * 100));
		return {
			label: item.label,
			delta: item.delta,
			absDelta,
			percentOfMax,
			direction: item.delta >= 0 ? 'up' : 'down'
		};
	});
}

export function summarizeAdaptiveDecisionRows(
	rows: RunMonitorAdaptiveDecisionRow[]
): RunMonitorAdaptiveDecisionSummary {
	const out: RunMonitorAdaptiveDecisionSummary = {
		total: 0,
		enforced: 0,
		byMode: {},
		bySeverity: { low: 0, medium: 0, high: 0 }
	};
	const normalized = Array.isArray(rows) ? rows : [];
	for (const row of normalized) {
		out.total += 1;
		if (row.enforced) out.enforced += 1;
		const mode = String(row.mode ?? '').trim().toLowerCase() || 'unknown';
		out.byMode[mode] = Number(out.byMode[mode] ?? 0) + 1;
		const severityRaw = String(row.explanation?.severity ?? 'low').trim().toLowerCase();
		const severity: 'low' | 'medium' | 'high' =
			severityRaw === 'high' ? 'high' : severityRaw === 'medium' ? 'medium' : 'low';
		out.bySeverity[severity] += 1;
	}
	return out;
}

export function buildTrendSparkline(
	points: Array<{ createdAt?: string; value?: number }>,
	options?: { width?: number; height?: number }
): RunMonitorTrendSparkline | null {
	const normalized = (Array.isArray(points) ? points : [])
		.map((point) => ({
			createdAt: String(point?.createdAt ?? '').trim(),
			value: Number(point?.value ?? NaN)
		}))
		.filter((point) => Number.isFinite(point.value));
	if (normalized.length < 2) return null;
	normalized.sort((a, b) => a.createdAt.localeCompare(b.createdAt));
	const width = Math.max(80, Number(options?.width ?? 520));
	const height = Math.max(40, Number(options?.height ?? 88));
	const values = normalized.map((point) => point.value);
	const minValue = Math.min(...values);
	const maxValue = Math.max(...values);
	const range = Math.max(1e-9, maxValue - minValue);
	const stepX = width / Math.max(1, normalized.length - 1);
	const toY = (value: number): number => {
		const ratio = (value - minValue) / range;
		return height - ratio * height;
	};
	const projectedPoints = normalized.map((point, index) => ({
		x: Number((index * stepX).toFixed(2)),
		y: Number(toY(point.value).toFixed(2)),
		value: point.value,
		createdAt: point.createdAt
	}));
	const path = projectedPoints
		.map((point, index) => `${index === 0 ? 'M' : 'L'} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`)
		.join(' ');
	const firstValue = normalized[0].value;
	const lastValue = normalized[normalized.length - 1].value;
	const meanValue = values.reduce((sum, value) => sum + value, 0) / Math.max(1, values.length);
	const deltaValue = lastValue - firstValue;
	const deltaPct = firstValue === 0 ? null : (deltaValue / firstValue) * 100.0;
	return {
		path,
		width,
		height,
		points: projectedPoints,
		baselines: {
			firstValueY: Number(toY(firstValue).toFixed(2)),
			meanValueY: Number(toY(meanValue).toFixed(2))
		},
		minValue,
		maxValue,
		lastValue,
		deltaValue,
		deltaPct,
		pointsCount: normalized.length
	};
}

export function filterRunMonitorAdaptiveDecisionRows(
	rows: RunMonitorAdaptiveDecisionRow[],
	modeFilter: RunMonitorAdaptiveModeFilter,
	modeSourceFilter: 'all' | 'env' | 'run_override',
	severityFilter: RunMonitorAdaptiveSeverityFilter,
	changedOnly: boolean = false,
	minScore: number = 0
): RunMonitorAdaptiveDecisionRow[] {
	const mode = String(modeFilter ?? 'all').trim().toLowerCase();
	const modeSource = String(modeSourceFilter ?? 'all').trim().toLowerCase();
	const severity = String(severityFilter ?? 'all').trim().toLowerCase();
	const normalizedMinScore = Math.max(0, Number(minScore ?? 0));
	return (Array.isArray(rows) ? rows : []).filter((row) => {
		const rowMode = String(row?.mode ?? '').trim().toLowerCase();
		const rowModeSource = String(row?.modeSource ?? '').trim().toLowerCase() || 'env';
		const rowSeverity = String(row?.explanation?.severity ?? '').trim().toLowerCase();
		if (mode !== 'all' && rowMode !== mode) return false;
		if (modeSource !== 'all' && rowModeSource !== modeSource) return false;
		if (severity !== 'all' && rowSeverity !== severity) return false;
		if (Number(row?.explanation?.score ?? 0) < normalizedMinScore) return false;
		if (!Boolean(changedOnly)) return true;
		const diff = row?.diffFromPrevious;
		if (!diff || typeof diff !== 'object') return false;
		const capDelta = diff.capDelta && typeof diff.capDelta === 'object' ? diff.capDelta : {};
		const hasCapDelta = Object.keys(capDelta).length > 0;
		const reasonsAdded = Array.isArray(diff.reasonsAdded) ? diff.reasonsAdded : [];
		const reasonsRemoved = Array.isArray(diff.reasonsRemoved) ? diff.reasonsRemoved : [];
		return Boolean(
			diff.modeChanged ||
				Number(diff.scoreDelta ?? 0) !== 0 ||
				hasCapDelta ||
				reasonsAdded.length > 0 ||
				reasonsRemoved.length > 0
		);
	});
}

function _historyRunId(row: Record<string, unknown> | null | undefined): string {
	return String(row?.runId ?? '').trim();
}

export function resolveRunMonitorRegressionPair(
	historyRows: Record<string, unknown>[],
	overridePair?: RunMonitorRegressionPair | null
): RunMonitorRegressionPair {
	const rows = Array.isArray(historyRows) ? historyRows : [];
	const overrideRunId = String(overridePair?.runId ?? '').trim();
	const overrideBaselineRunId = String(overridePair?.baselineRunId ?? '').trim();
	if (overrideRunId && overrideBaselineRunId) {
		const runExists = rows.some((row) => _historyRunId(row) === overrideRunId);
		const baselineExists = rows.some((row) => _historyRunId(row) === overrideBaselineRunId);
		if (runExists && baselineExists) {
			return { runId: overrideRunId, baselineRunId: overrideBaselineRunId };
		}
	}
	if (rows.length < 2) return { runId: '', baselineRunId: '' };
	return {
		runId: _historyRunId(rows[0]),
		baselineRunId: _historyRunId(rows[1])
	};
}

export function pickRunMonitorRegressionPairFromHistory(
	historyRows: Record<string, unknown>[],
	index: number
): RunMonitorRegressionPair {
	const rows = Array.isArray(historyRows) ? historyRows : [];
	const idx = Number(index ?? -1);
	if (!Number.isInteger(idx) || idx < 0) return { runId: '', baselineRunId: '' };
	const runId = _historyRunId(rows[idx]);
	const baselineRunId = _historyRunId(rows[idx + 1]);
	if (!runId || !baselineRunId) return { runId: '', baselineRunId: '' };
	return { runId, baselineRunId };
}

export function buildRunMonitorTransitionRows(events: unknown[]): RunMonitorTransitionRow[] {
	return (Array.isArray(events) ? events : [])
		.map((eventRaw) => {
			const event = asRecord(eventRaw);
			const payload = asRecord(event.payload);
			const type = String(event.type ?? '').trim();
			return {
				id: Number(event.id ?? 0),
				runId: String(event.runId ?? '').trim(),
				type,
				at: String(event.at ?? '').trim(),
				entity: String(payload.entity ?? '').trim().toLowerCase(),
				entityId: String(payload.entityId ?? '').trim(),
				source: String(payload.source ?? '').trim(),
				target: String(payload.target ?? '').trim(),
				reasonCode: String(payload.reason ?? payload.code ?? '').trim(),
				isViolation: type.toLowerCase() === 'state_transition_violation'
			} as RunMonitorTransitionRow;
		})
		.filter((row) => row.type.length > 0)
		.sort((a, b) => {
			if (Number.isFinite(a.id) && Number.isFinite(b.id) && a.id !== b.id) return b.id - a.id;
			return String(b.at ?? '').localeCompare(String(a.at ?? ''));
		});
}

export function filterRunMonitorTransitionRows(
	rows: RunMonitorTransitionRow[],
	filter: RunMonitorTransitionFilter
): RunMonitorTransitionRow[] {
	const mode = String(filter ?? 'all').trim().toLowerCase();
	return (Array.isArray(rows) ? rows : []).filter((row) => {
		if (mode === 'run') return row.entity === 'run';
		if (mode === 'node') return row.entity === 'node';
		if (mode === 'violations') return row.isViolation;
		return true;
	});
}

