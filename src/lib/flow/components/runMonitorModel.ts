import type { Edge, Node } from '@xyflow/svelte';

import type { PipelineEdgeData, PipelineNodeData } from '$lib/flow/types';
import { displayStatusFromBinding } from '$lib/flow/store/runScope';
import { projectEdgeStatus, type EdgeLifecycleStatus } from '$lib/flow/store/statusModel';

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
	waitQueueLength?: unknown;
	waitingNodeIds?: unknown;
};

type BlockedByNode = Record<
	string,
	{
		nodeId: string;
		reasonCode: string;
		handle?: string;
		plane?: 'work' | 'param' | 'control';
		updatedAt?: string;
	}
>;

export type RunMonitorNodeRow = {
	nodeId: string;
	label: string;
	status: string;
	pendingInputCount: number;
	inflight: number;
	inboundDepth: number;
	readyWork: boolean;
	blockedReasonCode: string | null;
	blockedHandle: string | null;
	blockedPlane: 'work' | 'param' | 'control' | null;
	updatedAt: string | null;
	isBlocked: boolean;
	isWaiting: boolean;
	isLlmHolder: boolean;
	isLlmWaiting: boolean;
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

type RunMonitorProjectionInput = {
	nodes: Node<PipelineNodeData & Record<string, unknown>>[];
	edges: Edge<PipelineEdgeData & Record<string, unknown>>[];
	nodeBindings?: Record<string, unknown>;
	queueRuntime?: {
		metrics?: Record<string, unknown>;
		schedulerSnapshot?: SchedulerSnapshot;
		llmLease?: LlmLease;
		blockedByNode?: BlockedByNode;
	};
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

export function buildRunMonitorNodeRows(input: RunMonitorProjectionInput): RunMonitorNodeRow[] {
	const nodes = input?.nodes ?? [];
	const nodeBindings = asRecord(input?.nodeBindings);
	const queueRuntime = asRecord(input?.queueRuntime);
	const snapshot = asRecord(queueRuntime.schedulerSnapshot) as SchedulerSnapshot;
	const blockedByNode = asRecord(queueRuntime.blockedByNode) as BlockedByNode;
	const llmLease = asRecord(queueRuntime.llmLease) as LlmLease;
	const perNodeMap = parseSchedulerPerNode(snapshot);
	const edgeMetrics = asRecord(asRecord(queueRuntime.metrics).edges);
	const inboundDepthByNode = buildInboundDepthByNode(input?.edges ?? [], edgeMetrics);

	const llmState = String(llmLease?.state ?? '').trim().toLowerCase();
	const llmHolderNodeId = String(llmLease?.holderNodeId ?? '').trim();
	const llmActorNodeId = String(llmLease?.nodeId ?? '').trim();
	const llmWaitingNodeIds = new Set(
		asArray<unknown>(llmLease?.waitingNodeIds)
			.map((value) => String(value ?? '').trim())
			.filter(Boolean)
	);

	return nodes.map((node) => {
		const nodeId = String(node?.id ?? '').trim();
		const schedulerRow = perNodeMap.get(nodeId);
		const blockedRow = asRecord(blockedByNode[nodeId]);
		const blockedReasonCode = String(blockedRow.reasonCode ?? '').trim();
		const blockedHandle = String(blockedRow.handle ?? '').trim();
		const blockedPlaneRaw = String(blockedRow.plane ?? '').trim().toLowerCase();
		const blockedPlane =
			blockedPlaneRaw === 'work' || blockedPlaneRaw === 'param' || blockedPlaneRaw === 'control'
				? (blockedPlaneRaw as 'work' | 'param' | 'control')
				: null;
		const pendingInputCount = Math.max(0, Number(schedulerRow?.pendingInputCount ?? 0));
		const inflight = Math.max(0, Number(schedulerRow?.inflight ?? 0));
		const isLlmWaiting =
			llmWaitingNodeIds.has(nodeId) || (llmState === 'waiting' && nodeId.length > 0 && llmActorNodeId === nodeId);

		return {
			nodeId,
			label: nodeLabel(node),
			status: displayStatusFromBinding(nodeBindings[nodeId] as any),
			pendingInputCount,
			inflight,
			inboundDepth: Math.max(0, Number(inboundDepthByNode.get(nodeId) ?? 0)),
			readyWork: Boolean(schedulerRow?.readyWork ?? false),
			blockedReasonCode: blockedReasonCode || schedulerRow?.lastBlockedReasonCode || null,
			blockedHandle: blockedHandle || null,
			blockedPlane,
			updatedAt: String(blockedRow.updatedAt ?? '').trim() || null,
			isBlocked: Boolean(blockedReasonCode),
			isWaiting: pendingInputCount > 0 && inflight === 0,
			isLlmHolder: llmState !== 'released' && nodeId.length > 0 && llmHolderNodeId === nodeId,
			isLlmWaiting
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

export function preferredMonitorEdgeFocusNodeId(sourceNodeId: string, targetNodeId: string): string {
	const target = String(targetNodeId ?? '').trim();
	if (target) return target;
	return String(sourceNodeId ?? '').trim();
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
