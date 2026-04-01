import type { Edge, Node } from '@xyflow/svelte';

import type { PipelineEdgeData, PipelineNodeData } from '$lib/flow/types';
import { displayStatusFromBinding } from '$lib/flow/store/runScope';

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
	depth: number;
	blocked: boolean;
	full: boolean;
	oldestAgeSec: number | null;
};

export type RunMonitorAdaptiveDecisionRow = {
	at: string;
	runId: string;
	mode: string;
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
	};
};

export type RunMonitorTrendSparkline = {
	path: string;
	width: number;
	height: number;
	minValue: number;
	maxValue: number;
	lastValue: number;
	deltaValue: number;
	deltaPct: number | null;
	pointsCount: number;
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
		const depth = Math.max(0, Number(metric.depth ?? 0));
		const blocked = Boolean(metric.blocked ?? false);
		const full = Boolean(metric.full ?? false);
		const rawAge = metric.oldestAgeSec;
		const oldestAgeSec = Number.isFinite(Number(rawAge)) ? Math.max(0, Number(rawAge)) : null;
		out.push({
			edgeId,
			handle,
			sourceNodeId,
			sourceLabel: String((labelById.get(sourceNodeId) ?? sourceNodeId) || '(unknown node)'),
			targetNodeId,
			targetLabel: String((labelById.get(targetNodeId) ?? targetNodeId) || '(unknown node)'),
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
	return rows
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
}

export function explainAdaptiveDecision(input: {
	enforced: boolean;
	reasons: string[];
	changedCaps: Record<string, { from: number; to: number }>;
	inputs: Record<string, unknown>;
	effectiveCaps: Record<string, number>;
}): { score: number; severity: 'low' | 'medium' | 'high'; signals: string[] } {
	const signals: string[] = [];
	let score = 0;
	if (input.enforced) {
		score += 20;
		signals.push('enforced_mode');
	}
	const changedEntries = Object.entries(input.changedCaps ?? {});
	if (changedEntries.length > 0) {
		score += Math.min(24, changedEntries.length * 8);
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
		score += Math.min(20, totalDelta * 3);
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
		score += weighted;
		signals.push(`reason:${reason}`);
	}
	const queueDepth = Number((input.inputs ?? {}).queueDepth ?? (input.inputs ?? {}).pendingQueueDepth ?? 0);
	if (Number.isFinite(queueDepth) && queueDepth > 0) {
		if (queueDepth >= 20) score += 10;
		else if (queueDepth >= 10) score += 6;
		else if (queueDepth >= 5) score += 3;
		signals.push(`queue_depth=${queueDepth}`);
	}
	const failureRate = Number((input.inputs ?? {}).failureRate ?? (input.inputs ?? {}).errorRate ?? 0);
	if (Number.isFinite(failureRate) && failureRate > 0) {
		if (failureRate >= 0.2) score += 15;
		else if (failureRate >= 0.1) score += 10;
		else if (failureRate >= 0.05) score += 5;
		signals.push(`failure_rate=${failureRate.toFixed(3)}`);
	}
	const leaseWaitMs = Number((input.inputs ?? {}).leaseWaitMs ?? 0);
	if (Number.isFinite(leaseWaitMs) && leaseWaitMs > 0) {
		if (leaseWaitMs >= 5000) score += 8;
		else if (leaseWaitMs >= 2000) score += 5;
		else if (leaseWaitMs >= 1000) score += 3;
		signals.push(`lease_wait_ms=${leaseWaitMs}`);
	}
	const bounded = Math.max(0, Math.min(100, Math.round(score)));
	const severity: 'low' | 'medium' | 'high' =
		bounded >= 70 ? 'high' : bounded >= 40 ? 'medium' : 'low';
	return { score: bounded, severity, signals };
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
	const path = normalized
		.map((point, index) => `${index === 0 ? 'M' : 'L'} ${(index * stepX).toFixed(2)} ${toY(point.value).toFixed(2)}`)
		.join(' ');
	const firstValue = normalized[0].value;
	const lastValue = normalized[normalized.length - 1].value;
	const deltaValue = lastValue - firstValue;
	const deltaPct = firstValue === 0 ? null : (deltaValue / firstValue) * 100.0;
	return {
		path,
		width,
		height,
		minValue,
		maxValue,
		lastValue,
		deltaValue,
		deltaPct,
		pointsCount: normalized.length
	};
}
