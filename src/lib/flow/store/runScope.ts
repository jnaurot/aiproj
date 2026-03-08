import type { Edge, Node } from '@xyflow/svelte';

import type { NodeStatus, PipelineEdgeData, PipelineNodeData } from '$lib/flow/types';

export type ActiveRunMode = 'from_start' | 'from_selected_onward' | 'selected_only';

export type NodeBindingLike = {
	isUpToDate?: boolean;
	status?: unknown;
	current?: { execKey?: string | null; artifactId?: string | null } | null;
	last?: { execKey?: string | null; artifactId?: string | null } | null;
	currentArtifactId?: string | null; // legacy
	lastArtifactId?: string | null; // legacy
	currentExecKey?: string | null; // legacy
	lastRunId?: string | null;
	lastExecKey?: string | null; // legacy
	[key: string]: unknown;
};

export type GraphFreshness = 'up_to_date' | 'stale' | 'never_run';

export function isBindingStale(binding: NodeBindingLike | null | undefined): boolean {
	if (!binding) return false;
	if (binding.isUpToDate === false) return true;
	const status = String(binding.status ?? '').toLowerCase();
	return status === 'stale';
}

export function displayStatusFromBinding(binding: NodeBindingLike | null | undefined): NodeStatus {
	if (!binding) return 'idle';
	const currentArtifactId = binding.current?.artifactId ?? binding.currentArtifactId;
	const lastArtifactId = binding.last?.artifactId ?? binding.lastArtifactId;
	const currentExecKey = binding.current?.execKey ?? binding.currentExecKey;
	const lastExecKey = binding.last?.execKey ?? binding.lastExecKey;
	const hasArtifact = Boolean(currentArtifactId || lastArtifactId);
	const raw = String(binding.status ?? '').toLowerCase();
	if (raw === 'running') return 'running';
	if (isBindingStale(binding)) return 'stale';
	if (typeof currentExecKey === 'string' && typeof lastExecKey === 'string' && currentExecKey !== lastExecKey) {
		return 'stale';
	}
	if (raw === 'succeeded_up_to_date' || raw === 'succeeded') return 'succeeded';
	if (hasArtifact && raw === '') return 'succeeded';
	if (raw === 'failed') return 'failed';
	if (raw === 'cancelled' || raw === 'canceled') return 'canceled';
	if (raw === 'stale') return 'stale';
	return 'idle';
}

function descendantIds(
	startId: string,
	edges: Edge<PipelineEdgeData & Record<string, unknown>>[]
): Set<string> {
	const out = new Set<string>([startId]);
	const q = [startId];
	while (q.length > 0) {
		const cur = q.shift()!;
		for (const e of edges) {
			if (e.source !== cur) continue;
			if (out.has(e.target)) continue;
			out.add(e.target);
			q.push(e.target);
		}
	}
	return out;
}

function buildAdj(edges: Edge[]): { up: Map<string, Set<string>>; down: Map<string, Set<string>> } {
	const up = new Map<string, Set<string>>();
	const down = new Map<string, Set<string>>();

	const add = (m: Map<string, Set<string>>, k: string, v: string) => {
		let s = m.get(k);
		if (!s) m.set(k, (s = new Set()));
		s.add(v);
	};

	for (const e of edges) {
		// ReactFlow/XYFlow convention: e.source -> e.target
		add(down, e.source, e.target);
		add(up, e.target, e.source);
	}
	return { up, down };
}

function collect(start: string, adj: Map<string, Set<string>>): Set<string> {
	const seen = new Set<string>();
	const stack = [start];
	while (stack.length) {
		const cur = stack.pop()!;
		const next = adj.get(cur);
		if (!next) continue;
		for (const n of next) {
			if (!seen.has(n)) {
				seen.add(n);
				stack.push(n);
			}
		}
	}
	return seen;
}

export function computePlannedNodeSet(
	nodes: Node<PipelineNodeData & Record<string, unknown>>[],
	edges: Edge<PipelineEdgeData & Record<string, unknown>>[],
	runFrom: string | null,
	runMode: ActiveRunMode
): Set<string> {
	// Full run
	if (runMode === 'from_start' || runFrom === null) {
		return new Set(nodes.map((n) => n.id));
	}

	const { up, down } = buildAdj(edges);

	const ancestors = collect(runFrom, up);
	const descendants = collect(runFrom, down);

	const planned = new Set<string>(ancestors);
	planned.add(runFrom);

	if (runMode === 'from_selected_onward') {
		for (const d of descendants) planned.add(d);
	}

	// selected_only: ancestors + selected only
	return planned;
}

export function shouldUpdateBinding(
	activeRunId: string | null,
	activeRunNodeSet: Set<string> | null | undefined,
	nodeId: string
): boolean {
	if (!nodeId) return false;
	if (!activeRunId) return true;
	if (!activeRunNodeSet || activeRunNodeSet.size === 0) return true;
	return activeRunNodeSet.has(nodeId);
}

export function buildRunCreateRequest(
	graph: { version: number; nodes: unknown[]; edges: unknown[] },
	graphId: string,
	runFrom: string | null,
	runMode?: ActiveRunMode,
	dirtyNodeIds?: string[]
): {
	graphId: string;
	graph: { version: number; nodes: unknown[]; edges: unknown[]; __executionHints?: { dirtyNodeIds: string[] } };
	runFrom?: string;
	runMode?: 'from_selected_onward' | 'selected_only';
} {
	const sanitizedDirty = Array.isArray(dirtyNodeIds)
		? Array.from(new Set(dirtyNodeIds.map((v) => String(v ?? '').trim()).filter(Boolean)))
		: [];
	const payloadGraph =
		sanitizedDirty.length > 0
			? {
					...graph,
					__executionHints: { dirtyNodeIds: sanitizedDirty }
				}
			: graph;

	if (runFrom === null || runMode === 'from_start' || !runMode) {
		return { graphId, graph: payloadGraph };
	}
	return {
		graphId,
		graph: payloadGraph,
		runFrom,
		runMode
	};
}

export function mergeBindingsSticky<T extends NodeBindingLike>(
	prev: Record<string, T>,
	patch: Record<string, T>
): Record<string, T> {
	const merged: Record<string, T> = { ...prev };
	for (const [nodeId, bindingPatch] of Object.entries(patch)) {
		if (!bindingPatch || typeof bindingPatch !== 'object') continue;
		const sanitized = Object.fromEntries(
			Object.entries(bindingPatch).filter(([, v]) => v !== undefined)
		) as T;
		merged[nodeId] = { ...(merged[nodeId] ?? ({} as T)), ...sanitized };
	}
	return merged;
}

export function computeGraphFreshness(bindings: Record<string, NodeBindingLike>): {
	freshness: GraphFreshness;
	staleNodeCount: number;
} {
	const values = Object.values(bindings);
	const hasRun = values.some((b) => !!(b.last?.artifactId ?? b.lastArtifactId) || !!b.lastRunId);
	if (!hasRun) return { freshness: 'never_run', staleNodeCount: 0 };
	const staleNodeCount = values.filter((b) => isBindingStale(b)).length;
	return {
		freshness: staleNodeCount > 0 ? 'stale' : 'up_to_date',
		staleNodeCount
	};
}

export function getStaleFlipNodeIds(
	prev: Record<string, NodeBindingLike>,
	next: Record<string, NodeBindingLike>
): string[] {
	const ids = new Set([...Object.keys(prev ?? {}), ...Object.keys(next ?? {})]);
	const flipped: string[] = [];
	for (const id of ids) {
		const before = isBindingStale(prev?.[id]);
		const after = isBindingStale(next?.[id]);
		if (before !== after) flipped.push(id);
	}
	return flipped;
}
