import type { Edge, Node } from '@xyflow/svelte';

import type { NodeStatus, PipelineEdgeData, PipelineNodeData } from '$lib/flow/types';
import type { CheckpointExecutionHints, CheckpointRegistry } from '$lib/flow/types/checkpoint';
import { projectNodeDisplayState, type NodeBindingProjectionInput } from './displayState';
import { projectNodeStatus, type NodeStatusProjection } from './statusModel';

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
	return projectNodeDisplayState(binding as NodeBindingProjectionInput | undefined, binding?.status);
}

export function statusProjectionFromBinding(binding: NodeBindingLike | null | undefined): NodeStatusProjection {
	return projectNodeStatus(binding as NodeBindingProjectionInput | undefined, binding?.status);
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

function buildUndirectedAdjacency(
	nodes: Node<PipelineNodeData & Record<string, unknown>>[],
	edges: Edge<PipelineEdgeData & Record<string, unknown>>[]
): Map<string, Set<string>> {
	const adj = new Map<string, Set<string>>();
	const ensure = (id: string) => {
		if (!adj.has(id)) adj.set(id, new Set<string>());
	};
	for (const node of nodes) {
		const id = String(node?.id ?? '').trim();
		if (!id) continue;
		ensure(id);
	}
	for (const edge of edges) {
		const src = String(edge?.source ?? '').trim();
		const dst = String(edge?.target ?? '').trim();
		if (!src || !dst) continue;
		ensure(src);
		ensure(dst);
		adj.get(src)!.add(dst);
		adj.get(dst)!.add(src);
	}
	return adj;
}

export function computeConnectedComponentNodeSets(
	nodes: Node<PipelineNodeData & Record<string, unknown>>[],
	edges: Edge<PipelineEdgeData & Record<string, unknown>>[]
): Set<string>[] {
	const adj = buildUndirectedAdjacency(nodes, edges);
	const visited = new Set<string>();
	const out: Set<string>[] = [];
	for (const node of nodes) {
		const seed = String(node?.id ?? '').trim();
		if (!seed || visited.has(seed)) continue;
		const component = new Set<string>();
		const queue: string[] = [seed];
		visited.add(seed);
		while (queue.length) {
			const cur = queue.shift()!;
			component.add(cur);
			const next = adj.get(cur);
			if (!next) continue;
			for (const neighbor of next) {
				if (visited.has(neighbor)) continue;
				visited.add(neighbor);
				queue.push(neighbor);
			}
		}
		out.push(component);
	}
	return out;
}

export function computeSelectedConnectedComponentNodeSet(
	nodes: Node<PipelineNodeData & Record<string, unknown>>[],
	edges: Edge<PipelineEdgeData & Record<string, unknown>>[],
	selectedNodeId: string | null
): Set<string> {
	const selected = String(selectedNodeId ?? '').trim();
	if (!selected) return new Set<string>();
	const components = computeConnectedComponentNodeSets(nodes, edges);
	return components.find((component) => component.has(selected)) ?? new Set<string>();
}

export function planRunConnectedComponents(
	nodes: Node<PipelineNodeData & Record<string, unknown>>[],
	edges: Edge<PipelineEdgeData & Record<string, unknown>>[],
	runFrom: string | null,
	runMode: ActiveRunMode
): Set<string>[] {
	if (runMode === 'from_start' || runFrom === null) {
		return computeConnectedComponentNodeSets(nodes, edges);
	}
	const selectedComponent = computeSelectedConnectedComponentNodeSet(nodes, edges, runFrom);
	return selectedComponent.size > 0 ? [selectedComponent] : [];
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
	dirtyNodeIds?: string[],
	cacheMode?: 'default_on' | 'force_off' | 'force_on',
	adaptiveMode?: 'off' | 'observe' | 'enforce' | null,
	checkpoints?: CheckpointRegistry
): {
	graphId: string;
	graph: {
		version: number;
		nodes: unknown[];
		edges: unknown[];
		__executionHints?: {
			dirtyNodeIds?: string[];
			checkpoints?: CheckpointExecutionHints['checkpoints'];
		};
	};
	runFrom?: string;
	runMode?: 'from_selected_onward' | 'selected_only';
	cacheMode?: 'default_on' | 'force_off' | 'force_on';
	adaptive?: { mode: 'off' | 'observe' | 'enforce' };
} {
	const sanitizedDirty = Array.isArray(dirtyNodeIds)
		? Array.from(new Set(dirtyNodeIds.map((v) => String(v ?? '').trim()).filter(Boolean)))
		: [];
	const sanitizedCheckpoints: CheckpointExecutionHints['checkpoints'] =
		checkpoints && typeof checkpoints === 'object'
			? Object.fromEntries(
					Object.entries(checkpoints)
						.map(([nodeId, checkpoint]) => {
							const nid = String(nodeId ?? '').trim();
							const aid = String((checkpoint as any)?.artifactId ?? '').trim();
							const execKey = String((checkpoint as any)?.execKey ?? '').trim();
							const fingerprintAtCreation = String((checkpoint as any)?.fingerprintAtCreation ?? '').trim();
							if (!nid || !aid || !execKey || !/^[0-9a-f]{64}$/i.test(fingerprintAtCreation)) return null;
							const rawOutputs = (checkpoint as any)?.outputs;
							const outputs =
								rawOutputs && typeof rawOutputs === 'object'
									? Object.fromEntries(
											Object.entries(rawOutputs as Record<string, any>)
												.map(([rawHandle, rawOutput]) => {
													const handle = String(rawHandle ?? '').trim();
													const outArtifactId = String((rawOutput as any)?.artifactId ?? '').trim();
													const outExecKey = String((rawOutput as any)?.execKey ?? '').trim();
													if (!handle || !outArtifactId) return null;
													return [
														handle,
														{
															artifactId: outArtifactId,
															...(outExecKey ? { execKey: outExecKey } : {})
														}
													];
												})
												.filter(
													(entry): entry is [
														string,
														{
															artifactId: string;
															execKey?: string;
														}
													] => entry !== null
												)
									  )
									: {};
							return [
								nid,
								{
									artifactId: aid,
									execKey,
									fingerprintAtCreation,
									...(Object.keys(outputs).length > 0 ? { outputs } : {})
								}
							];
						})
						.filter(
							(
								entry
							): entry is [
								string,
								{
									artifactId: string;
									execKey: string;
									fingerprintAtCreation: string;
									outputs?: Record<string, { artifactId: string; execKey?: string }>;
								}
							] => entry !== null
						)
			  )
			: {};
	const executionHints: {
		dirtyNodeIds?: string[];
		checkpoints?: CheckpointExecutionHints['checkpoints'];
	} = {};
	if (sanitizedDirty.length > 0) executionHints.dirtyNodeIds = sanitizedDirty;
	if (Object.keys(sanitizedCheckpoints).length > 0) executionHints.checkpoints = sanitizedCheckpoints;
	const payloadGraph =
		Object.keys(executionHints).length > 0
			? {
					...graph,
					__executionHints: executionHints
				}
			: graph;

	if (runFrom === null || runMode === 'from_start' || !runMode) {
		const base = cacheMode ? { graphId, graph: payloadGraph, cacheMode } : { graphId, graph: payloadGraph };
		if (adaptiveMode && ['off', 'observe', 'enforce'].includes(String(adaptiveMode))) {
			return { ...base, adaptive: { mode: adaptiveMode } };
		}
		return base;
	}
	const out = {
		graphId,
		graph: payloadGraph,
		runFrom,
		runMode
	} as {
		graphId: string;
		graph: {
			version: number;
			nodes: unknown[];
			edges: unknown[];
			__executionHints?: {
				dirtyNodeIds?: string[];
				checkpoints?: CheckpointExecutionHints['checkpoints'];
			};
		};
		runFrom?: string;
		runMode?: 'from_selected_onward' | 'selected_only';
		cacheMode?: 'default_on' | 'force_off' | 'force_on';
	};
	if (cacheMode) out.cacheMode = cacheMode;
	if (adaptiveMode && ['off', 'observe', 'enforce'].includes(String(adaptiveMode))) {
		out.adaptive = { mode: adaptiveMode };
	}
	return out;
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

