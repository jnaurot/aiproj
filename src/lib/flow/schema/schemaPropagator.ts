import type { Edge, Node } from '@xyflow/svelte';
import type { PipelineEdgeData, PipelineNodeData } from '$lib/flow/types';
import type { SchemaFunction, SchemaPlaneOutput, SchemaPlaneResult, SchemaPlaneState } from '$lib/flow/types/schemaPlane';
import { getSchemaFunction, OPAQUE_SCHEMA, UNKNOWN_SCHEMA } from './schemaRegistry';

type PipelineNode = Node<PipelineNodeData & Record<string, unknown>>;
type PipelineEdge = Edge<PipelineEdgeData & Record<string, unknown>>;
type ComponentGraph = { nodes: PipelineNode[]; edges: PipelineEdge[] };
type PropagatorOptions = {
	resolveComponentGraph?: (componentNodeId: string, componentNode: PipelineNode) => ComponentGraph | null;
	componentStack?: string[];
};

function dedupe<T>(items: T[]): T[] {
	return Array.from(new Set(items));
}

function toNodeMap(nodes: PipelineNode[]): Map<string, PipelineNode> {
	return new Map(nodes.map((node) => [node.id, node]));
}

function buildAdjacency(nodes: PipelineNode[], edges: PipelineEdge[]): Map<string, string[]> {
	const nodeIds = new Set(nodes.map((node) => node.id));
	const adj = new Map<string, string[]>();
	for (const id of nodeIds) adj.set(id, []);
	for (const edge of edges) {
		if (!nodeIds.has(edge.source) || !nodeIds.has(edge.target)) continue;
		adj.set(edge.source, [...(adj.get(edge.source) ?? []), edge.target]);
	}
	return adj;
}

function detectCycleNodes(nodes: PipelineNode[], edges: PipelineEdge[]): Set<string> {
	const adj = buildAdjacency(nodes, edges);
	const colors = new Map<string, 0 | 1 | 2>();
	const stack: string[] = [];
	const inCycle = new Set<string>();

	const dfs = (nodeId: string) => {
		colors.set(nodeId, 1);
		stack.push(nodeId);
		for (const nextId of adj.get(nodeId) ?? []) {
			const color = colors.get(nextId) ?? 0;
			if (color === 0) {
				dfs(nextId);
				continue;
			}
			if (color === 1) {
				const start = stack.lastIndexOf(nextId);
				if (start >= 0) {
					for (let i = start; i < stack.length; i += 1) inCycle.add(stack[i]);
				}
			}
		}
		stack.pop();
		colors.set(nodeId, 2);
	};

	for (const node of nodes) {
		if ((colors.get(node.id) ?? 0) === 0) dfs(node.id);
	}
	return inCycle;
}

function topologicalSort(nodes: PipelineNode[], edges: PipelineEdge[], excluded: Set<string>): string[] {
	const nodeIds = nodes.map((node) => node.id).filter((id) => !excluded.has(id));
	const indeg = new Map<string, number>(nodeIds.map((id) => [id, 0]));
	const adj = new Map<string, string[]>(nodeIds.map((id) => [id, []]));
	for (const edge of edges) {
		if (!indeg.has(edge.source) || !indeg.has(edge.target)) continue;
		adj.set(edge.source, [...(adj.get(edge.source) ?? []), edge.target]);
		indeg.set(edge.target, (indeg.get(edge.target) ?? 0) + 1);
	}
	const queue = nodeIds.filter((id) => (indeg.get(id) ?? 0) === 0);
	const order: string[] = [];
	while (queue.length > 0) {
		const cur = queue.shift()!;
		order.push(cur);
		for (const next of adj.get(cur) ?? []) {
			const nextDeg = (indeg.get(next) ?? 0) - 1;
			indeg.set(next, nextDeg);
			if (nextDeg === 0) queue.push(next);
		}
	}
	for (const id of nodeIds) {
		if (!order.includes(id)) order.push(id);
	}
	return order;
}

function incomingEdges(nodeId: string, edges: PipelineEdge[]): PipelineEdge[] {
	return edges
		.filter((edge) => edge.target === nodeId)
		.sort((a, b) => {
			const ah = String(a.targetHandle ?? '');
			const bh = String(b.targetHandle ?? '');
			if (ah !== bh) return ah.localeCompare(bh);
			return String(a.id ?? '').localeCompare(String(b.id ?? ''));
		});
}

function inputHandleOrder(node: PipelineNode, edges: PipelineEdge[]): string[] {
	const declared = Object.keys(((node.data as any)?.portDeclarations?.in ?? {}) as Record<string, unknown>);
	const fromEdges = incomingEdges(node.id, edges).map((edge) => String(edge.targetHandle ?? 'in'));
	return dedupe([...declared, ...fromEdges]).filter((h) => h.length > 0);
}

function asOutput(result: SchemaPlaneResult | undefined): SchemaPlaneOutput {
	if (!result) return OPAQUE_SCHEMA;
	if (result.ok) return result.output;
	return result.output ?? OPAQUE_SCHEMA;
}

function pickTerminalNodeId(nodes: PipelineNode[], edges: PipelineEdge[]): string | null {
	if (!Array.isArray(nodes) || nodes.length === 0) return null;
	const outDegree = new Map<string, number>();
	for (const node of nodes) outDegree.set(String(node.id ?? ''), 0);
	for (const edge of edges) {
		const source = String(edge.source ?? '');
		if (!outDegree.has(source)) continue;
		outDegree.set(source, (outDegree.get(source) ?? 0) + 1);
	}
	const leaf = nodes
		.map((node) => String(node.id ?? ''))
		.find((nodeId) => (outDegree.get(nodeId) ?? 0) === 0);
	return leaf ?? (String(nodes[0]?.id ?? '') || null);
}

function computeComponentNodeResult(
	node: PipelineNode,
	options: PropagatorOptions
): SchemaPlaneResult {
	const nodeId = String(node.id ?? '').trim();
	const stack = options.componentStack ?? [];
	if (!nodeId || stack.includes(nodeId)) return { ok: true, output: OPAQUE_SCHEMA };
	const resolver = options.resolveComponentGraph;
	if (!resolver) return { ok: true, output: OPAQUE_SCHEMA };
	const componentGraph = resolver(nodeId, node);
	if (!componentGraph) return { ok: true, output: OPAQUE_SCHEMA };
	const nestedState = propagateSchemas(componentGraph.nodes, componentGraph.edges, {
		...options,
		componentStack: [...stack, nodeId]
	});
	const terminalNodeId = pickTerminalNodeId(componentGraph.nodes, componentGraph.edges);
	const terminalResult = terminalNodeId ? nestedState.nodeSchemas?.[terminalNodeId] : null;
	const nestedErrorCount = Object.values(nestedState.nodeSchemas ?? {}).filter(
		(result) => result && result.ok === false
	).length;
	const output = asOutput(terminalResult ?? undefined);
	if (nestedErrorCount <= 0) return { ok: true, output };
	return {
		ok: true,
		output: {
			...output,
			properties: {
				...(output.properties ?? {}),
				component_internal_errors: nestedErrorCount
			},
			note: `Component has ${nestedErrorCount} internal schema error${nestedErrorCount === 1 ? '' : 's'}`
		}
	};
}

/**
 * Converts a ComponentTypedPrimitive field type (as stored in node.data.schema)
 * to the schema-plane column type enum.
 */
function componentFieldTypeToColumnType(raw: unknown): SchemaPlaneOutput['columns'][number]['type'] {
	const t = String(raw ?? '').trim().toLowerCase();
	if (t === 'text' || t === 'string') return 'string';
	if (t === 'number' || t === 'integer' || t === 'float') return 'number';
	if (t === 'boolean' || t === 'bool') return 'boolean';
	if (t === 'datetime' || t === 'date' || t === 'timestamp') return 'datetime';
	if (t === 'binary' || t === 'bytes') return 'binary';
	if (t === 'embeddings' || t === 'embedding' || t === 'tensor') return 'tensor';
	return 'unknown';
}

/**
 * Converts a ComponentTypedPrimitive root type to a schema-plane output mode.
 */
function componentTypeToMode(raw: unknown): SchemaPlaneOutput['mode'] {
	const t = String(raw ?? '').trim().toLowerCase();
	if (t === 'table' || t === 'json') return 'table';
	if (t === 'text' || t === 'string') return 'text';
	if (t === 'binary' || t === 'bytes') return 'binary';
	if (t === 'embeddings' || t === 'embedding') return 'tensor';
	return 'opaque';
}

/**
 * If a node has a manually declared output schema (source === 'declared' on
 * node.data.schema.expectedSchema), convert it to a SchemaPlaneResult and
 * return it. Returns null when no valid declared override is present.
 *
 * This implements the "manual" tier of schema authority: the user explicitly
 * asserts what the node outputs, overriding the schema function. This unblocks
 * downstream validation when inputs are opaque or the schema function cannot
 * determine output automatically.
 */
function getDeclaredOutputOverride(node: PipelineNode): SchemaPlaneResult | null {
	const schemaEnvelope = (node.data as any)?.schema;
	if (!schemaEnvelope || typeof schemaEnvelope !== 'object') return null;
	const expectedSchema = (schemaEnvelope as Record<string, unknown>).expectedSchema;
	if (!expectedSchema || typeof expectedSchema !== 'object') return null;
	const obs = expectedSchema as Record<string, unknown>;
	if (String(obs.source ?? '').trim() !== 'declared') return null;
	const typedSchema = obs.typedSchema;
	if (!typedSchema || typeof typedSchema !== 'object') return null;
	const ts = typedSchema as Record<string, unknown>;
	const mode = componentTypeToMode(ts.type);
	const rawFields = Array.isArray(ts.fields) ? ts.fields : [];
	const columns: SchemaPlaneOutput['columns'] = rawFields
		.map((field: unknown) => {
			if (!field || typeof field !== 'object') return null;
			const f = field as Record<string, unknown>;
			const name = String(f.name ?? '').trim();
			if (!name) return null;
			return {
				name,
				type: componentFieldTypeToColumnType(f.type),
				nullable: Boolean(f.nullable ?? false),
				properties: {}
			};
		})
		.filter((col): col is NonNullable<typeof col> => col !== null);
	return {
		ok: true,
		output: { mode, columns, note: 'declared' }
	};
}

function computeNodeResult(
	node: PipelineNode,
	edges: PipelineEdge[],
	results: Map<string, SchemaPlaneResult>,
	options: PropagatorOptions
): SchemaPlaneResult {
	const kind = String((node.data as any)?.kind ?? '').trim().toLowerCase();
	if (kind === 'component') return computeComponentNodeResult(node, options);

	// ── Declared output override (manual tier) ──────────────────────────────
	// When the user has explicitly saved an expected output schema
	// (source === 'declared'), honour it in place of the schema-function result.
	// This is the primary escape hatch for opaque-upstream situations.
	const declaredOverride = getDeclaredOutputOverride(node);
	if (declaredOverride) return declaredOverride;
	const fn: SchemaFunction | undefined = getSchemaFunction((node.data as any)?.kind ?? '');
	if (!fn) return { ok: true, output: OPAQUE_SCHEMA };
	const params = (((node.data as any)?.params ?? {}) as Record<string, unknown>) ?? {};
	const op = String((params as any)?.op ?? (node.data as any)?.transformKind ?? '').trim().toLowerCase();
	const ins = incomingEdges(node.id, edges);
	if (kind === 'transform' && op === 'join') {
		const joinInputs = ins.map((edge) => asOutput(results.get(edge.source)));
		const joinParams: Record<string, unknown> = {
			...params,
			__schemaInputRefs: ins.map((edge) => ({
				edgeId: String(edge.id ?? ''),
				sourceNodeId: String(edge.source ?? ''),
				targetHandle: String(edge.targetHandle ?? 'in').trim() || 'in'
			}))
		};
		if (joinInputs.length === 0) {
			return fn([], joinParams);
		}
		return fn(joinInputs, joinParams);
	}
	const handles = inputHandleOrder(node, edges);
	const inputs: SchemaPlaneOutput[] = [];
	for (const handle of handles) {
		const edge = ins.find((candidate) => String(candidate.targetHandle ?? 'in') === handle);
		if (!edge) {
			inputs.push(UNKNOWN_SCHEMA);
			continue;
		}
		inputs.push(asOutput(results.get(edge.source)));
	}
	if (handles.length === 0 && ins.length === 0) {
		// Source-style node.
		return fn([], params);
	}
	return fn(inputs, params);
}

export function buildSchemaPlaneState(
	nodeResults: Map<string, SchemaPlaneResult>,
	edges: PipelineEdge[]
): SchemaPlaneState {
	const nodeSchemas: Record<string, SchemaPlaneResult> = {};
	for (const [nodeId, result] of nodeResults.entries()) nodeSchemas[nodeId] = result;
	const edgeSchemas: Record<string, SchemaPlaneOutput> = {};
	for (const edge of edges) {
		if (!edge?.id) continue;
		edgeSchemas[edge.id] = asOutput(nodeResults.get(edge.source));
	}
	return { nodeSchemas, edgeSchemas };
}

export function propagateSchemas(
	nodes: PipelineNode[],
	edges: PipelineEdge[],
	options: PropagatorOptions = {}
): SchemaPlaneState {
	if (!Array.isArray(nodes) || nodes.length === 0) return { nodeSchemas: {}, edgeSchemas: {} };
	const nodeMap = toNodeMap(nodes);
	const cycleNodes = detectCycleNodes(nodes, edges);
	const order = topologicalSort(nodes, edges, cycleNodes);
	const results = new Map<string, SchemaPlaneResult>();

	for (const nodeId of cycleNodes) {
		results.set(nodeId, {
			ok: false,
			error: {
				code: 'CYCLE_DETECTED',
				message: 'Cycle detected in schema plane graph',
				handles: []
			},
			output: OPAQUE_SCHEMA
		});
	}

	for (const nodeId of order) {
		const node = nodeMap.get(nodeId);
		if (!node) continue;
		results.set(nodeId, computeNodeResult(node, edges, results, options));
	}

	return buildSchemaPlaneState(results, edges);
}
