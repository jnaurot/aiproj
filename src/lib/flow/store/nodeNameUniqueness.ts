import type { Node } from '@xyflow/svelte';
import type { PipelineNodeData } from '$lib/flow/types';

export type NodeNameDuplicateIssue = {
	scopeKey: string;
	scopeLabel: string;
	normalizedName: string;
	displayName: string;
	nodeIds: string[];
};

export type ParsedPromotedNodeId = {
	componentInstanceIds: string[];
	leafNodeId: string;
};

export function normalizeNodeName(value: unknown): string {
	return String(value ?? '').trim().toLowerCase();
}

export function displayNodeName(node: Node<PipelineNodeData>): string {
	const label = String((node?.data as any)?.label ?? '').trim();
	if (label.length > 0) return label;
	return String(node?.id ?? '').trim();
}

export function parsePromotedNodeId(nodeIdRaw: unknown): ParsedPromotedNodeId | null {
	let cursor = String(nodeIdRaw ?? '').trim();
	if (!cursor.startsWith('cmp:')) return null;
	const componentInstanceIds: string[] = [];
	let guard = 0;
	while (cursor.startsWith('cmp:') && guard < 32) {
		guard += 1;
		const rest = cursor.slice(4);
		const sep = rest.indexOf(':');
		if (sep <= 0) return null;
		const instanceId = rest.slice(0, sep).trim();
		if (!instanceId) return null;
		componentInstanceIds.push(instanceId);
		cursor = rest.slice(sep + 1);
	}
	const leafNodeId = String(cursor ?? '').trim();
	if (!leafNodeId) return null;
	return { componentInstanceIds, leafNodeId };
}

function nodeLabelById(nodes: Array<Node<PipelineNodeData>>, nodeId: string): string {
	const node = nodes.find((candidate) => String(candidate?.id ?? '') === nodeId);
	if (!node) return nodeId;
	return displayNodeName(node);
}

export function nodeScopeKey(nodeIdRaw: unknown): string {
	const parsed = parsePromotedNodeId(nodeIdRaw);
	if (!parsed) return 'root';
	return `cmp:${parsed.componentInstanceIds.join('/')}`;
}

export function nodeScopeLabel(
	nodes: Array<Node<PipelineNodeData>>,
	nodeIdRaw: unknown
): string {
	const parsed = parsePromotedNodeId(nodeIdRaw);
	if (!parsed) return 'root';
	const labels = parsed.componentInstanceIds.map((componentId) =>
		nodeLabelById(nodes, componentId)
	);
	return labels.join('.');
}

export function canonicalNodeName(
	nodes: Array<Node<PipelineNodeData>>,
	node: Node<PipelineNodeData>
): string {
	const local = displayNodeName(node);
	const parsed = parsePromotedNodeId(node.id);
	if (!parsed) return local;
	const prefix = parsed.componentInstanceIds.map((componentId) =>
		nodeLabelById(nodes, componentId)
	);
	return [...prefix, local].join('.');
}

export function findDuplicateNodeNames(nodes: Array<Node<PipelineNodeData>>): NodeNameDuplicateIssue[] {
	const byScopeAndName = new Map<
		string,
		{
			scopeKey: string;
			scopeLabel: string;
			displayName: string;
			nodeIds: string[];
		}
	>();
	for (const node of nodes) {
		const displayName = displayNodeName(node);
		const normalizedName = normalizeNodeName(displayName);
		if (!normalizedName) continue;
		const scopeKey = nodeScopeKey(node.id);
		const scopeLabel = nodeScopeLabel(nodes, node.id);
		const indexKey = `${scopeKey}::${normalizedName}`;
		const existing = byScopeAndName.get(indexKey);
		if (existing) {
			existing.nodeIds.push(String(node.id));
			continue;
		}
		byScopeAndName.set(indexKey, {
			scopeKey,
			scopeLabel,
			displayName,
			nodeIds: [String(node.id)]
		});
	}
	const duplicates: NodeNameDuplicateIssue[] = [];
	for (const [key, entry] of byScopeAndName.entries()) {
		if (entry.nodeIds.length <= 1) continue;
		const normalizedName = String(key.split('::')[1] ?? '').trim();
		duplicates.push({
			scopeKey: entry.scopeKey,
			scopeLabel: entry.scopeLabel,
			normalizedName,
			displayName: entry.displayName,
			nodeIds: [...entry.nodeIds]
		});
	}
	return duplicates;
}

export function resolveUniqueNodeName(
	existingNodes: Array<Node<PipelineNodeData>>,
	candidate: string,
	opts?: { excludeNodeId?: string | null; scopeNodeId?: string | null }
): string {
	const base = String(candidate ?? '').trim();
	const safeBase = base || 'Node';
	const excludeNodeId = String(opts?.excludeNodeId ?? '').trim();
	const scopeNodeId = String(opts?.scopeNodeId ?? opts?.excludeNodeId ?? '').trim();
	const scopeKey = nodeScopeKey(scopeNodeId);
	const existing = new Set<string>();
	for (const node of existingNodes) {
		if (excludeNodeId && String(node.id) === excludeNodeId) continue;
		if (scopeKey !== nodeScopeKey(node.id)) continue;
		const normalized = normalizeNodeName(displayNodeName(node));
		if (normalized) existing.add(normalized);
	}
	let attempt = safeBase;
	let n = 2;
	while (existing.has(normalizeNodeName(attempt))) {
		attempt = `${safeBase} (${n})`;
		n += 1;
	}
	return attempt;
}

export function findNodeIdByName(
	existingNodes: Array<Node<PipelineNodeData>>,
	candidate: string,
	opts?: { excludeNodeId?: string | null; scopeNodeId?: string | null }
): string | null {
	const normalizedCandidate = normalizeNodeName(candidate);
	if (!normalizedCandidate) return null;
	const excludeNodeId = String(opts?.excludeNodeId ?? '').trim();
	const scopeNodeId = String(opts?.scopeNodeId ?? opts?.excludeNodeId ?? '').trim();
	const scopeKey = nodeScopeKey(scopeNodeId);
	for (const node of existingNodes) {
		if (excludeNodeId && String(node.id) === excludeNodeId) continue;
		if (scopeKey !== nodeScopeKey(node.id)) continue;
		const normalized = normalizeNodeName(displayNodeName(node));
		if (normalized === normalizedCandidate) return String(node.id);
	}
	return null;
}

export function annotateCanonicalNodeNames(
	nodes: Array<Node<PipelineNodeData>>
): Array<Node<PipelineNodeData>> {
	return nodes.map((node) => {
		const canonical = canonicalNodeName(nodes, node);
		const existingMeta =
			(node.data as any)?.meta && typeof (node.data as any)?.meta === 'object'
				? { ...((node.data as any).meta as Record<string, unknown>) }
				: {};
		return {
			...node,
			data: {
				...node.data,
				meta: {
					...existingMeta,
					canonicalName: canonical
				}
			}
		};
	});
}
