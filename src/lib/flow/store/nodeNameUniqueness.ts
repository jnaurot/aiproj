import type { Node } from '@xyflow/svelte';
import type { PipelineNodeData } from '$lib/flow/types';

export type NodeNameDuplicateIssue = {
	normalizedName: string;
	displayName: string;
	nodeIds: string[];
};

export function normalizeNodeName(value: unknown): string {
	return String(value ?? '').trim().toLowerCase();
}

export function displayNodeName(node: Node<PipelineNodeData>): string {
	const label = String((node?.data as any)?.label ?? '').trim();
	if (label.length > 0) return label;
	return String(node?.id ?? '').trim();
}

export function findDuplicateNodeNames(nodes: Array<Node<PipelineNodeData>>): NodeNameDuplicateIssue[] {
	const byName = new Map<string, { displayName: string; nodeIds: string[] }>();
	for (const node of nodes) {
		const displayName = displayNodeName(node);
		const normalizedName = normalizeNodeName(displayName);
		if (!normalizedName) continue;
		const existing = byName.get(normalizedName);
		if (existing) {
			existing.nodeIds.push(String(node.id));
			continue;
		}
		byName.set(normalizedName, {
			displayName,
			nodeIds: [String(node.id)]
		});
	}
	const duplicates: NodeNameDuplicateIssue[] = [];
	for (const [normalizedName, entry] of byName.entries()) {
		if (entry.nodeIds.length <= 1) continue;
		duplicates.push({
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
	opts?: { excludeNodeId?: string | null }
): string {
	const base = String(candidate ?? '').trim();
	const safeBase = base || 'Node';
	const excludeNodeId = String(opts?.excludeNodeId ?? '').trim();
	const existing = new Set<string>();
	for (const node of existingNodes) {
		if (excludeNodeId && String(node.id) === excludeNodeId) continue;
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
	opts?: { excludeNodeId?: string | null }
): string | null {
	const normalizedCandidate = normalizeNodeName(candidate);
	if (!normalizedCandidate) return null;
	const excludeNodeId = String(opts?.excludeNodeId ?? '').trim();
	for (const node of existingNodes) {
		if (excludeNodeId && String(node.id) === excludeNodeId) continue;
		const normalized = normalizeNodeName(displayNodeName(node));
		if (normalized === normalizedCandidate) return String(node.id);
	}
	return null;
}
