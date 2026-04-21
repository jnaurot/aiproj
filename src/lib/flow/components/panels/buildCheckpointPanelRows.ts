import type { CheckpointRecord } from '$lib/flow/types/checkpoint';
import { parsePromotedCheckpointKey } from '$lib/flow/types/checkpoint';
import type { ComponentEditSession } from '$lib/flow/store/graphStore.types';
import type { CheckpointPanelRow } from './CheckpointRegistryPanel.svelte';

type NodeLike = { id: string; data?: Record<string, unknown> };

function nodeLabelFromNodes(nodeId: string, nodes: NodeLike[]): string {
	const node = nodes.find((n) => String(n?.id ?? '') === nodeId);
	if (!node) return nodeId;
	const label = String((node?.data as any)?.label ?? '').trim();
	return label || nodeId;
}

const COMPONENT_DRAFT_GRAPH_KEY = '__graphDraft';

function readDraftNodesFromCacheEntry(
	cacheEntry: unknown
): NodeLike[] {
	if (!cacheEntry || typeof cacheEntry !== 'object') return [];
	const graph = (cacheEntry as Record<string, unknown>)[COMPONENT_DRAFT_GRAPH_KEY];
	if (!graph || typeof graph !== 'object') return [];
	const nodes = (graph as any)?.nodes;
	return Array.isArray(nodes) ? nodes : [];
}

/**
 * Build checkpoint panel rows from the checkpoint registry, resolving names
 * for both graph-level and promoted (cmp:-prefixed) internal node checkpoints.
 *
 * Promoted entries use a `cmp:componentNodeId:innerNodeId` key format. Their
 * node names are resolved by looking up the component node label and the inner
 * node label from the component draft cache.
 *
 * Rows are sorted by createdAt descending, with promoted entries grouped
 * under their component node using an indented tree style.
 */
export function buildCheckpointPanelRows(
	checkpointRegistry: Record<string, unknown>,
	nodes: NodeLike[],
	componentContractDraftCache: Record<string, unknown>,
	componentEditSession: ComponentEditSession | null
): CheckpointPanelRow[] {
	const registry = checkpointRegistry ?? {};
	const rows: CheckpointPanelRow[] = [];

	// Track which component node IDs we've seen so we can emit group headers.
	const seenComponentNodes = new Map<string, string>();

	for (const [key, rawCheckpoint] of Object.entries(registry)) {
		const checkpoint = rawCheckpoint as CheckpointRecord;
		if (!checkpoint || typeof checkpoint !== 'object') continue;

		const promoted = parsePromotedCheckpointKey(key);
		if (promoted) {
			// This is a promoted internal checkpoint.
			const { componentNodeId, innerNodeId } = promoted;

			// Resolve component node label from top-level nodes.
			const componentLabel = nodeLabelFromNodes(componentNodeId, nodes);

			// Resolve inner node label from the component draft cache.
			// Find the component node to get its cache key.
			const componentNode = nodes.find(
				(n) => String(n?.id ?? '') === componentNodeId
			);
			const ref = ((componentNode?.data as any)?.params ?? {})?.componentRef ?? {};
			const componentId = String(ref?.componentId ?? '').trim();
			const revisionId = String(ref?.revisionId ?? '').trim();
			let innerLabel = innerNodeId;
			if (componentId && revisionId) {
				const cacheKey = `${componentId}@${revisionId}`;
				const cacheEntry = componentContractDraftCache[cacheKey];
				const draftNodes = readDraftNodesFromCacheEntry(cacheEntry);
				innerLabel = nodeLabelFromNodes(innerNodeId, draftNodes);
			}

			// Calculate depth from nesting level (cmp:A:B = depth 1, cmp:A:B:C = depth 2).
			const depth = (key.match(/:/g) ?? []).length - 1;

			rows.push({
				nodeId: key,
				nodeName: innerLabel,
				checkpoint,
				depth,
				isPromoted: true,
				componentPath: componentLabel,
				removable: false
			});
		} else {
			// Graph-level checkpoint.
			const nodeName = nodeLabelFromNodes(key, nodes);
			rows.push({
				nodeId: key,
				nodeName,
				checkpoint,
				depth: 0,
				isPromoted: false,
				removable: true
			});
		}
	}

	// Sort: promoted entries grouped by component, then by createdAt desc.
	// Graph-level entries first, then promoted entries grouped by componentNodeId.
	rows.sort((a, b) => {
		// Graph-level before promoted
		if (!a.isPromoted && b.isPromoted) return -1;
		if (a.isPromoted && !b.isPromoted) return 1;
		// Within promoted: group by componentPath
		if (a.isPromoted && b.isPromoted) {
			const aComponent = a.componentPath ?? '';
			const bComponent = b.componentPath ?? '';
			if (aComponent !== bComponent) return aComponent.localeCompare(bComponent);
		}
		// Within same group: by createdAt descending
		const aDate = String(a.checkpoint?.createdAt ?? '');
		const bDate = String(b.checkpoint?.createdAt ?? '');
		return bDate.localeCompare(aDate);
	});

	return rows;
}