import { projectNodeDisplayState, type NodeBindingProjectionInput } from '$lib/flow/store/displayState';

type NodeOutputInfoLike = {
	cached?: boolean;
	cacheDecision?: 'cache_hit' | 'cache_miss' | 'cache_hit_contract_mismatch';
};

type NodeBindingLike = {
	status?: string;
	isUpToDate?: boolean;
	current?: { execKey?: string | null; artifactId?: string | null } | null;
	currentArtifactId?: string | null;
};

type HeaderNodeStatus = 'idle' | 'stale' | 'running' | 'busy' | 'succeeded' | 'failed' | 'canceled';

export type HeaderCachePill = {
	label: 'cached' | 'cached:mismatch';
	className: string;
	title: string;
} | null;

function outputCacheLabel(nodeOut: NodeOutputInfoLike | undefined): 'cached' | 'cached:mismatch' | '' {
	if (nodeOut?.cacheDecision === 'cache_hit_contract_mismatch') return 'cached:mismatch';
	if (nodeOut?.cacheDecision === 'cache_hit') return 'cached';
	return '';
}

export function getHeaderNodeStatus(binding: NodeBindingLike | undefined): HeaderNodeStatus {
	return projectNodeDisplayState(binding as NodeBindingProjectionInput | undefined, binding?.status) as HeaderNodeStatus;
}

export function getHeaderCachePill(
	nodeOut: NodeOutputInfoLike | undefined,
	binding: NodeBindingLike | undefined,
	displayNodeStatus: string | null | undefined
): HeaderCachePill {
	const label = outputCacheLabel(nodeOut);
	const currentArtifactId = binding?.current?.artifactId ?? binding?.currentArtifactId ?? null;
	// Only show cache badge when the node is currently succeeded/up-to-date.
	// This prevents stale/running nodes from showing misleading cached status.
	if (!label || displayNodeStatus !== 'succeeded') return null;
	if (binding?.isUpToDate !== true) return null;
	if (!currentArtifactId) return null;
	const statusClass = `st-${displayNodeStatus ?? 'idle'}`;
	if (label === 'cached:mismatch') {
		return {
			label,
			className: `pill pill-cache ${statusClass} pill-cache-mismatch`,
			title: 'Cache hit but contract mismatch; recompute required'
		};
	}
	return {
		label,
		className: `pill pill-cache ${statusClass}`,
		title: 'Reused from cache'
	};
}

