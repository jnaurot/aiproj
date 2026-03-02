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

type HeaderNodeStatus = 'idle' | 'stale' | 'running' | 'succeeded' | 'failed' | 'canceled';

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
	if (!binding) return 'idle';
	const raw = String(binding.status ?? '').toLowerCase();
	const currentArtifactId = binding.current?.artifactId ?? binding.currentArtifactId ?? null;
	if (raw === 'running') return 'running';
	if (raw === 'failed') return 'failed';
	if (raw === 'cancelled' || raw === 'canceled') return 'canceled';
	if (raw === 'stale' || binding.isUpToDate === false) return 'stale';
	if ((raw === 'succeeded_up_to_date' || raw === 'succeeded') && currentArtifactId) return 'succeeded';
	// Succeeded without current artifact is stale relative to current config.
	if (raw === 'succeeded_up_to_date' || raw === 'succeeded') return 'stale';
	return 'idle';
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
