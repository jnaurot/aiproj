import type { NodeBindingInfo } from '$lib/flow/store/graphStore.types';

export type CheckpointEligibility = {
	canSave: boolean;
	reason: string | null;
	artifactId: string;
	execKey: string;
	memoKey: string;
};

export function computeCheckpointEligibility(binding: NodeBindingInfo | null | undefined): CheckpointEligibility {
	const source = (binding ?? {}) as NodeBindingInfo;
	const status = String(source?.status ?? '').trim().toLowerCase();
	if (status === 'running' || status === 'active' || status === 'busy' || status === 'blocked' || status === 'paused') {
		return {
			canSave: false,
			reason: 'Checkpoint save is unavailable while this node is executing.',
			artifactId: '',
			execKey: '',
			memoKey: ''
		};
	}

	const current = source?.current && typeof source.current === 'object' ? source.current : null;
	const last = source?.last && typeof source.last === 'object' ? source.last : null;
	const lineage = current && String(current.artifactId ?? '').trim() ? current : last;
	const artifactId = String(lineage?.artifactId ?? '').trim();
	const execKey = String(lineage?.execKey ?? '').trim();
	const memoKey = String(source?.memoState?.memoKey ?? '').trim();

	if (source?.isUpToDate === false || status === 'stale') {
		return {
			canSave: false,
			reason: 'Checkpoint save requires a current (non-stale) artifact binding.',
			artifactId,
			execKey,
			memoKey
		};
	}

	if (!artifactId) {
		return {
			canSave: false,
			reason: 'Checkpoint save requires a current bound artifact. Run this node first.',
			artifactId,
			execKey,
			memoKey
		};
	}
	if (!/^[0-9a-f]{64}$/i.test(memoKey)) {
		return {
			canSave: false,
			reason: 'Checkpoint save requires a memo fingerprint (memo key). Re-run the node to populate memo trace.',
			artifactId,
			execKey,
			memoKey
		};
	}
	return {
		canSave: true,
		reason: null,
		artifactId,
		execKey,
		memoKey
	};
}
