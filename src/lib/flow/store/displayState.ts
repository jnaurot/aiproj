import type { NodeStatus } from '$lib/flow/types';

export type RuntimeNodeStatus =
	| 'idle'
	| 'running'
	| 'active'
	| 'blocked'
	| 'paused'
	| 'succeeded_up_to_date'
	| 'succeeded'
	| 'failed'
	| 'canceled'
	| 'stale'
	| 'busy';

export type NodeBindingProjectionInput = {
	isUpToDate?: boolean;
	status?: unknown;
	current?: { execKey?: string | null; artifactId?: string | null } | null;
	last?: { execKey?: string | null; artifactId?: string | null } | null;
	currentArtifactId?: string | null;
	lastArtifactId?: string | null;
	currentExecKey?: string | null;
	lastExecKey?: string | null;
	[key: string]: unknown;
};

function normalizeRuntimeStatus(raw: unknown): RuntimeNodeStatus | null {
	const value = String(raw ?? '')
		.trim()
		.toLowerCase();
	if (!value) return null;
	if (value === 'idle') return 'idle';
	if (value === 'running') return 'running';
	if (value === 'active') return 'active';
	if (value === 'blocked') return 'blocked';
	if (value === 'paused') return 'paused';
	if (value === 'succeeded_up_to_date') return 'succeeded_up_to_date';
	if (value === 'succeeded') return 'succeeded';
	if (value === 'failed') return 'failed';
	if (value === 'canceled') return 'canceled';
	if (value === 'stale') return 'stale';
	if (value === 'busy') return 'busy';
	return null;
}

export function projectNodeDisplayState(
	binding: NodeBindingProjectionInput | null | undefined,
	runtimeStatus?: unknown
): NodeStatus {
	const source = binding ?? {};
	const normalizedRuntime = normalizeRuntimeStatus(runtimeStatus ?? source.status);
	const currentArtifactId = source.current?.artifactId ?? source.currentArtifactId;
	const lastArtifactId = source.last?.artifactId ?? source.lastArtifactId;
	const currentExecKey = source.current?.execKey ?? source.currentExecKey;
	const lastExecKey = source.last?.execKey ?? source.lastExecKey;
	const hasArtifact = Boolean(currentArtifactId || lastArtifactId);

	if (normalizedRuntime === 'running' || normalizedRuntime === 'active') return 'running';
	if (normalizedRuntime === 'busy' || normalizedRuntime === 'blocked' || normalizedRuntime === 'paused') return 'busy';
	if (normalizedRuntime === 'failed') return 'failed';
	if (normalizedRuntime === 'canceled') return 'canceled';

	if (source.isUpToDate === false || normalizedRuntime === 'stale') return 'stale';
	if (
		typeof currentExecKey === 'string' &&
		typeof lastExecKey === 'string' &&
		currentExecKey &&
		lastExecKey &&
		currentExecKey !== lastExecKey
	) {
		return 'stale';
	}

	if (normalizedRuntime === 'succeeded_up_to_date' || normalizedRuntime === 'succeeded') return 'succeeded';
	if (!normalizedRuntime && hasArtifact) return 'succeeded';
	return 'idle';
}
