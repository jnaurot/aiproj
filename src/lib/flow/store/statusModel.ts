import type { EdgeExecState, EdgeMode, NodeStatus } from '$lib/flow/types';

export type NodeLifecycleStatus =
	| 'idle'
	| 'waiting'
	| 'running'
	| 'blocked'
	| 'completed'
	| 'failed'
	| 'canceled'
	| 'skipped';

export type NodeExecutionStatus = 'inactive' | 'waiting' | 'running' | 'blocked' | 'finished';

export type NodeFreshnessStatus = 'unknown' | 'fresh' | 'stale';

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
	| 'skipped'
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

export type NodeStatusProjection = {
	lifecycle: NodeLifecycleStatus;
	execution: NodeExecutionStatus;
	freshness: NodeFreshnessStatus;
	hasArtifact: boolean;
	runtime: RuntimeNodeStatus | null;
	display: NodeStatus;
};

export type EdgeLifecycleStatus = 'inactive' | 'waiting' | 'running' | 'done';

export type EdgeStatusProjectionInput = {
	exec?: unknown;
	mode?: unknown;
	depth?: unknown;
	blocked?: unknown;
	full?: unknown;
};

export type EdgeStatusProjection = {
	lifecycle: EdgeLifecycleStatus;
	exec: EdgeExecState;
	diagnostics: {
		depth: number;
		blocked: boolean;
		full: boolean;
	};
};

export function normalizeRuntimeStatus(raw: unknown): RuntimeNodeStatus | null {
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
	if (value === 'cancelled') return 'canceled';
	if (value === 'skipped') return 'skipped';
	if (value === 'stale') return 'stale';
	if (value === 'busy') return 'busy';
	return null;
}

function normalizeEdgeExec(raw: unknown): EdgeExecState {
	const value = String(raw ?? '')
		.trim()
		.toLowerCase();
	if (value === 'active') return 'active';
	if (value === 'done') return 'done';
	return 'idle';
}

function normalizeEdgeMode(raw: unknown): EdgeMode {
	const value = String(raw ?? '')
		.trim()
		.toLowerCase();
	if (value === 'param') return 'param';
	if (value === 'control') return 'control';
	return 'work';
}

function hasExecDrift(source: NodeBindingProjectionInput): boolean {
	const currentExecKey = source.current?.execKey ?? source.currentExecKey;
	const lastExecKey = source.last?.execKey ?? source.lastExecKey;
	return (
		typeof currentExecKey === 'string' &&
		typeof lastExecKey === 'string' &&
		currentExecKey.length > 0 &&
		lastExecKey.length > 0 &&
		currentExecKey !== lastExecKey
	);
}

export function projectNodeStatus(
	binding: NodeBindingProjectionInput | null | undefined,
	runtimeStatus?: unknown
): NodeStatusProjection {
	const source = binding ?? {};
	const runtime = normalizeRuntimeStatus(runtimeStatus ?? source.status);
	const currentArtifactId = source.current?.artifactId ?? source.currentArtifactId;
	const lastArtifactId = source.last?.artifactId ?? source.lastArtifactId;
	const hasArtifact = Boolean(currentArtifactId || lastArtifactId);
	const stale = source.isUpToDate === false || runtime === 'stale' || hasExecDrift(source);

	let lifecycle: NodeLifecycleStatus = 'idle';
	let execution: NodeExecutionStatus = 'inactive';

	if (runtime === 'running' || runtime === 'active') {
		lifecycle = 'running';
		execution = 'running';
	} else if (runtime === 'blocked') {
		lifecycle = 'blocked';
		execution = 'blocked';
	} else if (runtime === 'busy' || runtime === 'paused') {
		lifecycle = 'waiting';
		execution = 'waiting';
	} else if (runtime === 'failed') {
		lifecycle = 'failed';
		execution = 'finished';
	} else if (runtime === 'canceled') {
		lifecycle = 'canceled';
		execution = 'finished';
	} else if (runtime === 'skipped') {
		lifecycle = 'skipped';
		execution = 'finished';
	} else if (runtime === 'succeeded_up_to_date' || runtime === 'succeeded') {
		lifecycle = 'completed';
		execution = 'finished';
	} else if (runtime === 'stale') {
		lifecycle = 'completed';
		execution = 'inactive';
	} else if (!runtime && hasArtifact) {
		lifecycle = 'completed';
		execution = 'finished';
	}

	let freshness: NodeFreshnessStatus = 'unknown';
	if (lifecycle === 'completed' || hasArtifact) {
		freshness = stale ? 'stale' : 'fresh';
	}

	const display = toDisplayNodeStatus(lifecycle, freshness);

	return {
		lifecycle,
		execution,
		freshness,
		hasArtifact,
		runtime,
		display
	};
}

export function toDisplayNodeStatus(
	lifecycle: NodeLifecycleStatus,
	freshness: NodeFreshnessStatus = 'unknown'
): NodeStatus {
	if (lifecycle === 'running') return 'running';
	if (lifecycle === 'waiting' || lifecycle === 'blocked') return 'busy';
	if (lifecycle === 'failed') return 'failed';
	if (lifecycle === 'canceled') return 'canceled';
	if (lifecycle === 'skipped') return 'skipped';
	if (lifecycle === 'completed') {
		return freshness === 'stale' ? 'stale' : 'succeeded';
	}
	return 'idle';
}

export function projectEdgeStatus(input: EdgeStatusProjectionInput): EdgeStatusProjection {
	const exec = normalizeEdgeExec(input.exec);
	const mode = normalizeEdgeMode(input.mode);
	const depth = Math.max(0, Number(input.depth ?? 0));
	const blocked = Boolean(input.blocked ?? false);
	const full = Boolean(input.full ?? false);

	let lifecycle: EdgeLifecycleStatus = 'inactive';
	if (exec === 'done') {
		lifecycle = 'done';
	} else if (mode === 'work' && exec === 'active') {
		lifecycle = 'running';
	} else if (depth > 0 || blocked || full) {
		lifecycle = 'waiting';
	}

	return {
		lifecycle,
		exec,
		diagnostics: { depth, blocked, full }
	};
}
