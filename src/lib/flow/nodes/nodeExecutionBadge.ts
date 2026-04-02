export type ConsumeMode = 'once' | 'single_item' | 'batch';

export type NodeRuntimeCounts = {
	accepted: number;
	rejected: number;
	total: number;
};

export type NodeExecutionBadge = {
	mode: ConsumeMode;
	label: string;
	detail: string;
};

function asRecord(value: unknown): Record<string, unknown> {
	return value && typeof value === 'object' ? (value as Record<string, unknown>) : {};
}

function clampInt(value: unknown): number {
	const numeric = Number(value ?? 0);
	if (!Number.isFinite(numeric)) return 0;
	return Math.max(0, Math.trunc(numeric));
}

export function normalizeConsumeMode(policy: unknown): ConsumeMode {
	const raw = String(asRecord(policy).consume_mode ?? 'once')
		.trim()
		.toLowerCase();
	if (raw === 'single_item' || raw === 'batch') return raw;
	return 'once';
}

export function resolveNodeRuntimeCounts(queueRuntime: unknown, nodeId: string): NodeRuntimeCounts {
	const runtime = asRecord(queueRuntime);
	const scoped = asRecord(runtime.runScoped);
	const scopedMetrics = asRecord(scoped.runtimeItemMetrics);
	const rootMetrics = asRecord(runtime.runtimeItemMetrics);
	const scopedCounters = asRecord(scopedMetrics.nodeCounters);
	const rootCounters = asRecord(rootMetrics.nodeCounters);
	const counter = asRecord(scopedCounters[nodeId] ?? rootCounters[nodeId]);
	const accepted = clampInt(counter.accepted);
	const rejected = clampInt(counter.rejected);
	return {
		accepted,
		rejected,
		total: accepted + rejected
	};
}

export function buildNodeExecutionBadge(mode: ConsumeMode, counts: NodeRuntimeCounts, batchSizeRaw: unknown): NodeExecutionBadge {
	const total = clampInt(counts.total);
	if (mode === 'single_item') {
		return {
			mode,
			label: 'single',
			detail: String(total)
		};
	}
	if (mode === 'batch') {
		const batchSize = Math.max(1, clampInt(batchSizeRaw) || 1);
		const batches = total <= 0 ? 0 : Math.ceil(total / batchSize);
		return {
			mode,
			label: 'batch',
			detail: `${total}/${batches}`
		};
	}
	return {
		mode: 'once',
		label: 'once',
		detail: total > 0 ? '1/1' : '0/1'
	};
}
