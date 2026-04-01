import type { RunEvent } from "$lib/flow/types/run";
import { backendUrl } from "$lib/flow/client/backend";

export type EventBatcherOptions = {
	maxBatchSize?: number;
	maxDelayMs?: number;
};

export function createEventBatcher<T>(
	onBatch: (events: T[]) => void,
	options?: EventBatcherOptions
) {
	const maxBatchSize = Math.max(1, Number(options?.maxBatchSize ?? 32));
	const maxDelayMs = Math.max(1, Number(options?.maxDelayMs ?? 16));
	let queue: T[] = [];
	let timer: ReturnType<typeof setTimeout> | null = null;

	const flush = () => {
		if (timer) {
			clearTimeout(timer);
			timer = null;
		}
		if (queue.length === 0) return;
		const batch = queue;
		queue = [];
		onBatch(batch);
	};

	const schedule = () => {
		if (timer) return;
		timer = setTimeout(() => {
			timer = null;
			flush();
		}, maxDelayMs);
	};

	return {
		push(event: T) {
			queue.push(event);
			if (queue.length >= maxBatchSize) {
				flush();
				return;
			}
			schedule();
		},
		flush,
		clear() {
			if (timer) {
				clearTimeout(timer);
				timer = null;
			}
			queue = [];
		}
	};
}

function requireGraphId(graphId: string): string {
  const g = String(graphId ?? "").trim();
  if (!g) throw new Error("graphId is required for artifact requests");
  return g;
}

function withGraphId(path: string, graphId: string, extra?: Record<string, string | number>) {
  const params = new URLSearchParams({ graphId: requireGraphId(graphId) });
  for (const [k, v] of Object.entries(extra ?? {})) params.set(k, String(v));
  return backendUrl(`${path}?${params.toString()}`);
}

export function getArtifactUrl(artifactId: string, graphId: string) {
  return withGraphId(`/runs/artifacts/${encodeURIComponent(artifactId)}`, graphId);
}

export function getArtifactMetaUrl(artifactId: string, graphId: string) {
  return withGraphId(`/runs/artifacts/${encodeURIComponent(artifactId)}/meta`, graphId);
}

export function getArtifactPreviewUrl(
  artifactId: string,
  graphId: string,
  offset: number,
  limit: number
) {
  return withGraphId(`/runs/artifacts/${encodeURIComponent(artifactId)}/preview`, graphId, {
    offset,
    limit
  });
}

export function getArtifactConsumersUrl(artifactId: string, graphId: string, limit = 50) {
  return withGraphId(`/runs/artifacts/${encodeURIComponent(artifactId)}/consumers`, graphId, {
    limit
  });
}

export function getArtifactLineageUrl(artifactId: string, graphId: string, depth = 1) {
  return withGraphId(`/runs/artifacts/${encodeURIComponent(artifactId)}/lineage`, graphId, {
    depth
  });
}

// src/lib/api/runs.ts
export async function createRun(req: {
  graphId: string;
  runFrom?: string | null;
  runMode?: "from_selected_onward" | "selected_only";
  cacheMode?: "default_on" | "force_off" | "force_on";
  adaptive?: { mode: "off" | "observe" | "enforce" };
  graph: any;
}) {
  const res = await fetch(backendUrl("/api/runs"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(req),
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`createRun failed: ${res.status} ${text}`);
  }

  return (await res.json()) as { runId: string; graphId: string };
}

export async function getRun(runId: string) {
  const res = await fetch(backendUrl(`/api/runs/${encodeURIComponent(runId)}`));
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`getRun failed: ${res.status} ${text}`);
  }
  return (await res.json()) as {
    runId: string;
    graphId?: string;
    status: string;
    runMode?: "from_start" | "from_selected_onward" | "selected_only";
    plannedNodeIds?: string[];
    nodeStatus?: Record<string, string>;
    nodeOutputs?: Record<string, string>;
    nodeBindings?: Record<string, Record<string, unknown>>;
  };
}

export type RegressionAlert = {
	type: 'latency_regression' | 'failure_regression' | string;
	reasonCode: string;
	nodeId?: string;
	errorCode?: string;
	metric?: string;
	baseline?: number;
	current?: number;
	driftPct?: number;
	thresholdPct?: number;
	delta?: number;
	thresholdAbs?: number;
};

export type ExperimentNodeTrendPoint = {
	runId: string;
	createdAt: string;
	nodeId: string;
	metric: string;
	value: number;
};

export type ExperimentSlaBreach = {
	runId: string;
	createdAt: string;
	nodeId: string;
	p95Ms: number;
	thresholdMs: number;
};

export type ExperimentFailureTaxonomyItem = {
	errorCode: string;
	count: number;
};

export async function getExperimentRegressions(params: {
	runId: string;
	baselineRunId?: string | null;
	latencyDriftPct?: number;
	failureDriftAbs?: number;
}) {
	const qs = new URLSearchParams();
	qs.set('runId', String(params.runId ?? '').trim());
	if (params.baselineRunId) qs.set('baselineRunId', String(params.baselineRunId).trim());
	if (Number.isFinite(Number(params.latencyDriftPct)))
		qs.set('latencyDriftPct', String(Number(params.latencyDriftPct)));
	if (Number.isFinite(Number(params.failureDriftAbs)))
		qs.set('failureDriftAbs', String(Math.max(0, Number(params.failureDriftAbs))));
	const res = await fetch(backendUrl(`/api/experiments/regressions?${qs.toString()}`));
	if (!res.ok) {
		const text = await res.text().catch(() => '');
		throw new Error(`getExperimentRegressions failed: ${res.status} ${text}`);
	}
	return (await res.json()) as {
		schemaVersion: 1;
		runId: string;
		baselineRunId: string;
		alerts: RegressionAlert[];
	};
}

export async function getExperimentNodeTrends(params: {
	graphId: string;
	nodeId?: string | null;
	metric?: string;
	limit?: number;
}) {
	const qs = new URLSearchParams();
	qs.set('graphId', String(params.graphId ?? '').trim());
	if (params.nodeId) qs.set('nodeId', String(params.nodeId).trim());
	if (params.metric) qs.set('metric', String(params.metric).trim());
	if (Number.isFinite(Number(params.limit))) qs.set('limit', String(Number(params.limit)));
	const res = await fetch(backendUrl(`/api/experiments/trends/nodes?${qs.toString()}`));
	if (!res.ok) {
		const text = await res.text().catch(() => '');
		throw new Error(`getExperimentNodeTrends failed: ${res.status} ${text}`);
	}
	return (await res.json()) as {
		schemaVersion: 1;
		graphId?: string | null;
		nodeId?: string | null;
		metric: string;
		points: ExperimentNodeTrendPoint[];
	};
}

export async function getExperimentSlaBreaches(params: {
	graphId: string;
	p95Ms?: number;
	limit?: number;
}) {
	const qs = new URLSearchParams();
	qs.set('graphId', String(params.graphId ?? '').trim());
	if (Number.isFinite(Number(params.p95Ms))) qs.set('p95Ms', String(Number(params.p95Ms)));
	if (Number.isFinite(Number(params.limit))) qs.set('limit', String(Number(params.limit)));
	const res = await fetch(backendUrl(`/api/experiments/sla/breaches?${qs.toString()}`));
	if (!res.ok) {
		const text = await res.text().catch(() => '');
		throw new Error(`getExperimentSlaBreaches failed: ${res.status} ${text}`);
	}
	return (await res.json()) as {
		schemaVersion: 1;
		graphId?: string | null;
		thresholdMs: number;
		breaches: ExperimentSlaBreach[];
	};
}

export async function getExperimentFailureTaxonomy(params: { graphId: string; limit?: number }) {
	const qs = new URLSearchParams();
	qs.set('graphId', String(params.graphId ?? '').trim());
	if (Number.isFinite(Number(params.limit))) qs.set('limit', String(Number(params.limit)));
	const res = await fetch(backendUrl(`/api/experiments/failures/taxonomy?${qs.toString()}`));
	if (!res.ok) {
		const text = await res.text().catch(() => '');
		throw new Error(`getExperimentFailureTaxonomy failed: ${res.status} ${text}`);
	}
	return (await res.json()) as {
		schemaVersion: 1;
		graphId?: string | null;
		taxonomy: ExperimentFailureTaxonomyItem[];
	};
}

export async function cancelRun(
	runId: string,
	options?: { hard?: boolean }
): Promise<{ runId: string; status: string; cancelRequested: boolean }> {
	const res = await fetch(backendUrl(`/api/runs/${encodeURIComponent(runId)}/cancel`), {
		method: 'POST',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify({ hard: Boolean(options?.hard ?? true) })
	});
	if (!res.ok) {
		const text = await res.text().catch(() => '');
		throw new Error(`cancelRun failed: ${res.status} ${text}`);
	}
	return (await res.json()) as { runId: string; status: string; cancelRequested: boolean };
}

export async function cancelAllRuns(req?: { graphId?: string; hard?: boolean }) {
	const res = await fetch(backendUrl('/api/runs/cancel-all'), {
		method: 'POST',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify({
			graphId: req?.graphId,
			hard: Boolean(req?.hard ?? true)
		})
	});
	if (!res.ok) {
		const text = await res.text().catch(() => '');
		throw new Error(`cancelAllRuns failed: ${res.status} ${text}`);
	}
	return (await res.json()) as {
		schemaVersion: 1;
		graphId?: string | null;
		hard: boolean;
		matchedRunIds: string[];
		cancelRequestedRunIds: string[];
		alreadyRequestedRunIds: string[];
		alreadyTerminalRunIds: string[];
		hardCancelledRunIds: string[];
	};
}

export async function pauseRun(
	runId: string
): Promise<{ runId: string; status: string; pauseRequested: boolean }> {
	const res = await fetch(backendUrl(`/api/runs/${encodeURIComponent(runId)}/pause`), {
		method: 'POST',
		headers: { 'Content-Type': 'application/json' }
	});
	if (!res.ok) {
		const text = await res.text().catch(() => '');
		throw new Error(`pauseRun failed: ${res.status} ${text}`);
	}
	return (await res.json()) as { runId: string; status: string; pauseRequested: boolean };
}

export async function resumeRun(
	runId: string
): Promise<{ runId: string; status: string; resumed: boolean }> {
	const res = await fetch(backendUrl(`/api/runs/${encodeURIComponent(runId)}/resume`), {
		method: 'POST',
		headers: { 'Content-Type': 'application/json' }
	});
	if (!res.ok) {
		const text = await res.text().catch(() => '');
		throw new Error(`resumeRun failed: ${res.status} ${text}`);
	}
	return (await res.json()) as { runId: string; status: string; resumed: boolean };
}

export async function acceptNodeParams(req: {
  runId: string;
  nodeId: string;
  graph: any;
  params: Record<string, unknown>;
}) {
  console.log("[accept-params] req.params", req.params);
  const res = await fetch(
    backendUrl(`/runs/${encodeURIComponent(req.runId)}/nodes/${encodeURIComponent(req.nodeId)}/accept-params`),
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ graph: req.graph, params: req.params })
    }
  );
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`acceptNodeParams failed: ${res.status} ${text}`);
  }
  return (await res.json()) as {
    runId: string;
    nodeId: string;
    affectedNodeIds: string[];
    status: string;
  };
}

export async function resolveSourceNode(req: {
	graphId: string;
	graph: any;
	nodeId: string;
	params?: Record<string, unknown>;
}) {
	const res = await fetch(backendUrl('/api/runs/resolve/source'), {
		method: 'POST',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify(req)
	});
	if (!res.ok) {
		const text = await res.text().catch(() => '');
		throw new Error(`resolveSourceNode failed: ${res.status} ${text}`);
	}
	return (await res.json()) as {
		graphId: string;
		nodeId: string;
		execKey: string;
		artifactId: string | null;
		cacheHit: boolean;
		artifact?: {
			artifactId: string;
			mimeType?: string;
			payloadType?: string;
			sizeBytes?: number;
			createdAt?: string;
			contentHash?: string;
		};
		snapshotId?: string;
	};
}


export function streamRunEvents(
  runId: string,
  onEvent: (ev: RunEvent) => void,
  onError: (err: unknown) => void
) {
  const es = new EventSource(backendUrl(`/api/runs/${runId}/events`));
  let closed = false;
  let terminalSeen = false;

  es.onmessage = (msg) => {
    if (closed) return;
    try {
      const parsed = JSON.parse(msg.data) as RunEvent;
      if ((parsed as any)?.type === 'run_finished' || (parsed as any)?.type === 'run_paused') terminalSeen = true;
      onEvent(parsed);
    } catch (e) {
      onError(e);
    }
  };

  es.onerror = (e) => {
    if (closed) return;
    if (terminalSeen || es.readyState === EventSource.CLOSED) {
      closed = true;
      es.close();
      return;
    }
    closed = true;
    es.close();
    onError(e);
  };

  return {
    close: () => {
      if (closed) return;
      closed = true;
      es.close();
    }
  };
}

export async function uploadSnapshot(file: File) {
  const body = new FormData();
  body.append("file", file);
  const res = await fetch(backendUrl("/api/snapshots"), {
    method: "POST",
    body,
  });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`uploadSnapshot failed: ${res.status} ${text}`);
  }
  return (await res.json()) as {
    snapshotId: string;
    metadata: {
      snapshotId: string;
      originalFilename?: string;
      byteSize?: number;
      mimeType?: string;
      importedAt?: string;
      graphId?: string;
    };
  };
}

export async function getSnapshotMeta(snapshotId: string) {
	const sid = String(snapshotId ?? '').trim();
	if (!sid) throw new Error('snapshotId is required');
	const res = await fetch(backendUrl(`/api/snapshots/${encodeURIComponent(sid)}/meta`));
	if (!res.ok) {
		const text = await res.text().catch(() => '');
		throw new Error(`getSnapshotMeta failed: ${res.status} ${text}`);
	}
	return (await res.json()) as {
		snapshotId: string;
		metadata?: {
			snapshotId?: string;
			originalFilename?: string;
			byteSize?: number;
			mimeType?: string;
			importedAt?: string;
			graphId?: string;
		};
	};
}

export async function getSnapshot(snapshotId: string) {
	const sid = String(snapshotId ?? '').trim();
	if (!sid) throw new Error('snapshotId is required');
	const res = await fetch(backendUrl(`/api/snapshots/${encodeURIComponent(sid)}`));
	if (!res.ok) {
		const text = await res.text().catch(() => '');
		throw new Error(`getSnapshot failed: ${res.status} ${text}`);
	}
	return (await res.json()) as {
		snapshotId: string;
		metadata?: {
			snapshotId?: string;
			originalFilename?: string;
			byteSize?: number;
			mimeType?: string;
			importedAt?: string;
			graphId?: string;
		};
	};
}

export async function getGlobalCacheConfig() {
	const res = await fetch(backendUrl('/runs/cache/config'));
	if (!res.ok) {
		const text = await res.text().catch(() => '');
		throw new Error(`getGlobalCacheConfig failed: ${res.status} ${text}`);
	}
	return (await res.json()) as {
		schemaVersion: number;
		enabled: boolean;
		mode?: 'default_on' | 'force_off' | 'force_on';
	};
}

export async function setGlobalCacheConfig(
	config: boolean | { enabled?: boolean; mode?: 'default_on' | 'force_off' | 'force_on' }
) {
	const payload =
		typeof config === 'boolean'
			? { enabled: config }
			: {
					enabled: config.enabled,
					mode: config.mode
				};
	const res = await fetch(backendUrl('/runs/cache/config'), {
		method: 'PUT',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify(payload)
	});
	if (!res.ok) {
		const text = await res.text().catch(() => '');
		throw new Error(`setGlobalCacheConfig failed: ${res.status} ${text}`);
	}
	return (await res.json()) as {
		schemaVersion: number;
		enabled: boolean;
		mode?: 'default_on' | 'force_off' | 'force_on';
	};
}

export type DbSchemaColumn = {
	name: string;
	normalizedType: string;
	nativeType: string;
	nullable: boolean;
	ordinal: number;
};

export type DbSchemaTable = {
	schema: string;
	name: string;
	kind: 'table' | 'view';
	columns: DbSchemaColumn[];
};

export async function getToolDbSchema(connectionRef: string) {
	const res = await fetch(backendUrl('/runs/tools/db/schema'), {
		method: 'POST',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify({ connectionRef })
	});
	if (!res.ok) {
		const text = await res.text().catch(() => '');
		throw new Error(`getToolDbSchema failed: ${res.status} ${text}`);
	}
	return (await res.json()) as {
		schemaVersion: number;
		connectionRef: string;
		tables: DbSchemaTable[];
	};
}
