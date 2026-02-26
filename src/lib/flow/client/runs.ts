import type { RunEvent } from "$lib/flow/types/run";

function requireGraphId(graphId: string): string {
  const g = String(graphId ?? "").trim();
  if (!g) throw new Error("graphId is required for artifact requests");
  return g;
}

function withGraphId(path: string, graphId: string, extra?: Record<string, string | number>) {
  const params = new URLSearchParams({ graphId: requireGraphId(graphId) });
  for (const [k, v] of Object.entries(extra ?? {})) params.set(k, String(v));
  return `${path}?${params.toString()}`;
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
  graph: any;
}) {
  const res = await fetch("/api/runs", {
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
  const res = await fetch(`/runs/${encodeURIComponent(runId)}`);
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

export async function acceptNodeParams(req: {
  runId: string;
  nodeId: string;
  graph: any;
  params: Record<string, unknown>;
}) {
  const res = await fetch(
    `/runs/${encodeURIComponent(req.runId)}/nodes/${encodeURIComponent(req.nodeId)}/accept-params`,
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


export function streamRunEvents(
  runId: string,
  onEvent: (ev: RunEvent) => void,
  onError: (err: unknown) => void
) {
  const es = new EventSource(`/api/runs/${runId}/events`);

  es.onmessage = (msg) => {
    try {
      onEvent(JSON.parse(msg.data));
    } catch (e) {
      onError(e);
    }
  };

  es.onerror = (e) => {
    es.close();
    onError(e);
  };

  return { close: () => es.close() };
}
