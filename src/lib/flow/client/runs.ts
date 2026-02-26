import type { RunEvent } from "$lib/flow/types/run";

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
