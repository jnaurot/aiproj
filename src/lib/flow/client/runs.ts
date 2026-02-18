import type { RunEvent } from "$lib/flow/types/run";

// src/lib/api/runs.ts
export async function createRun(req: { runFrom: string | null; graph: any }) {
  const payload = req
  console.log("RUN PAYLOAD (client -> server):", payload);
try {
  console.log("RUN PAYLOAD JSON:", JSON.stringify(payload, null, 2));
} catch (e) {
  console.warn("RUN PAYLOAD stringify failed", e);
}


//UNDO???
  const res = await fetch("/api/runs", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(req),
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`createRun failed: ${res.status} ${text}`);
  }

  return (await res.json()) as { runId: string };
}


export function streamRunEvents(
  runId: string,
  onEvent: (ev: RunEvent) => void,
  onError: (err: unknown) => void
) {
  console.log("In streamRunEvents")
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
