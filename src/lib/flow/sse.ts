import { applyRunEvent, currentRunId } from "./stores";
import type { RunEvent, KnownRunEvent } from "./types";

let es: EventSource | null = null;

export function connectRunEvents(runId: string) {
  disconnectRunEvents();
  currentRunId.set(runId);

  es = new EventSource(`/api/runs/${encodeURIComponent(runId)}/events`);

  es.onerror = () => {
    // Browser will retry automatically for SSE; you can add UI here if desired
  };
}

export function disconnectRunEvents() {
  if (es) {
    es.close();
    es = null;
  }
}

function isKnownRunEvent(ev: RunEvent): ev is KnownRunEvent {
  return (
    ev !== null &&
    typeof ev === "object" &&
    "type" in ev &&
    typeof (ev as any).type === "string" &&
    // optionally: only accept types we know
    [
      "run_started",
      "run_finished",
      "node_started",
      "node_finished",
      "edge_exec",
      "log",
      "node_output",
    ].includes((ev as any).type)
  );
}

es.onmessage = (msg) => {
  try {
    const ev = JSON.parse(msg.data) as RunEvent;

    if (isKnownRunEvent(ev)) {
      applyRunEvent(ev);        // ✅ KnownRunEvent
    } else {
      // ignore unknown events (or console.debug)
      // console.debug("Unknown run event", ev);
    }
  } catch {
    // ignore malformed events
  }
};
