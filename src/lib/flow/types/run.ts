// src/lib/flow/types/run.ts
export type RunStatus = "idle" | "running" | "succeeded" | "failed" | "canceled";

export type RunRequest = {
  runFrom: string | null; // null = from start
};

export type KnownRunEvent =
  | { type: "run_started"; runId: string; at: string; runFrom: string | null }
  | { type: "run_finished"; runId: string; at: string; status: RunStatus }
  | { type: "node_started"; runId: string; at: string; nodeId: string }
  | { type: "node_finished"; runId: string; at: string; nodeId: string; status: RunStatus; error?: string }
  | { type: "edge_exec"; runId: string; at: string; edgeId: string; exec: "idle" | "active" | "done" }
  | { type: "log"; runId: string; at: string; level: "info" | "warn" | "error"; message: string; nodeId?: string }
  | { type: "node_output"; runId: string; at: string; nodeId: string; artifactId: string; mimeType: string; preview?: string };

export type UnknownRunEvent = { type: string;[key: string]: unknown };

export type RunEvent = KnownRunEvent | UnknownRunEvent;
