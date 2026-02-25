// src/lib/flow/types/run.ts
export type RunStatus = "idle" | "running" | "succeeded" | "failed" | "canceled";

export type RunRequest = {
  runFrom: string | null; // null = from start
  runMode?: "from_start" | "from_selected_onward" | "selected_only";
};

export type KnownRunEvent =
  | {
      type: "run_started";
      runId: string;
      at: string;
      runFrom: string | null;
      runMode?: "from_start" | "from_selected_onward" | "selected_only";
      plannedNodeIds?: string[];
    }
  | { type: "run_finished"; runId: string; at: string; status: RunStatus }
  | { type: "node_started"; runId: string; at: string; nodeId: string }
  | { type: "node_finished"; runId: string; at: string; nodeId: string; status: RunStatus; error?: string }
  | { type: "edge_exec"; runId: string; at: string; edgeId: string; exec: "idle" | "active" | "done" }
  | { type: "log"; runId: string; at: string; level: "info" | "warn" | "error"; message: string; nodeId?: string }
  | { type: "node_output"; runId: string; at: string; nodeId: string; artifactId: string; mimeType?: string; portType?: string; preview?: string; cached?: boolean }
  | { type: "cache_decision"; schema_version?: number; runId: string; at: string; nodeId: string; nodeKind: string; decision: "cache_hit" | "cache_miss" | "cache_hit_contract_mismatch"; execKey: string; artifactId?: string; expectedPortType?: string; actualPortType?: string; producerExecKey?: string };

export type UnknownRunEvent = { type: string;[key: string]: unknown };

export type RunEvent = KnownRunEvent | UnknownRunEvent;
