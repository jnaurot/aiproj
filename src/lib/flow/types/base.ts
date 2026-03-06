import type { Node, Edge } from "@xyflow/svelte";

export const PORT_TYPES = ["table", "text", "json", "binary", "embeddings"];
export type PortType = typeof PORT_TYPES[number];
export function isPortType(value: unknown): value is PortType {
  return typeof value === "string" && PORT_TYPES.includes(value as any);
}
export type UpdateNodeConfig = {
  params?: unknown;
  ports?: {
    in?: PortType | null;
    out?: PortType | null;
  };
};

export type NodeKind = "source" | "transform" | "llm" | "tool";

export type NodeStatus =
  | "idle"
  | "stale"
  | "running"
  | "succeeded"
  | "failed"
  | "skipped"
  | "canceled";


export type EdgeExecState =
  | "idle"
  | "active"
  | "done";

export type NodeMeta = {
  createdAt?: string;     // ISO
  updatedAt?: string;     // ISO
  description?: string;
  tags?: string[];
  presetRef?: {
    id: string;
    name: string;
    subtype?: string;
    appliedAt: string;
    appliedParams: Record<string, unknown>;
    appliedPorts?: { in?: PortType | null; out?: PortType | null };
  };
};

export type BaseNodeData<K extends NodeKind, P> = {
  kind: K;
  label: string;
  params: P;
  status: NodeStatus;

  // execution bookkeeping (optional for now)
  lastRunId?: string;
  lastStartedAt?: string; // ISO
  lastEndedAt?: string;   // ISO
  error?: { message: string; code?: string; details?: unknown };

  // typing/contracts (optional but useful)
  ports?: { in?: PortType | null; out?: PortType | null };

  meta?: NodeMeta;
};

/** ✅ This is what lives inside edge.data */
export interface PipelineEdgeData extends Record<string, any> {
  exec: EdgeExecState; // make required to simplify runtime state
  contract?: {
    in?: PortType;
    out?: PortType;
    payload?: {
      source?: Record<string, any>;
      target?: Record<string, any>;
    };
  };
}

/** ✅ Actual edge object type */
export type PipelineEdge = Edge<PipelineEdgeData>;
