import type { Node, Edge } from "@xyflow/svelte";
import type { NodeSchemaEnvelope } from "$lib/flow/schema/schemaContract";

export const PAYLOAD_TYPES = ["table", "text", "json", "binary", "embeddings", "image", "audio", "video"];
export type PayloadType = typeof PAYLOAD_TYPES[number];
export function isPayloadType(value: unknown): value is PayloadType {
  return typeof value === "string" && PAYLOAD_TYPES.includes(value as any);
}
export type UpdateNodeConfig = {
  params?: unknown;
};

export type NodeKind = "source" | "transform" | "model" | "llm" | "tool" | "component";

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
export type EdgeMode = "work" | "param" | "control";

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
  };
};

export type BaseNodeData<K extends NodeKind, P> = {
  kind: K;
  label: string;
  params: P;
  status: NodeStatus;
  processingPolicy?: {
    consume_mode?: "once" | "single_item" | "batch";
    batch_size?: number;
    max_inflight?: number;
    input_handles?: Record<
      string,
      {
        consume_mode?: "once" | "single_item" | "batch";
        batch_size?: number;
        max_inflight?: number;
      }
    >;
  };

  // execution bookkeeping (optional for now)
  lastRunId?: string;
  lastStartedAt?: string; // ISO
  lastEndedAt?: string;   // ISO
  error?: { message: string; code?: string; details?: unknown };

  schema?: NodeSchemaEnvelope;

  meta?: NodeMeta;
};

/** ✅ This is what lives inside edge.data */
export interface PipelineEdgeData extends Record<string, any> {
  exec: EdgeExecState; // make required to simplify runtime state
  mode?: EdgeMode;
  fatal?: boolean;
  queue?: {
    max?: number;
    overflow?: "block" | "spill" | "error";
  };
  work?: {
    item_mode?: "artifact" | "json_items" | "table_rows";
    max_items?: number;
  };
  contract?: {
    in?: PayloadType;
    out?: PayloadType;
    payload?: {
      source?: Record<string, any>;
      target?: Record<string, any>;
    };
  };
}

/** ✅ Actual edge object type */
export type PipelineEdge = Edge<PipelineEdgeData>;
