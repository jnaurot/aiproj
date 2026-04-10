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
  | "busy"
  | "succeeded"
  | "failed"
  | "skipped"
  | "canceled";


export type EdgeExecState =
  | "idle"
  | "active"
  | "done";
export type EdgeMode = "work" | "param" | "control";
export type EdgeLinkKind = "data_link" | "control_link";
export type PortPlane = "work" | "param" | "control";
export type PortDirection = "in" | "out";
export type PortCardinality = "one" | "many";

export type NodePortDeclaration = {
	plane?: PortPlane;
	affinity?: PortPlane;
	required?: boolean;
	cardinality?: PortCardinality;
	behavior?: "once" | "single_item" | "batch";
};

export type NodeMeta = {
  createdAt?: string;     // ISO
  updatedAt?: string;     // ISO
  description?: string;
  tags?: string[];
  /**
   * Controls backend memoization eligibility for this node.
   * When false, memoization cache is bypassed for the node.
   */
  memoizable?: boolean;
  checkpointSummary?: {
    total: number;
    valid: number;
    stale: number;
  };
  nodeDoc?: {
    summary?: string;
    notes?: string[];
    disabled?: boolean;
    generated?: {
      summary: string;
      settings_explained: string[];
      context_notes: string[];
      generated_at: string;
      signature_key: string;
      provider_meta?: {
        provider?: string;
        model?: string;
      };
    };
  };
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
    on_error?: "fail_fast" | "skip_failed";
    input_handles?: Record<
      string,
      {
        consume_mode?: "once" | "single_item" | "batch";
        batch_size?: number;
        max_inflight?: number;
      }
    >;
  };
  portDeclarations?: {
    in?: Record<string, NodePortDeclaration>;
    out?: Record<string, NodePortDeclaration>;
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
  linkKind?: EdgeLinkKind;
  mode?: EdgeMode;
  fatal?: boolean;
  queue?: {
    max?: number;
    overflow?: "block" | "spill" | "error";
    policy?: "fifo" | "round_robin";
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
	snapshot?: {
		sourceSchemaFingerprint?: string;
		targetSchemaFingerprint?: string;
		compatible?: boolean;
		decision?: "native" | "coerced" | "adapter" | "incompatible";
		coercion?: {
			allowed?: boolean;
			lossy?: boolean;
			mode?: "native" | "widened" | "coerced";
		};
		updatedAt?: string;
	};
  };
}

/** ✅ Actual edge object type */
export type PipelineEdge = Edge<PipelineEdgeData>;
