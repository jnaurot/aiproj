// src/lib/flow/types/run.ts
export type RunStatus =
  | "idle"
  | "running"
  | "pausing"
  | "paused"
  | "resuming"
  | "succeeded"
  | "failed"
  | "canceled";

export type RunRequest = {
  runFrom: string | null; // null = from start
  runMode?: "from_start" | "from_selected_onward" | "selected_only";
};

export type NodeFinishedStatus =
	| "succeeded"
	| "failed"
	| "skipped"
	| "canceled"
	| "stale";

export type KnownRunEvent =
  | {
      type: "run_started";
      runId: string;
      at: string;
      runFrom: string | null;
      runMode?: "from_start" | "from_selected_onward" | "selected_only";
      plannedNodeIds?: string[];
      executionContract?: Record<string, unknown>;
    }
  | { type: "run_pause_requested"; runId: string; at: string }
  | { type: "run_pausing"; runId: string; at: string }
  | { type: "run_paused"; runId: string; at: string; snapshot?: Record<string, unknown> }
  | { type: "run_resume_requested"; runId: string; at: string }
  | { type: "run_resuming"; runId: string; at: string }
  | {
      type: "run_resumed";
      runId: string;
      at: string;
      runFrom?: string | null;
      runMode?: "from_start" | "from_selected_onward" | "selected_only";
      plannedNodeIds?: string[];
      executionContract?: Record<string, unknown>;
    }
  | {
      type: "run_resume_failed";
      runId: string;
      at: string;
      errorCode?: string;
      error?: string;
      details?: Record<string, unknown>;
    }
  | { type: "run_finished"; runId: string; at: string; status: RunStatus }
  | { type: "node_started"; runId: string; at: string; nodeId: string }
  | {
      type: "component_started";
      runId: string;
      at: string;
      nodeId: string;
      componentId?: string;
      componentRevisionId?: string;
    }
  | {
      type: "component_finished";
      runId: string;
      at: string;
      nodeId: string;
      componentId?: string;
      componentRevisionId?: string;
      status?: "succeeded" | "failed";
    }
  | {
      type: "component_failed";
      runId: string;
      at: string;
      nodeId: string;
      componentId?: string;
      componentRevisionId?: string;
      error?: string;
    }
  | {
      type: "node_finished";
      runId: string;
      at: string;
      nodeId: string;
      status: NodeFinishedStatus;
      error?: string;
      errorCode?: string;
      errorDetails?: {
        op?: string;
        paramPath?: string;
        missingColumns?: string[];
        availableColumns?: string[];
        availableColumnsSource?: "schema" | "inferred" | string;
        message?: string;
        [key: string]: unknown;
      };
    }
  | { type: "edge_exec"; runId: string; at: string; edgeId: string; exec: "idle" | "active" | "done" }
  | {
      type: "log";
      runId: string;
      at: string;
      level: "info" | "warn" | "error";
      message: string;
      nodeId?: string;
      componentPath?: string[];
    }
  | {
      type: "node_output";
      runId: string;
      at: string;
      nodeId: string;
      artifactId: string;
      mimeType?: string;
      payloadType?: string;
      preview?: string;
      cached?: boolean;
      sourceObservability?: Record<string, unknown>;
      primingArtifact?: Record<string, unknown>;
    }
  | { type: "cache_decision"; schema_version?: number; runId: string; at: string; nodeId: string; nodeKind: string; decision: "cache_hit" | "cache_miss" | "cache_hit_contract_mismatch"; execKey: string; artifactId?: string; expectedType?: string; actualType?: string; producerExecKey?: string }
  | { type: "cache_summary"; schema_version?: number; runId: string; at: string; cache_hit: number; cache_miss: number; cache_hit_contract_mismatch: number }
  | {
      type: "run_telemetry";
      schema_version?: number;
      runId: string;
      at: string;
      runtime_ms: number;
      peak_concurrency: number;
      executed: number;
      cached: number;
      failed: number;
      planned?: number;
      cache_hit: number;
      cache_miss: number;
      cache_hit_contract_mismatch: number;
      schema_infer?: { hit: number; miss: number; bypass: number };
    }
  | {
      type: "control_signal";
      runId: string;
      at: string;
      signal:
        | "ready"
        | "busy"
        | "drain"
        | "pause"
        | "blocked"
        | "resume"
        | "llm_acquired"
        | "llm_released";
      nodeId?: string;
      handle?: string;
    }
  | {
      type: "branch_cascade";
      runId: string;
      at: string;
      originNodeId: string;
      blockedNodeIds: string[];
      reasonCode?: string;
    }
  | {
      type: "queue_metrics";
      runId: string;
      at: string;
      metrics?: Record<string, unknown>;
      nodeMetrics?: Record<string, unknown>;
      runtimeItemMetrics?: Record<string, unknown>;
    }
  | {
      type: "node_decision";
      runId: string;
      at: string;
      nodeId: string;
      decision: "accept" | "reject";
      count?: number;
      reasonCode?: string;
    }
  | {
      type: "node_handle_satisfaction";
      runId: string;
      at: string;
      nodeId: string;
      handle: string;
      status: "all" | "partial" | "none";
      connectedEdges: number;
      providedEdges: number;
    }
  | {
      type: "node_input_warning";
      runId: string;
      at: string;
      nodeId: string;
      handle: string;
      edgeId: string;
      plane: "param" | "control";
      code: "PARAM_CONTROL_EMPTY_INPUT";
      reasonCode?: string;
      upstreamNodeId?: string;
      warningKey?: string;
    }
  | {
      type: "node_warning_summary";
      runId: string;
      at: string;
      warningKey: string;
      nodeId: string;
      handle: string;
      code: "PARAM_CONTROL_EMPTY_INPUT" | string;
      plane?: "param" | "control" | string;
      edgeId?: string;
      reasonCode?: string;
      upstreamNodeId?: string;
      count: number;
      firstAt?: string;
    }
  | {
      type: "node_blocked";
      schema_version?: number;
      runId: string;
      at: string;
      nodeId: string;
      reasonCode:
        | "WAITING_REQUIRED_INPUT"
        | "CONTROL_GATE_BLOCKED"
        | "WAITING_REQUIRED_PARAM"
        | "WAITING_REQUIRED_CONTROL"
        | "MAX_INFLIGHT_REACHED"
        | "UPSTREAM_NOT_READY"
        | "NO_READY_WORK"
        | "LLM_LEASE_UNAVAILABLE";
      handle?: string;
      plane?: "work" | "param" | "control";
      missingEdgeIds?: string[];
      waitingOnNodeIds?: string[];
      details?: Record<string, unknown>;
    }
  | {
      type: "control_gate_state";
      schema_version?: number;
      runId: string;
      at: string;
      nodeId: string;
      state: "blocked" | "open";
      handle?: string;
      reasonCode?: string;
      missingEdgeIds?: string[];
    }
  | {
      type: "scheduler_snapshot";
      schema_version?: number;
      runId: string;
      at: string;
      readyCount: number;
      inflightCount: number;
      pendingQueueDepth: number;
      runnableNodeCount: number;
      stalled: boolean;
      perNode?: Array<{
        nodeId: string;
        readyWork: boolean;
        inflight: number;
        pendingInputCount: number;
        lastBlockedReasonCode?: string;
      }>;
    }
  | {
      type: "llm_lease";
      schema_version?: number;
      runId: string;
      at: string;
      state: "waiting" | "acquired" | "released";
      nodeId?: string;
      holderNodeId?: string | null;
      waitQueueLength?: number;
      waitingNodeIds?: string[];
    }
  | {
      type: "node_not_resumable";
      runId: string;
      at: string;
      nodeId: string;
      reasonCode?: string;
    }
  | {
      type: "scheduler_adaptive_decision";
      schema_version?: number;
      runId: string;
      at: string;
      mode: "off" | "observe" | "enforce" | string;
      enforced?: boolean;
      inputs?: Record<string, unknown>;
      reasons?: string[];
      hardCaps?: Record<string, number>;
      minCaps?: Record<string, number>;
      proposedCaps?: Record<string, number>;
      effectiveCaps?: Record<string, number>;
      changedCaps?: Record<string, { from: number; to: number }>;
    };

export type UnknownRunEvent = { type: string;[key: string]: unknown };

export type RunEvent = KnownRunEvent | UnknownRunEvent;

export type InputContractAffinity = "workInputs" | "paramInputs" | "controlInputs";

export type InputContractShape = {
	defaultSchema?: Record<string, unknown>;
	handles?: Record<string, Record<string, unknown>>;
};

export type NodeInputContracts = {
	workInputs?: InputContractShape;
	paramInputs?: InputContractShape;
	controlInputs?: InputContractShape;
};
