import { writable, derived } from "svelte/store";
import type { Node, Edge } from "@xyflow/svelte";

import type {
  PipelineNodeData,
  PipelineEdgeData,
  NodeStatus,
  RunEvent,
  KnownRunEvent
} from "./types";
import { updateNodeParamsValidated } from "$lib/flow/store/graph";

type NodeRuntimePatch = {
  status?: NodeStatus;
  lastRunId?: string;
  lastStartedAt?: string;
  lastEndedAt?: string;
  // error?: PipelineNodeData["error"];

  // if your PipelineNodeData includes this (your BaseNode uses it)
  active?: boolean;
};

export const nodesStore = writable<Node<PipelineNodeData>[]>([]);
export const edgesStore = writable<Edge<PipelineEdgeData>[]>([]);

export const selectedNodeId = writable<string | null>(null);
export const selectedNode = derived(
  [nodesStore, selectedNodeId],
  ([$nodes, $id]) => $nodes.find((n) => n.id === $id) ?? null
);

export const currentRunId = writable<string | null>(null);
export const runStatus = writable<
  "idle" | "running" | "succeeded" | "failed" | "canceled"
>("idle");
export const logs = writable<
  { ts: string; level: string; message: string; nodeId?: string }[]
>([]);

export function upsertNodeParams(nodeId: string, patch: Record<string, unknown>) {
  nodesStore.update((nodes) => {
    const res = updateNodeParamsValidated(nodes, nodeId, patch);
    return res.nodes;
  });
}

export function setNodeRuntime(nodeId: string, patch: NodeRuntimePatch) {
  nodesStore.update((nodes) =>
    nodes.map((n) =>
      n.id === nodeId
        ? {
          ...n,
          data: {
            ...n.data,
            ...patch,
            // keep the discriminated union happy: status stays NodeStatus
            ...(patch.status ? { status: patch.status } : {})
          }
        }
        : n
    )
  );
}

/**
 * Phase2 edge state: store DOMAIN state only (exec + contract) in edge.data.
 * UI styling (class/animated) should be derived in FlowCanvas (view-only).
 */
export function setEdgeExec(edgeId: string, exec: "idle" | "active" | "done") {
  edgesStore.update((edges) =>
    edges.map((e) =>
      e.id === edgeId
        ? { ...e, data: { ...e.data, exec } }
        : e
    )
  );
}

export function resetRuntimeState() {
  nodesStore.update((nodes) =>
    nodes.map((n) => ({
      ...n,
      data: { ...n.data, active: false, status: "idle" as NodeStatus }
    }))
  );
  edgesStore.update((edges) =>
    edges.map((e) => ({
      ...e,
      data: { ...(e.data ?? {}), exec: "idle" as const }
    }))
  );
  logs.set([]);
  runStatus.set("idle");
  currentRunId.set(null);
}

export function applyRunEvent(ev: KnownRunEvent) {
  switch (ev.type) {
    case "run_started":
      runStatus.set("running");
      // if your event includes runId, keep it
      if ("runId" in ev) currentRunId.set(ev.runId);
      return;

    case "run_finished":
      runStatus.set(ev.status);
      return;

    case "node_started":
      setNodeRuntime(ev.nodeId, {
        active: true,
        status: "running",
        ...(("runId" in ev && ev.runId) ? { lastRunId: ev.runId } : {})
      });
      return;

    case "node_finished":
      setNodeRuntime(ev.nodeId, {
        active: false,
        status: ev.status
      });
      return;

    // ✅ Phase2 edge event (matches your types/run.ts)
    case "edge_exec":
      setEdgeExec(ev.edgeId, ev.exec);
      return;

    case "log":
      logs.update((l) => [
        ...l,
        { ts: ev.at, level: ev.level, message: ev.message, nodeId: ev.nodeId }
      ]);
      return;

    default:
      return;
  }
}
