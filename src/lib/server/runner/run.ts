import type { PipelineGraphDTO, KnownRunEvent } from "$lib/flow/types";
import { getBus } from "./events";
// import { compile } from "./compile"; // your DAG builder

type StripMeta<T> = T extends any ? Omit<T, "runId" | "at"> : never;
type EmitEvent = StripMeta<KnownRunEvent>;

function at() {
  return new Date().toISOString();
}

export async function runGraph(args: {
  runId: string;
  graph: PipelineGraphDTO;
  runFrom: string | null;
  options?: { dryRun?: boolean };
}) {
  const { runId, graph, runFrom } = args;
  const bus = getBus(runId);

const emit = (ev: EmitEvent) => {
  bus.emit({ ...ev, runId, at: at() } as KnownRunEvent);
};

  emit({ type: "run_started", runFrom });

  // compile plan
  const plan = compile(graph, runFrom);
  // plan.order: nodeIds in topo order
  // plan.incomingByNode: nodeId -> edgeId[]

  // reset all edges to idle
  for (const e of graph.edges) {
    emit({ type: "edge_exec", edgeId: e.id, exec: "idle" });
  }

  for (const nodeId of plan.order) {
    const incoming = plan.incomingByNode[nodeId] ?? [];

    // edges active
    for (const edgeId of incoming) {
      emit({ type: "edge_exec", edgeId, exec: "active" });
    }

    emit({ type: "node_started", nodeId });
    emit({ type: "log", level: "info", message: "Executing node", nodeId });

    // TODO: replace with real executor call
    await new Promise((r) => setTimeout(r, 250));

    emit({ type: "node_finished", nodeId, status: "succeeded" });

    // edges done
    for (const edgeId of incoming) {
      emit({ type: "edge_exec", edgeId, exec: "done" });
    }
  }

  emit({ type: "run_finished", status: "succeeded" });
}
