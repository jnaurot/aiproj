// src/lib/flow/store/graphStore.ts
import { writable, get, derived } from "svelte/store";
import type { Node, Edge } from "@xyflow/svelte";

import type { NodeStatus, NodeKind, PipelineNodeData, PipelineEdgeData, PipelineGraphDTO, PortType } from "$lib/flow/types";
import { isPortType } from "$lib/flow/types/base";
import { defaultSourceParamsByKind } from '$lib/flow/schema/sourceDefaults';
import { defaultLlmParamsByKind } from "$lib/flow/schema/llmDefaults";
import { defaultTransformParamsByKind } from "$lib/flow/schema/transformDefaults";
// import { defaultToolParamsByKind } from "$lib/flow/schema/toolDefaults";
import { defaultNodeData } from "$lib/flow/schema/defaults";
import { updateNodeParamsValidated } from "./graph";
import { saveGraphToLocalStorage, loadGraphFromLocalStorage, emptyGraph } from "./persist";
import { createRun, streamRunEvents } from "$lib/flow/client/runs";
import type { KnownRunEvent } from "$lib/flow/types/run";
import type { SourceKind, LlmKind, TransformKind } from "$lib/flow/types/paramsMap";

type NodeOutputInfo = { artifactId: string; mimeType: string; preview?: string }
type EdgeExec = "idle" | "active" | "done";
type LogLevel = "info" | "warn" | "error";
type RunLog = {
    id: number;          // ✅ ADD
    ts: string;
    level: LogLevel;
    message: string;
    nodeId?: string;
};
type RunStatus = "idle" | "running" | "succeeded" | "failed" | "canceled";
type InspectorState = {
    nodeId: string | null;
    draftParams: Record<string, any>;
    dirty: boolean;
}

const STALE: NodeStatus = "stale";
const IDLE: NodeStatus = "idle";
const RUNNING: NodeStatus = "running";
const SUCCEEDED: NodeStatus = "succeeded";
const allowedPorts = new Set(["table", "text", "json", "binary", "chat", "embeddings"]);
const initialInspector: InspectorState = {
    nodeId: null,
    draftParams: {},
    dirty: false
};

let logSeq = 0;

export type GraphState = {
    nodes: Node<PipelineNodeData & Record<string, unknown>>[];
    edges: Edge<PipelineEdgeData & Record<string, unknown>>[];
    selectedNodeId: string | null;
    inspector: InspectorState;           // ✅ add this
    logs: RunLog[];
    runStatus: RunStatus;
    nodeOutputs: Record<string, NodeOutputInfo>;
};

function nowTs() {
    return new Date().toLocaleTimeString();
}

function logPush(state: GraphState, level: LogLevel, message: string, nodeId?: string) {
    logSeq += 1;
    return {
        ...state,
        logs: [
            ...state.logs,
            { id: logSeq, ts: nowTs(), level, message, nodeId }
        ]
    };
}


function stripToDTO(nodes: Node<PipelineNodeData>[], edges: Edge<PipelineEdgeData>[]): PipelineGraphDTO {
    return { version: 1, nodes, edges };
}

function getPortType(
    nodes: Node<PipelineNodeData>[],
    sourceId: string,
    whichPort: "in" | "out"
): PortType | null {
    const n = nodes.find((x) => x.id === sourceId);
    if (!n) return null;
    return (n.data.ports?.[whichPort as "in" | "out"] ?? null) as PortType | null;
}

type EdgeInvalidReason =
    "missing_port_type"   // couldn't resolve out/in
    | "type_mismatch";      // outType !== in
type EdgeCheck =
    { ok: true; out?: PortType; in?: PortType }
    | { ok: false; reason: EdgeInvalidReason };



function isEdgeStillValid(
    nodes: Node<PipelineNodeData>[],
    e: Edge<PipelineEdgeData>
): EdgeCheck {
    const outPort = getPortType(nodes, e.source, "out");
    const inPort = getPortType(nodes, e.target, "in");

    if (outPort == null || inPort == null) {
        return { ok: false, reason: "missing_port_type" };
    }

    if (outPort !== inPort) {
        return { ok: false, reason: "type_mismatch" };
    }

    return { ok: true, out: outPort, in: inPort };
}

function resetEdgesExec(edges: Edge<PipelineEdgeData>[]): Edge<PipelineEdgeData>[] {
    return edges.map((e) => ({ ...e, data: { ...e.data, exec: "idle" as EdgeExec } }));
}

function setEdgeExec(edges: Edge<PipelineEdgeData>[], edgeId: string, exec: "idle" | "active" | "done") {
    return edges.map(e => e.id === edgeId ? { ...e, data: { ...e.data, exec: exec } } : e);
}

function downstreamIds(startId: string, edges: Edge<PipelineEdgeData>[]) {
    const adj = new Map<string, string[]>();
    for (const e of edges) adj.set(e.source, [...(adj.get(e.source) ?? []), e.target]);

    const seen = new Set<string>();
    const q = [startId];
    while (q.length) {
        const cur = q.shift()!;
        for (const nxt of adj.get(cur) ?? []) {
            if (!seen.has(nxt)) {
                seen.add(nxt);
                q.push(nxt);
            }
        }
    }
    return seen;
}

function pruneAndRecontractEdgesStrict(
    nodes: Node<PipelineNodeData>[],
    edges: Edge<PipelineEdgeData>[]
):
    | { ok: true; edges: Edge<PipelineEdgeData>[]; prunedIds: string[] }
    | { ok: false; error: string } {

    const next: Edge<PipelineEdgeData>[] = [];
    const prunedIds: string[] = [];

    for (const e of edges) {
        const chk = isEdgeStillValid(nodes, e);

        if (chk.ok === false) {
            if (chk.reason === "type_mismatch") {
                // allowed prune
                prunedIds.push(e.id);
                continue;
            }

            // NOT allowed to silently prune: graph invariants broken
            return {
                ok: false,
                error: `Edge ${e.id} has unresolved port types (source=${e.source}:${e.sourceHandle ?? "out"} target=${e.target}:${e.targetHandle ?? "in"})`
            };
        }

        next.push({
            ...e,
            data: {
                ...(e.data ?? {}),
                exec: (e.data?.exec ?? "idle"),
                contract: { out: chk.out, in: chk.in }
            }
        });
    }

    return { ok: true, edges: next, prunedIds };
}


function topoFrom(nodes: Node<PipelineNodeData>[], edges: Edge<PipelineEdgeData>[], startId: string | null) {
    const inDeg = new Map<string, number>();
    const adj = new Map<string, string[]>();

    for (const n of nodes) {
        inDeg.set(n.id, 0);
        adj.set(n.id, []);
    }

    for (const e of edges) {
        adj.get(e.source)!.push(e.target);
        inDeg.set(e.target, (inDeg.get(e.target) ?? 0) + 1);
    }

    const startSet = new Set<string>();
    if (startId) {
        startSet.add(startId);
        for (const d of downstreamIds(startId, edges)) startSet.add(d);
    } else {
        for (const [id, deg] of inDeg.entries()) if (deg === 0) startSet.add(id);
        const roots = [...startSet];
        for (const r of roots) for (const d of downstreamIds(r, edges)) startSet.add(d);
    }

    const inDeg2 = new Map<string, number>();
    for (const id of startSet) inDeg2.set(id, 0);
    for (const e of edges) {
        if (startSet.has(e.source) && startSet.has(e.target)) {
            inDeg2.set(e.target, (inDeg2.get(e.target) ?? 0) + 1);
        }
    }

    const q: string[] = [];
    for (const [id, deg] of inDeg2.entries()) if (deg === 0) q.push(id);

    const order: string[] = [];
    while (q.length) {
        const cur = q.shift()!;
        order.push(cur);
        for (const nxt of adj.get(cur) ?? []) {
            if (!startSet.has(nxt)) continue;
            const nd = (inDeg2.get(nxt) ?? 0) - 1;
            inDeg2.set(nxt, nd);
            if (nd === 0) q.push(nxt);
        }
    }

    if (order.length !== startSet.size) return [...startSet].sort();
    return order;
}

const loaded = loadGraphFromLocalStorage(emptyGraph);

const initialState: GraphState = {
    nodes: loaded.nodes,
    edges: loaded.edges,
    selectedNodeId: null,
    inspector: initialInspector,
    logs: [],
    runStatus: IDLE,
    nodeOutputs: {}
};

const statusOrIdle = (s: NodeStatus): NodeStatus => s;

export const graphStore = (() => {
    const { subscribe, set, update } = writable<GraphState>(initialState);

    function updateNodeConfigImpl(
        nodeId: string,
        config: { params?: unknown; ports?: { in?: PortType | null; out?: PortType | null } }
    ) {
        let out: { ok: boolean; error?: string; removedEdgeIds?: string[] } = { ok: true };

        console.log("updateNodeConfig called with:", nodeId, config);

        update((s) => {
            let nodes = s.nodes;
            let edges = s.edges;

            // 0) Ensure node exists
            const node = nodes.find((n) => n.id === nodeId);
            if (!node) {
                out = { ok: false, error: "Node not found" };
                return logPush(s, "warn", out.error!, nodeId);
            }

            // ---- 1) params (must be valid to commit) ----
            if (config.params !== undefined) {
                const res = updateNodeParamsValidated(nodes, nodeId, config.params);
                if (res.error) {
                    out = { ok: false, error: res.error };
                    return logPush(s, "error", res.error, nodeId);
                }
                nodes = res.nodes;
            }

            // ---- 2) ports (must be valid to commit) ----
            if (config.ports) {
                const { in: inPort, out: outPort } = config.ports;

                if (inPort !== undefined && inPort !== null && !isPortType(inPort)) {
                    out = { ok: false, error: `Invalid input port type: ${String(inPort)}` };
                    return logPush(s, "warn", out.error!, nodeId);
                }
                if (outPort !== undefined && outPort !== null && !isPortType(outPort)) {
                    out = { ok: false, error: `Invalid output port type: ${String(outPort)}` };
                    return logPush(s, "warn", out.error!, nodeId);
                }

                // capture connectivity BEFORE changes
                const incoming = edges.filter((e) => e.target === nodeId);
                const outgoing = edges.filter((e) => e.source === nodeId);

                nodes = nodes.map((n) => {
                    if (n.id !== nodeId) return n;

                    const curPorts = n.data.ports ?? {};
                    const mergedPorts = {
                        in: inPort !== undefined ? inPort : (curPorts.in ?? null),
                        out: outPort !== undefined ? outPort : (curPorts.out ?? null)
                    };

                    return {
                        ...n,
                        data: {
                            ...n.data,
                            ports: mergedPorts,
                            meta: { ...(n.data.meta ?? {}), updatedAt: new Date().toISOString() }
                        }
                    };
                });

                // NOW read what we actually set
                const updatedNode = nodes.find((n) => n.id === nodeId)!;
                const pin = updatedNode.data.ports?.in ?? null;
                const pout = updatedNode.data.ports?.out ?? null;

                // Invariant: cannot null a port that is currently used by edges
                if (incoming.length > 0 && pin == null) {
                    out = { ok: false, error: "Cannot set input port to null while node has incoming edges." };
                    return logPush(s, "warn", out.error!, nodeId);
                }
                if (outgoing.length > 0 && pout == null) {
                    out = { ok: false, error: "Cannot set output port to null while node has outgoing edges." };
                    return logPush(s, "warn", out.error!, nodeId);
                }

                const pr = pruneAndRecontractEdgesStrict(nodes, edges);
                if (pr.ok === false) {
                    out = { ok: false, error: pr.error };
                    return logPush(s, "warn", pr.error, nodeId);
                }
                edges = pr.edges;
                if (pr.prunedIds?.length) out.removedEdgeIds = pr.prunedIds;
            }

            const next = logPush({ ...s, nodes, edges }, "info", "Node config updated", nodeId);
            persist(next);
            return next;
        });

        return out;
    }

    type UpdateNodeConfig = {
        params?: unknown;
        ports?: {
            in?: PortType | null;   // apply to all input handles
            out?: PortType | null;  // apply to all output handles
        };
    };

    type PreviewUpdateResult =
        | { ok: true; prunedEdgeIds: string[]; nextNodes: Node<PipelineNodeData>[]; nextEdges: Edge<PipelineEdgeData>[] }
        | { ok: false; error: string };


    //BEGIN
    function patchInspectorDraft(patch: Record<string, any>) {
        console.log("patchInspectorDraft", patch);
        update((s) => {
            if (!s.inspector.nodeId) return s;
            return {
                ...s,
                inspector: {
                    ...s.inspector,
                    draftParams: { ...s.inspector.draftParams, ...patch },
                    dirty: true
                }
            };
        });
    }

    // optional: dropdown commit (keeps draft consistent + commits)
    function commitInspectorImmediate(patch: Record<string, any>) {
        const s = get({ subscribe } as any) as GraphState;
        const nodeId = s.inspector.nodeId;
        if (!nodeId) return { ok: false, error: "No node selected" };

        // 1) update draft (so Apply is non-detrimental)
        patchInspectorDraft(patch);

        // 2) commit patch (validated/stripped)
        return updateNodeConfigImpl(nodeId, { params: patch });
    }

    function applyInspectorDraft() {
        console.log("inside applyInspectorDraft");
        const s = get({ subscribe } as any) as GraphState;
        const nodeId = s.inspector.nodeId;
        if (!nodeId) return { ok: false, error: "No node selected" };

        const r = updateNodeConfigImpl(nodeId, { params: s.inspector.draftParams });

        // only clear dirty if commit succeeded (fail-closed keeps draft)
        if (r.ok) {
            update((st) => {
                const n = st.nodes.find((x) => x.id === nodeId);
                return {
                    ...st,
                    inspector: {
                        nodeId,
                        draftParams: structuredClone((n?.data.params ?? {}) as any),
                        dirty: false
                    }
                };
            });
        }
        return r;
    }

    function revertInspectorDraft() {
        console.log("inside revertInspectorDraft");
        update((s) => {
            const nodeId = s.inspector.nodeId;
            if (!nodeId) return s;
            const n = s.nodes.find((x) => x.id === nodeId);
            return {
                ...s,
                inspector: {
                    nodeId,
                    draftParams: structuredClone((n?.data.params ?? {}) as any),
                    dirty: false
                }
            };
        });
    }

    //END

    function persist(state: GraphState) {
        saveGraphToLocalStorage(stripToDTO(state.nodes, state.edges));
    }

    return {
        subscribe,
        patchInspectorDraft,
        commitInspectorImmediate,
        applyInspectorDraft,
        revertInspectorDraft,
        updateNodeConfig: updateNodeConfigImpl,

        setSourceKind(nodeId: string, nextKind: SourceKind) {
            const nextParams = structuredClone(defaultSourceParamsByKind[nextKind]);

            // 1) update structural subtype on the node
            update((s) => {
                const node = s.nodes.find(n => n.id === nodeId);
                if (!node) return logPush(s, "warn", "Node not found", nodeId);

                const nodes = s.nodes.map(n =>
                    n.id === nodeId
                        ? {
                            ...n,
                            data: {
                                ...n.data,
                                sourceKind: nextKind, // ✅ structural
                                meta: { ...(n.data.meta ?? {}), updatedAt: new Date().toISOString() }
                            }
                        }
                        : n
                );

                const next = { ...s, nodes };
                persist(next);
                return next;
            });

            // 2) replace params via your validated path (schema stripping happens here)
            const r = updateNodeConfigImpl(nodeId, { params: nextParams });

            // 3) ensure inspector draft matches immediately after type switch
            if (r.ok) {
                update((s) => {
                    const n = s.nodes.find(x => x.id === nodeId);
                    return {
                        ...s,
                        inspector: {
                            nodeId,
                            draftParams: structuredClone((n?.data.params ?? {}) as any),
                            dirty: false
                        }
                    };
                });
            }
            return r;
        },

        // graphStore.ts (inside your graphStore object)
        setLlmKind(nodeId: string, nextKind: LlmKind) {
            const nextParams = structuredClone(defaultLlmParamsByKind[nextKind]);

            // 1) update structural subtype on the node
            update((s) => {
                const node = s.nodes.find(n => n.id === nodeId);
                if (!node) return logPush(s, "warn", "Node not found", nodeId);

                const nodes = s.nodes.map(n =>
                    n.id === nodeId
                        ? {
                            ...n,
                            data: {
                                ...n.data,
                                llmKind: nextKind, // ✅ structural
                                meta: { ...(n.data.meta ?? {}), updatedAt: new Date().toISOString() }
                            }
                        }
                        : n
                );

                const next = { ...s, nodes };
                persist(next);
                return next;
            });

            // 2) replace params via your validated path (schema stripping happens here)
            const r = updateNodeConfigImpl(nodeId, { params: nextParams });

            // 3) ensure inspector draft matches immediately after type switch
            if (r.ok) {
                update((s) => {
                    const n = s.nodes.find(x => x.id === nodeId);
                    return {
                        ...s,
                        inspector: {
                            nodeId,
                            draftParams: structuredClone((n?.data.params ?? {}) as any),
                            dirty: false
                        }
                    };
                });
            }

            return r;
        },

                // graphStore.ts (inside your graphStore object)
        setTransformKind(nodeId: string, nextKind: TransformKind) {
            const nextParams = structuredClone(defaultTransformParamsByKind[nextKind]);

            // 1) update structural subtype on the node
            update((s) => {
                const node = s.nodes.find(n => n.id === nodeId);
                if (!node) return logPush(s, "warn", "Node not found", nodeId);

                const nodes = s.nodes.map(n =>
                    n.id === nodeId
                        ? {
                            ...n,
                            data: {
                                ...n.data,
                                transformKind: nextKind, // ✅ structural
                                meta: { ...(n.data.meta ?? {}), updatedAt: new Date().toISOString() }
                            }
                        }
                        : n
                );

                const next = { ...s, nodes };
                persist(next);
                return next;
            });

            // 2) replace params via your validated path (schema stripping happens here)
            const r = updateNodeConfigImpl(nodeId, { params: nextParams });

            // 3) ensure inspector draft matches immediately after type switch
            if (r.ok) {
                update((s) => {
                    const n = s.nodes.find(x => x.id === nodeId);
                    return {
                        ...s,
                        inspector: {
                            nodeId,
                            draftParams: structuredClone((n?.data.params ?? {}) as any),
                            dirty: false
                        }
                    };
                });
            }

            return r;
        },

                // graphStore.ts (inside your graphStore object) TODO review
        setToolKind(nodeId: string, nextKind: LlmKind) {
            const nextParams = structuredClone(defaultLlmParamsByKind[nextKind]);

            // 1) update structural subtype on the node
            update((s) => {
                const node = s.nodes.find(n => n.id === nodeId);
                if (!node) return logPush(s, "warn", "Node not found", nodeId);

                const nodes = s.nodes.map(n =>
                    n.id === nodeId
                        ? {
                            ...n,
                            data: {
                                ...n.data,
                                llmKind: nextKind, // ✅ structural
                                meta: { ...(n.data.meta ?? {}), updatedAt: new Date().toISOString() }
                            }
                        }
                        : n
                );

                const next = { ...s, nodes };
                persist(next);
                return next;
            });

            // 2) replace params via your validated path (schema stripping happens here)
            const r = updateNodeConfigImpl(nodeId, { params: nextParams });

            // 3) ensure inspector draft matches immediately after type switch
            if (r.ok) {
                update((s) => {
                    const n = s.nodes.find(x => x.id === nodeId);
                    return {
                        ...s,
                        inspector: {
                            nodeId,
                            draftParams: structuredClone((n?.data.params ?? {}) as any),
                            dirty: false
                        }
                    };
                });
            }

            return r;
        },


        // ----- sync entrypoints (because SvelteFlow uses bind:nodes/bind:edges) -----
        syncFromCanvas(nodes: Node<PipelineNodeData>[], edges: Edge<PipelineEdgeData>[]) {
            update((s) => {
                // avoid needless churn if same references
                if (s.nodes === nodes && s.edges === edges) return s;
                const next = { ...s, nodes, edges };
                persist(next);
                return next;
            });
        },

        // ----- selection -----
        selectNode(nodeId: string | null) {
            update((s) => {
                console.log("selectNode", nodeId, s.inspector.draftParams, s.inspector.draftParams?.type);

                if (!nodeId) {
                    return {
                        ...s,
                        selectedNodeId: null,
                        inspector: initialInspector
                    };
                }

                const n = s.nodes.find((x) => x.id === nodeId);
                console.log("selectNode", nodeId, n, s.inspector.draftParams, s.inspector.draftParams?.type);
                return {
                    ...s,
                    selectedNodeId: nodeId,
                    inspector: {
                        nodeId,
                        draftParams: structuredClone(((n?.data.params ?? {}) as any)),
                        dirty: false
                    }
                };
            });
        },


        // ----- node CRUD -----
        addNode(kind: NodeKind, position: { x: number; y: number }) {
            const id = `n_${crypto.randomUUID()}`;
            const node: Node<PipelineNodeData> = {
                id,
                type: kind,
                position,
                data: defaultNodeData(kind)
            };

            update((s) => {
                const next = logPush(
                    { ...s, nodes: [...s.nodes, node], selectedNodeId: id },
                    "info",
                    `Added node ${id} (${kind})`,
                    id
                );
                persist(next);
                return next;
            });

            return id;
        },

        deleteNode(nodeId: string) {
            update((s) => {
                const nodes = s.nodes.filter((n) => n.id !== nodeId);
                const edges = s.edges.filter((e) => e.source !== nodeId && e.target !== nodeId);
                const selectedNodeId = s.selectedNodeId === nodeId ? null : s.selectedNodeId;

                const next = logPush({ ...s, nodes, edges, selectedNodeId }, "info", `Deleted node ${nodeId}`, nodeId);
                persist(next);
                return next;
            });
        },

        // ----- edge CRUD -----
        deleteEdge(edgeId: string) {
            update((s) => {
                const edges = s.edges.filter((e) => e.id !== edgeId);
                const next = logPush({ ...s, edges }, "info", `Deleted edge ${edgeId}`);
                persist(next);
                return next;
            });
        },

        addEdge(edge: Edge<PipelineEdgeData>) {
            let out: { ok: boolean; id?: string; error?: string } = { ok: true };
            update((s) => {
                // basic sanity checks
                const sourceExists = s.nodes.some((n) => n.id === edge.source);
                const targetExists = s.nodes.some((n) => n.id === edge.target);
                if (!sourceExists || !targetExists) {
                    out = { ok: false, error: "Source or target node not found" };
                    return s;
                }

                // default id if absent
                const id = edge.id ?? `e_${crypto.randomUUID()}`;

                // duplicate id?
                if (s.edges.some((ee) => ee.id === id)) {
                    out = { ok: false, error: "Edge id already exists" };
                    return s;
                }

                // no self-connection
                if (edge.source === edge.target) {
                    out = { ok: false, error: "Cannot connect node to itself" };
                    return s;
                }

                // basic cycle prevention: if target reaches source already, adding would create cycle
                const adj = new Map<string, string[]>();
                for (const ee of s.edges) adj.set(ee.source, [...(adj.get(ee.source) ?? []), ee.target]);

                const seen = new Set<string>();
                const q = [edge.target];
                let createsCycle = false;
                while (q.length) {
                    const cur = q.shift()!;
                    if (cur === edge.source) {
                        createsCycle = true;
                        break;
                    }
                    for (const nxt of adj.get(cur) ?? []) {
                        if (!seen.has(nxt)) {
                            seen.add(nxt);
                            q.push(nxt);
                        }
                    }
                }
                if (createsCycle) {
                    out = { ok: false, error: "Connection would create a cycle" };
                    return s;
                }

                // validate port types + refresh contract
                const chk = isEdgeStillValid(s.nodes, { ...edge, id } as Edge<PipelineEdgeData>);
                if (chk.ok === false) {
                    out = { ok: false, error: chk.reason === "type_mismatch" ? "Incompatible port types" : "Cannot resolve port ttypes for this connection" };
                    return s;
                }

                const nextEdge: Edge<PipelineEdgeData> = {
                    ...edge,
                    id,
                    data: {
                        ...(edge.data ?? {}),
                        exec: (edge.data?.exec ?? "idle"),
                        contract: { out: chk.out, in: chk.in }
                    }
                };

                const next = logPush({ ...s, edges: [...s.edges, nextEdge] }, "info", `Added edge ${id}`);
                persist(next);
                out.id = id;
                return next;
            });

            return out;
        },

        updateNodeTitle(nodeId: string, label: string) {
            update((s) => {
                const nodes = s.nodes.map((n) => (n.id === nodeId ? { ...n, data: { ...n.data, label } } : n));
                const next = { ...s, nodes };
                persist(next);
                return next;
            });
        },

        //before extensive renovations

        // ----- clear edges of prior run's status (uses edge highlighting) -----
        resetRunUi() {
            update((s) => {
                const nodes = s.nodes.map((n) => ({
                    ...n,
                    data: { ...n.data, status: n.data.status === "stale" ? STALE : IDLE }
                }));
                const edges = resetEdgesExec(s.edges)
                const next = { ...s, nodes, edges, logs: [], runStatus: IDLE, nodeOutputs: {} };
                persist(next);
                return next;
            });
        },

        async runRemote(runFrom: string | null) {
            // prevent concurrent runs
            const s0 = get({ subscribe } as any) as GraphState;
            if (s0.runStatus === "running") return;

            // reset UI
            this.resetRunUi();
            update(s => ({ ...s, runStatus: "running" }));

            // snapshot graph DTO
            const s1 = get({ subscribe } as any) as GraphState;
            const payload = {
                runFrom,
                graph: { version: 1, nodes: s1.nodes, edges: s1.edges }
            };

            // create run
            let runId: string;

            try {
                ({ runId } = await createRun(payload));
            } catch (e) {
                update(s => logPush({ ...s, runStatus: "failed" }, "error", `Run create failed: ${String(e)}`));
                return;
            }

            await new Promise<void>((resolve) => {
                const sub = streamRunEvents(
                    runId,
                    (evt: KnownRunEvent) => {
                        update((s) => {
                            switch (evt.type) {

                                case "node_output": {
                                    console.log("node_output", evt.nodeId, evt.artifactId, evt.preview);
                                    const nodeOutputs = {
                                        ...s.nodeOutputs,
                                        [evt.nodeId]: {
                                            artifactId: evt.artifactId,
                                            mimeType: evt.mimeType,
                                            preview: evt.preview ?? undefined
                                        }
                                    };

                                    return {
                                        ...s,
                                        nodeOutputs
                                    };
                                }

                                case "run_started": {
                                    const next = {
                                        ...s,
                                        nodeOutputs: {}
                                    };

                                    return logPush(
                                        next,
                                        "info",
                                        `Run started ${evt.runFrom ? `(from ${evt.runFrom})` : "(from start)"}`
                                    );
                                }


                                case "node_started": {
                                    const nodes = s.nodes.map((n) =>
                                        n.id === evt.nodeId ? { ...n, data: { ...n.data, status: RUNNING } } : n
                                    );
                                    return logPush({ ...s, nodes }, "info", "Node started", evt.nodeId);
                                }

                                case "edge_exec": {
                                    // IMPORTANT: store only data.exec. Do NOT “decorate” here.
                                    const edges = s.edges.map((e) =>
                                        e.id === evt.edgeId
                                            ? { ...e, data: { ...(e.data ?? {}), exec: evt.exec } }
                                            : e
                                    );
                                    return { ...s, edges };
                                }

                                case "log":
                                    return logPush(s, evt.level, evt.message, evt.nodeId);

                                case "node_finished": {
                                    const nodes = s.nodes.map((n) =>
                                        n.id === evt.nodeId ? { ...n, data: { ...n.data, status: evt.status } } : n  // was status: evt.status
                                    );
                                    return logPush({ ...s, nodes }, "info", `Node finished (${evt.status})`, evt.nodeId);
                                }

                                case "run_finished": {
                                    const next = logPush(
                                        { ...s, runStatus: evt.status },
                                        "info",
                                        `Run finished (${evt.status})`
                                    );
                                    persist(next);
                                    return next;
                                }

                                default:
                                    return s;
                            }
                        });

                        if (evt.type === "run_finished") {
                            sub.close();
                            resolve();
                        }
                    },
                    () => {
                        update((s) => logPush({ ...s, runStatus: "failed" }, "error", "Event stream error"));
                        resolve();
                    }
                );
            });

        }

    };
})();

export const selectedNode = derived(graphStore, ($s) =>
    $s.selectedNodeId
        ? $s.nodes.find((n) => n.id === $s.selectedNodeId) ?? null
        : null
);
