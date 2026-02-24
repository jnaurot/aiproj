import asyncio
import json
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from app.runner.nodes.transform import (
    normalize_transform_params,
    canonical_json,
    inputs_fingerprint,
    load_table_from_artifact_bytes,
    run_transform,
    sha256_hex,
)


from .compile import compile_plan
from .events import RunEventBus
from .validator import GraphValidator
from .metadata import ExecutionContext, NodeOutput
from .artifacts import Artifact, MemoryArtifactStore, RunBindings
from .cache import ExecutionCache

from ..executors.source import exec_source
from ..executors.llm import exec_llm
from ..executors.tool import exec_tool


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def node_map(graph: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {n["id"]: n for n in graph.get("nodes", [])}


def edge_map(graph: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {e["id"]: e for e in graph.get("edges", []) if "id" in e}


def upstream_node_ids(edges: Dict[str, Dict[str, Any]], node_id: str) -> list[str]:
    return [e["source"] for e in edges.values() if e.get("target") == node_id]

def resolve_input_refs(edges: Dict[str, Dict[str, Any]], node_id: str, node_to_artifact: Dict[str, str]) -> list[tuple[str, str]]:
    """
    Returns stable (port, upstream_artifact_id) pairs for edges targeting node_id.
    Port name is taken from edge.targetHandle if present; else 'in'.
    Only includes edges whose source node has produced an artifact.
    """
    refs: list[tuple[str, str]] = []
    for e in edges.values():
        if e.get("target") != node_id:
            continue
        src = e.get("source")
        if not src or src not in node_to_artifact:
            continue
        port = e.get("targetHandle") or "in"
        refs.append((port, node_to_artifact[src]))
    # stable order
    refs.sort(key=lambda x: x[0])
    return refs



async def run_graph(
    run_id: str, 
    graph: Dict[str, Any], 
    run_from: Optional[str], 
    bus: RunEventBus, 
    artifact_store=None, 
    cache=None
    ):
    # ---- Create execution context ONCE (do not recreate later) ----
    artifact_store = artifact_store or MemoryArtifactStore()
    bindings = RunBindings(run_id)

    context = ExecutionContext(
        run_id=run_id,
        bus=bus,
        artifact_store=artifact_store,
        bindings=bindings,
    )
    
    print("[context]", type(context.bus), type(context.artifact_store), type(context.bindings))

    node_to_artifact: dict[str, str] = {}
    cache = cache or ExecutionCache()

    # ===== PHASE 1: PRE-EXECUTION VALIDATION =====
    validator = GraphValidator()
    validation = validator.validate_pre_execution(graph)

    if not validation.valid:
        for error in validation.errors:
            await context.bus.emit({
                "type": "log",
                "runId": run_id,
                "at": iso_now(),
                "level": "error",
                "message": f"[{error.code}] {error.message}",
                "nodeId": error.node_id
            })
        await context.bus.emit({
            "type": "run_finished",
            "runId": run_id,
            "at": iso_now(),
            "status": "failed"
        })
        return

    for warning in validation.warnings:
        await context.bus.emit({
            "type": "log",
            "runId": run_id,
            "at": iso_now(),
            "level": "warn",
            "message": f"[{warning.code}] {warning.message}",
            "nodeId": warning.node_id
        })

    # ===== PHASE 2: EXECUTION =====
    await context.bus.emit({
        "type": "run_started",
        "runId": run_id,
        "at": iso_now(),
        "runFrom": run_from
    })

    try:
        plan = compile_plan(graph, run_from)
        nodes = node_map(graph)
        edges = edge_map(graph)

        for node_id in plan.order:
            await context.bus.emit({
                "type": "node_started",
                "runId": run_id,
                "at": iso_now(),
                "nodeId": node_id
            })

            # Activate incoming edges
            for edge_id in plan.incoming_edges.get(node_id, []):
                await context.bus.emit({
                    "type": "edge_exec",
                    "runId": run_id,
                    "at": iso_now(),
                    "edgeId": edge_id,
                    "exec": "active"
                })

            n = nodes[node_id]
            kind = n["data"]["kind"]
            params = n["data"].get("params", {}) or {}

            # Upstream resolution
            up_nodes = sorted(upstream_node_ids(edges, node_id))
            upstream_ids = [node_to_artifact[nid] for nid in up_nodes if nid in node_to_artifact]


            exec_key = cache.execution_key(
                node_kind=kind,
                params=params,
                upstream_artifact_ids=upstream_ids,
                execution_version=context.execution_version,
            )
            artifact_id = exec_key

            # ---- Cache check goes HERE (before execution) ----
            cached_artifact_id = await cache.get_artifact_id(exec_key)
            if cached_artifact_id and await context.artifact_store.exists(cached_artifact_id):
                context.bindings.bind(node_id=node_id, artifact_id=cached_artifact_id, status="cached")
                node_to_artifact[node_id] = cached_artifact_id

                # Verification (you asked for checks)
                print(f"[cache-hit] node={node_id} artifact={cached_artifact_id[:10]}...")

                cached_art = await context.artifact_store.get(cached_artifact_id)
                await context.bus.emit({
                    "type": "node_output",
                    "runId": run_id,
                    "nodeId": node_id,
                    "at": iso_now(),
                    "artifactId": cached_artifact_id,
                    "mimeType": cached_art.mime_type,
                })

                await context.bus.emit({
                    "type": "node_finished",
                    "runId": run_id,
                    "at": iso_now(),
                    "nodeId": node_id,
                    "status": "succeeded",
                    "cached": True
                })

                # Mark incoming edges as done
                for edge_id in plan.incoming_edges.get(node_id, []):
                    await context.bus.emit({
                        "type": "edge_exec",
                        "runId": run_id,
                        "at": iso_now(),
                        "edgeId": edge_id,
                        "exec": "done"
                    })
                continue

            # ---- Execute node ----
            try:
                await asyncio.sleep(0.5)  # visual delay

                if kind == "source":
                    output = await exec_source(run_id, n, context, upstream_artifact_ids=upstream_ids)
                    print("[run_graph] bound artifact", artifact_id[:10], "to node", node_id)
###
                elif kind == "transform":
                    await context.bus.emit({
                        "type": "log",
                        "runId": run_id,
                        "at": iso_now(),
                        "level": "info",
                        "message": "transform: start",
                        "nodeId": node_id,
                    })

                    if not params.get("enabled", True):
                        await context.bus.emit({
                            "type": "log",
                            "runId": run_id,
                            "at": iso_now(),
                            "level": "info",
                            "message": "transform: disabled; skipping",
                            "nodeId": node_id,
                        })
                        # Create a no-op NodeOutput (or mark succeeded with no artifact).
                        # Here: succeed but emit node_finished; keep artifact binding unchanged.
                        output = NodeOutput(status="succeeded", data=None, metadata=None, execution_time_ms=0.0)
                    else:
                        # 1) collect upstream artifacts (port -> artifact_id)
                        input_refs = resolve_input_refs(edges, node_id, node_to_artifact)  # [(port, artifact_id), ...]
                        input_tables = {}  # port -> DataFrame

                        for port, upstream_artifact_id in input_refs:
                            art = await context.artifact_store.get(upstream_artifact_id)
                            b = await context.artifact_store.read(upstream_artifact_id)
                            df = load_table_from_artifact_bytes(art.mime_type or "application/octet-stream", b)
                            input_tables[port] = df

                        # join lookup (node_id -> DataFrame), best-effort
                        join_lookup: dict[str, Any] = {}
                        for upstream_node_id, upstream_artifact_id in node_to_artifact.items():
                            art = await context.artifact_store.get(upstream_artifact_id)
                            b = await context.artifact_store.read(upstream_artifact_id)
                            try:
                                join_lookup[upstream_node_id] = load_table_from_artifact_bytes(art.mime_type or "", b)
                            except Exception:
                                pass

                        # 2) exec_key (transform-specific)
                        # NOTE: you currently use context.execution_version; keep that as build/version here too.
                        build_version = context.execution_version

                        norm = normalize_transform_params(params)
                        fp = {
                            "build": build_version,
                            "kind": "transform",
                            "params": norm,
                            "inputs": inputs_fingerprint(input_refs),
                        }
                        exec_key = sha256_hex(canonical_json(fp).encode("utf-8"))

                        # 3) cache hit?
                        cached_artifact_id = await cache.get_artifact_id(exec_key)
                        if cached_artifact_id and await context.artifact_store.exists(cached_artifact_id):
                            context.bindings.bind(node_id=node_id, artifact_id=cached_artifact_id, status="cached")
                            node_to_artifact[node_id] = cached_artifact_id

                            print(f"[cache-hit] transform node={node_id} artifact={cached_artifact_id[:10]}...")

                            # Emit node_output so UI can fetch bytes by artifactId
                            cached_art = await context.artifact_store.get(cached_artifact_id)
                            await context.bus.emit({
                                "type": "node_output",
                                "runId": run_id,
                                "nodeId": node_id,
                                "at": iso_now(),
                                "artifactId": cached_artifact_id,
                                "mimeType": cached_art.mime_type,
                            })

                            # finish the node (skip compute)
                            await context.bus.emit({
                                "type": "node_finished",
                                "runId": run_id,
                                "at": iso_now(),
                                "nodeId": node_id,
                                "status": "succeeded",
                                "cached": True
                            })

                            # Mark incoming edges as done
                            for edge_id in plan.incoming_edges.get(node_id, []):
                                await context.bus.emit({
                                    "type": "edge_exec",
                                    "runId": run_id,
                                    "at": iso_now(),
                                    "edgeId": edge_id,
                                    "exec": "done"
                                })
                            continue

                        # 4) execute
                        res = run_transform(params=norm, input_tables=input_tables, join_lookup=join_lookup)

                        await context.bus.emit({
                            "type": "log",
                            "runId": run_id,
                            "at": iso_now(),
                            "level": "info",
                            "message": f"transform: produced {len(res.payload_bytes)} bytes, content_hash={res.meta.get('content_hash')}",
                            "nodeId": node_id,
                        })

                        # 5) store artifact bytes + cache
                        artifact_id = exec_key  # keep your convention

                        artifact = Artifact(
                            artifact_id=artifact_id,
                            node_kind=kind,
                            # params_hash=cache.params_hash(params),
                            params_hash = sha256_hex(canonical_json(norm).encode("utf-8")),
                            upstream_ids=sorted([aid for _, aid in input_refs]),
                            created_at=datetime.now(timezone.utc),
                            execution_version=context.execution_version,
                            mime_type=res.mime_type,
                            size_bytes=len(res.payload_bytes),
                            storage_uri=f"memory://{artifact_id}",
                            schema=None,
                        )

                        await context.artifact_store.write(artifact, res.payload_bytes)

                        # bind + node_to_artifact
                        context.bindings.bind(node_id=node_id, artifact_id=artifact_id, status="computed")
                        node_to_artifact[node_id] = artifact_id

                        # cache index
                        await cache.store_artifact_id(exec_key, artifact_id)

                        print(f"[artifact] transform node={node_id} bytes={len(res.payload_bytes)} id={artifact_id[:10]}...")

                        # emit node_output (UI fetches by artifactId)
                        await context.bus.emit({
                            "type": "node_output",
                            "runId": run_id,
                            "nodeId": node_id,
                            "at": iso_now(),
                            "artifactId": artifact_id,
                            "mimeType": res.mime_type,
                        })

                        # return a NodeOutput for legacy metadata flow
                        output = NodeOutput(
                            status="succeeded",
                            data=None,
                            metadata=None,
                            # metadata={
                            #     **(res.meta or {}),
                            #     "artifact_id": artifact_id,
                            #     "mime_type": res.mime_type,
                            #     "exec_key": exec_key,
                            # },
                            execution_time_ms=0.0
                        )
                elif kind == "llm":
                    print("[run_graph] LLM up_nodes:", up_nodes)
                    print("[run_graph] LLM upstream_ids:", upstream_ids)
                    print("[run_graph] node_to_artifact keys:", list(node_to_artifact.keys()))
                    output = await exec_llm(run_id, n, context, upstream_artifact_ids=upstream_ids)
                elif kind == "tool":
                    output = await exec_tool(run_id, n, context, upstream_artifact_ids=upstream_ids)
                else:
                    raise RuntimeError(f"Unknown node kind: {kind}")

                # Validate output
                if output.status == "failed":
                    raise RuntimeError(output.error or "Node execution failed")

                # Store output for legacy flow / UI
                context.outputs[node_id] = output

                if kind == "transform":
                    await context.bus.emit({
                        "type": "node_finished",
                        "runId": run_id,
                        "at": iso_now(),
                        "nodeId": node_id,
                        "status": output.status
                    })
                else:
                    # ---- Artifact write + binding ----
                    mime_type = "application/octet-stream"
                    payload_bytes: bytes
                    data_value = getattr(output, "data", None)

                    if kind == "source":
                        ports = (n.get("data", {}).get("ports", {}) or {})
                        out_contract = ports.get("out")  # "table" or "text"/"binary"/etc

                        if out_contract == "table":
                            rows = data_value
                            if not isinstance(rows, list):
                                raise RuntimeError(
                                    f"Source table output must be list[dict], got {type(rows)}"
                                )

                            import io
                            import pandas as pd

                            df = pd.DataFrame(rows)
                            buf = io.StringIO()
                            df.to_csv(buf, index=False, lineterminator="\n")
                            payload_bytes = buf.getvalue().encode("utf-8")
                            mime_type = "text/csv; charset=utf-8"
                        elif isinstance(data_value, bytes):
                            payload_bytes = data_value
                            mime_type = "application/octet-stream"
                        elif isinstance(data_value, str):
                            payload_bytes = data_value.encode("utf-8")
                            mime_type = "text/plain; charset=utf-8"
                        elif data_value is None:
                            payload_bytes = b""
                            mime_type = "text/plain; charset=utf-8"
                        else:
                            payload_bytes = json.dumps(data_value, ensure_ascii=False).encode("utf-8")
                            mime_type = "application/json"

                    elif kind == "llm":
                        md = getattr(output, "metadata", None)
                        md_mime = getattr(md, "mime_type", None) if md is not None else None

                        if isinstance(data_value, bytes):
                            payload_bytes = data_value
                            mime_type = md_mime or "application/octet-stream"
                        elif data_value is None:
                            payload_bytes = b""
                            mime_type = md_mime or "text/plain; charset=utf-8"
                        else:
                            payload_bytes = str(data_value).encode("utf-8")
                            mime_type = md_mime or "text/plain; charset=utf-8"

                    else:
                        if isinstance(data_value, bytes):
                            payload_bytes = data_value
                            mime_type = "application/octet-stream"
                        elif isinstance(data_value, str):
                            payload_bytes = data_value.encode("utf-8")
                            mime_type = "text/plain; charset=utf-8"
                        elif data_value is None:
                            payload_bytes = b""
                            mime_type = "application/json"
                        else:
                            payload_bytes = json.dumps(data_value, ensure_ascii=False).encode("utf-8")
                            mime_type = "application/json"

                    artifact = Artifact(
                        artifact_id=artifact_id,
                        node_kind=kind,
                        params_hash=cache.params_hash(params),
                        upstream_ids=sorted(upstream_ids),
                        created_at=datetime.now(timezone.utc),
                        execution_version=context.execution_version,
                        mime_type=mime_type,
                        size_bytes=len(payload_bytes),
                        storage_uri=f"memory://{artifact_id}",
                        schema=None,
                    )

                    await context.artifact_store.write(artifact, payload_bytes)
                    context.bindings.bind(node_id=node_id, artifact_id=artifact_id, status="computed")
                    node_to_artifact[node_id] = artifact_id
                    await context.bus.emit({
                        "type": "node_output",
                        "runId": run_id,
                        "nodeId": node_id,
                        "at": iso_now(),
                        "artifactId": artifact_id,
                        "mimeType": artifact.mime_type,
                    })

                    # Update cache index
                    await cache.store_artifact_id(exec_key, artifact_id)

                    # Verification print
                    print(f"[artifact] node={node_id} kind={kind} bytes={len(payload_bytes)} \n\tid={artifact_id}...")

                    await context.bus.emit({
                        "type": "node_finished",
                        "runId": run_id,
                        "at": iso_now(),
                        "nodeId": node_id,
                        "status": output.status
                    })

            except Exception as ex:
                traceback.print_exc()
                await context.bus.emit({
                    "type": "log",
                    "runId": run_id,
                    "at": iso_now(),
                    "level": "error",
                    "message": str(ex),
                    "nodeId": node_id
                })
                await context.bus.emit({
                    "type": "node_finished",
                    "runId": run_id,
                    "at": iso_now(),
                    "nodeId": node_id,
                    "status": "failed",
                    "error": str(ex)
                })
                await context.bus.emit({
                    "type": "run_finished",
                    "runId": run_id,
                    "at": iso_now(),
                    "status": "failed"
                })
                return

            # Mark incoming edges as done
            for edge_id in plan.incoming_edges.get(node_id, []):
                await context.bus.emit({
                    "type": "edge_exec",
                    "runId": run_id,
                    "at": iso_now(),
                    "edgeId": edge_id,
                    "exec": "done"
                })

            await asyncio.sleep(0.05)

        await context.bus.emit({
            "type": "run_finished",
            "runId": run_id,
            "at": iso_now(),
            "status": "succeeded"
        })
    except asyncio.CancelledError:
        await context.bus.emit({
            "type": "run_finished",
            "runId": run_id,
            "at": iso_now(),
            "status": "run_canceled"
        })
        raise  # ✅ important: propagate cancellation
    except Exception as ex:
        traceback.print_exc()
        await context.bus.emit({
            "type": "log",
            "runId": run_id,
            "at": iso_now(),
            "level": "error",
            "message": str(ex)
        })
        await context.bus.emit({
            "type": "run_finished",
            "runId": run_id,
            "at": iso_now(),
            "status": "failed"
        })
