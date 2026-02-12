import asyncio
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from .compile import compile_plan
from .events import RunEventBus
from .validator import GraphValidator
from .metadata import ExecutionContext, NodeOutput
from .artifacts import Artifact, MemoryArtifactStore, RunBindings
from .cache import ExecutionCache

from ..executors.source import exec_source
from ..executors.transform import exec_transform
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


async def run_graph(run_id: str, graph: Dict[str, Any], run_from: Optional[str], bus: RunEventBus):
    # ---- Create execution context ONCE (do not recreate later) ----
    artifact_store = MemoryArtifactStore()
    bindings = RunBindings(run_id)

    context = ExecutionContext(
        run_id=run_id,
        bus=bus,
        artifact_store=artifact_store,
        bindings=bindings,
    )
    
    print("[context]", type(context.bus), type(context.artifact_store), type(context.bindings))

    node_to_artifact: dict[str, str] = {}
    cache = ExecutionCache()

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
            up_nodes = upstream_node_ids(edges, node_id)
            upstream_ids = [node_to_artifact[nid] for nid in up_nodes if nid in node_to_artifact] or []

            # Compatibility path: existing metadata flow
            upstream_outputs = [context.outputs[nid] for nid in up_nodes if nid in context.outputs]
            input_metadata = upstream_outputs[0].metadata if upstream_outputs else None


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

                elif kind == "transform":
                    output = await exec_transform(run_id, n, context, input_metadata, upstream_artifact_ids=upstream_ids)
                elif kind == "llm":
                    print("[run_graph] LLM up_nodes:", up_nodes)
                    print("[run_graph] LLM upstream_ids:", upstream_ids)
                    print("[run_graph] node_to_artifact keys:", list(node_to_artifact.keys()))
                    output = await exec_llm(run_id, n, context, input_metadata, upstream_artifact_ids=upstream_ids)
                elif kind == "tool":
                    output = await exec_tool(run_id, n, context, input_metadata, upstream_artifact_ids=upstream_ids)
                else:
                    raise RuntimeError(f"Unknown node kind: {kind}")

                # Validate output
                if output.status == "failed":
                    raise RuntimeError(output.error or "Node execution failed")
                if output.status == "succeeded" and not output.metadata:
                    raise RuntimeError("Node succeeded but produced no metadata")

                # Store output for legacy flow / UI
                context.outputs[node_id] = output

                # ---- Artifact write + binding ----
                mime_type = "application/json"
                payload_bytes: bytes

                if kind == "source":
                    # Store actual source content (not NodeOutput JSON)
                    if not output.metadata or not getattr(output.metadata, "file_path", None):
                        raise RuntimeError("Source succeeded but missing metadata.file_path")

                    fp = output.metadata.file_path

                    # Best-effort read; for now cap size so we don't blow memory
                    with open(fp, "rb") as f:
                        payload_bytes = f.read()

                    mime_type = getattr(output.metadata, "mime_type", None) or "application/octet-stream"

                elif kind == "llm":
                    # Store the model output text/JSON
                    # Prefer output.value if you set it; fallback to NodeOutput JSON
                    v = getattr(output, "value", None)
                    if v is None:
                        payload_bytes = output.model_dump_json().encode("utf-8")
                        mime_type = "application/json"
                    else:
                        if isinstance(v, (dict, list)):
                            payload_bytes = json.dumps(v, ensure_ascii=False).encode("utf-8")
                            mime_type = "application/json"
                        else:
                            payload_bytes = str(v).encode("utf-8")
                            mime_type = "text/plain"

                else:
                    # Default fallback (until you convert other nodes)
                    payload_bytes = output.model_dump_json().encode("utf-8")
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
                #TODO UI FIX
                preview_text=""
                await context.bus.emit({
                    "type": "node_output",
                    "runId": run_id,
                    "nodeId": node_id,
                    "at": iso_now(),
                    "artifactId": artifact_id,
                    "mimeType": artifact.mime_type,
                    "preview": preview_text,
                })



                # Update cache index
                await cache.store_artifact_id(exec_key, artifact_id)

                # Verification print
                print(f"[artifact] node={node_id} kind={kind} bytes={len(payload_bytes)} id={artifact_id[:10]}...")

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
