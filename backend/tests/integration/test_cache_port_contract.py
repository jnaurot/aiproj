import importlib
import sys
import types

import pytest

if "duckdb" not in sys.modules:
    sys.modules["duckdb"] = types.SimpleNamespace()

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import NodeOutput


def _tool_graph(*, out_port: str = "json", label: str = "Tool") -> dict:
    return {
        "nodes": [
            {
                "id": "tool_1",
                "data": {
                    "kind": "tool",
                    "label": label,
                    "params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {}}},
                    "ports": {"in": None, "out": out_port},
                },
            }
        ],
        "edges": [],
    }


@pytest.mark.asyncio
async def test_cache_hit_with_compatible_port_metadata_change_succeeds(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")
    call_count = {"tool": 0}

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        call_count["tool"] += 1
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"kind": "json", "payload": {"ok": True}, "meta": {"status": "ok"}},
        )

    monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

    artifact_root = tmp_path / "artifact-root"
    store = DiskArtifactStore(artifact_root)
    cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))

    events_1 = []
    await run_mod.run_graph(
        run_id="run-cache-port-1",
        graph=_tool_graph(out_port="json", label="Tool A"),
        run_from=None,
        bus=RunEventBus("run-cache-port-1", on_emit=lambda e: events_1.append(dict(e))),
        artifact_store=store,
        cache=cache,
    )
    out_1 = [e for e in events_1 if e.get("type") == "node_output" and e.get("nodeId") == "tool_1"]
    assert out_1
    artifact_id = out_1[-1]["artifactId"]
    assert call_count["tool"] == 1

    # Change only non-determinism metadata (label), keep compatible out port.
    events_2 = []
    await run_mod.run_graph(
        run_id="run-cache-port-2",
        graph=_tool_graph(out_port="json", label="Tool B"),
        run_from=None,
        bus=RunEventBus("run-cache-port-2", on_emit=lambda e: events_2.append(dict(e))),
        artifact_store=store,
        cache=cache,
    )
    out_2 = [e for e in events_2 if e.get("type") == "node_output" and e.get("nodeId") == "tool_1"]
    assert out_2 and out_2[-1]["artifactId"] == artifact_id
    assert out_2[-1].get("cached") is True
    finish_2 = [e for e in events_2 if e.get("type") == "node_finished" and e.get("nodeId") == "tool_1"]
    assert finish_2 and finish_2[-1].get("status") == "succeeded" and finish_2[-1].get("cached") is True
    assert call_count["tool"] == 1


@pytest.mark.asyncio
async def test_cache_hit_with_incompatible_declared_out_fails_contract(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")
    call_count = {"tool": 0}

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        call_count["tool"] += 1
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"kind": "json", "payload": {"ok": True}, "meta": {"status": "ok"}},
        )

    monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

    artifact_root = tmp_path / "artifact-root"
    store = DiskArtifactStore(artifact_root)
    cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))

    events_1 = []
    await run_mod.run_graph(
        run_id="run-cache-port-mismatch-1",
        graph=_tool_graph(out_port="json", label="Tool A"),
        run_from=None,
        bus=RunEventBus("run-cache-port-mismatch-1", on_emit=lambda e: events_1.append(dict(e))),
        artifact_store=store,
        cache=cache,
    )
    out_1 = [e for e in events_1 if e.get("type") == "node_output" and e.get("nodeId") == "tool_1"]
    assert out_1
    artifact_id = out_1[-1]["artifactId"]
    assert call_count["tool"] == 1

    # Change only declared out type; expect cache hit + deterministic contract failure.
    events_2 = []
    await run_mod.run_graph(
        run_id="run-cache-port-mismatch-2",
        graph=_tool_graph(out_port="text", label="Tool A"),
        run_from=None,
        bus=RunEventBus("run-cache-port-mismatch-2", on_emit=lambda e: events_2.append(dict(e))),
        artifact_store=store,
        cache=cache,
    )

    out_2 = [e for e in events_2 if e.get("type") == "node_output" and e.get("nodeId") == "tool_1"]
    assert out_2 and out_2[-1]["artifactId"] == artifact_id
    assert out_2[-1].get("cached") is True

    finish_2 = [e for e in events_2 if e.get("type") == "node_finished" and e.get("nodeId") == "tool_1"]
    assert finish_2
    assert finish_2[-1].get("status") == "failed"
    assert finish_2[-1].get("cached") is True
    assert "contract mismatch" in str(finish_2[-1].get("error", "")).lower()
    assert finish_2[-1].get("expectedPortType") == "text"
    assert finish_2[-1].get("actualPortType") == "json"
    assert finish_2[-1].get("artifactId") == artifact_id
    assert isinstance(finish_2[-1].get("producerExecKey"), str) or finish_2[-1].get("producerExecKey") is None

    run_done = [e for e in events_2 if e.get("type") == "run_finished"]
    assert run_done and run_done[-1].get("status") == "failed"

    cache_events = [e for e in events_2 if e.get("type") == "cache_decision" and e.get("nodeId") == "tool_1"]
    mismatch_events = [e for e in cache_events if e.get("decision") == "cache_hit_contract_mismatch"]
    assert mismatch_events
    assert all(e.get("schema_version") == 1 for e in mismatch_events)

    # No recompute: second run still served from cache path.
    assert call_count["tool"] == 1
