from __future__ import annotations

import importlib
import sys
import types

import pytest

from app.runner.artifacts import MemoryArtifactStore
from app.runner.cache import ExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import NodeOutput


def _ensure_duckdb_stub() -> None:
    if "duckdb" not in sys.modules:
        sys.modules["duckdb"] = types.SimpleNamespace()


@pytest.mark.asyncio
async def test_port_runtime_transitive_cascade_golden(monkeypatch) -> None:
    _ensure_duckdb_stub()
    run_mod = importlib.import_module("app.runner.run")
    executed: list[str] = []

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        node_id = str(node.get("id") or "")
        executed.append(node_id)
        if node_id == "select":
            raise run_mod.ContractMismatchError(
                "required work input handle had none provided",
                code="HANDLE_INPUT_NONE_PROVIDED",
                details={"expected": {"handle": "in"}, "actual": {"provided": 0}},
            )
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"kind": "json", "payload": {"node": node_id}, "meta": {}},
        )

    monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
    monkeypatch.setenv("RUNNER_MAX_CONCURRENCY", "1")
    events: list[dict] = []
    graph = {
        "nodes": [
            {"id": "jobs", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
            {"id": "select", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
            {"id": "generate", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
            {"id": "email", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
            {"id": "audit_root", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
            {"id": "audit_leaf", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
        ],
        "edges": [
            {"id": "e_jobs_select", "source": "jobs", "target": "select"},
            {"id": "e_select_generate", "source": "select", "target": "generate"},
            {"id": "e_generate_email", "source": "generate", "target": "email"},
            {"id": "e_audit", "source": "audit_root", "target": "audit_leaf"},
        ],
    }
    await run_mod.run_graph(
        run_id="run-port-runtime-cascade-golden",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-port-runtime-cascade-golden", on_emit=lambda evt: events.append(dict(evt))),
        artifact_store=MemoryArtifactStore(),
        cache=ExecutionCache(),
        graph_id="g-port-runtime-cascade-golden",
    )

    assert "jobs" in executed
    assert "select" in executed
    assert "generate" not in executed
    assert "email" not in executed
    assert "audit_root" in executed
    assert "audit_leaf" in executed

    cascades = [evt for evt in events if str(evt.get("type") or "") == "branch_cascade"]
    assert cascades
    cascade = cascades[-1]
    assert str(cascade.get("originNodeId") or "") == "select"
    assert str(cascade.get("reasonCode") or "") == "HANDLE_INPUT_NONE_PROVIDED"
    blocked = set(str(item or "") for item in (cascade.get("blockedNodeIds") or []))
    assert "generate" in blocked
    assert "email" in blocked

    finished = [evt for evt in events if str(evt.get("type") or "") == "run_finished"]
    assert finished
    assert str(finished[-1].get("status") or "") == "failed"
