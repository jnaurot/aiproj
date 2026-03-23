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
async def test_nonfatal_failure_cascades_transitively_but_unrelated_branch_runs(monkeypatch) -> None:
    _ensure_duckdb_stub()
    run_mod = importlib.import_module("app.runner.run")
    executed: list[str] = []

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        node_id = str(node["id"])
        executed.append(node_id)
        if node_id == "fail_mid":
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
            {"id": "fail_root", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
            {"id": "fail_mid", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
            {"id": "fail_leaf", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
            {"id": "fail_tail", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
            {"id": "ok_root", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
            {"id": "ok_leaf", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
        ],
        "edges": [
            {"id": "e_fail_1", "source": "fail_root", "target": "fail_mid"},
            {"id": "e_fail_2", "source": "fail_mid", "target": "fail_leaf"},
            {"id": "e_fail_3", "source": "fail_leaf", "target": "fail_tail"},
            {"id": "e_ok_1", "source": "ok_root", "target": "ok_leaf"},
        ],
    }
    await run_mod.run_graph(
        run_id="run-cascade-transitive",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-cascade-transitive", on_emit=lambda evt: events.append(dict(evt))),
        artifact_store=MemoryArtifactStore(),
        cache=ExecutionCache(),
        graph_id="g-cascade-transitive",
    )

    assert "fail_root" in executed
    assert "fail_mid" in executed
    assert "fail_leaf" not in executed
    assert "fail_tail" not in executed
    assert "ok_root" in executed
    assert "ok_leaf" in executed

    cascade = [evt for evt in events if str(evt.get("type") or "") == "branch_cascade"]
    assert cascade, "expected branch_cascade event"
    last_cascade = cascade[-1]
    assert str(last_cascade.get("originNodeId") or "") == "fail_mid"
    blocked = set(str(item or "") for item in (last_cascade.get("blockedNodeIds") or []))
    assert "fail_leaf" in blocked
    assert "fail_tail" in blocked

    blocked_signals = {
        str(evt.get("nodeId") or "")
        for evt in events
        if str(evt.get("type") or "") == "control_signal" and str(evt.get("signal") or "") == "blocked"
    }
    assert "fail_leaf" in blocked_signals
    assert "fail_tail" in blocked_signals

    finished = [evt for evt in events if str(evt.get("type") or "") == "run_finished"]
    assert finished
    assert str(finished[-1].get("status") or "") == "failed"
