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


REQUIRED_BY_TYPE = {
    "run_started": {"runId", "at"},
    "run_finished": {"runId", "at", "status"},
    "node_started": {"runId", "at", "nodeId"},
    "node_finished": {"runId", "at", "nodeId", "status"},
    "node_output": {"runId", "at", "nodeId", "artifactId"},
    "level_started": {"runId", "at", "levelIndex", "nodesInLevel"},
    "level_finished": {"runId", "at", "levelIndex", "elapsedMs"},
    "cache_decision": {"schema_version", "runId", "at", "nodeId", "nodeKind", "decision", "reason", "execKey"},
    "cache_summary": {"schema_version", "runId", "at", "cache_hit", "cache_miss", "cache_hit_contract_mismatch"},
    "edge_exec": {"runId", "at", "edgeId", "exec"},
    "log": {"runId", "at", "level", "message"},
}

ALLOWED_TYPES = set(REQUIRED_BY_TYPE) | {
    "run_cancel_requested",
    "run_cancelled",
    "node_cancelled",
    "scheduler_cancelled",
    "run_telemetry",
}

ALLOWED_CACHE_REASONS = {
    "CACHE_HIT",
    "CACHE_ENTRY_MISSING",
    "INPUTS_UNRESOLVED",
    "PARAMS_CHANGED",
    "INPUT_CHANGED",
    "ENV_CHANGED",
    "BUILD_CHANGED",
    "UNCACHEABLE_EFFECTFUL_TOOL",
    "GLOBAL_FORCE_OFF",
    "NODE_POLICY_PREFER_OFF",
    "NODE_POLICY_FORCE_OFF",
    "CONTRACT_MISMATCH",
}


@pytest.mark.asyncio
async def test_event_schema_smoke(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data="hello")

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"kind": "json", "payload": {"ok": True}, "meta": {"status": "ok"}},
        )

    monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
    monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

    events = []
    artifact_root = tmp_path / "artifact-root"
    bus = RunEventBus("run-event-schema-1", on_emit=lambda e: events.append(dict(e)))
    await run_mod.run_graph(
        run_id="run-event-schema-1",
        graph={
            "nodes": [
                {
                    "id": "source_1",
                    "data": {
                        "kind": "source",
                        "label": "Source",
                        "sourceKind": "file",
                        "params": {"file_path": "dummy.txt", "file_format": "txt"},
                        "ports": {"in": None, "out": "text"},
                    },
                },
                {
                    "id": "tool_1",
                    "data": {
                        "kind": "tool",
                        "label": "Tool",
                        "params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {}}},
                        "ports": {"in": "text", "out": "json"},
                    },
                },
            ],
            "edges": [{"id": "e1", "source": "source_1", "target": "tool_1"}],
        },
        run_from=None,
        bus=bus,
        artifact_store=DiskArtifactStore(artifact_root),
        cache=SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite")),
        graph_id="graph-event-schema-1",
    )

    assert events, "Expected emitted runtime events"
    unknown = [e.get("type") for e in events if e.get("type") not in ALLOWED_TYPES]
    assert not unknown, f"Unknown event types seen: {sorted(set(unknown))}"

    for evt in events:
        assert isinstance(evt.get("type"), str) and evt["type"]
        required = REQUIRED_BY_TYPE.get(evt["type"])
        if not required:
            continue
        missing = [k for k in required if k not in evt]
        assert not missing, f"Event {evt['type']} missing fields: {missing}"

        if evt["type"] == "cache_decision":
            assert evt.get("schema_version") == 1
            assert evt.get("decision") in {
                "cache_hit",
                "cache_miss",
                "cache_hit_contract_mismatch",
            }
            assert evt.get("reason") in ALLOWED_CACHE_REASONS
        if evt["type"] == "cache_summary":
            assert evt.get("schema_version") == 1
