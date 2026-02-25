import importlib
import sys
import time
import types

import pytest
from fastapi.testclient import TestClient

if "duckdb" not in sys.modules:
    sys.modules["duckdb"] = types.SimpleNamespace()

from app.runner.metadata import NodeOutput


@pytest.mark.asyncio
async def test_public_response_schema_versions_and_required_fields(monkeypatch):
    run_mod = importlib.import_module("app.runner.run")

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"kind": "json", "payload": {"ok": True}, "meta": {"status": "ok"}},
        )

    monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
    monkeypatch.setenv("ARTIFACT_STORE", "memory")

    from app.main import app

    graph = {
        "nodes": [
            {
                "id": "tool_1",
                "data": {
                    "kind": "tool",
                    "label": "Tool",
                    "params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {}}},
                    "ports": {"in": None, "out": "json"},
                },
            }
        ],
        "edges": [],
    }

    with TestClient(app) as client:
        create = client.post("/runs", json={"runFrom": None, "graph": graph})
        assert create.status_code == 200, create.text
        created = create.json()
        assert created.get("schemaVersion") == 1
        assert isinstance(created.get("runId"), str) and created["runId"]
        run_id = created["runId"]

        status = None
        for _ in range(60):
            res = client.get(f"/runs/{run_id}")
            assert res.status_code == 200, res.text
            status = res.json()
            if status.get("status") in {"succeeded", "failed", "cancelled"}:
                break
            time.sleep(0.05)

        assert status is not None
        assert status.get("schemaVersion") == 1
        for key in ("runId", "status", "createdAt", "nodeStatus", "nodeOutputs"):
            assert key in status
        assert status["status"] == "succeeded"

        list_runs = client.get("/runs")
        assert list_runs.status_code == 200, list_runs.text
        listed = list_runs.json()
        assert listed.get("schemaVersion") == 1
        assert isinstance(listed.get("runs"), list)

        node_outputs = status.get("nodeOutputs") or {}
        artifact_id = node_outputs.get("tool_1")
        assert artifact_id

        meta = client.get(f"/runs/artifacts/{artifact_id}/meta")
        assert meta.status_code == 200, meta.text
        meta_json = meta.json()
        assert meta_json.get("schemaVersion") == 1
        for key in (
            "artifactId",
            "nodeKind",
            "portType",
            "mimeType",
            "payloadSchema",
            "producerNodeId",
            "producerRunId",
        ):
            assert key in meta_json

        lineage = client.get(f"/runs/artifacts/{artifact_id}/lineage?depth=1")
        assert lineage.status_code == 200, lineage.text
        lineage_json = lineage.json()
        assert lineage_json.get("schemaVersion") == 1
        for key in ("artifactId", "depth", "lineage"):
            assert key in lineage_json
