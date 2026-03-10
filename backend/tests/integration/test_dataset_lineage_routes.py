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
async def test_dataset_version_lineage_and_lookup_route(monkeypatch):
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"records": [{"id": 1, "label": "a"}]},
		)

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}, "meta": {"status": "ok"}},
		)

	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	monkeypatch.setenv("ARTIFACT_STORE", "memory")

	from app.main import app

	snapshot_id = "8c0b7decd4ead5c08a9e284cc6ecd85d52418ff95464c7f40d70751655c41707"
	graph = {
		"nodes": [
			{
				"id": "source_1",
				"data": {
					"kind": "source",
					"label": "Source",
					"sourceKind": "file",
					"params": {
						"source_type": "text",
						"text": "hello",
						"output_mode": "json",
						"snapshot_id": snapshot_id,
					},
					"ports": {"in": None, "out": "json"},
				},
			},
			{
				"id": "tool_1",
				"data": {
					"kind": "tool",
					"label": "Tool",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {}}},
					"ports": {"in": "json", "out": "json"},
				},
			},
		],
		"edges": [{"id": "e1", "source": "source_1", "target": "tool_1"}],
	}

	with TestClient(app) as client:
		create = client.post("/runs", json={"graphId": "graph-dsml-lineage", "runFrom": None, "graph": graph})
		assert create.status_code == 200, create.text
		payload = create.json()
		run_id = payload["runId"]
		graph_id = payload["graphId"]

		status = None
		for _ in range(80):
			res = client.get(f"/runs/{run_id}")
			assert res.status_code == 200, res.text
			status = res.json()
			if status.get("status") in {"succeeded", "failed", "cancelled"}:
				break
			time.sleep(0.05)
		assert status is not None
		assert status.get("status") == "succeeded"

		node_outputs = status.get("nodeOutputs") or {}
		source_artifact_id = str(node_outputs.get("source_1") or "")
		tool_artifact_id = str(node_outputs.get("tool_1") or "")
		assert source_artifact_id
		assert tool_artifact_id

		meta = client.get(f"/runs/artifacts/{tool_artifact_id}/meta?graphId={graph_id}")
		assert meta.status_code == 200, meta.text
		meta_body = meta.json()
		assert meta_body.get("datasetVersionId") == tool_artifact_id
		lineage = meta_body.get("lineage") or {}
		assert lineage.get("datasetVersionId") == tool_artifact_id
		assert lineage.get("parentArtifactIds") == [source_artifact_id]
		assert any(
			str(ref.get("role") or "") == "producer" and str(ref.get("artifactId") or "") == tool_artifact_id
			for ref in (lineage.get("runRefs") or [])
		)
		assert any(
			str(ref.get("role") or "") == "parent_producer" and str(ref.get("artifactId") or "") == source_artifact_id
			for ref in (lineage.get("runRefs") or [])
		)
		assert any(
			str(ref.get("snapshotId") or "") == snapshot_id
			for ref in (lineage.get("snapshotRefs") or [])
		)

		dataset = client.get(f"/runs/datasets/{tool_artifact_id}?graphId={graph_id}")
		assert dataset.status_code == 200, dataset.text
		dataset_body = dataset.json()
		assert dataset_body.get("datasetVersionId") == tool_artifact_id
		assert dataset_body.get("artifactId") == tool_artifact_id
		assert (dataset_body.get("lineage") or {}).get("parentArtifactIds") == [source_artifact_id]
