import importlib
import sys
import time
import types

import pytest
from fastapi.testclient import TestClient

if "duckdb" not in sys.modules:
	sys.modules["duckdb"] = types.SimpleNamespace()

from app.runner.metadata import NodeOutput


def _single_source_graph() -> dict:
	return {
		"nodes": [
			{
				"id": "source_1",
				"data": {
					"kind": "source",
					"label": "Source",
					"sourceKind": "file",
					"params": {"rel_path": ".", "filename": "same.json", "file_format": "json"},
				},
			}
		],
		"edges": [],
	}


def _wait_terminal(client: TestClient, run_id: str) -> dict:
	last = None
	for _ in range(120):
		res = client.get(f"/runs/{run_id}")
		assert res.status_code == 200, res.text
		last = res.json()
		if str(last.get("status") or "") in {"succeeded", "failed", "canceled"}:
			return last
		time.sleep(0.05)
	assert last is not None
	return last


@pytest.mark.asyncio
async def test_artifact_id_collision_is_accessible_from_both_graph_scopes(monkeypatch):
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"same": True, "value": 1}, "meta": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	monkeypatch.setenv("ARTIFACT_STORE", "memory")

	from app.main import app

	with TestClient(app) as client:
		create_a = client.post(
			"/runs",
			json={"graphId": "graph-collision-a", "runFrom": None, "graph": _single_source_graph()},
		)
		assert create_a.status_code == 200, create_a.text
		run_a = str(create_a.json().get("runId") or "")
		status_a = _wait_terminal(client, run_a)
		artifact_a = str((status_a.get("nodeOutputs") or {}).get("source_1") or "").strip()
		assert artifact_a

		create_b = client.post(
			"/runs",
			json={"graphId": "graph-collision-b", "runFrom": None, "graph": _single_source_graph()},
		)
		assert create_b.status_code == 200, create_b.text
		run_b = str(create_b.json().get("runId") or "")
		status_b = _wait_terminal(client, run_b)
		artifact_b = str((status_b.get("nodeOutputs") or {}).get("source_1") or "").strip()
		assert artifact_b

		# This test intentionally forces/validates same-content collision behavior.
		assert artifact_a == artifact_b

		meta_a = client.get(f"/runs/artifacts/{artifact_a}/meta", params={"graphId": "graph-collision-a"})
		meta_b = client.get(f"/runs/artifacts/{artifact_b}/meta", params={"graphId": "graph-collision-b"})

		assert meta_a.status_code == 200, meta_a.text
		assert (
			meta_b.status_code == 200
		), "If this fails (404), artifact ownership is pinned to first graph scope on content collision."

