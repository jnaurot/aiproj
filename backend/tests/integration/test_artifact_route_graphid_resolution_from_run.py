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
					"params": {"rel_path": ".", "filename": "route.json", "file_format": "json"},
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
async def test_run_route_fallback_graphid_can_open_artifact(monkeypatch):
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"route": True}, "meta": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	monkeypatch.setenv("ARTIFACT_STORE", "memory")

	from app.main import app

	with TestClient(app) as client:
		create = client.post(
			"/runs",
			json={"graphId": "graph-route-fallback", "runFrom": None, "graph": _single_source_graph()},
		)
		assert create.status_code == 200, create.text
		run_id = str(create.json().get("runId") or "")
		status = _wait_terminal(client, run_id)
		artifact_id = str((status.get("nodeOutputs") or {}).get("source_1") or "").strip()
		assert artifact_id

		rt = app.state.runtime
		original_get_run = rt.get_run
		try:
			rt.get_run = lambda _rid: None
			run_lookup = client.get(f"/runs/{run_id}")
			assert run_lookup.status_code == 200, run_lookup.text
			body = run_lookup.json()
			graph_id = str(body.get("graphId") or "").strip()
			assert graph_id == "graph-route-fallback"

			meta = client.get(f"/runs/artifacts/{artifact_id}/meta", params={"graphId": graph_id})
			assert meta.status_code == 200, meta.text
		finally:
			rt.get_run = original_get_run

