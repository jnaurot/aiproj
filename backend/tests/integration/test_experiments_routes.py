import importlib
import sys
import time
import types

import pytest
from fastapi.testclient import TestClient

if "duckdb" not in sys.modules:
	sys.modules["duckdb"] = types.SimpleNamespace()

from app.runner.metadata import NodeOutput


def _graph_with_metric(metric_value: float) -> dict:
	return {
		"nodes": [
			{
				"id": "tool_1",
				"data": {
					"kind": "tool",
					"label": "Tool",
					"params": {
						"provider": "builtin",
						"builtin": {
							"toolId": "noop",
							"profileId": "core",
							"args": {"metric_value": metric_value},
						},
					},
					"ports": {"in": None, "out": "json"},
				},
			}
		],
		"edges": [],
	}


@pytest.mark.asyncio
async def test_experiment_tracking_and_run_compare(monkeypatch):
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		params = ((node.get("data") or {}).get("params") or {})
		builtin = params.get("builtin") if isinstance(params.get("builtin"), dict) else {}
		args = builtin.get("args") if isinstance(builtin.get("args"), dict) else {}
		metric_value = float(args.get("metric_value") or 0.0)
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={
				"kind": "json",
				"payload": {
					"metrics": {"score": metric_value},
					"metrics_train": {"loss": 1.0 - metric_value},
				},
				"meta": {
					"status": "ok",
					"builtin_environment": {
						"profileId": "core",
						"source": "profile",
						"installTarget": "cpu_dev",
						"packages": ["numpy", "requests"],
						"locked": "sha256:lock-core",
					},
				},
			},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	monkeypatch.setenv("ARTIFACT_STORE", "memory")

	from app.main import app

	def _wait_run_finished(client: TestClient, run_id: str) -> dict:
		status = None
		for _ in range(80):
			res = client.get(f"/runs/{run_id}")
			assert res.status_code == 200, res.text
			status = res.json()
			if status.get("status") in {"succeeded", "failed", "cancelled"}:
				break
			time.sleep(0.05)
		assert status is not None
		return status

	def _wait_experiment(client: TestClient, run_id: str) -> dict:
		body = None
		for _ in range(80):
			res = client.get(f"/experiments/runs/{run_id}")
			if res.status_code == 200:
				body = res.json()
				break
			time.sleep(0.05)
		assert body is not None
		return body

	with TestClient(app) as client:
		create_a = client.post(
			"/runs",
			json={"graphId": "graph-exp-compare", "runFrom": None, "graph": _graph_with_metric(0.8)},
		)
		assert create_a.status_code == 200, create_a.text
		run_a = create_a.json()["runId"]
		_wait_run_finished(client, run_a)
		exp_a = _wait_experiment(client, run_a)
		exp_a_payload = exp_a.get("experiment") or {}
		metrics_a = ((exp_a_payload.get("metrics") or {}).get("flat") or {})
		assert float(metrics_a.get("tool_1.metrics.score")) == pytest.approx(0.8)
		assert exp_a_payload.get("environment", {}).get("builtinProfiles") == ["core"]
		assert exp_a_payload.get("environment", {}).get("locks") == ["sha256:lock-core"]

		create_b = client.post(
			"/runs",
			json={"graphId": "graph-exp-compare", "runFrom": None, "graph": _graph_with_metric(0.9)},
		)
		assert create_b.status_code == 200, create_b.text
		run_b = create_b.json()["runId"]
		_wait_run_finished(client, run_b)
		_wait_experiment(client, run_b)

		listed = client.get("/experiments?graphId=graph-exp-compare&limit=10&offset=0")
		assert listed.status_code == 200, listed.text
		experiments = listed.json().get("experiments") or []
		assert any(str(row.get("runId") or "") == run_a for row in experiments)
		assert any(str(row.get("runId") or "") == run_b for row in experiments)

		compare = client.get(f"/experiments/compare?runA={run_a}&runB={run_b}")
		assert compare.status_code == 200, compare.text
		comparison = (compare.json().get("comparison") or {})
		assert int(comparison.get("sharedMetricCount") or 0) >= 2
		assert "tool_1" in (comparison.get("changedNodes") or [])
		deltas = comparison.get("metricDeltas") or []
		assert any(
			str(row.get("metric") or "") == "tool_1.metrics.score"
			and float(row.get("delta") or 0.0) == pytest.approx(0.1)
			for row in deltas
		)
