import importlib
import time
from uuid import uuid4

from fastapi.testclient import TestClient

from app.runner.metadata import NodeOutput


def _graph_for_metric(metric_value: float) -> dict:
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


def test_model_registry_routes_register_list_promote(monkeypatch):
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
				"payload": {"metrics": {"score": metric_value}},
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

	model_name = f"Model-{uuid4().hex[:8]}"

	with TestClient(app) as client:
		created_run = client.post(
			"/runs",
			json={"graphId": "graph-registry", "runFrom": None, "graph": _graph_for_metric(0.82)},
		)
		assert created_run.status_code == 200, created_run.text
		run_id = created_run.json()["runId"]
		_wait_run_finished(client, run_id)
		_wait_experiment(client, run_id)

		registered_v1 = client.post(
			"/models/versions/register",
			json={"modelName": model_name, "runId": run_id, "stage": "candidate"},
		)
		assert registered_v1.status_code == 200, registered_v1.text
		v1 = registered_v1.json()
		assert v1["versionNumber"] == 1
		assert v1["stage"] == "candidate"
		assert v1["runId"] == run_id
		assert (v1.get("provenance") or {}).get("source") == "run_experiment"

		model_id = v1["modelId"]

		registered_v2 = client.post(
			"/models/versions/register",
			json={
				"modelId": model_id,
				"modelName": model_name,
				"metrics": {"flat": {"score": 0.91}},
				"provenance": {"datasetVersionId": "ds-v2"},
			},
		)
		assert registered_v2.status_code == 200, registered_v2.text
		v2 = registered_v2.json()
		assert v2["versionNumber"] == 2

		models = client.get("/models?limit=100&offset=0")
		assert models.status_code == 200, models.text
		rows = models.json().get("models") or []
		assert any(str(r.get("modelId") or "") == model_id for r in rows)

		versions_before = client.get(f"/models/{model_id}/versions")
		assert versions_before.status_code == 200, versions_before.text
		assert len(versions_before.json().get("versions") or []) >= 2

		invalid_transition = client.post(
			f"/models/{model_id}/versions/{v2['versionId']}/promote",
			json={"toStage": "prod"},
		)
		assert invalid_transition.status_code == 400, invalid_transition.text

		promote_baseline_v1 = client.post(
			f"/models/{model_id}/versions/{v1['versionId']}/promote",
			json={"toStage": "baseline"},
		)
		assert promote_baseline_v1.status_code == 200, promote_baseline_v1.text

		conflict = client.post(
			f"/models/{model_id}/versions/{v2['versionId']}/promote",
			json={"toStage": "baseline", "force": False},
		)
		assert conflict.status_code == 409, conflict.text
		assert (conflict.json().get("detail") or {}).get("code") == "STAGE_CONFLICT"

		forced = client.post(
			f"/models/{model_id}/versions/{v2['versionId']}/promote",
			json={"toStage": "baseline", "force": True, "promotedBy": "qa"},
		)
		assert forced.status_code == 200, forced.text
		assert forced.json().get("demotedVersionId") == v1["versionId"]

		promote_prod = client.post(
			f"/models/{model_id}/versions/{v2['versionId']}/promote",
			json={"toStage": "prod"},
		)
		assert promote_prod.status_code == 200, promote_prod.text
		assert promote_prod.json().get("toStage") == "prod"


def test_model_registry_promotion_requires_admin_when_enabled(monkeypatch):
	monkeypatch.setenv("MODEL_REGISTRY_REQUIRE_ADMIN", "1")
	from app.main import app

	model_name = f"AdminModel-{uuid4().hex[:8]}"
	with TestClient(app) as client:
		created = client.post(
			"/models/versions/register",
			json={"modelName": model_name, "metrics": {"flat": {"score": 0.8}}},
		)
		assert created.status_code == 200, created.text
		body = created.json()
		model_id = body["modelId"]
		version_id = body["versionId"]

		denied = client.post(
			f"/models/{model_id}/versions/{version_id}/promote",
			json={"toStage": "baseline"},
		)
		assert denied.status_code == 403, denied.text

		allowed = client.post(
			f"/models/{model_id}/versions/{version_id}/promote",
			headers={"x-model-admin": "true"},
			json={"toStage": "baseline"},
		)
		assert allowed.status_code == 200, allowed.text
