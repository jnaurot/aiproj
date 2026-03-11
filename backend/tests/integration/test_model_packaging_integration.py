import json

import pytest

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.run import run_graph


def _graph_with_packaged_predict(*, bad_rows: bool = False) -> dict:
	predict_rows = (
		[{"x1": 0.12, "x2": 1.05}, {"x1": 1.18, "x2": 0.12}]
		if not bad_rows
		else [{"x1": 0.12}]
	)
	return {
		"nodes": [
			{
				"id": "train",
				"data": {
					"kind": "tool",
					"label": "Train",
					"params": {
						"provider": "builtin",
						"builtin": {
							"toolId": "ml.sklearn.train_classifier",
							"profileId": "ml",
							"args": {
								"rows": [
									{"x1": 0.1, "x2": 1.1, "label": "A"},
									{"x1": 0.2, "x2": 1.0, "label": "A"},
									{"x1": 1.2, "x2": 0.1, "label": "B"},
									{"x1": 1.1, "x2": 0.2, "label": "B"},
								],
								"label_col": "label",
								"feature_cols": ["x1", "x2"],
							},
						},
					},
				},
			},
			{
				"id": "predict",
				"data": {
					"kind": "tool",
					"label": "Predict",
					"params": {
						"provider": "builtin",
						"builtin": {
							"toolId": "ml.sklearn.package_predict",
							"profileId": "ml",
							"args": {"rows": predict_rows},
						},
					},
				},
			},
		],
		"edges": [{"id": "e_train_predict", "source": "train", "target": "predict"}],
	}


@pytest.mark.asyncio
async def test_model_package_roundtrip_predict_integration(tmp_path):
	pytest.importorskip("sklearn")
	events = []
	store = DiskArtifactStore(tmp_path / "artifacts")
	cache = SqliteExecutionCache(str(tmp_path / "artifacts" / "meta" / "artifacts.sqlite"))
	await run_graph(
		run_id="run-model-package-roundtrip",
		graph=_graph_with_packaged_predict(bad_rows=False),
		run_from=None,
		bus=RunEventBus("run-model-package-roundtrip", on_emit=lambda e: events.append(dict(e))),
		artifact_store=store,
		cache=cache,
		graph_id="graph-model-package-roundtrip",
	)

	predict_finished = [e for e in events if e.get("type") == "node_finished" and e.get("nodeId") == "predict"]
	assert predict_finished
	assert predict_finished[-1].get("status") == "succeeded"
	predict_output = [e for e in events if e.get("type") == "node_output" and e.get("nodeId") == "predict"]
	assert predict_output
	artifact_id = str(predict_output[-1].get("artifactId") or "")
	assert artifact_id
	envelope = json.loads((await store.read(artifact_id)).decode("utf-8"))
	payload = envelope.get("payload") if isinstance(envelope, dict) else {}
	assert isinstance(payload, dict)
	assert payload.get("task") == "classification"
	predictions = payload.get("predictions") or []
	assert isinstance(predictions, list)
	assert len(predictions) == 2


@pytest.mark.asyncio
async def test_model_package_signature_enforcement_integration(tmp_path):
	pytest.importorskip("sklearn")
	events = []
	store = DiskArtifactStore(tmp_path / "artifacts")
	cache = SqliteExecutionCache(str(tmp_path / "artifacts" / "meta" / "artifacts.sqlite"))
	await run_graph(
		run_id="run-model-package-signature-fail",
		graph=_graph_with_packaged_predict(bad_rows=True),
		run_from=None,
		bus=RunEventBus("run-model-package-signature-fail", on_emit=lambda e: events.append(dict(e))),
		artifact_store=store,
		cache=cache,
		graph_id="graph-model-package-signature-fail",
	)

	predict_finished = [e for e in events if e.get("type") == "node_finished" and e.get("nodeId") == "predict"]
	assert predict_finished
	assert predict_finished[-1].get("status") == "failed"
	error = str(predict_finished[-1].get("error") or "").lower()
	assert "signature mismatch" in error
