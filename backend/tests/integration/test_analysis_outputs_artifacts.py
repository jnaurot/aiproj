import json

import pytest

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.run import run_graph


def _graph_with_ml_analysis_tool() -> dict:
	return {
		"nodes": [
			{
				"id": "tool_1",
				"data": {
					"kind": "tool",
					"label": "ML Analysis",
					"ports": {"in": "json", "out": "json"},
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
									{"x1": 0.15, "x2": 1.05, "label": "A"},
									{"x1": 1.15, "x2": 0.15, "label": "B"},
								],
								"label_col": "label",
								"feature_cols": ["x1", "x2"],
								"max_iter": 200,
								"calibration_bins": 5,
							},
						},
					},
				},
			}
		],
		"edges": [],
	}


@pytest.mark.asyncio
async def test_ml_analysis_outputs_are_exposed_as_typed_artifacts(tmp_path):
	pytest.importorskip("sklearn")
	events = []
	store = DiskArtifactStore(tmp_path / "artifacts")
	cache = SqliteExecutionCache(str(tmp_path / "artifacts" / "meta" / "artifacts.sqlite"))
	await run_graph(
		run_id="run-ml-analysis-artifacts",
		graph=_graph_with_ml_analysis_tool(),
		run_from=None,
		bus=RunEventBus("run-ml-analysis-artifacts", on_emit=lambda e: events.append(dict(e))),
		artifact_store=store,
		cache=cache,
		graph_id="graph-ml-analysis-artifacts",
	)

	finish = [e for e in events if e.get("type") == "node_finished" and e.get("nodeId") == "tool_1"]
	assert finish and finish[-1].get("status") == "succeeded"
	out = [e for e in events if e.get("type") == "node_output" and e.get("nodeId") == "tool_1"]
	assert out
	artifact_id = str(out[-1].get("artifactId") or "")
	assert artifact_id

	meta = await store.get(artifact_id)
	assert isinstance(meta.payload_schema, dict)
	assert meta.payload_schema.get("type") == "json"
	keys_sample = meta.payload_schema.get("keys_sample") if isinstance(meta.payload_schema, dict) else []
	assert isinstance(keys_sample, list)
	assert "analysis_artifacts" in keys_sample

	payload_bytes = await store.read(artifact_id)
	envelope = json.loads(payload_bytes.decode("utf-8"))
	assert str(envelope.get("kind") or "") == "json"
	payload = envelope.get("payload") if isinstance(envelope.get("payload"), dict) else {}
	analysis_artifacts = payload.get("analysis_artifacts") if isinstance(payload, dict) else []
	assert isinstance(analysis_artifacts, list)
	artifact_names = {str(a.get("name")) for a in analysis_artifacts if isinstance(a, dict)}
	assert {"feature_importance", "confusion_matrix", "calibration"}.issubset(artifact_names)
	fi_artifact = next(
		(a for a in analysis_artifacts if isinstance(a, dict) and str(a.get("name") or "") == "feature_importance"),
		None,
	)
	assert isinstance(fi_artifact, dict)
	assert ((fi_artifact.get("typed_schema") or {}).get("type")) == "table"
	fi_rows = fi_artifact.get("rows") or []
	assert isinstance(fi_rows, list)
	assert fi_rows and "feature" in fi_rows[0] and "importance" in fi_rows[0]
