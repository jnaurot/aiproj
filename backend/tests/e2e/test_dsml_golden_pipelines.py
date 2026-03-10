from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable

import pytest

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.run import run_graph


def _tool_node(
	node_id: str,
	tool_id: str,
	args: dict[str, Any],
	*,
	profile_id: str = "ml",
	in_port: str | None = None,
	out_port: str | None = "json",
) -> dict[str, Any]:
	return {
		"id": node_id,
		"data": {
			"kind": "tool",
			"label": node_id,
			"ports": {"in": in_port, "out": out_port},
			"params": {
				"provider": "builtin",
				"builtin": {
					"toolId": tool_id,
					"profileId": profile_id,
					"args": args,
				},
			},
		},
	}


def _classification_rows() -> list[dict[str, Any]]:
	return [
		{"x1": 0.10, "x2": 1.20, "label": "A"},
		{"x1": 0.20, "x2": 1.10, "label": "A"},
		{"x1": 0.15, "x2": 1.05, "label": "A"},
		{"x1": 1.15, "x2": 0.20, "label": "B"},
		{"x1": 1.25, "x2": 0.10, "label": "B"},
		{"x1": 1.05, "x2": 0.25, "label": "B"},
	]


def _regression_rows() -> list[dict[str, Any]]:
	return [
		{"x1": 0.0, "x2": 1.0, "target": 2.0},
		{"x1": 1.0, "x2": 0.0, "target": 3.0},
		{"x1": 2.0, "x2": 1.0, "target": 7.0},
		{"x1": 3.0, "x2": 2.0, "target": 11.0},
		{"x1": 4.0, "x2": 3.0, "target": 15.0},
	]


def _retrain_initial_rows() -> list[dict[str, Any]]:
	# XOR-ish pattern is intentionally hard for linear models.
	return [
		{"x1": 0.0, "x2": 0.0, "label": "A"},
		{"x1": 0.0, "x2": 1.0, "label": "B"},
		{"x1": 1.0, "x2": 0.0, "label": "B"},
		{"x1": 1.0, "x2": 1.0, "label": "A"},
	]


def _drift_rows() -> list[dict[str, Any]]:
	return [
		{"label": "A", "prediction": "B"},
		{"label": "A", "prediction": "B"},
		{"label": "A", "prediction": "A"},
		{"label": "B", "prediction": "A"},
		{"label": "B", "prediction": "A"},
		{"label": "B", "prediction": "B"},
	]


def _classification_graph() -> dict[str, Any]:
	return {
		"nodes": [
			_tool_node(
				"train_cls",
				"ml.sklearn.train_classifier",
				{
					"rows": _classification_rows(),
					"label_col": "label",
					"feature_cols": ["x1", "x2"],
					"max_iter": 400,
					"calibration_bins": 5,
				},
			)
		],
		"edges": [],
	}


def _regression_graph() -> dict[str, Any]:
	return {
		"nodes": [
			_tool_node(
				"train_reg",
				"ml.sklearn.train_regressor",
				{
					"rows": _regression_rows(),
					"label_col": "target",
					"feature_cols": ["x1", "x2"],
				},
			)
		],
		"edges": [],
	}


def _drift_graph() -> dict[str, Any]:
	return {
		"nodes": [
			_tool_node(
				"eval_drift",
				"ml.sklearn.evaluate",
				{
					"task": "classification",
					"rows": _drift_rows(),
					"label_col": "label",
					"pred_col": "prediction",
				},
			)
		],
		"edges": [],
	}


def _retrain_graph() -> dict[str, Any]:
	return {
		"nodes": [
			_tool_node(
				"train_initial",
				"ml.sklearn.train_classifier",
				{
					"rows": _retrain_initial_rows(),
					"label_col": "label",
					"feature_cols": ["x1", "x2"],
					"max_iter": 400,
				},
			),
			_tool_node(
				"train_retrained",
				"ml.sklearn.train_classifier",
				{
					"rows": _classification_rows(),
					"label_col": "label",
					"feature_cols": ["x1", "x2"],
					"max_iter": 400,
				},
				in_port="json",
			),
		],
		"edges": [{"id": "e_train_initial_to_retrained", "source": "train_initial", "target": "train_retrained"}],
	}


def _latest(events: list[dict[str, Any]], event_type: str, node_id: str | None = None) -> dict[str, Any]:
	filtered = [e for e in events if str(e.get("type") or "") == event_type]
	if node_id is not None:
		filtered = [e for e in filtered if str(e.get("nodeId") or "") == node_id]
	assert filtered, f"missing event type={event_type} node={node_id}"
	return filtered[-1]


def _extract_run_reproducibility(events: list[dict[str, Any]]) -> dict[str, Any]:
	started = _latest(events, "run_started")
	repro = started.get("reproducibility")
	assert isinstance(repro, dict)
	assert int(repro.get("schemaVersion") or 0) == 1
	return repro


def _assert_cache_hits(events: list[dict[str, Any]], node_ids: list[str]) -> None:
	for node_id in node_ids:
		decision = _latest(events, "cache_decision", node_id=node_id)
		assert str(decision.get("decision") or "") == "cache_hit", (
			f"expected cache_hit for {node_id}, got {decision.get('decision')} ({decision.get('reason')})"
		)


async def _artifact_payload_for_node(
	events: list[dict[str, Any]],
	store: DiskArtifactStore,
	node_id: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
	out_evt = _latest(events, "node_output", node_id=node_id)
	artifact_id = str(out_evt.get("artifactId") or "").strip()
	assert artifact_id, f"missing artifact id for node {node_id}"
	meta = await store.get(artifact_id)
	assert isinstance(meta.payload_schema, dict)
	assert str(meta.payload_schema.get("type") or "") == "json"
	envelope = json.loads((await store.read(artifact_id)).decode("utf-8"))
	assert str(envelope.get("kind") or "") == "json"
	payload = envelope.get("payload")
	assert isinstance(payload, dict)
	return payload, meta.payload_schema


async def _assert_classification_golden(events: list[dict[str, Any]], store: DiskArtifactStore) -> None:
	payload, payload_schema = await _artifact_payload_for_node(events, store, "train_cls")
	metrics = payload.get("metrics_train")
	assert isinstance(metrics, dict)
	assert float(metrics.get("accuracy") or 0.0) >= 0.95
	assert float(metrics.get("f1") or 0.0) >= 0.95
	keys_sample = payload_schema.get("keys_sample") if isinstance(payload_schema, dict) else []
	assert isinstance(keys_sample, list)
	assert "analysis_artifacts" in keys_sample
	analysis_artifacts = payload.get("analysis_artifacts")
	assert isinstance(analysis_artifacts, list) and analysis_artifacts
	for artifact in analysis_artifacts:
		assert isinstance(artifact, dict)
		typed_schema = artifact.get("typed_schema")
		assert isinstance(typed_schema, dict)
		assert str(typed_schema.get("type") or "") == "table"
		assert isinstance(artifact.get("rows"), list)


async def _assert_regression_golden(events: list[dict[str, Any]], store: DiskArtifactStore) -> None:
	payload, payload_schema = await _artifact_payload_for_node(events, store, "train_reg")
	metrics = payload.get("metrics_train")
	assert isinstance(metrics, dict)
	assert float(metrics.get("r2") or 0.0) >= 0.99
	assert float(metrics.get("rmse") or 1.0) <= 0.01
	keys_sample = payload_schema.get("keys_sample") if isinstance(payload_schema, dict) else []
	assert isinstance(keys_sample, list)
	assert "model_package" in keys_sample
	analysis_artifacts = payload.get("analysis_artifacts")
	assert isinstance(analysis_artifacts, list) and analysis_artifacts
	for artifact in analysis_artifacts:
		assert isinstance(artifact, dict)
		typed_schema = artifact.get("typed_schema")
		assert isinstance(typed_schema, dict)
		assert str(typed_schema.get("type") or "") == "table"


async def _assert_drift_golden(events: list[dict[str, Any]], store: DiskArtifactStore) -> None:
	payload, payload_schema = await _artifact_payload_for_node(events, store, "eval_drift")
	metrics = payload.get("metrics")
	assert isinstance(metrics, dict)
	# Golden drift baseline: degraded performance should remain below this threshold.
	assert float(metrics.get("accuracy") or 1.0) <= 0.50
	assert float(metrics.get("f1") or 1.0) <= 0.50
	keys_sample = payload_schema.get("keys_sample") if isinstance(payload_schema, dict) else []
	assert isinstance(keys_sample, list)
	assert "analysis_artifacts" in keys_sample
	analysis_artifacts = payload.get("analysis_artifacts")
	assert isinstance(analysis_artifacts, list) and analysis_artifacts
	cm = next((a for a in analysis_artifacts if isinstance(a, dict) and str(a.get("name") or "") == "confusion_matrix"), None)
	assert isinstance(cm, dict)
	assert isinstance(cm.get("rows"), list)
	assert isinstance(cm.get("typed_schema"), dict)
	assert str((cm.get("typed_schema") or {}).get("type") or "") == "table"


async def _assert_retrain_golden(events: list[dict[str, Any]], store: DiskArtifactStore) -> None:
	initial_payload, initial_schema = await _artifact_payload_for_node(events, store, "train_initial")
	retrained_payload, retrained_schema = await _artifact_payload_for_node(events, store, "train_retrained")
	initial_metrics = initial_payload.get("metrics_train")
	retrained_metrics = retrained_payload.get("metrics_train")
	assert isinstance(initial_metrics, dict)
	assert isinstance(retrained_metrics, dict)
	initial_acc = float(initial_metrics.get("accuracy") or 0.0)
	retrained_acc = float(retrained_metrics.get("accuracy") or 0.0)
	assert retrained_acc >= (initial_acc + 0.20)
	for schema in (initial_schema, retrained_schema):
		keys_sample = schema.get("keys_sample") if isinstance(schema, dict) else []
		assert isinstance(keys_sample, list)
		assert "model_package" in keys_sample


@dataclass(frozen=True)
class GoldenScenario:
	id: str
	graph_factory: Callable[[], dict[str, Any]]
	cache_node_ids: list[str]
	assertions: Callable[[list[dict[str, Any]], DiskArtifactStore], Any]


SCENARIOS: list[GoldenScenario] = [
	GoldenScenario(
		id="classification",
		graph_factory=_classification_graph,
		cache_node_ids=["train_cls"],
		assertions=_assert_classification_golden,
	),
	GoldenScenario(
		id="regression",
		graph_factory=_regression_graph,
		cache_node_ids=["train_reg"],
		assertions=_assert_regression_golden,
	),
	GoldenScenario(
		id="drift",
		graph_factory=_drift_graph,
		cache_node_ids=["eval_drift"],
		assertions=_assert_drift_golden,
	),
	GoldenScenario(
		id="retrain",
		graph_factory=_retrain_graph,
		cache_node_ids=["train_initial", "train_retrained"],
		assertions=_assert_retrain_golden,
	),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", SCENARIOS, ids=[scenario.id for scenario in SCENARIOS])
async def test_e2e_dsml_golden_pipelines_cache_repro_and_schema(tmp_path, scenario: GoldenScenario):
	pytest.importorskip("sklearn")
	scenario_root = tmp_path / scenario.id
	store = DiskArtifactStore(scenario_root / "artifacts")
	cache = SqliteExecutionCache(str(scenario_root / "artifacts" / "meta" / "artifacts.sqlite"))
	graph = scenario.graph_factory()

	events_1: list[dict[str, Any]] = []
	run_id_1 = f"run-dsml-golden-{scenario.id}-1"
	await run_graph(
		run_id=run_id_1,
		graph=graph,
		run_from=None,
		bus=RunEventBus(run_id_1, on_emit=lambda e: events_1.append(dict(e))),
		artifact_store=store,
		cache=cache,
		graph_id=f"graph-dsml-golden-{scenario.id}",
	)
	assert str(_latest(events_1, "run_finished").get("status") or "") == "succeeded"
	await scenario.assertions(events_1, store)
	repro_1 = _extract_run_reproducibility(events_1)

	events_2: list[dict[str, Any]] = []
	run_id_2 = f"run-dsml-golden-{scenario.id}-2"
	await run_graph(
		run_id=run_id_2,
		graph=graph,
		run_from=None,
		bus=RunEventBus(run_id_2, on_emit=lambda e: events_2.append(dict(e))),
		artifact_store=store,
		cache=cache,
		graph_id=f"graph-dsml-golden-{scenario.id}",
	)
	assert str(_latest(events_2, "run_finished").get("status") or "") == "succeeded"
	_assert_cache_hits(events_2, scenario.cache_node_ids)
	repro_2 = _extract_run_reproducibility(events_2)
	assert repro_2 == repro_1
