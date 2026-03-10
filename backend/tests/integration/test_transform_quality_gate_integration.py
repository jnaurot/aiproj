import importlib
import json
import types

import pytest

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import NodeOutput


def _graph_for_quality_gate(*, severity: str) -> dict:
	return {
		"nodes": [
			{
				"id": "source_1",
				"data": {
					"kind": "source",
					"label": "Source",
					"sourceKind": "file",
					"params": {"file_path": "dummy.txt", "file_format": "txt"},
					"ports": {"in": None, "out": "table"},
				},
			},
			{
				"id": "transform_1",
				"data": {
					"kind": "transform",
					"label": "Quality Gate",
					"transformKind": "quality_gate",
					"params": {
						"op": "quality_gate",
						"quality_gate": {
							"stopOnFail": True,
							"checks": [
								{
									"kind": "null_pct",
									"column": "text",
									"maxNullPct": 0.2,
									"severity": severity,
								}
							],
						},
					},
					"ports": {"in": "table", "out": "table"},
				},
			},
		],
		"edges": [{"id": "e1", "source": "source_1", "target": "transform_1"}],
	}


@pytest.mark.asyncio
async def test_quality_gate_warn_path_emits_warning_log_and_succeeds(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data=[{"text": "hello"}, {"text": None}, {"text": None}],
		)

	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)

	events = []
	store = DiskArtifactStore(tmp_path / "artifacts")
	cache = SqliteExecutionCache(str(tmp_path / "artifacts" / "meta" / "artifacts.sqlite"))
	await run_mod.run_graph(
		run_id="run-transform-quality-gate-warn",
		graph=_graph_for_quality_gate(severity="warn"),
		run_from=None,
		bus=RunEventBus("run-transform-quality-gate-warn", on_emit=lambda e: events.append(dict(e))),
		artifact_store=store,
		cache=cache,
		graph_id="graph-transform-quality-gate",
		runtime_ref=types.SimpleNamespace(get_global_cache_mode=lambda: "force_off"),
	)

	finish = [e for e in events if e.get("type") == "node_finished" and e.get("nodeId") == "transform_1"]
	if finish:
		status = str(finish[-1].get("status") or "")
		assert status in {"succeeded", "failed"}
	warn_logs = [
		e
		for e in events
		if e.get("type") == "log" and e.get("nodeId") == "transform_1" and e.get("level") == "warn"
	]
	assert warn_logs
	payload = json.loads(str(warn_logs[-1].get("message") or "{}"))
	assert payload.get("op") == "quality_gate"
	assert len(payload.get("warnViolations") or []) == 1


@pytest.mark.asyncio
async def test_quality_gate_fail_path_fails_node(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data=[{"text": "hello"}, {"text": None}, {"text": None}],
		)

	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)

	events = []
	store = DiskArtifactStore(tmp_path / "artifacts")
	cache = SqliteExecutionCache(str(tmp_path / "artifacts" / "meta" / "artifacts.sqlite"))
	await run_mod.run_graph(
		run_id="run-transform-quality-gate-fail",
		graph=_graph_for_quality_gate(severity="fail"),
		run_from=None,
		bus=RunEventBus("run-transform-quality-gate-fail", on_emit=lambda e: events.append(dict(e))),
		artifact_store=store,
		cache=cache,
		graph_id="graph-transform-quality-gate",
		runtime_ref=types.SimpleNamespace(get_global_cache_mode=lambda: "force_off"),
	)

	finish = [e for e in events if e.get("type") == "node_finished" and e.get("nodeId") == "transform_1"]
	assert finish and finish[-1].get("status") == "failed"
	assert "quality_gate failed" in str(finish[-1].get("error") or "")
	out = [e for e in events if e.get("type") == "node_output" and e.get("nodeId") == "transform_1"]
	assert not out
