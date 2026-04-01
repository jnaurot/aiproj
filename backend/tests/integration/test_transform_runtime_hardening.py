import importlib
import logging
import types

import pytest

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import NodeOutput


def _graph_select_success() -> dict:
	return {
		"nodes": [
			{
				"id": "source_1",
				"data": {
					"kind": "source",
					"label": "Source",
					"sourceKind": "file",
					"params": {"file_path": "dummy.json", "file_format": "json"},
				},
			},
			{
				"id": "transform_1",
				"data": {
					"kind": "transform",
					"label": "JsonToTable",
					"transformKind": "json_to_table",
					"params": {
						"op": "json_to_table",
						"json_to_table": {"orient": "records", "rowsKey": "rows"},
					},
				},
			},
		],
		"edges": [{"id": "e1", "source": "source_1", "target": "transform_1"}],
	}


def _graph_select_failure_missing_column() -> dict:
	graph = _graph_select_success()
	graph["nodes"].append(
		{
			"id": "transform_2",
			"data": {
				"kind": "transform",
				"label": "Select",
				"transformKind": "select",
				"params": {
					"op": "select",
					"select": {"mode": "include", "columns": ["does_not_exist"], "keepOrder": "custom", "strict": True},
				},
			},
		}
	)
	graph["edges"].append({"id": "e2", "source": "transform_1", "target": "transform_2"})
	return graph


async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
	return NodeOutput(
		status="succeeded",
		metadata=None,
		execution_time_ms=1.0,
		data=[{"text": "hello"}, {"text": "world"}],
	)


async def _run_graph(run_mod, graph: dict, tmp_path, run_id: str):
	events = []
	store = DiskArtifactStore(tmp_path / "artifacts")
	cache = SqliteExecutionCache(str(tmp_path / "artifacts" / "meta" / "artifacts.sqlite"))
	await run_mod.run_graph(
		run_id=run_id,
		graph=graph,
		run_from=None,
		bus=RunEventBus(run_id, on_emit=lambda e: events.append(dict(e))),
		artifact_store=store,
		cache=cache,
		graph_id=f"graph-{run_id}",
		runtime_ref=types.SimpleNamespace(get_global_cache_mode=lambda: "force_off"),
	)
	return events


@pytest.mark.asyncio
async def test_transform_runner_no_stdout_debug_prints_success_path(monkeypatch, tmp_path, capsys):
	run_mod = importlib.import_module("app.runner.run")
	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	events = await _run_graph(run_mod, _graph_select_success(), tmp_path, "run-transform-hardening-success")
	finish = [e for e in events if e.get("type") == "node_finished" and e.get("nodeId") == "transform_1"]
	assert finish and finish[-1].get("status") == "succeeded"
	stdout = capsys.readouterr().out
	assert "[transform-input-schema]" not in stdout
	assert "[transform-output-schema]" not in stdout
	assert "[artifact] transform node=" not in stdout


@pytest.mark.asyncio
async def test_transform_runner_no_stdout_debug_prints_failure_path(monkeypatch, tmp_path, capsys):
	run_mod = importlib.import_module("app.runner.run")
	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	events = await _run_graph(run_mod, _graph_select_failure_missing_column(), tmp_path, "run-transform-hardening-failure")
	finish = [e for e in events if e.get("type") == "node_finished" and e.get("nodeId") == "transform_1"]
	finish = [e for e in events if e.get("type") == "node_finished" and e.get("nodeId") == "transform_2"]
	assert finish and finish[-1].get("status") == "failed"
	stdout = capsys.readouterr().out
	assert "[schema-mismatch]" not in stdout
	assert "[dedupe-debug]" not in stdout


@pytest.mark.asyncio
async def test_transform_schema_mismatch_diagnostics_emitted_via_bus_not_stdout(monkeypatch, tmp_path, capsys):
	run_mod = importlib.import_module("app.runner.run")
	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	events = await _run_graph(
		run_mod,
		_graph_select_failure_missing_column(),
		tmp_path,
		"run-transform-hardening-schema-mismatch",
	)
	error_logs = [
		e
		for e in events
		if e.get("type") == "log" and e.get("nodeId") == "transform_2" and "transform: schema-mismatch" in str(e.get("message") or "")
	]
	assert error_logs
	stdout = capsys.readouterr().out
	assert "[schema-mismatch]" not in stdout


@pytest.mark.asyncio
async def test_transform_debug_logs_gated_by_debug_flags(monkeypatch, tmp_path, caplog):
	run_mod = importlib.import_module("app.runner.run")
	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)

	with caplog.at_level(logging.INFO):
		await _run_graph(run_mod, _graph_select_success(), tmp_path / "info", "run-transform-hardening-info")
	info_records = [r for r in caplog.records if "[external-schema]" in str(r.message)]
	assert info_records == []
	caplog.clear()

	with caplog.at_level(logging.DEBUG):
		await _run_graph(run_mod, _graph_select_success(), tmp_path / "debug", "run-transform-hardening-debug")
	debug_records = [r for r in caplog.records if "[external-schema]" in str(r.message)]
	assert debug_records


@pytest.mark.asyncio
async def test_transform_run_logs_include_structured_schema_diagnostics_without_stdout(monkeypatch, tmp_path, capsys):
	run_mod = importlib.import_module("app.runner.run")
	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	events = await _run_graph(
		run_mod,
		_graph_select_failure_missing_column(),
		tmp_path,
		"run-transform-hardening-structured-diagnostics",
	)
	schema_logs = [e for e in events if e.get("type") == "log" and "schema-mismatch" in str(e.get("message") or "")]
	assert schema_logs
	finish = [e for e in events if e.get("type") == "node_finished" and e.get("nodeId") == "transform_2"]
	assert finish and finish[-1].get("errorCode") == "MISSING_COLUMN"
	stdout = capsys.readouterr().out
	assert "[transform-input-schema]" not in stdout
	assert "[schema-mismatch]" not in stdout
