import importlib
import types

import pytest

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import NodeOutput


async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
	return NodeOutput(
		status="succeeded",
		metadata=None,
		execution_time_ms=1.0,
		data=[{"text": "alpha", "value": 1}, {"text": "beta", "value": 2}],
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


def _graph_json_to_table_then_select_missing_col() -> dict:
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
				"id": "transform_json_to_table",
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
			{
				"id": "transform_select",
				"data": {
					"kind": "transform",
					"label": "Select",
					"transformKind": "select",
					"params": {
						"op": "select",
						"select": {"mode": "include", "columns": ["does_not_exist"], "keepOrder": "custom", "strict": True},
					},
				},
			},
		],
		"edges": [
			{"id": "e1", "source": "source_1", "target": "transform_json_to_table"},
			{"id": "e2", "source": "transform_json_to_table", "target": "transform_select"},
		],
	}


def _graph_source_to_select_contract_mismatch() -> dict:
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
				"id": "transform_select",
				"data": {
					"kind": "transform",
					"label": "Select",
					"transformKind": "select",
					"params": {
						"op": "select",
						"select": {"mode": "include", "columns": ["text"], "keepOrder": "custom", "strict": True},
					},
				},
			},
		],
		"edges": [{"id": "e1", "source": "source_1", "target": "transform_select"}],
	}


def _graph_json_to_table_then_derive_invalid_expr() -> dict:
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
				"id": "transform_json_to_table",
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
			{
				"id": "transform_derive",
				"data": {
					"kind": "transform",
					"label": "Derive",
					"transformKind": "derive",
					"params": {
						"op": "derive",
						"derive": {
							"mode": "sql",
							"columns": [{"name": "broken_col", "expr": "(((("}],
							"rules": [],
						},
					},
				},
			},
		],
		"edges": [
			{"id": "e1", "source": "source_1", "target": "transform_json_to_table"},
			{"id": "e2", "source": "transform_json_to_table", "target": "transform_derive"},
		],
	}


def _node_finish(events: list[dict], node_id: str) -> dict:
	matches = [e for e in events if e.get("type") == "node_finished" and e.get("nodeId") == node_id]
	assert matches
	return matches[-1]


@pytest.mark.asyncio
async def test_transform_missing_column_error_payload_has_precise_param_path(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")
	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	events = await _run_graph(
		run_mod,
		_graph_json_to_table_then_select_missing_col(),
		tmp_path,
		"run-transform-error-missing-column-param-path",
	)
	finish = _node_finish(events, "transform_select")
	assert finish.get("status") == "failed"
	assert finish.get("errorCode") == "MISSING_COLUMN"
	details = finish.get("errorDetails") if isinstance(finish.get("errorDetails"), dict) else {}
	assert details.get("op") == "select"
	assert details.get("paramPath") == "select.columns"
	assert isinstance(details.get("missingColumns"), list)
	assert "does_not_exist" in details.get("missingColumns", [])


@pytest.mark.asyncio
async def test_transform_contract_mismatch_error_payload_consistency_across_ops(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")
	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	events = await _run_graph(
		run_mod,
		_graph_source_to_select_contract_mismatch(),
		tmp_path,
		"run-transform-error-contract-mismatch-consistency",
	)
	finish = _node_finish(events, "transform_select")
	assert finish.get("status") == "failed"
	assert finish.get("errorCode") == "CONTRACT_EDGE_PAYLOAD_TYPE_MISMATCH"
	details = finish.get("errorDetails") if isinstance(finish.get("errorDetails"), dict) else {}
	assert isinstance(details.get("expected"), dict)
	assert isinstance(details.get("actual"), dict)
	assert details.get("expected", {}).get("inputType") == "table"
	assert details.get("actual", {}).get("actualType") == "json"


@pytest.mark.asyncio
async def test_transform_expr_invalid_error_payload_includes_op_and_param_path(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")
	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	events = await _run_graph(
		run_mod,
		_graph_json_to_table_then_derive_invalid_expr(),
		tmp_path,
		"run-transform-error-expr-invalid",
	)
	finish = _node_finish(events, "transform_derive")
	assert finish.get("status") == "failed"
	assert finish.get("errorCode") == "EXPR_INVALID"
	details = finish.get("errorDetails") if isinstance(finish.get("errorDetails"), dict) else {}
	assert details.get("op") == "derive"
	assert details.get("paramPath") == "params.derive.columns"


@pytest.mark.asyncio
async def test_transform_runtime_error_fallback_does_not_drop_structured_fields(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")
	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	events = await _run_graph(
		run_mod,
		_graph_json_to_table_then_select_missing_col(),
		tmp_path,
		"run-transform-error-structured-fields-retained",
	)
	finish = _node_finish(events, "transform_select")
	assert finish.get("status") == "failed"
	assert finish.get("errorCode") == "MISSING_COLUMN"
	details = finish.get("errorDetails") if isinstance(finish.get("errorDetails"), dict) else {}
	assert isinstance(details.get("missingColumns"), list)
	assert details.get("paramPath") == "select.columns"
