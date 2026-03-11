from __future__ import annotations

import importlib
import sys
import types
from typing import Any

import pytest

if "duckdb" not in sys.modules:
	sys.modules["duckdb"] = types.SimpleNamespace()

from app.runner.artifacts import MemoryArtifactStore
from app.runner.cache import ExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import NodeOutput
from app.runner.run import run_graph


def _graph_with_expected_schema(expected_fields: list[dict[str, Any]]) -> dict[str, Any]:
	return {
		"nodes": [
			{
				"id": "src_expected",
				"type": "source",
				"position": {"x": 0, "y": 0},
				"data": {
					"kind": "source",
					"label": "Source",
					"sourceKind": "file",
					"status": "idle",
					"params": {
						"source_type": "file",
						"rel_path": ".",
						"filename": "sample.csv",
						"file_format": "csv",
						"output_mode": "table",
					},
					"schema": {
						"expectedSchema": {
							"source": "declared",
							"typedSchema": {"type": "table", "fields": expected_fields},
						}
					},
				},
			}
		],
		"edges": [],
	}


def _event(events: list[dict[str, Any]], event_type: str, node_id: str | None = None) -> dict[str, Any]:
	matches = [e for e in events if str(e.get("type") or "") == event_type]
	if node_id is not None:
		matches = [e for e in matches if str(e.get("nodeId") or "") == node_id]
	assert matches, f"missing event={event_type} node={node_id}"
	return matches[-1]


@pytest.mark.asyncio
async def test_schema_first_lifecycle_runtime_enforcement_success(monkeypatch):
	monkeypatch.setenv("STRICT_COERCION_POLICY", "1")
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data=[
				{"id": 1, "price": 9.99},
				{"id": 2, "price": 1.25},
			],
		)

	original_exec_source = run_mod.exec_source
	run_mod.exec_source = _fake_exec_source
	try:
		events: list[dict[str, Any]] = []
		await run_graph(
			run_id="run-schema-first-success",
			graph=_graph_with_expected_schema(
				[
					{"name": "id", "type": "unknown", "nullable": False},
					{"name": "price", "type": "unknown", "nullable": False},
				]
			),
			run_from=None,
			bus=RunEventBus("run-schema-first-success", on_emit=lambda e: events.append(dict(e))),
			artifact_store=MemoryArtifactStore(),
			cache=ExecutionCache(),
			graph_id="graph-schema-first-success",
		)
		finished = _event(events, "node_finished", node_id="src_expected")
		assert str(finished.get("status") or "") == "succeeded"
		assert str(_event(events, "run_finished").get("status") or "") == "succeeded"
	finally:
		run_mod.exec_source = original_exec_source


@pytest.mark.asyncio
@pytest.mark.parametrize(
	("expected_fields", "rows", "error_code", "detail_key"),
	[
		(
			[
				{"name": "id", "type": "unknown", "nullable": False},
				{"name": "price", "type": "unknown", "nullable": False},
				{"name": "category", "type": "unknown", "nullable": True},
			],
			[{"id": 1, "price": 10.0}],
			"SCHEMA_MISSING_FIELD",
			"missingColumns",
		),
		(
			[
				{"name": "id", "type": "unknown", "nullable": False},
				{"name": "price", "type": "text", "nullable": False},
			],
			[{"id": 1, "price": 10.0}],
			"SCHEMA_TYPE_MISMATCH",
			"mismatchedColumns",
		),
	],
)
async def test_schema_first_lifecycle_runtime_enforcement_failures(
	monkeypatch,
	expected_fields: list[dict[str, Any]],
	rows: list[dict[str, Any]],
	error_code: str,
	detail_key: str,
):
	monkeypatch.setenv("STRICT_COERCION_POLICY", "1")
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data=rows,
		)

	original_exec_source = run_mod.exec_source
	run_mod.exec_source = _fake_exec_source
	try:
		events: list[dict[str, Any]] = []
		await run_graph(
			run_id=f"run-schema-first-{error_code.lower()}",
			graph=_graph_with_expected_schema(expected_fields),
			run_from=None,
			bus=RunEventBus("run-schema-first-failure", on_emit=lambda e: events.append(dict(e))),
			artifact_store=MemoryArtifactStore(),
			cache=ExecutionCache(),
			graph_id=f"graph-schema-first-{error_code.lower()}",
		)
		finished = _event(events, "node_finished", node_id="src_expected")
		assert str(finished.get("status") or "") == "failed"
		assert str(finished.get("errorCode") or "") == error_code
		details = finished.get("errorDetails") if isinstance(finished.get("errorDetails"), dict) else {}
		assert isinstance(details.get("expected"), dict)
		assert isinstance(details.get("actual"), dict)
		if detail_key == "mismatchedColumns":
			assert isinstance((details.get("actual") or {}).get(detail_key), list)
		else:
			assert isinstance(details.get(detail_key), list)
		assert str(_event(events, "run_finished").get("status") or "") == "failed"
	finally:
		run_mod.exec_source = original_exec_source
