import importlib
import json
import re
import sys
import types
from pathlib import Path

import pytest

if "duckdb" not in sys.modules:
	sys.modules["duckdb"] = types.SimpleNamespace()

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import NodeOutput


_TRACE_PATTERN = re.compile(r"^\[trace\]\[memo\.execute_decision\]\s+(?P<payload>\{.*\})$")


def _memo_trace_events(events: list[dict], node_id: str | None = None) -> list[dict]:
	rows: list[dict] = []
	for evt in events:
		if str(evt.get("type") or "") != "log":
			continue
		if node_id is not None and str(evt.get("nodeId") or "") != str(node_id):
			continue
		msg = str(evt.get("message") or "")
		match = _TRACE_PATTERN.match(msg)
		if not match:
			continue
		rows.append(json.loads(match.group("payload")))
	return rows


def _source_tool_graph(file_path: str, *, source_meta: dict | None = None, tool_args: dict | None = None) -> dict:
	p = Path(file_path)
	return {
		"nodes": [
			{
				"id": "source_1",
				"data": {
					"kind": "source",
					"label": "Source",
					"sourceKind": "file",
					"meta": source_meta or {},
					"params": {
						"rel_path": str(p.parent),
						"filename": p.name,
						"file_format": "json",
						"output_mode": "json",
					},
				},
			},
			{
				"id": "tool_1",
				"data": {
					"kind": "tool",
					"label": "Tool",
					"params": {
						"provider": "builtin",
						"builtin": {"toolId": "noop", "profileId": "core", "args": tool_args or {}},
					},
				},
			},
		],
		"edges": [{"id": "e1", "source": "source_1", "target": "tool_1"}],
	}


@pytest.mark.asyncio
async def test_second_run_emits_memo_reuse_decisions(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")
	monkeypatch.setenv("WORKSPACE_ROOT_WORKSPACE", str(tmp_path))

	async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
		params = node["data"]["params"]
		p = (Path(params["rel_path"]) / params["filename"]).resolve()
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"text": p.read_text(encoding="utf-8")}},
		)

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

	file_path = tmp_path / "input.json"
	file_path.write_text('{"text":"alpha"}', encoding="utf-8")
	graph = _source_tool_graph(str(file_path))
	artifact_root = tmp_path / "artifact-root-memo-integration"
	store = DiskArtifactStore(artifact_root)
	cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))

	events_1: list[dict] = []
	await run_mod.run_graph(
		run_id="run-memo-1",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-memo-1", on_emit=lambda e: events_1.append(dict(e))),
		artifact_store=store,
		cache=cache,
		graph_id="graph-memo",
	)
	first_source = _memo_trace_events(events_1, "source_1")
	first_tool = _memo_trace_events(events_1, "tool_1")
	assert first_source and first_source[-1].get("decision") == "compute"
	assert first_tool and first_tool[-1].get("decision") == "compute"

	events_2: list[dict] = []
	await run_mod.run_graph(
		run_id="run-memo-2",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-memo-2", on_emit=lambda e: events_2.append(dict(e))),
		artifact_store=store,
		cache=cache,
		graph_id="graph-memo",
	)
	second_source = _memo_trace_events(events_2, "source_1")
	second_tool = _memo_trace_events(events_2, "tool_1")
	assert second_source and second_source[-1].get("decision") == "reuse"
	assert second_tool and second_tool[-1].get("decision") == "reuse"


@pytest.mark.asyncio
async def test_param_change_invalidates_downstream_memo(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"text": "alpha"}},
		)

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data={"kind": "json", "payload": {"ok": True}})

	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

	file_path = tmp_path / "input.json"
	file_path.write_text('{"text":"alpha"}', encoding="utf-8")
	artifact_root = tmp_path / "artifact-root-memo-param"
	store = DiskArtifactStore(artifact_root)
	cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))

	graph_v1 = _source_tool_graph(str(file_path), tool_args={"k": 1})
	await run_mod.run_graph(
		run_id="run-memo-param-1",
		graph=graph_v1,
		run_from=None,
		bus=RunEventBus("run-memo-param-1", on_emit=lambda e: None),
		artifact_store=store,
		cache=cache,
		graph_id="graph-memo-param",
	)

	events_2: list[dict] = []
	graph_v2 = _source_tool_graph(str(file_path), tool_args={"k": 2})
	await run_mod.run_graph(
		run_id="run-memo-param-2",
		graph=graph_v2,
		run_from=None,
		bus=RunEventBus("run-memo-param-2", on_emit=lambda e: events_2.append(dict(e))),
		artifact_store=store,
		cache=cache,
		graph_id="graph-memo-param",
	)
	source_trace = _memo_trace_events(events_2, "source_1")
	tool_trace = _memo_trace_events(events_2, "tool_1")
	assert source_trace and source_trace[-1].get("decision") == "reuse"
	assert tool_trace and tool_trace[-1].get("decision") == "compute"


@pytest.mark.asyncio
async def test_non_memoizable_node_emits_skip_non_memoizable(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"text": "alpha"}},
		)

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data={"kind": "json", "payload": {"ok": True}})

	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

	file_path = tmp_path / "input.json"
	file_path.write_text('{"text":"alpha"}', encoding="utf-8")
	graph = _source_tool_graph(str(file_path), source_meta={"memoizable": False})
	artifact_root = tmp_path / "artifact-root-non-memoizable"
	store = DiskArtifactStore(artifact_root)
	cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))

	events_1: list[dict] = []
	await run_mod.run_graph(
		run_id="run-non-memo-1",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-non-memo-1", on_emit=lambda e: events_1.append(dict(e))),
		artifact_store=store,
		cache=cache,
		graph_id="graph-non-memo",
	)
	first = _memo_trace_events(events_1, "source_1")
	assert first and first[-1].get("decision") == "skip_non_memoizable"

	events_2: list[dict] = []
	await run_mod.run_graph(
		run_id="run-non-memo-2",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-non-memo-2", on_emit=lambda e: events_2.append(dict(e))),
		artifact_store=store,
		cache=cache,
		graph_id="graph-non-memo",
	)
	second = _memo_trace_events(events_2, "source_1")
	assert second and second[-1].get("decision") == "skip_non_memoizable"
