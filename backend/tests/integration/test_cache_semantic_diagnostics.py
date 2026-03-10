import importlib
import os
import sys
import time
import types
from pathlib import Path

import pytest

if "duckdb" not in sys.modules:
	sys.modules["duckdb"] = types.SimpleNamespace()

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import NodeOutput


def _source_tool_graph(file_path: str) -> dict:
	p = Path(file_path)
	return {
		"nodes": [
			{
				"id": "source_1",
				"data": {
					"kind": "source",
					"label": "Source",
					"sourceKind": "file",
					"params": {
						"rel_path": str(p.parent),
						"filename": p.name,
						"file_format": "txt",
						"output_mode": "text",
					},
					"ports": {"in": None, "out": "text"},
				},
			},
			{
				"id": "tool_1",
				"data": {
					"kind": "tool",
					"label": "Tool",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop", "profileId": "core", "args": {}}},
					"ports": {"in": "text", "out": "json"},
				},
			},
		],
		"edges": [{"id": "e1", "source": "source_1", "target": "tool_1"}],
	}


def _latest_cache_decision(events: list[dict], node_id: str) -> dict:
	rows = [e for e in events if e.get("type") == "cache_decision" and e.get("nodeId") == node_id]
	assert rows, f"missing cache_decision for {node_id}"
	return rows[-1]


@pytest.mark.asyncio
async def test_semantic_cache_reasons_for_source_and_downstream_input_change(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")
	monkeypatch.setenv("WORKSPACE_ROOT_WORKSPACE", str(tmp_path))

	async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
		params = node["data"]["params"]
		p = (Path(params["rel_path"]) / params["filename"]).resolve()
		return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data=p.read_text(encoding="utf-8"))

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}, "meta": {"status": "ok"}},
		)

	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

	file_path = tmp_path / "input.txt"
	file_path.write_text("alpha", encoding="utf-8")
	graph = _source_tool_graph(str(file_path))
	artifact_root = tmp_path / "artifact-root-semantic-cache"
	store = DiskArtifactStore(artifact_root)
	cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))

	events_1: list[dict] = []
	await run_mod.run_graph(
		run_id="run-semantic-1",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-semantic-1", on_emit=lambda e: events_1.append(dict(e))),
		artifact_store=store,
		cache=cache,
		graph_id="graph-semantic-cache",
	)
	assert _latest_cache_decision(events_1, "source_1").get("reason") == "CACHE_ENTRY_MISSING"
	assert _latest_cache_decision(events_1, "tool_1").get("reason") == "CACHE_ENTRY_MISSING"

	events_2: list[dict] = []
	await run_mod.run_graph(
		run_id="run-semantic-2",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-semantic-2", on_emit=lambda e: events_2.append(dict(e))),
		artifact_store=store,
		cache=cache,
		graph_id="graph-semantic-cache",
	)
	assert _latest_cache_decision(events_2, "source_1").get("decision") == "cache_hit"
	assert _latest_cache_decision(events_2, "tool_1").get("decision") == "cache_hit"

	time.sleep(0.02)
	file_path.write_text("alpha-updated", encoding="utf-8")
	os.utime(file_path, None)

	events_3: list[dict] = []
	await run_mod.run_graph(
		run_id="run-semantic-3",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-semantic-3", on_emit=lambda e: events_3.append(dict(e))),
		artifact_store=store,
		cache=cache,
		graph_id="graph-semantic-cache",
	)
	assert _latest_cache_decision(events_3, "source_1").get("reason") == "PARAMS_CHANGED"
	assert _latest_cache_decision(events_3, "tool_1").get("reason") == "INPUT_CHANGED"


@pytest.mark.asyncio
async def test_semantic_cache_reason_build_changed(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")
	code_hash_state = {"tool": "a" * 64}

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}, "meta": {"status": "ok"}},
		)

	def _fake_code_hash(kind: str) -> str:
		if str(kind or "") == "tool":
			return str(code_hash_state["tool"])
		return "f" * 64

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	monkeypatch.setattr(run_mod, "_executor_code_hash_for_kind", _fake_code_hash)

	graph = {
		"nodes": [
			{
				"id": "tool_1",
				"data": {
					"kind": "tool",
					"label": "Tool",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop", "profileId": "core", "args": {}}},
					"ports": {"in": None, "out": "json"},
				},
			}
		],
		"edges": [],
	}
	artifact_root = tmp_path / "artifact-root-build-change"
	store = DiskArtifactStore(artifact_root)
	cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))

	events_1: list[dict] = []
	await run_mod.run_graph(
		run_id="run-build-1",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-build-1", on_emit=lambda e: events_1.append(dict(e))),
		artifact_store=store,
		cache=cache,
		graph_id="graph-build-change",
	)
	assert _latest_cache_decision(events_1, "tool_1").get("reason") == "CACHE_ENTRY_MISSING"

	code_hash_state["tool"] = "b" * 64

	events_2: list[dict] = []
	await run_mod.run_graph(
		run_id="run-build-2",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-build-2", on_emit=lambda e: events_2.append(dict(e))),
		artifact_store=store,
		cache=cache,
		graph_id="graph-build-change",
	)
	assert _latest_cache_decision(events_2, "tool_1").get("reason") == "BUILD_CHANGED"
