import importlib
import sys
import types

import pytest

if "duckdb" not in sys.modules:
	sys.modules["duckdb"] = types.SimpleNamespace()

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runtime import RuntimeManager


def _single_noop_tool_graph() -> dict:
	return {
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


@pytest.mark.asyncio
async def test_env_profile_lock_mismatch_fails_preflight(tmp_path):
	run_mod = importlib.import_module("app.runner.run")
	events = []
	graph = {
		"nodes": [
			{
				"id": "tool_1",
				"data": {
					"kind": "tool",
					"label": "Locked Tool",
					"params": {
						"provider": "builtin",
						"builtin": {"toolId": "noop", "profileId": "core", "locked": "sha256:deadbeef", "args": {}},
					},
					"ports": {"in": None, "out": "json"},
				},
			}
		],
		"edges": [],
	}
	await run_mod.run_graph(
		run_id="run-lock-mismatch",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-lock-mismatch", on_emit=lambda e: events.append(dict(e))),
		artifact_store=DiskArtifactStore(tmp_path / "artifacts"),
		cache=SqliteExecutionCache(str(tmp_path / "artifacts" / "meta" / "artifacts.sqlite")),
		graph_id="graph-lock-mismatch",
	)
	node_finished = [e for e in events if e.get("type") == "node_finished" and e.get("nodeId") == "tool_1"]
	assert node_finished
	assert node_finished[-1].get("status") == "failed"
	assert str(node_finished[-1].get("errorCode") or "") == "ENV_PROFILE_LOCK_MISMATCH"
	run_finished = [e for e in events if e.get("type") == "run_finished"]
	assert run_finished and run_finished[-1].get("status") == "failed"


@pytest.mark.asyncio
async def test_resource_limit_node_cap_fails_early(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")
	monkeypatch.setenv("RUNNER_MAX_NODES", "1")
	events = []
	graph = {
		"nodes": [
			{
				"id": "tool_1",
				"data": {
					"kind": "tool",
					"label": "Tool 1",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop", "profileId": "core", "args": {}}},
					"ports": {"in": None, "out": "json"},
				},
			},
			{
				"id": "tool_2",
				"data": {
					"kind": "tool",
					"label": "Tool 2",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop", "profileId": "core", "args": {}}},
					"ports": {"in": "json", "out": "json"},
				},
			},
		],
		"edges": [{"id": "e1", "source": "tool_1", "target": "tool_2"}],
	}
	await run_mod.run_graph(
		run_id="run-resource-limit-nodes",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-resource-limit-nodes", on_emit=lambda e: events.append(dict(e))),
		artifact_store=DiskArtifactStore(tmp_path / "artifacts"),
		cache=SqliteExecutionCache(str(tmp_path / "artifacts" / "meta" / "artifacts.sqlite")),
		graph_id="graph-resource-limit-nodes",
	)
	run_finished = [e for e in events if e.get("type") == "run_finished"]
	assert run_finished
	assert run_finished[-1].get("status") == "failed"
	logs = [e for e in events if e.get("type") == "log"]
	assert any("RESOURCE_LIMIT_NODES" in str(l.get("message") or "") for l in logs)


@pytest.mark.asyncio
async def test_run_timeout_guardrail_fails_run(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")
	monkeypatch.setenv("RUNNER_MAX_RUNTIME_MS", "10")
	events = []
	await run_mod.run_graph(
		run_id="run-timeout-guardrail",
		graph=_single_noop_tool_graph(),
		run_from=None,
		bus=RunEventBus("run-timeout-guardrail", on_emit=lambda e: events.append(dict(e))),
		artifact_store=DiskArtifactStore(tmp_path / "artifacts"),
		cache=SqliteExecutionCache(str(tmp_path / "artifacts" / "meta" / "artifacts.sqlite")),
		graph_id="graph-timeout-guardrail",
	)
	run_finished = [e for e in events if e.get("type") == "run_finished"]
	assert run_finished
	assert run_finished[-1].get("status") == "failed"
	assert str(run_finished[-1].get("errorCode") or "") == "RUN_TIMEOUT"


@pytest.mark.asyncio
async def test_security_disallowed_subprocess_capability(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")
	events = []
	graph = {
		"nodes": [
			{
				"id": "shell_1",
				"data": {
					"kind": "tool",
					"label": "Shell",
					"params": {
						"provider": "shell",
						"permissions": {"subprocess": False},
						"shell": {"command": "echo hi"},
					},
					"ports": {"in": None, "out": "text"},
				},
			}
		],
		"edges": [],
	}
	await run_mod.run_graph(
		run_id="run-security-cap",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-security-cap", on_emit=lambda e: events.append(dict(e))),
		artifact_store=DiskArtifactStore(tmp_path / "artifacts"),
		cache=SqliteExecutionCache(str(tmp_path / "artifacts" / "meta" / "artifacts.sqlite")),
		graph_id="graph-security-cap",
	)
	node_finished = [e for e in events if e.get("type") == "node_finished" and e.get("nodeId") == "shell_1"]
	assert node_finished
	assert node_finished[-1].get("status") == "failed"
	assert "permissions.subprocess=true" in str(node_finished[-1].get("error") or "")


@pytest.mark.asyncio
async def test_reproducibility_metadata_persists_in_replay(monkeypatch, tmp_path):
	monkeypatch.setenv("ARTIFACT_STORE", "disk")
	monkeypatch.setenv("ARTIFACT_DIR", str(tmp_path / "artifact-root"))

	rt1 = RuntimeManager()
	run_id = "run-repro-replay-1"
	rt1.create_run(run_id)
	await rt1.start_run(run_id, _single_noop_tool_graph(), run_from=None, graph_id="graph-repro-replay-1")
	await rt1.get_run(run_id).task

	replay_1 = await rt1.list_run_events(run_id, after_id=0, limit=2000)
	run_started_1 = [
		(e.get("payload") if isinstance(e.get("payload"), dict) else {})
		for e in replay_1
		if str(e.get("type") or "") == "run_started"
	]
	assert run_started_1
	repro_1 = run_started_1[-1].get("reproducibility")
	assert isinstance(repro_1, dict)
	assert int((repro_1.get("schemaVersion") or 0)) == 1
	guardrails = repro_1.get("guardrails") if isinstance(repro_1.get("guardrails"), dict) else {}
	assert isinstance(guardrails, dict)
	assert "concurrencyCaps" in guardrails

	rt2 = RuntimeManager()
	replay_2 = await rt2.list_run_events(run_id, after_id=0, limit=2000)
	run_started_2 = [
		(e.get("payload") if isinstance(e.get("payload"), dict) else {})
		for e in replay_2
		if str(e.get("type") or "") == "run_started"
	]
	assert run_started_2
	repro_2 = run_started_2[-1].get("reproducibility")
	assert repro_2 == repro_1
