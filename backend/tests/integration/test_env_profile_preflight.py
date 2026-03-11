import importlib
from types import SimpleNamespace

import pytest

from app.runner.artifacts import MemoryArtifactStore
from app.runner.cache import ExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import NodeOutput


def _python_tool_graph() -> dict:
	return {
		"nodes": [
			{
				"id": "tool_python",
				"data": {
					"kind": "tool",
					"label": "Tool Python",
					"params": {
						"provider": "python",
						"builtin": {"profileId": "full"},
						"python": {"code": "print('ok')", "args": {}, "capture_output": True},
					},
				},
			}
		],
		"edges": [],
	}


@pytest.mark.asyncio
async def test_run_preflight_blocks_when_builtin_profile_missing(monkeypatch):
	run_mod = importlib.import_module("app.runner.run")
	store = MemoryArtifactStore()
	cache = ExecutionCache()
	exec_tool_called = {"value": False}

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		exec_tool_called["value"] = True
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "text", "payload": "ok", "meta": {"status": "ok"}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	monkeypatch.setattr(run_mod, "missing_packages_for_packages", lambda packages: list(packages))

	events = []
	await run_mod.run_graph(
		run_id="run-env-profile-missing",
		graph=_python_tool_graph(),
		run_from=None,
		bus=RunEventBus("run-env-profile-missing", on_emit=lambda e: events.append(dict(e))),
		artifact_store=store,
		cache=cache,
		runtime_ref=SimpleNamespace(),
		graph_id="graph-env-profile-missing",
	)

	assert exec_tool_called["value"] is False
	assert any(
		e.get("type") == "log"
		and "ENV_PROFILE_MISSING" in str(e.get("message") or "")
		for e in events
	)
	assert any(
		e.get("type") == "log"
		and "Install profile:" in str(e.get("message") or "")
		and "POST /env/profiles/install" in str(e.get("message") or "")
		for e in events
	)
	assert any(
		e.get("type") == "node_finished"
		and str(e.get("nodeId") or "") == "tool_python"
		and str(e.get("errorCode") or "") == "ENV_PROFILE_MISSING"
		for e in events
	)
	assert any(e.get("type") == "run_finished" and e.get("status") == "failed" for e in events)


@pytest.mark.asyncio
async def test_run_preflight_allows_when_builtin_profile_is_ready(monkeypatch):
	run_mod = importlib.import_module("app.runner.run")
	store = MemoryArtifactStore()
	cache = ExecutionCache()

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "text", "payload": "ok", "meta": {"status": "ok"}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	monkeypatch.setattr(run_mod, "missing_packages_for_packages", lambda _packages: [])

	events = []
	await run_mod.run_graph(
		run_id="run-env-profile-ready",
		graph=_python_tool_graph(),
		run_from=None,
		bus=RunEventBus("run-env-profile-ready", on_emit=lambda e: events.append(dict(e))),
		artifact_store=store,
		cache=cache,
		runtime_ref=SimpleNamespace(),
		graph_id="graph-env-profile-ready",
	)

	assert not any("ENV_PROFILE_MISSING" in str(e.get("message") or "") for e in events if e.get("type") == "log")
	assert any(
		e.get("type") == "node_finished"
		and str(e.get("nodeId") or "") == "tool_python"
		and str(e.get("status") or "") == "succeeded"
		for e in events
	)
	assert any(e.get("type") == "run_finished" and e.get("status") == "succeeded" for e in events)


@pytest.mark.asyncio
async def test_run_preflight_install_then_rerun_succeeds(monkeypatch):
	run_mod = importlib.import_module("app.runner.run")
	store = MemoryArtifactStore()
	cache = ExecutionCache()
	install_state = {"installed": False}
	exec_tool_called = {"count": 0}

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		exec_tool_called["count"] += 1
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "text", "payload": "ok", "meta": {"status": "ok"}},
		)

	def _missing_packages(_packages):
		return [] if install_state["installed"] else ["numpy"]

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	monkeypatch.setattr(run_mod, "missing_packages_for_packages", _missing_packages)

	first_events = []
	await run_mod.run_graph(
		run_id="run-env-profile-rerun-1",
		graph=_python_tool_graph(),
		run_from=None,
		bus=RunEventBus("run-env-profile-rerun-1", on_emit=lambda e: first_events.append(dict(e))),
		artifact_store=store,
		cache=cache,
		runtime_ref=SimpleNamespace(),
		graph_id="graph-env-profile-rerun",
	)
	assert exec_tool_called["count"] == 0
	assert any(
		e.get("type") == "node_finished"
		and str(e.get("nodeId") or "") == "tool_python"
		and str(e.get("errorCode") or "") == "ENV_PROFILE_MISSING"
		for e in first_events
	)

	# Simulate "Install profile" action completed between runs.
	install_state["installed"] = True
	second_events = []
	await run_mod.run_graph(
		run_id="run-env-profile-rerun-2",
		graph=_python_tool_graph(),
		run_from=None,
		bus=RunEventBus("run-env-profile-rerun-2", on_emit=lambda e: second_events.append(dict(e))),
		artifact_store=store,
		cache=cache,
		runtime_ref=SimpleNamespace(),
		graph_id="graph-env-profile-rerun",
	)
	assert exec_tool_called["count"] == 1
	assert any(
		e.get("type") == "node_finished"
		and str(e.get("nodeId") or "") == "tool_python"
		and str(e.get("status") or "") == "succeeded"
		for e in second_events
	)
	assert any(e.get("type") == "run_finished" and e.get("status") == "succeeded" for e in second_events)
