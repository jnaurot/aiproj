from __future__ import annotations

import importlib
import sys
import types

import pytest

from app.executors import llm as llm_exec
from app.runner.artifacts import MemoryArtifactStore
from app.runner.cache import ExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import NodeOutput


def _ensure_duckdb_stub() -> None:
	if "duckdb" not in sys.modules:
		sys.modules["duckdb"] = types.SimpleNamespace()


@pytest.mark.asyncio
async def test_model_node_started_emits_only_after_llm_lease_acquired(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_ollama(
		run_id,
		node,
		context,
		_ignored,
		params_override,
		input_text=None,
		input_items=None,
		input_media=None,
		template_values=None,
		upstream_artifact_ids=None,
	):
		return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data="ok")

	monkeypatch.setenv("RUNNER_MAX_MODEL_PROVIDER_OLLAMA", "1")
	llm_exec._reset_provider_lease_state_for_tests()
	monkeypatch.setattr(llm_exec, "exec_llm_ollama", _fake_exec_ollama)

	graph = {
		"nodes": [
			{
				"id": "n_model",
				"data": {
					"kind": "model",
					"llmKind": "ollama",
					"modelKind": "llm",
					"params": {
						"model": "glm-4.7-flash:latest",
						"base_url": "http://127.0.0.1:11434",
						"user_prompt": "hello",
						"output_mode": "text",
						"allow_prompt_only_model_execution": True,
					},
				},
			}
		],
		"edges": [],
	}
	events: list[dict] = []
	bus = RunEventBus("run-exec-policy-lease", on_emit=lambda evt: events.append(dict(evt)))
	await run_mod.run_graph(
		run_id="run-exec-policy-lease",
		graph=graph,
		run_from=None,
		bus=bus,
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-exec-policy-lease",
	)

	def _idx(predicate):
		for i, evt in enumerate(events):
			if predicate(evt):
				return i
		return -1

	acquired_idx = _idx(
		lambda evt: str(evt.get("type") or "") == "llm_lease"
		and str(evt.get("state") or "") == "acquired"
		and str(evt.get("nodeId") or "") == "n_model"
	)
	started_idx = _idx(
		lambda evt: str(evt.get("type") or "") == "node_started"
		and str(evt.get("nodeId") or "") == "n_model"
	)
	assert acquired_idx >= 0
	assert started_idx >= 0
	assert started_idx > acquired_idx


@pytest.mark.asyncio
async def test_node_started_emits_exactly_once_for_non_lease_and_lease_nodes(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}},
		)

	async def _fake_exec_ollama(
		run_id,
		node,
		context,
		_ignored,
		params_override,
		input_text=None,
		input_items=None,
		input_media=None,
		template_values=None,
		upstream_artifact_ids=None,
	):
		return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data="ok")

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	monkeypatch.setenv("RUNNER_MAX_MODEL_PROVIDER_OLLAMA", "1")
	llm_exec._reset_provider_lease_state_for_tests()
	monkeypatch.setattr(llm_exec, "exec_llm_ollama", _fake_exec_ollama)

	graph_non_lease = {
		"nodes": [
			{
				"id": "n_tool_up",
				"data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}},
			},
			{
				"id": "n_tool_down",
				"data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}},
			},
		],
		"edges": [
			{"id": "e_tool", "source": "n_tool_up", "target": "n_tool_down", "targetHandle": "in", "data": {"mode": "work"}},
		],
	}
	events_non_lease: list[dict] = []
	bus_non_lease = RunEventBus("run-exec-policy-start-once-non-lease", on_emit=lambda evt: events_non_lease.append(dict(evt)))
	await run_mod.run_graph(
		run_id="run-exec-policy-start-once-non-lease",
		graph=graph_non_lease,
		run_from=None,
		bus=bus_non_lease,
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-exec-policy-start-once-non-lease",
	)
	started_tool_up = [
		evt
		for evt in events_non_lease
		if str(evt.get("type") or "") == "node_started" and str(evt.get("nodeId") or "") == "n_tool_up"
	]
	assert len(started_tool_up) == 1

	graph_lease = {
		"nodes": [
			{
				"id": "n_model",
				"data": {
					"kind": "model",
					"llmKind": "ollama",
					"modelKind": "llm",
					"params": {
						"model": "glm-4.7-flash:latest",
						"base_url": "http://127.0.0.1:11434",
						"user_prompt": "hello",
						"output_mode": "text",
						"allow_prompt_only_model_execution": True,
					},
				},
			},
		],
		"edges": [],
	}
	events_lease: list[dict] = []
	bus = RunEventBus("run-exec-policy-start-once", on_emit=lambda evt: events_lease.append(dict(evt)))
	await run_mod.run_graph(
		run_id="run-exec-policy-start-once",
		graph=graph_lease,
		run_from=None,
		bus=bus,
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-exec-policy-start-once",
	)

	started_model = [
		evt for evt in events_lease if str(evt.get("type") or "") == "node_started" and str(evt.get("nodeId") or "") == "n_model"
	]
	assert len(started_model) == 1


@pytest.mark.asyncio
async def test_model_timeout_before_lease_has_no_node_started_or_active_work_edges(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	monkeypatch.setenv("RUNNER_MAX_MODEL_PROVIDER_OLLAMA", "1")
	monkeypatch.setenv("RUNNER_MODEL_PROVIDER_ACQUIRE_TIMEOUT_OLLAMA", "0.01")
	llm_exec._reset_provider_lease_state_for_tests()
	sem = llm_exec._provider_semaphore("ollama")
	assert sem is not None
	# Saturate provider so model times out before acquiring a lease.
	await sem.acquire()
	try:
		graph = {
			"nodes": [
				{
					"id": "n_model",
					"data": {
						"kind": "model",
						"llmKind": "ollama",
						"modelKind": "llm",
						"params": {
							"model": "glm-4.7-flash:latest",
							"base_url": "http://127.0.0.1:11434",
							"user_prompt": "hello",
							"output_mode": "text",
							"allow_prompt_only_model_execution": True,
						},
					},
				},
			],
			"edges": [],
		}
		events: list[dict] = []
		bus = RunEventBus("run-exec-policy-timeout-before-lease", on_emit=lambda evt: events.append(dict(evt)))
		await run_mod.run_graph(
			run_id="run-exec-policy-timeout-before-lease",
			graph=graph,
			run_from=None,
			bus=bus,
			artifact_store=MemoryArtifactStore(),
			cache=ExecutionCache(),
			graph_id="graph-exec-policy-timeout-before-lease",
		)
	finally:
		sem.release()

	started_model = [
		evt for evt in events if str(evt.get("type") or "") == "node_started" and str(evt.get("nodeId") or "") == "n_model"
	]
	model_active_edges = [
		evt
		for evt in events
		if str(evt.get("type") or "") == "edge_exec"
		and str(evt.get("exec") or "") == "active"
	]
	model_finished = [
		evt
		for evt in events
		if str(evt.get("type") or "") == "node_finished"
		and str(evt.get("nodeId") or "") == "n_model"
	]
	assert started_model == []
	assert model_active_edges == []
	assert model_finished and str(model_finished[-1].get("status") or "") in {"failed", "stale"}


@pytest.mark.asyncio
async def test_edge_exec_active_and_done_are_work_plane_only(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node["id"])
		if node_id == "src_work":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "text", "payload": "work"},
			)
		if node_id == "src_param":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": {"cfg": 1}},
			)
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

	graph = {
		"nodes": [
			{"id": "src_work", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "src_param", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "dst", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
		],
		"edges": [
			{"id": "e_work", "source": "src_work", "target": "dst", "targetHandle": "in", "data": {"mode": "work"}},
			{
				"id": "e_param",
				"source": "src_param",
				"target": "dst",
				"targetHandle": "param_config",
				"data": {"mode": "param"},
			},
		],
	}
	events: list[dict] = []
	bus = RunEventBus("run-exec-policy-work-plane", on_emit=lambda evt: events.append(dict(evt)))
	await run_mod.run_graph(
		run_id="run-exec-policy-work-plane",
		graph=graph,
		run_from=None,
		bus=bus,
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-exec-policy-work-plane",
	)

	param_edge_exec_events = [
		evt
		for evt in events
		if str(evt.get("type") or "") == "edge_exec"
		and str(evt.get("edgeId") or "") == "e_param"
		and str(evt.get("exec") or "") in {"active", "done"}
	]
	work_edge_exec_events = [
		evt
		for evt in events
		if str(evt.get("type") or "") == "edge_exec"
		and str(evt.get("edgeId") or "") == "e_work"
		and str(evt.get("exec") or "") in {"active", "done"}
	]
	assert work_edge_exec_events
	assert param_edge_exec_events == []
