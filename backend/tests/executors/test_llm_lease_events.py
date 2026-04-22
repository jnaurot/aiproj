from __future__ import annotations

import asyncio

import pytest

from app.executors import llm as llm_exec
from app.runner.artifacts import MemoryArtifactStore, RunBindings
from app.runner.events import RunEventBus
from app.runner.metadata import GraphContext, NodeOutput


def _model_node(node_id: str) -> dict:
	return {
		"id": node_id,
		"data": {
			"kind": "model",
			"llmKind": "ollama",
			"modelKind": "llm",
			"params": {
				"model": "glm-4.7-flash:latest",
				"base_url": "http://127.0.0.1:11434",
				"user_prompt": f"hello from {node_id}",
				"output_mode": "text",
				"allow_prompt_only_model_execution": True,
			},
		},
	}


@pytest.mark.asyncio
async def test_exec_llm_emits_llm_lease_waiting_acquired_released(monkeypatch) -> None:
	events: list[dict] = []
	bus = RunEventBus("run-llm-lease-001", on_emit=lambda evt: events.append(dict(evt)))
	context = GraphContext(
		run_id="run-llm-lease-001",
		bus=bus,
		artifact_store=MemoryArtifactStore(),
		bindings=RunBindings("run-llm-lease-001", graph_id="graph-llm-lease-001"),
		graph_id="graph-llm-lease-001",
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

	monkeypatch.setenv("RUNNER_MAX_MODEL_PROVIDER_OLLAMA", "1")
	llm_exec._reset_provider_lease_state_for_tests()
	monkeypatch.setattr(llm_exec, "exec_llm_ollama", _fake_exec_ollama)

	node = {
		"id": "n_model_lease",
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
	out = await llm_exec.exec_llm("run-llm-lease-001", node, context, upstream_artifact_ids=[])
	assert out.status == "succeeded"
	lease_events = [evt for evt in events if str(evt.get("type") or "") == "llm_lease"]
	assert len(lease_events) >= 3
	states = [str(evt.get("state") or "") for evt in lease_events]
	assert states[0] == "waiting"
	assert "acquired" in states
	assert states[-1] == "released"


@pytest.mark.asyncio
async def test_exec_llm_release_handoff_keeps_other_holder_visible(monkeypatch) -> None:
	events: list[dict] = []
	bus = RunEventBus("run-llm-lease-handoff", on_emit=lambda evt: events.append(dict(evt)))
	context = GraphContext(
		run_id="run-llm-lease-handoff",
		bus=bus,
		artifact_store=MemoryArtifactStore(),
		bindings=RunBindings("run-llm-lease-handoff", graph_id="graph-llm-lease-handoff"),
		graph_id="graph-llm-lease-handoff",
	)

	first_started = asyncio.Event()
	release_first = asyncio.Event()
	second_started = asyncio.Event()

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
		node_id = str(node.get("id") or "")
		if node_id == "n_model_1":
			first_started.set()
			await release_first.wait()
		if node_id == "n_model_2":
			second_started.set()
			await asyncio.sleep(0.05)
		return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data=node_id)

	monkeypatch.setenv("RUNNER_MAX_MODEL_PROVIDER_OLLAMA", "2")
	llm_exec._reset_provider_lease_state_for_tests()
	monkeypatch.setattr(llm_exec, "exec_llm_ollama", _fake_exec_ollama)

	task_1 = asyncio.create_task(
		llm_exec.exec_llm("run-llm-lease-handoff", _model_node("n_model_1"), context, upstream_artifact_ids=[])
	)
	await asyncio.wait_for(first_started.wait(), timeout=2.0)
	task_2 = asyncio.create_task(
		llm_exec.exec_llm("run-llm-lease-handoff", _model_node("n_model_2"), context, upstream_artifact_ids=[])
	)
	await asyncio.wait_for(second_started.wait(), timeout=2.0)
	release_first.set()
	out_1, out_2 = await asyncio.gather(task_1, task_2)
	assert out_1.status == "succeeded"
	assert out_2.status == "succeeded"

	lease_events = [evt for evt in events if str(evt.get("type") or "") == "llm_lease"]
	assert lease_events
	release_1_events = [
		evt
		for evt in lease_events
		if str(evt.get("state") or "") == "released" and str(evt.get("nodeId") or "") == "n_model_1"
	]
	assert release_1_events
	release_1 = release_1_events[-1]
	assert str(release_1.get("holderNodeId") or "") == "n_model_2"
