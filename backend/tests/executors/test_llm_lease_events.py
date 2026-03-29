from __future__ import annotations

import pytest

from app.executors import llm as llm_exec
from app.runner.artifacts import MemoryArtifactStore, RunBindings
from app.runner.events import RunEventBus
from app.runner.metadata import GraphContext, NodeOutput


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
		upstream_artifact_ids=None,
	):
		return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data="ok")

	monkeypatch.setenv("RUNNER_MAX_MODEL_PROVIDER_OLLAMA", "1")
	llm_exec._MODEL_PROVIDER_SEMAPHORES.clear()
	llm_exec._MODEL_PROVIDER_WAITERS.clear()
	llm_exec._MODEL_PROVIDER_HOLDERS.clear()
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
