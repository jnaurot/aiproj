from __future__ import annotations

import asyncio

import pytest

from app.executors import llm as llm_exec
from app.runner.artifacts import MemoryArtifactStore, RunBindings
from app.runner.events import RunEventBus
from app.runner.metadata import GraphContext, NodeOutput


@pytest.mark.asyncio
async def test_runtime_env_cap_change_applies_without_process_restart(monkeypatch) -> None:
	llm_exec._reset_provider_lease_state_for_tests()
	monkeypatch.setenv("RUNNER_MAX_MODEL_PROVIDER_OLLAMA", "1")
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
		upstream_artifact_ids=None,
	):
		node_id = str(node.get("id") or "")
		if node_id == "n_model_1":
			first_started.set()
			await release_first.wait()
		elif node_id == "n_model_2":
			second_started.set()
		return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data=node_id)

	monkeypatch.setattr(llm_exec, "exec_llm_ollama", _fake_exec_ollama)
	bus = RunEventBus("run-model-dynamic-cap", on_emit=lambda evt: None)
	context = GraphContext(
		run_id="run-model-dynamic-cap",
		bus=bus,
		artifact_store=MemoryArtifactStore(),
		bindings=RunBindings("run-model-dynamic-cap", graph_id="graph-model-dynamic-cap"),
		graph_id="graph-model-dynamic-cap",
	)

	def _node(node_id: str) -> dict:
		return {
			"id": node_id,
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

	task_one = asyncio.create_task(llm_exec.exec_llm("run-model-dynamic-cap", _node("n_model_1"), context, upstream_artifact_ids=[]))
	await asyncio.wait_for(first_started.wait(), timeout=2.0)
	monkeypatch.setenv("RUNNER_MAX_MODEL_PROVIDER_OLLAMA", "2")
	task_two = asyncio.create_task(llm_exec.exec_llm("run-model-dynamic-cap", _node("n_model_2"), context, upstream_artifact_ids=[]))
	await asyncio.wait_for(second_started.wait(), timeout=2.0)
	release_first.set()
	out_one, out_two = await asyncio.gather(task_one, task_two)
	assert out_one.status == "succeeded"
	assert out_two.status == "succeeded"
