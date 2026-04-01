from __future__ import annotations

import json

import pytest

from app.executors import llm as llm_exec
from app.runner.artifacts import MemoryArtifactStore, RunBindings
from app.runner.events import RunEventBus
from app.runner.metadata import GraphContext, NodeOutput


def _context(run_id: str) -> GraphContext:
	return GraphContext(
		run_id=run_id,
		bus=RunEventBus(run_id, on_emit=lambda evt: None),
		artifact_store=MemoryArtifactStore(),
		bindings=RunBindings(run_id, graph_id=f"graph-{run_id}"),
		graph_id=f"graph-{run_id}",
	)


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["ollama", "openai_compat"])
async def test_model_prompt_only_allowed_executes_without_upstream_artifacts(monkeypatch, provider: str):
	called = {"count": 0}

	async def _fake_exec(
		run_id,
		node,
		context,
		input_metadata,
		params,
		input_text=None,
		input_items=None,
		input_media=None,
		template_values=None,
		upstream_artifact_ids=None,
	):
		called["count"] += 1
		return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data="ok")

	if provider == "ollama":
		monkeypatch.setattr(llm_exec, "exec_llm_ollama", _fake_exec)
	else:
		monkeypatch.setattr(llm_exec, "exec_llm_openai_compat", _fake_exec)
	params = {
		"model": "demo-model",
		"user_prompt": "Say hi",
		"output_mode": "text",
		"allow_prompt_only_model_execution": True,
	}
	if provider == "ollama":
		params["base_url"] = "http://127.0.0.1:11434"
	else:
		params["connection_ref"] = "OPENAI_API_KEY"
	node = {
		"id": "n_prompt_only",
		"data": {
			"kind": "model",
			"llmKind": provider,
			"modelKind": "llm",
			"params": params,
		},
	}
	out = await llm_exec.exec_llm(f"run-prompt-only-{provider}", node, _context(f"run-prompt-only-{provider}"), upstream_artifact_ids=[])
	assert out.status == "succeeded"
	assert called["count"] == 1


@pytest.mark.asyncio
async def test_model_prompt_only_disallowed_fails_structured():
	node = {
		"id": "n_prompt_only_disallowed",
		"data": {
			"kind": "model",
			"llmKind": "ollama",
			"modelKind": "llm",
			"params": {
				"base_url": "http://127.0.0.1:11434",
				"model": "demo-model",
				"user_prompt": "Say hi",
				"output_mode": "text",
				"allow_prompt_only_model_execution": False,
			},
		},
	}
	out = await llm_exec.exec_llm(
		"run-prompt-only-disallowed",
		node,
		_context("run-prompt-only-disallowed"),
		upstream_artifact_ids=[],
	)
	assert out.status == "failed"
	payload = json.loads(str(out.error or "{}"))
	assert payload.get("errorCode") == "MODEL_MISSING_UPSTREAM_INPUT"
