from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from app.executors import llm as llm_exec
from app.runner.artifacts import MemoryArtifactStore, RunBindings
from app.runner.events import RunEventBus
from app.runner.metadata import FileMetadata, GraphContext, NodeOutput


def _context(run_id: str) -> GraphContext:
	return GraphContext(
		run_id=run_id,
		bus=RunEventBus(run_id, on_emit=lambda evt: None),
		artifact_store=MemoryArtifactStore(),
		bindings=RunBindings(run_id, graph_id=f"graph-{run_id}"),
		graph_id=f"graph-{run_id}",
	)


def _base_node(params: dict) -> dict:
	return {
		"id": "n_model_mapping",
		"data": {
			"kind": "model",
			"llmKind": "ollama",
			"modelKind": "llm",
			"params": params,
		},
	}


@pytest.mark.asyncio
async def test_model_input_mapping_resolves_work_and_param_values(monkeypatch):
	captured: dict = {}

	async def _fake_exec_ollama(
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
		captured["template_values"] = dict(template_values or {})
		return NodeOutput(
			status="succeeded",
			metadata=FileMetadata(
				file_path="memory://model-output",
				file_type="json",
				mime_type="application/json",
				content_hash="x" * 64,
				created_at=datetime.now(timezone.utc),
			),
			execution_time_ms=1.0,
			data="ok",
		)

	monkeypatch.setattr(llm_exec, "exec_llm_ollama", _fake_exec_ollama)
	params = {
		"model": "glm-4.7-flash:latest",
		"base_url": "http://127.0.0.1:11434",
		"user_prompt": "job={job_json} context={resume_context}",
		"output_mode": "text",
		"input_mapping": {
			"job_json": "in",
			"resume_context": "param_context",
		},
		"_input_mapping_values": {
			"param_context": "resume text here",
		},
		"_work_item": {
			"itemMode": "json_items",
			"itemIndex": 0,
			"itemPreview": {"id": "job_1", "title": "Engineer"},
		},
	}
	out = await llm_exec.exec_llm(
		"run-model-mapping-ok",
		_base_node(params),
		_context("run-model-mapping-ok"),
		upstream_artifact_ids=[],
	)
	assert out.status == "succeeded"
	values = captured.get("template_values") or {}
	assert values.get("job_json")
	assert values.get("resume_context") == "resume text here"
	obs = dict((out.metadata.observability or {}) if out.metadata is not None else {})
	assert isinstance(obs.get("inputMapping"), dict)
	assert "job_json" in list((obs.get("inputMapping") or {}).get("resolvedKeys") or [])


@pytest.mark.asyncio
async def test_model_input_mapping_missing_key_fails_structured(monkeypatch):
	async def _fake_exec_ollama(
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
		return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data="ok")

	monkeypatch.setattr(llm_exec, "exec_llm_ollama", _fake_exec_ollama)
	params = {
		"model": "glm-4.7-flash:latest",
		"base_url": "http://127.0.0.1:11434",
		"user_prompt": "context={resume_context}",
		"output_mode": "text",
		"input_mapping": {"resume_context": "param_context"},
		"_work_item": {
			"itemMode": "json_items",
			"itemIndex": 0,
			"itemPreview": {"id": "job_1", "title": "Engineer"},
		},
	}
	out = await llm_exec.exec_llm(
		"run-model-mapping-missing",
		_base_node(params),
		_context("run-model-mapping-missing"),
		upstream_artifact_ids=[],
	)
	assert out.status == "failed"
	payload = json.loads(str(out.error or "{}"))
	assert payload.get("errorCode") == "MODEL_INPUT_MAPPING_MISSING"
	assert isinstance(payload.get("missing"), list)


@pytest.mark.asyncio
async def test_model_work_item_artifact_mode_uses_queued_artifact_not_upstream_binding(monkeypatch):
	captured: dict = {}

	async def _fake_exec_ollama(
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
		captured["input_text"] = input_text
		captured["input_items"] = list(input_items or [])
		return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data="ok")

	async def _fake_serialize_artifact_input(context, artifact_id, input_encoding):
		return f"from:{artifact_id}:{input_encoding}"

	monkeypatch.setattr(llm_exec, "exec_llm_ollama", _fake_exec_ollama)
	monkeypatch.setattr(llm_exec, "_serialize_artifact_input", _fake_serialize_artifact_input)

	params = {
		"model": "glm-4.7-flash:latest",
		"base_url": "http://127.0.0.1:11434",
		"user_prompt": "Use {input}",
		"output_mode": "text",
		"input_mapping": {"input": "in"},
		"_work_item": {
			"itemMode": "artifact",
			"itemIndex": 0,
			"artifactId": "artifact_from_queue",
			"itemPreview": None,
		},
	}
	out = await llm_exec.exec_llm(
		"run-model-artifact-work-item",
		_base_node(params),
		_context("run-model-artifact-work-item"),
		upstream_artifact_ids=["artifact_from_mutable_binding"],
	)
	assert out.status == "succeeded"
	assert captured.get("input_text") == "from:artifact_from_queue:text"
	assert captured.get("input_items") == ["from:artifact_from_queue:text"]


@pytest.mark.asyncio
async def test_model_work_item_artifact_mode_multimodal_uses_queued_artifact_for_media(monkeypatch):
	captured: dict = {}

	async def _fake_exec_ollama(
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
		captured["input_text"] = input_text
		captured["input_media"] = list(input_media or [])
		return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data="ok")

	async def _fake_serialize_artifact_input(context, artifact_id, input_encoding):
		return f"text:{artifact_id}"

	async def _fake_serialize_image_media(context, artifact_id):
		return {"type": "image", "artifactId": artifact_id}

	monkeypatch.setattr(llm_exec, "exec_llm_ollama", _fake_exec_ollama)
	monkeypatch.setattr(llm_exec, "_serialize_artifact_input", _fake_serialize_artifact_input)
	monkeypatch.setattr(llm_exec, "_serialize_image_media", _fake_serialize_image_media)

	params = {
		"model": "glm-4.7-flash:latest",
		"base_url": "http://127.0.0.1:11434",
		"user_prompt": "Use {input}",
		"output_mode": "text",
		"_work_item": {
			"itemMode": "artifact",
			"itemIndex": 0,
			"artifactId": "artifact_from_queue",
			"itemPreview": None,
		},
	}
	node = _base_node(params)
	node["data"]["modelKind"] = "vision"
	out = await llm_exec.exec_llm(
		"run-model-artifact-work-item-vision",
		node,
		_context("run-model-artifact-work-item-vision"),
		upstream_artifact_ids=["artifact_from_mutable_binding"],
	)
	assert out.status == "succeeded"
	assert captured.get("input_text") == "text:artifact_from_queue"
	input_media = captured.get("input_media") or []
	assert input_media
	assert input_media[0].get("artifactId") == "artifact_from_queue"
