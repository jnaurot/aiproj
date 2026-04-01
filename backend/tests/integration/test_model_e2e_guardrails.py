from __future__ import annotations

import asyncio
import importlib
import json
from datetime import datetime, timezone

import pytest

from app.executors import llm as llm_exec
from app.runner.artifacts import DiskArtifactStore, MemoryArtifactStore, RunBindings
from app.runner.cache import ExecutionCache, SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import FileMetadata, GraphContext, NodeOutput


def _model_node(node_id: str, overrides: dict | None = None) -> dict:
	params = {
		"model": "glm-4.7-flash:latest",
		"base_url": "http://127.0.0.1:11434",
		"user_prompt": "hello {input}",
		"output_mode": "text",
		"allow_prompt_only_model_execution": True,
	}
	if isinstance(overrides, dict):
		params.update(overrides)
	return {
		"id": node_id,
		"data": {
			"kind": "model",
			"llmKind": "ollama",
			"modelKind": "llm",
			"params": params,
		},
	}


def _source_node(node_id: str) -> dict:
	return {
		"id": node_id,
		"data": {
			"kind": "source",
			"sourceKind": "file",
			"params": {"rel_path": ".", "filename": f"{node_id}.txt", "file_format": "txt"},
		},
	}


def _json_metadata(node_id: str) -> FileMetadata:
	return FileMetadata(
		file_path=f"memory://{node_id}.json",
		file_type="json",
		mime_type="application/json",
		content_hash="a" * 64,
		created_at=datetime.now(timezone.utc),
		payload_type="json",
		data_schema={"type": "json", "fields": []},
	)


def _context(run_id: str) -> GraphContext:
	return GraphContext(
		run_id=run_id,
		bus=RunEventBus(run_id, on_emit=lambda evt: None),
		artifact_store=MemoryArtifactStore(),
		bindings=RunBindings(run_id, graph_id=f"graph-{run_id}"),
		graph_id=f"graph-{run_id}",
	)


@pytest.mark.asyncio
async def test_e2e_model_json_strict_happy_path(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			data="job body",
			metadata=FileMetadata(
				file_path="memory://source.txt",
				file_type="txt",
				mime_type="text/plain; charset=utf-8",
				content_hash="b" * 64,
				created_at=datetime.now(timezone.utc),
				payload_type="text",
				data_schema={"type": "text"},
			),
			execution_time_ms=1.0,
		)

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
		return NodeOutput(
			status="succeeded",
			metadata=_json_metadata(str(node.get("id") or "model")),
			execution_time_ms=1.0,
			data={"ok": True},
		)

	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	monkeypatch.setattr(llm_exec, "exec_llm_ollama", _fake_exec_ollama)
	llm_exec._reset_provider_lease_state_for_tests()
	monkeypatch.setenv("RUNNER_MAX_MODEL_PROVIDER_OLLAMA", "1")

	graph = {
		"nodes": [
			_source_node("n_source"),
			_model_node("n_model", {"output_mode": "json", "output_schema": {"type": "object"}}),
		],
		"edges": [{"id": "e1", "source": "n_source", "target": "n_model", "targetHandle": "in"}],
	}
	graph["nodes"][1]["data"]["schema"] = {
		"expectedSchema": {
			"typedSchema": {"type": "json", "fields": []},
			"source": "declared",
			"state": "fresh",
		}
	}
	events: list[dict] = []
	artifact_root = tmp_path / "artifacts-model-json-happy"
	await run_mod.run_graph(
		run_id="run-model-json-happy",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-model-json-happy", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=DiskArtifactStore(artifact_root),
		cache=SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite")),
		graph_id="graph-model-json-happy",
	)

	finished = [e for e in events if e.get("type") == "node_finished" and e.get("nodeId") == "n_model"]
	acquired = [e for e in events if e.get("type") == "llm_lease" and e.get("state") == "acquired" and e.get("nodeId") == "n_model"]
	released = [e for e in events if e.get("type") == "llm_lease" and e.get("state") == "released" and e.get("nodeId") == "n_model"]
	assert finished and finished[-1].get("status") == "succeeded", events
	assert acquired and released


@pytest.mark.asyncio
async def test_e2e_model_provider_fifo_contention(monkeypatch):
	llm_exec._reset_provider_lease_state_for_tests()
	monkeypatch.setenv("RUNNER_MAX_MODEL_PROVIDER_OLLAMA", "1")
	first_started = asyncio.Event()
	release_first = asyncio.Event()
	second_started = asyncio.Event()

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
		node_id = str(node.get("id") or "")
		if node_id == "n_model_1":
			first_started.set()
			await release_first.wait()
		if node_id == "n_model_2":
			second_started.set()
		return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data=node_id)

	monkeypatch.setattr(llm_exec, "exec_llm_ollama", _fake_exec_ollama)
	events: list[dict] = []
	context = GraphContext(
		run_id="run-model-fifo",
		bus=RunEventBus("run-model-fifo", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		bindings=RunBindings("run-model-fifo", graph_id="graph-model-fifo"),
		graph_id="graph-model-fifo",
	)

	task_1 = asyncio.create_task(llm_exec.exec_llm("run-model-fifo", _model_node("n_model_1"), context, upstream_artifact_ids=[]))
	await asyncio.wait_for(first_started.wait(), timeout=2.0)
	task_2 = asyncio.create_task(llm_exec.exec_llm("run-model-fifo", _model_node("n_model_2"), context, upstream_artifact_ids=[]))
	await asyncio.sleep(0.05)
	waiting_2 = [e for e in events if e.get("type") == "llm_lease" and e.get("state") == "waiting" and e.get("nodeId") == "n_model_2"]
	assert waiting_2
	release_first.set()
	out_1, out_2 = await asyncio.gather(task_1, task_2)
	assert out_1.status == "succeeded"
	assert out_2.status == "succeeded"
	assert second_started.is_set()


@pytest.mark.asyncio
async def test_e2e_model_dynamic_cap_change(monkeypatch):
	llm_exec._reset_provider_lease_state_for_tests()
	monkeypatch.setenv("RUNNER_MAX_MODEL_PROVIDER_OLLAMA", "1")
	first_started = asyncio.Event()
	release_first = asyncio.Event()
	second_started = asyncio.Event()

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
		node_id = str(node.get("id") or "")
		if node_id == "n_model_1":
			first_started.set()
			await release_first.wait()
		if node_id == "n_model_2":
			second_started.set()
		return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data=node_id)

	monkeypatch.setattr(llm_exec, "exec_llm_ollama", _fake_exec_ollama)
	context = _context("run-model-dynamic-cap-e2e")
	task_1 = asyncio.create_task(llm_exec.exec_llm("run-model-dynamic-cap-e2e", _model_node("n_model_1"), context, upstream_artifact_ids=[]))
	await asyncio.wait_for(first_started.wait(), timeout=2.0)
	monkeypatch.setenv("RUNNER_MAX_MODEL_PROVIDER_OLLAMA", "2")
	task_2 = asyncio.create_task(llm_exec.exec_llm("run-model-dynamic-cap-e2e", _model_node("n_model_2"), context, upstream_artifact_ids=[]))
	await asyncio.wait_for(second_started.wait(), timeout=2.0)
	release_first.set()
	out_1, out_2 = await asyncio.gather(task_1, task_2)
	assert out_1.status == "succeeded"
	assert out_2.status == "succeeded"


@pytest.mark.asyncio
async def test_e2e_pause_resume_model_waiting_and_acquired_paths(monkeypatch):
	run_mod = importlib.import_module("app.runner.run")
	llm_exec._reset_provider_lease_state_for_tests()
	monkeypatch.setenv("RUNNER_MAX_MODEL_PROVIDER_OLLAMA", "1")
	acquired_1 = asyncio.Event()
	release_1 = asyncio.Event()
	second_started = asyncio.Event()

	async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			data=f"seed:{str(node.get('id') or '')}",
			metadata=FileMetadata(
				file_path=f"memory://{str(node.get('id') or '')}.txt",
				file_type="txt",
				mime_type="text/plain; charset=utf-8",
				content_hash="d" * 64,
				created_at=datetime.now(timezone.utc),
				payload_type="text",
				data_schema={"type": "text"},
			),
			execution_time_ms=1.0,
		)

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
		node_id = str(node.get("id") or "")
		if node_id == "n_model_1":
			acquired_1.set()
			await release_1.wait()
		if node_id == "n_model_2":
			second_started.set()
		return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data=node_id)

	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	monkeypatch.setattr(llm_exec, "exec_llm_ollama", _fake_exec_ollama)
	graph = {
		"nodes": [
			_source_node("n_source_1"),
			_source_node("n_source_2"),
			_model_node("n_model_1"),
			_model_node("n_model_2"),
		],
		"edges": [
			{"id": "e_s1", "source": "n_source_1", "target": "n_model_1", "targetHandle": "in"},
			{"id": "e_s2", "source": "n_source_2", "target": "n_model_2", "targetHandle": "in"},
		],
	}
	pause_event = asyncio.Event()
	events_pause: list[dict] = []
	task = asyncio.create_task(
		run_mod.run_graph(
			run_id="run-model-pause-resume-wait-acquire",
			graph=graph,
			run_from=None,
			bus=RunEventBus("run-model-pause-resume-wait-acquire", on_emit=lambda evt: events_pause.append(dict(evt))),
			artifact_store=MemoryArtifactStore(),
			cache=ExecutionCache(),
			graph_id="graph-model-pause-resume-wait-acquire",
			pause_event=pause_event,
		)
	)
	await asyncio.wait_for(acquired_1.wait(), timeout=2.0)
	await asyncio.sleep(0.05)
	pause_event.set()
	release_1.set()
	await asyncio.wait_for(task, timeout=5.0)
	paused_evt = next((evt for evt in events_pause if str(evt.get("type") or "") == "run_paused"), None)
	assert isinstance(paused_evt, dict)
	snapshot = paused_evt.get("snapshot")
	assert isinstance(snapshot, dict)

	events_resume: list[dict] = []
	await run_mod.run_graph(
		run_id="run-model-pause-resume-wait-acquire",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-model-pause-resume-wait-acquire", on_emit=lambda evt: events_resume.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-model-pause-resume-wait-acquire",
		resume_snapshot=snapshot,
	)
	assert any(str(evt.get("type") or "") == "run_resumed" for evt in events_resume)
	assert any(str(evt.get("type") or "") == "run_finished" and str(evt.get("status") or "") == "succeeded" for evt in events_resume)
	assert second_started.is_set()


@pytest.mark.asyncio
async def test_e2e_model_input_mapping_mixed_handles(monkeypatch):
	run_mod = importlib.import_module("app.runner.run")
	captured_values: list[dict[str, str]] = []

	async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		if node_id == "n_source_job":
			data = '{"id":"job-1","title":"Engineer"}'
		else:
			data = "resume-body"
		return NodeOutput(
			status="succeeded",
			data=data,
			metadata=FileMetadata(
				file_path=f"memory://{node_id}.txt",
				file_type="txt",
				mime_type="text/plain; charset=utf-8",
				content_hash="c" * 64,
				created_at=datetime.now(timezone.utc),
				payload_type="text",
				data_schema={"type": "text"},
			),
			execution_time_ms=1.0,
		)

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
		captured_values.append(dict(template_values or {}))
		return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data="ok")

	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	monkeypatch.setattr(llm_exec, "exec_llm_ollama", _fake_exec_ollama)
	graph = {
		"nodes": [
			_source_node("n_source_job"),
			_source_node("n_source_resume"),
			_model_node(
				"n_model",
				{
					"user_prompt": "job={job_json} context={resume_context}",
					"input_mapping": {"job_json": "in", "resume_context": "param_context"},
				},
			),
		],
		"edges": [
			{"id": "e_job", "source": "n_source_job", "target": "n_model", "targetHandle": "in", "data": {"mode": "work"}},
			{
				"id": "e_resume",
				"source": "n_source_resume",
				"target": "n_model",
				"targetHandle": "param_context",
				"data": {"mode": "param"},
			},
		],
	}
	events: list[dict] = []
	await run_mod.run_graph(
		run_id="run-model-mapping-mixed-e2e",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-model-mapping-mixed-e2e", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-model-mapping-mixed-e2e",
	)
	assert captured_values, events
	assert captured_values[0].get("job_json")
	assert captured_values[0].get("resume_context") == "resume-body"
	finished = [evt for evt in events if evt.get("type") == "node_finished" and evt.get("nodeId") == "n_model"]
	assert finished and finished[-1].get("status") == "succeeded"


@pytest.mark.asyncio
async def test_regression_node_started_post_lease_acquire_only(monkeypatch):
	run_mod = importlib.import_module("app.runner.run")
	llm_exec._reset_provider_lease_state_for_tests()
	monkeypatch.setenv("RUNNER_MAX_MODEL_PROVIDER_OLLAMA", "1")

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
	events: list[dict] = []
	await run_mod.run_graph(
		run_id="run-model-reg-started-order",
		graph={"nodes": [_model_node("n_model")], "edges": []},
		run_from=None,
		bus=RunEventBus("run-model-reg-started-order", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-model-reg-started-order",
	)
	acquired_idx = next(i for i, e in enumerate(events) if e.get("type") == "llm_lease" and e.get("state") == "acquired")
	started_idx = next(i for i, e in enumerate(events) if e.get("type") == "node_started")
	assert started_idx > acquired_idx


@pytest.mark.asyncio
async def test_regression_model_error_not_flattened(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")
	artifact_root = tmp_path / "artifacts-model-reg-error"

	async def _fake_exec_model(run_id, node, context, upstream_artifact_ids=None, on_execution_started=None):
		if callable(on_execution_started):
			await on_execution_started()
		return NodeOutput(
			status="failed",
			metadata=None,
			execution_time_ms=1.0,
			error=json.dumps({"code": "MODEL_FAIL", "errorCode": "MODEL_FAIL", "message": "nope", "details": {"k": "v"}}),
		)

	monkeypatch.setattr(run_mod, "exec_llm", _fake_exec_model)
	events: list[dict] = []
	await run_mod.run_graph(
		run_id="run-model-reg-error",
		graph={"nodes": [_model_node("n_model")], "edges": []},
		run_from=None,
		bus=RunEventBus("run-model-reg-error", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=DiskArtifactStore(artifact_root),
		cache=SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite")),
		graph_id="graph-model-reg-error",
	)
	finished = [evt for evt in events if evt.get("type") == "node_finished" and evt.get("nodeId") == "n_model"]
	assert finished and finished[-1].get("status") == "failed"
	payload = json.loads(str(finished[-1].get("error") or "{}"))
	assert payload.get("errorCode") == "MODEL_FAIL"
	assert isinstance(payload.get("details"), dict)


def test_regression_output_mode_authoritative():
	node = {
		"id": "n",
		"data": {
			"kind": "model",
			"schema": {
				"expectedSchema": {
					"typedSchema": {"type": "embeddings", "fields": []},
					"source": "declared",
					"state": "fresh",
				}
			},
		},
	}
	norm = llm_exec.normalize_llm_params({"output_mode": "text", "output_schema": {"type": "object"}})
	assert llm_exec._resolve_llm_output_mode(node, norm) == "text"


def test_regression_provider_cap_reload_behavior(monkeypatch):
	llm_exec._reset_provider_lease_state_for_tests()
	monkeypatch.setenv("RUNNER_MAX_MODEL_PROVIDER_OLLAMA", "1")
	sem = llm_exec._provider_semaphore("ollama")
	assert sem is not None
	initial = int(getattr(sem, "_value", 0))
	monkeypatch.setenv("RUNNER_MAX_MODEL_PROVIDER_OLLAMA", "2")
	sem2 = llm_exec._provider_semaphore("ollama")
	assert sem2 is sem
	assert int(getattr(sem, "_value", 0)) >= initial


@pytest.mark.asyncio
async def test_regression_input_mapping_execution_path(monkeypatch):
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
		return NodeOutput(status="succeeded", metadata=_json_metadata("n_model"), execution_time_ms=1.0, data="ok")

	monkeypatch.setattr(llm_exec, "exec_llm_ollama", _fake_exec_ollama)
	params = {
		"model": "glm-4.7-flash:latest",
		"base_url": "http://127.0.0.1:11434",
		"user_prompt": "job={job_json} context={resume_context}",
		"output_mode": "text",
		"allow_prompt_only_model_execution": True,
		"input_mapping": {"job_json": "in", "resume_context": "param_context"},
		"_input_mapping_values": {"param_context": "resume text"},
		"_work_item": {
			"itemMode": "json_items",
			"itemIndex": 0,
			"itemPreview": {"id": "job_1", "title": "Engineer"},
		},
	}
	out = await llm_exec.exec_llm(
		"run-model-reg-mapping",
		_model_node("n_model", params).copy(),
		_context("run-model-reg-mapping"),
		upstream_artifact_ids=[],
	)
	assert out.status == "succeeded"
	assert captured.get("template_values", {}).get("job_json")
	assert captured.get("template_values", {}).get("resume_context") == "resume text"
