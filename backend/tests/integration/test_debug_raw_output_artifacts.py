import importlib
import json
import re

import pytest

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import FileMetadata, NodeOutput


_ARTIFACT_RE = re.compile(r"\bartifactId=([^\s]+)")
_SOURCE_ARTIFACT_RE = re.compile(r"\bsourceArtifactId=([^\s]+)")


def _extract_artifact_id(message: str) -> str:
	match = _ARTIFACT_RE.search(str(message or ""))
	return str(match.group(1) if match else "")


def _extract_source_artifact_id(message: str) -> str:
	match = _SOURCE_ARTIFACT_RE.search(str(message or ""))
	return str(match.group(1) if match else "")


async def _fake_exec_source_json(run_id, node, context, upstream_artifact_ids=None):
	return NodeOutput(
		status="succeeded",
		data={"alpha": 1, "beta": "two"},
		metadata=FileMetadata(
			file_path="memory://input.json",
			file_type="json",
			mime_type="application/json",
			size_bytes=24,
			data_schema={"type": "json"},
			content_hash="src-json-hash",
			node_id=node["id"],
			params_hash="src-json-params",
		),
		execution_time_ms=1.0,
	)


async def _fake_exec_source_text(run_id, node, context, upstream_artifact_ids=None):
	return NodeOutput(
		status="succeeded",
		data="sample text input",
		metadata=FileMetadata(
			file_path="memory://input.txt",
			file_type="txt",
			mime_type="text/plain; charset=utf-8",
			size_bytes=17,
			data_schema={"type": "text"},
			content_hash="src-text-hash",
			node_id=node["id"],
			params_hash="src-text-params",
		),
		execution_time_ms=1.0,
	)


class _SpacedJsonStreamResponse:
	async def __aenter__(self):
		return self

	async def __aexit__(self, exc_type, exc, tb):
		return False

	def raise_for_status(self):
		return None

	async def aiter_lines(self):
		yield 'data: {"choices":[{"delta":{"content":"{\\n  \\"ok\\" : true\\n}"}}]}'
		yield "data: [DONE]"


class _SpacedJsonClient:
	async def __aenter__(self):
		return self

	async def __aexit__(self, exc_type, exc, tb):
		return False

	def stream(self, method, url, json=None, headers=None):
		return _SpacedJsonStreamResponse()

	async def post(self, url, json=None, headers=None):
		class _Resp:
			def raise_for_status(self):
				return None

			def json(self):
				return {"choices": [{"message": {"content": "{\n  \"ok\" : true\n}"}}]}

		return _Resp()


def _source_only_graph_debug_enabled():
	return {
		"nodes": [
			{
				"id": "source_1",
				"data": {
					"kind": "source",
					"label": "Source",
					"sourceKind": "file",
					"params": {
						"rel_path": ".",
						"filename": "in.json",
						"file_format": "json",
						"debug": {"enabled": True, "log_raw_output": True},
					},
				},
			}
		],
		"edges": [],
	}


def _model_graph_debug_enabled():
	return {
		"nodes": [
			{
				"id": "source_1",
				"data": {
					"kind": "source",
					"label": "Source",
					"sourceKind": "file",
					"params": {"rel_path": ".", "filename": "in.txt", "file_format": "txt"},
				},
			},
			{
				"id": "model_1",
				"data": {
					"kind": "model",
					"label": "Model",
					"llmKind": "openai_compat",
					"modelKind": "llm",
					"taskKind": "extract",
					"schema": {
						"expectedSchema": {
							"typedSchema": {"type": "json", "fields": []},
							"source": "declared",
							"state": "fresh",
						}
					},
					"params": {
						"base_url": "https://raw-output.local",
						"model": "gpt-raw-output",
						"user_prompt": "Extract JSON",
						"output_mode": "json",
						"output_schema": {"type": "object"},
						"debug": {"enabled": True, "log_input_preview": False, "log_raw_output": True},
					},
				},
			},
		],
		"edges": [{"id": "e1", "source": "source_1", "target": "model_1"}],
	}


@pytest.mark.asyncio
async def test_debug_raw_output_artifact_created_for_source_node(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")
	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source_json)

	events = []
	artifact_root = tmp_path / "artifacts-source-debug-raw"
	artifact_store = DiskArtifactStore(artifact_root)
	await run_mod.run_graph(
		run_id="run-source-debug-raw",
		graph=_source_only_graph_debug_enabled(),
		run_from=None,
		bus=RunEventBus("run-source-debug-raw", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=artifact_store,
		cache=SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite")),
		graph_id="graph-source-debug-raw",
	)

	debug_logs = [
		evt
		for evt in events
		if str(evt.get("type") or "") == "log"
		and str(evt.get("nodeId") or "") == "source_1"
		and "[debug] raw_output artifactId=" in str(evt.get("message") or "")
	]
	assert debug_logs
	debug_artifact_id = _extract_artifact_id(str(debug_logs[-1].get("message") or ""))
	assert debug_artifact_id
	assert await artifact_store.exists(debug_artifact_id)
	raw_payload = await artifact_store.read(debug_artifact_id)
	assert b'"alpha"' in raw_payload
	assert b'"beta"' in raw_payload


@pytest.mark.asyncio
async def test_debug_raw_output_artifact_for_model_preserves_preparse_text(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")
	openai_mod = importlib.import_module("app.executors.llm_openai_compat")

	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source_text)
	monkeypatch.setattr(openai_mod.httpx, "AsyncClient", lambda *args, **kwargs: _SpacedJsonClient())

	events = []
	artifact_root = tmp_path / "artifacts-model-debug-raw"
	artifact_store = DiskArtifactStore(artifact_root)
	await run_mod.run_graph(
		run_id="run-model-debug-raw",
		graph=_model_graph_debug_enabled(),
		run_from=None,
		bus=RunEventBus("run-model-debug-raw", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=artifact_store,
		cache=SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite")),
		graph_id="graph-model-debug-raw",
	)

	debug_logs = [
		evt
		for evt in events
		if str(evt.get("type") or "") == "log"
		and str(evt.get("nodeId") or "") == "model_1"
		and "[debug] raw_output artifactId=" in str(evt.get("message") or "")
	]
	assert debug_logs
	debug_message = str(debug_logs[-1].get("message") or "")
	debug_artifact_id = _extract_artifact_id(debug_message)
	source_artifact_id = _extract_source_artifact_id(debug_message)
	assert debug_artifact_id
	assert source_artifact_id

	assert await artifact_store.exists(debug_artifact_id)
	assert await artifact_store.exists(source_artifact_id)
	debug_raw = await artifact_store.read(debug_artifact_id)
	normalized_output = await artifact_store.read(source_artifact_id)
	assert debug_raw != normalized_output
	assert b"\n" in debug_raw
	assert b'"ok"' in normalized_output


async def _fake_exec_source_jobs(run_id, node, context, upstream_artifact_ids=None):
	return NodeOutput(
		status="succeeded",
		data=[
			{"job_id": "job-1", "company_name": "Company A", "title": "Role A"},
			{"job_id": "job-2", "company_name": "Company B", "title": "Role B"},
		],
		metadata=FileMetadata(
			file_path="memory://jobs.json",
			file_type="json",
			mime_type="application/json",
			size_bytes=128,
			data_schema={"type": "json"},
			content_hash="jobs-src-hash",
			node_id=node["id"],
			params_hash="jobs-src-params",
		),
		execution_time_ms=1.0,
	)


async def _fake_exec_tool_echo_item(run_id, node, context, upstream_artifact_ids=None):
	node_id = str(node.get("id") or "")
	if node_id == "source_jobs":
		return await _fake_exec_source_jobs(run_id, node, context, upstream_artifact_ids=upstream_artifact_ids)
	work_item = ((node.get("data", {}).get("params", {}) or {}).get("_work_item") or {})
	item_preview = work_item.get("itemPreview")
	job_id = ""
	company_name = ""
	if isinstance(item_preview, dict):
		job_id = str(item_preview.get("job_id") or "")
		company_name = str(item_preview.get("company_name") or "")
	return NodeOutput(
		status="succeeded",
		data={"job_id": job_id, "company_name": company_name},
		metadata=FileMetadata(
			file_path=f"memory://{job_id or 'job'}.json",
			file_type="json",
			mime_type="application/json",
			size_bytes=96,
			data_schema={"type": "json"},
			content_hash=f"llm-{job_id or 'job'}",
			node_id=node["id"],
			params_hash=f"llm-params-{job_id or 'job'}",
		),
		execution_time_ms=1.0,
	)


@pytest.mark.asyncio
async def test_single_item_streaming_outputs_distinct_artifacts_and_output_logs(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")
	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool_echo_item)

	graph = {
		"nodes": [
			{
				"id": "source_jobs",
				"data": {
					"kind": "tool",
					"label": "Source Jobs",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
			{
				"id": "score_tool",
				"data": {
					"kind": "tool",
					"label": "Score Tool",
					"processingPolicy": {"consume_mode": "single_item", "batch_size": 1, "max_inflight": 1},
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
		],
		"edges": [
			{
				"id": "e_source_model",
				"source": "source_jobs",
				"target": "score_tool",
				"data": {
					"mode": "work",
					"queue": {"max": 1000, "overflow": "block"},
					"work": {"item_mode": "json_items", "max_items": 256},
				},
			}
		],
	}

	events = []
	artifact_root = tmp_path / "artifacts-distinct-streaming"
	artifact_store = DiskArtifactStore(artifact_root)
	await run_mod.run_graph(
		run_id="run-distinct-streaming",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-distinct-streaming", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=artifact_store,
		cache=SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite")),
		graph_id="graph-distinct-streaming",
	)

	model_outputs = [
		evt
		for evt in events
		if str(evt.get("type") or "") == "node_output" and str(evt.get("nodeId") or "") == "score_tool"
	]
	assert len(model_outputs) >= 2
	model_artifact_ids = [str(evt.get("artifactId") or "").strip() for evt in model_outputs]
	model_artifact_ids = [artifact_id for artifact_id in model_artifact_ids if artifact_id]
	assert len(model_artifact_ids) >= 2
	assert len(set(model_artifact_ids)) >= 2

	output_logs = [
		evt
		for evt in events
		if str(evt.get("type") or "") == "log"
		and str(evt.get("nodeId") or "") == "score_tool"
		and "[output] artifactId=" in str(evt.get("message") or "")
	]
	assert output_logs

	payloads = []
	for artifact_id in set(model_artifact_ids):
		assert await artifact_store.exists(artifact_id)
		raw = await artifact_store.read(artifact_id)
		payload = json.loads(raw.decode("utf-8"))
		payloads.append(json.dumps(payload, sort_keys=True))

	assert any("Company A" in payload for payload in payloads)
	assert any("Company B" in payload for payload in payloads)
