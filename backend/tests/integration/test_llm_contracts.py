import importlib
import sys
import types

import pytest

if "duckdb" not in sys.modules:
	sys.modules["duckdb"] = types.SimpleNamespace()

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import NodeOutput


class _FakeStreamResponse:
	def __init__(self, lines):
		self._lines = lines

	async def __aenter__(self):
		return self

	async def __aexit__(self, exc_type, exc, tb):
		return False

	def raise_for_status(self):
		return None

	async def aiter_lines(self):
		for line in self._lines:
			yield line


class _FakeResponse:
	def __init__(self, payload):
		self._payload = payload

	def raise_for_status(self):
		return None

	def json(self):
		return self._payload


class _FakeAsyncClient:
	def __init__(self, state):
		self._state = state

	async def __aenter__(self):
		return self

	async def __aexit__(self, exc_type, exc, tb):
		return False

	def stream(self, method, url, json=None, headers=None):
		self._state["chat_calls"] += 1
		self._state["urls"].append(url)
		return _FakeStreamResponse(self._state["stream_lines"])

	async def post(self, url, json=None, headers=None):
		self._state["post_calls"] += 1
		self._state["urls"].append(url)
		if url.endswith("/v1/embeddings"):
			return _FakeResponse(self._state["embedding_payload"])
		return _FakeResponse(self._state["post_payload"])


def _graph(output_mode: str, params_patch: dict | None = None) -> dict:
	params = {
		"base_url": "https://fake-openai.local",
		"model": "fake-model",
		"user_prompt": "Summarize {input}",
		"output_mode": output_mode,
		"output_schema": {"type": "object", "properties": {"ok": {"type": "boolean"}}, "required": ["ok"]},
		"output_strict": True,
	}
	if params_patch:
		params.update(params_patch)
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
						"filename": "dummy.txt",
						"file_format": "txt",
						"output_mode": "text",
					},
				},
			},
			{
				"id": "llm_1",
				"data": {
					"kind": "llm",
					"label": "LLM",
					"llmKind": "openai_compat",
					"params": params,
				},
			},
		],
		"edges": [{"id": "e1", "source": "source_1", "target": "llm_1"}],
	}


async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
	return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data="{\"ok\":true}")


@pytest.mark.asyncio
async def test_llm_json_strict_invalid_json_fails_without_binding(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")
	openai_mod = importlib.import_module("app.executors.llm_openai_compat")
	state = {
		"stream_lines": [
			'data: {"choices":[{"delta":{"content":"not-json"}}]}',
			"data: [DONE]",
		],
		"post_payload": {"choices": [{"message": {"content": "not-json"}}]},
		"embedding_payload": {},
		"chat_calls": 0,
		"post_calls": 0,
		"urls": [],
	}
	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	monkeypatch.setattr(openai_mod.httpx, "AsyncClient", lambda *args, **kwargs: _FakeAsyncClient(state))

	events = []
	artifact_root = tmp_path / "artifacts-json-invalid"
	await run_mod.run_graph(
		run_id="run-json-invalid",
		graph=_graph("json"),
		run_from=None,
		bus=RunEventBus("run-json-invalid", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=DiskArtifactStore(artifact_root),
		cache=SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite")),
		graph_id="graph-json-invalid",
	)

	assert any(e.get("type") == "node_finished" and e.get("nodeId") == "llm_1" and e.get("status") == "failed" for e in events)
	assert not any(e.get("type") == "node_output" and e.get("nodeId") == "llm_1" for e in events)


@pytest.mark.asyncio
async def test_llm_json_strict_success_and_cache_reuse(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")
	openai_mod = importlib.import_module("app.executors.llm_openai_compat")
	state = {
		"stream_lines": [
			'data: {"choices":[{"delta":{"content":"{\\\"ok\\\":true}"}}]}',
			"data: [DONE]",
		],
		"post_payload": {"choices": [{"message": {"content": "{\"ok\":true}"}}]},
		"embedding_payload": {},
		"chat_calls": 0,
		"post_calls": 0,
		"urls": [],
	}
	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	monkeypatch.setattr(openai_mod.httpx, "AsyncClient", lambda *args, **kwargs: _FakeAsyncClient(state))

	artifact_root = tmp_path / "artifacts-json-cache"
	store = DiskArtifactStore(artifact_root)
	cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))
	graph = _graph("json")

	events_1 = []
	await run_mod.run_graph(
		run_id="run-json-cache-1",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-json-cache-1", on_emit=lambda evt: events_1.append(dict(evt))),
		artifact_store=store,
		cache=cache,
		graph_id="graph-json-cache",
	)
	assert any(e.get("type") == "node_output" and e.get("nodeId") == "llm_1" for e in events_1)

	events_2 = []
	await run_mod.run_graph(
		run_id="run-json-cache-2",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-json-cache-2", on_emit=lambda evt: events_2.append(dict(evt))),
		artifact_store=store,
		cache=cache,
		graph_id="graph-json-cache",
	)
	assert any(e.get("type") == "node_finished" and e.get("nodeId") == "llm_1" and e.get("cached") is True for e in events_2)
	assert state["chat_calls"] == 1


@pytest.mark.asyncio
async def test_llm_embeddings_dims_mismatch_fails_without_binding(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")
	openai_mod = importlib.import_module("app.executors.llm_openai_compat")
	state = {
		"stream_lines": [],
		"post_payload": {},
		"embedding_payload": {"data": [{"embedding": [0.1, 0.2]}]},
		"chat_calls": 0,
		"post_calls": 0,
		"urls": [],
	}
	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	monkeypatch.setattr(openai_mod.httpx, "AsyncClient", lambda *args, **kwargs: _FakeAsyncClient(state))

	events = []
	artifact_root = tmp_path / "artifacts-embeddings-invalid"
	await run_mod.run_graph(
		run_id="run-embeddings-invalid",
		graph=_graph("embeddings", {"embedding_contract": {"dims": 3, "dtype": "float32", "layout": "1d"}}),
		run_from=None,
		bus=RunEventBus("run-embeddings-invalid", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=DiskArtifactStore(artifact_root),
		cache=SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite")),
		graph_id="graph-embeddings-invalid",
	)

	assert any(e.get("type") == "node_finished" and e.get("nodeId") == "llm_1" and e.get("status") == "failed" for e in events)
	assert not any(e.get("type") == "node_output" and e.get("nodeId") == "llm_1" for e in events)


@pytest.mark.asyncio
async def test_llm_embeddings_success_cache_reuse_and_endpoint(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")
	openai_mod = importlib.import_module("app.executors.llm_openai_compat")
	state = {
		"stream_lines": [],
		"post_payload": {},
		"embedding_payload": {"data": [{"embedding": [0.1, 0.2, 0.3]}]},
		"chat_calls": 0,
		"post_calls": 0,
		"urls": [],
	}
	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	monkeypatch.setattr(openai_mod.httpx, "AsyncClient", lambda *args, **kwargs: _FakeAsyncClient(state))

	artifact_root = tmp_path / "artifacts-embeddings-cache"
	store = DiskArtifactStore(artifact_root)
	cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))
	graph = _graph("embeddings", {"embedding_contract": {"dims": 3, "dtype": "float32", "layout": "1d"}})

	events_1 = []
	await run_mod.run_graph(
		run_id="run-embeddings-cache-1",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-embeddings-cache-1", on_emit=lambda evt: events_1.append(dict(evt))),
		artifact_store=store,
		cache=cache,
		graph_id="graph-embeddings-cache",
	)
	assert any(e.get("type") == "node_output" and e.get("nodeId") == "llm_1" for e in events_1)

	events_2 = []
	await run_mod.run_graph(
		run_id="run-embeddings-cache-2",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-embeddings-cache-2", on_emit=lambda evt: events_2.append(dict(evt))),
		artifact_store=store,
		cache=cache,
		graph_id="graph-embeddings-cache",
	)
	assert any(e.get("type") == "node_finished" and e.get("nodeId") == "llm_1" and e.get("cached") is True for e in events_2)
	assert any(url.endswith("/v1/embeddings") for url in state["urls"])
	assert state["post_calls"] == 1


@pytest.mark.asyncio
async def test_ollama_visible_thinking_emits_delta_but_output_is_final_only(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")
	ollama_mod = importlib.import_module("app.executors.llm_ollama")

	class _OllamaClient:
		async def __aenter__(self):
			return self

		async def __aexit__(self, exc_type, exc, tb):
			return False

		def stream(self, method, url, json=None):
			lines = [
				'{"message":{"content":"hello","thinking":"reason-1"}}',
				'{"done":true}',
			]
			return _FakeStreamResponse(lines)

		async def post(self, url, json=None):
			return _FakeResponse({"message": {"content": "hello"}})

	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	monkeypatch.setattr(ollama_mod.httpx, "AsyncClient", lambda *args, **kwargs: _OllamaClient())

	graph = {
		"nodes": [
			{
				"id": "source_1",
				"data": {
					"kind": "source",
					"label": "Source",
					"sourceKind": "file",
					"params": {"rel_path": ".", "filename": "dummy.txt", "file_format": "txt", "output_mode": "text"},
				},
			},
			{
				"id": "llm_1",
				"data": {
					"kind": "llm",
					"label": "LLM",
					"llmKind": "ollama",
					"params": {
						"base_url": "http://localhost:11434",
						"model": "llama",
						"user_prompt": "Say hi",
						"output_mode": "text",
						"thinking": {"enabled": True, "mode": "visible"},
					},
				},
			},
		],
		"edges": [{"id": "e1", "source": "source_1", "target": "llm_1"}],
	}

	events: list[dict] = []
	artifact_root = tmp_path / "artifacts-ollama-thinking"
	store = DiskArtifactStore(artifact_root)
	cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))
	await run_mod.run_graph(
		run_id="run-ollama-thinking",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-ollama-thinking", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=store,
		cache=cache,
		graph_id="graph-ollama-thinking",
	)

	assert any(e.get("type") == "llm_thinking_delta" and e.get("nodeId") == "llm_1" for e in events)
	out = [e for e in events if e.get("type") == "node_output" and e.get("nodeId") == "llm_1"]
	assert out
	payload = await store.read(out[-1]["artifactId"])
	assert payload.decode("utf-8") == "hello"
