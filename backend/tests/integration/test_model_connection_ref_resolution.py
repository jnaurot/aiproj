import importlib

import pytest

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import FileMetadata, NodeOutput


class _FakeConnRefStreamResponse:
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


class _FakeConnRefClient:
	def __init__(self, state):
		self._state = state

	async def __aenter__(self):
		return self

	async def __aexit__(self, exc_type, exc, tb):
		return False

	def stream(self, method, url, json=None, headers=None):
		self._state["urls"].append(url)
		self._state["headers"].append(headers or {})
		return _FakeConnRefStreamResponse(['data: {"choices":[{"delta":{"content":"ok"}}]}', "data: [DONE]"])

	async def post(self, url, json=None, headers=None):
		raise AssertionError("connection-ref test should use stream endpoint")


async def _fake_exec_source_text(run_id, node, context, upstream_artifact_ids=None):
	return NodeOutput(
		status="succeeded",
		data="hello world",
		metadata=FileMetadata(
			file_path="memory://source.txt",
			file_type="txt",
			mime_type="text/plain; charset=utf-8",
			size_bytes=11,
			data_schema={"type": "text"},
			content_hash="texthash",
			node_id=node["id"],
			params_hash="paramhash",
		),
		execution_time_ms=1.0,
	)


@pytest.mark.asyncio
async def test_model_connection_ref_resolves_server_side_without_secret_leak(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")
	openai_mod = importlib.import_module("app.executors.llm_openai_compat")
	state = {"urls": [], "headers": []}
	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source_text)
	monkeypatch.setattr(openai_mod.httpx, "AsyncClient", lambda *args, **kwargs: _FakeConnRefClient(state))
	monkeypatch.setenv(
		"MODEL_CONNECTION_PROFILES_JSON",
		'{"openai_prod":{"base_url":"https://resolved.example.com","api_key_ref":"OPENAI_SECRET"}}',
	)
	monkeypatch.setenv("OPENAI_SECRET", "sk-test-secret")

	graph = {
		"nodes": [
			{
				"id": "source_1",
				"data": {
					"kind": "source",
					"label": "Source",
					"sourceKind": "file",
					"params": {"rel_path": ".", "filename": "x.txt", "file_format": "txt"},
				},
			},
			{
				"id": "model_1",
				"data": {
					"kind": "model",
					"label": "Model",
					"llmKind": "openai_compat",
					"modelKind": "llm",
					"taskKind": "generate",
					"params": {
						"connection_ref": "openai_prod",
						"model": "gpt-demo",
						"user_prompt": "Summarize",
						"output_mode": "text",
					},
				},
			},
		],
		"edges": [{"id": "e1", "source": "source_1", "target": "model_1"}],
	}

	events = []
	artifact_root = tmp_path / "artifacts-model-conn-ref"
	await run_mod.run_graph(
		run_id="run-model-conn-ref",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-model-conn-ref", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=DiskArtifactStore(artifact_root),
		cache=SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite")),
		graph_id="graph-model-conn-ref",
	)

	assert any(e.get("type") == "node_output" and e.get("nodeId") == "model_1" for e in events)
	assert len(state["urls"]) > 0
	assert state["urls"][-1].startswith("https://resolved.example.com/")
	assert state["headers"][-1].get("Authorization") == "Bearer sk-test-secret"
	log_blob = "\n".join(str(e.get("message") or "") for e in events if e.get("type") == "log")
	assert "sk-test-secret" not in log_blob
