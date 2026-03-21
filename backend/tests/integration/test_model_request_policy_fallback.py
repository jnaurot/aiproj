import importlib

import httpx
import pytest

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import FileMetadata, NodeOutput


class _FakeFallbackStreamResponse:
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


class _FakeFallbackClient:
	def __init__(self, state):
		self._state = state

	async def __aenter__(self):
		return self

	async def __aexit__(self, exc_type, exc, tb):
		return False

	def stream(self, method, url, json=None, headers=None):
		self._state["urls"].append(url)
		if "primary.local" in url:
			raise httpx.ConnectError("primary down", request=httpx.Request("POST", url))
		return _FakeFallbackStreamResponse(['data: {"choices":[{"delta":{"content":"fallback ok"}}]}', "data: [DONE]"])

	async def post(self, url, json=None, headers=None):
		raise AssertionError("fallback policy test should use chat stream endpoint")


async def _fake_exec_source_text(run_id, node, context, upstream_artifact_ids=None):
	return NodeOutput(
		status="succeeded",
		data="input",
		metadata=FileMetadata(
			file_path="memory://input.txt",
			file_type="txt",
			mime_type="text/plain; charset=utf-8",
			size_bytes=5,
			data_schema={"type": "text"},
			content_hash="inputhash",
			node_id=node["id"],
			params_hash="paramhash",
		),
		execution_time_ms=1.0,
	)


@pytest.mark.asyncio
async def test_model_request_policy_fallback_chain_recovers_on_secondary(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")
	openai_mod = importlib.import_module("app.executors.llm_openai_compat")
	state = {"urls": []}
	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source_text)
	monkeypatch.setattr(openai_mod.httpx, "AsyncClient", lambda *args, **kwargs: _FakeFallbackClient(state))

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
						"base_url": "https://primary.local",
						"model": "gpt-demo",
						"user_prompt": "Summarize",
						"output_mode": "text",
						"request_policy": {
							"retries": 0,
							"fallback_chain": [
								{"llm_kind": "openai_compat", "base_url": "https://secondary.local", "model": "gpt-demo-b"}
							],
						},
					},
				},
			},
		],
		"edges": [{"id": "e1", "source": "source_1", "target": "model_1"}],
	}

	events = []
	artifact_root = tmp_path / "artifacts-model-fallback-policy"
	await run_mod.run_graph(
		run_id="run-model-fallback-policy",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-model-fallback-policy", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=DiskArtifactStore(artifact_root),
		cache=SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite")),
		graph_id="graph-model-fallback-policy",
	)

	assert any(e.get("type") == "node_output" and e.get("nodeId") == "model_1" for e in events)
	assert any("primary.local" in url for url in state["urls"])
	assert any("secondary.local" in url for url in state["urls"])
	assert any("MODEL_FALLBACK_ACTIVATED" in str(e.get("message") or "") for e in events if e.get("type") == "log")
