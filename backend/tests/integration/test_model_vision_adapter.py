import importlib

import pytest

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import FileMetadata, NodeOutput


class _FakeVisionStreamResponse:
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


class _FakeVisionClient:
	def __init__(self, state):
		self._state = state

	async def __aenter__(self):
		return self

	async def __aexit__(self, exc_type, exc, tb):
		return False

	def stream(self, method, url, json=None, headers=None):
		self._state["payloads"].append(json or {})
		self._state["urls"].append(url)
		return _FakeVisionStreamResponse(['data: {"choices":[{"delta":{"content":"caption ok"}}]}', "data: [DONE]"])

	async def post(self, url, json=None, headers=None):
		raise AssertionError("vision caption path should use chat stream endpoint")


async def _fake_exec_source_image(run_id, node, context, upstream_artifact_ids=None):
	return NodeOutput(
		status="succeeded",
		data=b"\x89PNG\r\n",
		metadata=FileMetadata(
			file_path="memory://vision.png",
			file_type="image",
			mime_type="image/png",
			size_bytes=6,
			data_schema={"type": "image"},
			content_hash="visionhash",
			node_id=node["id"],
			params_hash="paramhash",
		),
		execution_time_ms=1.0,
	)


@pytest.mark.asyncio
async def test_model_vision_openai_payload_includes_image_parts(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")
	openai_mod = importlib.import_module("app.executors.llm_openai_compat")
	state = {"payloads": [], "urls": []}
	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source_image)
	monkeypatch.setattr(openai_mod.httpx, "AsyncClient", lambda *args, **kwargs: _FakeVisionClient(state))

	graph = {
		"nodes": [
			{
				"id": "source_1",
				"data": {
					"kind": "source",
					"label": "Source",
					"sourceKind": "file",
					"params": {"rel_path": ".", "filename": "x.png", "file_format": "png"},
				},
			},
			{
				"id": "model_1",
				"data": {
					"kind": "model",
					"label": "Model",
					"llmKind": "openai_compat",
					"modelKind": "vision",
					"taskKind": "caption",
					"params": {
						"base_url": "https://fake-openai.local",
						"model": "fake-vision",
						"user_prompt": "Caption this image",
						"output_mode": "text",
					},
				},
			},
		],
		"edges": [{"id": "e1", "source": "source_1", "target": "model_1"}],
	}

	events = []
	artifact_root = tmp_path / "artifacts-model-vision"
	await run_mod.run_graph(
		run_id="run-model-vision",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-model-vision", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=DiskArtifactStore(artifact_root),
		cache=SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite")),
		graph_id="graph-model-vision",
	)

	assert any(e.get("type") == "node_output" and e.get("nodeId") == "model_1" for e in events)
	assert len(state["payloads"]) > 0
	messages = state["payloads"][-1].get("messages") or []
	assert isinstance(messages, list) and len(messages) > 0
	last_content = messages[-1].get("content")
	assert isinstance(last_content, list)
	assert any(isinstance(part, dict) and part.get("type") == "image_url" for part in last_content)
