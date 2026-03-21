import importlib

import pytest

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import FileMetadata, NodeOutput


class _FakeMultimodalStreamResponse:
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


class _FakeMultimodalClient:
	def __init__(self, state):
		self._state = state

	async def __aenter__(self):
		return self

	async def __aexit__(self, exc_type, exc, tb):
		return False

	def stream(self, method, url, json=None, headers=None):
		self._state["payloads"].append(json or {})
		return _FakeMultimodalStreamResponse(['data: {"choices":[{"delta":{"content":"ok"}}]}', "data: [DONE]"])

	async def post(self, url, json=None, headers=None):
		raise AssertionError("multimodal envelope path should use chat stream endpoint")


async def _fake_exec_source_text(run_id, node, context, upstream_artifact_ids=None):
	return NodeOutput(
		status="succeeded",
		data="base text",
		metadata=FileMetadata(
			file_path="memory://input.txt",
			file_type="txt",
			mime_type="text/plain; charset=utf-8",
			size_bytes=9,
			data_schema={"type": "text"},
			content_hash="texthash",
			node_id=node["id"],
			params_hash="paramhash",
		),
		execution_time_ms=1.0,
	)


@pytest.mark.asyncio
async def test_model_multimodal_envelope_serializes_deterministically(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")
	openai_mod = importlib.import_module("app.executors.llm_openai_compat")
	state = {"payloads": []}
	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source_text)
	monkeypatch.setattr(openai_mod.httpx, "AsyncClient", lambda *args, **kwargs: _FakeMultimodalClient(state))

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
					"modelKind": "multimodal",
					"taskKind": "extract",
					"params": {
						"base_url": "https://fake-openai.local",
						"model": "fake-multimodal",
						"user_prompt": "Extract entities",
						"output_mode": "text",
						"input_envelope": [
							{"type": "text", "text": "context block"},
							{"type": "image", "dataUrl": "data:image/png;base64,QUJD"},
							{"type": "audio", "dataUrl": "data:audio/wav;base64,QUJD"},
						],
					},
				},
			},
		],
		"edges": [{"id": "e1", "source": "source_1", "target": "model_1"}],
	}

	events = []
	artifact_root = tmp_path / "artifacts-model-multimodal-envelope"
	await run_mod.run_graph(
		run_id="run-model-multimodal-envelope",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-model-multimodal-envelope", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=DiskArtifactStore(artifact_root),
		cache=SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite")),
		graph_id="graph-model-multimodal-envelope",
	)

	assert any(e.get("type") == "node_output" and e.get("nodeId") == "model_1" for e in events)
	assert len(state["payloads"]) > 0
	payload = state["payloads"][-1]
	messages = payload.get("messages") or []
	assert isinstance(messages, list) and len(messages) > 0
	content = messages[-1].get("content")
	assert isinstance(content, list)
	assert any(isinstance(part, dict) and part.get("type") == "image_url" for part in content)
	assert any(isinstance(part, dict) and part.get("type") == "input_audio" for part in content)
	assert any(isinstance(part, dict) and part.get("type") == "text" and "context block" in str(part.get("text")) for part in content)
