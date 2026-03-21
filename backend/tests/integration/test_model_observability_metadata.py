import importlib

import pytest

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import FileMetadata, NodeOutput


class _FakeObsStreamResponse:
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


class _FakeObsClient:
	async def __aenter__(self):
		return self

	async def __aexit__(self, exc_type, exc, tb):
		return False

	def stream(self, method, url, json=None, headers=None):
		return _FakeObsStreamResponse(['data: {"choices":[{"delta":{"content":"ok"}}]}', "data: [DONE]"])

	async def post(self, url, json=None, headers=None):
		raise AssertionError("observability test should use stream endpoint")


async def _fake_exec_source_text(run_id, node, context, upstream_artifact_ids=None):
	return NodeOutput(
		status="succeeded",
		data="observability input",
		metadata=FileMetadata(
			file_path="memory://input.txt",
			file_type="txt",
			mime_type="text/plain; charset=utf-8",
			size_bytes=18,
			data_schema={"type": "text"},
			content_hash="obs-input",
			node_id=node["id"],
			params_hash="paramhash",
		),
		execution_time_ms=1.0,
	)


@pytest.mark.asyncio
async def test_model_output_includes_observability_metadata(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")
	openai_mod = importlib.import_module("app.executors.llm_openai_compat")
	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source_text)
	monkeypatch.setattr(openai_mod.httpx, "AsyncClient", lambda *args, **kwargs: _FakeObsClient())

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
						"base_url": "https://observability.local",
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
	artifact_root = tmp_path / "artifacts-model-observability"
	await run_mod.run_graph(
		run_id="run-model-observability",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-model-observability", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=DiskArtifactStore(artifact_root),
		cache=SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite")),
		graph_id="graph-model-observability",
	)

	obs_events = [e for e in events if e.get("type") == "model_observability" and e.get("nodeId") == "model_1"]
	assert len(obs_events) > 0
	latest_obs = obs_events[-1]
	assert latest_obs.get("provider") == "openai_compat"
	assert latest_obs.get("model") == "gpt-demo"
	assert latest_obs.get("cache_decision") == "executed"
	assert isinstance(latest_obs.get("total_tokens_est"), int)
