import importlib

import pytest

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import FileMetadata, NodeOutput


class _FakePromptRevStreamResponse:
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


class _FakePromptRevClient:
	def __init__(self, state):
		self._state = state

	async def __aenter__(self):
		return self

	async def __aexit__(self, exc_type, exc, tb):
		return False

	def stream(self, method, url, json=None, headers=None):
		self._state["calls"] += 1
		return _FakePromptRevStreamResponse(['data: {"choices":[{"delta":{"content":"ok"}}]}', "data: [DONE]"])

	async def post(self, url, json=None, headers=None):
		raise AssertionError("prompt revision cache test should use stream endpoint")


async def _fake_exec_source_text(run_id, node, context, upstream_artifact_ids=None):
	return NodeOutput(
		status="succeeded",
		data="cache input",
		metadata=FileMetadata(
			file_path="memory://input.txt",
			file_type="txt",
			mime_type="text/plain; charset=utf-8",
			size_bytes=11,
			data_schema={"type": "text"},
			content_hash="cache-input",
			node_id=node["id"],
			params_hash="paramhash",
		),
		execution_time_ms=1.0,
	)


def _graph(prompt_revision_id: str):
	return {
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
						"base_url": "https://prompt-rev.local",
						"model": "gpt-demo",
						"user_prompt": "Summarize",
						"prompt_revision_id": prompt_revision_id,
						"output_mode": "text",
					},
				},
			},
		],
		"edges": [{"id": "e1", "source": "source_1", "target": "model_1"}],
	}


@pytest.mark.asyncio
async def test_prompt_revision_change_invalidates_cache(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")
	openai_mod = importlib.import_module("app.executors.llm_openai_compat")
	state = {"calls": 0}
	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source_text)
	monkeypatch.setattr(openai_mod.httpx, "AsyncClient", lambda *args, **kwargs: _FakePromptRevClient(state))

	artifact_root = tmp_path / "artifacts-model-prompt-rev"
	(artifact_root / "meta").mkdir(parents=True, exist_ok=True)
	cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))

	events_1 = []
	await run_mod.run_graph(
		run_id="run-prompt-rev-1",
		graph=_graph("pr_001"),
		run_from=None,
		bus=RunEventBus("run-prompt-rev-1", on_emit=lambda evt: events_1.append(dict(evt))),
		artifact_store=DiskArtifactStore(artifact_root),
		cache=cache,
		graph_id="graph-model-prompt-rev",
	)
	assert any(e.get("type") == "node_output" and e.get("nodeId") == "model_1" for e in events_1)
	assert state["calls"] == 1

	events_2 = []
	await run_mod.run_graph(
		run_id="run-prompt-rev-2",
		graph=_graph("pr_001"),
		run_from=None,
		bus=RunEventBus("run-prompt-rev-2", on_emit=lambda evt: events_2.append(dict(evt))),
		artifact_store=DiskArtifactStore(artifact_root),
		cache=cache,
		graph_id="graph-model-prompt-rev",
	)
	assert any(e.get("type") == "node_finished" and e.get("nodeId") == "model_1" and e.get("cached") is True for e in events_2)
	assert state["calls"] == 1

	events_3 = []
	await run_mod.run_graph(
		run_id="run-prompt-rev-3",
		graph=_graph("pr_002"),
		run_from=None,
		bus=RunEventBus("run-prompt-rev-3", on_emit=lambda evt: events_3.append(dict(evt))),
		artifact_store=DiskArtifactStore(artifact_root),
		cache=cache,
		graph_id="graph-model-prompt-rev",
	)
	assert any(e.get("type") == "node_output" and e.get("nodeId") == "model_1" for e in events_3)
	assert state["calls"] == 2
