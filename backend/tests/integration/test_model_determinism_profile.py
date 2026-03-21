import importlib

import pytest

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import FileMetadata, NodeOutput


class _FakeDeterminismStreamResponse:
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


class _FakeDeterminismClient:
	def __init__(self, state):
		self._state = state

	async def __aenter__(self):
		return self

	async def __aexit__(self, exc_type, exc, tb):
		return False

	def stream(self, method, url, json=None, headers=None):
		self._state["payloads"].append(json or {})
		return _FakeDeterminismStreamResponse(['data: {"choices":[{"delta":{"content":"ok"}}]}', "data: [DONE]"])

	async def post(self, url, json=None, headers=None):
		raise AssertionError("determinism test should use stream endpoint")


async def _fake_exec_source_text(run_id, node, context, upstream_artifact_ids=None):
	return NodeOutput(
		status="succeeded",
		data="deterministic input",
		metadata=FileMetadata(
			file_path="memory://input.txt",
			file_type="txt",
			mime_type="text/plain; charset=utf-8",
			size_bytes=19,
			data_schema={"type": "text"},
			content_hash="det-input",
			node_id=node["id"],
			params_hash="paramhash",
		),
		execution_time_ms=1.0,
	)


def _graph():
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
						"base_url": "https://determinism.local",
						"model": "gpt-demo",
						"user_prompt": "Summarize",
						"output_mode": "text",
						"request_policy": {"determinism": {"enabled": True, "seed": 123, "stable_order": True}},
					},
				},
			},
		],
		"edges": [{"id": "e1", "source": "source_1", "target": "model_1"}],
	}


@pytest.mark.asyncio
async def test_model_determinism_profile_emits_stable_hash_and_seed(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")
	openai_mod = importlib.import_module("app.executors.llm_openai_compat")
	state = {"payloads": []}
	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source_text)
	monkeypatch.setattr(openai_mod.httpx, "AsyncClient", lambda *args, **kwargs: _FakeDeterminismClient(state))

	graph = _graph()
	all_events = []
	for idx in range(2):
		events = []
		artifact_root = tmp_path / f"artifacts-model-determinism-{idx}"
		await run_mod.run_graph(
			run_id=f"run-model-determinism-{idx}",
			graph=graph,
			run_from=None,
			bus=RunEventBus(f"run-model-determinism-{idx}", on_emit=lambda evt: events.append(dict(evt))),
			artifact_store=DiskArtifactStore(artifact_root),
			cache=SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite")),
			graph_id="graph-model-determinism",
		)
		all_events.append(events)

	det_logs = []
	for events in all_events:
		entries = [str(e.get("message") or "") for e in events if e.get("type") == "log" and "MODEL_DETERMINISM:" in str(e.get("message") or "")]
		assert len(entries) > 0
		det_logs.append(entries[-1])
	assert det_logs[0] == det_logs[1]
	assert len(state["payloads"]) >= 2
	assert state["payloads"][-1].get("seed") == 123
