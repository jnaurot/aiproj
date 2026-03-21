import importlib
import json

import pytest

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import FileMetadata, NodeOutput


class _BadJsonStreamResponse:
	async def __aenter__(self):
		return self

	async def __aexit__(self, exc_type, exc, tb):
		return False

	def raise_for_status(self):
		return None

	async def aiter_lines(self):
		yield 'data: {"choices":[{"delta":{"content":"not-json"}}]}'
		yield "data: [DONE]"


class _BadJsonClient:
	async def __aenter__(self):
		return self

	async def __aexit__(self, exc_type, exc, tb):
		return False

	def stream(self, method, url, json=None, headers=None):
		return _BadJsonStreamResponse()

	async def post(self, url, json=None, headers=None):
		class _Resp:
			def raise_for_status(self):
				return None

			def json(self):
				return {"choices": [{"message": {"content": "not-json"}}]}

		return _Resp()


async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
	return NodeOutput(
		status="succeeded",
		data="sample text input",
		metadata=FileMetadata(
			file_path="memory://input.txt",
			file_type="txt",
			mime_type="text/plain; charset=utf-8",
			size_bytes=17,
			data_schema={"type": "text"},
			content_hash="text-hash",
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
						"base_url": "https://hardening.local",
						"model": "gpt-hardening",
						"user_prompt": "Extract JSON",
						"output_mode": "json",
						"output_schema": {"type": "object"},
					},
				},
			},
		],
		"edges": [{"id": "e1", "source": "source_1", "target": "model_1"}],
	}


@pytest.mark.asyncio
async def test_model_runtime_hardening_structured_error_and_no_model_debug_prints(monkeypatch, tmp_path, capsys):
	run_mod = importlib.import_module("app.runner.run")
	openai_mod = importlib.import_module("app.executors.llm_openai_compat")

	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	monkeypatch.setattr(openai_mod.httpx, "AsyncClient", lambda *args, **kwargs: _BadJsonClient())

	events = []
	artifact_root = tmp_path / "artifacts-hardening"
	await run_mod.run_graph(
		run_id="run-model-hardening",
		graph=_graph(),
		run_from=None,
		bus=RunEventBus("run-model-hardening", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=DiskArtifactStore(artifact_root),
		cache=SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite")),
		graph_id="graph-model-hardening",
	)

	finished = [e for e in events if e.get("type") == "node_finished" and e.get("nodeId") == "model_1"]
	assert finished and finished[-1].get("status") == "failed"
	error_raw = str(finished[-1].get("error") or "")
	error_obj = json.loads(error_raw)
	assert error_obj.get("code")
	assert error_obj.get("errorCode") == error_obj.get("code")
	assert isinstance(error_obj.get("message"), str) and error_obj.get("message")

	stdout = capsys.readouterr().out
	assert "LLM EXEC raw_params" not in stdout
	assert "[VALIDATOR] LLM NODE RAW" not in stdout
	assert "[SCHEMAS] LLM params BEFORE normalize" not in stdout
	assert "[run_graph] LLM upstream_ids" not in stdout
	assert "[ollama] node_id:" not in stdout
	assert "IN COMPILE_PLAN" not in stdout
