import importlib

import pytest

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from tests.integration.test_llm_contracts import _FakeAsyncClient, _graph, _fake_exec_source


@pytest.mark.asyncio
@pytest.mark.parametrize(
	("output_mode", "params_patch", "stream_lines", "post_payload", "embedding_payload"),
	[
		(
			"json",
			None,
			['data: {"choices":[{"delta":{"content":"{\\\"ok\\\":true}"}}]}', "data: [DONE]"],
			{"choices": [{"message": {"content": "{\"ok\":true}"}}]},
			{},
		),
		(
			"embeddings",
			{"embedding_contract": {"dims": 2, "dtype": "float32", "layout": "1d"}},
			[],
			{},
			{"data": [{"embedding": [0.1, 0.2]}]},
		),
	],
)
async def test_model_node_uses_adapter_backed_openai_executor(
	monkeypatch, tmp_path, output_mode, params_patch, stream_lines, post_payload, embedding_payload
):
	run_mod = importlib.import_module("app.runner.run")
	openai_mod = importlib.import_module("app.executors.llm_openai_compat")
	state = {
		"stream_lines": stream_lines,
		"post_payload": post_payload,
		"embedding_payload": embedding_payload,
		"chat_calls": 0,
		"post_calls": 0,
		"urls": [],
	}
	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	monkeypatch.setattr(openai_mod.httpx, "AsyncClient", lambda *args, **kwargs: _FakeAsyncClient(state))

	graph = _graph(output_mode, params_patch=params_patch)
	graph["nodes"][1]["data"]["kind"] = "model"
	graph["nodes"][1]["data"]["modelKind"] = "llm"
	graph["nodes"][1]["data"]["taskKind"] = "generate"

	events = []
	artifact_root = tmp_path / f"artifacts-model-{output_mode}"
	await run_mod.run_graph(
		run_id=f"run-model-{output_mode}",
		graph=graph,
		run_from=None,
		bus=RunEventBus(f"run-model-{output_mode}", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=DiskArtifactStore(artifact_root),
		cache=SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite")),
		graph_id=f"graph-model-{output_mode}",
	)

	assert any(e.get("type") == "node_output" and e.get("nodeId") == "llm_1" for e in events)
