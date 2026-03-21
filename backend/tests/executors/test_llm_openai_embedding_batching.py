import json

import pytest

from app.executors.llm_openai_compat import exec_llm_openai_compat
from app.runner.artifacts import DiskArtifactStore, RunBindings
from app.runner.events import RunEventBus
from app.runner.metadata import GraphContext
from app.runner.schemas import LLMParams


class _FakeEmbedResponse:
	def __init__(self, payload):
		self._payload = payload

	def raise_for_status(self):
		return None

	def json(self):
		return self._payload


class _FakeEmbedClient:
	def __init__(self, state):
		self._state = state

	async def __aenter__(self):
		return self

	async def __aexit__(self, exc_type, exc, tb):
		return False

	async def post(self, url, json=None, headers=None):
		items = json.get("input")
		if isinstance(items, str):
			items = [items]
		self._state["calls"].append(list(items or []))
		rows = []
		for idx, _item in enumerate(items or []):
			global_index = self._state["next_index"]
			rows.append({"index": idx, "embedding": [float(global_index), float(global_index) + 0.25]})
			self._state["next_index"] += 1
		return _FakeEmbedResponse({"data": rows})


@pytest.mark.asyncio
async def test_openai_embeddings_micro_batches_preserve_order(monkeypatch, tmp_path):
	from app.executors import llm_openai_compat as mod

	state = {"calls": [], "next_index": 0}
	monkeypatch.setattr(mod.httpx, "AsyncClient", lambda *args, **kwargs: _FakeEmbedClient(state))

	params = LLMParams.model_validate(
		{
			"base_url": "https://embeddings.local",
			"model": "text-embedding-3-small",
			"user_prompt": "embed",
			"output_mode": "embeddings",
			"embedding_contract": {"dims": 2, "dtype": "float32", "layout": "2d"},
			"request_policy": {"batching": {"enabled": True, "max_items": 2}, "retries": 0},
		}
	)
	events = []
	context = GraphContext(
		run_id="run-embed-batch",
		graph_id="graph-embed-batch",
		bus=RunEventBus("run-embed-batch", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=DiskArtifactStore(tmp_path / "artifacts"),
		bindings=RunBindings("run-embed-batch", graph_id="graph-embed-batch"),
	)

	out = await exec_llm_openai_compat(
		run_id="run-embed-batch",
		node={"id": "model_1"},
		context=context,
		input_metadata=None,
		params=params,
		input_text="x",
		input_items=["a", "b", "c", "d", "e"],
		upstream_artifact_ids=["upstream_1"],
	)

	assert out.status == "succeeded"
	assert state["calls"] == [["a", "b"], ["c", "d"], ["e"]]
	payload = json.loads(str(out.data or "{}"))
	assert payload.get("layout") == "2d"
	assert payload.get("dims") == 2
	assert payload.get("data") == [
		[0.0, 0.25],
		[1.0, 1.25],
		[2.0, 2.25],
		[3.0, 3.25],
		[4.0, 4.25],
	]
	assert any("micro-batch" in str(e.get("message") or "") for e in events if e.get("type") == "log")
