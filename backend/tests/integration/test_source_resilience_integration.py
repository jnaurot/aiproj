import json
from urllib.parse import parse_qs, urlparse

import httpx
import pytest

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.run import run_graph


def _graph_api_source(params: dict) -> dict:
	merged_params = {"source_type": "api", "output_mode": "json", **params}
	return {
		"nodes": [
			{
				"id": "src_api",
				"data": {"kind": "source", "sourceKind": "api", "label": "API Source", "params": merged_params},
			}
		],
		"edges": [],
	}


@pytest.mark.asyncio
async def test_source_large_payload_processing_remains_bounded(monkeypatch, tmp_path):
	rows = [{"id": idx, "value": f"v{idx}"} for idx in range(2000)]
	response_json = json.dumps(rows, separators=(",", ":"))
	input_bytes = len(response_json.encode("utf-8"))

	class _Resp:
		status_code = 200
		headers = {"content-type": "application/json"}

		def raise_for_status(self):
			return None

		def json(self):
			return rows

		@property
		def text(self):
			return response_json

	class _Client:
		async def __aenter__(self):
			return self

		async def __aexit__(self, exc_type, exc, tb):
			return False

		async def request(self, **_kwargs):
			return _Resp()

	monkeypatch.setattr("app.executors.source.httpx.AsyncClient", _Client)
	store = DiskArtifactStore(tmp_path / "artifacts")
	cache = SqliteExecutionCache(str(tmp_path / "artifacts" / "meta" / "artifacts.sqlite"))
	events: list[dict] = []
	await run_graph(
		run_id="run-source-large-payload",
		graph=_graph_api_source({"url": "https://example.com/data", "method": "GET", "output": {"mode": "json"}}),
		run_from=None,
		bus=RunEventBus("run-source-large-payload", on_emit=lambda e: events.append(dict(e))),
		artifact_store=store,
		cache=cache,
		graph_id="graph-source-large-payload",
	)
	out = [e for e in events if e.get("type") == "node_output" and e.get("nodeId") == "src_api"]
	assert out
	artifact_id = str(out[-1].get("artifactId") or "")
	meta = await store.get(artifact_id)
	assert int(meta.size_bytes) <= int(input_bytes * 3)


@pytest.mark.asyncio
async def test_source_partition_observability_contains_per_partition_outcomes(monkeypatch, tmp_path):
	class _Resp:
		status_code = 200
		headers = {"content-type": "application/json"}

		def raise_for_status(self):
			return None

		def json(self):
			return [{"id": 1}]

		@property
		def text(self):
			return '[{"id":1}]'

	class _Client:
		async def __aenter__(self):
			return self

		async def __aexit__(self, exc_type, exc, tb):
			return False

		async def request(self, **kwargs):
			query = parse_qs(urlparse(str(kwargs.get("url", ""))).query)
			part = str((query.get("partition") or [""])[0])
			if part == "2":
				raise httpx.ConnectError("partition down", request=httpx.Request("GET", kwargs.get("url", "https://example.com")))
			return _Resp()

	monkeypatch.setattr("app.executors.source.httpx.AsyncClient", _Client)
	store = DiskArtifactStore(tmp_path / "artifacts")
	cache = SqliteExecutionCache(str(tmp_path / "artifacts" / "meta" / "artifacts.sqlite"))
	events: list[dict] = []
	await run_graph(
		run_id="run-source-partition-observability",
		graph=_graph_api_source(
			{
				"url": "https://example.com/data",
				"method": "GET",
				"output": {"mode": "json"},
				"partition": {
					"enabled": True,
					"kind": "static_list",
					"static_values": [1, 2, 3],
					"on_error": "skip_failed",
					"bind_key": "partition",
					"parallelism_cap": 2,
				},
				"retry": {"max_attempts": 1},
			}
		),
		run_from=None,
		bus=RunEventBus("run-source-partition-observability", on_emit=lambda e: events.append(dict(e))),
		artifact_store=store,
		cache=cache,
		graph_id="graph-source-partition-observability",
	)
	out = [e for e in events if e.get("type") == "node_output" and e.get("nodeId") == "src_api"]
	assert out
	obs = out[-1].get("sourceObservability") if isinstance(out[-1], dict) else {}
	assert isinstance(obs, dict)
	assert int(obs.get("partition_count") or 0) == 3
	assert int(obs.get("retry_count") or 0) >= 0
