import json

import pytest

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.run import run_graph


def _graph_object_store_source(*, params: dict) -> dict:
	return {
		"nodes": [
			{
				"id": "src_obj",
				"data": {
					"kind": "source",
					"sourceKind": "object_store",
					"label": "ObjectStore",
					"params": params,
				},
			}
		],
		"edges": [],
	}


async def _run_and_read_object_store_output(*, tmp_path, graph: dict, run_id: str):
	events: list[dict] = []
	store = DiskArtifactStore(tmp_path / "artifacts")
	cache = SqliteExecutionCache(str(tmp_path / "artifacts" / "meta" / "artifacts.sqlite"))
	await run_graph(
		run_id=run_id,
		graph=graph,
		run_from=None,
		bus=RunEventBus(run_id, on_emit=lambda e: events.append(dict(e))),
		artifact_store=store,
		cache=cache,
		graph_id="graph-source-object-store",
	)
	node_finished = [e for e in events if e.get("type") == "node_finished" and e.get("nodeId") == "src_obj"]
	assert node_finished and node_finished[-1].get("status") == "succeeded"
	node_output = [e for e in events if e.get("type") == "node_output" and e.get("nodeId") == "src_obj"]
	assert node_output
	artifact_id = str(node_output[-1].get("artifactId") or "")
	assert artifact_id
	meta = await store.get(artifact_id)
	payload = await store.read(artifact_id)
	return payload, meta


@pytest.mark.asyncio
async def test_source_object_store_provider_mode_end_to_end_json(monkeypatch, tmp_path):
	monkeypatch.setenv("OBJ_CONN_IT_JSON", '{"region_name":"us-east-1"}')
	monkeypatch.setattr(
		"app.executors.source._download_object_store_provider_bytes",
		lambda *_args, **_kwargs: b'{"id":1,"name":"alice"}',
	)
	graph = _graph_object_store_source(
		params={
			"provider": "s3",
			"object_store_mode": "provider",
			"connection_ref": "OBJ_CONN_IT_JSON",
			"bucket": "demo",
			"key": "obj.json",
			"file_format": "json",
		}
	)
	payload, meta = await _run_and_read_object_store_output(
		tmp_path=tmp_path, graph=graph, run_id="run-source-object-store-provider-json"
	)
	decoded = json.loads(payload.decode("utf-8"))
	assert isinstance(decoded, dict)
	assert decoded.get("id") == 1
	source_obs = ((meta.payload_schema or {}).get("source_observability") or {})
	assert source_obs.get("source_kind") == "object_store"
	assert source_obs.get("object_store_mode") == "provider"


@pytest.mark.asyncio
async def test_source_object_store_provider_mode_end_to_end_table_csv(monkeypatch, tmp_path):
	monkeypatch.setenv("OBJ_CONN_IT_TABLE", '{"region_name":"us-east-1"}')
	monkeypatch.setattr(
		"app.executors.source._download_object_store_provider_bytes",
		lambda *_args, **_kwargs: b"id,name\n1,alice\n2,bob\n",
	)
	graph = _graph_object_store_source(
		params={
			"provider": "s3",
			"object_store_mode": "provider",
			"connection_ref": "OBJ_CONN_IT_TABLE",
			"bucket": "demo",
			"key": "rows.csv",
			"file_format": "csv",
		}
	)
	payload, meta = await _run_and_read_object_store_output(
		tmp_path=tmp_path, graph=graph, run_id="run-source-object-store-provider-table"
	)
	text = payload.decode("utf-8")
	assert "id,name" in text
	assert "alice" in text
	assert (((meta.payload_schema or {}).get("source_observability") or {}).get("output_mode") == "table")


@pytest.mark.asyncio
async def test_source_object_store_provider_mode_end_to_end_binary(monkeypatch, tmp_path):
	monkeypatch.setenv("OBJ_CONN_IT_BIN", '{"region_name":"us-east-1"}')
	monkeypatch.setattr(
		"app.executors.source._download_object_store_provider_bytes",
		lambda *_args, **_kwargs: b"\x89PNG\r\n\x1a\n",
	)
	graph = _graph_object_store_source(
		params={
			"provider": "s3",
			"object_store_mode": "provider",
			"connection_ref": "OBJ_CONN_IT_BIN",
			"bucket": "demo",
			"key": "logo.png",
			"file_format": "png",
		}
	)
	payload, meta = await _run_and_read_object_store_output(
		tmp_path=tmp_path, graph=graph, run_id="run-source-object-store-provider-binary"
	)
	assert payload.startswith(b"\x89PNG")
	assert (((meta.payload_schema or {}).get("source_observability") or {}).get("output_mode") == "binary")


@pytest.mark.asyncio
async def test_source_object_store_mock_mode_end_to_end(monkeypatch, tmp_path):
	mock_root = tmp_path / "mock-root"
	(mock_root / "demo").mkdir(parents=True, exist_ok=True)
	(mock_root / "demo" / "rows.csv").write_text("id,name\n1,alice\n2,bob\n", encoding="utf-8")
	monkeypatch.setenv("OBJECT_STORE_MOCK_ROOT", str(mock_root))
	graph = _graph_object_store_source(
		params={
			"provider": "s3",
			"object_store_mode": "mock",
			"bucket": "demo",
			"key": "rows.csv",
			"file_format": "csv",
		}
	)
	payload, meta = await _run_and_read_object_store_output(
		tmp_path=tmp_path, graph=graph, run_id="run-source-object-store-mock"
	)
	text = payload.decode("utf-8")
	assert "id,name" in text
	assert "alice" in text
	assert ((meta.payload_schema or {}).get("source_observability") or {}).get("object_store_mode") == "mock"
