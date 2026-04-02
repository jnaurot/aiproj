from fastapi.testclient import TestClient

from app.main import app
from app.runner.node_state import build_exec_key, build_node_state_hash, build_source_fingerprint
from app.runner.run import _determinism_env_for_node, _normalized_params_for_exec_key
from app.runner.artifacts import Artifact
from datetime import datetime, timezone


def _base_source_node(node_id: str, source_kind: str, params: dict) -> dict:
	return {
		"id": node_id,
		"data": {
			"kind": "source",
			"label": "Source",
			"sourceKind": source_kind,
			"params": params,
		},
	}


def test_resolve_source_all_kinds_returns_normalized_envelope():
	graph = {
		"version": 1,
		"nodes": [
			_base_source_node("src_file", "file", {"snapshotId": "a" * 64, "file_format": "txt"}),
			_base_source_node("src_db", "database", {"connection_ref": "conn:db", "query": "select 1"}),
			_base_source_node("src_api", "api", {"url": "https://example.com", "method": "GET"}),
			_base_source_node(
				"src_obj",
				"object_store",
				{"bucket": "demo", "key": "data.txt", "file_format": "txt", "connection_ref": "conn:obj"},
			),
			_base_source_node("src_wh", "warehouse", {"connection_ref": "conn:warehouse", "query": "select 1"}),
		],
		"edges": [],
	}

	with TestClient(app) as client:
		for node_id, source_kind in [
			("src_file", "file"),
			("src_db", "database"),
			("src_api", "api"),
			("src_obj", "object_store"),
			("src_wh", "warehouse"),
		]:
			res = client.post(
				"/runs/resolve/source",
				json={"graphId": "graph-resolve-all-kinds", "graph": graph, "nodeId": node_id},
			)
			assert res.status_code == 200, res.text
			body = res.json()
			assert body.get("graphId") == "graph-resolve-all-kinds"
			assert body.get("nodeId") == node_id
			assert body.get("sourceKind") == source_kind
			assert isinstance(body.get("execKey"), str) and body.get("execKey")
			assert "cacheHit" in body
			assert isinstance(body.get("resolutionMeta"), dict)
			assert "runtimeCacheMode" in body.get("resolutionMeta")
			assert "sourceCacheEnabled" in body.get("resolutionMeta")


def test_resolve_source_missing_required_params_returns_400():
	graph = {
		"version": 1,
		"nodes": [
			_base_source_node("src_bad_api", "api", {"method": "GET"}),
		],
		"edges": [],
	}
	with TestClient(app) as client:
		res = client.post(
			"/runs/resolve/source",
			json={"graphId": "graph-resolve-bad-api", "graph": graph, "nodeId": "src_bad_api"},
		)
		assert res.status_code == 400, res.text
		body = res.json()
		assert isinstance(body.get("detail"), dict)
		assert str((body.get("detail") or {}).get("errorCode") or "") == "SOURCE_RESOLVE_CONFIG_INVALID"
		assert str((body.get("detail") or {}).get("sourceKind") or "") == "api"


def _resolve_payload_for(node_id: str, source_kind: str, params: dict) -> dict:
	return {
		"graphId": "graph-resolve-parity",
		"graph": {
			"version": 1,
			"nodes": [_base_source_node(node_id, source_kind, params)],
			"edges": [],
		},
		"nodeId": node_id,
	}


def _expected_exec_key(graph_id: str, node_id: str, source_kind: str, params: dict) -> str:
	node = _base_source_node(node_id, source_kind, params)
	params_raw = dict(params or {})
	params_raw["source_type"] = source_kind
	normalized = _normalized_params_for_exec_key(kind="source", node=node, params=params_raw)
	determinism_env = _determinism_env_for_node("source", normalized)
	source_fingerprint = build_source_fingerprint(node, normalized)
	node_state_hash = build_node_state_hash(
		node=node,
		params=normalized,
		execution_version="v1",
		source_fingerprint=source_fingerprint,
	)
	return build_exec_key(
		graph_id=graph_id,
		node_id=node_id,
		node_kind="source",
		node_state_hash=node_state_hash,
		upstream_artifact_ids=[],
		input_refs=[],
		determinism_env=determinism_env,
		execution_version="v1",
		node_impl_version="SOURCE@1",
	)


def test_resolve_source_file_exec_key_parity():
	payload = _resolve_payload_for("src_file_parity", "file", {"snapshotId": "b" * 64, "file_format": "txt"})
	with TestClient(app) as client:
		res = client.post("/runs/resolve/source", json=payload)
		assert res.status_code == 200, res.text
		body = res.json()
		expected = _expected_exec_key("graph-resolve-parity", "src_file_parity", "file", {"snapshotId": "b" * 64, "file_format": "txt"})
		assert body.get("execKey") == expected


def test_resolve_source_database_exec_key_parity():
	params = {"connection_ref": "conn:db", "query": "select 1"}
	payload = _resolve_payload_for("src_db_parity", "database", params)
	with TestClient(app) as client:
		res = client.post("/runs/resolve/source", json=payload)
		assert res.status_code == 200, res.text
		body = res.json()
		expected = _expected_exec_key("graph-resolve-parity", "src_db_parity", "database", params)
		assert body.get("execKey") == expected


def test_resolve_source_api_exec_key_parity():
	params = {"url": "https://example.com/items", "method": "GET"}
	payload = _resolve_payload_for("src_api_parity", "api", params)
	with TestClient(app) as client:
		res = client.post("/runs/resolve/source", json=payload)
		assert res.status_code == 200, res.text
		body = res.json()
		expected = _expected_exec_key("graph-resolve-parity", "src_api_parity", "api", params)
		assert body.get("execKey") == expected


def test_resolve_source_object_store_exec_key_parity():
	params = {"connection_ref": "conn:obj", "bucket": "jobs", "key": "rows.json", "file_format": "json"}
	payload = _resolve_payload_for("src_obj_parity", "object_store", params)
	with TestClient(app) as client:
		res = client.post("/runs/resolve/source", json=payload)
		assert res.status_code == 200, res.text
		body = res.json()
		expected = _expected_exec_key("graph-resolve-parity", "src_obj_parity", "object_store", params)
		assert body.get("execKey") == expected


def test_resolve_source_warehouse_exec_key_parity():
	params = {"connection_ref": "conn:wh", "query": "select 1 as ok"}
	payload = _resolve_payload_for("src_wh_parity", "warehouse", params)
	with TestClient(app) as client:
		res = client.post("/runs/resolve/source", json=payload)
		assert res.status_code == 200, res.text
		body = res.json()
		expected = _expected_exec_key("graph-resolve-parity", "src_wh_parity", "warehouse", params)
		assert body.get("execKey") == expected


def test_resolve_source_cache_hit_truth_matches_artifact_store(monkeypatch):
	params = {"snapshotId": "c" * 64, "file_format": "txt"}
	payload = _resolve_payload_for("src_cache_truth", "file", params)
	expected_exec = _expected_exec_key("graph-resolve-parity", "src_cache_truth", "file", params)

	with TestClient(app) as client:
		rt = client.app.state.runtime
		store = rt.artifact_store

		async def _exists_true(_artifact_id: str) -> bool:
			return True

		async def _get_hit(_artifact_id: str):
			return Artifact(
				artifact_id=expected_exec,
				node_kind="source",
				params_hash="p",
				upstream_ids=[],
				created_at=datetime.now(timezone.utc),
				execution_version="v1",
				mime_type="text/plain",
				payload_type="text",
				size_bytes=12,
				storage_uri="memory://resolve-cache-hit",
				payload_schema={"type": "text"},
				content_hash="h",
				graph_id="graph-resolve-parity",
				node_id="src_cache_truth",
				exec_key=expected_exec,
			)

		monkeypatch.setattr(store, "exists", _exists_true)
		monkeypatch.setattr(store, "get", _get_hit)
		res = client.post("/runs/resolve/source", json=payload)
		assert res.status_code == 200, res.text
		body = res.json()
		assert body.get("cacheHit") is True
		assert body.get("artifactId") == expected_exec
