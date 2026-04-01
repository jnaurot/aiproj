from fastapi.testclient import TestClient

from app.main import app


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
