import hashlib
from pathlib import Path
import sys
import types
import time

import pytest
from fastapi.testclient import TestClient

if "duckdb" not in sys.modules:
	sys.modules["duckdb"] = types.SimpleNamespace()

from app.main import app
from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus


def test_snapshot_upload_is_content_addressed_and_readable(monkeypatch, tmp_path):
	artifact_dir = tmp_path / "artifacts"
	monkeypatch.setenv("ARTIFACT_STORE", "disk")
	monkeypatch.setenv("ARTIFACT_DIR", str(artifact_dir))

	payload = b"hello snapshot"
	expected_id = hashlib.sha256(payload).hexdigest()
	with TestClient(app) as client:
		r1 = client.post(
			"/snapshots",
			files={"file": ("hello.txt", payload, "text/plain")},
		)
		assert r1.status_code == 200, r1.text
		body1 = r1.json()
		assert body1["snapshotId"] == expected_id
		assert body1["metadata"]["originalFilename"] == "hello.txt"

		r2 = client.post(
			"/snapshots",
			files={"file": ("renamed.txt", payload, "text/plain")},
		)
		assert r2.status_code == 200, r2.text
		body2 = r2.json()
		assert body2["snapshotId"] == expected_id

		raw = client.get(f"/runs/artifacts/{expected_id}?graphId=__snapshots__")
		assert raw.status_code == 200
		assert raw.content == payload

		meta = client.get(f"/runs/artifacts/{expected_id}/meta?graphId=__snapshots__")
		assert meta.status_code == 200
		assert meta.json()["artifactId"] == expected_id
		snap_meta = client.get(f"/snapshots/{expected_id}")
		assert snap_meta.status_code == 200
		assert snap_meta.json()["metadata"]["snapshotId"] == expected_id
		assert snap_meta.json()["metadata"]["byteSize"] == len(payload)
		snap_meta_via_meta = client.get(f"/snapshots/{expected_id}/meta")
		assert snap_meta_via_meta.status_code == 200
		assert snap_meta_via_meta.json()["metadata"]["snapshotId"] == expected_id


@pytest.mark.asyncio
async def test_snapshot_metadata_persists_across_store_restart(tmp_path):
	artifact_root = tmp_path / "artifact-root"
	store_1 = DiskArtifactStore(artifact_root)
	payload = b"persistent snapshot bytes"
	snapshot_id = hashlib.sha256(payload).hexdigest()
	src_file = tmp_path / "source.bin"
	src_file.write_bytes(payload)
	meta = {
		"snapshotId": snapshot_id,
		"originalFilename": "source.bin",
		"byteSize": len(payload),
		"mimeType": "application/octet-stream",
		"importedAt": "2026-02-27T00:00:00Z",
		"graphId": "__snapshots__",
	}
	await store_1.write_snapshot_from_file(
		snapshot_id=snapshot_id,
		file_path=src_file,
		metadata=meta,
		mime_type="application/octet-stream",
	)

	store_2 = DiskArtifactStore(artifact_root)
	assert await store_2.exists(snapshot_id) is True
	assert await store_2.read(snapshot_id) == payload
	meta_2 = await store_2.get_snapshot_metadata(snapshot_id)
	assert isinstance(meta_2, dict)
	assert meta_2.get("snapshotId") == snapshot_id
	assert int(meta_2.get("byteSize", -1)) == len(payload)


def test_snapshot_meta_endpoint_persists_after_backend_restart(monkeypatch, tmp_path):
	artifact_dir = tmp_path / "artifacts"
	monkeypatch.setenv("ARTIFACT_STORE", "disk")
	monkeypatch.setenv("ARTIFACT_DIR", str(artifact_dir))
	payload = b"restart metadata payload"
	snapshot_id = hashlib.sha256(payload).hexdigest()

	with TestClient(app) as client:
		r = client.post(
			"/snapshots",
			files={"file": ("README.md", payload, "text/plain")},
		)
		assert r.status_code == 200
		assert r.json()["snapshotId"] == snapshot_id

	with TestClient(app) as client:
		meta = client.get(f"/snapshots/{snapshot_id}/meta")
		assert meta.status_code == 200
		body = meta.json()
		assert body["snapshotId"] == snapshot_id
		assert body["metadata"]["originalFilename"] == "README.md"
		assert int(body["metadata"]["byteSize"]) == len(payload)
		assert isinstance(body["metadata"]["importedAt"], str) and body["metadata"]["importedAt"]


def test_resolve_source_exec_key_and_cache_hit_by_snapshot(monkeypatch, tmp_path):
	artifact_dir = tmp_path / "artifacts"
	monkeypatch.setenv("ARTIFACT_STORE", "disk")
	monkeypatch.setenv("ARTIFACT_DIR", str(artifact_dir))

	payload_a = b"snapshot-A-content"
	payload_b = b"snapshot-B-content"
	with TestClient(app) as client:
		ra = client.post("/snapshots", files={"file": ("A.txt", payload_a, "text/plain")})
		rb = client.post("/snapshots", files={"file": ("B.txt", payload_b, "text/plain")})
		assert ra.status_code == 200 and rb.status_code == 200
		sid_a = ra.json()["snapshotId"]
		sid_b = rb.json()["snapshotId"]

		graph = {
			"version": 1,
			"nodes": [
				{
					"id": "source_1",
					"data": {
						"kind": "source",
						"sourceKind": "file",
						"params": {"snapshotId": sid_a, "file_format": "txt", "encoding": "utf-8"},
					},
				}
			],
			"edges": [],
		}
		run_req = {
			"graphId": "graph-resolve-source",
			"runFrom": None,
			"runMode": "from_selected_onward",
			"graph": graph,
		}
		run_res = client.post("/runs", json=run_req)
		assert run_res.status_code == 200, run_res.text
		run_id = run_res.json()["runId"]
		for _ in range(200):
			st = client.get(f"/runs/{run_id}")
			assert st.status_code == 200
			if st.json().get("status") in {"succeeded", "failed", "cancelled"}:
				break
			time.sleep(0.01)
		assert st.json().get("status") == "succeeded"

		resolved_a = client.post(
			"/runs/resolve/source",
			json={"graphId": "graph-resolve-source", "graph": graph, "nodeId": "source_1"},
		)
		assert resolved_a.status_code == 200, resolved_a.text
		body_a = resolved_a.json()
		assert body_a["snapshotId"] == sid_a
		assert body_a["cacheHit"] is True
		assert isinstance(body_a["artifactId"], str) and body_a["artifactId"]
		exec_a = body_a["execKey"]

		graph_b = {
			**graph,
			"nodes": [
				{
					**graph["nodes"][0],
					"data": {
						**graph["nodes"][0]["data"],
						"params": {"snapshotId": sid_b, "file_format": "txt", "encoding": "utf-8"},
					},
				}
			],
		}
		resolved_b = client.post(
			"/runs/resolve/source",
			json={"graphId": "graph-resolve-source", "graph": graph_b, "nodeId": "source_1"},
		)
		assert resolved_b.status_code == 200, resolved_b.text
		body_b = resolved_b.json()
		assert body_b["snapshotId"] == sid_b
		assert body_b["execKey"] != exec_a
		assert body_b["artifactId"] is None
		assert body_b["cacheHit"] is False


@pytest.mark.asyncio
async def test_source_file_node_uses_snapshot_id_and_caches(monkeypatch, tmp_path):
	from app.runner import run as run_mod

	artifact_root = tmp_path / "artifact-root"
	store = DiskArtifactStore(artifact_root)
	cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))

	source_bytes = b"snapshot text body"
	snapshot_id = hashlib.sha256(source_bytes).hexdigest()
	snapshot_file = tmp_path / "upload.bin"
	snapshot_file.write_bytes(source_bytes)
	await store.write_snapshot_from_file(
		snapshot_id=snapshot_id,
		file_path=snapshot_file,
		metadata={
			"snapshotId": snapshot_id,
			"originalFilename": "upload.txt",
			"byteSize": len(source_bytes),
			"mimeType": "text/plain",
			"importedAt": "2026-01-01T00:00:00Z",
			"graphId": "__snapshots__",
		},
		mime_type="text/plain",
	)

	graph = {
		"nodes": [
			{
				"id": "source_1",
				"data": {
					"kind": "source",
					"label": "Source",
					"sourceKind": "file",
					"params": {"snapshotId": snapshot_id, "file_format": "txt", "encoding": "utf-8"},
				},
			}
		],
		"edges": [],
	}

	events_1: list[dict] = []
	await run_mod.run_graph(
		run_id="run-snapshot-1",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-snapshot-1", on_emit=lambda e: events_1.append(dict(e))),
		artifact_store=store,
		cache=cache,
		graph_id="graph-snapshot-source",
	)
	assert any(e.get("type") == "log" and "Using snapshotId=" in str(e.get("message")) for e in events_1)
	assert any(e.get("type") == "node_output" and e.get("nodeId") == "source_1" for e in events_1)

	events_2: list[dict] = []
	await run_mod.run_graph(
		run_id="run-snapshot-2",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-snapshot-2", on_emit=lambda e: events_2.append(dict(e))),
		artifact_store=store,
		cache=cache,
		graph_id="graph-snapshot-source",
	)
	assert any(
		e.get("type") == "cache_decision"
		and e.get("nodeId") == "source_1"
		and e.get("decision") == "cache_hit"
		for e in events_2
	)
