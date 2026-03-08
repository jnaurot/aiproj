from __future__ import annotations

import pytest

from app.runner.artifacts import Artifact, MemoryArtifactStore
from app.runner.schema_infer import get_schema_infer_stats, infer_json_schema_cached


@pytest.mark.asyncio
async def test_memory_artifact_store_memo_stats_track_hits():
	store = MemoryArtifactStore()
	artifact = Artifact(
		artifact_id="aid_1",
		node_kind="tool",
		params_hash="p1",
		upstream_ids=[],
		created_at=__import__("datetime").datetime.now(__import__("datetime").timezone.utc),
		execution_version="v1",
		mime_type="application/json",
		port_type="json",
		size_bytes=0,
		storage_uri="memory://aid_1",
		payload_schema={"schema_version": 1, "type": "json"},
	)
	await store.write(artifact, b'{"ok":true}')
	await store.get("aid_1")
	await store.get("aid_1")
	await store.read("aid_1")
	await store.read("aid_1")
	stats = store.get_memo_stats()
	assert stats["meta_hit"] >= 1
	assert stats["blob_hit"] >= 1
	assert stats["meta_entries"] >= 1
	assert stats["blob_entries"] >= 1


def test_json_schema_infer_cache_records_hits():
	before = get_schema_infer_stats()
	payload = {"a": 1, "b": [{"x": "y"}]}
	infer_json_schema_cached(payload)
	infer_json_schema_cached(payload)
	after = get_schema_infer_stats()
	assert int(after["hit"]) >= int(before["hit"]) + 1
