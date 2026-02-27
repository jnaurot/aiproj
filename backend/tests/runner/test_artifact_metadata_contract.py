from datetime import datetime, timezone

import pytest

from app.runner.artifacts import Artifact, MemoryArtifactStore


def _artifact_base(payload_schema):
    return Artifact(
        artifact_id="a" * 64,
        node_kind="source",
        params_hash="p" * 64,
        upstream_ids=[],
        created_at=datetime.now(timezone.utc),
        execution_version="v1",
        mime_type="text/plain; charset=utf-8",
        port_type="text",
        size_bytes=0,
        storage_uri="artifact://placeholder",
        payload_schema=payload_schema,
        run_id="run-1",
        graph_id="graph-1",
        node_id="n1",
        exec_key="a" * 64,
    )


@pytest.mark.asyncio
async def test_artifact_write_fails_when_metadata_v1_missing_required_fields():
    store = MemoryArtifactStore()
    art = _artifact_base(
        {
            "schema_version": 1,
            "type": "text",
            "artifactMetadataV1": {
                "metadataVersion": 1,
                "execKey": "a" * 64,
            },
        }
    )
    with pytest.raises(ValueError):
        await store.write(art, b"hello")


@pytest.mark.asyncio
async def test_artifact_write_succeeds_with_metadata_v1():
    store = MemoryArtifactStore()
    art = _artifact_base(
        {
            "schema_version": 1,
            "type": "text",
            "artifactMetadataV1": {
                "metadataVersion": 1,
                "execKey": "a" * 64,
                "nodeId": "n1",
                "nodeType": "source",
                "nodeImplVersion": "SOURCE@1",
                "paramsFingerprint": "p" * 64,
                "upstreamArtifactIds": [],
                "contractFingerprint": "c" * 64,
                "mimeType": "text/plain; charset=utf-8",
                "portType": "text",
                "createdAt": datetime.now(timezone.utc).isoformat(),
                "runId": "run-1",
                "graphId": "graph-1",
            },
        }
    )
    out = await store.write(art, b"hello")
    assert out == "a" * 64
