import importlib
import secrets
import sys
import types
from pathlib import Path

import pytest

if "duckdb" not in sys.modules:
    sys.modules["duckdb"] = types.SimpleNamespace()

from app.runner.metadata import NodeOutput
from app.runtime import RuntimeManager


def _tool_graph(node_id: str = "tool_1") -> dict:
    return {
        "nodes": [
            {
                "id": node_id,
                "data": {
                    "kind": "tool",
                    "label": "Tool",
                    "params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {}}},
                    "ports": {"in": None, "out": "json"},
                },
            }
        ],
        "edges": [],
    }


@pytest.mark.asyncio
async def test_global_mark_sweep_keeps_referenced_and_collects_dangling(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"kind": "json", "payload": {"ok": True}, "meta": {"status": "ok"}},
        )

    monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
    monkeypatch.setenv("ARTIFACT_STORE", "disk")
    monkeypatch.setenv("ARTIFACT_DIR", str(tmp_path / "artifact-root"))

    rt = RuntimeManager()
    run_id = "run-gc-global-1"
    rt.create_run(run_id)
    await rt.start_run(run_id, _tool_graph(), run_from=None)
    await rt.get_run(run_id).task

    handle = rt.get_run(run_id)
    artifact_id = handle.node_outputs["tool_1"]
    art = await rt.artifact_store.get(artifact_id)
    referenced_blob_path = Path(art.storage_uri)
    assert referenced_blob_path.exists()

    # Create a dangling blob that is not referenced by metadata.
    dangling_hash = secrets.token_hex(32)
    dangling_path = rt.artifact_store._blob_path(dangling_hash)  # test-only private access
    dangling_path.parent.mkdir(parents=True, exist_ok=True)
    dangling_path.write_bytes(b"dangling")
    assert dangling_path.exists()

    dry = await rt.artifact_store.gc_orphan_blobs(mode="dry_run")
    assert dry["mode"] == "dry_run"
    assert dry["blobs_deleted"] == 0
    assert dangling_hash in dry["orphan_hashes"]
    assert art.content_hash not in dry["orphan_hashes"]

    done = await rt.artifact_store.gc_orphan_blobs(mode="delete")
    assert done["mode"] == "delete"
    assert dangling_hash in done["orphan_hashes"]
    assert done["blobs_deleted"] >= 1

    # Safety: referenced blob is preserved.
    assert referenced_blob_path.exists()
    assert await rt.artifact_store.exists(artifact_id)
    # Liveness: dangling blob is eventually collected.
    assert not dangling_path.exists()

