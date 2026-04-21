"""Integration tests for the by_node artifact retention mode.

by_node retention keeps the N most-recent distinct artifacts per
(graph_id, node_id) pair, regardless of which run produced them.
Checkpoint-pinned artifacts are always preserved.

Key properties verified here:
- Default mode is by_node
- Same artifact reused by many runs survives as long as it is the latest
- Sibling nodes are independent: A-only runs don't age out B's artifact
- Old (superseded) artifacts are pruned once a newer one exists
- Checkpoint-pinned artifacts survive even when beyond keep_versions
- Non-runtime artifacts (no graph_id / node_id) are not touched
- Non-terminal runs do not trigger a sweep
- off mode disables all pruning
- Both MemoryArtifactStore and DiskArtifactStore behave identically
"""
import os
from datetime import datetime, timezone

import pytest

from app.runner.artifacts import Artifact, DiskArtifactStore, MemoryArtifactStore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts(offset_ms: int = 0) -> str:
    """Return a stable ISO timestamp offset by *offset_ms* milliseconds so
    that ordering within a test is deterministic even if the clock doesn't
    advance between writes."""
    base = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    from datetime import timedelta
    return (base + timedelta(milliseconds=offset_ms)).isoformat()


def _mk_artifact(
    *,
    artifact_id: str,
    run_id: str,
    graph_id: str,
    node_id: str,
    created_at: str | None = None,
) -> Artifact:
    ts = created_at or datetime.now(timezone.utc).isoformat()
    return Artifact(
        artifact_id=artifact_id,
        node_kind="transform",
        params_hash="ph",
        upstream_ids=[],
        created_at=datetime.fromisoformat(ts),
        execution_version="v1",
        mime_type="application/json",
        payload_type="json",
        size_bytes=4,
        storage_uri="memory://placeholder",
        payload_schema={
            "type": "json",
            "artifactMetadataV1": {
                "metadataVersion": 1,
                "execKey": artifact_id,
                "nodeId": node_id,
                "nodeType": "transform",
                "nodeImplVersion": "v1",
                "paramsFingerprint": "p",
                "upstreamArtifactIds": [],
                "contractFingerprint": "c",
                "schemaFingerprint": "s",
                "mimeType": "application/json",
                "payloadType": "json",
                "createdAt": ts,
            },
        },
        run_id=run_id,
        graph_id=graph_id,
        node_id=node_id,
        exec_key=artifact_id,
    )


async def _write(store, *, artifact_id: str, run_id: str, graph_id: str, node_id: str, created_at: str | None = None):
    art = _mk_artifact(artifact_id=artifact_id, run_id=run_id, graph_id=graph_id, node_id=node_id, created_at=created_at)
    await store.write(art, b'{"v":1}')
    return artifact_id


# ---------------------------------------------------------------------------
# Default mode
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_by_node_is_default_mode(monkeypatch):
    """ARTIFACT_RETENTION_MODE defaults to by_node."""
    monkeypatch.delenv("ARTIFACT_RETENTION_MODE", raising=False)
    from app.runner import artifacts as art_mod
    assert art_mod._retention_mode() == "by_node"


# ---------------------------------------------------------------------------
# Core: same exec_key used by many runs is never pruned
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_by_node_single_artifact_reused_across_runs_survives(monkeypatch):
    """A node whose exec_key never changes produces exactly one artifact.
    That artifact must survive no matter how many runs reference it via
    cache hits — only one artifact object exists in the store."""
    monkeypatch.setenv("ARTIFACT_RETENTION_MODE", "by_node")
    monkeypatch.setenv("ARTIFACT_KEEP_NODE_VERSIONS", "2")
    store = MemoryArtifactStore()
    graph_id = "g-reuse"

    # Run 1 writes the artifact.
    await store.record_run("r1", "running")
    await _write(store, artifact_id="exec-key-abc", run_id="r1", graph_id=graph_id, node_id="n-a", created_at=_ts(0))
    await store.update_run_status("r1", "succeeded")

    # Runs 2–6 are cache hits — the runner calls exists(exec_key) = True and
    # does NOT write a new artifact.  We simulate 5 more terminal runs without
    # any new write for n-a.
    for i in range(2, 7):
        await store.record_run(f"r{i}", "running")
        # Different node to create a real run record without touching n-a.
        await _write(store, artifact_id=f"exec-key-other-r{i}", run_id=f"r{i}", graph_id=graph_id, node_id="n-b", created_at=_ts(i * 10))
        await store.update_run_status(f"r{i}", "succeeded")

    # The original artifact for n-a is still the only / latest one → survives.
    assert await store.exists("exec-key-abc"), (
        "single artifact reused by many runs must not be pruned by by_node retention"
    )


@pytest.mark.asyncio
async def test_by_node_single_artifact_reused_across_runs_disk(monkeypatch, tmp_path):
    """Same test as above for DiskArtifactStore."""
    monkeypatch.setenv("ARTIFACT_RETENTION_MODE", "by_node")
    monkeypatch.setenv("ARTIFACT_KEEP_NODE_VERSIONS", "2")
    store = DiskArtifactStore(tmp_path / "store-reuse")
    graph_id = "g-reuse-disk"

    await store.record_run("r1", "running")
    await _write(store, artifact_id="exec-key-disk-abc", run_id="r1", graph_id=graph_id, node_id="n-a", created_at=_ts(0))
    await store.update_run_status("r1", "succeeded")

    for i in range(2, 7):
        await store.record_run(f"r{i}", "running")
        await _write(store, artifact_id=f"exec-key-disk-other-r{i}", run_id=f"r{i}", graph_id=graph_id, node_id="n-b", created_at=_ts(i * 10))
        await store.update_run_status(f"r{i}", "succeeded")

    assert await store.exists("exec-key-disk-abc"), (
        "DiskArtifactStore: single artifact reused by many runs must not be pruned"
    )


# ---------------------------------------------------------------------------
# Core: sibling nodes are independent
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_by_node_run_from_selected_does_not_prune_sibling(monkeypatch):
    """'Run from selected A' must not age out node B's artifact.
    This reproduces the original bug: 5 A-only runs pushing B's artifact
    out of the run-based window."""
    monkeypatch.setenv("ARTIFACT_RETENTION_MODE", "by_node")
    monkeypatch.setenv("ARTIFACT_KEEP_NODE_VERSIONS", "2")
    store = MemoryArtifactStore()
    graph_id = "g-sibling"

    # Run 1: node B executes and produces its artifact.
    await store.record_run("r1", "running")
    await _write(store, artifact_id="exec-node-b-v1", run_id="r1", graph_id=graph_id, node_id="n-b", created_at=_ts(0))
    await store.update_run_status("r1", "succeeded")

    # Runs 2–6: only node A executes (run-from-selected A).
    for i in range(2, 7):
        await store.record_run(f"r{i}", "running")
        await _write(store, artifact_id=f"exec-node-a-r{i}", run_id=f"r{i}", graph_id=graph_id, node_id="n-a", created_at=_ts(i * 10))
        await store.update_run_status(f"r{i}", "succeeded")

    # Node B's artifact is still the latest for (g-sibling, n-b) → survives.
    assert await store.exists("exec-node-b-v1"), (
        "node B artifact must survive 5 A-only 'run-from-selected' runs"
    )


@pytest.mark.asyncio
async def test_by_node_run_from_selected_does_not_prune_sibling_disk(monkeypatch, tmp_path):
    """Same sibling-independence test for DiskArtifactStore."""
    monkeypatch.setenv("ARTIFACT_RETENTION_MODE", "by_node")
    monkeypatch.setenv("ARTIFACT_KEEP_NODE_VERSIONS", "2")
    store = DiskArtifactStore(tmp_path / "store-sibling")
    graph_id = "g-sibling-disk"

    await store.record_run("r1", "running")
    await _write(store, artifact_id="exec-node-b-disk-v1", run_id="r1", graph_id=graph_id, node_id="n-b", created_at=_ts(0))
    await store.update_run_status("r1", "succeeded")

    for i in range(2, 7):
        await store.record_run(f"r{i}", "running")
        await _write(store, artifact_id=f"exec-node-a-disk-r{i}", run_id=f"r{i}", graph_id=graph_id, node_id="n-a", created_at=_ts(i * 10))
        await store.update_run_status(f"r{i}", "succeeded")

    assert await store.exists("exec-node-b-disk-v1"), (
        "DiskArtifactStore: node B artifact must survive A-only runs"
    )


# ---------------------------------------------------------------------------
# Core: superseded artifacts are pruned
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_by_node_superseded_artifact_is_pruned(monkeypatch):
    """When keep_versions=1 and a node produces a new artifact, the previous
    one is pruned (it has been genuinely superseded)."""
    monkeypatch.setenv("ARTIFACT_RETENTION_MODE", "by_node")
    monkeypatch.setenv("ARTIFACT_KEEP_NODE_VERSIONS", "1")
    store = MemoryArtifactStore()
    graph_id = "g-supersede"

    await store.record_run("r1", "running")
    await _write(store, artifact_id="exec-node-a-old", run_id="r1", graph_id=graph_id, node_id="n-a", created_at=_ts(0))
    await store.update_run_status("r1", "succeeded")

    # Verify old artifact exists before it is superseded.
    assert await store.exists("exec-node-a-old")

    # Node A recomputes with new params → new artifact.
    await store.record_run("r2", "running")
    await _write(store, artifact_id="exec-node-a-new", run_id="r2", graph_id=graph_id, node_id="n-a", created_at=_ts(100))
    await store.update_run_status("r2", "succeeded")

    # Old artifact pruned; new artifact kept.
    assert not await store.exists("exec-node-a-old"), "superseded artifact must be pruned with keep_versions=1"
    assert await store.exists("exec-node-a-new"), "new artifact must be kept"


@pytest.mark.asyncio
async def test_by_node_keep_two_versions(monkeypatch, tmp_path):
    """With keep_versions=2, the two most-recent artifacts survive;
    anything older is pruned."""
    monkeypatch.setenv("ARTIFACT_RETENTION_MODE", "by_node")
    monkeypatch.setenv("ARTIFACT_KEEP_NODE_VERSIONS", "2")
    store = DiskArtifactStore(tmp_path / "store-keep2")
    graph_id = "g-keep2"

    for i in range(1, 5):
        await store.record_run(f"r{i}", "running")
        await _write(store, artifact_id=f"exec-v{i}", run_id=f"r{i}", graph_id=graph_id, node_id="n-a", created_at=_ts(i * 100))
        await store.update_run_status(f"r{i}", "succeeded")

    # Versions 1 and 2 are pruned; 3 and 4 are kept.
    assert not await store.exists("exec-v1"), "oldest artifact should be pruned"
    assert not await store.exists("exec-v2"), "second oldest artifact should be pruned"
    assert await store.exists("exec-v3"), "second-most-recent artifact must be kept"
    assert await store.exists("exec-v4"), "most-recent artifact must be kept"


@pytest.mark.asyncio
async def test_by_node_keep_two_versions_memory(monkeypatch):
    """Same keep_versions=2 test for MemoryArtifactStore."""
    monkeypatch.setenv("ARTIFACT_RETENTION_MODE", "by_node")
    monkeypatch.setenv("ARTIFACT_KEEP_NODE_VERSIONS", "2")
    store = MemoryArtifactStore()
    graph_id = "g-keep2-mem"

    for i in range(1, 5):
        await store.record_run(f"r{i}", "running")
        await _write(store, artifact_id=f"exec-mem-v{i}", run_id=f"r{i}", graph_id=graph_id, node_id="n-a", created_at=_ts(i * 100))
        await store.update_run_status(f"r{i}", "succeeded")

    assert not await store.exists("exec-mem-v1")
    assert not await store.exists("exec-mem-v2")
    assert await store.exists("exec-mem-v3")
    assert await store.exists("exec-mem-v4")


# ---------------------------------------------------------------------------
# Checkpoint pins
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_by_node_checkpoint_survives_rotation(monkeypatch):
    """A checkpoint-pinned artifact is never deleted even when keep_versions=1
    and newer artifacts exist for the same node."""
    monkeypatch.setenv("ARTIFACT_RETENTION_MODE", "by_node")
    monkeypatch.setenv("ARTIFACT_KEEP_NODE_VERSIONS", "1")
    store = MemoryArtifactStore()
    graph_id = "g-ckpt"

    # Run 1: produces the artifact that the user checkpoints.
    await store.record_run("r1", "running")
    await _write(store, artifact_id="exec-ckpt-aid", run_id="r1", graph_id=graph_id, node_id="n-a", created_at=_ts(0))
    await store.update_run_status("r1", "succeeded")

    # User creates a checkpoint pinning this artifact.
    await store.put_checkpoint_pins(graph_id, [("exec-ckpt-aid", "n-a")])

    # Run 2: node recomputes, new artifact.
    await store.record_run("r2", "running")
    await _write(store, artifact_id="exec-new-aid", run_id="r2", graph_id=graph_id, node_id="n-a", created_at=_ts(100))
    await store.update_run_status("r2", "succeeded")

    # Checkpoint artifact must survive even though keep_versions=1 and a newer
    # artifact exists.
    assert await store.exists("exec-ckpt-aid"), "checkpoint-pinned artifact must not be pruned"
    assert await store.exists("exec-new-aid"), "new artifact must also be kept"


@pytest.mark.asyncio
async def test_by_node_checkpoint_survives_rotation_disk(monkeypatch, tmp_path):
    """Same checkpoint survival test for DiskArtifactStore."""
    monkeypatch.setenv("ARTIFACT_RETENTION_MODE", "by_node")
    monkeypatch.setenv("ARTIFACT_KEEP_NODE_VERSIONS", "1")
    store = DiskArtifactStore(tmp_path / "store-ckpt")
    graph_id = "g-ckpt-disk"

    await store.record_run("r1", "running")
    await _write(store, artifact_id="exec-disk-ckpt-aid", run_id="r1", graph_id=graph_id, node_id="n-a", created_at=_ts(0))
    await store.update_run_status("r1", "succeeded")

    await store.put_checkpoint_pins(graph_id, [("exec-disk-ckpt-aid", "n-a")])

    await store.record_run("r2", "running")
    await _write(store, artifact_id="exec-disk-new-aid", run_id="r2", graph_id=graph_id, node_id="n-a", created_at=_ts(100))
    await store.update_run_status("r2", "succeeded")

    assert await store.exists("exec-disk-ckpt-aid"), "checkpoint-pinned artifact must not be pruned (disk)"
    assert await store.exists("exec-disk-new-aid")


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_by_node_non_runtime_artifacts_untouched(monkeypatch):
    """Artifacts without graph_id or node_id (e.g. snapshots) are not pruned."""
    monkeypatch.setenv("ARTIFACT_RETENTION_MODE", "by_node")
    monkeypatch.setenv("ARTIFACT_KEEP_NODE_VERSIONS", "1")
    store = MemoryArtifactStore()

    # Write two artifacts with no graph_id / node_id.
    for i in range(1, 3):
        await store.record_run(f"r{i}", "running")
        art = Artifact(
            artifact_id=f"snap-{i}",
            node_kind="snapshot",
            params_hash="ph",
            upstream_ids=[],
            created_at=datetime.now(timezone.utc),
            execution_version="v1",
            mime_type="application/json",
            payload_type="json",
            size_bytes=4,
            storage_uri="memory://snap",
            payload_schema={
                "type": "json",
                "artifactMetadataV1": {
                    "metadataVersion": 1, "execKey": f"snap-{i}", "nodeId": "",
                    "nodeType": "snapshot", "nodeImplVersion": "v1",
                    "paramsFingerprint": "p", "upstreamArtifactIds": [],
                    "contractFingerprint": "c", "schemaFingerprint": "s",
                    "mimeType": "application/json", "payloadType": "json",
                    "createdAt": datetime.now(timezone.utc).isoformat(),
                },
            },
            run_id=None,
            graph_id=None,
            node_id=None,
            exec_key=f"snap-{i}",
        )
        await store.write(art, b'{}')
        await store.update_run_status(f"r{i}", "succeeded")

    assert await store.exists("snap-1"), "non-runtime artifact must not be pruned"
    assert await store.exists("snap-2"), "non-runtime artifact must not be pruned"


@pytest.mark.asyncio
async def test_by_node_non_terminal_run_does_not_trigger_sweep(monkeypatch):
    """Retention sweep must not fire on non-terminal status updates."""
    monkeypatch.setenv("ARTIFACT_RETENTION_MODE", "by_node")
    monkeypatch.setenv("ARTIFACT_KEEP_NODE_VERSIONS", "1")
    store = MemoryArtifactStore()
    graph_id = "g-nonterminal"

    # Two runs each producing an artifact for the same node; neither completes.
    for i in range(1, 3):
        await store.record_run(f"r{i}", "running")
        await _write(store, artifact_id=f"exec-nt-v{i}", run_id=f"r{i}", graph_id=graph_id, node_id="n-a", created_at=_ts(i * 100))
        await store.update_run_status(f"r{i}", "paused")

    # No sweep fired — both artifacts survive even though keep_versions=1.
    assert await store.exists("exec-nt-v1"), "artifact must survive if its run never reached terminal status"
    assert await store.exists("exec-nt-v2")


@pytest.mark.asyncio
async def test_by_node_off_mode_preserves_all(monkeypatch):
    """ARTIFACT_RETENTION_MODE=off must disable all pruning."""
    monkeypatch.setenv("ARTIFACT_RETENTION_MODE", "off")
    store = MemoryArtifactStore()
    graph_id = "g-off"

    for i in range(1, 10):
        await store.record_run(f"r{i}", "running")
        await _write(store, artifact_id=f"exec-off-v{i}", run_id=f"r{i}", graph_id=graph_id, node_id="n-a", created_at=_ts(i * 100))
        await store.update_run_status(f"r{i}", "succeeded")

    for i in range(1, 10):
        assert await store.exists(f"exec-off-v{i}"), f"exec-off-v{i} must not be pruned when mode=off"


@pytest.mark.asyncio
async def test_by_node_multiple_nodes_independent(monkeypatch):
    """Retention operates independently per node; pruning one node's old
    artifacts must not affect other nodes."""
    monkeypatch.setenv("ARTIFACT_RETENTION_MODE", "by_node")
    monkeypatch.setenv("ARTIFACT_KEEP_NODE_VERSIONS", "1")
    store = MemoryArtifactStore()
    graph_id = "g-multi"

    # Node A: 3 versions — only the latest (v3) should survive.
    # Node B: 1 version — must always survive.
    for i in range(1, 4):
        await store.record_run(f"ra{i}", "running")
        await _write(store, artifact_id=f"exec-a-v{i}", run_id=f"ra{i}", graph_id=graph_id, node_id="n-a", created_at=_ts(i * 10))
        await store.update_run_status(f"ra{i}", "succeeded")

    await store.record_run("rb1", "running")
    await _write(store, artifact_id="exec-b-v1", run_id="rb1", graph_id=graph_id, node_id="n-b", created_at=_ts(0))
    await store.update_run_status("rb1", "succeeded")

    assert not await store.exists("exec-a-v1"), "node-a v1 should be pruned"
    assert not await store.exists("exec-a-v2"), "node-a v2 should be pruned"
    assert await store.exists("exec-a-v3"), "node-a v3 must be kept"
    assert await store.exists("exec-b-v1"), "node-b v1 must be kept (only version)"
