import importlib
import os
import sys
import time
import types
from pathlib import Path

import pytest

if "duckdb" not in sys.modules:
    sys.modules["duckdb"] = types.SimpleNamespace()

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import NodeOutput


def _source_graph(file_path: str) -> dict:
    p = Path(file_path)
    return {
        "nodes": [
            {
                "id": "source_1",
                "data": {
                    "kind": "source",
                    "label": "Source",
                    "sourceKind": "file",
                    "params": {"rel_path": str(p.parent), "filename": p.name, "file_format": "txt", "output_mode": "text"},
                    "ports": {"in": None, "out": "text"},
                },
            }
        ],
        "edges": [],
    }


def _source_graph_with_mode(file_path: str, *, mode: str, port: str) -> dict:
    p = Path(file_path)
    return {
        "nodes": [
            {
                "id": "source_1",
                "data": {
                    "kind": "source",
                    "label": "Source",
                    "sourceKind": "file",
                    "params": {
                        "rel_path": str(p.parent),
                        "filename": p.name,
                        "file_format": "csv",
                        "output_mode": mode,
                    },
                    "ports": {"in": None, "out": port},
                },
            }
        ],
        "edges": [],
    }


@pytest.mark.asyncio
async def test_source_file_fingerprint_drives_cache_hit_and_miss(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")
    calls = {"source": 0}
    monkeypatch.setenv("WORKSPACE_ROOT_WORKSPACE", str(tmp_path))

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        calls["source"] += 1
        params = node["data"]["params"]
        p = (Path(params["rel_path"]) / params["filename"]).resolve()
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data=p.read_text(encoding="utf-8"),
        )

    monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)

    file_path = tmp_path / "input.txt"
    file_path.write_text("alpha", encoding="utf-8")

    artifact_root = tmp_path / "artifact-root"
    store = DiskArtifactStore(artifact_root)
    cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))
    graph = _source_graph(str(file_path))

    events_1: list[dict] = []
    await run_mod.run_graph(
        run_id="run-source-fp-1",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-source-fp-1", on_emit=lambda e: events_1.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-source-fp",
    )
    decisions_1 = [e for e in events_1 if e.get("type") == "cache_decision" and e.get("nodeId") == "source_1"]
    assert decisions_1 and decisions_1[-1].get("decision") == "cache_miss"
    out_1 = [e for e in events_1 if e.get("type") == "node_output" and e.get("nodeId") == "source_1"]
    assert out_1
    first_artifact_id = out_1[-1]["artifactId"]
    assert calls["source"] == 1

    events_2: list[dict] = []
    await run_mod.run_graph(
        run_id="run-source-fp-2",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-source-fp-2", on_emit=lambda e: events_2.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-source-fp",
    )
    decisions_2 = [e for e in events_2 if e.get("type") == "cache_decision" and e.get("nodeId") == "source_1"]
    assert decisions_2 and decisions_2[-1].get("decision") == "cache_hit"
    out_2 = [e for e in events_2 if e.get("type") == "node_output" and e.get("nodeId") == "source_1"]
    assert out_2 and out_2[-1]["artifactId"] == first_artifact_id
    assert out_2[-1].get("cached") is True
    assert calls["source"] == 1

    # Ensure fingerprint changes by modifying bytes and mtime.
    time.sleep(0.02)
    file_path.write_text("alpha-updated", encoding="utf-8")
    os.utime(file_path, None)

    events_3: list[dict] = []
    await run_mod.run_graph(
        run_id="run-source-fp-3",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-source-fp-3", on_emit=lambda e: events_3.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-source-fp",
    )
    decisions_3 = [e for e in events_3 if e.get("type") == "cache_decision" and e.get("nodeId") == "source_1"]
    assert decisions_3 and decisions_3[-1].get("decision") == "cache_miss"
    out_3 = [e for e in events_3 if e.get("type") == "node_output" and e.get("nodeId") == "source_1"]
    assert out_3 and out_3[-1]["artifactId"] != first_artifact_id
    assert calls["source"] == 2


@pytest.mark.asyncio
async def test_source_output_mode_change_recomputes_cache_key(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")
    calls = {"source": 0}
    monkeypatch.setenv("WORKSPACE_ROOT_WORKSPACE", str(tmp_path))

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        calls["source"] += 1
        params = node["data"]["params"]
        p = (Path(params["rel_path"]) / params["filename"]).resolve()
        output_mode = str(params.get("output_mode") or "text")
        if output_mode == "table":
            data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        elif output_mode == "json":
            data = {"rows": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]}
        else:
            data = p.read_text(encoding="utf-8")
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data=data,
        )

    monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)

    file_path = tmp_path / "input.csv"
    file_path.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")

    artifact_root = tmp_path / "artifact-root-mode-switch"
    store = DiskArtifactStore(artifact_root)
    cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))

    graph_table = _source_graph_with_mode(str(file_path), mode="table", port="table")

    events_1: list[dict] = []
    await run_mod.run_graph(
        run_id="run-source-mode-1",
        graph=graph_table,
        run_from=None,
        bus=RunEventBus("run-source-mode-1", on_emit=lambda e: events_1.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-source-mode",
    )
    out_1 = [e for e in events_1 if e.get("type") == "node_output" and e.get("nodeId") == "source_1"]
    decisions_1 = [e for e in events_1 if e.get("type") == "cache_decision" and e.get("nodeId") == "source_1"]
    assert out_1
    assert decisions_1 and decisions_1[-1].get("decision") == "cache_miss"
    first_artifact_id = str(out_1[-1]["artifactId"])
    assert calls["source"] == 1

    events_2: list[dict] = []
    await run_mod.run_graph(
        run_id="run-source-mode-2",
        graph=graph_table,
        run_from=None,
        bus=RunEventBus("run-source-mode-2", on_emit=lambda e: events_2.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-source-mode",
    )
    decisions_2 = [e for e in events_2 if e.get("type") == "cache_decision" and e.get("nodeId") == "source_1"]
    assert decisions_2 and decisions_2[-1].get("decision") == "cache_hit"
    assert calls["source"] == 1

    graph_json = _source_graph_with_mode(str(file_path), mode="json", port="json")
    events_3: list[dict] = []
    await run_mod.run_graph(
        run_id="run-source-mode-3",
        graph=graph_json,
        run_from=None,
        bus=RunEventBus("run-source-mode-3", on_emit=lambda e: events_3.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-source-mode",
    )
    out_3 = [e for e in events_3 if e.get("type") == "node_output" and e.get("nodeId") == "source_1"]
    decisions_3 = [e for e in events_3 if e.get("type") == "cache_decision" and e.get("nodeId") == "source_1"]
    assert out_3
    assert decisions_3 and decisions_3[-1].get("decision") == "cache_miss"
    assert str(out_3[-1]["artifactId"]) != first_artifact_id
    assert calls["source"] == 2


@pytest.mark.asyncio
async def test_global_force_off_disables_cache_even_when_source_cache_enabled(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")
    calls = {"source": 0}
    monkeypatch.setenv("WORKSPACE_ROOT_WORKSPACE", str(tmp_path))

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        calls["source"] += 1
        return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data="hello")

    monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)

    file_path = tmp_path / "input.txt"
    file_path.write_text("alpha", encoding="utf-8")
    graph = _source_graph(str(file_path))

    artifact_root = tmp_path / "artifact-root-force-off"
    store = DiskArtifactStore(artifact_root)
    cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))

    # Warm cache once in default mode.
    await run_mod.run_graph(
        run_id="run-force-off-1",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-force-off-1"),
        artifact_store=store,
        cache=cache,
        graph_id="graph-force-off",
    )
    assert calls["source"] == 1

    class _ForceOffRuntime:
        def get_global_cache_mode(self):
            return "force_off"

    events_2: list[dict] = []
    await run_mod.run_graph(
        run_id="run-force-off-2",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-force-off-2", on_emit=lambda e: events_2.append(dict(e))),
        artifact_store=store,
        cache=cache,
        runtime_ref=_ForceOffRuntime(),
        graph_id="graph-force-off",
    )
    decisions = [e for e in events_2 if e.get("type") == "cache_decision" and e.get("nodeId") == "source_1"]
    assert decisions
    assert decisions[-1].get("decision") == "cache_miss"
    assert decisions[-1].get("reason") == "GLOBAL_FORCE_OFF"
    assert calls["source"] == 2


@pytest.mark.asyncio
async def test_source_api_cache_policy_never_forces_execution(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")
    calls = {"source": 0}

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        calls["source"] += 1
        return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data={"ok": True})

    monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)

    graph = {
        "nodes": [
            {
                "id": "source_api",
                "data": {
                    "kind": "source",
                    "label": "Source API",
                    "sourceKind": "api",
                    "params": {
                        "url": "https://api.example.com/data",
                        "method": "GET",
                        "output_mode": "json",
                        "cache_policy": {"mode": "never"},
                    },
                    "ports": {"in": None, "out": "json"},
                },
            }
        ],
        "edges": [],
    }
    artifact_root = tmp_path / "artifact-root-never"
    store = DiskArtifactStore(artifact_root)
    cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))

    for i in range(2):
        await run_mod.run_graph(
            run_id=f"run-source-never-{i}",
            graph=graph,
            run_from=None,
            bus=RunEventBus(f"run-source-never-{i}"),
            artifact_store=store,
            cache=cache,
            graph_id="graph-source-never",
        )
    assert calls["source"] == 2


@pytest.mark.asyncio
async def test_source_image_png_sets_image_mime_and_rerun_hits_cache(tmp_path):
    run_mod = importlib.import_module("app.runner.run")
    file_path = tmp_path / "pixel.png"
    file_path.write_bytes(b"\x89PNG\r\n\x1a\nfake-png-bytes")

    graph = {
        "nodes": [
            {
                "id": "source_img",
                "data": {
                    "kind": "source",
                    "label": "Source Image",
                    "sourceKind": "file",
                    "params": {
                        "rel_path": str(tmp_path),
                        "filename": file_path.name,
                        "file_format": "png",
                    },
                    "ports": {"in": None, "out": "binary"},
                },
            }
        ],
        "edges": [],
    }
    artifact_root = tmp_path / "artifact-root-image"
    store = DiskArtifactStore(artifact_root)
    cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))

    events_1: list[dict] = []
    await run_mod.run_graph(
        run_id="run-source-image-1",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-source-image-1", on_emit=lambda e: events_1.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-source-image",
    )
    decisions_1 = [e for e in events_1 if e.get("type") == "cache_decision" and e.get("nodeId") == "source_img"]
    assert decisions_1 and decisions_1[-1].get("decision") == "cache_miss"
    out_1 = [e for e in events_1 if e.get("type") == "node_output" and e.get("nodeId") == "source_img"]
    assert out_1
    assert out_1[-1].get("mimeType") == "image/png"
    first_artifact_id = str(out_1[-1]["artifactId"])
    default_logs_1 = [
        e
        for e in events_1
        if e.get("type") == "log"
        and e.get("nodeId") == "source_img"
        and "Schema defaulted: default=IMAGE_V1" in str(e.get("message", ""))
    ]
    assert default_logs_1

    events_2: list[dict] = []
    await run_mod.run_graph(
        run_id="run-source-image-2",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-source-image-2", on_emit=lambda e: events_2.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-source-image",
    )
    decisions_2 = [e for e in events_2 if e.get("type") == "cache_decision" and e.get("nodeId") == "source_img"]
    assert decisions_2 and decisions_2[-1].get("decision") == "cache_hit"
    out_2 = [e for e in events_2 if e.get("type") == "node_output" and e.get("nodeId") == "source_img"]
    assert out_2
    assert out_2[-1].get("mimeType") == "image/png"
    assert out_2[-1]["artifactId"] == first_artifact_id
    default_logs_2 = [
        e
        for e in events_2
        if e.get("type") == "log"
        and e.get("nodeId") == "source_img"
        and "Schema defaulted: default=IMAGE_V1" in str(e.get("message", ""))
    ]
    assert default_logs_2


@pytest.mark.asyncio
async def test_source_audio_wav_sets_audio_mime_and_rerun_hits_cache(tmp_path):
    run_mod = importlib.import_module("app.runner.run")
    file_path = tmp_path / "tone.wav"
    file_path.write_bytes(b"RIFF----WAVEfmt fake-wav-bytes")

    graph = {
        "nodes": [
            {
                "id": "source_audio",
                "data": {
                    "kind": "source",
                    "label": "Source Audio",
                    "sourceKind": "file",
                    "params": {
                        "rel_path": str(tmp_path),
                        "filename": file_path.name,
                        "file_format": "wav",
                    },
                    "ports": {"in": None, "out": "binary"},
                },
            }
        ],
        "edges": [],
    }
    artifact_root = tmp_path / "artifact-root-audio"
    store = DiskArtifactStore(artifact_root)
    cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))

    events_1: list[dict] = []
    await run_mod.run_graph(
        run_id="run-source-audio-1",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-source-audio-1", on_emit=lambda e: events_1.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-source-audio",
    )
    decisions_1 = [e for e in events_1 if e.get("type") == "cache_decision" and e.get("nodeId") == "source_audio"]
    assert decisions_1 and decisions_1[-1].get("decision") == "cache_miss"
    out_1 = [e for e in events_1 if e.get("type") == "node_output" and e.get("nodeId") == "source_audio"]
    assert out_1
    assert out_1[-1].get("mimeType") == "audio/wav"
    first_artifact_id = str(out_1[-1]["artifactId"])
    default_logs_1 = [
        e
        for e in events_1
        if e.get("type") == "log"
        and e.get("nodeId") == "source_audio"
        and "Schema defaulted: default=AUDIO_V1" in str(e.get("message", ""))
    ]
    assert default_logs_1

    events_2: list[dict] = []
    await run_mod.run_graph(
        run_id="run-source-audio-2",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-source-audio-2", on_emit=lambda e: events_2.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-source-audio",
    )
    decisions_2 = [e for e in events_2 if e.get("type") == "cache_decision" and e.get("nodeId") == "source_audio"]
    assert decisions_2 and decisions_2[-1].get("decision") == "cache_hit"
    out_2 = [e for e in events_2 if e.get("type") == "node_output" and e.get("nodeId") == "source_audio"]
    assert out_2
    assert out_2[-1].get("mimeType") == "audio/wav"
    assert out_2[-1]["artifactId"] == first_artifact_id
    default_logs_2 = [
        e
        for e in events_2
        if e.get("type") == "log"
        and e.get("nodeId") == "source_audio"
        and "Schema defaulted: default=AUDIO_V1" in str(e.get("message", ""))
    ]
    assert default_logs_2


@pytest.mark.asyncio
async def test_source_video_mp4_sets_video_mime_and_rerun_hits_cache(tmp_path):
    run_mod = importlib.import_module("app.runner.run")
    file_path = tmp_path / "clip.mp4"
    file_path.write_bytes(b"\x00\x00\x00\x18ftypmp42fake-mp4-bytes")

    graph = {
        "nodes": [
            {
                "id": "source_video",
                "data": {
                    "kind": "source",
                    "label": "Source Video",
                    "sourceKind": "file",
                    "params": {
                        "rel_path": str(tmp_path),
                        "filename": file_path.name,
                        "file_format": "mp4",
                    },
                    "ports": {"in": None, "out": "binary"},
                },
            }
        ],
        "edges": [],
    }
    artifact_root = tmp_path / "artifact-root-video"
    store = DiskArtifactStore(artifact_root)
    cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))

    events_1: list[dict] = []
    await run_mod.run_graph(
        run_id="run-source-video-1",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-source-video-1", on_emit=lambda e: events_1.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-source-video",
    )
    decisions_1 = [e for e in events_1 if e.get("type") == "cache_decision" and e.get("nodeId") == "source_video"]
    assert decisions_1 and decisions_1[-1].get("decision") == "cache_miss"
    out_1 = [e for e in events_1 if e.get("type") == "node_output" and e.get("nodeId") == "source_video"]
    assert out_1
    assert out_1[-1].get("mimeType") == "video/mp4"
    first_artifact_id = str(out_1[-1]["artifactId"])
    default_logs_1 = [
        e
        for e in events_1
        if e.get("type") == "log"
        and e.get("nodeId") == "source_video"
        and "Schema defaulted: default=VIDEO_V1" in str(e.get("message", ""))
    ]
    assert default_logs_1

    events_2: list[dict] = []
    await run_mod.run_graph(
        run_id="run-source-video-2",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-source-video-2", on_emit=lambda e: events_2.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-source-video",
    )
    decisions_2 = [e for e in events_2 if e.get("type") == "cache_decision" and e.get("nodeId") == "source_video"]
    assert decisions_2 and decisions_2[-1].get("decision") == "cache_hit"
    out_2 = [e for e in events_2 if e.get("type") == "node_output" and e.get("nodeId") == "source_video"]
    assert out_2
    assert out_2[-1].get("mimeType") == "video/mp4"
    assert out_2[-1]["artifactId"] == first_artifact_id
    default_logs_2 = [
        e
        for e in events_2
        if e.get("type") == "log"
        and e.get("nodeId") == "source_video"
        and "Schema defaulted: default=VIDEO_V1" in str(e.get("message", ""))
    ]
    assert default_logs_2
