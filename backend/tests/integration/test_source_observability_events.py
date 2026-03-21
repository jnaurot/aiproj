import pytest

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.run import run_graph


@pytest.mark.asyncio
async def test_source_node_output_event_includes_source_observability(tmp_path):
    events = []
    artifact_root = tmp_path / "artifact-root"
    source_dir = tmp_path / "source"
    source_dir.mkdir(parents=True, exist_ok=True)
    source_file = source_dir / "input.txt"
    source_file.write_text("alpha\nbeta\n", encoding="utf-8")
    bus = RunEventBus("run-source-obs-1", on_emit=lambda e: events.append(dict(e)))

    await run_graph(
        run_id="run-source-obs-1",
        graph={
            "nodes": [
                {
                    "id": "source_1",
                    "data": {
                        "kind": "source",
                        "label": "Source",
                        "sourceKind": "file",
                        "params": {
                            "rel_path": str(source_dir),
                            "filename": "input.txt",
                            "file_format": "txt",
                            "output": {"mode": "table"},
                            "priming": {"enabled": True, "mode": "priming_only", "sample_rows": 5, "sample_bytes": 64},
                        },
                    },
                }
            ],
            "edges": [],
        },
        run_from=None,
        bus=bus,
        artifact_store=DiskArtifactStore(artifact_root),
        cache=SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite")),
        graph_id="graph-source-obs-1",
    )

    node_outputs = [e for e in events if e.get("type") == "node_output" and e.get("nodeId") == "source_1"]
    assert node_outputs, "Expected node_output for source_1"
    source_obs = node_outputs[-1].get("sourceObservability")
    assert isinstance(source_obs, dict)
    assert source_obs.get("source_kind") == "file"
    assert source_obs.get("output_mode") in {"text", "table"}
    priming_artifact = node_outputs[-1].get("primingArtifact")
    assert isinstance(priming_artifact, dict)
    inferred_schema = priming_artifact.get("inferred_schema")
    assert isinstance(inferred_schema, dict)
    assert inferred_schema.get("type") in {"table", "text"}


@pytest.mark.asyncio
async def test_source_node_output_event_includes_runtime_table_columns_for_csv(tmp_path):
    events = []
    artifact_root = tmp_path / "artifact-root-2"
    source_dir = tmp_path / "source-2"
    source_dir.mkdir(parents=True, exist_ok=True)
    source_file = source_dir / "input.csv"
    source_file.write_text("name,age\nalice,31\nbob,28\n", encoding="utf-8")
    bus = RunEventBus("run-source-obs-2", on_emit=lambda e: events.append(dict(e)))

    await run_graph(
        run_id="run-source-obs-2",
        graph={
            "nodes": [
                {
                    "id": "source_csv",
                    "data": {
                        "kind": "source",
                        "label": "Source",
                        "sourceKind": "file",
                        "params": {
                            "rel_path": str(source_dir),
                            "filename": "input.csv",
                            "file_format": "csv",
                            "output": {"mode": "table"},
                        },
                    },
                }
            ],
            "edges": [],
        },
        run_from=None,
        bus=bus,
        artifact_store=DiskArtifactStore(artifact_root),
        cache=SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite")),
        graph_id="graph-source-obs-2",
    )

    node_outputs = [e for e in events if e.get("type") == "node_output" and e.get("nodeId") == "source_csv"]
    assert node_outputs, "Expected node_output for source_csv"
    source_obs = node_outputs[-1].get("sourceObservability")
    assert isinstance(source_obs, dict)
    assert isinstance(source_obs.get("table_columns"), list)
    col_names = [str(c.get("name")) for c in (source_obs.get("table_columns") or []) if isinstance(c, dict)]
    assert col_names == ["name", "age"]
