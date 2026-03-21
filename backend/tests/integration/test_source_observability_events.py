import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from decimal import Decimal
from types import SimpleNamespace
import array
import wave

import app.executors.source as source_mod
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


@pytest.mark.asyncio
async def test_source_node_output_event_includes_parquet_logical_metadata(tmp_path):
    events = []
    artifact_root = tmp_path / "artifact-root-3"
    source_dir = tmp_path / "source-3"
    source_dir.mkdir(parents=True, exist_ok=True)
    source_file = source_dir / "input.parquet"
    pq.write_table(
        pa.table(
            {
                "amount": pa.array([Decimal("12.34")], type=pa.decimal128(10, 2)),
                "tags": pa.array([["x", "y"]], type=pa.list_(pa.string())),
            }
        ),
        source_file,
    )
    bus = RunEventBus("run-source-obs-3", on_emit=lambda e: events.append(dict(e)))

    await run_graph(
        run_id="run-source-obs-3",
        graph={
            "nodes": [
                {
                    "id": "source_parquet",
                    "data": {
                        "kind": "source",
                        "label": "Source",
                        "sourceKind": "file",
                        "params": {
                            "rel_path": str(source_dir),
                            "filename": "input.parquet",
                            "file_format": "parquet",
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
        graph_id="graph-source-obs-3",
    )

    node_outputs = [e for e in events if e.get("type") == "node_output" and e.get("nodeId") == "source_parquet"]
    assert node_outputs, "Expected node_output for source_parquet"
    source_obs = node_outputs[-1].get("sourceObservability")
    assert isinstance(source_obs, dict)
    logical = source_obs.get("parquet_logical_types")
    assert isinstance(logical, dict)
    assert logical.get("amount") == "decimal(10,2)"


@pytest.mark.asyncio
async def test_source_node_output_event_includes_json_streaming_metadata(tmp_path):
    events = []
    artifact_root = tmp_path / "artifact-root-4"
    source_dir = tmp_path / "source-4"
    source_dir.mkdir(parents=True, exist_ok=True)
    source_file = source_dir / "input.ndjson"
    source_file.write_text('{"id":1}\n{"id":2}\n{"id":3}\n', encoding="utf-8")
    bus = RunEventBus("run-source-obs-4", on_emit=lambda e: events.append(dict(e)))

    await run_graph(
        run_id="run-source-obs-4",
        graph={
            "nodes": [
                {
                    "id": "source_json_stream",
                    "data": {
                        "kind": "source",
                        "label": "Source",
                        "sourceKind": "file",
                        "params": {
                            "rel_path": str(source_dir),
                            "filename": "input.ndjson",
                            "file_format": "json",
                            "json_mode": "ndjson",
                            "json_streaming_enabled": True,
                            "json_stream_chunk_lines": 1,
                            "json_stream_max_records": 2,
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
        graph_id="graph-source-obs-4",
    )

    node_outputs = [e for e in events if e.get("type") == "node_output" and e.get("nodeId") == "source_json_stream"]
    assert node_outputs, "Expected node_output for source_json_stream"
    source_obs = node_outputs[-1].get("sourceObservability")
    assert isinstance(source_obs, dict)
    stream_meta = source_obs.get("json_streaming")
    assert isinstance(stream_meta, dict)
    assert stream_meta.get("records_emitted") == 2


@pytest.mark.asyncio
async def test_source_node_output_event_includes_json_flatten_metadata(tmp_path):
    events = []
    artifact_root = tmp_path / "artifact-root-5"
    source_dir = tmp_path / "source-5"
    source_dir.mkdir(parents=True, exist_ok=True)
    source_file = source_dir / "input.json"
    source_file.write_text('{"id":1,"user":{"name":"alice"}}', encoding="utf-8")
    bus = RunEventBus("run-source-obs-5", on_emit=lambda e: events.append(dict(e)))

    await run_graph(
        run_id="run-source-obs-5",
        graph={
            "nodes": [
                {
                    "id": "source_json_flat",
                    "data": {
                        "kind": "source",
                        "label": "Source",
                        "sourceKind": "file",
                        "schema": {"expectedSchema": {"typedSchema": {"type": "table"}}},
                        "params": {
                            "rel_path": str(source_dir),
                            "filename": "input.json",
                            "file_format": "json",
                            "json_mode": "document",
                            "json_flatten_strategy": "deep",
                            "json_flatten_separator": "_",
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
        graph_id="graph-source-obs-5",
    )

    node_outputs = [e for e in events if e.get("type") == "node_output" and e.get("nodeId") == "source_json_flat"]
    assert node_outputs, "Expected node_output for source_json_flat"
    source_obs = node_outputs[-1].get("sourceObservability")
    assert isinstance(source_obs, dict)
    flatten = source_obs.get("json_flatten")
    assert isinstance(flatten, dict)
    assert flatten.get("strategy") == "deep"


@pytest.mark.asyncio
async def test_source_node_output_event_includes_excel_provenance_metadata(tmp_path, monkeypatch):
    import pandas as pd

    events = []
    artifact_root = tmp_path / "artifact-root-6"
    source_dir = tmp_path / "source-6"
    source_dir.mkdir(parents=True, exist_ok=True)
    source_file = source_dir / "input.xlsx"
    source_file.write_bytes(b"fake-xlsx")

    def _fake_read_excel(*_args, **kwargs):
        sheet_name = kwargs.get("sheet_name", 0)
        if sheet_name is None:
            return {"S1": pd.DataFrame({"id": [1]}), "S2": pd.DataFrame({"id": [2]})}
        return pd.DataFrame({"id": [1]})

    monkeypatch.setattr("app.executors.source.pd.read_excel", _fake_read_excel)
    bus = RunEventBus("run-source-obs-6", on_emit=lambda e: events.append(dict(e)))

    await run_graph(
        run_id="run-source-obs-6",
        graph={
            "nodes": [
                {
                    "id": "source_excel_multi",
                    "data": {
                        "kind": "source",
                        "label": "Source",
                        "sourceKind": "file",
                        "schema": {"expectedSchema": {"typedSchema": {"type": "table"}}},
                        "params": {
                            "rel_path": str(source_dir),
                            "filename": "input.xlsx",
                            "file_format": "excel",
                            "excel_import_strategy": "stack",
                            "excel_merged_cells_policy": "ffill",
                            "excel_date_policy": "coerce",
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
        graph_id="graph-source-obs-6",
    )

    node_outputs = [e for e in events if e.get("type") == "node_output" and e.get("nodeId") == "source_excel_multi"]
    assert node_outputs, "Expected node_output for source_excel_multi"
    source_obs = node_outputs[-1].get("sourceObservability")
    assert isinstance(source_obs, dict)
    provenance = source_obs.get("excel_provenance")
    assert isinstance(provenance, dict)
    assert provenance.get("strategy") == "stack"
    policy = source_obs.get("excel_policy")
    assert isinstance(policy, dict)
    assert policy.get("merged_cells_policy") == "ffill"


@pytest.mark.asyncio
async def test_source_node_output_event_includes_txt_recordization_metadata(tmp_path):
    events = []
    artifact_root = tmp_path / "artifact-root-7"
    source_dir = tmp_path / "source-7"
    source_dir.mkdir(parents=True, exist_ok=True)
    source_file = source_dir / "input.txt"
    source_file.write_text("a\nb\nc\n", encoding="utf-8")
    bus = RunEventBus("run-source-obs-7", on_emit=lambda e: events.append(dict(e)))

    await run_graph(
        run_id="run-source-obs-7",
        graph={
            "nodes": [
                {
                    "id": "source_txt_records",
                    "data": {
                        "kind": "source",
                        "label": "Source",
                        "sourceKind": "file",
                        "schema": {"expectedSchema": {"typedSchema": {"type": "table"}}},
                        "params": {
                            "rel_path": str(source_dir),
                            "filename": "input.txt",
                            "file_format": "txt",
                            "txt_record_mode": "lines",
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
        graph_id="graph-source-obs-7",
    )

    node_outputs = [e for e in events if e.get("type") == "node_output" and e.get("nodeId") == "source_txt_records"]
    assert node_outputs, "Expected node_output for source_txt_records"
    source_obs = node_outputs[-1].get("sourceObservability")
    assert isinstance(source_obs, dict)
    txt_meta = source_obs.get("txt_recordization")
    assert isinstance(txt_meta, dict)
    assert txt_meta.get("mode") == "lines"


@pytest.mark.asyncio
async def test_source_node_output_event_includes_pdf_metadata(tmp_path, monkeypatch):
    events = []
    artifact_root = tmp_path / "artifact-root-8"
    source_dir = tmp_path / "source-8"
    source_dir.mkdir(parents=True, exist_ok=True)
    source_file = source_dir / "input.pdf"
    source_file.write_bytes(b"%PDF-FAKE")

    class _Page:
        def extract_text(self):
            return "page text"

        def extract_tables(self):
            return []

    class _Pdf:
        def __init__(self):
            self.pages = [_Page()]

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(source_mod, "HAS_PDF", True)
    monkeypatch.setattr(source_mod, "pdfplumber", SimpleNamespace(open=lambda *_args, **_kwargs: _Pdf()), raising=False)
    bus = RunEventBus("run-source-obs-8", on_emit=lambda e: events.append(dict(e)))

    await run_graph(
        run_id="run-source-obs-8",
        graph={
            "nodes": [
                {
                    "id": "source_pdf_meta",
                    "data": {
                        "kind": "source",
                        "label": "Source",
                        "sourceKind": "file",
                        "params": {
                            "rel_path": str(source_dir),
                            "filename": "input.pdf",
                            "file_format": "pdf",
                            "pdf_extraction_mode": "hybrid",
                            "pdf_page_mode": "sample",
                            "pdf_page_sample": 1,
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
        graph_id="graph-source-obs-8",
    )

    node_outputs = [e for e in events if e.get("type") == "node_output" and e.get("nodeId") == "source_pdf_meta"]
    assert node_outputs, "Expected node_output for source_pdf_meta"
    source_obs = node_outputs[-1].get("sourceObservability")
    assert isinstance(source_obs, dict)
    pdf_meta = source_obs.get("pdf_metadata")
    assert isinstance(pdf_meta, dict)
    assert pdf_meta.get("requested_mode") == "hybrid"
    assert pdf_meta.get("selected_pages") == [0]
    pages = pdf_meta.get("pages")
    assert isinstance(pages, list)
    assert len(pages) == 1


@pytest.mark.asyncio
async def test_source_node_output_event_includes_image_metadata(tmp_path):
    pytest.importorskip("PIL")
    from PIL import Image

    events = []
    artifact_root = tmp_path / "artifact-root-9"
    source_dir = tmp_path / "source-9"
    source_dir.mkdir(parents=True, exist_ok=True)
    source_file = source_dir / "input.png"
    Image.new("RGB", (6, 5), color=(10, 20, 30)).save(source_file)
    bus = RunEventBus("run-source-obs-9", on_emit=lambda e: events.append(dict(e)))

    await run_graph(
        run_id="run-source-obs-9",
        graph={
            "nodes": [
                {
                    "id": "source_image_meta",
                    "data": {
                        "kind": "source",
                        "label": "Source",
                        "sourceKind": "file",
                        "params": {
                            "rel_path": str(source_dir),
                            "filename": "input.png",
                            "file_format": "png",
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
        graph_id="graph-source-obs-9",
    )

    node_outputs = [e for e in events if e.get("type") == "node_output" and e.get("nodeId") == "source_image_meta"]
    assert node_outputs, "Expected node_output for source_image_meta"
    source_obs = node_outputs[-1].get("sourceObservability")
    assert isinstance(source_obs, dict)
    img_meta = source_obs.get("image_metadata")
    assert isinstance(img_meta, dict)
    assert img_meta.get("width") == 6
    assert img_meta.get("height") == 5


@pytest.mark.asyncio
async def test_source_node_output_event_includes_audio_metadata(tmp_path):
    events = []
    artifact_root = tmp_path / "artifact-root-10"
    source_dir = tmp_path / "source-10"
    source_dir.mkdir(parents=True, exist_ok=True)
    source_file = source_dir / "input.wav"
    with wave.open(str(source_file), "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(8000)
        wf.writeframes(array.array("h", [500] * 800).tobytes())
    bus = RunEventBus("run-source-obs-10", on_emit=lambda e: events.append(dict(e)))

    await run_graph(
        run_id="run-source-obs-10",
        graph={
            "nodes": [
                {
                    "id": "source_audio_meta",
                    "data": {
                        "kind": "source",
                        "label": "Source",
                        "sourceKind": "file",
                        "params": {
                            "rel_path": str(source_dir),
                            "filename": "input.wav",
                            "file_format": "wav",
                            "audio_normalize": True,
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
        graph_id="graph-source-obs-10",
    )

    node_outputs = [e for e in events if e.get("type") == "node_output" and e.get("nodeId") == "source_audio_meta"]
    assert node_outputs, "Expected node_output for source_audio_meta"
    source_obs = node_outputs[-1].get("sourceObservability")
    assert isinstance(source_obs, dict)
    audio_meta = source_obs.get("audio_metadata")
    assert isinstance(audio_meta, dict)
    assert audio_meta.get("sample_rate") == 8000
