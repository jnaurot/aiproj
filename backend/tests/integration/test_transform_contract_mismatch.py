import importlib
import json
import sys
import types

import pytest

if "duckdb" not in sys.modules:
    sys.modules["duckdb"] = types.SimpleNamespace()

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import NodeOutput


def _graph_for_select_missing_column() -> dict:
    return {
        "nodes": [
            {
                "id": "source_1",
                "data": {
                    "kind": "source",
                    "label": "Source",
                    "sourceKind": "file",
                    "params": {"file_path": "dummy.csv", "file_format": "csv"},
                    "ports": {"in": None, "out": "table"},
                },
            },
            {
                "id": "transform_1",
                "data": {
                    "kind": "transform",
                    "label": "Transform",
                    "transformKind": "select",
                    "params": {"op": "select", "select": {"columns": ["missing_col"]}},
                    "ports": {"in": "table", "out": "table"},
                },
            },
        ],
        "edges": [{"id": "e1", "source": "source_1", "target": "transform_1"}],
    }


def _graph_for_dedupe(by_cols: list[str]) -> dict:
    return {
        "nodes": [
            {
                "id": "source_1",
                "data": {
                    "kind": "source",
                    "label": "Source",
                    "sourceKind": "file",
                    "params": {"file_path": "dummy.csv", "file_format": "csv"},
                    "ports": {"in": None, "out": "table"},
                },
            },
            {
                "id": "transform_1",
                "data": {
                    "kind": "transform",
                    "label": "Transform",
                    "transformKind": "dedupe",
                    "params": {"op": "dedupe", "dedupe": {"allColumns": False, "by": by_cols}},
                    "ports": {"in": "table", "out": "table"},
                },
            },
        ],
        "edges": [{"id": "e1", "source": "source_1", "target": "transform_1"}],
    }


@pytest.mark.asyncio
async def test_transform_select_missing_columns_emits_payload_mismatch_details(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")
    transform_calls = {"count": 0}

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data=[{"text": "hello"}],
        )

    def _fake_run_transform(params, input_tables, join_lookup=None):
        transform_calls["count"] += 1
        return types.SimpleNamespace(
            payload_bytes=b"text\nhello\n",
            mime_type="text/csv; charset=utf-8",
            meta={"columns": ["text"]},
        )

    monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
    monkeypatch.setattr(run_mod, "run_transform", _fake_run_transform)

    events = []
    store = DiskArtifactStore(tmp_path / "artifacts")
    cache = SqliteExecutionCache(str(tmp_path / "artifacts" / "meta" / "artifacts.sqlite"))
    await run_mod.run_graph(
        run_id="run-transform-missing-cols",
        graph=_graph_for_select_missing_column(),
        run_from=None,
        bus=RunEventBus("run-transform-missing-cols", on_emit=lambda e: events.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-transform-contract-mismatch",
    )

    assert transform_calls["count"] == 0
    finish = [e for e in events if e.get("type") == "node_finished" and e.get("nodeId") == "transform_1"]
    assert finish and finish[-1].get("status") == "failed"
    assert "payload schema mismatch" in str(finish[-1].get("error", "")).lower()
    assert finish[-1].get("errorCode") == "MISSING_COLUMN"
    assert (finish[-1].get("errorDetails") or {}).get("paramPath") == "select.columns"
    assert (finish[-1].get("errorDetails") or {}).get("missingColumns") == ["missing_col"]
    assert (finish[-1].get("errorDetails") or {}).get("availableColumns") == ["text"]
    assert (finish[-1].get("errorDetails") or {}).get("availableColumnsSource") in {"schema", "inferred"}

    out = [e for e in events if e.get("type") == "node_output" and e.get("nodeId") == "transform_1"]
    assert not out
    details_logs = [
        e for e in events if e.get("type") == "log" and e.get("nodeId") == "transform_1" and '"missingColumns"' in str(e.get("message", ""))
    ]
    assert details_logs
    detail_payload = json.loads(str(details_logs[-1]["message"]))
    assert detail_payload.get("code") == "MISSING_COLUMN"
    assert detail_payload.get("errorCode") == "MISSING_COLUMN"
    assert detail_payload.get("missingColumns") == ["missing_col"]
    assert detail_payload.get("availableColumns") == ["text"]
    assert detail_payload.get("paramPath") == "select.columns"
    transform_logs = [
        e for e in events if e.get("type") == "log" and e.get("nodeId") == "transform_1"
    ]
    assert not any("transform: produced" in str(e.get("message", "")) for e in transform_logs)


@pytest.mark.asyncio
async def test_transform_dedupe_empty_by_returns_column_selection_required(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")
    transform_calls = {"count": 0}

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data=[{"text": "hello", "other": "x"}],
        )

    def _fake_run_transform(params, input_tables, join_lookup=None):
        transform_calls["count"] += 1
        return types.SimpleNamespace(
            payload_bytes=b"text\nhello\n",
            mime_type="text/csv; charset=utf-8",
            meta={"columns": ["text"]},
        )

    monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
    monkeypatch.setattr(run_mod, "run_transform", _fake_run_transform)

    events = []
    store = DiskArtifactStore(tmp_path / "artifacts")
    cache = SqliteExecutionCache(str(tmp_path / "artifacts" / "meta" / "artifacts.sqlite"))
    await run_mod.run_graph(
        run_id="run-transform-dedupe-none",
        graph=_graph_for_dedupe([]),
        run_from=None,
        bus=RunEventBus("run-transform-dedupe-none", on_emit=lambda e: events.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-transform-contract-mismatch",
    )

    assert transform_calls["count"] == 0
    finish = [e for e in events if e.get("type") == "node_finished" and e.get("nodeId") == "transform_1"]
    assert finish and finish[-1].get("status") == "failed"
    assert finish[-1].get("errorCode") == "COLUMN_SELECTION_REQUIRED"
    details = finish[-1].get("errorDetails") or {}
    assert details.get("paramPath") == "params.dedupe.by"
    assert details.get("missingColumns") == []
    assert details.get("availableColumns") == ["text", "other"]


@pytest.mark.asyncio
async def test_transform_dedupe_missing_column_returns_missing_column(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data=[{"text": "hello", "other": "x"}],
        )

    monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)

    events = []
    store = DiskArtifactStore(tmp_path / "artifacts")
    cache = SqliteExecutionCache(str(tmp_path / "artifacts" / "meta" / "artifacts.sqlite"))
    await run_mod.run_graph(
        run_id="run-transform-dedupe-missing",
        graph=_graph_for_dedupe(["text", "nope"]),
        run_from=None,
        bus=RunEventBus("run-transform-dedupe-missing", on_emit=lambda e: events.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-transform-contract-mismatch",
    )

    finish = [e for e in events if e.get("type") == "node_finished" and e.get("nodeId") == "transform_1"]
    assert finish and finish[-1].get("status") == "failed"
    assert finish[-1].get("errorCode") == "MISSING_COLUMN"
    details = finish[-1].get("errorDetails") or {}
    assert details.get("paramPath") == "params.dedupe.by"
    assert details.get("missingColumns") == ["nope"]
    assert details.get("availableColumns") == ["text", "other"]


@pytest.mark.asyncio
async def test_transform_non_table_payload_schema_type_fails_with_expected_actual(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")
    transform_calls = {"count": 0}
    monkeypatch.setenv("STRICT_SCHEMA_EDGE_CHECKS", "0")
    monkeypatch.setenv("STRICT_COERCION_POLICY", "0")

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data=[{"text": "hello"}],
        )

    def _fake_source_payload_schema(out_contract, data_value, source_meta=None):
        return {"schema_version": 1, "type": "json"}

    def _fake_run_transform(params, input_tables, join_lookup=None):
        transform_calls["count"] += 1
        return types.SimpleNamespace(
            payload_bytes=b"text\nhello\n",
            mime_type="text/csv; charset=utf-8",
            meta={"columns": ["text"]},
        )

    monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
    monkeypatch.setattr(run_mod, "_source_payload_schema", _fake_source_payload_schema)
    monkeypatch.setattr(run_mod, "run_transform", _fake_run_transform)

    events = []
    store = DiskArtifactStore(tmp_path / "artifacts")
    cache = SqliteExecutionCache(str(tmp_path / "artifacts" / "meta" / "artifacts.sqlite"))
    graph = _graph_for_select_missing_column()
    graph["nodes"][1]["data"]["params"] = {"op": "limit", "limit": {"n": 1}}

    await run_mod.run_graph(
        run_id="run-transform-schema-type",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-transform-schema-type", on_emit=lambda e: events.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-transform-contract-mismatch",
    )

    assert transform_calls["count"] == 0
    out = [e for e in events if e.get("type") == "node_output" and e.get("nodeId") == "transform_1"]
    assert not out
    details_logs = [
        e for e in events if e.get("type") == "log" and e.get("nodeId") == "transform_1" and '"payloadType"' in str(e.get("message", ""))
    ]
    assert details_logs
    detail_payload = json.loads(str(details_logs[-1]["message"]))
    assert detail_payload.get("code") == "PAYLOAD_SCHEMA_MISMATCH"
    assert detail_payload.get("expected", {}).get("payloadType") == "table"
    assert detail_payload.get("actual", {}).get("payloadType") == "json"
    assert isinstance(detail_payload.get("actual", {}).get("artifactId"), str)


@pytest.mark.asyncio
async def test_transform_table_input_requires_typed_columns(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")
    transform_calls = {"count": 0}

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data=[{"text": "hello"}],
        )

    def _fake_source_payload_schema(out_contract, data_value, source_meta=None):
        # Simulate legacy/invalid table payload metadata without typed columns.
        return {"schema_version": 1, "type": "table", "columns": []}

    def _fake_run_transform(params, input_tables, join_lookup=None):
        transform_calls["count"] += 1
        return types.SimpleNamespace(
            payload_bytes=b"text\nhello\n",
            mime_type="text/csv; charset=utf-8",
            meta={"columns": ["text"]},
        )

    monkeypatch.setenv("STRICT_SCHEMA_EDGE_CHECKS", "1")
    monkeypatch.setenv("STRICT_COERCION_POLICY", "1")
    monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
    monkeypatch.setattr(run_mod, "_source_payload_schema", _fake_source_payload_schema)
    monkeypatch.setattr(run_mod, "run_transform", _fake_run_transform)

    events = []
    store = DiskArtifactStore(tmp_path / "artifacts")
    cache = SqliteExecutionCache(str(tmp_path / "artifacts" / "meta" / "artifacts.sqlite"))
    graph = _graph_for_select_missing_column()
    graph["nodes"][1]["data"]["params"] = {"op": "limit", "limit": {"n": 1}}

    await run_mod.run_graph(
        run_id="run-transform-typed-columns-required",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-transform-typed-columns-required", on_emit=lambda e: events.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-transform-contract-mismatch",
    )

    assert transform_calls["count"] == 0
    finish = [e for e in events if e.get("type") == "node_finished" and e.get("nodeId") == "transform_1"]
    assert finish and finish[-1].get("status") == "failed"
    assert finish[-1].get("errorCode") == "CONTRACT_EDGE_TYPED_SCHEMA_MISSING"
    details = finish[-1].get("errorDetails") or {}
    assert details.get("expected", {}).get("inPortType") == "table"
    assert details.get("expected", {}).get("typedSchema", {}).get("fields") == "non-empty"
    assert details.get("actual", {}).get("typedSchema", {}).get("fields") == []

    out = [e for e in events if e.get("type") == "node_output" and e.get("nodeId") == "transform_1"]
    assert not out
    detail_logs = [
        e
        for e in events
        if e.get("type") == "log"
        and e.get("nodeId") == "transform_1"
        and '"CONTRACT_EDGE_TYPED_SCHEMA_MISSING"' in str(e.get("message", ""))
    ]
    assert detail_logs


@pytest.mark.asyncio
async def test_transform_derive_engine_error_emits_expr_invalid(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data=[{"text": "hello"}],
        )

    def _fake_run_transform(params, input_tables, join_lookup=None):
        raise RuntimeError("Binder Error: column \"oops\" not found")

    monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
    monkeypatch.setattr(run_mod, "run_transform", _fake_run_transform)

    events = []
    store = DiskArtifactStore(tmp_path / "artifacts")
    cache = SqliteExecutionCache(str(tmp_path / "artifacts" / "meta" / "artifacts.sqlite"))
    graph = _graph_for_select_missing_column()
    graph["nodes"][1]["data"]["transformKind"] = "derive"
    graph["nodes"][1]["data"]["params"] = {
        "op": "derive",
        "derive": {"columns": [{"name": "x", "expr": "oops + 1"}]},
    }

    await run_mod.run_graph(
        run_id="run-transform-derive-expr-invalid",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-transform-derive-expr-invalid", on_emit=lambda e: events.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-transform-contract-mismatch",
    )

    finish = [e for e in events if e.get("type") == "node_finished" and e.get("nodeId") == "transform_1"]
    assert finish and finish[-1].get("status") == "failed"
    out = [e for e in events if e.get("type") == "node_output" and e.get("nodeId") == "transform_1"]
    assert not out
    details_logs = [
        e for e in events if e.get("type") == "log" and e.get("nodeId") == "transform_1" and '"EXPR_INVALID"' in str(e.get("message", ""))
    ]
    assert details_logs
    detail_payload = json.loads(str(details_logs[-1]["message"]))
    assert detail_payload.get("code") == "EXPR_INVALID"
    assert detail_payload.get("expected", {}).get("op") == "derive"
    assert "column" in str(detail_payload.get("actual", {}).get("engineError", "")).lower()


@pytest.mark.asyncio
async def test_source_and_transform_emit_table_schema_envelope(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data=[{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
        )

    def _fake_run_transform(params, input_tables, join_lookup=None):
        return types.SimpleNamespace(
            payload_bytes=b"id\n1\n2\n",
            mime_type="text/csv; charset=utf-8",
            meta={"columns": ["id"], "row_count": 2},
        )

    monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
    monkeypatch.setattr(run_mod, "run_transform", _fake_run_transform)

    events = []
    store = DiskArtifactStore(tmp_path / "artifacts")
    cache = SqliteExecutionCache(str(tmp_path / "artifacts" / "meta" / "artifacts.sqlite"))
    graph = _graph_for_select_missing_column()
    graph["nodes"][1]["data"]["params"] = {"op": "select", "select": {"columns": ["id"]}}

    await run_mod.run_graph(
        run_id="run-transform-schema-envelope",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-transform-schema-envelope", on_emit=lambda e: events.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-transform-schema-envelope",
    )

    source_output = [e for e in events if e.get("type") == "node_output" and e.get("nodeId") == "source_1"]
    transform_output = [e for e in events if e.get("type") == "node_output" and e.get("nodeId") == "transform_1"]
    assert source_output and transform_output

    source_art = await store.get(source_output[-1]["artifactId"])
    transform_art = await store.get(transform_output[-1]["artifactId"])

    src_ps = source_art.payload_schema or {}
    xfm_ps = transform_art.payload_schema or {}

    src_schema = src_ps.get("schema")
    xfm_schema = xfm_ps.get("schema")
    assert isinstance(src_schema, dict)
    assert isinstance(xfm_schema, dict)
    assert src_schema.get("contract") == "TABLE_V1"
    assert xfm_schema.get("contract") == "TABLE_V1"
    assert isinstance((src_schema.get("table") or {}).get("columns"), list)
    assert isinstance((xfm_schema.get("table") or {}).get("columns"), list)
    assert (xfm_schema.get("stats") or {}).get("rowCount") == 2
