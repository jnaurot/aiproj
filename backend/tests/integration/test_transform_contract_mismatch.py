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
                    "params": {"file_path": "dummy.txt", "file_format": "txt"},
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
    )

    assert transform_calls["count"] == 0
    finish = [e for e in events if e.get("type") == "node_finished" and e.get("nodeId") == "transform_1"]
    assert finish and finish[-1].get("status") == "failed"
    assert "payload schema mismatch" in str(finish[-1].get("error", "")).lower()

    out = [e for e in events if e.get("type") == "node_output" and e.get("nodeId") == "transform_1"]
    assert out
    error_artifact_id = out[-1]["artifactId"]
    payload = json.loads((await store.read(error_artifact_id)).decode("utf-8"))
    assert payload.get("code") == "PAYLOAD_SCHEMA_MISMATCH"
    assert payload.get("details", {}).get("missingColumns") == ["missing_col"]
    assert payload.get("details", {}).get("expected", {}).get("requiredColumns") == ["missing_col"]
    transform_logs = [
        e for e in events if e.get("type") == "log" and e.get("nodeId") == "transform_1"
    ]
    assert not any("transform: produced" in str(e.get("message", "")) for e in transform_logs)


@pytest.mark.asyncio
async def test_transform_non_table_payload_schema_type_fails_with_expected_actual(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")
    transform_calls = {"count": 0}

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data=[{"text": "hello"}],
        )

    def _fake_source_payload_schema(out_contract, data_value):
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
    )

    assert transform_calls["count"] == 0
    out = [e for e in events if e.get("type") == "node_output" and e.get("nodeId") == "transform_1"]
    assert out
    payload = json.loads((await store.read(out[-1]["artifactId"])).decode("utf-8"))
    details = payload.get("details", {})
    assert payload.get("code") == "PAYLOAD_SCHEMA_MISMATCH"
    assert details.get("expected", {}).get("payloadType") == "table"
    assert details.get("actual", {}).get("payloadType") == "json"
    assert isinstance(details.get("actual", {}).get("artifactId"), str)


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
    )

    out = [e for e in events if e.get("type") == "node_output" and e.get("nodeId") == "transform_1"]
    assert out
    payload = json.loads((await store.read(out[-1]["artifactId"])).decode("utf-8"))
    assert payload.get("code") == "EXPR_INVALID"
    assert payload.get("details", {}).get("expected", {}).get("op") == "derive"
    assert "column" in str(payload.get("details", {}).get("actual", {}).get("engineError", "")).lower()
