import sys
import types
from datetime import datetime, timezone

if "duckdb" not in sys.modules:
    sys.modules["duckdb"] = types.SimpleNamespace()

from app.runner.artifacts import Artifact
from app.runner.contracts import (
    AUDIO_V1,
    IMAGE_V1,
    JSON_ANY_V1,
    TABLE_ANY_V1,
    TEXT_V1,
    VIDEO_V1,
    canonical_schema_for_contract,
    default_contract_for_node,
    schema_fingerprint,
)
from app.runner.run import (
    _available_columns_for_port,
    _cached_artifact_contract_mismatch,
    _expected_schema_contract_for_node,
    _missing_column_details,
    _source_payload_schema,
    _table_schema_envelope,
)


def _node(kind: str, *, source_kind: str | None = None, params: dict | None = None) -> dict:
    data = {"kind": kind, "params": params or {}, "ports": {"in": None, "out": "table"}}
    if source_kind:
        data["sourceKind"] = source_kind
    return {"id": "n1", "data": data}


def _artifact_with_schema_fp(schema_fp: str) -> Artifact:
    return Artifact(
        artifact_id="a" * 64,
        node_kind="source",
        params_hash="p" * 64,
        upstream_ids=[],
        created_at=datetime.now(timezone.utc),
        execution_version="v1",
        mime_type="application/octet-stream",
        port_type="binary",
        size_bytes=0,
        storage_uri="artifact://a",
        payload_schema={
            "schema_version": 1,
            "type": "binary",
            "artifactMetadataV1": {
                "metadataVersion": 1,
                "execKey": "a" * 64,
                "nodeId": "n1",
                "nodeType": "source",
                "nodeImplVersion": "SOURCE@1",
                "paramsFingerprint": "p" * 64,
                "upstreamArtifactIds": [],
                "contractFingerprint": schema_fp,
                "schemaFingerprint": schema_fp,
                "mimeType": "application/octet-stream",
                "portType": "binary",
                "createdAt": datetime.now(timezone.utc).isoformat(),
                "runId": "r1",
                "graphId": "g1",
            },
        },
        run_id="r1",
        graph_id="g1",
        node_id="n1",
        exec_key="a" * 64,
    )


def test_default_contract_mapping():
    assert default_contract_for_node(_node("source", source_kind="file")) == TABLE_ANY_V1
    assert (
        default_contract_for_node(
            _node("source", source_kind="file", params={"file_format": "png"})
        )
        == IMAGE_V1
    )
    assert (
        default_contract_for_node(
            _node("source", source_kind="file", params={"file_format": "jpg"})
        )
        == IMAGE_V1
    )
    assert (
        default_contract_for_node(
            _node("source", source_kind="file", params={"file_format": "tif"})
        )
        == IMAGE_V1
    )
    assert (
        default_contract_for_node(
            _node("source", source_kind="file", params={"file_format": "mp3"})
        )
        == AUDIO_V1
    )
    assert (
        default_contract_for_node(
            _node("source", source_kind="file", params={"file_format": "mp4"})
        )
        == VIDEO_V1
    )
    assert default_contract_for_node(_node("source", source_kind="api")) == JSON_ANY_V1
    assert default_contract_for_node(_node("llm", params={"output_mode": "text"})) == TEXT_V1


def test_expected_schema_source_explicit_vs_default():
    explicit_node = _node("llm", params={"output_schema": {"type": "object"}})
    default_node = _node("source", source_kind="file")

    explicit = _expected_schema_contract_for_node(explicit_node)
    defaulted = _expected_schema_contract_for_node(default_node)

    assert explicit["schemaSource"] == "explicit"
    assert str(explicit["schemaFingerprint"])
    assert str(defaulted["schemaSource"]).startswith("default:")
    assert str(defaulted["schemaFingerprint"])


def test_default_schema_still_gates_cache_mismatch():
    node = _node("source", source_kind="file")
    expected = _expected_schema_contract_for_node(node)
    wrong = schema_fingerprint(canonical_schema_for_contract(TEXT_V1))
    art = _artifact_with_schema_fp(wrong)
    mismatch = _cached_artifact_contract_mismatch("source", node, art, expected)
    assert mismatch is not None
    assert mismatch.get("mismatchKind") == "schema_fingerprint"
    assert str(mismatch.get("expectedSchemaSource", "")).startswith("default:")


def test_image_default_schema_fingerprint_is_format_agnostic():
    png_expected = _expected_schema_contract_for_node(
        _node("source", source_kind="file", params={"file_format": "png"})
    )
    jpg_expected = _expected_schema_contract_for_node(
        _node("source", source_kind="file", params={"file_format": "jpg"})
    )
    jpeg_expected = _expected_schema_contract_for_node(
        _node("source", source_kind="file", params={"file_format": "jpeg"})
    )
    tif_expected = _expected_schema_contract_for_node(
        _node("source", source_kind="file", params={"file_format": "tif"})
    )
    assert png_expected["schemaSource"] == "default:IMAGE_V1"
    assert jpg_expected["schemaSource"] == "default:IMAGE_V1"
    assert jpeg_expected["schemaSource"] == "default:IMAGE_V1"
    assert tif_expected["schemaSource"] == "default:IMAGE_V1"
    assert png_expected["schemaFingerprint"] == jpg_expected["schemaFingerprint"]
    assert png_expected["schemaFingerprint"] == jpeg_expected["schemaFingerprint"]
    assert png_expected["schemaFingerprint"] == tif_expected["schemaFingerprint"]


def test_audio_default_schema_fingerprint_is_format_agnostic():
    mp3_expected = _expected_schema_contract_for_node(
        _node("source", source_kind="file", params={"file_format": "mp3"})
    )
    wav_expected = _expected_schema_contract_for_node(
        _node("source", source_kind="file", params={"file_format": "wav"})
    )
    assert mp3_expected["schemaSource"] == "default:AUDIO_V1"
    assert wav_expected["schemaSource"] == "default:AUDIO_V1"
    assert mp3_expected["schemaFingerprint"] == wav_expected["schemaFingerprint"]


def test_video_default_schema_fingerprint_is_format_agnostic():
    mp4_expected = _expected_schema_contract_for_node(
        _node("source", source_kind="file", params={"file_format": "mp4"})
    )
    webm_expected = _expected_schema_contract_for_node(
        _node("source", source_kind="file", params={"file_format": "webm"})
    )
    assert mp4_expected["schemaSource"] == "default:VIDEO_V1"
    assert webm_expected["schemaSource"] == "default:VIDEO_V1"
    assert mp4_expected["schemaFingerprint"] == webm_expected["schemaFingerprint"]


def test_table_v1_schema_fingerprint_ignores_stats_and_provenance():
    base = {
        "contract": "TABLE_V1",
        "version": 1,
        "table": {
            "columns": [
                {"name": "id", "type": "int64"},
                {"name": "name", "type": "string"},
            ]
        },
    }
    with_stats = {
        **base,
        "stats": {"rowCount": 10},
        "provenance": {"sourceKind": "db", "dbName": "example", "tableName": "users"},
    }
    with_other_stats = {
        **base,
        "stats": {"rowCount": 999},
        "provenance": {"sourceKind": "api", "endpoint": "https://example.com"},
    }
    assert schema_fingerprint(base) == schema_fingerprint(with_stats)
    assert schema_fingerprint(base) == schema_fingerprint(with_other_stats)


def test_table_v1_schema_fingerprint_ignores_coercion_descriptor():
    base = {
        "contract": "TABLE_V1",
        "version": 1,
        "table": {"columns": [{"name": "text", "type": "string"}]},
    }
    with_coercion = {
        "contract": "TABLE_V1",
        "version": 1,
        "table": {
            "columns": [{"name": "text", "type": "string"}],
            "coercion": {"mode": "text_1row", "lossy": False, "notes": "wrapped"},
        },
    }
    assert schema_fingerprint(base) == schema_fingerprint(with_coercion)


def test_table_v1_schema_fingerprint_changes_when_columns_change():
    a = {
        "contract": "TABLE_V1",
        "version": 1,
        "table": {"columns": [{"name": "id", "type": "int64"}]},
    }
    b = {
        "contract": "TABLE_V1",
        "version": 1,
        "table": {"columns": [{"name": "user_id", "type": "int64"}]},
    }
    c = {
        "contract": "TABLE_V1",
        "version": 1,
        "table": {"columns": [{"name": "id", "type": "string"}]},
    }
    assert schema_fingerprint(a) != schema_fingerprint(b)
    assert schema_fingerprint(a) != schema_fingerprint(c)


def test_table_v1_schema_fingerprint_normalizes_missing_types_to_unknown():
    missing = {
        "contract": "TABLE_V1",
        "version": 1,
        "table": {"columns": [{"name": "id"}]},
    }
    unknown = {
        "contract": "TABLE_V1",
        "version": 1,
        "table": {"columns": [{"name": "id", "type": "unknown"}]},
    }
    assert schema_fingerprint(missing) == schema_fingerprint(unknown)


def test_table_v1_schema_fingerprint_changes_when_split_out_column_changes():
    split_a = {
        "contract": "TABLE_V1",
        "version": 1,
        "table": {
            "columns": [
                {"name": "part", "type": "string"},
                {"name": "index", "type": "int"},
                {"name": "source_row", "type": "int"},
            ]
        },
    }
    split_b = {
        "contract": "TABLE_V1",
        "version": 1,
        "table": {
            "columns": [
                {"name": "chunk", "type": "string"},
                {"name": "index", "type": "int"},
                {"name": "source_row", "type": "int"},
            ]
        },
    }
    assert schema_fingerprint(split_a) != schema_fingerprint(split_b)


def test_source_payload_schema_carries_coercion_from_source_metadata():
    source_meta = types.SimpleNamespace(
        data_schema={"table_coercion": {"mode": "json_object_1row", "lossy": False}}
    )
    payload = _source_payload_schema("table", [{"a": 1}], source_meta)
    assert isinstance(payload, dict)
    assert payload.get("coercion", {}).get("mode") == "json_object_1row"


def test_source_payload_schema_prefers_typed_table_columns_from_source_metadata():
    source_meta = types.SimpleNamespace(
        data_schema={
            "table_columns": [
                {"name": "id", "type": "int"},
                {"name": "created_at", "type": "datetime"},
            ]
        }
    )
    payload = _source_payload_schema("table", [{"id": 1, "created_at": "2026-01-01"}], source_meta)
    assert isinstance(payload, dict)
    assert payload.get("columns") == [
        {"name": "id", "type": "int"},
        {"name": "created_at", "type": "datetime"},
    ]


def test_table_schema_envelope_places_coercion_under_table():
    env = _table_schema_envelope(
        columns=[{"name": "a", "type": "int"}],
        row_count=1,
        coercion={"mode": "binary_hex_1row", "lossy": True, "notes": "wrapped"},
    )
    assert env.get("table", {}).get("coercion", {}).get("mode") == "binary_hex_1row"
    assert env.get("table", {}).get("coercion", {}).get("lossy") is True


def test_available_columns_prefers_schema_then_inferred():
    cols, source = _available_columns_for_port(
        port="in",
        input_schema_cols_by_port={"in": [{"name": "text"}, {"name": "other"}]},
        input_columns={"in": ["text", "other", "runtime_only"]},
    )
    assert cols == ["text", "other"]
    assert source == "schema"

    cols2, source2 = _available_columns_for_port(
        port="in",
        input_schema_cols_by_port={"in": []},
        input_columns={"in": ["text", "other"]},
    )
    assert cols2 == ["text", "other"]
    assert source2 == "inferred"


def test_missing_column_details_payload_shape():
    details = _missing_column_details(
        op="dedupe",
        param_path="by",
        missing_columns=["missing"],
        available_columns=["text", "other"],
        available_source="schema",
    )
    assert details["errorCode"] == "MISSING_COLUMN"
    assert details["op"] == "dedupe"
    assert details["paramPath"] == "by"
    assert details["missingColumns"] == ["missing"]
    assert details["availableColumns"] == ["text", "other"]
    assert details["availableColumnsSource"] == "schema"
