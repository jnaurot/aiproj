import asyncio
from types import SimpleNamespace

import httpx
import pandas as pd
import pytest

from app.executors.source import exec_source
from app.runner.metadata import FileMetadata, NodeOutput


def _ctx():
	async def _emit(*_args, **_kwargs):
		return None

	return SimpleNamespace(
		bus=SimpleNamespace(emit=_emit),
		artifact_store=SimpleNamespace(),
		graph_id="graph_test",
	)


@pytest.mark.asyncio
async def test_source_file_csv_success(tmp_path):
	file_path = tmp_path / "data.csv"
	pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}).to_csv(file_path, index=False)

	node = {
		"id": "n_source",
		"data": {
			"params": {
				"source_type": "file",
				"file_path": str(file_path),
				"file_format": "csv",
				"output_mode": "table",
			}
		},
	}
	result = await exec_source("run_1", node, _ctx())
	assert isinstance(result, NodeOutput)
	assert result.status == "succeeded"
	assert isinstance(result.data, list)
	assert result.metadata is not None
	assert result.metadata.row_count == 2


@pytest.mark.asyncio
async def test_source_file_not_found_returns_failed():
	node = {
		"id": "n_source",
		"data": {
			"params": {
				"source_type": "file",
				"file_path": "does-not-exist.csv",
				"file_format": "csv",
			}
		},
	}
	result = await exec_source("run_2", node, _ctx())
	assert result.status == "failed"
	assert "not found" in (result.error or "").lower()


@pytest.mark.asyncio
async def test_source_api_success(monkeypatch):
	class _Resp:
		headers = {"content-type": "application/json"}

		def raise_for_status(self):
			return None

		def json(self):
			return [{"ok": True}]

		@property
		def text(self):
			return '[{"ok":true}]'

	class _Client:
		async def __aenter__(self):
			return self

		async def __aexit__(self, exc_type, exc, tb):
			return False

		async def request(self, **kwargs):
			return _Resp()

	monkeypatch.setattr("app.executors.source.httpx.AsyncClient", _Client)
	node = {
		"id": "n_api",
		"data": {"params": {"source_type": "api", "url": "https://example.com", "method": "GET"}},
	}
	result = await exec_source("run_3", node, _ctx())
	assert result.status == "succeeded"


@pytest.mark.asyncio
async def test_source_api_missing_auth_secret_fails_fast(monkeypatch):
	class _Client:
		async def __aenter__(self):
			return self

		async def __aexit__(self, exc_type, exc, tb):
			return False

		async def request(self, **kwargs):
			raise AssertionError("request should not be called when auth secret is missing")

	monkeypatch.setattr("app.executors.source.httpx.AsyncClient", _Client)
	monkeypatch.delenv("API_TOKEN_MISSING", raising=False)
	node = {
		"id": "n_api_auth_missing",
		"data": {
			"sourceKind": "api",
			"params": {
				"url": "https://example.com",
				"method": "GET",
				"auth_type": "bearer",
				"auth_token_ref": "API_TOKEN_MISSING",
			},
		},
	}
	result = await exec_source("run_api_auth_missing", node, _ctx())
	assert result.status == "failed"
	assert "MISSING_SECRET" in str(result.error or "")


@pytest.mark.asyncio
async def test_source_api_retry_succeeds_after_transient_failure(monkeypatch):
	attempts = {"n": 0}

	class _Resp:
		status_code = 200
		headers = {"content-type": "application/json"}

		def raise_for_status(self):
			return None

		def json(self):
			return {"ok": True}

		@property
		def text(self):
			return '{"ok":true}'

	class _Client:
		async def __aenter__(self):
			return self

		async def __aexit__(self, exc_type, exc, tb):
			return False

		async def request(self, **kwargs):
			attempts["n"] += 1
			if attempts["n"] == 1:
				raise httpx.ConnectError("boom", request=httpx.Request("GET", kwargs.get("url", "https://example.com")))
			return _Resp()

	monkeypatch.setattr("app.executors.source.httpx.AsyncClient", _Client)
	node = {
		"id": "n_api_retry",
		"data": {
			"sourceKind": "api",
			"params": {
				"url": "https://example.com",
				"method": "GET",
				"retry": {"max_attempts": 2, "backoff_seconds": 0, "jitter_seconds": 0},
			},
		},
	}
	result = await exec_source("run_api_retry", node, _ctx())
	assert result.status == "succeeded"
	assert attempts["n"] == 2


@pytest.mark.asyncio
async def test_source_invalid_type_returns_failed():
	node = {"id": "n_bad", "data": {"params": {"source_type": "invalid"}}}
	result = await exec_source("run_4", node, _ctx())
	assert result.status == "failed"
	assert "unknown source_type" in (result.error or "").lower()


@pytest.mark.asyncio
async def test_source_database_preserves_string_query_when_source_kind_on_node(monkeypatch):
	captured = {}

	async def _fake_handle_database_source(node_id, params, bus, run_id, graph_id, forced_output_mode=None):
		captured["query"] = params.get("query")
		return NodeOutput(status="succeeded", data=[], metadata=None, execution_time_ms=0.0)

	monkeypatch.setattr("app.executors.source._handle_database_source", _fake_handle_database_source)
	node = {
		"id": "n_db",
		"data": {
			"sourceKind": "database",
			"params": {
				"connection_string": "postgresql://user:pass@host/db",
				"query": "select * from faculty;",
				"limit": 5,
			},
		},
	}
	result = await exec_source("run_db", node, _ctx())
	assert result.status == "succeeded"
	assert captured.get("query") == "select * from faculty;"


@pytest.mark.asyncio
async def test_source_output_mode_override_honors_params_output_mode(tmp_path):
	file_path = tmp_path / "data.csv"
	pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}).to_csv(file_path, index=False)

	node = {
		"id": "n_source_mode_override",
		"data": {
			"sourceKind": "file",
			"params": {
				"file_path": str(file_path),
				"file_format": "csv",
				"output": {"mode": "json"},
			},
		},
	}
	result = await exec_source("run_mode_override", node, _ctx())
	assert result.status == "succeeded"
	assert isinstance(result.data, list)


@pytest.mark.asyncio
async def test_source_database_uses_connection_ref_from_env(monkeypatch):
	class _Engine:
		def dispose(self):
			return None

	captured = {}

	def _fake_create_engine(conn_string):
		captured["conn_string"] = conn_string
		return _Engine()

	def _fake_read_sql(query, _engine):
		captured["query"] = query
		return pd.DataFrame({"id": [1], "name": ["x"]})

	import app.executors.source as source_mod

	monkeypatch.setattr("app.executors.source.HAS_DATABASE", True)
	monkeypatch.setattr(source_mod, "sqlalchemy", SimpleNamespace(create_engine=_fake_create_engine), raising=False)
	monkeypatch.setattr("app.executors.source.pd.read_sql", _fake_read_sql)
	monkeypatch.setenv("DB_CONN_TEST", "sqlite:///memory")
	node = {
		"id": "n_db_ref",
		"data": {
			"sourceKind": "database",
			"params": {
				"connection_ref": "DB_CONN_TEST",
				"query": "select * from items",
			},
		},
	}
	result = await exec_source("run_db_ref", node, _ctx())
	assert result.status == "succeeded"
	assert captured.get("conn_string") == "sqlite:///memory"
	assert "select * from items" in str(captured.get("query") or "").lower()


@pytest.mark.asyncio
async def test_source_database_missing_connection_ref_secret_fails(monkeypatch):
	monkeypatch.setattr("app.executors.source.HAS_DATABASE", True)
	monkeypatch.delenv("DB_CONN_MISSING", raising=False)
	node = {
		"id": "n_db_ref_missing",
		"data": {
			"sourceKind": "database",
			"params": {
				"connection_ref": "DB_CONN_MISSING",
				"table_name": "items",
			},
		},
	}
	result = await exec_source("run_db_ref_missing", node, _ctx())
	assert result.status == "failed"
	assert "MISSING_SECRET" in str(result.error or "")


@pytest.mark.asyncio
async def test_source_database_rejects_unsafe_table_name(monkeypatch):
	monkeypatch.setattr("app.executors.source.HAS_DATABASE", True)
	node = {
		"id": "n_db_bad_table",
		"data": {
			"sourceKind": "database",
			"params": {
				"connection_string": "sqlite:///memory",
				"table_name": "users; drop table users;--",
			},
		},
	}
	result = await exec_source("run_db_bad_table", node, _ctx())
	assert result.status == "failed"
	assert "INVALID_IDENTIFIER" in str(result.error or "")


@pytest.mark.asyncio
async def test_source_database_incremental_filters_by_saved_cursor(monkeypatch, tmp_path):
	class _Engine:
		def dispose(self):
			return None

	def _fake_create_engine(_conn_string):
		return _Engine()

	def _fake_read_sql(_query, _engine):
		return pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})

	import app.executors.source as source_mod

	state_file = tmp_path / "source_incremental_state.json"
	monkeypatch.setenv("SOURCE_INCREMENTAL_STATE_FILE", str(state_file))
	monkeypatch.setattr("app.executors.source.HAS_DATABASE", True)
	monkeypatch.setattr(source_mod, "sqlalchemy", SimpleNamespace(create_engine=_fake_create_engine), raising=False)
	monkeypatch.setattr("app.executors.source.pd.read_sql", _fake_read_sql)

	node = {
		"id": "n_db_incremental",
		"data": {
			"sourceKind": "database",
			"params": {
				"connection_string": "sqlite:///memory",
				"query": "select * from items",
				"incremental": {"enabled": True, "cursor_column": "id", "cursor_type": "int"},
			},
		},
	}

	first = await exec_source("run_db_inc_1", node, _ctx())
	assert first.status == "succeeded"
	assert isinstance(first.data, list)
	assert len(first.data) == 2

	second = await exec_source("run_db_inc_2", node, _ctx())
	assert second.status == "succeeded"
	assert isinstance(second.data, list)
	assert len(second.data) == 0


@pytest.mark.asyncio
async def test_source_object_store_mock_text_succeeds():
	node = {
		"id": "n_obj_store",
		"data": {
			"sourceKind": "object_store",
			"params": {
				"provider": "s3",
				"bucket": "demo",
				"key": "rows.csv",
				"file_format": "csv",
				"mock_text": "id,name\n1,alice\n2,bob\n",
			},
		},
	}
	result = await exec_source("run_obj_store", node, _ctx())
	assert result.status == "succeeded"
	assert isinstance(result.data, list)
	assert len(result.data) == 2
	assert isinstance(result.metadata.data_schema.get("source_observability"), dict)
	assert result.metadata.data_schema.get("source_observability", {}).get("source_kind") == "object_store"


@pytest.mark.asyncio
async def test_source_warehouse_mock_rows_succeeds():
	node = {
		"id": "n_wh",
		"data": {
			"sourceKind": "warehouse",
			"params": {
				"provider": "snowflake",
				"connection_ref": "conn:warehouse_default",
				"query": "select * from demo",
				"mock_rows": [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
			},
		},
	}
	result = await exec_source("run_wh", node, _ctx())
	assert result.status == "succeeded"
	assert isinstance(result.data, list)
	assert len(result.data) == 2
	assert isinstance(result.metadata.data_schema.get("source_observability"), dict)
	assert result.metadata.data_schema.get("source_observability", {}).get("source_kind") == "warehouse"


@pytest.mark.asyncio
async def test_source_priming_only_sets_metadata_marker(tmp_path):
	file_path = tmp_path / "data.csv"
	pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}).to_csv(file_path, index=False)
	node = {
		"id": "n_source_prime",
		"data": {
			"sourceKind": "file",
			"params": {
				"file_path": str(file_path),
				"file_format": "csv",
				"priming": {"enabled": True, "mode": "priming_only"},
			},
		},
	}
	result = await exec_source("run_prime_only", node, _ctx())
	assert result.status == "succeeded"
	assert isinstance(result.metadata.data_schema, dict)
	assert (result.metadata.data_schema.get("priming") or {}).get("enabled") is True
	assert (result.metadata.data_schema.get("priming") or {}).get("priming_only") is True
	assert isinstance(result.metadata.priming_artifact, dict)
	assert (result.metadata.priming_artifact or {}).get("version") == 1
	assert (result.metadata.priming_artifact or {}).get("schema_fingerprint")


def _priming_node(source_kind: str, priming: dict) -> dict:
	return {
		"id": f"n_{source_kind}_prime",
		"data": {
			"sourceKind": source_kind,
			"params": {
				"source_type": source_kind,
				"url": "https://example.com",
				"query": "select 1",
				"connection_string": "sqlite:///memory",
				"bucket": "demo",
				"key": "data.txt",
				"filename": "data.txt",
				"rel_path": ".",
				"file_format": "txt",
				"priming": priming,
			},
		},
	}


def _output(data, output_mode: str) -> NodeOutput:
	return NodeOutput(
		status="succeeded",
		data=data,
		metadata=FileMetadata(
			file_path="memory://source.out",
			file_type="csv" if output_mode == "table" else ("json" if output_mode == "json" else ("binary" if output_mode == "binary" else "txt")),
			mime_type="application/json" if output_mode == "json" else "text/plain",
			size_bytes=1,
			data_schema={"source_kind": "mock", "output_mode": output_mode},
			content_hash="h",
			node_id="n",
			params_hash="p",
		),
		execution_time_ms=0.0,
	)


def _ctx_with_events(events: list[dict]):
	async def _emit(payload):
		events.append(dict(payload))
		return None

	return SimpleNamespace(
		bus=SimpleNamespace(emit=_emit),
		artifact_store=SimpleNamespace(),
		graph_id="graph_test",
	)


@pytest.mark.asyncio
@pytest.mark.parametrize(
	"source_kind,handler_name,output_mode,data,assertion",
	[
		("file", "_handle_file_source", "table", [{"id": 1}, {"id": 2}, {"id": 3}], lambda out: isinstance(out, list) and 1 <= len(out) <= 2),
		("database", "_handle_database_source", "table", [{"id": 1}, {"id": 2}, {"id": 3}], lambda out: isinstance(out, list) and 1 <= len(out) <= 2),
		("api", "_handle_api_source", "json", [{"id": 1}, {"id": 2}, {"id": 3}], lambda out: isinstance(out, list) and 1 <= len(out) <= 2),
		("object_store", "_handle_object_store_source", "text", "abcdefghij", lambda out: isinstance(out, str) and len(out) < 10),
		("warehouse", "_handle_warehouse_source", "table", [{"id": 1}, {"id": 2}, {"id": 3}], lambda out: isinstance(out, list) and 1 <= len(out) <= 2),
	],
)
async def test_source_priming_bounds_apply_per_source_kind(monkeypatch, source_kind, handler_name, output_mode, data, assertion):
	async def _fake(*_args, **_kwargs):
		return _output(data, output_mode)

	monkeypatch.setattr(f"app.executors.source.{handler_name}", _fake)
	node = _priming_node(
		source_kind,
		{"enabled": True, "mode": "priming_only", "sample_rows": 2, "sample_bytes": 8, "timeout_ms": 1},
	)
	result = await exec_source("run_prime_bounds", node, _ctx())
	assert result.status == "succeeded"
	assert assertion(result.data)
	priming_meta = (result.metadata.data_schema or {}).get("priming") or {}
	assert priming_meta.get("enabled") is True
	assert priming_meta.get("sample_rows") == 2
	assert priming_meta.get("sample_bytes") == 8


@pytest.mark.asyncio
async def test_source_priming_timeout_sets_timed_out_marker(monkeypatch):
	async def _slow_file(*_args, **_kwargs):
		await asyncio.sleep(0.01)
		return _output([{"id": 1}, {"id": 2}], "table")

	monkeypatch.setattr("app.executors.source._handle_file_source", _slow_file)
	node = _priming_node(
		"file",
		{"enabled": True, "mode": "priming_only", "sample_rows": 1, "sample_bytes": 1024, "timeout_ms": 1},
	)
	result = await exec_source("run_prime_timeout", node, _ctx())
	assert result.status == "succeeded"
	assert ((result.metadata.data_schema or {}).get("priming") or {}).get("timed_out") is True


@pytest.mark.parametrize(
	"data,mode,mime_hint,file_hint,expected_type,expected_mime",
	[
		("plain text", "text", "text/plain; charset=utf-8", "", "text", "text/plain; charset=utf-8"),
		('{"ok":true}', "text", "text/plain; charset=utf-8", "", "json", "application/json"),
		([{"a": 1}], "table", "text/csv", "", "table", "text/csv"),
		([{"a": 1}], "json", "application/json", "", "json", "application/json"),
		(b"\x89PNG\r\n", "binary", "application/octet-stream", "png", "image", "image/png"),
		(b"RIFF....", "binary", "application/octet-stream", "wav", "audio", "audio/wav"),
	],
)
def test_detect_payload_and_mime_vectors(data, mode, mime_hint, file_hint, expected_type, expected_mime):
	import app.executors.source as source_mod

	detected = source_mod._detect_payload_and_mime(
		data=data,
		output_mode=mode,
		current_mime=mime_hint,
		file_format_hint=file_hint or None,
	)
	assert detected["payload_type"] == expected_type
	assert detected["mime_type"] == expected_mime
	assert 0.0 <= float(detected["confidence"]) <= 1.0
	assert isinstance(detected["detected_by"], str) and detected["detected_by"]


@pytest.mark.asyncio
async def test_ambiguous_detection_emits_warning_log(monkeypatch):
	async def _fake_file(*_args, **_kwargs):
		return _output("not-binary-content", "binary")

	events: list[dict] = []
	monkeypatch.setattr("app.executors.source._handle_file_source", _fake_file)
	node = _priming_node("file", {"enabled": True, "mode": "priming_only", "sample_rows": 2, "sample_bytes": 100, "timeout_ms": 100})
	result = await exec_source("run_prime_ambiguous", node, _ctx_with_events(events))
	assert result.status == "succeeded"
	assert any("PRIMING_TYPE_DETECTION_AMBIGUOUS" in str(e.get("message") or "") for e in events if e.get("type") == "log")
