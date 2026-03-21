from types import SimpleNamespace

import httpx
import pandas as pd
import pytest

from app.executors.source import exec_source
from app.runner.metadata import NodeOutput


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
