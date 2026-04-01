import pandas as pd
import pytest

from app.runner.nodes import transform as transform_mod
from app.runner.nodes.transform import TransformSqlGuardError, normalize_transform_params, run_transform


def _table() -> pd.DataFrame:
	return pd.DataFrame([{"id": 1, "v": "a"}, {"id": 2, "v": "b"}])


def test_transform_sql_safe_mode_blocks_non_select_statement():
	params = normalize_transform_params(
		{
			"op": "sql",
			"sql": {"query": "delete from input where id = 1", "safe_mode": True},
		}
	)
	with pytest.raises(TransformSqlGuardError) as exc:
		run_transform(params=params, input_tables={"in": _table()}, join_lookup=None)
	assert exc.value.code == "TRANSFORM_SQL_SAFE_MODE_VIOLATION"
	assert exc.value.details.get("paramPath") == "params.sql.query"


def test_transform_sql_timeout_enforced(monkeypatch):
	params = normalize_transform_params(
		{
			"op": "sql",
			"sql": {"query": "select * from input", "max_runtime_ms": 1},
		}
	)

	def _slow_run(con, query):
		import time

		time.sleep(0.05)
		return con.execute(query).df()

	monkeypatch.setattr(transform_mod, "_run_sql_dataframe", _slow_run)
	with pytest.raises(TransformSqlGuardError) as exc:
		run_transform(params=params, input_tables={"in": _table()}, join_lookup=None)
	assert exc.value.code == "TRANSFORM_SQL_TIMEOUT"
	assert exc.value.details.get("maxRuntimeMs") == 1


def test_transform_sql_output_row_limit_enforced():
	params = normalize_transform_params(
		{
			"op": "sql",
			"sql": {"query": "select * from input", "max_output_rows": 1},
		}
	)
	with pytest.raises(TransformSqlGuardError) as exc:
		run_transform(params=params, input_tables={"in": _table()}, join_lookup=None)
	assert exc.value.code == "TRANSFORM_SQL_OUTPUT_ROW_LIMIT_EXCEEDED"


def test_transform_sql_structured_error_payloads_include_details():
	params = normalize_transform_params(
		{
			"op": "sql",
			"sql": {"query": "select * from input", "max_output_rows": 1},
		}
	)
	with pytest.raises(TransformSqlGuardError) as exc:
		run_transform(params=params, input_tables={"in": _table()}, join_lookup=None)
	details = exc.value.details
	assert details.get("op") == "sql"
	assert details.get("paramPath") == "params.sql.max_output_rows"
	assert details.get("maxOutputRows") == 1
	assert details.get("actualRows") == 2


def test_transform_sql_defaults_applied_when_not_explicitly_set(monkeypatch):
	monkeypatch.setenv("TRANSFORM_SQL_MAX_OUTPUT_ROWS", "1")
	params = normalize_transform_params(
		{
			"op": "sql",
			"sql": {"query": "select * from input"},
		}
	)
	with pytest.raises(TransformSqlGuardError) as exc:
		run_transform(params=params, input_tables={"in": _table()}, join_lookup=None)
	assert exc.value.code == "TRANSFORM_SQL_OUTPUT_ROW_LIMIT_EXCEEDED"
