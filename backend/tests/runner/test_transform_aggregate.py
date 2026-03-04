from __future__ import annotations

import pandas as pd
import pytest

from app.runner.nodes.transform import execute_transform_op, normalize_transform_params


def _fixture_df() -> pd.DataFrame:
	return pd.DataFrame(
		[
			{"category": "alpha", "text": "hi", "price": 10.0, "qty": 1, "user": "u1"},
			{"category": "alpha", "text": "hello", "price": 20.0, "qty": 2, "user": "u2"},
			{"category": "beta", "text": None, "price": 5.0, "qty": 1, "user": "u1"},
			{"category": "beta", "text": "world", "price": 7.0, "qty": 3, "user": "u3"},
			{"category": "beta", "text": "!", "price": None, "qty": 5, "user": "u3"},
			{"category": None, "text": "x", "price": 1.0, "qty": 1, "user": "u9"},
		]
	)


def _aggregate_params(*, group_by: list[str], metrics: list[dict]) -> dict:
	return normalize_transform_params(
		{
			"op": "aggregate",
			"aggregate": {
				"groupBy": group_by,
				"metrics": metrics,
			},
		}
	)


def test_aggregate_global_metrics():
	df = _fixture_df()
	params = _aggregate_params(
		group_by=[],
		metrics=[
			{"name": "row_count", "op": "count_rows"},
			{"name": "avg_price", "op": "mean", "column": "price"},
			{"name": "max_qty", "op": "max", "column": "qty"},
		],
	)
	out = execute_transform_op("aggregate", params, {"in": df}).to_dict(orient="records")
	assert len(out) == 1
	assert out[0]["row_count"] == 6
	assert out[0]["avg_price"] == pytest.approx(8.6)
	assert out[0]["max_qty"] == 5


def test_aggregate_grouped_metrics_deterministic_order():
	df = _fixture_df()
	params = _aggregate_params(
		group_by=["category"],
		metrics=[
			{"name": "row_count", "op": "count_rows"},
			{"name": "distinct_users", "op": "count_distinct", "column": "user"},
			{"name": "avg_length_text", "op": "avg_length", "column": "text"},
		],
	)
	out1 = execute_transform_op("aggregate", params, {"in": df}).to_dict(orient="records")
	out2 = execute_transform_op("aggregate", params, {"in": df}).to_dict(orient="records")
	assert out1 == out2
	assert out1[0]["category"] == "alpha"
	assert out1[1]["category"] == "beta"
	assert pd.isna(out1[2]["category"])
	assert out1[0]["row_count"] == 2
	assert out1[0]["distinct_users"] == 2
	assert out1[1]["row_count"] == 3
	assert out1[1]["distinct_users"] == 2
	assert out1[2]["row_count"] == 1
	assert out1[2]["distinct_users"] == 1


def test_aggregate_missing_groupby_column_fails():
	df = _fixture_df()
	params = _aggregate_params(
		group_by=["does_not_exist"],
		metrics=[{"name": "row_count", "op": "count_rows"}],
	)
	with pytest.raises(ValueError, match="aggregate columns missing from input"):
		execute_transform_op("aggregate", params, {"in": df})


def test_aggregate_missing_metric_column_fails():
	df = _fixture_df()
	params = _aggregate_params(
		group_by=[],
		metrics=[{"name": "sum_nope", "op": "sum", "column": "nope"}],
	)
	with pytest.raises(ValueError, match="aggregate columns missing from input"):
		execute_transform_op("aggregate", params, {"in": df})


def test_aggregate_groupby_null_is_its_own_group():
	df = _fixture_df()
	params = _aggregate_params(
		group_by=["category"],
		metrics=[{"name": "rows", "op": "count_rows"}],
	)
	out = execute_transform_op("aggregate", params, {"in": df}).to_dict(orient="records")
	null_group = [r for r in out if pd.isna(r["category"])]
	assert len(null_group) == 1
	assert null_group[0]["rows"] == 1
