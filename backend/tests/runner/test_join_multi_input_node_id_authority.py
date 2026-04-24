from __future__ import annotations

import pandas as pd
import pytest

from app.runner.nodes.transform import duckdb, normalize_transform_params, run_transform


def _require_duckdb() -> None:
	if duckdb is None:
		pytest.skip("duckdb not installed in test environment")


def _table_a() -> pd.DataFrame:
	return pd.DataFrame(
		[
			{"id": 1, "a_val": "a1"},
			{"id": 2, "a_val": "a2"},
			{"id": 3, "a_val": "a3"},
		]
	)


def _table_b() -> pd.DataFrame:
	return pd.DataFrame(
		[
			{"id": 1, "b_val": "b1"},
			{"id": 2, "b_val": "b2"},
			{"id": 4, "b_val": "b4"},
		]
	)


def _table_c() -> pd.DataFrame:
	return pd.DataFrame(
		[
			{"id": 1, "c_val": "c1"},
			{"id": 2, "c_val": "c2"},
			{"id": 5, "c_val": "c5"},
		]
	)


def test_join_multi_input_clauses_resolve_by_node_id_authority() -> None:
	_require_duckdb()
	norm = normalize_transform_params(
		{
			"op": "join",
			"join": {
				"clauses": [
					{
						"leftNodeId": "n_left",
						"leftCol": "id",
						"rightNodeId": "n_mid",
						"rightCol": "id",
						"how": "inner",
					},
					{
						"leftNodeId": "n_left",
						"leftCol": "id",
						"rightNodeId": "n_right",
						"rightCol": "id",
						"how": "inner",
					},
				]
			},
		}
	)
	res = run_transform(
		params=norm,
		input_tables={"in": _table_a()},
		join_lookup={"n_left": _table_a(), "n_mid": _table_b(), "n_right": _table_c()},
	)
	out_df = pd.read_csv(pd.io.common.BytesIO(res.payload_bytes))
	assert len(out_df) == 2
	assert set(out_df["id"].tolist()) == {1, 2}


def test_join_rejects_unresolved_node_id_with_deterministic_error() -> None:
	_require_duckdb()
	norm = normalize_transform_params(
		{
			"op": "join",
			"join": {
				"clauses": [
					{
						"leftNodeId": "n_left",
						"leftCol": "id",
						"rightNodeId": "n_missing",
						"rightCol": "id",
						"how": "inner",
					}
				]
			},
		}
	)
	with pytest.raises(ValueError, match="unknown node"):
		run_transform(
			params=norm,
			input_tables={"in": _table_a()},
			join_lookup={"n_left": _table_a()},
		)

