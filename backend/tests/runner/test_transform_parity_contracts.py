from __future__ import annotations

import json

import pandas as pd
import pytest

from app.runner.nodes.transform import OP_KEYS, duckdb, normalize_transform_params, run_transform
from app.runner.schemas import TransformParamsCurrent


def _base_table() -> pd.DataFrame:
	return pd.DataFrame(
		[
			{"id": 1, "text": "alpha one", "value": 10, "group": "a"},
			{"id": 2, "text": "beta two", "value": 20, "group": "a"},
			{"id": 3, "text": "gamma three", "value": 30, "group": "b"},
		]
	)


def _other_table() -> pd.DataFrame:
	return pd.DataFrame(
		[
			{"id": 1, "label": "x"},
			{"id": 2, "label": "y"},
			{"id": 4, "label": "z"},
		]
	)


@pytest.mark.parametrize(
	("raw", "expected_payload_key"),
	[
		({"op": "filter", "filter": {"expr": "value >= 20"}}, "filter"),
		({"op": "select", "select": {"mode": "include", "columns": ["id", "text"], "strict": True}}, "select"),
		({"op": "rename", "rename": {"map": {"text": "body"}}}, "rename"),
		(
			{
				"op": "derive",
				"derive": {
					"columns": [
						{"name": "value_x2", "expr": "value * 2"},
					]
				},
			},
			"derive",
		),
		(
			{
				"op": "aggregate",
				"aggregate": {
					"groupBy": ["group"],
					"metrics": [{"name": "row_count", "op": "count_rows", "column": None}],
				},
			},
			"aggregate",
		),
		(
			{
				"op": "join",
				"join": {
					"clauses": [
						{
							"leftNodeId": "left",
							"leftCol": "id",
							"rightNodeId": "right",
							"rightCol": "id",
							"how": "inner",
						}
					]
				},
			},
			"join",
		),
		({"op": "sort", "sort": {"by": [{"col": "value", "dir": "desc"}]}}, "sort"),
		({"op": "limit", "limit": {"n": 2}}, "limit"),
		({"op": "dedupe", "dedupe": {"allColumns": False, "by": ["id"], "keep": "first"}}, "dedupe"),
		(
			{
				"op": "split",
				"split": {
					"sourceColumn": "text",
					"outColumn": "part",
					"mode": "lines",
					"lineBreak": "any",
					"trim": True,
					"dropEmpty": True,
					"emitIndex": True,
					"emitSourceRow": True,
					"maxParts": 100,
				},
			},
			"split",
		),
		(
			{
				"op": "quality_gate",
				"quality_gate": {
					"checks": [{"kind": "null_pct", "column": "text", "maxNullPct": 0, "severity": "warn"}],
					"stopOnFail": True,
				},
			},
			"quality_gate",
		),
		({"op": "sql", "sql": {"dialect": "duckdb", "query": "select * from input"}}, "sql"),
		({"op": "json_to_table", "json_to_table": {"orient": "records", "rowsKey": "rows"}}, "json_to_table"),
		(
			{
				"op": "text_to_table",
				"text_to_table": {"mode": "lines", "column": "text", "delimiter": ",", "hasHeader": True},
			},
			"text_to_table",
		),
		({"op": "table_to_json", "table_to_json": {"orient": "records", "pretty": False}}, "table_to_json"),
	],
)
def test_transform_normalization_and_schema_contract_per_op(raw: dict, expected_payload_key: str) -> None:
	norm = normalize_transform_params(raw)
	assert norm["op"] == raw["op"]
	assert isinstance(norm.get(expected_payload_key), dict)
	active_payload_keys = {k for k in OP_KEYS.values() if isinstance(norm.get(k), dict)}
	assert active_payload_keys == {expected_payload_key}

	model = TransformParamsCurrent.model_validate(norm)
	assert model.validate_required() == []


@pytest.mark.parametrize(
	("params", "payload_type"),
	[
		({"op": "filter", "filter": {"expr": "value >= 20"}}, "table"),
		({"op": "select", "select": {"mode": "include", "columns": ["id", "text"], "strict": True}}, "table"),
		({"op": "rename", "rename": {"map": {"text": "body"}}}, "table"),
		({"op": "derive", "derive": {"columns": [{"name": "value_x2", "expr": "value * 2"}]}}, "table"),
		(
			{
				"op": "aggregate",
				"aggregate": {"groupBy": ["group"], "metrics": [{"name": "row_count", "op": "count_rows", "column": None}]},
			},
			"table",
		),
		(
			{
				"op": "join",
				"join": {
					"clauses": [
						{
							"leftNodeId": "left",
							"leftCol": "id",
							"rightNodeId": "right",
							"rightCol": "id",
							"how": "inner",
						}
					]
				},
			},
			"table",
		),
		({"op": "sort", "sort": {"by": [{"col": "value", "dir": "desc"}]}}, "table"),
		({"op": "limit", "limit": {"n": 2}}, "table"),
		({"op": "dedupe", "dedupe": {"allColumns": False, "by": ["id"], "keep": "first"}}, "table"),
		(
			{
				"op": "split",
				"split": {
					"sourceColumn": "text",
					"outColumn": "part",
					"mode": "regex",
					"pattern": r"\s+",
					"lineBreak": "any",
					"trim": True,
					"dropEmpty": True,
					"emitIndex": True,
					"emitSourceRow": True,
					"maxParts": 100,
				},
			},
			"table",
		),
		(
			{
				"op": "quality_gate",
				"quality_gate": {
					"checks": [{"kind": "null_pct", "column": "text", "maxNullPct": 0, "severity": "warn"}],
					"stopOnFail": True,
				},
			},
			"table",
		),
		({"op": "sql", "sql": {"dialect": "duckdb", "query": "select id, text from input"}}, "table"),
		({"op": "json_to_table", "json_to_table": {"orient": "records", "rowsKey": "rows"}}, "table"),
		(
			{
				"op": "text_to_table",
				"text_to_table": {"mode": "lines", "column": "line", "delimiter": ",", "hasHeader": True},
			},
			"table",
		),
		({"op": "table_to_json", "table_to_json": {"orient": "records", "pretty": False}}, "json"),
	],
)
def test_transform_behavior_contract_per_op(params: dict, payload_type: str) -> None:
	norm = normalize_transform_params(params)
	if duckdb is None and norm["op"] in {"filter", "select", "rename", "derive", "join", "sort", "limit", "sql"}:
		pytest.skip("duckdb not installed in test environment")
	input_tables = {"in": _base_table()}
	join_lookup = None
	if norm["op"] == "join":
		join_lookup = {"left": _base_table(), "right": _other_table()}
	elif norm["op"] == "json_to_table":
		input_tables = {"in": pd.DataFrame([{"rows": [{"id": 1}, {"id": 2}]}])}
	elif norm["op"] == "text_to_table":
		input_tables = {"in": pd.DataFrame([{"text": "a\nb\nc"}])}

	res = run_transform(params=norm, input_tables=input_tables, join_lookup=join_lookup)
	assert res.meta.get("payloadType") == payload_type
	exec_meta = res.meta.get("execution")
	assert isinstance(exec_meta, dict)
	assert isinstance(exec_meta.get("input"), dict)
	assert isinstance(exec_meta.get("output"), dict)
	assert isinstance(exec_meta.get("drift"), dict)
	assert isinstance(exec_meta.get("cost"), dict)
	assert isinstance(exec_meta.get("schemaChecks"), dict)
	assert isinstance(exec_meta.get("determinism"), dict)
	assert isinstance(exec_meta.get("planner"), dict)
	assert isinstance(exec_meta.get("timeline"), list)
	assert isinstance(exec_meta.get("rowDiagnostics"), dict)
	assert isinstance(exec_meta["input"].get("rows"), int)
	assert isinstance(exec_meta["output"].get("rows"), int)
	assert exec_meta["schemaChecks"].get("mandatory") is True
	if payload_type == "json":
		json.loads(res.payload_bytes.decode("utf-8"))
