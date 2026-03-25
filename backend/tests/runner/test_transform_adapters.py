from __future__ import annotations

import json

import pandas as pd

from app.runner.nodes.transform import (
	execute_transform_op,
	load_table_from_json_bytes,
	load_table_from_text_bytes,
	normalize_transform_params,
	run_transform,
)


def test_normalize_transform_adapter_defaults():
	params = normalize_transform_params({"op": "json_to_table", "json_to_table": {}})
	assert params["json_to_table"]["orient"] == "records"
	assert params["json_to_table"]["rowsKey"] == "rows"

	params = normalize_transform_params({"op": "text_to_table", "text_to_table": {}})
	assert params["text_to_table"]["mode"] == "lines"
	assert params["text_to_table"]["column"] == "text"
	assert params["text_to_table"]["hasHeader"] is True

	params = normalize_transform_params({"op": "table_to_json", "table_to_json": {}})
	assert params["table_to_json"]["orient"] == "records"
	assert params["table_to_json"]["pretty"] is False


def test_text_to_table_lines_renames_default_text_column():
	df = pd.DataFrame({"text": ["a", "b", "c"]})
	params = normalize_transform_params(
		{
			"op": "text_to_table",
			"text_to_table": {"mode": "lines", "column": "line"},
		}
	)
	out = execute_transform_op("text_to_table", params, {"in": df})
	assert list(out.columns) == ["line"]
	assert out.to_dict(orient="records") == [{"line": "a"}, {"line": "b"}, {"line": "c"}]


def test_table_to_json_emits_json_artifact():
	df = pd.DataFrame([{"id": 1, "value": "x"}, {"id": 2, "value": "y"}])
	params = normalize_transform_params(
		{
			"op": "table_to_json",
			"table_to_json": {"orient": "records", "pretty": False},
		}
	)

	res = run_transform(params=params, input_tables={"in": df}, join_lookup=None)
	assert res.mime_type.startswith("application/json")
	assert res.meta.get("payloadType") == "json"
	assert res.additional_outputs == {}
	parsed = json.loads(res.payload_bytes.decode("utf-8"))
	assert parsed == [{"id": 1, "value": "x"}, {"id": 2, "value": "y"}]


def test_json_to_table_records_adapter_parses_scalar_arrays():
	payload = b'["a","b","c"]'
	out = load_table_from_json_bytes(payload, orient="records", rows_key="rows")
	assert list(out.columns) == ["value"]
	assert out.to_dict(orient="records") == [{"value": "a"}, {"value": "b"}, {"value": "c"}]


def test_json_to_table_object_adapter_respects_rows_key():
	payload = b'{"rows":[{"id":1,"name":"x"},{"id":2,"name":"y"}]}'
	out = load_table_from_json_bytes(payload, orient="object", rows_key="rows")
	assert list(out.columns) == ["id", "name"]
	assert out.to_dict(orient="records") == [{"id": 1, "name": "x"}, {"id": 2, "name": "y"}]


def test_text_to_table_csv_adapter_honors_header_and_delimiter():
	payload = b"col_a;col_b\n1;2\n3;4\n"
	out = load_table_from_text_bytes(
		payload,
		mode="csv",
		column="text",
		delimiter=";",
		has_header=True,
	)
	assert list(out.columns) == ["col_a", "col_b"]
	assert out.to_dict(orient="records") == [{"col_a": 1, "col_b": 2}, {"col_a": 3, "col_b": 4}]


def test_text_to_table_tsv_adapter_no_header_generates_named_columns():
	payload = b"a\tb\nc\td\n"
	out = load_table_from_text_bytes(
		payload,
		mode="tsv",
		column="item",
		delimiter=",",
		has_header=False,
	)
	assert list(out.columns) == ["item", "item_1"]
	assert out.to_dict(orient="records") == [{"item": "a", "item_1": "b"}, {"item": "c", "item_1": "d"}]

