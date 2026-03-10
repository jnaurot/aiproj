from __future__ import annotations

import json

import pandas as pd

from app.runner.nodes.transform import execute_transform_op, normalize_transform_params, run_transform


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
	assert res.meta.get("port_type") == "json"
	parsed = json.loads(res.payload_bytes.decode("utf-8"))
	assert parsed == [{"id": 1, "value": "x"}, {"id": 2, "value": "y"}]
