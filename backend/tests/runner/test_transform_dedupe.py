from __future__ import annotations

import pandas as pd
import pytest

from app.runner.nodes.transform import execute_transform_op, normalize_transform_params


def _dedupe_params(**overrides):
	base = {
		"op": "dedupe",
		"dedupe": {
			"by": [],
			"keep": "first",
		},
	}
	base["dedupe"].update(overrides)
	return normalize_transform_params(base)


def test_dedupe_by_column():
	df = pd.DataFrame(
		[
			{"text": "a", "other": 1},
			{"text": "a", "other": 2},
			{"text": "b", "other": 3},
		]
	)
	out = execute_transform_op("dedupe", _dedupe_params(by=["text"]), {"in": df})
	assert out.to_dict(orient="records") == [
		{"text": "a", "other": 1},
		{"text": "b", "other": 3},
	]


def test_dedupe_all_columns_when_by_empty():
	df = pd.DataFrame(
		[
			{"text": "a", "other": 1},
			{"text": "a", "other": 1},
			{"text": "a", "other": 2},
		]
	)
	out = execute_transform_op("dedupe", _dedupe_params(by=[]), {"in": df})
	assert out.to_dict(orient="records") == [
		{"text": "a", "other": 1},
		{"text": "a", "other": 2},
	]


def test_dedupe_keep_first_is_deterministic():
	df = pd.DataFrame(
		[
			{"text": "x", "rank": 2},
			{"text": "x", "rank": 1},
			{"text": "x", "rank": 3},
		]
	)
	params = _dedupe_params(by=["text"])
	out1 = execute_transform_op("dedupe", params, {"in": df}).to_dict(orient="records")
	out2 = execute_transform_op("dedupe", params, {"in": df}).to_dict(orient="records")
	assert out1 == out2 == [{"text": "x", "rank": 2}]


def test_dedupe_missing_by_column_fails():
	df = pd.DataFrame([{"text": "a"}])
	with pytest.raises(ValueError, match="dedupe.by columns missing from input"):
		execute_transform_op("dedupe", _dedupe_params(by=["nope"]), {"in": df})


def test_dedupe_legacy_all_columns_payload_is_compatible():
	df = pd.DataFrame([{"text": "a"}, {"text": "a"}])
	params = normalize_transform_params({"op": "dedupe", "dedupe": {"allColumns": True}})
	out = execute_transform_op("dedupe", params, {"in": df}).to_dict(orient="records")
	assert out == [{"text": "a"}]


def test_dedupe_requires_by_when_all_columns_false():
	df = pd.DataFrame([{"text": "a"}, {"text": "a"}])
	params = normalize_transform_params({"op": "dedupe", "dedupe": {"allColumns": False, "by": []}})
	with pytest.raises(ValueError, match="allColumns=false"):
		execute_transform_op("dedupe", params, {"in": df})
