from __future__ import annotations

import pandas as pd
import pytest

from app.runner.nodes.transform import execute_transform_op, normalize_transform_params, run_transform


def _quality_gate_params(checks: list[dict], *, stop_on_fail: bool = True) -> dict:
	return normalize_transform_params(
		{
			"op": "quality_gate",
			"quality_gate": {
				"checks": checks,
				"stopOnFail": stop_on_fail,
			},
		}
	)


def test_quality_gate_null_pct_fail_raises():
	df = pd.DataFrame([{"text": "a"}, {"text": None}, {"text": None}])
	params = _quality_gate_params(
		[
			{
				"kind": "null_pct",
				"column": "text",
				"maxNullPct": 0.2,
				"severity": "fail",
			}
		]
	)
	with pytest.raises(ValueError, match="quality_gate failed"):
		execute_transform_op("quality_gate", params, {"in": df})


def test_quality_gate_warn_only_passes_and_emits_meta():
	df = pd.DataFrame([{"text": "a"}, {"text": None}, {"text": None}])
	params = _quality_gate_params(
		[
			{
				"kind": "null_pct",
				"column": "text",
				"maxNullPct": 0.2,
				"severity": "warn",
			}
		]
	)
	out = run_transform(params=params, input_tables={"in": df}, join_lookup=None)
	assert out.meta.get("row_count") == 3
	assert out.meta.get("columns") == ["text"]
	report = out.meta.get("quality_gate") or {}
	assert report.get("failed") is False
	assert report.get("checksEvaluated") == 1
	assert len(report.get("warnViolations") or []) == 1
	assert len(report.get("failViolations") or []) == 0


def test_quality_gate_leakage_fail_detects_high_correlation():
	df = pd.DataFrame(
		[
			{"feature": 0, "target": 0},
			{"feature": 1, "target": 1},
			{"feature": 2, "target": 2},
			{"feature": 3, "target": 3},
		]
	)
	params = _quality_gate_params(
		[
			{
				"kind": "leakage",
				"featureColumn": "feature",
				"targetColumn": "target",
				"maxAbsCorrelation": 0.8,
				"severity": "fail",
			}
		]
	)
	with pytest.raises(ValueError, match="quality_gate failed"):
		execute_transform_op("quality_gate", params, {"in": df})


def test_quality_gate_missing_column_fails():
	df = pd.DataFrame([{"text": "a"}])
	params = _quality_gate_params(
		[
			{
				"kind": "uniqueness",
				"column": "id",
				"minUniqueRatio": 1.0,
				"severity": "fail",
			}
		]
	)
	with pytest.raises(ValueError, match="quality_gate failed"):
		execute_transform_op("quality_gate", params, {"in": df})
