from __future__ import annotations

import json

from app.runner.nodes.transform import normalize_transform_params, run_transform


def _json_filter_params(rules: dict, **kwargs):
	params = {
		"op": "json_filter",
		"json_filter": {
			"mode": "rules",
			"rules": rules,
			"route_reject": True,
			"include_reject_meta": True,
		},
	}
	params["json_filter"].update(kwargs)
	return normalize_transform_params(params)


def test_json_filter_routes_match_to_out() -> None:
	params = _json_filter_params(
		{
			"kind": "group",
			"op": "all",
			"conditions": [
				{"kind": "condition", "path": "pass", "op": "eq", "value": True},
				{"kind": "condition", "path": "score", "op": "gte", "value": 70},
			],
		}
	)
	res = run_transform(
		params=params,
		input_tables={"in": None},
		join_lookup=None,
		param_inputs={"_json_filter_in": {"job_id": "1", "pass": True, "score": 85}},
	)
	payload = json.loads(res.payload_bytes.decode("utf-8"))
	assert payload["job_id"] == "1"
	assert payload["score"] == 85
	assert "out_reject" in res.additional_outputs
	reject_payload = json.loads(res.additional_outputs["out_reject"].payload_bytes.decode("utf-8"))
	assert reject_payload == []


def test_json_filter_routes_non_match_to_out_reject_with_reason() -> None:
	params = _json_filter_params(
		{
			"kind": "group",
			"op": "all",
			"conditions": [{"kind": "condition", "path": "score", "op": "gte", "value": 70}],
		}
	)
	res = run_transform(
		params=params,
		input_tables={"in": None},
		join_lookup=None,
		param_inputs={"_json_filter_in": {"job_id": "1", "score": 45}},
	)
	pass_payload = json.loads(res.payload_bytes.decode("utf-8"))
	assert pass_payload == []
	reject_payload = json.loads(res.additional_outputs["out_reject"].payload_bytes.decode("utf-8"))
	assert isinstance(reject_payload, list)
	assert reject_payload[0]["job_id"] == "1"
	assert reject_payload[0]["_reject"]["reason"] in {"rejected", "type_mismatch", "missing_path", "no_match"}


def test_json_filter_missing_path_is_rejected_with_missing_path_reason() -> None:
	params = _json_filter_params(
		{
			"kind": "group",
			"op": "all",
			"conditions": [{"kind": "condition", "path": "job.salary", "op": "gte", "value": 70000}],
		}
	)
	res = run_transform(
		params=params,
		input_tables={"in": None},
		join_lookup=None,
		param_inputs={"_json_filter_in": {"job_id": "1", "score": 90}},
	)
	reject_payload = json.loads(res.additional_outputs["out_reject"].payload_bytes.decode("utf-8"))
	assert reject_payload[0]["_reject"]["reason"] == "missing_path"
	assert reject_payload[0]["_reject"]["path"] == "job.salary"


def test_json_filter_resolves_param_config_value_from_path() -> None:
	params = _json_filter_params(
		{
			"kind": "group",
			"op": "all",
			"conditions": [
				{
					"kind": "condition",
					"path": "candidate_required_location",
					"op": "in",
					"value": {"valueFrom": {"handle": "param_config", "path": "location"}},
				}
			],
		}
	)
	res = run_transform(
		params=params,
		input_tables={"in": None},
		join_lookup=None,
		param_inputs={
			"_json_filter_in": {"job_id": "1", "candidate_required_location": "USA"},
			"param_config": {"location": ["USA", "Remote"]},
		},
	)
	payload = json.loads(res.payload_bytes.decode("utf-8"))
	assert payload["job_id"] == "1"


def test_json_filter_between_operator_accepts_range() -> None:
	params = _json_filter_params(
		{
			"kind": "group",
			"op": "all",
			"conditions": [{"kind": "condition", "path": "score", "op": "between", "value": [70, 100]}],
		}
	)
	res = run_transform(
		params=params,
		input_tables={"in": None},
		join_lookup=None,
		param_inputs={"_json_filter_in": {"job_id": "1", "score": 88}},
	)
	payload = json.loads(res.payload_bytes.decode("utf-8"))
	assert payload["score"] == 88
