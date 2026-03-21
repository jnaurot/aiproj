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
				"op": "null_policy",
				"null_policy": {"mode": "fill_stat", "columns": ["value"], "stat": "mean", "rules": []},
			},
			"null_policy",
		),
		(
			{
				"op": "outlier_policy",
				"outlier_policy": {"mode": "clip", "method": "iqr", "columns": ["value"]},
			},
			"outlier_policy",
		),
		(
			{
				"op": "text_clean",
				"text_clean": {"columns": ["text"], "lowercase": True, "unicodeNormalize": "nfkc"},
			},
			"text_clean",
		),
		(
			{
				"op": "nlp_normalize",
				"nlp_normalize": {
					"columns": ["text"],
					"language": "en",
					"removeStopwords": True,
					"stemmer": "none",
					"lemmatizer": "none",
				},
			},
			"nlp_normalize",
		),
		(
			{
				"op": "tokenize_chunk",
				"tokenize_chunk": {"columns": ["text"], "tokenizer": "whitespace", "maxTokens": 4, "overlap": 1},
			},
			"tokenize_chunk",
		),
		(
			{
				"op": "dataset_split",
				"dataset_split": {"strategy": "random", "trainRatio": 0.8, "valRatio": 0.1, "testRatio": 0.1, "seed": 42},
			},
			"dataset_split",
		),
		(
			{
				"op": "class_imbalance",
				"class_imbalance": {"strategy": "report", "labelColumn": "group", "targetRatio": 1, "seed": 42},
			},
			"class_imbalance",
		),
		(
			{
				"op": "categorical_encode",
				"categorical_encode": {"columns": ["group"], "encoding": "one_hot", "unknownPolicy": "ignore"},
			},
			"categorical_encode",
		),
		(
			{
				"op": "numeric_scale",
				"numeric_scale": {"columns": ["value"], "method": "standard"},
			},
			"numeric_scale",
		),
		(
			{
				"op": "embedding",
				"embedding": {"columns": ["text"], "provider": "local_hash", "model": "text-embedding-3-small", "dimensions": 8},
			},
			"embedding",
		),
		(
			{
				"op": "feature_selection",
				"feature_selection": {"method": "manual", "selectedColumns": ["id", "value"]},
			},
			"feature_selection",
		),
		(
			{
				"op": "leakage_detect",
				"leakage_detect": {"splitColumn": "split", "keyColumns": ["id"], "labelColumn": "group", "maxAllowedOverlap": 1.0},
			},
			"leakage_detect",
		),
		(
			{
				"op": "quality_profile",
				"quality_profile": {"columns": ["text"], "includeHistograms": True, "includeSamples": True},
			},
			"quality_profile",
		),
		(
			{
				"op": "drift_compare",
				"drift_compare": {"compareColumns": ["group"], "metric": "psi", "threshold": 0.2, "failOnDrift": False},
			},
			"drift_compare",
		),
		(
			{
				"op": "determinism_profile",
				"determinism_profile": {"strict": True, "seed": 42, "stableSort": True, "stableCoercion": True},
			},
			"determinism_profile",
		),
		(
			{
				"op": "fit_state_registry",
				"fit_state_registry": {"mode": "fit", "stateKey": "default", "includeColumns": ["id", "value"]},
			},
			"fit_state_registry",
		),
		(
			{
				"op": "pii_guard",
				"pii_guard": {"columns": ["text"], "action": "report", "failOnDetect": False},
			},
			"pii_guard",
		),
		(
			{
				"op": "inference_parity",
				"inference_parity": {"trainSignature": "a", "inferenceSignature": "a", "failOnMismatch": True},
			},
			"inference_parity",
		),
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
		(
			{
				"op": "ml_contract",
				"ml_contract": {
					"taskType": "classification",
					"labelColumn": "group",
					"featureColumns": ["text", "value"],
					"idColumn": "id",
					"timestampColumn": "",
					"allowExtraFeatures": True,
					"requireNonNullLabel": True,
				},
			},
			"ml_contract",
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
				"op": "null_policy",
				"null_policy": {"mode": "fill_stat", "columns": ["value"], "stat": "mean", "rules": []},
			},
			"table",
		),
		(
			{
				"op": "outlier_policy",
				"outlier_policy": {"mode": "clip", "method": "iqr", "columns": ["value"]},
			},
			"table",
		),
		(
			{
				"op": "text_clean",
				"text_clean": {"columns": ["text"], "lowercase": True, "unicodeNormalize": "nfkc"},
			},
			"table",
		),
		(
			{
				"op": "nlp_normalize",
				"nlp_normalize": {
					"columns": ["text"],
					"language": "en",
					"removeStopwords": True,
					"stemmer": "none",
					"lemmatizer": "none",
				},
			},
			"table",
		),
		(
			{
				"op": "tokenize_chunk",
				"tokenize_chunk": {"columns": ["text"], "tokenizer": "whitespace", "maxTokens": 4, "overlap": 1},
			},
			"table",
		),
		(
			{
				"op": "dataset_split",
				"dataset_split": {"strategy": "random", "trainRatio": 0.8, "valRatio": 0.1, "testRatio": 0.1, "seed": 42},
			},
			"table",
		),
		(
			{
				"op": "class_imbalance",
				"class_imbalance": {"strategy": "report", "labelColumn": "group", "targetRatio": 1, "seed": 42},
			},
			"table",
		),
		(
			{
				"op": "categorical_encode",
				"categorical_encode": {"columns": ["group"], "encoding": "one_hot", "unknownPolicy": "ignore"},
			},
			"table",
		),
		(
			{
				"op": "numeric_scale",
				"numeric_scale": {"columns": ["value"], "method": "standard"},
			},
			"table",
		),
		(
			{
				"op": "embedding",
				"embedding": {"columns": ["text"], "provider": "local_hash", "model": "text-embedding-3-small", "dimensions": 8},
			},
			"table",
		),
		(
			{
				"op": "feature_selection",
				"feature_selection": {"method": "manual", "selectedColumns": ["id", "value"]},
			},
			"table",
		),
		(
			{
				"op": "leakage_detect",
				"leakage_detect": {"splitColumn": "split", "keyColumns": ["id"], "labelColumn": "group", "maxAllowedOverlap": 1.0},
			},
			"table",
		),
		(
			{
				"op": "quality_profile",
				"quality_profile": {"columns": ["text"], "includeHistograms": True, "includeSamples": True},
			},
			"table",
		),
		(
			{
				"op": "drift_compare",
				"drift_compare": {"compareColumns": ["group"], "metric": "psi", "threshold": 0.2, "failOnDrift": False},
			},
			"table",
		),
		(
			{
				"op": "determinism_profile",
				"determinism_profile": {"strict": True, "seed": 42, "stableSort": True, "stableCoercion": True},
			},
			"table",
		),
		(
			{
				"op": "fit_state_registry",
				"fit_state_registry": {"mode": "fit", "stateKey": "default", "includeColumns": ["id", "value"]},
			},
			"table",
		),
		(
			{
				"op": "pii_guard",
				"pii_guard": {"columns": ["text"], "action": "report", "failOnDetect": False},
			},
			"table",
		),
		(
			{
				"op": "inference_parity",
				"inference_parity": {"trainSignature": "a", "inferenceSignature": "a", "failOnMismatch": True},
			},
			"table",
		),
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
		(
			{
				"op": "ml_contract",
				"ml_contract": {
					"taskType": "classification",
					"labelColumn": "group",
					"featureColumns": ["text", "value"],
					"idColumn": "id",
					"timestampColumn": "",
					"allowExtraFeatures": True,
					"requireNonNullLabel": True,
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
	elif norm["op"] == "null_policy":
		input_tables = {"in": pd.DataFrame([{"id": 1, "value": 10.0}, {"id": 2, "value": None}])}
	elif norm["op"] == "outlier_policy":
		input_tables = {"in": pd.DataFrame([{"id": 1, "value": 10.0}, {"id": 2, "value": 1_000_000.0}])}
	elif norm["op"] == "text_clean":
		input_tables = {"in": pd.DataFrame([{"id": 1, "text": "HELLO   https://x.com"}])}
	elif norm["op"] == "nlp_normalize":
		input_tables = {"in": pd.DataFrame([{"id": 1, "text": "The runners are running"}])}
	elif norm["op"] == "tokenize_chunk":
		input_tables = {"in": pd.DataFrame([{"id": 1, "text": "one two three four five six"}])}
	elif norm["op"] == "dataset_split":
		input_tables = {"in": pd.DataFrame([{"id": i, "group": "a" if i % 2 == 0 else "b"} for i in range(12)])}
	elif norm["op"] == "class_imbalance":
		input_tables = {"in": pd.DataFrame([{"group": "a"}] * 8 + [{"group": "b"}] * 4)}
	elif norm["op"] == "categorical_encode":
		input_tables = {"in": pd.DataFrame([{"group": "a"}, {"group": "b"}])}
	elif norm["op"] == "numeric_scale":
		input_tables = {"in": pd.DataFrame([{"value": 1.0}, {"value": 2.0}, {"value": 3.0}])}
	elif norm["op"] == "embedding":
		input_tables = {"in": pd.DataFrame([{"text": "hello world"}])}
	elif norm["op"] == "feature_selection":
		input_tables = {"in": pd.DataFrame([{"id": 1, "value": 2.0, "group": "a"}])}
	elif norm["op"] == "leakage_detect":
		input_tables = {"in": pd.DataFrame([{"id": 1, "group": "a", "split": "train"}, {"id": 2, "group": "b", "split": "test"}])}
	elif norm["op"] == "quality_profile":
		input_tables = {"in": pd.DataFrame([{"text": "hello", "group": "a"}])}
	elif norm["op"] == "drift_compare":
		input_tables = {"in": pd.DataFrame([{"group": "a", "split": "train"}, {"group": "b", "split": "test"}])}
	elif norm["op"] == "determinism_profile":
		input_tables = {"in": pd.DataFrame([{"id": 2}, {"id": 1}])}
	elif norm["op"] == "fit_state_registry":
		input_tables = {"in": pd.DataFrame([{"id": 1, "value": 2.0}])}
	elif norm["op"] == "pii_guard":
		input_tables = {"in": pd.DataFrame([{"text": "foo@example.com"}])}
	elif norm["op"] == "inference_parity":
		input_tables = {"in": pd.DataFrame([{"id": 1}])}

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
	if norm["op"] == "ml_contract":
		ml_meta = res.meta.get("ml_contract")
		assert isinstance(ml_meta, dict)
		assert ml_meta.get("labelColumn") == "group"
	if norm["op"] == "null_policy":
		null_meta = res.meta.get("null_policy")
		assert isinstance(null_meta, dict)
		assert isinstance(null_meta.get("beforeNullCountByColumn"), dict)
	if norm["op"] == "outlier_policy":
		outlier_meta = res.meta.get("outlier_policy")
		assert isinstance(outlier_meta, dict)
		assert isinstance(outlier_meta.get("perColumn"), dict)
	if norm["op"] == "text_clean":
		clean_meta = res.meta.get("text_clean")
		assert isinstance(clean_meta, dict)
		assert isinstance(clean_meta.get("changedRowsByColumn"), dict)
	if norm["op"] == "nlp_normalize":
		nlp_meta = res.meta.get("nlp_normalize")
		assert isinstance(nlp_meta, dict)
		assert nlp_meta.get("language") == "en"
	if norm["op"] == "tokenize_chunk":
		chunk_meta = res.meta.get("tokenize_chunk")
		assert isinstance(chunk_meta, dict)
		assert isinstance((chunk_meta.get("chunkStats") or {}).get("tokenHistogram"), dict)
	if payload_type == "json":
		json.loads(res.payload_bytes.decode("utf-8"))
