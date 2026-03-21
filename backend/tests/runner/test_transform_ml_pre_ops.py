from __future__ import annotations

import io
import pandas as pd
import pytest

from app.runner.nodes.transform import normalize_transform_params, run_transform


def test_null_policy_fill_stat_fills_missing_values() -> None:
	df = pd.DataFrame([{"x": 1.0}, {"x": None}, {"x": 3.0}])
	params = normalize_transform_params(
		{
			"op": "null_policy",
			"null_policy": {"mode": "fill_stat", "columns": ["x"], "stat": "mean"},
		}
	)
	res = run_transform(params=params, input_tables={"in": df}, join_lookup=None)
	out = pd.read_csv(io.BytesIO(res.payload_bytes))
	assert out["x"].isna().sum() == 0
	assert float(out["x"].iloc[1]) == 2.0
	meta = res.meta.get("null_policy") or {}
	assert int(meta.get("filledCountByColumn", {}).get("x", 0)) == 1


def test_null_policy_drop_rows_removes_null_rows() -> None:
	df = pd.DataFrame([{"x": 1.0}, {"x": None}, {"x": 3.0}])
	params = normalize_transform_params(
		{
			"op": "null_policy",
			"null_policy": {"mode": "drop_rows", "columns": ["x"]},
		}
	)
	res = run_transform(params=params, input_tables={"in": df}, join_lookup=None)
	out = pd.read_csv(io.BytesIO(res.payload_bytes))
	assert len(out) == 2
	meta = res.meta.get("null_policy") or {}
	assert int(meta.get("droppedRows", 0)) == 1


def test_outlier_policy_clip_caps_extreme_values() -> None:
	df = pd.DataFrame([{"x": 10.0}, {"x": 11.0}, {"x": 1000.0}])
	params = normalize_transform_params(
		{
			"op": "outlier_policy",
			"outlier_policy": {"mode": "clip", "method": "quantile", "columns": ["x"], "lowerQuantile": 0.0, "upperQuantile": 0.5},
		}
	)
	res = run_transform(params=params, input_tables={"in": df}, join_lookup=None)
	out = pd.read_csv(io.BytesIO(res.payload_bytes))
	assert float(out["x"].max()) <= 11.0
	meta = res.meta.get("outlier_policy") or {}
	assert isinstance(meta.get("perColumn"), dict)


def test_outlier_policy_drop_removes_outlier_rows() -> None:
	df = pd.DataFrame([{"x": 10.0}, {"x": 11.0}, {"x": 1000.0}])
	params = normalize_transform_params(
		{
			"op": "outlier_policy",
			"outlier_policy": {"mode": "drop", "method": "quantile", "columns": ["x"], "lowerQuantile": 0.0, "upperQuantile": 0.5},
		}
	)
	res = run_transform(params=params, input_tables={"in": df}, join_lookup=None)
	out = pd.read_csv(io.BytesIO(res.payload_bytes))
	assert len(out) < len(df)
	meta = res.meta.get("outlier_policy") or {}
	assert int(meta.get("droppedRows", 0)) == (len(df) - len(out))


def test_text_clean_normalizes_noise() -> None:
	df = pd.DataFrame([{"text": "HELLO   https://example.com  Foo@Bar.com"}])
	params = normalize_transform_params(
		{
			"op": "text_clean",
			"text_clean": {
				"columns": ["text"],
				"lowercase": True,
				"removeUrls": True,
				"removeEmails": True,
				"normalizeWhitespace": True,
			},
		}
	)
	res = run_transform(params=params, input_tables={"in": df}, join_lookup=None)
	out = pd.read_csv(io.BytesIO(res.payload_bytes))
	assert out["text"].iloc[0] == "hello"
	meta = res.meta.get("text_clean") or {}
	assert int(meta.get("changedRowsByColumn", {}).get("text", 0)) == 1


def test_nlp_normalize_removes_stopwords() -> None:
	df = pd.DataFrame([{"text": "the cat and the dog"}])
	params = normalize_transform_params(
		{
			"op": "nlp_normalize",
			"nlp_normalize": {
				"columns": ["text"],
				"language": "en",
				"removeStopwords": True,
				"stemmer": "none",
				"lemmatizer": "none",
			},
		}
	)
	res = run_transform(params=params, input_tables={"in": df}, join_lookup=None)
	out = pd.read_csv(io.BytesIO(res.payload_bytes))
	assert out["text"].iloc[0] == "cat dog"
	meta = res.meta.get("nlp_normalize") or {}
	assert int(meta.get("removedStopwordsByColumn", {}).get("text", 0)) >= 2


def test_nlp_normalize_unsupported_language_fails() -> None:
	df = pd.DataFrame([{"text": "hola mundo"}])
	params = normalize_transform_params(
		{
			"op": "nlp_normalize",
			"nlp_normalize": {"columns": ["text"], "language": "es"},
		}
	)
	with pytest.raises(ValueError, match="not supported"):
		run_transform(params=params, input_tables={"in": df}, join_lookup=None)


def test_tokenize_chunk_emits_chunk_stats_without_truncation() -> None:
	df = pd.DataFrame([{"text": "one two three four five six"}])
	params = normalize_transform_params(
		{
			"op": "tokenize_chunk",
			"tokenize_chunk": {
				"columns": ["text"],
				"tokenizer": "whitespace",
				"maxTokens": 4,
				"overlap": 1,
				"outColumn": "chunk",
			},
		}
	)
	res = run_transform(params=params, input_tables={"in": df}, join_lookup=None)
	out = pd.read_csv(io.BytesIO(res.payload_bytes))
	assert "chunk" in out.columns
	assert int(len(out)) >= 2
	meta = res.meta.get("tokenize_chunk") or {}
	stats = meta.get("chunkStats") or {}
	assert int(stats.get("numChunks", 0)) == len(out)
	assert int(stats.get("droppedTokens", 0)) == 0


def test_dataset_split_adds_split_column_and_is_seeded() -> None:
	df = pd.DataFrame([{"id": i, "label": "a" if i % 2 == 0 else "b"} for i in range(20)])
	params = normalize_transform_params(
		{
			"op": "dataset_split",
			"dataset_split": {
				"strategy": "random",
				"trainRatio": 0.7,
				"valRatio": 0.2,
				"testRatio": 0.1,
				"seed": 123,
				"shuffle": True,
			},
		}
	)
	res1 = run_transform(params=params, input_tables={"in": df}, join_lookup=None)
	res2 = run_transform(params=params, input_tables={"in": df}, join_lookup=None)
	out1 = pd.read_csv(io.BytesIO(res1.payload_bytes))
	out2 = pd.read_csv(io.BytesIO(res2.payload_bytes))
	assert out1["split"].tolist() == out2["split"].tolist()
	assert set(out1["split"].unique().tolist()).issubset({"train", "val", "test"})
	meta = res1.meta.get("dataset_split") or {}
	assert isinstance(meta.get("counts"), dict)


def test_class_imbalance_report_emits_before_after() -> None:
	df = pd.DataFrame([{"label": "major"}] * 8 + [{"label": "minor"}] * 2)
	params = normalize_transform_params(
		{
			"op": "class_imbalance",
			"class_imbalance": {
				"strategy": "report",
				"labelColumn": "label",
				"targetRatio": 1.0,
				"seed": 7,
			},
		}
	)
	res = run_transform(params=params, input_tables={"in": df}, join_lookup=None)
	out = pd.read_csv(io.BytesIO(res.payload_bytes))
	assert len(out) == len(df)
	meta = res.meta.get("class_imbalance") or {}
	assert isinstance(meta.get("before"), dict)
	assert isinstance(meta.get("after"), dict)


def test_categorical_encode_one_hot_emits_new_columns() -> None:
	df = pd.DataFrame([{"cat": "a"}, {"cat": "b"}, {"cat": "a"}])
	params = normalize_transform_params(
		{
			"op": "categorical_encode",
			"categorical_encode": {"columns": ["cat"], "encoding": "one_hot", "unknownPolicy": "ignore"},
		}
	)
	res = run_transform(params=params, input_tables={"in": df}, join_lookup=None)
	out = pd.read_csv(io.BytesIO(res.payload_bytes))
	assert any(c.startswith("cat_") for c in out.columns)
	meta = res.meta.get("categorical_encode") or {}
	assert meta.get("encoding") == "one_hot"


def test_numeric_scale_standard_scales_column() -> None:
	df = pd.DataFrame([{"x": 1.0}, {"x": 2.0}, {"x": 3.0}])
	params = normalize_transform_params(
		{
			"op": "numeric_scale",
			"numeric_scale": {"columns": ["x"], "method": "standard"},
		}
	)
	res = run_transform(params=params, input_tables={"in": df}, join_lookup=None)
	out = pd.read_csv(io.BytesIO(res.payload_bytes))
	assert abs(float(out["x"].mean())) < 1e-6
	meta = res.meta.get("numeric_scale") or {}
	assert meta.get("method") == "standard"


def test_embedding_generates_vector_column() -> None:
	df = pd.DataFrame([{"text": "hello world"}])
	params = normalize_transform_params(
		{
			"op": "embedding",
			"embedding": {
				"columns": ["text"],
				"provider": "local_hash",
				"model": "text-embedding-3-small",
				"dimensions": 8,
				"outputColumn": "embedding",
			},
		}
	)
	res = run_transform(params=params, input_tables={"in": df}, join_lookup=None)
	out = pd.read_csv(io.BytesIO(res.payload_bytes))
	assert "embedding" in out.columns
	meta = res.meta.get("embedding") or {}
	assert int(meta.get("dimensions", 0)) == 8


def test_feature_selection_manual_keeps_selected_columns() -> None:
	df = pd.DataFrame([{"x": 1.0, "y": 2.0, "z": 3.0}])
	params = normalize_transform_params(
		{
			"op": "feature_selection",
			"feature_selection": {
				"method": "manual",
				"selectedColumns": ["x", "z"],
			},
		}
	)
	res = run_transform(params=params, input_tables={"in": df}, join_lookup=None)
	out = pd.read_csv(io.BytesIO(res.payload_bytes))
	assert list(out.columns) == ["x", "z"]
	meta = res.meta.get("feature_selection") or {}
	assert meta.get("method") == "manual"


def test_leakage_detect_reports_overlap() -> None:
	df = pd.DataFrame(
		[
			{"id": 1, "split": "train"},
			{"id": 2, "split": "train"},
			{"id": 1, "split": "test"},
		]
	)
	params = normalize_transform_params(
		{
			"op": "leakage_detect",
			"leakage_detect": {"splitColumn": "split", "keyColumns": ["id"], "maxAllowedOverlap": 0.0},
		}
	)
	with pytest.raises(ValueError, match="leakage_detect failed"):
		run_transform(params=params, input_tables={"in": df}, join_lookup=None)


def test_pii_guard_masks_email() -> None:
	df = pd.DataFrame([{"text": "contact me at foo@example.com"}])
	params = normalize_transform_params(
		{
			"op": "pii_guard",
			"pii_guard": {"columns": ["text"], "action": "mask", "failOnDetect": False},
		}
	)
	res = run_transform(params=params, input_tables={"in": df}, join_lookup=None)
	out = pd.read_csv(io.BytesIO(res.payload_bytes))
	assert "[EMAIL]" in str(out["text"].iloc[0])
	meta = res.meta.get("pii_guard") or {}
	assert int(meta.get("detectedRows", 0)) == 1


def test_inference_parity_fails_on_mismatch() -> None:
	df = pd.DataFrame([{"x": 1}])
	params = normalize_transform_params(
		{
			"op": "inference_parity",
			"inference_parity": {
				"trainSignature": "abc",
				"inferenceSignature": "xyz",
				"failOnMismatch": True,
			},
		}
	)
	with pytest.raises(ValueError, match="inference_parity failed"):
		run_transform(params=params, input_tables={"in": df}, join_lookup=None)
