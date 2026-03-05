from __future__ import annotations

import pandas as pd
import pytest

from app.runner.nodes.transform import execute_transform_op


def _split_params(**overrides):
	base = {
		"op": "split",
		"split": {
			"sourceColumn": "text",
			"outColumn": "part",
			"mode": "sentences",
			"pattern": r"(?<=[.!?])\s+",
			"delimiter": "\n",
			"flags": "",
			"trim": True,
			"dropEmpty": True,
			"emitIndex": True,
			"emitSourceRow": True,
			"maxParts": 5000,
		},
	}
	base["split"].update(overrides)
	return base


def _input_df(text: str) -> pd.DataFrame:
	return pd.DataFrame([{"text": text}])


def test_split_sentences_deterministic():
	df = _input_df("Hi there.  This is fine! Right?")
	params = _split_params(mode="sentences")
	out1 = execute_transform_op("split", params, {"in": df})
	out2 = execute_transform_op("split", params, {"in": df})
	assert out1.to_dict(orient="records") == out2.to_dict(orient="records")
	assert [r["part"] for r in out1.to_dict(orient="records")] == [
		"Hi there.",
		"This is fine!",
		"Right?",
	]


def test_split_lines_normalizes_linebreaks():
	df = _input_df("a\r\nb\nc\rd")
	params = _split_params(mode="lines")
	out = execute_transform_op("split", params, {"in": df}).to_dict(orient="records")
	assert [r["part"] for r in out] == ["a", "b", "c", "d"]


def test_split_lines_lf_mode():
	df = _input_df("a\nb\nc")
	params = _split_params(mode="lines", lineBreak="lf")
	out = execute_transform_op("split", params, {"in": df}).to_dict(orient="records")
	assert [r["part"] for r in out] == ["a", "b", "c"]


def test_split_lines_crlf_mode():
	df = _input_df("a\r\nb\r\nc")
	params = _split_params(mode="lines", lineBreak="crlf")
	out = execute_transform_op("split", params, {"in": df}).to_dict(orient="records")
	assert [r["part"] for r in out] == ["a", "b", "c"]


def test_split_lines_cr_mode():
	df = _input_df("a\rb\rc")
	params = _split_params(mode="lines", lineBreak="cr")
	out = execute_transform_op("split", params, {"in": df}).to_dict(orient="records")
	assert [r["part"] for r in out] == ["a", "b", "c"]


def test_split_regex_mode():
	df = _input_df("a   b\tc")
	params = _split_params(mode="regex", pattern=r"\s+")
	out = execute_transform_op("split", params, {"in": df}).to_dict(orient="records")
	assert [r["part"] for r in out] == ["a", "b", "c"]


def test_split_delimiter_mode_with_escapes():
	df = _input_df("x\ny\nz")
	params = _split_params(mode="delimiter", delimiter=r"\n")
	out = execute_transform_op("split", params, {"in": df}).to_dict(orient="records")
	assert [r["part"] for r in out] == ["x", "y", "z"]


def test_split_max_parts_caps_per_row():
	df = _input_df("a,b,c,d,e")
	params = _split_params(mode="delimiter", delimiter=",", maxParts=3)
	out = execute_transform_op("split", params, {"in": df}).to_dict(orient="records")
	assert [r["part"] for r in out] == ["a", "b", "c"]


def test_split_max_chars_guard():
	df = _input_df("x" * (2_000_001))
	params = _split_params(mode="lines")
	with pytest.raises(ValueError, match="exceeds max chars"):
		execute_transform_op("split", params, {"in": df})


def test_split_sentences_normalizes_whitespace_and_keeps_punctuation():
	df = _input_df("A. B. C. D E F, G H? I\nJ K. L\nM.")
	params = _split_params(mode="sentences")
	out = execute_transform_op("split", params, {"in": df}).to_dict(orient="records")
	parts = [r["part"] for r in out]
	assert parts == ["A.", "B.", "C.", "D E F, G H?", "I J K.", "L M."]
	assert all("\n" not in p and "\t" not in p for p in parts)
	assert all("  " not in p for p in parts)


def test_split_sentences_collapses_whitespace_before_split():
	df = _input_df("A.\n\nB\t\tC.   D.")
	params = _split_params(mode="sentences")
	out = execute_transform_op("split", params, {"in": df}).to_dict(orient="records")
	parts = [r["part"] for r in out]
	assert parts == ["A.", "B C.", "D."]
	assert all("\n" not in p and "\t" not in p for p in parts)
	assert all("  " not in p for p in parts)


def test_split_sentences_preserves_terminating_punctuation():
	df = _input_df("Hello! Are you ok? Yes.")
	params = _split_params(mode="sentences")
	out = execute_transform_op("split", params, {"in": df}).to_dict(orient="records")
	assert [r["part"] for r in out] == ["Hello!", "Are you ok?", "Yes."]


def test_split_cap_logs_when_max_parts_reached(caplog):
	df = _input_df("a,b,c,d")
	params = _split_params(mode="delimiter", delimiter=",", maxParts=2)
	with caplog.at_level("WARNING"):
		out = execute_transform_op("split", params, {"in": df}).to_dict(orient="records")
	assert [r["part"] for r in out] == ["a", "b"]
	assert any("Split capped: emitted=2 parts (maxParts=2) for row=0" in m for m in caplog.messages)
