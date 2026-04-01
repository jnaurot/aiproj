import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.executors.source import _source_out_mode_from_node, exec_source
from app.runner.contracts import (
	AUDIO_V1,
	BINARY_V1,
	IMAGE_V1,
	JSON_ANY_V1,
	TABLE_V1,
	TEXT_V1,
	VIDEO_V1,
	default_contract_for_node,
)


def _fixture_cases() -> list[dict]:
	fixture_path = Path(__file__).resolve().parents[3] / "shared" / "test_fixtures" / "source_contract_parity.v1.json"
	payload = json.loads(fixture_path.read_text(encoding="utf-8"))
	cases = payload.get("cases") if isinstance(payload, dict) else None
	assert isinstance(cases, list) and cases, "source contract parity fixture must contain cases"
	return [c for c in cases if isinstance(c, dict)]


def _contract_to_out_type(contract: str) -> str:
	mapping = {
		TABLE_V1: "table",
		JSON_ANY_V1: "json",
		TEXT_V1: "text",
		BINARY_V1: "binary",
		IMAGE_V1: "binary",
		AUDIO_V1: "binary",
		VIDEO_V1: "binary",
	}
	return mapping.get(str(contract), "binary")


def _source_node(node_data: dict) -> dict:
	return {
		"id": "src_fixture",
		"data": {
			"kind": "source",
			"label": "Source",
			**dict(node_data or {}),
		},
	}


def _ctx():
	async def _emit(*_args, **_kwargs):
		return None

	return SimpleNamespace(
		bus=SimpleNamespace(emit=_emit),
		artifact_store=SimpleNamespace(),
		graph_id="graph_test",
	)


def test_source_default_mode_contract_parity_fixture_backend():
	for case in _fixture_cases():
		expected = str(case.get("expectedOutType") or "")
		node_data = case.get("nodeData") if isinstance(case.get("nodeData"), dict) else {}
		node = _source_node(node_data)
		contract = default_contract_for_node(node)
		contract_out = _contract_to_out_type(contract)
		assert contract_out == expected, f"{case.get('id')}: contract mismatch ({contract} -> {contract_out}, expected {expected})"
		derived_mode = _source_out_mode_from_node(node)
		assert str(derived_mode or "") == expected, f"{case.get('id')}: mode mismatch ({derived_mode}, expected {expected})"


@pytest.mark.asyncio
@pytest.mark.parametrize(
	"case_id,params_builder",
	[
		("file_csv_default", lambda tmp: {"file_path": str(tmp / "data.csv"), "file_format": "csv"}),
		("file_txt_default", lambda tmp: {"file_path": str(tmp / "note.txt"), "file_format": "txt"}),
		(
			"object_store_json",
			lambda _tmp: {
				"provider": "s3",
				"object_store_mode": "mock",
				"bucket": "demo",
				"key": "file.json",
				"file_format": "json",
				"mock_text": '{"ok":true}',
			},
		),
		(
			"object_store_binary",
			lambda _tmp: {
				"provider": "s3",
				"object_store_mode": "mock",
				"bucket": "demo",
				"key": "img.png",
				"file_format": "png",
				"mock_text": "PNG",
			},
		),
	],
)
async def test_source_runtime_output_mode_matches_contract_fixture(case_id: str, params_builder, tmp_path):
	case = next((c for c in _fixture_cases() if str(c.get("id")) == case_id), None)
	assert isinstance(case, dict), f"missing fixture case {case_id}"
	expected = str(case.get("expectedOutType") or "")
	node_data = dict(case.get("nodeData") or {})
	params = params_builder(tmp_path)
	if case_id == "file_csv_default":
		(tmp_path / "data.csv").write_text("id,name\n1,alice\n", encoding="utf-8")
	if case_id == "file_txt_default":
		(tmp_path / "note.txt").write_text("hello", encoding="utf-8")
	node = _source_node(
		{
			**node_data,
			"params": {
				**(dict(node_data.get("params") or {})),
				**params,
			},
		}
	)
	result = await exec_source(f"run-{case_id}", node, _ctx())
	assert result.status == "succeeded"
	output_mode = str(((result.metadata.data_schema or {}).get("output_mode")) or "")
	assert output_mode == expected
