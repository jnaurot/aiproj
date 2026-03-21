from pathlib import Path
import json

from app.runner.schemas import SourceFileParams, normalize_source_params_frontend


def _fixture_cases():
	fixture_path = Path(__file__).resolve().parents[3] / "shared" / "test_fixtures" / "source_filetype_parity.v1.json"
	payload = json.loads(fixture_path.read_text(encoding="utf-8"))
	return payload.get("cases", [])


def test_source_filetype_parity_fixture_defaults_match_backend_schema():
	for case in _fixture_cases():
		file_format = str(case["file_format"])
		expected = str(case["expected_output"])
		params = SourceFileParams.model_validate(
			{
				"rel_path": ".",
				"filename": f"data.{file_format}",
				"file_format": file_format,
			}
		)
		assert params.output_mode == expected


def test_source_filetype_parity_fixture_camel_case_normalization_keeps_contract():
	for case in _fixture_cases():
		file_format = str(case["file_format"])
		expected = str(case["expected_output"])
		normalized = normalize_source_params_frontend(
			{
				"source_type": "file",
				"filename": f"data.{file_format}",
				"file_format": file_format,
			}
		)
		params = SourceFileParams.model_validate(normalized)
		assert params.output_mode == expected
