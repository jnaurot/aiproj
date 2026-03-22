from __future__ import annotations

import pytest

from app.schema_contracts import canonicalize_schema_envelope


def test_canonicalize_schema_envelope_preserves_expected_input_schemas() -> None:
	raw = {
		"expectedInputSchemas": {
			"in": {
				"source": "declared",
				"state": "fresh",
				"typedSchema": {"type": "json", "fields": []},
			},
			"param_config": {
				"source": "declared",
				"state": "fresh",
				"typedSchema": {"type": "json", "fields": []},
			},
		}
	}
	canonical, changed = canonicalize_schema_envelope(raw)
	assert isinstance(canonical, dict)
	assert changed is False
	assert "expectedInputSchemas" in canonical
	assert canonical["expectedInputSchemas"]["in"]["typedSchema"]["type"] == "json"
	assert canonical["expectedInputSchemas"]["param_config"]["typedSchema"]["type"] == "json"


def test_canonicalize_schema_envelope_rejects_non_object_expected_input_schemas() -> None:
	raw = {"expectedInputSchemas": []}
	with pytest.raises(Exception):
		canonicalize_schema_envelope(raw)
