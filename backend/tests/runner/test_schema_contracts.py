from app.schema_contracts import canonicalize_schema_envelope


def test_canonicalize_schema_envelope_strips_unknown_keys():
	raw = {
		"inferredSchema": {
			"source": "sample",
			"typedSchema": {"type": "table", "fields": [{"name": "id", "type": "json", "nullable": False}]},
			"unexpected": True,
		},
		"extraChannel": {},
	}
	canonical, changed = canonicalize_schema_envelope(raw)
	assert changed is True
	assert isinstance(canonical, dict)
	assert "extraChannel" not in canonical
	assert "unexpected" not in ((canonical or {}).get("inferredSchema") or {})
	assert str((((canonical or {}).get("inferredSchema") or {}).get("source") or "")) == "sample"


def test_canonicalize_schema_envelope_rejects_non_object():
	canonical, changed = canonicalize_schema_envelope(["invalid"])
	assert canonical is None
	assert changed is True
