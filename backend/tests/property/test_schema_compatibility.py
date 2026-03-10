from __future__ import annotations

from hypothesis import given, strategies as st

from app.runner.run import _typed_schema_compatibility


_PRIMITIVE_TYPES = st.sampled_from(["unknown", "table", "json", "text", "binary", "embeddings"])
_POLICY = st.sampled_from(["safe_only", "allow_lossy"])
_FIELD_NAME = st.text(
	alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters=["_"]),
	min_size=1,
	max_size=8,
)
_FIELD_TYPE = st.sampled_from(["unknown", "int64", "float64", "string", "boolean"])


def _typed_schema(type_name: str, fields: list[tuple[str, str]]) -> dict:
	return {
		"type": type_name,
		"fields": [{"name": name, "type": field_type, "nullable": False} for name, field_type in fields],
	}


@given(
	expected_type=_PRIMITIVE_TYPES,
	actual_type=_PRIMITIVE_TYPES,
	expected_fields=st.lists(st.tuples(_FIELD_NAME, _FIELD_TYPE), unique_by=lambda item: item[0], max_size=4),
	actual_fields=st.lists(st.tuples(_FIELD_NAME, _FIELD_TYPE), unique_by=lambda item: item[0], max_size=4),
	policy=_POLICY,
)
def test_typed_schema_compatibility_invariants(
	expected_type: str,
	actual_type: str,
	expected_fields: list[tuple[str, str]],
	actual_fields: list[tuple[str, str]],
	policy: str,
) -> None:
	expected = _typed_schema(expected_type, expected_fields)
	actual = _typed_schema(actual_type, actual_fields)

	ok, info = _typed_schema_compatibility(expected=expected, actual=actual, policy=policy)

	# Invariant: allow_lossy never fails compatibility.
	if policy == "allow_lossy":
		assert ok is True
		return

	# Invariant: safe_only fails when concrete types mismatch (except unknown).
	if expected_type != "unknown" and actual_type != "unknown" and expected_type != actual_type:
		assert ok is False
		assert info.get("reason") == "type_mismatch"
		return

	# Invariant: safe_only table expected requires expected fields subset.
	if expected_type == "table" and actual_type == "table":
		expected_names = [name for name, _ in expected_fields]
		actual_names = {name for name, _ in actual_fields}
		expected_types = {name: f_type for name, f_type in expected_fields}
		actual_types = {name: f_type for name, f_type in actual_fields}
		mismatched = sorted(
			[
				name
				for name, exp_type in expected_types.items()
				if name in actual_types
				and exp_type not in {"", "unknown"}
				and actual_types[name] not in {"", "unknown"}
				and exp_type != actual_types[name]
			]
		)
		if mismatched:
			assert ok is False
			assert info.get("reason") == "column_type_mismatch"
			assert sorted(info.get("mismatchedColumns") or []) == mismatched
			return

		expected_set = {f for f in expected_names}
		actual_set = {f for f in actual_names}
		missing = sorted(expected_set - actual_set)
		if missing:
			assert ok is False
			assert info.get("reason") == "missing_columns"
			assert sorted(info.get("missingColumns") or []) == missing
		else:
			assert ok is True
			assert info.get("missingColumns") == []
		return

	# Otherwise compatibility should pass under safe_only.
	assert ok is True
