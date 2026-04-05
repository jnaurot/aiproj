from __future__ import annotations

from app.component_contracts import build_component_migration_report, component_contract_diff


def test_component_revision_upgrade_contract_hard_fail_and_mapping_paths() -> None:
	before = [
		{"handle_id": "h_out_text", "kind": "data_output", "native_type": {"type": "text"}, "published": True},
		{"handle_id": "h_out_json", "kind": "data_output", "native_type": {"type": "json"}, "published": True},
	]
	after_breaking = [
		{"handle_id": "h_out_text_v2", "kind": "data_output", "native_type": {"type": "text"}, "published": True},
		{"handle_id": "h_out_json", "kind": "data_output", "native_type": {"type": "json"}, "published": True},
	]

	diff = component_contract_diff(before, after_breaking)
	assert "h_out_text" in diff["removed"]

	no_map = build_component_migration_report(before, after_breaking, {})
	assert no_map["breaking"] is True
	assert any(item.get("status") == "unmapped" for item in no_map["actions"])

	with_map = build_component_migration_report(before, after_breaking, {"h_out_text": "h_out_text_v2"})
	assert with_map["breaking"] is True
	assert any(item.get("status") == "mapped" for item in with_map["actions"])
