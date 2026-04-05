from app.component_contracts import build_component_migration_report


def test_migration_report_contains_actionable_items():
	report = build_component_migration_report(
		from_published=[
			{"handle_id": "h1", "kind": "data_output", "native_contract": {"type": "text", "fields": []}}
		],
		to_published=[
			{"handle_id": "h1", "kind": "data_output", "native_contract": {"type": "json", "fields": []}}
		],
		compatibility_mapping={},
	)
	assert report["breaking"] is True
	assert isinstance(report.get("actions"), list)
	assert any(action.get("kind") == "retyped" for action in report["actions"])

