from dataclasses import dataclass

from app.routes.components import _apply_dependency_revision_overrides


@dataclass
class _Revision:
	definition: dict


class _Store:
	def __init__(self):
		self._map = {
			("cmp_x", "crev_1"): _Revision(
				definition={
					"api": {"inputs": [], "outputs": []},
					"exposureRegistry": [
						{
							"handle_id": "data_out::out_data",
							"alias": "out_data",
							"internal_source_path": "out:out_data",
							"kind": "data_output",
							"native_contract": {"type": "text", "fields": []},
							"exposed": True,
							"published": True,
							"debug_visible": False,
						}
					],
				}
			),
			("cmp_x", "crev_2"): _Revision(
				definition={
					"api": {"inputs": [], "outputs": []},
					"exposureRegistry": [
						{
							"handle_id": "data_out::out_data",
							"alias": "out_data",
							"internal_source_path": "out:out_data",
							"kind": "data_output",
							"native_contract": {"type": "json", "fields": []},
							"exposed": True,
							"published": True,
							"debug_visible": False,
						}
					],
				}
			),
		}

	def get_revision(self, component_id: str, revision_id: str):
		return self._map.get((component_id, revision_id))


def test_override_allowed_when_breaking_change_has_explicit_mapping():
	definition = {
		"graph": {
			"nodes": [
				{
					"id": "n1",
					"data": {
						"kind": "component",
						"params": {"componentRef": {"componentId": "cmp_x", "revisionId": "crev_1"}},
					},
				}
			],
			"edges": [],
		}
	}
	next_def, diagnostics = _apply_dependency_revision_overrides(
		definition,
		overrides=[
			{
				"componentId": "cmp_x",
				"fromRevisionId": "crev_1",
				"toRevisionId": "crev_2",
				"compatibilityMapping": {"data_out::out_data": "data_out::out_data"},
			}
		],
		component_store=_Store(),
	)
	assert not any(d.get("code") == "COMPONENT_DEPENDENCY_OVERRIDE_INCOMPATIBLE" for d in diagnostics)
	ref = (
		(next_def.get("graph") or {}).get("nodes")[0].get("data", {}).get("params", {}).get("componentRef", {})
	)
	assert str(ref.get("revisionId") or "") == "crev_2"

