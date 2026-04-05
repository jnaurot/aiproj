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


def test_override_rejected_when_breaking_published_contract_has_no_mapping():
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
	_next, diagnostics = _apply_dependency_revision_overrides(
		definition,
		overrides=[
			{
				"componentId": "cmp_x",
				"fromRevisionId": "crev_1",
				"toRevisionId": "crev_2",
			}
		],
		component_store=_Store(),
	)
	assert any(d.get("code") == "COMPONENT_DEPENDENCY_OVERRIDE_INCOMPATIBLE" for d in diagnostics)

