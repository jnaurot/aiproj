from __future__ import annotations

from pathlib import Path

from app.component_revisions import ComponentRevisionStore


def _sample_definition(label: str):
    return {
        "graph": {
            "version": 1,
            "nodes": [
                {
                    "id": "n1",
                    "type": "source",
                    "position": {"x": 0, "y": 0},
                    "data": {
                        "kind": "source",
                        "label": label,
                        "params": {},
                        "ports": {"in": None, "out": "table"},
                    },
                }
            ],
            "edges": [],
        },
        "api": {
            "inputs": [
                {
                    "name": "input",
                    "portType": "table",
                    "required": True,
                    "typedSchema": {"type": "table", "fields": []},
                }
            ],
            "outputs": [
                {
                    "name": "output",
                    "portType": "json",
                    "required": True,
                    "typedSchema": {"type": "json", "fields": []},
                }
            ],
        },
        "configSchema": {},
    }


def test_component_revision_store_roundtrip(tmp_path: Path):
    db = tmp_path / "components.sqlite"
    store = ComponentRevisionStore(str(db))

    r1 = store.create_revision(
        component_id="cmp_test",
        definition=_sample_definition("v1"),
        message="first",
    )
    assert r1.component_id == "cmp_test"
    assert r1.parent_revision_id is None
    assert r1.revision_id

    r2 = store.create_revision(
        component_id="cmp_test",
        definition=_sample_definition("v2"),
        message="second",
    )
    assert r2.parent_revision_id == r1.revision_id

    components = store.list_components(limit=10, offset=0)
    assert len(components) == 1
    assert components[0]["componentId"] == "cmp_test"
    assert components[0]["latestRevisionId"] == r2.revision_id

    revisions = store.list_revisions("cmp_test", limit=10, offset=0)
    assert len(revisions) == 2
    assert revisions[0]["revisionId"] == r2.revision_id
    assert revisions[1]["revisionId"] == r1.revision_id

    row = store.get_revision("cmp_test", r1.revision_id)
    assert row is not None
    assert row.definition["graph"]["nodes"][0]["data"]["label"] == "v1"

