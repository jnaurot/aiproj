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


def test_component_revision_store_rename_delete_component_and_revision(tmp_path: Path):
    db = tmp_path / "components.sqlite"
    store = ComponentRevisionStore(str(db))

    r1 = store.create_revision(
        component_id="cmp_ops",
        revision_id="crev_a",
        definition=_sample_definition("a"),
        message="a",
    )
    r2 = store.create_revision(
        component_id="cmp_ops",
        revision_id="crev_b",
        definition=_sample_definition("b"),
        message="b",
    )
    assert r1.revision_id == "crev_a"
    assert r2.revision_id == "crev_b"

    renamed = store.rename_component(from_component_id="cmp_ops", to_component_id="cmp_ops_v2")
    assert renamed["ok"] is True
    assert renamed["componentId"] == "cmp_ops_v2"
    assert store.get_revision("cmp_ops", "crev_a") is None
    assert store.get_revision("cmp_ops_v2", "crev_a") is not None

    deleted_mid = store.delete_revision("cmp_ops_v2", "crev_a")
    assert deleted_mid["ok"] is True
    assert deleted_mid["componentDeleted"] is False
    assert deleted_mid["remainingLatestRevisionId"] == "crev_b"
    remaining = store.list_revisions("cmp_ops_v2", limit=10, offset=0)
    assert [r["revisionId"] for r in remaining] == ["crev_b"]

    deleted_last = store.delete_revision("cmp_ops_v2", "crev_b")
    assert deleted_last["ok"] is True
    assert deleted_last["componentDeleted"] is True
    assert deleted_last["remainingLatestRevisionId"] is None
    assert store.list_components(limit=10, offset=0) == []

    store.create_revision(
        component_id="cmp_delete_me",
        revision_id="crev_1",
        definition=_sample_definition("x"),
    )
    store.create_revision(
        component_id="cmp_delete_me",
        revision_id="crev_2",
        definition=_sample_definition("y"),
    )
    deleted_component = store.delete_component("cmp_delete_me")
    assert deleted_component["ok"] is True
    assert deleted_component["deletedRevisions"] == 2
    assert deleted_component["deletedComponents"] == 1
    assert store.list_revisions("cmp_delete_me", limit=10, offset=0) == []

