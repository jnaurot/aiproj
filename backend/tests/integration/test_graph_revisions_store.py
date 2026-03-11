from __future__ import annotations

from pathlib import Path

from app.graph_revisions import GraphRevisionStore


def _sample_graph(label: str):
    return {
        "version": 1,
        "nodes": [
            {
                "id": "n1",
                "type": "source",
                "position": {"x": 0, "y": 0},
                "data": {"kind": "source", "label": label, "params": {}},
            }
        ],
        "edges": [],
    }


def test_graph_revision_store_roundtrip(tmp_path: Path):
    db = tmp_path / "graphs.sqlite"
    store = GraphRevisionStore(str(db))

    r1 = store.create_revision(
        graph_id="graph_test",
        graph=_sample_graph("v1"),
        message="first",
    )
    assert r1.graph_id == "graph_test"
    assert r1.parent_revision_id is None
    assert r1.revision_id

    latest1 = store.get_latest("graph_test")
    assert latest1 is not None
    assert latest1.revision_id == r1.revision_id
    assert latest1.graph["nodes"][0]["data"]["label"] == "v1"

    r2 = store.create_revision(
        graph_id="graph_test",
        graph=_sample_graph("v2"),
        message="second",
    )
    assert r2.parent_revision_id == r1.revision_id

    latest2 = store.get_latest("graph_test")
    assert latest2 is not None
    assert latest2.revision_id == r2.revision_id
    assert latest2.graph["nodes"][0]["data"]["label"] == "v2"

    revisions = store.list_revisions("graph_test", limit=10, offset=0)
    assert len(revisions) == 2
    assert revisions[0]["revisionId"] == r2.revision_id
    assert revisions[1]["revisionId"] == r1.revision_id

    row = store.get_revision("graph_test", r1.revision_id)
    assert row is not None
    assert row.graph["nodes"][0]["data"]["label"] == "v1"

