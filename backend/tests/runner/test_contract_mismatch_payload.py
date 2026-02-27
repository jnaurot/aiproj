from datetime import datetime, timezone

from app.runner.artifacts import Artifact
from app.runner.run import _cached_artifact_contract_mismatch


def test_cached_contract_mismatch_includes_fingerprints():
    node = {
        "id": "n1",
        "data": {
            "kind": "tool",
            "ports": {"out": "json"},
            "params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
        },
    }
    art = Artifact(
        artifact_id="a" * 64,
        node_kind="tool",
        params_hash="p" * 64,
        upstream_ids=[],
        created_at=datetime.now(timezone.utc),
        execution_version="v1",
        mime_type="text/plain; charset=utf-8",
        port_type="text",
        size_bytes=4,
        storage_uri="artifact://a",
        payload_schema={"schema_version": 1, "type": "text"},
        run_id="run-1",
        graph_id="graph-1",
        node_id="n1",
        exec_key="a" * 64,
    )

    mismatch = _cached_artifact_contract_mismatch("tool", node, art)
    assert mismatch is not None
    assert mismatch["expectedPortType"] == "json"
    assert mismatch["actualPortType"] == "text"
    assert mismatch["mismatchKind"] == "port_type"
    assert isinstance(mismatch.get("expectedContractFingerprint"), str)
    assert isinstance(mismatch.get("actualContractFingerprint"), str)
