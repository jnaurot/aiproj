import hashlib
import json
from pathlib import Path
import sys
import types

from fastapi.testclient import TestClient

if "duckdb" not in sys.modules:
    sys.modules["duckdb"] = types.SimpleNamespace()

from app.main import app


def test_capabilities_endpoint_matches_shared_contract_file():
    shared_path = Path(__file__).resolve().parents[3] / "shared" / "port_capabilities.v1.json"
    raw = shared_path.read_bytes()
    payload = json.loads(raw.decode("utf-8"))
    expected_sig = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()

    with TestClient(app) as client:
        res = client.get("/capabilities")
        assert res.status_code == 200, res.text
        body = res.json()

    assert body.get("schemaVersion") == 1
    assert body.get("signature") == expected_sig
    assert body.get("capabilities") == payload
    assert body.get("featureFlags", {}).get("STRICT_SCHEMA_EDGE_CHECKS") in {True, False}
    assert body.get("featureFlags", {}).get("STRICT_COERCION_POLICY") in {True, False}
