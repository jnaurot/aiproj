from fastapi.testclient import TestClient

from app.main import app


def test_capability_matrix_roundtrip_backend_to_ui():
	with TestClient(app) as client:
		res = client.get("/capabilities")
		assert res.status_code == 200, res.text
		body = res.json()
		caps = body.get("capabilities") if isinstance(body, dict) else {}
		source_caps = ((caps or {}).get("nodes") or {}).get("source") if isinstance(caps, dict) else {}
		assert isinstance(source_caps, dict)
		kind_caps = source_caps.get("kindCapabilities")
		assert isinstance(kind_caps, dict)
		for kind in ("file", "database", "api", "object_store", "warehouse"):
			entry = kind_caps.get(kind)
			assert isinstance(entry, dict)
			level = str(entry.get("supportLevel") or "").strip().lower()
			assert level in {"production", "preview", "mock_only"}
