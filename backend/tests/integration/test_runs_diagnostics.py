from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app


def test_runs_diagnostics_endpoint_available():
	with TestClient(app) as client:
		res = client.get("/runs/diagnostics")
		assert res.status_code == 200, res.text
		body = res.json()
		assert body["schemaVersion"] == 1
		assert "artifactMemo" in body
		assert "activeRuns" in body
