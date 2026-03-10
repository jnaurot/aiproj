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
		assert "featureFlags" in body
		assert body["featureFlags"]["STRICT_SCHEMA_EDGE_CHECKS"] in {True, False}
		assert body["featureFlags"]["STRICT_SCHEMA_EDGE_CHECKS_V2"] in {True, False}
		assert body["featureFlags"]["STRICT_COERCION_POLICY"] in {True, False}
		assert "LEGACY_COMPONENT_WRAPPER_FALLBACK" not in body["featureFlags"]
		assert "rolloutMetrics" in body
		metrics = body["rolloutMetrics"]
		assert isinstance(metrics["schemaFailures"], int)
		assert isinstance(metrics["coercionApplied"], int)
		assert isinstance(metrics["componentBindingFailures"], int)
