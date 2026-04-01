from __future__ import annotations

import asyncio

from fastapi.testclient import TestClient

from app.main import app


def _summary(*, run_id: str, created_at: str, status: str, p95_ms: float, failure_code: str | None = None):
	failures = {failure_code: 1} if failure_code else {}
	return {
		"runId": run_id,
		"graphId": "graph-analytics-1",
		"createdAt": created_at,
		"status": status,
		"params": {"nodes": {}},
		"metrics": {"flat": {}},
		"environment": {},
		"artifacts": [],
		"artifactIds": [],
		"analytics": {
			"runTelemetry": {"runtime_ms": 1200, "peak_concurrency": 3},
			"nodeLatencyMs": {"node_a": {"p95Ms": p95_ms, "p50Ms": 100.0, "avgMs": 110.0, "maxMs": p95_ms, "count": 1}},
			"queueDepthTrend": [{"at": created_at, "depth": 3}],
			"failureCategories": failures,
		},
	}


def test_experiments_analytics_trends_and_taxonomy_routes():
	with TestClient(app) as client:
		rt = app.state.runtime
		asyncio.run(rt.artifact_store.upsert_run_experiment(_summary(run_id="run-a1", created_at="2026-03-31T00:00:00Z", status="succeeded", p95_ms=900.0)))
		asyncio.run(rt.artifact_store.upsert_run_experiment(_summary(run_id="run-a2", created_at="2026-03-31T00:10:00Z", status="failed", p95_ms=3500.0, failure_code="MODEL_EXECUTION_FAILED")))

		run_trends = client.get("/experiments/trends/runs", params={"graphId": "graph-analytics-1"})
		assert run_trends.status_code == 200, run_trends.text
		run_points = run_trends.json().get("points") or []
		assert len(run_points) >= 2

		node_trends = client.get(
			"/experiments/trends/nodes",
			params={"graphId": "graph-analytics-1", "nodeId": "node_a", "metric": "p95Ms"},
		)
		assert node_trends.status_code == 200, node_trends.text
		node_points = node_trends.json().get("points") or []
		assert any(float(p.get("value") or 0.0) >= 3500.0 for p in node_points)

		breaches = client.get("/experiments/sla/breaches", params={"graphId": "graph-analytics-1", "p95Ms": 2000})
		assert breaches.status_code == 200, breaches.text
		rows = breaches.json().get("breaches") or []
		assert any(str(row.get("runId") or "") == "run-a2" for row in rows)

		taxonomy = client.get("/experiments/failures/taxonomy", params={"graphId": "graph-analytics-1"})
		assert taxonomy.status_code == 200, taxonomy.text
		items = taxonomy.json().get("taxonomy") or []
		assert any(str(item.get("errorCode") or "") == "MODEL_EXECUTION_FAILED" for item in items)

		regressions = client.get(
			"/experiments/regressions",
			params={
				"runId": "run-a2",
				"baselineRunId": "run-a1",
				"latencyDriftPct": 20,
				"failureDriftAbs": 1,
			},
		)
		assert regressions.status_code == 200, regressions.text
		alerts = regressions.json().get("alerts") or []
		assert any(str(alert.get("reasonCode") or "") == "LATENCY_DRIFT" for alert in alerts)
		assert any(str(alert.get("reasonCode") or "") == "FAILURE_DRIFT" for alert in alerts)

		regressions_latency_only = client.get(
			"/experiments/regressions",
			params={
				"runId": "run-a2",
				"baselineRunId": "run-a1",
				"alertType": "latency",
				"latencyDriftPct": 20,
				"failureDriftAbs": 1,
			},
		)
		assert regressions_latency_only.status_code == 200, regressions_latency_only.text
		latency_alerts = regressions_latency_only.json().get("alerts") or []
		assert latency_alerts
		assert all(str(alert.get("type") or "") == "latency_regression" for alert in latency_alerts)

		regressions_failure_only = client.get(
			"/experiments/regressions",
			params={
				"runId": "run-a2",
				"baselineRunId": "run-a1",
				"alertType": "failure",
				"latencyDriftPct": 20,
				"failureDriftAbs": 1,
			},
		)
		assert regressions_failure_only.status_code == 200, regressions_failure_only.text
		failure_alerts = regressions_failure_only.json().get("alerts") or []
		assert failure_alerts
		assert all(str(alert.get("type") or "") == "failure_regression" for alert in failure_alerts)
