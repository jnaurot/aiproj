from __future__ import annotations

import asyncio

from fastapi.testclient import TestClient

from app.main import app


def _summary(
	*,
	run_id: str,
	created_at: str,
	status: str,
	p95_ms: float,
	failure_code: str | None = None,
	graph_id: str = "graph-analytics-1",
):
	failures = {failure_code: 1} if failure_code else {}
	return {
		"runId": run_id,
		"graphId": graph_id,
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
		asyncio.run(
			rt.event_store.append_event(
				{
					"type": "scheduler_adaptive_decision",
					"runId": "run-a2",
					"at": "2026-03-31T00:10:30Z",
					"mode": "enforce",
					"enforced": True,
					"inputs": {"queueDepth": 12},
					"reasons": ["queue_pressure"],
					"changedCaps": {"global": {"from": 4, "to": 2}},
					"effectiveCaps": {"global": 2},
					"explanation": {"score": 78, "severity": "high"},
				}
			)
		)
		asyncio.run(
			rt.event_store.append_event(
				{
					"type": "scheduler_adaptive_decision",
					"runId": "run-a1",
					"at": "2026-03-31T00:00:30Z",
					"mode": "observe",
					"enforced": False,
					"inputs": {"queueDepth": 4},
					"reasons": ["recovery"],
					"changedCaps": {},
					"effectiveCaps": {"global": 4},
					"explanation": {"score": 24, "severity": "low"},
				}
			)
		)

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

		bottlenecks = client.get(
			"/experiments/bottlenecks",
			params={
				"graphId": "graph-analytics-1",
				"sort": "score_desc",
				"limit": 10,
				"offset": 0,
			},
		)
		assert bottlenecks.status_code == 200, bottlenecks.text
		bottlenecks_body = bottlenecks.json()
		assert str(bottlenecks_body.get("sort") or "") == "score_desc"
		assert int(bottlenecks_body.get("total") or 0) >= 1
		bn_nodes = bottlenecks_body.get("nodes") or []
		assert bn_nodes
		assert str((bn_nodes[0] or {}).get("nodeId") or "") == "node_a"

		bottlenecks_invalid_sort = client.get(
			"/experiments/bottlenecks",
			params={"graphId": "graph-analytics-1", "sort": "bogus"},
		)
		assert bottlenecks_invalid_sort.status_code == 400, bottlenecks_invalid_sort.text

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
		regressions_body = regressions.json()
		assert str(regressions_body.get("sort") or "") == "default"
		assert int(regressions_body.get("total") or 0) >= 2
		alerts = regressions_body.get("alerts") or []
		assert any(str(alert.get("reasonCode") or "") == "LATENCY_DRIFT" for alert in alerts)
		assert any(str(alert.get("reasonCode") or "") == "FAILURE_DRIFT" for alert in alerts)
		assert all(str(alert.get("severity") or "") in {"low", "medium", "high"} for alert in alerts)

		regressions_sorted = client.get(
			"/experiments/regressions",
			params={
				"runId": "run-a2",
				"baselineRunId": "run-a1",
				"sort": "impact_desc",
				"limit": 1,
				"offset": 0,
				"latencyDriftPct": 20,
				"failureDriftAbs": 1,
			},
		)
		assert regressions_sorted.status_code == 200, regressions_sorted.text
		reg_sorted_body = regressions_sorted.json()
		assert str(reg_sorted_body.get("sort") or "") == "impact_desc"
		assert int(reg_sorted_body.get("limit") or 0) == 1
		assert len(reg_sorted_body.get("alerts") or []) == 1

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

		regressions_high_only = client.get(
			"/experiments/regressions",
			params={
				"runId": "run-a2",
				"baselineRunId": "run-a1",
				"severity": "high",
				"latencyDriftPct": 20,
				"failureDriftAbs": 1,
			},
		)
		assert regressions_high_only.status_code == 200, regressions_high_only.text
		high_alerts = regressions_high_only.json().get("alerts") or []
		assert high_alerts
		assert all(str(alert.get("severity") or "") == "high" for alert in high_alerts)

		regressions_invalid_type = client.get(
			"/experiments/regressions",
			params={
				"runId": "run-a2",
				"baselineRunId": "run-a1",
				"alertType": "bogus",
			},
		)
		assert regressions_invalid_type.status_code == 400, regressions_invalid_type.text

		regressions_invalid_sort = client.get(
			"/experiments/regressions",
			params={
				"runId": "run-a2",
				"baselineRunId": "run-a1",
				"sort": "bogus",
			},
		)
		assert regressions_invalid_sort.status_code == 400, regressions_invalid_sort.text

		regressions_invalid_severity = client.get(
			"/experiments/regressions",
			params={
				"runId": "run-a2",
				"baselineRunId": "run-a1",
				"severity": "bogus",
			},
		)
		assert regressions_invalid_severity.status_code == 400, regressions_invalid_severity.text

		adaptive_decisions = client.get(
			"/experiments/adaptive/decisions",
			params={
				"graphId": "graph-analytics-1",
				"mode": "enforce",
				"severity": "high",
				"sort": "impact_desc",
				"limit": 10,
				"offset": 0,
			},
		)
		assert adaptive_decisions.status_code == 200, adaptive_decisions.text
		adaptive_body = adaptive_decisions.json()
		assert str(adaptive_body.get("sort") or "") == "impact_desc"
		assert str(adaptive_body.get("mode") or "") == "enforce"
		assert str(adaptive_body.get("severity") or "") == "high"
		assert int(adaptive_body.get("total") or 0) >= 1
		decision_rows = adaptive_body.get("decisions") or []
		assert decision_rows
		assert all(str(row.get("mode") or "") == "enforce" for row in decision_rows)
		assert all(str((row.get("explanation") or {}).get("severity") or "") == "high" for row in decision_rows)
		assert all(str(row.get("modeSource") or "") in {"env", "run_override"} for row in decision_rows)

		adaptive_invalid_sort = client.get(
			"/experiments/adaptive/decisions",
			params={
				"graphId": "graph-analytics-1",
				"sort": "bogus",
			},
		)
		assert adaptive_invalid_sort.status_code == 400, adaptive_invalid_sort.text

		adaptive_invalid_mode = client.get(
			"/experiments/adaptive/decisions",
			params={
				"graphId": "graph-analytics-1",
				"mode": "bad",
			},
		)
		assert adaptive_invalid_mode.status_code == 400, adaptive_invalid_mode.text

		adaptive_invalid_severity = client.get(
			"/experiments/adaptive/decisions",
			params={
				"graphId": "graph-analytics-1",
				"severity": "bad",
			},
		)
		assert adaptive_invalid_severity.status_code == 400, adaptive_invalid_severity.text

		adaptive_paged = client.get(
			"/experiments/adaptive/decisions",
			params={
				"graphId": "graph-analytics-1",
				"startAt": "2026-03-31T00:00:00Z",
				"endAt": "2026-03-31T00:11:00Z",
				"sort": "created_asc",
				"limit": 1,
				"offset": 0,
			},
		)
		assert adaptive_paged.status_code == 200, adaptive_paged.text
		adaptive_paged_body = adaptive_paged.json()
		assert str(adaptive_paged_body.get("sort") or "") == "created_asc"
		assert int(adaptive_paged_body.get("limit") or 0) == 1
		assert int(adaptive_paged_body.get("offset") or 0) == 0
		assert int(adaptive_paged_body.get("total") or 0) >= 1
		assert len(adaptive_paged_body.get("decisions") or []) == 1


def test_experiments_analytics_supports_time_window_and_pagination():
	with TestClient(app) as client:
		rt = app.state.runtime
		graph_id = "graph-analytics-window"
		asyncio.run(
			rt.artifact_store.upsert_run_experiment(
				_summary(
					run_id="run-win-1",
					created_at="2026-03-31T00:00:00Z",
					status="succeeded",
					p95_ms=900.0,
					failure_code="E_OLD",
					graph_id=graph_id,
				)
			)
		)
		asyncio.run(
			rt.artifact_store.upsert_run_experiment(
				_summary(
					run_id="run-win-2",
					created_at="2026-03-31T00:10:00Z",
					status="succeeded",
					p95_ms=1900.0,
					failure_code="E_MID",
					graph_id=graph_id,
				)
			)
		)
		asyncio.run(
			rt.artifact_store.upsert_run_experiment(
				_summary(
					run_id="run-win-3",
					created_at="2026-03-31T00:20:00Z",
					status="failed",
					p95_ms=3900.0,
					failure_code="E_NEW",
					graph_id=graph_id,
				)
			)
		)

		window_params = {
			"graphId": graph_id,
			"startAt": "2026-03-31T00:09:00Z",
			"endAt": "2026-03-31T00:21:00Z",
		}

		run_trends = client.get("/experiments/trends/runs", params={**window_params, "limit": 1, "offset": 1})
		assert run_trends.status_code == 200, run_trends.text
		run_body = run_trends.json()
		assert int(run_body.get("total") or 0) >= 2
		assert int(run_body.get("offset") or 0) == 1
		run_points = run_body.get("points") or []
		assert len(run_points) == 1
		assert str(run_points[0].get("runId") or "") == "run-win-3"
		assert str(run_body.get("sort") or "") == "created_asc"

		run_trends_runtime_desc = client.get(
			"/experiments/trends/runs",
			params={**window_params, "sort": "runtime_desc", "limit": 2, "offset": 0},
		)
		assert run_trends_runtime_desc.status_code == 200, run_trends_runtime_desc.text
		runtime_points = run_trends_runtime_desc.json().get("points") or []
		assert len(runtime_points) >= 1
		assert float(runtime_points[0].get("runtimeMs") or 0.0) >= float(
			(runtime_points[-1].get("runtimeMs") or 0.0)
		)

		node_trends = client.get(
			"/experiments/trends/nodes",
			params={**window_params, "nodeId": "node_a", "metric": "p95Ms", "limit": 2, "offset": 0},
		)
		assert node_trends.status_code == 200, node_trends.text
		node_body = node_trends.json()
		assert int(node_body.get("total") or 0) == 2
		assert len(node_body.get("points") or []) == 2
		assert str(node_body.get("sort") or "") == "created_asc"

		node_trends_value_desc = client.get(
			"/experiments/trends/nodes",
			params={
				**window_params,
				"nodeId": "node_a",
				"metric": "p95Ms",
				"sort": "value_desc",
				"limit": 2,
				"offset": 0,
			},
		)
		assert node_trends_value_desc.status_code == 200, node_trends_value_desc.text
		node_desc_points = node_trends_value_desc.json().get("points") or []
		assert len(node_desc_points) >= 1
		assert float(node_desc_points[0].get("value") or 0.0) >= float(
			(node_desc_points[-1].get("value") or 0.0)
		)

		breaches = client.get(
			"/experiments/sla/breaches",
			params={**window_params, "p95Ms": 1000, "limit": 1, "offset": 0},
		)
		assert breaches.status_code == 200, breaches.text
		breach_body = breaches.json()
		assert int(breach_body.get("total") or 0) >= 2
		assert len(breach_body.get("breaches") or []) == 1

		taxonomy = client.get(
			"/experiments/failures/taxonomy",
			params={**window_params, "limit": 10, "offset": 0},
		)
		assert taxonomy.status_code == 200, taxonomy.text
		tax_body = taxonomy.json()
		codes = {str(item.get("errorCode") or "") for item in (tax_body.get("taxonomy") or [])}
		assert "E_OLD" not in codes
		assert "E_MID" in codes or "E_NEW" in codes

		invalid_window = client.get(
			"/experiments/trends/runs",
			params={"graphId": graph_id, "startAt": "not-an-iso"},
		)
		assert invalid_window.status_code == 400, invalid_window.text

		invalid_sort = client.get(
			"/experiments/trends/nodes",
			params={"graphId": graph_id, "sort": "bogus"},
		)
		assert invalid_sort.status_code == 400, invalid_sort.text
