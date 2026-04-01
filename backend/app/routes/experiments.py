from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query, Request

router = APIRouter()


def _extract_flat_metrics(summary: Dict[str, Any]) -> Dict[str, float]:
	metrics = summary.get("metrics") if isinstance(summary.get("metrics"), dict) else {}
	flat = metrics.get("flat") if isinstance(metrics.get("flat"), dict) else {}
	out: Dict[str, float] = {}
	for key, raw in flat.items():
		name = str(key or "").strip()
		if not name:
			continue
		if isinstance(raw, (int, float)):
			out[name] = float(raw)
	return out


def _extract_param_nodes(summary: Dict[str, Any]) -> Dict[str, Any]:
	params = summary.get("params") if isinstance(summary.get("params"), dict) else {}
	nodes = params.get("nodes") if isinstance(params.get("nodes"), dict) else {}
	return nodes


async def _get_summary_or_404(request: Request, run_id: str) -> Dict[str, Any]:
	rt = request.app.state.runtime
	get_fn = getattr(rt.artifact_store, "get_run_experiment", None)
	if not callable(get_fn):
		raise HTTPException(404, "experiment tracking unavailable")
	row = await get_fn(str(run_id or "").strip())
	if not isinstance(row, dict):
		raise HTTPException(404, "Experiment run summary not found")
	return row


async def _list_summaries(
	request: Request,
	*,
	graph_id: Optional[str],
	limit: int = 200,
) -> list[Dict[str, Any]]:
	rt = request.app.state.runtime
	list_fn = getattr(rt.artifact_store, "list_run_experiments", None)
	if not callable(list_fn):
		raise HTTPException(404, "experiment tracking unavailable")
	rows = await list_fn(graph_id=graph_id, limit=limit, offset=0)
	return [row for row in rows if isinstance(row, dict)]


@router.get("")
async def list_experiments(
	request: Request,
	graphId: Optional[str] = Query(default=None),
	limit: int = Query(default=50, ge=1, le=500),
	offset: int = Query(default=0, ge=0),
):
	rt = request.app.state.runtime
	list_fn = getattr(rt.artifact_store, "list_run_experiments", None)
	if not callable(list_fn):
		raise HTTPException(404, "experiment tracking unavailable")
	rows = await list_fn(graph_id=graphId, limit=limit, offset=offset)
	return {
		"schemaVersion": 1,
		"graphId": str(graphId or "").strip() or None,
		"limit": int(limit),
		"offset": int(offset),
		"experiments": rows,
	}


@router.get("/runs/{run_id}")
async def get_run_experiment(run_id: str, request: Request):
	row = await _get_summary_or_404(request, run_id)
	return {"schemaVersion": 1, "experiment": row}


@router.get("/compare")
async def compare_runs(
	request: Request,
	runA: str = Query(...),
	runB: str = Query(...),
):
	a = await _get_summary_or_404(request, runA)
	b = await _get_summary_or_404(request, runB)
	a_flat = _extract_flat_metrics(a)
	b_flat = _extract_flat_metrics(b)
	a_keys = set(a_flat.keys())
	b_keys = set(b_flat.keys())
	common = sorted(a_keys & b_keys)
	added = sorted(b_keys - a_keys)
	removed = sorted(a_keys - b_keys)

	metric_deltas: list[Dict[str, Any]] = []
	for metric in common:
		av = float(a_flat[metric])
		bv = float(b_flat[metric])
		delta = bv - av
		pct_delta = None if av == 0 else (delta / av) * 100.0
		metric_deltas.append(
			{
				"metric": metric,
				"runA": av,
				"runB": bv,
				"delta": delta,
				"pctDelta": pct_delta,
			}
		)
	metric_deltas.sort(key=lambda row: abs(float(row.get("delta") or 0.0)), reverse=True)

	a_nodes = _extract_param_nodes(a)
	b_nodes = _extract_param_nodes(b)
	changed_nodes = sorted(
		[
			nid
			for nid in set(a_nodes.keys()) | set(b_nodes.keys())
			if a_nodes.get(nid) != b_nodes.get(nid)
		]
	)

	return {
		"schemaVersion": 1,
		"runA": a,
		"runB": b,
		"comparison": {
			"sharedMetricCount": len(common),
			"addedMetricCount": len(added),
			"removedMetricCount": len(removed),
			"changedNodeCount": len(changed_nodes),
			"changedNodes": changed_nodes,
			"metricDeltas": metric_deltas,
			"addedMetrics": added,
			"removedMetrics": removed,
		},
	}


@router.get("/trends/runs")
async def run_trends(
	request: Request,
	graphId: Optional[str] = Query(default=None),
	limit: int = Query(default=100, ge=1, le=1000),
):
	rows = await _list_summaries(request, graph_id=graphId, limit=limit)
	points: list[Dict[str, Any]] = []
	for row in rows:
		analytics = row.get("analytics") if isinstance(row.get("analytics"), dict) else {}
		run_telemetry = analytics.get("runTelemetry") if isinstance(analytics.get("runTelemetry"), dict) else {}
		points.append(
			{
				"runId": str(row.get("runId") or ""),
				"createdAt": str(row.get("createdAt") or ""),
				"status": str(row.get("status") or ""),
				"runtimeMs": int(run_telemetry.get("runtime_ms") or 0),
				"peakConcurrency": int(run_telemetry.get("peak_concurrency") or 0),
			}
		)
	points.sort(key=lambda p: str(p.get("createdAt") or ""))
	return {
		"schemaVersion": 1,
		"graphId": str(graphId or "").strip() or None,
		"points": points,
	}


@router.get("/trends/nodes")
async def node_trends(
	request: Request,
	graphId: Optional[str] = Query(default=None),
	nodeId: Optional[str] = Query(default=None),
	metric: str = Query(default="p95Ms"),
	limit: int = Query(default=100, ge=1, le=1000),
):
	rows = await _list_summaries(request, graph_id=graphId, limit=limit)
	points: list[Dict[str, Any]] = []
	for row in rows:
		analytics = row.get("analytics") if isinstance(row.get("analytics"), dict) else {}
		lat = analytics.get("nodeLatencyMs") if isinstance(analytics.get("nodeLatencyMs"), dict) else {}
		for nid, item in lat.items():
			if nodeId and str(nid) != str(nodeId):
				continue
			if not isinstance(item, dict):
				continue
			value = item.get(metric)
			if not isinstance(value, (int, float)):
				continue
			points.append(
				{
					"runId": str(row.get("runId") or ""),
					"createdAt": str(row.get("createdAt") or ""),
					"nodeId": str(nid),
					"metric": str(metric),
					"value": float(value),
				}
			)
	points.sort(key=lambda p: (str(p.get("nodeId") or ""), str(p.get("createdAt") or "")))
	return {
		"schemaVersion": 1,
		"graphId": str(graphId or "").strip() or None,
		"nodeId": str(nodeId or "").strip() or None,
		"metric": metric,
		"points": points,
	}


@router.get("/sla/breaches")
async def sla_breaches(
	request: Request,
	graphId: Optional[str] = Query(default=None),
	p95Ms: float = Query(default=2000.0, gt=0.0),
	limit: int = Query(default=100, ge=1, le=1000),
):
	rows = await _list_summaries(request, graph_id=graphId, limit=limit)
	breaches: list[Dict[str, Any]] = []
	for row in rows:
		analytics = row.get("analytics") if isinstance(row.get("analytics"), dict) else {}
		lat = analytics.get("nodeLatencyMs") if isinstance(analytics.get("nodeLatencyMs"), dict) else {}
		for nid, item in lat.items():
			if not isinstance(item, dict):
				continue
			p95 = item.get("p95Ms")
			if not isinstance(p95, (int, float)):
				continue
			if float(p95) <= float(p95Ms):
				continue
			breaches.append(
				{
					"runId": str(row.get("runId") or ""),
					"createdAt": str(row.get("createdAt") or ""),
					"nodeId": str(nid),
					"p95Ms": float(p95),
					"thresholdMs": float(p95Ms),
				}
			)
	breaches.sort(key=lambda row: float(row.get("p95Ms") or 0.0), reverse=True)
	return {
		"schemaVersion": 1,
		"graphId": str(graphId or "").strip() or None,
		"thresholdMs": float(p95Ms),
		"breaches": breaches,
	}


@router.get("/failures/taxonomy")
async def failure_taxonomy(
	request: Request,
	graphId: Optional[str] = Query(default=None),
	limit: int = Query(default=100, ge=1, le=1000),
):
	rows = await _list_summaries(request, graph_id=graphId, limit=limit)
	counts: Dict[str, int] = {}
	for row in rows:
		analytics = row.get("analytics") if isinstance(row.get("analytics"), dict) else {}
		failures = analytics.get("failureCategories") if isinstance(analytics.get("failureCategories"), dict) else {}
		for code, raw in failures.items():
			key = str(code or "").strip() or "unknown"
			try:
				value = int(raw)
			except Exception:
				value = 0
			counts[key] = int(counts.get(key, 0)) + max(0, value)
	items = [{"errorCode": code, "count": int(count)} for code, count in counts.items()]
	items.sort(key=lambda row: int(row.get("count") or 0), reverse=True)
	return {
		"schemaVersion": 1,
		"graphId": str(graphId or "").strip() or None,
		"taxonomy": items,
	}


def _safe_float(value: Any, default: float = 0.0) -> float:
	try:
		return float(value)
	except Exception:
		return float(default)


@router.get("/regressions")
async def regression_detection(
	request: Request,
	runId: str = Query(..., min_length=1),
	baselineRunId: Optional[str] = Query(default=None),
	latencyDriftPct: float = Query(default=25.0, ge=0.0),
	failureDriftAbs: int = Query(default=1, ge=0),
):
	current = await _get_summary_or_404(request, runId)
	if baselineRunId:
		baseline = await _get_summary_or_404(request, baselineRunId)
	else:
		graph_id = str(current.get("graphId") or "").strip() or None
		rows = await _list_summaries(request, graph_id=graph_id, limit=200)
		rows_sorted = sorted(rows, key=lambda row: str(row.get("createdAt") or ""), reverse=True)
		baseline = next((row for row in rows_sorted if str(row.get("runId") or "") != str(runId)), None)
		if not isinstance(baseline, dict):
			raise HTTPException(404, "No baseline run available for regression detection")

	alerts: list[Dict[str, Any]] = []
	current_analytics = current.get("analytics") if isinstance(current.get("analytics"), dict) else {}
	baseline_analytics = baseline.get("analytics") if isinstance(baseline.get("analytics"), dict) else {}
	current_latency = (
		current_analytics.get("nodeLatencyMs") if isinstance(current_analytics.get("nodeLatencyMs"), dict) else {}
	)
	baseline_latency = (
		baseline_analytics.get("nodeLatencyMs") if isinstance(baseline_analytics.get("nodeLatencyMs"), dict) else {}
	)
	for node_id in sorted(set(current_latency.keys()) & set(baseline_latency.keys())):
		cur_item = current_latency.get(node_id) if isinstance(current_latency.get(node_id), dict) else {}
		base_item = baseline_latency.get(node_id) if isinstance(baseline_latency.get(node_id), dict) else {}
		cur_p95 = _safe_float(cur_item.get("p95Ms"), 0.0)
		base_p95 = _safe_float(base_item.get("p95Ms"), 0.0)
		if base_p95 <= 0:
			continue
		drift_pct = ((cur_p95 - base_p95) / base_p95) * 100.0
		if drift_pct < float(latencyDriftPct):
			continue
		alerts.append(
			{
				"type": "latency_regression",
				"nodeId": str(node_id),
				"metric": "p95Ms",
				"baseline": base_p95,
				"current": cur_p95,
				"driftPct": drift_pct,
				"thresholdPct": float(latencyDriftPct),
				"reasonCode": "LATENCY_DRIFT",
			}
		)

	current_failures = (
		current_analytics.get("failureCategories")
		if isinstance(current_analytics.get("failureCategories"), dict)
		else {}
	)
	baseline_failures = (
		baseline_analytics.get("failureCategories")
		if isinstance(baseline_analytics.get("failureCategories"), dict)
		else {}
	)
	for code in sorted(set(current_failures.keys()) | set(baseline_failures.keys())):
		cur_count = int(_safe_float(current_failures.get(code), 0.0))
		base_count = int(_safe_float(baseline_failures.get(code), 0.0))
		delta = cur_count - base_count
		if delta < int(failureDriftAbs):
			continue
		alerts.append(
			{
				"type": "failure_regression",
				"errorCode": str(code),
				"baseline": base_count,
				"current": cur_count,
				"delta": delta,
				"thresholdAbs": int(failureDriftAbs),
				"reasonCode": "FAILURE_DRIFT",
			}
		)

	alerts.sort(key=lambda row: (str(row.get("reasonCode") or ""), str(row.get("nodeId") or row.get("errorCode") or "")))
	return {
		"schemaVersion": 1,
		"runId": str(runId),
		"baselineRunId": str(baseline.get("runId") or ""),
		"alerts": alerts,
	}
