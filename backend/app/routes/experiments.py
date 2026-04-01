from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query, Request

router = APIRouter()


def _parse_iso_utc(value: Optional[str]) -> Optional[datetime]:
	if not value:
		return None
	text = str(value).strip()
	if not text:
		return None
	try:
		if text.endswith("Z"):
			text = f"{text[:-1]}+00:00"
		parsed = datetime.fromisoformat(text)
		if parsed.tzinfo is None:
			parsed = parsed.replace(tzinfo=timezone.utc)
		return parsed.astimezone(timezone.utc)
	except Exception:
		return None


def _created_at_in_window(
	created_at: str,
	*,
	start_at: Optional[datetime],
	end_at: Optional[datetime],
) -> bool:
	current = _parse_iso_utc(created_at)
	if current is None:
		return False
	if start_at is not None and current < start_at:
		return False
	if end_at is not None and current > end_at:
		return False
	return True


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
	startAt: Optional[str] = Query(default=None),
	endAt: Optional[str] = Query(default=None),
	sort: str = Query(default="created_asc"),
	limit: int = Query(default=100, ge=1, le=1000),
	offset: int = Query(default=0, ge=0),
):
	sort_mode = str(sort or "created_asc").strip().lower()
	if sort_mode not in {"created_asc", "created_desc", "runtime_desc"}:
		raise HTTPException(400, "sort must be one of: created_asc, created_desc, runtime_desc")
	start_at = _parse_iso_utc(startAt)
	end_at = _parse_iso_utc(endAt)
	if (startAt and start_at is None) or (endAt and end_at is None):
		raise HTTPException(400, "startAt/endAt must be ISO-8601 timestamps")
	rows = await _list_summaries(
		request, graph_id=graphId, limit=_query_limit_with_offset(limit, offset)
	)
	points: list[Dict[str, Any]] = []
	for row in rows:
		created_at = str(row.get("createdAt") or "")
		if not _created_at_in_window(created_at, start_at=start_at, end_at=end_at):
			continue
		analytics = row.get("analytics") if isinstance(row.get("analytics"), dict) else {}
		run_telemetry = analytics.get("runTelemetry") if isinstance(analytics.get("runTelemetry"), dict) else {}
		points.append(
			{
				"runId": str(row.get("runId") or ""),
				"createdAt": created_at,
				"status": str(row.get("status") or ""),
				"runtimeMs": int(run_telemetry.get("runtime_ms") or 0),
				"peakConcurrency": int(run_telemetry.get("peak_concurrency") or 0),
			}
		)
	if sort_mode == "created_desc":
		points.sort(key=lambda p: str(p.get("createdAt") or ""), reverse=True)
	elif sort_mode == "runtime_desc":
		points.sort(
			key=lambda p: (
				float(p.get("runtimeMs") or 0.0),
				str(p.get("createdAt") or ""),
			),
			reverse=True,
		)
	else:
		points.sort(key=lambda p: str(p.get("createdAt") or ""))
	paged_points = points[offset : offset + limit]
	return {
		"schemaVersion": 1,
		"graphId": str(graphId or "").strip() or None,
		"startAt": startAt,
		"endAt": endAt,
		"sort": sort_mode,
		"limit": int(limit),
		"offset": int(offset),
		"total": len(points),
		"points": paged_points,
	}


@router.get("/trends/nodes")
async def node_trends(
	request: Request,
	graphId: Optional[str] = Query(default=None),
	nodeId: Optional[str] = Query(default=None),
	metric: str = Query(default="p95Ms"),
	startAt: Optional[str] = Query(default=None),
	endAt: Optional[str] = Query(default=None),
	sort: str = Query(default="created_asc"),
	limit: int = Query(default=100, ge=1, le=1000),
	offset: int = Query(default=0, ge=0),
):
	sort_mode = str(sort or "created_asc").strip().lower()
	if sort_mode not in {"created_asc", "created_desc", "value_desc"}:
		raise HTTPException(400, "sort must be one of: created_asc, created_desc, value_desc")
	start_at = _parse_iso_utc(startAt)
	end_at = _parse_iso_utc(endAt)
	if (startAt and start_at is None) or (endAt and end_at is None):
		raise HTTPException(400, "startAt/endAt must be ISO-8601 timestamps")
	rows = await _list_summaries(
		request, graph_id=graphId, limit=_query_limit_with_offset(limit, offset)
	)
	points: list[Dict[str, Any]] = []
	for row in rows:
		created_at = str(row.get("createdAt") or "")
		if not _created_at_in_window(created_at, start_at=start_at, end_at=end_at):
			continue
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
					"createdAt": created_at,
					"nodeId": str(nid),
					"metric": str(metric),
					"value": float(value),
				}
			)
	if sort_mode == "created_desc":
		points.sort(
			key=lambda p: (str(p.get("nodeId") or ""), str(p.get("createdAt") or "")),
			reverse=True,
		)
	elif sort_mode == "value_desc":
		points.sort(
			key=lambda p: (
				float(p.get("value") or 0.0),
				str(p.get("nodeId") or ""),
				str(p.get("createdAt") or ""),
			),
			reverse=True,
		)
	else:
		points.sort(key=lambda p: (str(p.get("nodeId") or ""), str(p.get("createdAt") or "")))
	paged_points = points[offset : offset + limit]
	return {
		"schemaVersion": 1,
		"graphId": str(graphId or "").strip() or None,
		"nodeId": str(nodeId or "").strip() or None,
		"metric": metric,
		"startAt": startAt,
		"endAt": endAt,
		"sort": sort_mode,
		"limit": int(limit),
		"offset": int(offset),
		"total": len(points),
		"points": paged_points,
	}


@router.get("/sla/breaches")
async def sla_breaches(
	request: Request,
	graphId: Optional[str] = Query(default=None),
	p95Ms: float = Query(default=2000.0, gt=0.0),
	startAt: Optional[str] = Query(default=None),
	endAt: Optional[str] = Query(default=None),
	limit: int = Query(default=100, ge=1, le=1000),
	offset: int = Query(default=0, ge=0),
):
	start_at = _parse_iso_utc(startAt)
	end_at = _parse_iso_utc(endAt)
	if (startAt and start_at is None) or (endAt and end_at is None):
		raise HTTPException(400, "startAt/endAt must be ISO-8601 timestamps")
	rows = await _list_summaries(
		request, graph_id=graphId, limit=_query_limit_with_offset(limit, offset)
	)
	breaches: list[Dict[str, Any]] = []
	for row in rows:
		created_at = str(row.get("createdAt") or "")
		if not _created_at_in_window(created_at, start_at=start_at, end_at=end_at):
			continue
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
					"createdAt": created_at,
					"nodeId": str(nid),
					"p95Ms": float(p95),
					"thresholdMs": float(p95Ms),
				}
			)
	breaches.sort(key=lambda row: float(row.get("p95Ms") or 0.0), reverse=True)
	paged_breaches = breaches[offset : offset + limit]
	return {
		"schemaVersion": 1,
		"graphId": str(graphId or "").strip() or None,
		"thresholdMs": float(p95Ms),
		"startAt": startAt,
		"endAt": endAt,
		"limit": int(limit),
		"offset": int(offset),
		"total": len(breaches),
		"breaches": paged_breaches,
	}


@router.get("/failures/taxonomy")
async def failure_taxonomy(
	request: Request,
	graphId: Optional[str] = Query(default=None),
	startAt: Optional[str] = Query(default=None),
	endAt: Optional[str] = Query(default=None),
	limit: int = Query(default=100, ge=1, le=1000),
	offset: int = Query(default=0, ge=0),
):
	start_at = _parse_iso_utc(startAt)
	end_at = _parse_iso_utc(endAt)
	if (startAt and start_at is None) or (endAt and end_at is None):
		raise HTTPException(400, "startAt/endAt must be ISO-8601 timestamps")
	rows = await _list_summaries(
		request, graph_id=graphId, limit=_query_limit_with_offset(limit, offset)
	)
	counts: Dict[str, int] = {}
	for row in rows:
		created_at = str(row.get("createdAt") or "")
		if not _created_at_in_window(created_at, start_at=start_at, end_at=end_at):
			continue
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
	paged_items = items[offset : offset + limit]
	return {
		"schemaVersion": 1,
		"graphId": str(graphId or "").strip() or None,
		"startAt": startAt,
		"endAt": endAt,
		"limit": int(limit),
		"offset": int(offset),
		"total": len(items),
		"taxonomy": paged_items,
	}


def _safe_float(value: Any, default: float = 0.0) -> float:
	try:
		return float(value)
	except Exception:
		return float(default)


def _query_limit_with_offset(
	limit: int,
	offset: int,
	*,
	min_probe: int = 1000,
	cap: int = 5000,
) -> int:
	return max(1, min(int(cap), max(int(min_probe), int(limit) + int(offset))))


@router.get("/regressions")
async def regression_detection(
	request: Request,
	runId: str = Query(..., min_length=1),
	baselineRunId: Optional[str] = Query(default=None),
	alertType: str = Query(default="all"),
	sort: str = Query(default="default"),
	limit: int = Query(default=100, ge=1, le=2000),
	offset: int = Query(default=0, ge=0),
	latencyDriftPct: float = Query(default=25.0, ge=0.0),
	failureDriftAbs: int = Query(default=1, ge=0),
):
	alert_type = str(alertType or "all").strip().lower()
	if alert_type not in {"all", "latency", "failure"}:
		raise HTTPException(400, "alertType must be one of: all, latency, failure")
	sort_mode = str(sort or "default").strip().lower()
	if sort_mode not in {"default", "impact_desc", "impact_asc"}:
		raise HTTPException(400, "sort must be one of: default, impact_desc, impact_asc")
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
	if alert_type in {"all", "latency"}:
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
	if alert_type in {"all", "failure"}:
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

	def _impact_value(row: Dict[str, Any]) -> float:
		if str(row.get("type") or "").strip().lower() == "latency_regression":
			return abs(float(row.get("driftPct") or 0.0))
		return abs(float(row.get("delta") or 0.0))

	if sort_mode == "impact_desc":
		alerts.sort(
			key=lambda row: (
				_impact_value(row),
				str(row.get("reasonCode") or ""),
				str(row.get("nodeId") or row.get("errorCode") or ""),
			),
			reverse=True,
		)
	elif sort_mode == "impact_asc":
		alerts.sort(
			key=lambda row: (
				_impact_value(row),
				str(row.get("reasonCode") or ""),
				str(row.get("nodeId") or row.get("errorCode") or ""),
			),
		)
	else:
		alerts.sort(
			key=lambda row: (
				str(row.get("reasonCode") or ""),
				str(row.get("nodeId") or row.get("errorCode") or ""),
			)
		)
	paged_alerts = alerts[offset : offset + limit]
	return {
		"schemaVersion": 1,
		"runId": str(runId),
		"baselineRunId": str(baseline.get("runId") or ""),
		"alertType": alert_type,
		"sort": sort_mode,
		"limit": int(limit),
		"offset": int(offset),
		"total": len(alerts),
		"alerts": paged_alerts,
	}
