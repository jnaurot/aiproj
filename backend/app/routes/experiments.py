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
