from __future__ import annotations

import os
from typing import Any, Dict, Optional

from fastapi import APIRouter, Header, HTTPException, Query, Request
from pydantic import BaseModel, field_validator

router = APIRouter()


def _require_admin_for_promotion(x_model_admin: Optional[str]) -> None:
	require_admin = (os.getenv("MODEL_REGISTRY_REQUIRE_ADMIN") or "").strip().lower() in {"1", "true", "yes", "on"}
	if not require_admin:
		return
	header_value = str(x_model_admin or "").strip().lower()
	if header_value not in {"1", "true", "yes", "on", "admin"}:
		raise HTTPException(status_code=403, detail="model promotion requires admin permission")


def _get_store(request: Request):
	store = getattr(request.app.state, "model_registry", None)
	if store is None:
		raise HTTPException(status_code=500, detail="model registry store unavailable")
	return store


async def _resolve_run_experiment(request: Request, run_id: Optional[str]) -> Optional[Dict[str, Any]]:
	rid = str(run_id or "").strip()
	if not rid:
		return None
	rt = request.app.state.runtime
	get_fn = getattr(rt.artifact_store, "get_run_experiment", None)
	if not callable(get_fn):
		raise HTTPException(status_code=404, detail="experiment tracking unavailable")
	row = await get_fn(rid)
	if not isinstance(row, dict):
		raise HTTPException(status_code=404, detail=f"run experiment not found: {rid}")
	return row


class ModelRegisterVersionRequest(BaseModel):
	modelId: Optional[str] = None
	modelName: Optional[str] = None
	versionId: Optional[str] = None
	stage: str = "candidate"
	runId: Optional[str] = None
	artifactId: Optional[str] = None
	metrics: Optional[Dict[str, Any]] = None
	params: Optional[Dict[str, Any]] = None
	environment: Optional[Dict[str, Any]] = None
	provenance: Optional[Dict[str, Any]] = None

	@field_validator("stage")
	@classmethod
	def validate_stage(cls, v):
		s = str(v or "").strip().lower()
		if s not in {"candidate", "baseline", "prod"}:
			raise ValueError("stage must be one of: candidate, baseline, prod")
		return s


class ModelPromoteRequest(BaseModel):
	toStage: str
	force: bool = False
	promotedBy: Optional[str] = None

	@field_validator("toStage")
	@classmethod
	def validate_stage(cls, v):
		s = str(v or "").strip().lower()
		if s not in {"candidate", "baseline", "prod"}:
			raise ValueError("toStage must be one of: candidate, baseline, prod")
		return s


@router.post("/versions/register")
async def register_model_version(req: ModelRegisterVersionRequest, request: Request):
	store = _get_store(request)
	summary = await _resolve_run_experiment(request, req.runId)
	metrics = req.metrics if isinstance(req.metrics, dict) else {}
	params = req.params if isinstance(req.params, dict) else {}
	environment = req.environment if isinstance(req.environment, dict) else {}
	provenance = req.provenance if isinstance(req.provenance, dict) else {}
	graph_id = None
	if isinstance(summary, dict):
		graph_id = str(summary.get("graphId") or "").strip() or None
		summary_metrics = summary.get("metrics") if isinstance(summary.get("metrics"), dict) else {}
		summary_params = summary.get("params") if isinstance(summary.get("params"), dict) else {}
		summary_env = summary.get("environment") if isinstance(summary.get("environment"), dict) else {}
		summary_artifacts = summary.get("artifacts") if isinstance(summary.get("artifacts"), list) else []
		if not metrics and summary_metrics:
			metrics = summary_metrics
		if not params and summary_params:
			params = summary_params
		if not environment and summary_env:
			environment = summary_env
		provenance = {
			"source": "run_experiment",
			"runId": str(summary.get("runId") or req.runId or ""),
			"graphId": graph_id,
			"artifactIds": [
				str(a.get("artifactId"))
				for a in summary_artifacts
				if isinstance(a, dict) and str(a.get("artifactId") or "").strip()
			],
			**provenance,
		}

	try:
		version = store.register_version(
			model_id=req.modelId,
			model_name=req.modelName,
			version_id=req.versionId,
			stage=req.stage,
			run_id=req.runId,
			graph_id=graph_id,
			artifact_id=req.artifactId,
			metrics=metrics,
			params=params,
			environment=environment,
			provenance=provenance,
		)
	except ValueError as ex:
		raise HTTPException(status_code=400, detail=str(ex))

	return {
		"schemaVersion": 1,
		"modelId": version.model_id,
		"versionId": version.version_id,
		"versionNumber": version.version_number,
		"stage": version.stage,
		"createdAt": version.created_at,
		"runId": version.run_id,
		"graphId": version.graph_id,
		"artifactId": version.artifact_id,
		"provenance": version.provenance,
	}


@router.get("")
async def list_models(
	request: Request,
	limit: int = Query(default=50, ge=1, le=500),
	offset: int = Query(default=0, ge=0),
):
	store = _get_store(request)
	rows = store.list_models(limit=limit, offset=offset)
	return {"schemaVersion": 1, "models": rows}


@router.get("/{model_id}")
async def get_model(model_id: str, request: Request):
	store = _get_store(request)
	row = store.get_model(model_id)
	if not isinstance(row, dict):
		raise HTTPException(status_code=404, detail="model not found")
	return {"schemaVersion": 1, "model": row}


@router.get("/{model_id}/versions")
async def list_model_versions(
	model_id: str,
	request: Request,
	limit: int = Query(default=50, ge=1, le=500),
	offset: int = Query(default=0, ge=0),
):
	store = _get_store(request)
	rows = store.list_versions(model_id, limit=limit, offset=offset)
	return {"schemaVersion": 1, "modelId": model_id, "versions": rows}


@router.get("/{model_id}/versions/{version_id}")
async def get_model_version(model_id: str, version_id: str, request: Request):
	store = _get_store(request)
	row = store.get_version(model_id, version_id)
	if not isinstance(row, dict):
		raise HTTPException(status_code=404, detail="version not found")
	return {"schemaVersion": 1, "modelId": model_id, "version": row}


@router.post("/{model_id}/versions/{version_id}/promote")
async def promote_model_version(
	model_id: str,
	version_id: str,
	req: ModelPromoteRequest,
	request: Request,
	x_model_admin: Optional[str] = Header(default=None),
):
	_require_admin_for_promotion(x_model_admin)
	store = _get_store(request)
	try:
		result = store.promote_version(
			model_id=model_id,
			version_id=version_id,
			to_stage=req.toStage,
			force=bool(req.force),
			promoted_by=req.promotedBy,
		)
	except LookupError:
		raise HTTPException(status_code=404, detail="version not found")
	except RuntimeError as ex:
		message = str(ex)
		if message.startswith("stage_conflict:"):
			parts = message.split(":")
			stage = parts[1] if len(parts) > 1 else "unknown"
			conflict = parts[2] if len(parts) > 2 else ""
			raise HTTPException(
				status_code=409,
				detail={
					"code": "STAGE_CONFLICT",
					"stage": stage,
					"conflictVersionId": conflict or None,
					"message": f"stage '{stage}' already has a version",
				},
			)
		raise HTTPException(status_code=409, detail=message)
	except ValueError as ex:
		raise HTTPException(status_code=400, detail=str(ex))
	return {"schemaVersion": 1, "modelId": model_id, "versionId": version_id, **result}
