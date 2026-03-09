from __future__ import annotations

import re
import sys
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator

from ..executors.builtin_profiles import (
	BUILTIN_PROFILE_PACKAGES,
	missing_packages_for_packages,
	resolve_builtin_environment,
)
from ..services.env_installer import EnvInstallError, EnvInstallerService, InstallAudit

router = APIRouter()
_INSTALLER = EnvInstallerService()

_PACKAGE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._\-]*(?:[<>=!~].+)?$")


def _platform_notes_for_profile(profile_id: str) -> List[str]:
	notes: List[str] = []
	if profile_id in {"llm_finetune", "full"} and sys.platform.startswith("win"):
		notes.append("GPU packages may need CUDA-specific wheels on Windows.")
	return notes


def _normalize_custom_packages(value: Any) -> List[str]:
	if value is None:
		return []
	if not isinstance(value, list):
		raise ValueError("customPackages must be an array")
	out: List[str] = []
	seen: set[str] = set()
	for idx, raw in enumerate(value):
		if not isinstance(raw, str):
			raise ValueError(f"customPackages[{idx}] must be a string")
		item = raw.strip()
		if not item:
			raise ValueError(f"customPackages[{idx}] cannot be empty")
		if not _PACKAGE_NAME_RE.match(item):
			raise ValueError(f"customPackages[{idx}] has invalid package spec '{item}'")
		if item in seen:
			continue
		seen.add(item)
		out.append(item)
	return out


def _missing_packages(packages: List[str]) -> List[str]:
	return missing_packages_for_packages(packages)


class EnvValidateRequest(BaseModel):
	profileId: str = "core"
	customPackages: Optional[List[str]] = None

	@field_validator("profileId")
	@classmethod
	def _validate_profile_id(cls, value: str) -> str:
		profile_id = str(value or "").strip()
		if not profile_id:
			raise ValueError("profileId is required")
		return profile_id

	@field_validator("customPackages")
	@classmethod
	def _validate_custom_packages(cls, value: Optional[List[str]]) -> Optional[List[str]]:
		if value is None:
			return None
		return _normalize_custom_packages(value)


class EnvInstallRequest(EnvValidateRequest):
	pass


def _audit_to_payload(audit: InstallAudit) -> Dict[str, Any]:
	return {
		"attemptId": audit.attempt_id,
		"startedAt": audit.started_at,
		"finishedAt": audit.finished_at,
		"command": list(audit.command),
		"requested": list(audit.requested),
		"allowed": list(audit.allowed),
		"blocked": list(audit.blocked),
		"returncode": audit.returncode,
		"status": audit.status,
		"durationMs": audit.duration_ms,
		"stdoutTail": audit.stdout_tail,
		"stderrTail": audit.stderr_tail,
	}


def _resolve_request(profile_id: str, custom_packages: Optional[List[str]]) -> Dict[str, Any]:
	try:
		return resolve_builtin_environment(
			{
				"toolId": "noop",
				"profileId": profile_id,
				"customPackages": custom_packages or [],
			}
		)
	except ValueError as exc:
		raise HTTPException(
			status_code=422,
			detail={
				"code": "ENV_PROFILE_INVALID",
				"message": str(exc),
				"profileId": profile_id,
			},
		) from exc


def _profile_status(profile_id: str, packages: List[str]) -> Dict[str, Any]:
	missing = _missing_packages(packages)
	installed = len(missing) == 0
	return {
		"profileId": profile_id,
		"packages": packages,
		"installed": installed,
		"missingPackages": missing,
		"health": "ok" if installed else "missing",
		"platformNotes": _platform_notes_for_profile(profile_id),
	}


@router.get("/profiles")
async def list_env_profiles() -> Dict[str, Any]:
	profiles: List[Dict[str, Any]] = []
	for profile_id in BUILTIN_PROFILE_PACKAGES.keys():
		packages = list(BUILTIN_PROFILE_PACKAGES.get(profile_id, []))
		status = _profile_status(profile_id, packages)
		status["available"] = True
		profiles.append(status)
	return {
		"schemaVersion": 1,
		"profiles": profiles,
		"python": {
			"executable": sys.executable,
			"version": sys.version.split()[0],
		},
	}


@router.post("/profiles/validate")
async def validate_env_profile(req: EnvValidateRequest) -> Dict[str, Any]:
	resolved = _resolve_request(req.profileId, req.customPackages)
	packages = list(resolved.get("packages") or [])
	status = _profile_status(str(resolved.get("profileId") or req.profileId), packages)
	return {
		"schemaVersion": 1,
		"profileId": resolved.get("profileId"),
		"source": resolved.get("source"),
		"packages": packages,
		"installed": status["installed"],
		"missingPackages": status["missingPackages"],
		"health": status["health"],
		"platformNotes": _platform_notes_for_profile(str(resolved.get("profileId") or req.profileId)),
	}


@router.post("/profiles/install")
async def install_env_profile(req: EnvInstallRequest) -> Dict[str, Any]:
	resolved = _resolve_request(req.profileId, req.customPackages)
	profile_id = str(resolved.get("profileId") or req.profileId)
	packages = list(resolved.get("packages") or [])
	missing_before = _missing_packages(packages)
	if len(missing_before) == 0:
		return {
			"schemaVersion": 1,
			"profileId": profile_id,
			"source": resolved.get("source"),
			"packages": packages,
			"status": "already_installed",
			"installed": True,
			"missingPackages": [],
			"audit": None,
		}
	try:
		install_result = await _INSTALLER.install_packages(missing_before)
	except EnvInstallError as exc:
		audit_payload = _audit_to_payload(exc.audit)
		if exc.code == "ENV_PROFILE_PACKAGE_BLOCKED":
			raise HTTPException(
				status_code=422,
				detail={
					"code": "ENV_PROFILE_PACKAGE_BLOCKED",
					"profileId": profile_id,
					"message": str(exc),
					"blockedPackages": list(exc.audit.blocked),
					"audit": audit_payload,
				},
			) from exc
		raise HTTPException(
			status_code=422,
			detail={
				"code": "ENV_PROFILE_INSTALL_FAILED",
				"profileId": profile_id,
				"message": str(exc),
				"missingPackages": missing_before,
				"stderr": exc.audit.stderr_tail,
				"audit": audit_payload,
			},
		) from exc

	missing_after = _missing_packages(packages)
	return {
		"schemaVersion": 1,
		"profileId": profile_id,
		"source": resolved.get("source"),
		"packages": packages,
		"status": "installed" if len(missing_after) == 0 else "partial",
		"installed": len(missing_after) == 0,
		"missingPackages": missing_after,
		"audit": _audit_to_payload(install_result.audit),
	}
