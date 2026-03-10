from __future__ import annotations

import importlib.metadata as metadata
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List


_SPEC_SPLIT_RE = re.compile(r"[<>=!~\[\]]")
_CUDA_TOKEN_RE = re.compile(
	r"(?:^|[^a-z0-9])("
	r"nvidia-|pytorch-cuda|cuda|cudnn|cublas|cufft|curand|cusolver|cusparse|nccl|nvjitlink|nvtx|"
	r"cu11|cu12|cu121|cu122|cu123|cu124|cu125|cu126|cu128"
	r")(?:[^a-z0-9]|$)",
	re.IGNORECASE,
)


@dataclass(frozen=True)
class CudaViolation:
	source: str
	item: str
	reason: str


def _normalize_spec_base(spec: str) -> str:
	base = _SPEC_SPLIT_RE.split(str(spec or "").strip(), maxsplit=1)[0].strip().lower()
	return base.replace("_", "-")


def _contains_cuda_marker(value: str) -> bool:
	return bool(_CUDA_TOKEN_RE.search(str(value or "").lower()))


def find_cuda_violations_in_specs(specs: Iterable[str], source: str) -> List[CudaViolation]:
	out: List[CudaViolation] = []
	for raw in specs:
		spec = str(raw or "").strip()
		if not spec:
			continue
		base = _normalize_spec_base(spec)
		if _contains_cuda_marker(base) or _contains_cuda_marker(spec):
			out.append(CudaViolation(source=source, item=spec, reason="cuda-linked package spec"))
	return out


def _iter_requirements_from_lock(path: Path) -> Iterable[str]:
	for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
		raw = line.strip()
		if not raw or raw.startswith("#"):
			continue
		if raw.startswith("--"):
			continue
		if raw.startswith("-e "):
			continue
		yield raw


def find_cuda_violations_in_lockfile(path: Path) -> List[CudaViolation]:
	violations: List[CudaViolation] = []
	if not path.exists():
		return violations
	for spec in _iter_requirements_from_lock(path):
		violations.extend(find_cuda_violations_in_specs([spec], source=str(path)))
	return violations


def _is_effective_installed_requirement(req_text: str) -> bool:
	req = str(req_text or "").strip()
	if not req:
		return False
	try:
		from packaging.markers import default_environment
		from packaging.requirements import InvalidRequirement, Requirement
	except Exception:
		parts = req.split(";", maxsplit=1)
		if len(parts) < 2:
			return True
		marker = str(parts[1] or "").strip().lower()
		if not marker:
			return True
		if "extra" in marker and "==" in marker and '""' not in marker and "''" not in marker:
			return False
		return True
	try:
		parsed = Requirement(req)
	except InvalidRequirement:
		return True
	if parsed.marker is None:
		return True
	env = default_environment()
	env["extra"] = ""
	try:
		return bool(parsed.marker.evaluate(env))
	except Exception:
		return True


def find_cuda_violations_in_installed_distributions() -> List[CudaViolation]:
	violations: List[CudaViolation] = []
	for dist in metadata.distributions():
		name = str(dist.metadata.get("Name") or dist.metadata.get("Summary") or "").strip()
		version = str(dist.version or "").strip()
		label = f"{name}=={version}" if name and version else (name or version or "<unknown>")
		if _contains_cuda_marker(name):
			violations.append(
				CudaViolation(source="installed", item=label, reason="cuda-linked installed distribution")
			)
		for req in dist.requires or []:
			if not _is_effective_installed_requirement(req):
				continue
			if _contains_cuda_marker(req):
				violations.append(
					CudaViolation(
						source=f"installed:{name or '<unknown>'}",
						item=req,
						reason="cuda-linked dependency requirement",
					)
				)
	return violations


def _format_violation(v: CudaViolation) -> str:
	return f"[{v.source}] {v.item} ({v.reason})"


def ensure_no_cuda_or_raise(
	*,
	check_installed: bool = True,
	extra_lockfiles: Iterable[Path] | None = None,
) -> None:
	if str(os.getenv("NO_CUDA_GUARD_DISABLED", "")).strip().lower() in {"1", "true", "yes", "on"}:
		return

	violations: List[CudaViolation] = []
	for path in extra_lockfiles or []:
		violations.extend(find_cuda_violations_in_lockfile(path))
	if check_installed:
		violations.extend(find_cuda_violations_in_installed_distributions())
	if not violations:
		return
	msg = "\n".join(_format_violation(v) for v in violations[:30])
	raise RuntimeError(
		"NO_CUDA_GUARD_VIOLATION: CUDA-linked packages were detected.\n"
		f"{msg}\n"
		"Set NO_CUDA_GUARD_DISABLED=1 to bypass temporarily."
	)
