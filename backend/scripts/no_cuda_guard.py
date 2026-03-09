from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
	sys.path.insert(0, str(ROOT))

from app.services.no_cuda_guard import (
	_format_violation,
	find_cuda_violations_in_installed_distributions,
	find_cuda_violations_in_lockfile,
	find_cuda_violations_in_specs,
)

LOCK_DIR = ROOT / "requirements" / "locks"


def _specs_from_pyproject() -> List[str]:
	try:
		import tomllib
	except Exception as exc:  # pragma: no cover
		raise RuntimeError(f"Unable to load tomllib: {exc}") from exc
	pyproject = ROOT / "pyproject.toml"
	data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
	specs: List[str] = []
	for item in data.get("project", {}).get("dependencies", []) or []:
		if isinstance(item, str):
			specs.append(item)
	groups = data.get("dependency-groups", {}) or {}
	if isinstance(groups, dict):
		for deps in groups.values():
			if isinstance(deps, list):
				for item in deps:
					if isinstance(item, str):
						specs.append(item)
	return specs


def main() -> int:
	parser = argparse.ArgumentParser(description="Fail if CUDA-linked packages are present.")
	parser.add_argument("--check-pyproject", action="store_true")
	parser.add_argument("--check-lockfiles", action="store_true")
	parser.add_argument("--check-installed", action="store_true")
	args = parser.parse_args()

	if not (args.check_pyproject or args.check_lockfiles or args.check_installed):
		args.check_pyproject = True
		args.check_lockfiles = True
		args.check_installed = True

	violations = []
	if args.check_pyproject:
		violations.extend(find_cuda_violations_in_specs(_specs_from_pyproject(), source="pyproject.toml"))
	if args.check_lockfiles:
		violations.extend(find_cuda_violations_in_lockfile(ROOT / "uv.lock"))
		for path in sorted(LOCK_DIR.glob("*.txt")):
			violations.extend(find_cuda_violations_in_lockfile(path))
	if args.check_installed:
		violations.extend(find_cuda_violations_in_installed_distributions())

	if violations:
		print("NO_CUDA_GUARD_VIOLATION")
		for item in violations[:60]:
			print(_format_violation(item))
		return 2

	print("NO_CUDA_GUARD_OK")
	return 0


if __name__ == "__main__":
	raise SystemExit(main())
