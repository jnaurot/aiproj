from __future__ import annotations

import json
import os
from pathlib import Path


def _safe_read(path: Path) -> str:
	try:
		return path.read_text(encoding="utf-8", errors="ignore").strip()
	except Exception:
		return ""


def main() -> int:
	kfd = Path("/dev/kfd")
	dri = Path("/dev/dri")

	result: dict[str, object] = {
		"schemaVersion": 1,
		"rocm": {
			"kfdExists": kfd.exists(),
			"driExists": dri.exists(),
			"driRenderNodes": sorted(p.name for p in dri.glob("renderD*")) if dri.exists() else [],
		},
		"runtime": {
			"hipVisibleDevices": os.getenv("HIP_VISIBLE_DEVICES", ""),
			"hsaOverrideGfxVersion": os.getenv("HSA_OVERRIDE_GFX_VERSION", ""),
		},
	}

	version_path = Path("/opt/rocm/.info/version")
	ver = _safe_read(version_path)
	if ver:
		result["rocmVersion"] = ver

	ok = bool(result["rocm"]["kfdExists"]) and bool(result["rocm"]["driExists"])
	result["ok"] = ok
	print(json.dumps(result))
	return 0 if ok else 2


if __name__ == "__main__":
	raise SystemExit(main())
