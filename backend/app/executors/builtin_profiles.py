from __future__ import annotations

import importlib.util
import re
import sys
from typing import Any, Dict, List


BUILTIN_PROFILE_PACKAGES: Dict[str, List[str]] = {
    "core": ["numpy", "requests", "pydantic", "python-dateutil"],
    # Layer 1 data profile: ROCm/CUDA-neutral tabular stack.
    "data": ["numpy", "pandas", "polars", "pyarrow"],
    # Layer 1 classical ML profile: data stack + sklearn/scipy.
    "ml": ["numpy", "pandas", "polars", "scikit-learn", "scipy"],
    "llm_finetune": [
        "torch",
        "transformers",
        "datasets",
        "tokenizers",
        "safetensors",
        "huggingface_hub",
        "accelerate",
        "peft",
        "trl",
        "sentencepiece",
        "evaluate",
    ],
    "full": [
        "numpy",
        "requests",
        "pydantic",
        "python-dateutil",
        "polars",
        "pandas",
        "pyarrow",
        "openpyxl",
        "duckdb",
        "scikit-learn",
        "scipy",
        "xgboost",
        "lightgbm",
        "matplotlib",
        "seaborn",
        "torch",
        "transformers",
        "datasets",
        "tokenizers",
        "safetensors",
        "huggingface_hub",
        "accelerate",
        "peft",
        "trl",
        "sentencepiece",
        "evaluate",
    ],
    "custom": [],
}

BUILTIN_PROFILE_INSTALL_TARGETS: Dict[str, str] = {
    "core": "cpu_dev",
    "data": "cpu_dev",
    "ml": "cpu_dev",
    "llm_finetune": "rocm_train",
    "full": "rocm_train",
    "custom": "cpu_dev",
}

_PACKAGE_SPLIT_RE = re.compile(r"[<>=!~]")
_PACKAGE_MODULE_ALIASES: Dict[str, str] = {
    "python-dateutil": "dateutil",
    "scikit-learn": "sklearn",
}


def _normalize_custom_packages(value: Any) -> List[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("builtin.customPackages must be an array of strings")
    out: List[str] = []
    seen: set[str] = set()
    for idx, raw in enumerate(value):
        if not isinstance(raw, str):
            raise ValueError(f"builtin.customPackages[{idx}] must be a string")
        pkg = raw.strip()
        if not pkg:
            raise ValueError(f"builtin.customPackages[{idx}] cannot be empty")
        if pkg in seen:
            continue
        seen.add(pkg)
        out.append(pkg)
    return out


def resolve_builtin_environment(builtin_cfg: Dict[str, Any]) -> Dict[str, Any]:
    profile_id = str((builtin_cfg or {}).get("profileId") or "core").strip()
    if profile_id not in BUILTIN_PROFILE_PACKAGES:
        allowed = ", ".join(sorted(BUILTIN_PROFILE_PACKAGES.keys()))
        raise ValueError(f"Unsupported builtin.profileId '{profile_id}' (allowed: {allowed})")

    custom_packages = _normalize_custom_packages((builtin_cfg or {}).get("customPackages"))
    if profile_id != "custom" and custom_packages:
        raise ValueError("builtin.customPackages is only allowed when builtin.profileId='custom'")
    if profile_id == "custom" and not custom_packages:
        raise ValueError("builtin.customPackages must include at least one package for profileId='custom'")

    if profile_id == "custom":
        packages = custom_packages
        source = "custom"
    else:
        packages = list(BUILTIN_PROFILE_PACKAGES[profile_id])
        source = "profile"
    install_target = str(BUILTIN_PROFILE_INSTALL_TARGETS.get(profile_id) or "cpu_dev")

    locked_raw = (builtin_cfg or {}).get("locked")
    locked = str(locked_raw).strip() if isinstance(locked_raw, str) else ""

    resolved = {
        "profileId": profile_id,
        "packages": packages,
        "source": source,
        "installTarget": install_target,
    }
    if locked:
        resolved["locked"] = locked
    return resolved


def package_module_name(package_spec: str) -> str:
    base = _PACKAGE_SPLIT_RE.split(str(package_spec or "").strip(), maxsplit=1)[0].strip()
    lowered = base.lower()
    if lowered in _PACKAGE_MODULE_ALIASES:
        return _PACKAGE_MODULE_ALIASES[lowered]
    return lowered.replace("-", "_")


def missing_packages_for_packages(packages: List[str]) -> List[str]:
    missing: List[str] = []
    for pkg in packages or []:
        module_name = package_module_name(pkg)
        if not module_name:
            missing.append(str(pkg))
            continue
        try:
            found = importlib.util.find_spec(module_name) is not None
        except (ValueError, ImportError):
            # Test doubles can inject modules without __spec__; treat presence in
            # sys.modules as installed instead of failing resolution.
            found = module_name in sys.modules
        if not found:
            missing.append(str(pkg))
    return missing
