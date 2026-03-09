from __future__ import annotations

import importlib
import os

import pytest


HF_CORE_MODULES = [
	"transformers",
	"datasets",
	"tokenizers",
	"safetensors",
	"huggingface_hub",
]


_RUN_SMOKE = str(os.getenv("RUN_HF_CORE_IMPORT_SMOKE", "")).strip().lower() in {"1", "true", "yes", "on"}


@pytest.mark.skipif(not _RUN_SMOKE, reason="Enable with RUN_HF_CORE_IMPORT_SMOKE=1")
@pytest.mark.parametrize("module_name", HF_CORE_MODULES)
def test_hf_core_import_smoke(module_name: str):
	mod = importlib.import_module(module_name)
	assert mod is not None
