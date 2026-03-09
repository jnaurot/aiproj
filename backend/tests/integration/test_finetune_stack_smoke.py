from __future__ import annotations

import importlib
import os

import pytest


FINETUNE_MODULES = [
	"accelerate",
	"peft",
	"trl",
]

_RUN_SMOKE = str(os.getenv("RUN_FINETUNE_STACK_SMOKE", "")).strip().lower() in {
	"1",
	"true",
	"yes",
	"on",
}


@pytest.mark.skipif(not _RUN_SMOKE, reason="Enable with RUN_FINETUNE_STACK_SMOKE=1")
@pytest.mark.parametrize("module_name", FINETUNE_MODULES)
def test_finetune_stack_import_smoke(module_name: str):
	mod = importlib.import_module(module_name)
	assert mod is not None


@pytest.mark.skipif(not _RUN_SMOKE, reason="Enable with RUN_FINETUNE_STACK_SMOKE=1")
def test_finetune_stack_minimal_dry_run_config():
	from peft import LoraConfig, TaskType
	from trl import SFTConfig

	lora = LoraConfig(
		r=4,
		lora_alpha=8,
		target_modules=["q_proj"],
		lora_dropout=0.05,
		task_type=TaskType.CAUSAL_LM,
	)
	assert lora.r == 4
	assert lora.task_type == TaskType.CAUSAL_LM

	sft = SFTConfig(
		output_dir="./.tmp-smoke-sft",
		max_steps=1,
		per_device_train_batch_size=1,
		logging_steps=1,
		report_to=[],
	)
	assert sft.max_steps == 1
	assert sft.per_device_train_batch_size == 1
