from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Dict

import pytest

from app.runner.artifacts import Artifact
from app.runner.run import (
	_build_non_work_injection_values,
	_collect_placeholder_tokens,
	_inject_placeholders,
)


def _artifact(
	artifact_id: str,
	*,
	mime_type: str,
	payload_schema: Dict[str, Any],
	payload_type: str | None = None,
) -> Artifact:
	return Artifact(
		artifact_id=artifact_id,
		node_kind="source",
		params_hash="p",
		upstream_ids=[],
		created_at=datetime.now(timezone.utc),
		execution_version="v1",
		mime_type=mime_type,
		payload_type=payload_type,
		size_bytes=1,
		storage_uri=f"memory://{artifact_id}",
		payload_schema=payload_schema,
	)


def test_collect_placeholder_tokens_scans_nested_params() -> None:
	params = {
		"user_prompt": "Use {input} + {param_context} + {param_filters}",
		"notes": ["ignore", "{control_in}", {"x": "raw {not_connected}"}],
	}
	assert _collect_placeholder_tokens(params) == {
		"input",
		"param_context",
		"param_filters",
		"control_in",
		"not_connected",
	}


def test_inject_placeholders_handles_exact_and_inline_tokens() -> None:
	params = {
		"prompt": "A:{param_context} B:{param_filters} C:{control_in}",
		"schema": "{param_filters}",
		"control": "{control_in}",
	}
	injected = {
		"param_context": "resume text",
		"param_filters": {"projects": ["rag", "lora"]},
		"control_in": {"signal": "go"},
	}
	out = _inject_placeholders(params, injected)
	assert out["prompt"] == 'A:resume text B:{"projects": ["rag", "lora"]} C:{"signal": "go"}'
	assert out["schema"] == {"projects": ["rag", "lora"]}
	assert out["control"] == {"signal": "go"}


class _FakeArtifactStore:
	def __init__(self, artifacts: Dict[str, Artifact], payloads: Dict[str, bytes]) -> None:
		self._artifacts = artifacts
		self._payloads = payloads

	async def get(self, artifact_id: str) -> Artifact:
		return self._artifacts[artifact_id]

	async def read(self, artifact_id: str) -> bytes:
		return self._payloads[artifact_id]


@pytest.mark.asyncio
async def test_build_non_work_injection_values_materializes_and_groups() -> None:
	artifacts = {
		"a_ctx": _artifact("a_ctx", mime_type="text/plain", payload_schema={"type": "text"}),
		"a_filter_1": _artifact("a_filter_1", mime_type="application/json", payload_schema={"type": "json"}),
		"a_filter_2": _artifact("a_filter_2", mime_type="application/json", payload_schema={"type": "json"}),
		"a_ctl": _artifact("a_ctl", mime_type="application/json", payload_schema={"type": "json"}),
	}
	payloads = {
		"a_ctx": b"James Naurot resume",
		"a_filter_1": b'{"project":"rag"}',
		"a_filter_2": b'{"project":"lora"}',
		"a_ctl": b'{"signal":"continue"}',
	}
	context = SimpleNamespace(artifact_store=_FakeArtifactStore(artifacts, payloads))
	input_refs = [
		("in", "a_ctx"),
		("param_context", "a_ctx"),
		("param_filters", "a_filter_1"),
		("param_filters", "a_filter_2"),
		("control_in", "a_ctl"),
	]

	out = await _build_non_work_injection_values(
		context,
		input_refs,
		handles_to_inject={"param_context", "param_filters", "control_in"},
	)

	assert out["param_context"] == "James Naurot resume"
	assert out["control_in"] == {"signal": "continue"}
	assert out["param_filters"] == [{"project": "rag"}, {"project": "lora"}]
