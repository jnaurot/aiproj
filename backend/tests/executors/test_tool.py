from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.executors.tool import exec_tool, iso_now
from app.runner.metadata import NodeOutput


def _ctx():
	artifact_store = SimpleNamespace(
		get=AsyncMock(side_effect=RuntimeError("no artifacts")),
		read=AsyncMock(side_effect=RuntimeError("no artifacts")),
	)
	return SimpleNamespace(bus=SimpleNamespace(emit=AsyncMock()), artifact_store=artifact_store, graph_id="graph_test")


@pytest.mark.asyncio
async def test_iso_now_format():
	value = iso_now()
	assert isinstance(value, str)
	assert "T" in value


@pytest.mark.asyncio
async def test_exec_tool_unsupported_provider_fails():
	node = {"id": "n1", "data": {"params": {"provider": "unsupported"}}}
	result = await exec_tool("run_1", node, _ctx())
	assert isinstance(result, NodeOutput)
	assert result.status == "failed"
	assert "unsupported tool provider" in (result.error or "").lower()


@pytest.mark.asyncio
async def test_exec_tool_python_success():
	node = {
		"id": "n_py",
		"data": {"params": {"provider": "python", "python": {"code": "result = {'ok': True}"}}},
	}
	result = await exec_tool("run_2", node, _ctx())
	assert result.status == "succeeded"
	assert isinstance(result.data, dict)
	assert result.data.get("kind") == "json"


@pytest.mark.asyncio
async def test_exec_tool_python_error_returns_failed():
	node = {
		"id": "n_py_fail",
		"data": {"params": {"provider": "python", "python": {"code": "raise ValueError('boom')"}}},
	}
	result = await exec_tool("run_3", node, _ctx())
	assert result.status == "failed"
	assert "failed" in (result.error or "").lower()


@pytest.mark.asyncio
async def test_exec_tool_api_missing_url_fails():
	node = {"id": "n_api", "data": {"params": {"provider": "api", "url": ""}}}
	result = await exec_tool("run_4", node, _ctx())
	assert result.status == "failed"
	assert "url" in (result.error or "").lower()


@pytest.mark.asyncio
async def test_exec_tool_builtin_profile_resolves_environment():
	node = {
		"id": "n_builtin_ok",
		"data": {
			"params": {
				"provider": "builtin",
				"builtin": {"toolId": "noop", "profileId": "data"},
			}
		},
	}
	result = await exec_tool("run_5", node, _ctx())
	assert result.status == "succeeded"
	assert isinstance(result.data, dict)
	meta = result.data.get("meta") if isinstance(result.data, dict) else {}
	assert isinstance(meta, dict)
	env = meta.get("builtin_environment")
	assert isinstance(env, dict)
	assert env.get("profileId") == "data"
	assert "polars" in (env.get("packages") or [])


@pytest.mark.asyncio
async def test_exec_tool_builtin_invalid_profile_fails():
	node = {
		"id": "n_builtin_bad",
		"data": {
			"params": {
				"provider": "builtin",
				"builtin": {"toolId": "noop", "profileId": "bad_profile"},
			}
		},
	}
	result = await exec_tool("run_6", node, _ctx())
	assert result.status == "failed"
	assert "profileid" in (result.error or "").lower()
