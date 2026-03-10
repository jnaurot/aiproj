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


@pytest.mark.asyncio
async def test_exec_tool_builtin_core_array_summary_stats():
	node = {
		"id": "n_builtin_core_array",
		"data": {
			"params": {
				"provider": "builtin",
				"builtin": {
					"toolId": "core.array.summary_stats",
					"profileId": "core",
					"args": {"values": [1, 2, 3, 4]},
				},
			}
		},
	}
	result = await exec_tool("run_core_arr", node, _ctx())
	assert result.status == "succeeded"
	assert isinstance(result.data, dict)
	payload = result.data.get("payload")
	assert isinstance(payload, dict)
	assert payload.get("count") == 4
	assert payload.get("min") == 1.0
	assert payload.get("max") == 4.0


@pytest.mark.asyncio
async def test_exec_tool_builtin_core_datetime_parse():
	node = {
		"id": "n_builtin_core_dt",
		"data": {
			"params": {
				"provider": "builtin",
				"builtin": {
					"toolId": "core.datetime.parse",
					"profileId": "core",
					"args": {"value": "2026-03-09T18:00:00-05:00"},
				},
			}
		},
	}
	result = await exec_tool("run_core_dt", node, _ctx())
	assert result.status == "succeeded"
	assert isinstance(result.data, dict)
	payload = result.data.get("payload")
	assert isinstance(payload, dict)
	assert str(payload.get("iso", "")).startswith("2026-03-09T18:00:00")


@pytest.mark.asyncio
async def test_exec_tool_builtin_core_schema_validate():
	node = {
		"id": "n_builtin_core_schema",
		"data": {
			"params": {
				"provider": "builtin",
				"builtin": {
					"toolId": "core.json.validate_schema",
					"profileId": "core",
					"args": {
						"payload": {"name": "alice", "age": 42},
						"fields": {
							"name": {"type": "string", "required": True},
							"age": {"type": "integer", "required": True},
						},
					},
				},
			}
		},
	}
	result = await exec_tool("run_core_schema", node, _ctx())
	assert result.status == "succeeded"
	assert isinstance(result.data, dict)
	payload = result.data.get("payload")
	assert isinstance(payload, dict)
	assert payload.get("valid") is True
	assert isinstance(payload.get("value"), dict)
	assert payload.get("value", {}).get("age") == 42


@pytest.mark.asyncio
async def test_exec_tool_builtin_core_http_requires_net_permission():
	node = {
		"id": "n_builtin_core_http_perm",
		"data": {
			"params": {
				"provider": "builtin",
				"permissions": {"net": False},
				"builtin": {
					"toolId": "core.http.request_json",
					"profileId": "core",
					"args": {"url": "https://example.com"},
				},
			}
		},
	}
	result = await exec_tool("run_core_http_perm", node, _ctx())
	assert result.status == "failed"
	assert "permissions.net" in (result.error or "")


@pytest.mark.asyncio
async def test_exec_tool_builtin_core_http_json_success(monkeypatch):
	class _Resp:
		status_code = 200
		ok = True
		url = "https://example.com"
		reason = "OK"
		headers = {"content-type": "application/json"}

		@staticmethod
		def json():
			return {"ok": True, "n": 1}

		text = '{"ok": true, "n": 1}'

	def _fake_request(*args, **kwargs):
		return _Resp()

	monkeypatch.setattr("requests.request", _fake_request)

	node = {
		"id": "n_builtin_core_http_ok",
		"data": {
			"params": {
				"provider": "builtin",
				"permissions": {"net": True},
				"builtin": {
					"toolId": "core.http.request_json",
					"profileId": "core",
					"args": {"url": "https://example.com", "method": "GET"},
				},
			}
		},
	}
	result = await exec_tool("run_core_http_ok", node, _ctx())
	assert result.status == "succeeded"
	assert isinstance(result.data, dict)
	payload = result.data.get("payload")
	assert isinstance(payload, dict)
	assert payload.get("status_code") == 200
	assert payload.get("payload", {}).get("ok") is True
