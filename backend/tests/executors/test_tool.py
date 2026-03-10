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


@pytest.mark.asyncio
async def test_exec_tool_builtin_data_pandas_profile():
	node = {
		"id": "n_builtin_data_pd_profile",
		"data": {
			"params": {
				"provider": "builtin",
				"builtin": {
					"toolId": "data.pandas.profile",
					"profileId": "data",
					"args": {
						"rows": [{"id": 1, "city": "Boston"}, {"id": 2, "city": "Austin"}],
						"sample_size": 2,
					},
				},
			}
		},
	}
	result = await exec_tool("run_data_pd_profile", node, _ctx())
	assert result.status == "succeeded"
	payload = (result.data or {}).get("payload")
	assert isinstance(payload, dict)
	assert payload.get("row_count") == 2
	assert "city" in (payload.get("columns") or [])


@pytest.mark.asyncio
async def test_exec_tool_builtin_data_pandas_select_columns():
	node = {
		"id": "n_builtin_data_pd_select",
		"data": {
			"params": {
				"provider": "builtin",
				"builtin": {
					"toolId": "data.pandas.select_columns",
					"profileId": "data",
					"args": {
						"rows": [{"id": 1, "city": "Boston", "score": 0.8}],
						"columns": ["id", "score"],
					},
				},
			}
		},
	}
	result = await exec_tool("run_data_pd_select", node, _ctx())
	assert result.status == "succeeded"
	payload = (result.data or {}).get("payload")
	assert isinstance(payload, dict)
	assert payload.get("columns") == ["id", "score"]
	rows = payload.get("rows") or []
	assert isinstance(rows, list)
	assert rows and "city" not in rows[0]


@pytest.mark.asyncio
async def test_exec_tool_builtin_data_polars_profile():
	pytest.importorskip("polars")
	node = {
		"id": "n_builtin_data_pl_profile",
		"data": {
			"params": {
				"provider": "builtin",
				"builtin": {
					"toolId": "data.polars.profile",
					"profileId": "data",
					"args": {
						"rows": [{"id": 1, "city": "Boston"}, {"id": 2, "city": None}],
						"sample_size": 2,
					},
				},
			}
		},
	}
	result = await exec_tool("run_data_pl_profile", node, _ctx())
	assert result.status == "succeeded"
	payload = (result.data or {}).get("payload")
	assert isinstance(payload, dict)
	assert payload.get("row_count") == 2
	assert "city" in (payload.get("null_count") or {})


@pytest.mark.asyncio
async def test_exec_tool_builtin_data_pyarrow_schema():
	pytest.importorskip("pyarrow")
	node = {
		"id": "n_builtin_data_pa_schema",
		"data": {
			"params": {
				"provider": "builtin",
				"builtin": {
					"toolId": "data.pyarrow.schema",
					"profileId": "data",
					"args": {
						"rows": [{"id": 1, "city": "Boston"}, {"id": 2, "city": "Austin"}],
					},
				},
			}
		},
	}
	result = await exec_tool("run_data_pa_schema", node, _ctx())
	assert result.status == "succeeded"
	payload = (result.data or {}).get("payload")
	assert isinstance(payload, dict)
	fields = payload.get("fields") or []
	assert isinstance(fields, list)
	assert any(isinstance(f, dict) and f.get("name") == "city" for f in fields)


@pytest.mark.asyncio
async def test_exec_tool_builtin_ml_sklearn_classification_report():
	pytest.importorskip("sklearn")
	node = {
		"id": "n_builtin_ml_cls",
		"data": {
			"params": {
				"provider": "builtin",
				"builtin": {
					"toolId": "ml.sklearn.classification_report",
					"profileId": "ml",
					"args": {
						"rows": [
							{"x1": 0.1, "x2": 1.1, "label": "A"},
							{"x1": 0.2, "x2": 1.0, "label": "A"},
							{"x1": 1.2, "x2": 0.1, "label": "B"},
							{"x1": 1.1, "x2": 0.2, "label": "B"},
							{"x1": 0.15, "x2": 1.05, "label": "A"},
							{"x1": 1.15, "x2": 0.15, "label": "B"},
						],
						"label_col": "label",
						"feature_cols": ["x1", "x2"],
						"test_size": 0.33,
						"random_state": 42,
					},
				},
			}
		},
	}
	result = await exec_tool("run_ml_cls", node, _ctx())
	assert result.status == "succeeded"
	payload = (result.data or {}).get("payload")
	assert isinstance(payload, dict)
	assert payload.get("model") == "LogisticRegression"
	assert isinstance(payload.get("metrics"), dict)
	assert "accuracy" in payload.get("metrics", {})


@pytest.mark.asyncio
async def test_exec_tool_builtin_ml_sklearn_regression_report():
	pytest.importorskip("sklearn")
	node = {
		"id": "n_builtin_ml_reg",
		"data": {
			"params": {
				"provider": "builtin",
				"builtin": {
					"toolId": "ml.sklearn.regression_report",
					"profileId": "ml",
					"args": {
						"rows": [
							{"x1": 1, "x2": 2, "y": 5},
							{"x1": 2, "x2": 1, "y": 5},
							{"x1": 3, "x2": 4, "y": 11},
							{"x1": 4, "x2": 3, "y": 11},
							{"x1": 5, "x2": 6, "y": 17},
							{"x1": 6, "x2": 5, "y": 17},
						],
						"label_col": "y",
						"feature_cols": ["x1", "x2"],
						"test_size": 0.33,
						"random_state": 42,
					},
				},
			}
		},
	}
	result = await exec_tool("run_ml_reg", node, _ctx())
	assert result.status == "succeeded"
	payload = (result.data or {}).get("payload")
	assert isinstance(payload, dict)
	assert payload.get("model") == "LinearRegression"
	assert isinstance(payload.get("metrics"), dict)
	assert "rmse" in payload.get("metrics", {})


@pytest.mark.asyncio
async def test_exec_tool_builtin_ml_scipy_describe():
	pytest.importorskip("scipy")
	node = {
		"id": "n_builtin_ml_scipy",
		"data": {
			"params": {
				"provider": "builtin",
				"builtin": {
					"toolId": "ml.scipy.describe",
					"profileId": "ml",
					"args": {
						"rows": [{"a": 1, "b": 10}, {"a": 2, "b": 20}, {"a": 3, "b": 30}, {"a": 4, "b": 40}],
						"numeric_cols": ["a", "b"],
					},
				},
			}
		},
	}
	result = await exec_tool("run_ml_scipy", node, _ctx())
	assert result.status == "succeeded"
	payload = (result.data or {}).get("payload")
	assert isinstance(payload, dict)
	summary = payload.get("summary") or {}
	assert isinstance(summary, dict)
	assert "a" in summary
	assert "mean" in (summary.get("a") or {})
