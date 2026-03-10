import base64
import io
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
async def test_exec_tool_builtin_ml_sklearn_train_classifier():
	pytest.importorskip("sklearn")
	node = {
		"id": "n_builtin_ml_train_cls",
		"data": {
			"params": {
				"provider": "builtin",
				"builtin": {
					"toolId": "ml.sklearn.train_classifier",
					"profileId": "ml",
					"args": {
						"rows": [
							{"x1": 0.1, "x2": 1.1, "label": "A"},
							{"x1": 0.2, "x2": 1.0, "label": "A"},
							{"x1": 1.2, "x2": 0.1, "label": "B"},
							{"x1": 1.1, "x2": 0.2, "label": "B"},
						],
						"label_col": "label",
						"feature_cols": ["x1", "x2"],
						"max_iter": 200,
					},
				},
			}
		},
	}
	result = await exec_tool("run_ml_train_cls", node, _ctx())
	assert result.status == "succeeded"
	payload = (result.data or {}).get("payload")
	assert isinstance(payload, dict)
	assert payload.get("task") == "classification"
	assert payload.get("model") == "LogisticRegression"
	assert isinstance(payload.get("metrics_train"), dict)
	assert "accuracy" in payload.get("metrics_train", {})
	assert isinstance(payload.get("model_spec"), dict)
	model_package = payload.get("model_package")
	assert isinstance(model_package, dict)
	assert model_package.get("format") == "aip.model_package.v1"
	files = model_package.get("files") or {}
	assert isinstance(files, dict)
	assert {"model.bin", "signature.json", "env_lock.json"}.issubset(set(files.keys()))
	model_bin = files.get("model.bin") or {}
	assert isinstance(model_bin, dict)
	model_bytes = base64.b64decode(str(model_bin.get("content_b64") or "").encode("ascii"), validate=False)
	assert len(model_bytes) > 0
	signature_file = files.get("signature.json") or {}
	assert isinstance((signature_file.get("content") or {}), dict)
	assert ((signature_file.get("content") or {}).get("format")) == "aip.model_signature.v1"
	analysis_artifacts = payload.get("analysis_artifacts") or []
	assert isinstance(analysis_artifacts, list)
	artifact_names = {str(a.get("name")) for a in analysis_artifacts if isinstance(a, dict)}
	assert {"feature_importance", "confusion_matrix", "calibration"}.issubset(artifact_names)
	feature_importance = next(
		(a for a in analysis_artifacts if isinstance(a, dict) and a.get("name") == "feature_importance"),
		None,
	)
	assert isinstance(feature_importance, dict)
	assert ((feature_importance.get("typed_schema") or {}).get("type")) == "table"
	fi_rows = feature_importance.get("rows") or []
	assert isinstance(fi_rows, list)
	assert fi_rows and "feature" in fi_rows[0] and "importance" in fi_rows[0]


@pytest.mark.asyncio
async def test_exec_tool_builtin_ml_sklearn_train_regressor():
	pytest.importorskip("sklearn")
	node = {
		"id": "n_builtin_ml_train_reg",
		"data": {
			"params": {
				"provider": "builtin",
				"builtin": {
					"toolId": "ml.sklearn.train_regressor",
					"profileId": "ml",
					"args": {
						"rows": [
							{"x1": 1, "x2": 2, "y": 5},
							{"x1": 2, "x2": 1, "y": 5},
							{"x1": 3, "x2": 4, "y": 11},
							{"x1": 4, "x2": 3, "y": 11},
						],
						"label_col": "y",
						"feature_cols": ["x1", "x2"],
					},
				},
			}
		},
	}
	result = await exec_tool("run_ml_train_reg", node, _ctx())
	assert result.status == "succeeded"
	payload = (result.data or {}).get("payload")
	assert isinstance(payload, dict)
	assert payload.get("task") == "regression"
	assert payload.get("model") == "LinearRegression"
	assert isinstance(payload.get("metrics_train"), dict)
	assert "rmse" in payload.get("metrics_train", {})
	assert isinstance(payload.get("model_spec"), dict)
	model_package = payload.get("model_package")
	assert isinstance(model_package, dict)
	files = model_package.get("files") or {}
	assert isinstance(files, dict)
	assert {"model.bin", "signature.json", "env_lock.json"}.issubset(set(files.keys()))
	signature_file = files.get("signature.json") or {}
	assert isinstance(signature_file, dict)
	signature_content = signature_file.get("content") or {}
	assert isinstance(signature_content, dict)
	assert signature_content.get("task") == "regression"
	analysis_artifacts = payload.get("analysis_artifacts") or []
	assert isinstance(analysis_artifacts, list)
	artifact_names = {str(a.get("name")) for a in analysis_artifacts if isinstance(a, dict)}
	assert {"feature_importance", "residuals"}.issubset(artifact_names)
	residuals = next((a for a in analysis_artifacts if isinstance(a, dict) and a.get("name") == "residuals"), None)
	assert isinstance(residuals, dict)
	assert ((residuals.get("typed_schema") or {}).get("type")) == "table"
	res_rows = residuals.get("rows") or []
	assert isinstance(res_rows, list)
	assert res_rows and "residual" in res_rows[0]


@pytest.mark.asyncio
async def test_exec_tool_builtin_ml_sklearn_cross_validate():
	pytest.importorskip("sklearn")
	node = {
		"id": "n_builtin_ml_cv_cls",
		"data": {
			"params": {
				"provider": "builtin",
				"builtin": {
					"toolId": "ml.sklearn.cross_validate",
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
						"task": "classification",
						"label_col": "label",
						"feature_cols": ["x1", "x2"],
						"cv": 3,
					},
				},
			}
		},
	}
	result = await exec_tool("run_ml_cv_cls", node, _ctx())
	assert result.status == "succeeded"
	payload = (result.data or {}).get("payload")
	assert isinstance(payload, dict)
	assert payload.get("task") == "classification"
	metrics = payload.get("metrics_cv") or {}
	assert isinstance(metrics, dict)
	assert "accuracy" in metrics
	assert isinstance((metrics.get("accuracy") or {}).get("fold_values"), list)


@pytest.mark.asyncio
async def test_exec_tool_builtin_ml_sklearn_evaluate():
	pytest.importorskip("sklearn")
	node = {
		"id": "n_builtin_ml_eval_cls",
		"data": {
			"params": {
				"provider": "builtin",
				"builtin": {
					"toolId": "ml.sklearn.evaluate",
					"profileId": "ml",
					"args": {
						"rows": [
							{"label": "A", "prediction": "A"},
							{"label": "A", "prediction": "B"},
							{"label": "B", "prediction": "B"},
							{"label": "B", "prediction": "B"},
						],
						"task": "classification",
						"label_col": "label",
						"pred_col": "prediction",
					},
				},
			}
		},
	}
	result = await exec_tool("run_ml_eval_cls", node, _ctx())
	assert result.status == "succeeded"
	payload = (result.data or {}).get("payload")
	assert isinstance(payload, dict)
	assert payload.get("task") == "classification"
	assert isinstance(payload.get("metrics"), dict)
	assert "f1" in payload.get("metrics", {})
	analysis_artifacts = payload.get("analysis_artifacts") or []
	assert isinstance(analysis_artifacts, list)
	artifact_names = {str(a.get("name")) for a in analysis_artifacts if isinstance(a, dict)}
	assert "confusion_matrix" in artifact_names


@pytest.mark.asyncio
async def test_exec_tool_builtin_ml_sklearn_evaluate_with_calibration():
	pytest.importorskip("sklearn")
	node = {
		"id": "n_builtin_ml_eval_cls_calib",
		"data": {
			"params": {
				"provider": "builtin",
				"builtin": {
					"toolId": "ml.sklearn.evaluate",
					"profileId": "ml",
					"args": {
						"rows": [
							{"label": "A", "prediction": "A", "pred_proba": 0.82},
							{"label": "A", "prediction": "B", "pred_proba": 0.41},
							{"label": "B", "prediction": "B", "pred_proba": 0.77},
							{"label": "B", "prediction": "B", "pred_proba": 0.93},
						],
						"task": "classification",
						"label_col": "label",
						"pred_col": "prediction",
						"proba_col": "pred_proba",
						"calibration_bins": 4,
					},
				},
			}
		},
	}
	result = await exec_tool("run_ml_eval_cls_calib", node, _ctx())
	assert result.status == "succeeded"
	payload = (result.data or {}).get("payload")
	assert isinstance(payload, dict)
	analysis_artifacts = payload.get("analysis_artifacts") or []
	assert isinstance(analysis_artifacts, list)
	artifact_names = {str(a.get("name")) for a in analysis_artifacts if isinstance(a, dict)}
	assert {"confusion_matrix", "calibration"}.issubset(artifact_names)
	calibration = next((a for a in analysis_artifacts if isinstance(a, dict) and a.get("name") == "calibration"), None)
	assert isinstance(calibration, dict)
	calib_rows = calibration.get("rows") or []
	assert isinstance(calib_rows, list)
	assert len(calib_rows) == 4


@pytest.mark.asyncio
async def test_exec_tool_builtin_ml_sklearn_package_predict_roundtrip():
	pytest.importorskip("sklearn")
	pytest.importorskip("joblib")
	train_node = {
		"id": "n_builtin_ml_train_pkg_cls",
		"data": {
			"params": {
				"provider": "builtin",
				"builtin": {
					"toolId": "ml.sklearn.train_classifier",
					"profileId": "ml",
					"args": {
						"rows": [
							{"x1": 0.1, "x2": 1.1, "label": "A"},
							{"x1": 0.2, "x2": 1.0, "label": "A"},
							{"x1": 1.2, "x2": 0.1, "label": "B"},
							{"x1": 1.1, "x2": 0.2, "label": "B"},
						],
						"label_col": "label",
						"feature_cols": ["x1", "x2"],
					},
				},
			}
		},
	}
	train_result = await exec_tool("run_ml_train_pkg_cls", train_node, _ctx())
	assert train_result.status == "succeeded"
	train_payload = (train_result.data or {}).get("payload")
	assert isinstance(train_payload, dict)
	model_package = train_payload.get("model_package")
	assert isinstance(model_package, dict)

	files = model_package.get("files") or {}
	model_bin = files.get("model.bin") or {}
	model_bytes = base64.b64decode(str(model_bin.get("content_b64") or "").encode("ascii"), validate=False)
	assert len(model_bytes) > 0
	assert io.BytesIO(model_bytes).getbuffer().nbytes > 0

	predict_node = {
		"id": "n_builtin_ml_predict_pkg_cls",
		"data": {
			"params": {
				"provider": "builtin",
				"builtin": {
					"toolId": "ml.sklearn.package_predict",
					"profileId": "ml",
					"args": {
						"rows": [
							{"x1": 0.12, "x2": 1.05},
							{"x1": 1.18, "x2": 0.12},
						],
						"model_package": model_package,
					},
				},
			}
		},
	}
	predict_result = await exec_tool("run_ml_predict_pkg_cls", predict_node, _ctx())
	assert predict_result.status == "succeeded"
	predict_payload = (predict_result.data or {}).get("payload")
	assert isinstance(predict_payload, dict)
	assert predict_payload.get("task") == "classification"
	predictions = predict_payload.get("predictions") or []
	assert isinstance(predictions, list)
	assert len(predictions) == 2
	assert all("prediction" in row for row in predictions if isinstance(row, dict))


@pytest.mark.asyncio
async def test_exec_tool_builtin_ml_sklearn_package_predict_signature_mismatch_fails():
	pytest.importorskip("sklearn")
	train_node = {
		"id": "n_builtin_ml_train_pkg_reg",
		"data": {
			"params": {
				"provider": "builtin",
				"builtin": {
					"toolId": "ml.sklearn.train_regressor",
					"profileId": "ml",
					"args": {
						"rows": [
							{"x1": 1, "x2": 2, "y": 5},
							{"x1": 2, "x2": 1, "y": 5},
							{"x1": 3, "x2": 4, "y": 11},
							{"x1": 4, "x2": 3, "y": 11},
						],
						"label_col": "y",
						"feature_cols": ["x1", "x2"],
					},
				},
			}
		},
	}
	train_result = await exec_tool("run_ml_train_pkg_reg", train_node, _ctx())
	assert train_result.status == "succeeded"
	train_payload = (train_result.data or {}).get("payload")
	assert isinstance(train_payload, dict)
	model_package = train_payload.get("model_package")
	assert isinstance(model_package, dict)

	predict_node = {
		"id": "n_builtin_ml_predict_pkg_reg_fail",
		"data": {
			"params": {
				"provider": "builtin",
				"builtin": {
					"toolId": "ml.sklearn.package_predict",
					"profileId": "ml",
					"args": {
						"rows": [
							{"x1": 1.5},
						],
						"model_package": model_package,
					},
				},
			}
		},
	}
	predict_result = await exec_tool("run_ml_predict_pkg_reg_fail", predict_node, _ctx())
	assert predict_result.status == "failed"
	assert "signature mismatch" in str(predict_result.error or "").lower()


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
