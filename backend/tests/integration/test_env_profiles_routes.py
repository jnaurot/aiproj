from __future__ import annotations

from types import SimpleNamespace

from fastapi.testclient import TestClient

from app.main import app


def test_env_profiles_list_returns_schema_and_profiles():
	with TestClient(app) as client:
		res = client.get("/env/profiles")
		assert res.status_code == 200, res.text
		body = res.json()
		assert body.get("schemaVersion") == 1
		profiles = body.get("profiles")
		assert isinstance(profiles, list)
		assert any((p or {}).get("profileId") == "core" for p in profiles)


def test_env_profiles_validate_rejects_invalid_profile():
	with TestClient(app) as client:
		res = client.post("/env/profiles/validate", json={"profileId": "nope"})
		assert res.status_code == 422, res.text
		detail = res.json().get("detail") or {}
		assert detail.get("code") == "ENV_PROFILE_INVALID"


def test_env_profiles_validate_returns_deterministic_status(monkeypatch):
	from app.routes import env_profiles as mod

	monkeypatch.setattr(mod, "_missing_packages", lambda pkgs: ["numpy"] if "numpy" in pkgs else [])
	with TestClient(app) as client:
		res = client.post("/env/profiles/validate", json={"profileId": "core"})
		assert res.status_code == 200, res.text
		body = res.json()
		assert body.get("schemaVersion") == 1
		assert body.get("profileId") == "core"
		assert body.get("installed") is False
		assert body.get("health") == "missing"
		assert "numpy" in (body.get("missingPackages") or [])


def test_env_profiles_install_uses_runner_and_reports_success(monkeypatch):
	from app.routes import env_profiles as mod

	call_count = {"n": 0}

	def _missing_packages_two_stage(pkgs):
		call_count["n"] += 1
		# First check (before install) missing; second check (after install) installed.
		return list(pkgs) if call_count["n"] == 1 else []

	monkeypatch.setattr(mod, "_missing_packages", _missing_packages_two_stage)
	install_audit = SimpleNamespace(
		attempt_id="envinst_test",
		started_at="2026-03-09T00:00:00Z",
		finished_at="2026-03-09T00:00:01Z",
		command=["python", "-m", "pip", "install", "numpy"],
		requested=["numpy"],
		allowed=["numpy"],
		blocked=[],
		returncode=0,
		status="installed",
		duration_ms=1000,
		stdout_tail="ok",
		stderr_tail="",
	)
	async def _install_success(pkgs):
		return SimpleNamespace(installed=list(pkgs), audit=install_audit)

	monkeypatch.setattr(mod._INSTALLER, "install_packages", _install_success)

	with TestClient(app) as client:
		res = client.post("/env/profiles/install", json={"profileId": "core"})
		assert res.status_code == 200, res.text
		body = res.json()
		assert body.get("schemaVersion") == 1
		assert body.get("status") == "installed"
		assert body.get("installed") is True
		assert body.get("missingPackages") == []
		assert (body.get("audit") or {}).get("attemptId") == "envinst_test"


def test_env_profiles_install_reports_pip_failure(monkeypatch):
	from app.routes import env_profiles as mod
	from app.services.env_installer import EnvInstallError

	monkeypatch.setattr(mod, "_missing_packages", lambda pkgs: list(pkgs))
	failed_audit = SimpleNamespace(
		attempt_id="envinst_fail",
		started_at="2026-03-09T00:00:00Z",
		finished_at="2026-03-09T00:00:01Z",
		command=["python", "-m", "pip", "install", "numpy"],
		requested=["numpy"],
		allowed=["numpy"],
		blocked=[],
		returncode=1,
		status="failed",
		duration_ms=1000,
		stdout_tail="",
		stderr_tail="pip failed for test",
	)
	async def _raise_install_error(pkgs):
		raise EnvInstallError("ENV_PROFILE_INSTALL_FAILED", "pip install failed", failed_audit)  # type: ignore[arg-type]

	monkeypatch.setattr(
		mod,
		"_INSTALLER",
		SimpleNamespace(install_packages=_raise_install_error),
	)
	with TestClient(app) as client:
		res = client.post("/env/profiles/install", json={"profileId": "core"})
		assert res.status_code == 422, res.text
		detail = res.json().get("detail") or {}
		assert detail.get("code") == "ENV_PROFILE_INSTALL_FAILED"
		assert (detail.get("audit") or {}).get("attemptId") == "envinst_fail"


def test_env_vars_list_contains_supported_runtime_envs(monkeypatch, tmp_path):
	from app.services import runtime_env as env_mod

	monkeypatch.setattr(env_mod, "_OVERRIDES_PATH", tmp_path / "runtime_env_overrides.json")
	monkeypatch.setattr(env_mod, "_LOADED", False)
	monkeypatch.setattr(env_mod, "_OVERRIDES", {})
	with TestClient(app) as client:
		res = client.get("/env/vars")
		assert res.status_code == 200, res.text
		body = res.json()
		assert body.get("schemaVersion") == 1
		rows = body.get("vars") or []
		assert any((r or {}).get("name") == "RUNNER_MAX_CONCURRENCY" for r in rows)
		assert any((r or {}).get("name") == "ARTIFACT_STORE" for r in rows)
		assert any((r or {}).get("name") == "NODE_DOC_FEEDBACK_LLM_TIMEOUT_SECONDS" for r in rows)
		assert any((r or {}).get("name") == "NODE_DOC_EXPLAIN_LLM_TIMEOUT_SECONDS" for r in rows)


def test_env_vars_set_and_clear_override(monkeypatch, tmp_path):
	from app.services import runtime_env as env_mod

	monkeypatch.setattr(env_mod, "_OVERRIDES_PATH", tmp_path / "runtime_env_overrides.json")
	monkeypatch.setattr(env_mod, "_LOADED", False)
	monkeypatch.setattr(env_mod, "_OVERRIDES", {})
	with TestClient(app) as client:
		set_res = client.post(
			"/env/vars",
			json={"updates": [{"name": "RUNNER_MAX_CONCURRENCY", "value": "7"}]},
		)
		assert set_res.status_code == 200, set_res.text
		set_body = set_res.json()
		assert "RUNNER_MAX_CONCURRENCY" in (set_body.get("updated") or [])
		rows = set_body.get("vars") or []
		target = next((r for r in rows if (r or {}).get("name") == "RUNNER_MAX_CONCURRENCY"), {})
		assert target.get("value") == "7"
		assert target.get("source") == "override"

		clear_res = client.post(
			"/env/vars",
			json={"updates": [{"name": "RUNNER_MAX_CONCURRENCY", "unset": True}]},
		)
		assert clear_res.status_code == 200, clear_res.text
		clear_body = clear_res.json()
		rows2 = clear_body.get("vars") or []
		target2 = next((r for r in rows2 if (r or {}).get("name") == "RUNNER_MAX_CONCURRENCY"), {})
		assert target2.get("source") in {"default", "env", "unset"}


def test_env_vars_rejects_invalid_key(monkeypatch, tmp_path):
	from app.services import runtime_env as env_mod

	monkeypatch.setattr(env_mod, "_OVERRIDES_PATH", tmp_path / "runtime_env_overrides.json")
	monkeypatch.setattr(env_mod, "_LOADED", False)
	monkeypatch.setattr(env_mod, "_OVERRIDES", {})
	with TestClient(app) as client:
		res = client.post("/env/vars", json={"updates": [{"name": "bad-key", "value": "1"}]})
		assert res.status_code == 422, res.text
		detail = res.json().get("detail") or {}
		assert detail.get("code") == "ENV_VARS_UPDATE_FAILED"
