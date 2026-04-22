from __future__ import annotations

from app.runner import run as run_mod


def _clear_model_cap_env(monkeypatch) -> None:
	for key in ("RUNNER_MAX_MODEL", "RUNNER_MAX_LLM", "RUN_MAX_LLM", "TUNNER_MAX_MODEL"):
		monkeypatch.delenv(key, raising=False)


def test_model_cap_prefers_runner_max_model(monkeypatch) -> None:
	_clear_model_cap_env(monkeypatch)
	monkeypatch.setenv("RUNNER_MAX_MODEL", "7")
	monkeypatch.setenv("RUNNER_MAX_LLM", "5")
	monkeypatch.setenv("RUN_MAX_LLM", "4")
	monkeypatch.setenv("TUNNER_MAX_MODEL", "3")
	value, source, notes = run_mod.__resolve_max_model_cap_for_test()
	assert value == 7
	assert source == "RUNNER_MAX_MODEL"
	assert notes == []


def test_model_cap_falls_back_to_legacy_runner_max_llm(monkeypatch) -> None:
	_clear_model_cap_env(monkeypatch)
	monkeypatch.setenv("RUNNER_MAX_LLM", "5")
	value, source, notes = run_mod.__resolve_max_model_cap_for_test()
	assert value == 5
	assert source == "RUNNER_MAX_LLM"
	assert any("compatibility alias RUNNER_MAX_LLM" in note for note in notes)


def test_model_cap_accepts_run_max_llm_compat(monkeypatch) -> None:
	_clear_model_cap_env(monkeypatch)
	monkeypatch.setenv("RUN_MAX_LLM", "6")
	value, source, notes = run_mod.__resolve_max_model_cap_for_test()
	assert value == 6
	assert source == "RUN_MAX_LLM"
	assert any("compatibility alias RUN_MAX_LLM" in note for note in notes)


def test_model_cap_accepts_tunner_typo_compat(monkeypatch) -> None:
	_clear_model_cap_env(monkeypatch)
	monkeypatch.setenv("TUNNER_MAX_MODEL", "8")
	value, source, notes = run_mod.__resolve_max_model_cap_for_test()
	assert value == 8
	assert source == "TUNNER_MAX_MODEL"
	assert any("compatibility alias TUNNER_MAX_MODEL" in note for note in notes)


def test_model_cap_invalid_values_fall_back_to_default(monkeypatch) -> None:
	_clear_model_cap_env(monkeypatch)
	monkeypatch.setenv("RUNNER_MAX_MODEL", "abc")
	monkeypatch.setenv("RUNNER_MAX_LLM", "0")
	value, source, notes = run_mod.__resolve_max_model_cap_for_test()
	assert value == 2
	assert source == "default"
	assert any("ignored invalid RUNNER_MAX_MODEL" in note for note in notes)
	assert any("ignored non-positive RUNNER_MAX_LLM" in note for note in notes)
