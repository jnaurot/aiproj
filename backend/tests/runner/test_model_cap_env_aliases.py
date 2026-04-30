from __future__ import annotations

from app.runner import run as run_mod


def _clear_model_cap_env(monkeypatch) -> None:
	monkeypatch.delenv("RUNNER_MAX_MODEL", raising=False)


def test_model_cap_uses_runner_max_model(monkeypatch) -> None:
	_clear_model_cap_env(monkeypatch)
	monkeypatch.setenv("RUNNER_MAX_MODEL", "7")
	value, source, notes = run_mod.__resolve_max_model_cap_for_test()
	assert value == 7
	assert source == "RUNNER_MAX_MODEL"
	assert notes == []


def test_model_cap_invalid_values_fall_back_to_default(monkeypatch) -> None:
	_clear_model_cap_env(monkeypatch)
	monkeypatch.setenv("RUNNER_MAX_MODEL", "abc")
	value, source, notes = run_mod.__resolve_max_model_cap_for_test()
	assert value == 2
	assert source == "default"
	assert any("ignored invalid RUNNER_MAX_MODEL" in note for note in notes)


def test_model_cap_non_positive_values_fall_back_to_default(monkeypatch) -> None:
	_clear_model_cap_env(monkeypatch)
	monkeypatch.setenv("RUNNER_MAX_MODEL", "0")
	value, source, notes = run_mod.__resolve_max_model_cap_for_test()
	assert value == 2
	assert source == "default"
	assert any("ignored non-positive RUNNER_MAX_MODEL" in note for note in notes)
