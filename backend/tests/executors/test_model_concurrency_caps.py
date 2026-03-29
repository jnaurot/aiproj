from app.executors import llm as llm_exec


def test_provider_cap_uses_default(monkeypatch):
	monkeypatch.setenv("RUNNER_MAX_MODEL_PROVIDER", "3")
	monkeypatch.delenv("RUNNER_MAX_MODEL_PROVIDER_OPENAI_COMPAT", raising=False)
	assert llm_exec._provider_cap_value("openai_compat") == 3


def test_provider_cap_prefers_specific_override(monkeypatch):
	monkeypatch.setenv("RUNNER_MAX_MODEL_PROVIDER", "3")
	monkeypatch.setenv("RUNNER_MAX_MODEL_PROVIDER_OPENAI_COMPAT", "1")
	assert llm_exec._provider_cap_value("openai_compat") == 1


def test_provider_cap_invalid_or_zero_disables(monkeypatch):
	monkeypatch.setenv("RUNNER_MAX_MODEL_PROVIDER_OPENAI_COMPAT", "0")
	assert llm_exec._provider_cap_value("openai_compat") == 0
	monkeypatch.setenv("RUNNER_MAX_MODEL_PROVIDER_OPENAI_COMPAT", "not-a-number")
	assert llm_exec._provider_cap_value("openai_compat") == 0


def test_provider_acquire_timeout_uses_default(monkeypatch):
	monkeypatch.setenv("RUNNER_MODEL_PROVIDER_ACQUIRE_TIMEOUT", "45")
	monkeypatch.delenv("RUNNER_MODEL_PROVIDER_ACQUIRE_TIMEOUT_OPENAI_COMPAT", raising=False)
	assert llm_exec._provider_acquire_timeout_seconds("openai_compat") == 45.0


def test_provider_acquire_timeout_prefers_specific_override(monkeypatch):
	monkeypatch.setenv("RUNNER_MODEL_PROVIDER_ACQUIRE_TIMEOUT", "45")
	monkeypatch.setenv("RUNNER_MODEL_PROVIDER_ACQUIRE_TIMEOUT_OPENAI_COMPAT", "12.5")
	assert llm_exec._provider_acquire_timeout_seconds("openai_compat") == 12.5


def test_provider_acquire_timeout_invalid_or_zero_disables(monkeypatch):
	monkeypatch.setenv("RUNNER_MODEL_PROVIDER_ACQUIRE_TIMEOUT_OPENAI_COMPAT", "0")
	assert llm_exec._provider_acquire_timeout_seconds("openai_compat") == 0.0
	monkeypatch.setenv("RUNNER_MODEL_PROVIDER_ACQUIRE_TIMEOUT_OPENAI_COMPAT", "not-a-number")
	assert llm_exec._provider_acquire_timeout_seconds("openai_compat") == 0.0
