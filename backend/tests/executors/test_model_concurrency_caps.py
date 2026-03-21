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
