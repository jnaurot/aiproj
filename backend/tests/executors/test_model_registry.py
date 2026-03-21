import pytest

from app.executors.model_registry import resolve_model_connection
from app.runner.schemas import LLMParams


def _params(**overrides) -> LLMParams:
	base = {
		"model": "demo-model",
		"user_prompt": "hello",
		"base_url": "http://fallback.local",
	}
	base.update(overrides)
	return LLMParams.model_validate(base)


def test_resolve_model_connection_from_registry_json(monkeypatch):
	monkeypatch.setenv(
		"MODEL_CONNECTION_PROFILES_JSON",
		'{"openai_prod":{"base_url":"https://api.example.com","api_key_ref":"OPENAI_KEY"}}',
	)
	monkeypatch.setenv("OPENAI_KEY", "secret-value")
	params = _params(connection_ref="openai_prod", base_url="http://ignored.local")
	resolved = resolve_model_connection(params, provider="openai_compat")
	assert resolved.base_url == "https://api.example.com"
	assert resolved.api_key == "secret-value"
	assert resolved.profile_id == "openai_prod"
	assert resolved.resolved_from == "connection_ref"


def test_resolve_model_connection_from_env_profile(monkeypatch):
	monkeypatch.delenv("MODEL_CONNECTION_PROFILES_JSON", raising=False)
	monkeypatch.setenv("MODEL_CONN_DEV_LOCAL", '{"base_url":"http://127.0.0.1:11434"}')
	params = _params(connection_ref="dev-local", base_url=None)
	resolved = resolve_model_connection(params, provider="ollama")
	assert resolved.base_url == "http://127.0.0.1:11434"
	assert resolved.api_key is None
	assert resolved.profile_id == "dev-local"


def test_resolve_model_connection_missing_profile_errors(monkeypatch):
	monkeypatch.delenv("MODEL_CONNECTION_PROFILES_JSON", raising=False)
	monkeypatch.delenv("MODEL_CONN_MISSING_PROFILE", raising=False)
	params = _params(connection_ref="missing-profile", base_url=None)
	with pytest.raises(ValueError, match="MISSING_SECRET: connection_ref 'missing-profile' is not set"):
		resolve_model_connection(params, provider="openai_compat")
