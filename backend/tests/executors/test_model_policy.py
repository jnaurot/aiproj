from app.executors.model_policy import (
	circuit_guard_allows,
	circuit_record_failure,
	circuit_record_success,
	normalize_request_policy,
	policy_backoff_seconds,
)
from app.runner.schemas import LLMParams


def _params(**overrides) -> LLMParams:
	base = {
		"model": "demo-model",
		"user_prompt": "hello",
		"base_url": "http://localhost:11434",
	}
	base.update(overrides)
	return LLMParams.model_validate(base)


def test_normalize_request_policy_defaults_from_legacy_fields():
	params = _params(max_retries=4, timeout_seconds=21)
	policy = normalize_request_policy(params)
	assert policy.retries == 4
	assert policy.timeout_seconds == 21
	assert policy.backoff_base_seconds == 0.5
	assert policy.backoff_max_seconds == 8.0
	assert policy.batch_enabled is True
	assert policy.batch_max_items == 64


def test_normalize_request_policy_explicit_values():
	params = _params(
		request_policy={
			"retries": 2,
			"timeout_seconds": 9,
			"backoff": {"base_seconds": 1.5, "max_seconds": 4.0, "jitter_seconds": 0.2},
			"circuit_breaker": {"enabled": True, "fail_threshold": 2, "reset_seconds": 30},
			"batching": {"enabled": True, "max_items": 16},
			"determinism": {"enabled": True, "seed": 42, "stable_order": True},
			"fallback_chain": [{"llmKind": "openai_compat", "model": "gpt-4.1-mini"}],
		}
	)
	policy = normalize_request_policy(params)
	assert policy.retries == 2
	assert policy.timeout_seconds == 9
	assert policy.backoff_base_seconds == 1.5
	assert policy.backoff_max_seconds == 4.0
	assert policy.backoff_jitter_seconds == 0.2
	assert policy.circuit_enabled is True
	assert policy.circuit_fail_threshold == 2
	assert policy.batch_enabled is True
	assert policy.batch_max_items == 16
	assert policy.deterministic_enabled is True
	assert policy.deterministic_seed == 42
	assert policy.deterministic_stable_order is True
	assert len(policy.fallback_chain) == 1


def test_policy_backoff_seconds_capped():
	params = _params(request_policy={"backoff": {"base_seconds": 1.0, "max_seconds": 2.0}})
	policy = normalize_request_policy(params)
	assert policy_backoff_seconds(policy, 1) == 1.0
	assert policy_backoff_seconds(policy, 4) == 2.0


def test_circuit_breaker_opens_and_resets():
	params = _params(request_policy={"circuit_breaker": {"enabled": True, "fail_threshold": 2, "reset_seconds": 10}})
	policy = normalize_request_policy(params)
	key = "openai_compat::http://x::model"
	now = 1000.0
	assert circuit_guard_allows(key, policy, now_ts=now) is True
	circuit_record_failure(key, policy, now_ts=now)
	assert circuit_guard_allows(key, policy, now_ts=now + 0.1) is True
	circuit_record_failure(key, policy, now_ts=now + 0.2)
	assert circuit_guard_allows(key, policy, now_ts=now + 1.0) is False
	assert circuit_guard_allows(key, policy, now_ts=now + 11.0) is True
	circuit_record_success(key, policy)
	assert circuit_guard_allows(key, policy, now_ts=now + 11.1) is True
