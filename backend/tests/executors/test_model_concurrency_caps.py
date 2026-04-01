import pytest
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


def test_provider_semaphore_reloads_on_cap_increase(monkeypatch):
	llm_exec._reset_provider_lease_state_for_tests()
	monkeypatch.setenv("RUNNER_MAX_MODEL_PROVIDER_OPENAI_COMPAT", "1")
	sem = llm_exec._provider_semaphore("openai_compat")
	assert sem is not None
	assert getattr(sem, "_value", 0) == 1
	monkeypatch.setenv("RUNNER_MAX_MODEL_PROVIDER_OPENAI_COMPAT", "3")
	updated = llm_exec._provider_semaphore("openai_compat")
	assert updated is sem
	assert getattr(updated, "_value", 0) == 3


@pytest.mark.asyncio
async def test_provider_semaphore_reloads_on_cap_decrease_for_future_acquires(monkeypatch):
	llm_exec._reset_provider_lease_state_for_tests()
	monkeypatch.setenv("RUNNER_MAX_MODEL_PROVIDER_OPENAI_COMPAT", "2")
	sem = llm_exec._provider_semaphore("openai_compat")
	assert sem is not None
	await sem.acquire()
	llm_exec._provider_mark_acquired("openai_compat")
	await sem.acquire()
	llm_exec._provider_mark_acquired("openai_compat")
	assert getattr(sem, "_value", 0) == 0
	monkeypatch.setenv("RUNNER_MAX_MODEL_PROVIDER_OPENAI_COMPAT", "1")
	updated = llm_exec._provider_semaphore("openai_compat")
	assert updated is sem
	assert llm_exec._MODEL_PROVIDER_PERMIT_DEBT.get("openai_compat") == 1
	release_state_1 = llm_exec._provider_release_permit("openai_compat", sem)
	assert release_state_1.released is False
	assert release_state_1.debt == 0
	assert getattr(sem, "_value", 0) == 0
	release_state_2 = llm_exec._provider_release_permit("openai_compat", sem)
	assert release_state_2.released is True
	assert getattr(sem, "_value", 0) == 1


def test_provider_wait_queue_and_holder_state_remain_consistent_after_recap(monkeypatch):
	llm_exec._reset_provider_lease_state_for_tests()
	monkeypatch.setenv("RUNNER_MAX_MODEL_PROVIDER_OLLAMA", "2")
	llm_exec._provider_waiter_add("ollama", "n_wait_a")
	llm_exec._provider_waiter_add("ollama", "n_wait_b")
	llm_exec._provider_holder_set("ollama", "n_holder")
	sem = llm_exec._provider_semaphore("ollama")
	assert sem is not None
	monkeypatch.setenv("RUNNER_MAX_MODEL_PROVIDER_OLLAMA", "1")
	llm_exec._provider_semaphore("ollama")
	assert llm_exec._provider_waiting_nodes("ollama") == ["n_wait_a", "n_wait_b"]
	assert llm_exec._provider_holder_get("ollama") == "n_holder"


def test_provider_cap_reconciliation_emits_log(monkeypatch, caplog):
	llm_exec._reset_provider_lease_state_for_tests()
	monkeypatch.setenv("RUNNER_MAX_MODEL_PROVIDER_OLLAMA", "1")
	llm_exec._provider_semaphore("ollama")
	monkeypatch.setenv("RUNNER_MAX_MODEL_PROVIDER_OLLAMA", "3")
	with caplog.at_level("INFO"):
		llm_exec._provider_semaphore("ollama")
	assert any("MODEL_PROVIDER_CAP_RECONCILED" in rec.message for rec in caplog.records)
