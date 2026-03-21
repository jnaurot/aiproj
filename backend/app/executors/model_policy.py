from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List

from app.runner.schemas import LLMParams


@dataclass(frozen=True)
class RequestPolicy:
	retries: int
	timeout_seconds: float
	backoff_base_seconds: float
	backoff_max_seconds: float
	backoff_jitter_seconds: float
	circuit_enabled: bool
	circuit_fail_threshold: int
	circuit_reset_seconds: float
	batch_enabled: bool
	batch_max_items: int
	deterministic_enabled: bool
	deterministic_seed: int | None
	deterministic_stable_order: bool
	fallback_chain: List[Dict[str, Any]]


def _as_int(v: Any, default: int, minimum: int = 0, maximum: int = 1000) -> int:
	try:
		n = int(v)
	except Exception:
		return default
	return max(minimum, min(maximum, n))


def _as_float(v: Any, default: float, minimum: float = 0.0, maximum: float = 3600.0) -> float:
	try:
		n = float(v)
	except Exception:
		return default
	return max(minimum, min(maximum, n))


def normalize_request_policy(params: LLMParams) -> RequestPolicy:
	raw = params.request_policy if isinstance(params.request_policy, dict) else {}
	backoff = raw.get("backoff") if isinstance(raw.get("backoff"), dict) else {}
	circuit = raw.get("circuit_breaker") if isinstance(raw.get("circuit_breaker"), dict) else {}
	batching = raw.get("batching") if isinstance(raw.get("batching"), dict) else {}
	determinism = raw.get("determinism") if isinstance(raw.get("determinism"), dict) else {}
	fallback = raw.get("fallback_chain")
	if not isinstance(fallback, list):
		fallback = []
	return RequestPolicy(
		retries=_as_int(raw.get("retries"), default=int(params.max_retries), minimum=0, maximum=20),
		timeout_seconds=_as_float(raw.get("timeout_seconds"), default=float(params.timeout_seconds), minimum=1.0, maximum=3600.0),
		backoff_base_seconds=_as_float(backoff.get("base_seconds"), default=0.5, minimum=0.0, maximum=60.0),
		backoff_max_seconds=_as_float(backoff.get("max_seconds"), default=8.0, minimum=0.0, maximum=300.0),
		backoff_jitter_seconds=_as_float(backoff.get("jitter_seconds"), default=0.0, minimum=0.0, maximum=60.0),
		circuit_enabled=bool(circuit.get("enabled", False)),
		circuit_fail_threshold=_as_int(circuit.get("fail_threshold"), default=5, minimum=1, maximum=100),
		circuit_reset_seconds=_as_float(circuit.get("reset_seconds"), default=30.0, minimum=1.0, maximum=3600.0),
		batch_enabled=bool(batching.get("enabled", True)),
		batch_max_items=_as_int(batching.get("max_items"), default=64, minimum=1, maximum=4096),
		deterministic_enabled=bool(determinism.get("enabled", False)),
		deterministic_seed=(
			_as_int(determinism.get("seed"), default=0, minimum=-2147483648, maximum=2147483647)
			if determinism.get("seed") is not None
			else None
		),
		deterministic_stable_order=bool(determinism.get("stable_order", True)),
		fallback_chain=[x for x in fallback if isinstance(x, dict)],
	)


_CIRCUIT_LOCK = threading.Lock()
_CIRCUIT_STATE: Dict[str, Dict[str, float]] = {}


def circuit_guard_allows(key: str, policy: RequestPolicy, now_ts: float | None = None) -> bool:
	if not policy.circuit_enabled:
		return True
	now = float(now_ts if now_ts is not None else time.time())
	with _CIRCUIT_LOCK:
		state = _CIRCUIT_STATE.get(key) or {}
		open_until = float(state.get("open_until") or 0.0)
	return now >= open_until


def circuit_record_success(key: str, policy: RequestPolicy) -> None:
	if not policy.circuit_enabled:
		return
	with _CIRCUIT_LOCK:
		_CIRCUIT_STATE[key] = {"failures": 0.0, "open_until": 0.0}


def circuit_record_failure(key: str, policy: RequestPolicy, now_ts: float | None = None) -> None:
	if not policy.circuit_enabled:
		return
	now = float(now_ts if now_ts is not None else time.time())
	with _CIRCUIT_LOCK:
		state = _CIRCUIT_STATE.get(key) or {"failures": 0.0, "open_until": 0.0}
		failures = float(state.get("failures") or 0.0) + 1.0
		open_until = float(state.get("open_until") or 0.0)
		if failures >= float(policy.circuit_fail_threshold):
			open_until = now + float(policy.circuit_reset_seconds)
			failures = 0.0
		_CIRCUIT_STATE[key] = {"failures": failures, "open_until": open_until}


def policy_backoff_seconds(policy: RequestPolicy, attempt: int) -> float:
	n = max(1, int(attempt))
	delay = policy.backoff_base_seconds * (2.0 ** (n - 1))
	delay = min(delay, policy.backoff_max_seconds)
	if policy.backoff_jitter_seconds > 0:
		# deterministic pseudo-jitter by attempt index
		offset = (n % 3) * policy.backoff_jitter_seconds / 2.0
		delay = min(policy.backoff_max_seconds, delay + offset)
	return delay
