from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class AdaptiveInputs:
	queue_depth: int
	ready_count: int
	avg_latency_ms: float
	failure_rate: float
	lease_wait_ms: float


def normalize_mode(raw: Any) -> str:
	mode = str(raw or "").strip().lower()
	if mode in {"off", "observe", "enforce"}:
		return mode
	return "off"


def apply_adaptive_policy(
	*,
	current_caps: Dict[str, int],
	hard_caps: Dict[str, int],
	min_caps: Dict[str, int] | None,
	inputs: AdaptiveInputs,
	config: Dict[str, float],
) -> Dict[str, Any]:
	min_caps = dict(min_caps or {})

	# Safety rails: always clamp to [min_cap, hard_cap]
	def _clamp(key: str, value: int) -> int:
		hard = max(1, int(hard_caps.get(key, 1)))
		floor = max(1, int(min_caps.get(key, 1)))
		if floor > hard:
			floor = hard
		return max(floor, min(hard, int(value)))

	next_caps = {k: _clamp(k, int(current_caps.get(k, hard_caps.get(k, 1)))) for k in hard_caps.keys()}
	reasons: list[str] = []

	queue_high = int(config.get("queue_high", 32))
	queue_low = int(config.get("queue_low", 4))
	latency_high_ms = float(config.get("latency_high_ms", 1200.0))
	failure_high = float(config.get("failure_high", 0.25))
	lease_wait_high_ms = float(config.get("lease_wait_high_ms", 800.0))
	step_down = max(1, int(config.get("step_down", 1)))
	step_up = max(1, int(config.get("step_up", 1)))

	pressure = (
		int(inputs.queue_depth) >= queue_high
		or float(inputs.avg_latency_ms) >= latency_high_ms
		or float(inputs.failure_rate) >= failure_high
		or float(inputs.lease_wait_ms) >= lease_wait_high_ms
	)
	recovery = (
		int(inputs.queue_depth) <= queue_low
		and float(inputs.failure_rate) <= max(0.0, failure_high * 0.4)
		and float(inputs.avg_latency_ms) <= max(1.0, latency_high_ms * 0.6)
	)

	if pressure:
		next_caps["global"] = _clamp("global", next_caps["global"] - step_down)
		next_caps["model"] = _clamp("model", next_caps["model"] - step_down)
		next_caps["tool"] = _clamp("tool", next_caps["tool"] - step_down)
		reasons.append("pressure")
		if float(inputs.failure_rate) >= failure_high:
			next_caps["model"] = _clamp("model", next_caps["model"] - step_down)
			reasons.append("failure_rate_high")
	elif recovery:
		next_caps["global"] = _clamp("global", next_caps["global"] + step_up)
		next_caps["source"] = _clamp("source", next_caps["source"] + step_up)
		next_caps["transform"] = _clamp("transform", next_caps["transform"] + step_up)
		next_caps["tool"] = _clamp("tool", next_caps["tool"] + step_up)
		reasons.append("recovery")

	changed = {
		k: {"from": int(current_caps.get(k, next_caps[k])), "to": int(v)}
		for k, v in next_caps.items()
		if int(current_caps.get(k, v)) != int(v)
	}

	return {
		"nextCaps": next_caps,
		"changedCaps": changed,
		"changed": bool(changed),
		"reasons": reasons,
		"inputs": {
			"queueDepth": int(inputs.queue_depth),
			"readyCount": int(inputs.ready_count),
			"avgLatencyMs": float(inputs.avg_latency_ms),
			"failureRate": float(inputs.failure_rate),
			"leaseWaitMs": float(inputs.lease_wait_ms),
		},
	}
