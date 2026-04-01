from app.runner.adaptive_scheduler import AdaptiveInputs, apply_adaptive_policy, normalize_mode


def test_normalize_mode_accepts_known_values():
	assert normalize_mode("off") == "off"
	assert normalize_mode("observe") == "observe"
	assert normalize_mode("enforce") == "enforce"
	assert normalize_mode(" OFF ") == "off"
	assert normalize_mode("invalid") == "off"


def test_apply_adaptive_policy_is_bounded_by_hard_and_min_caps():
	decision = apply_adaptive_policy(
		current_caps={"global": 9, "source": 0, "transform": 5, "model": 4, "tool": -2},
		hard_caps={"global": 4, "source": 2, "transform": 2, "model": 2, "tool": 2},
		min_caps={"global": 2, "source": 1, "transform": 1, "model": 1, "tool": 1},
		inputs=AdaptiveInputs(queue_depth=1, ready_count=1, avg_latency_ms=10.0, failure_rate=0.0, lease_wait_ms=0.0),
		config={},
	)
	next_caps = decision["nextCaps"]
	assert next_caps["global"] <= 4 and next_caps["global"] >= 2
	assert next_caps["source"] <= 2 and next_caps["source"] >= 1
	assert next_caps["transform"] <= 2 and next_caps["transform"] >= 1
	assert next_caps["model"] <= 2 and next_caps["model"] >= 1
	assert next_caps["tool"] <= 2 and next_caps["tool"] >= 1


def test_apply_adaptive_policy_pressure_reduces_caps():
	decision = apply_adaptive_policy(
		current_caps={"global": 4, "source": 2, "transform": 2, "model": 2, "tool": 2},
		hard_caps={"global": 4, "source": 2, "transform": 2, "model": 2, "tool": 2},
		min_caps={"global": 1, "source": 1, "transform": 1, "model": 1, "tool": 1},
		inputs=AdaptiveInputs(queue_depth=100, ready_count=30, avg_latency_ms=3000.0, failure_rate=0.5, lease_wait_ms=2000.0),
		config={},
	)
	assert decision["changed"] is True
	assert decision["nextCaps"]["global"] < 4
	assert decision["nextCaps"]["model"] < 2
	assert "pressure" in decision["reasons"]


def test_apply_adaptive_policy_recovery_increases_caps():
	decision = apply_adaptive_policy(
		current_caps={"global": 1, "source": 1, "transform": 1, "model": 1, "tool": 1},
		hard_caps={"global": 4, "source": 2, "transform": 2, "model": 2, "tool": 2},
		min_caps={"global": 1, "source": 1, "transform": 1, "model": 1, "tool": 1},
		inputs=AdaptiveInputs(queue_depth=0, ready_count=0, avg_latency_ms=10.0, failure_rate=0.0, lease_wait_ms=0.0),
		config={},
	)
	assert decision["changed"] is True
	assert decision["nextCaps"]["global"] > 1
	assert decision["nextCaps"]["tool"] > 1
	assert "recovery" in decision["reasons"]
