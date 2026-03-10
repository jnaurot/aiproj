import sys
import types

if "duckdb" not in sys.modules:
	sys.modules["duckdb"] = types.SimpleNamespace()

from app.runner.run import _determinism_env_for_node, _tool_exec_key


def test_tool_determinism_env_includes_profile_lock_and_code_hash():
	env = _determinism_env_for_node(
		"tool",
		{
			"provider": "builtin",
			"builtin": {"toolId": "noop", "profileId": "core", "args": {}},
		},
	)
	assert str(env.get("tool_provider") or "") == "builtin"
	assert isinstance(env.get("tool_profile_lock"), str) and str(env.get("tool_profile_lock"))
	assert isinstance(env.get("executor_code_hash"), str) and len(str(env.get("executor_code_hash"))) == 64


def test_tool_exec_key_changes_when_profile_lock_changes():
	params = {
		"provider": "builtin",
		"builtin": {"toolId": "noop", "profileId": "core", "args": {}},
	}
	base_env = {"tool_provider": "builtin", "executor_code_hash": "a" * 64}
	key_a = _tool_exec_key(
		params=params,
		input_refs=[("in", "a" * 64)],
		determinism_env={**base_env, "tool_profile_lock": "lock-v1"},
		execution_version="v1",
	)
	key_b = _tool_exec_key(
		params=params,
		input_refs=[("in", "a" * 64)],
		determinism_env={**base_env, "tool_profile_lock": "lock-v2"},
		execution_version="v1",
	)
	assert key_a != key_b


def test_tool_exec_key_changes_when_code_hash_changes():
	params = {
		"provider": "builtin",
		"builtin": {"toolId": "noop", "profileId": "core", "args": {}},
	}
	base_env = {"tool_provider": "builtin", "tool_profile_lock": "lock-v1"}
	key_a = _tool_exec_key(
		params=params,
		input_refs=[("in", "a" * 64)],
		determinism_env={**base_env, "executor_code_hash": "a" * 64},
		execution_version="v1",
	)
	key_b = _tool_exec_key(
		params=params,
		input_refs=[("in", "a" * 64)],
		determinism_env={**base_env, "executor_code_hash": "b" * 64},
		execution_version="v1",
	)
	assert key_a != key_b
