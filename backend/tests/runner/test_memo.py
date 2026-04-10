from app.runner.memo import (
	_canonical_params,
	compute_memo_key,
	compute_memo_key_for_node,
)


def test_canonical_params_sorts_keys():
	params_a = {"b": 1, "a": 2}
	params_b = {"a": 2, "b": 1}
	assert _canonical_params(params_a) == _canonical_params(params_b)


def test_canonical_params_normalizes_floats():
	assert _canonical_params({"x": 0.30000000000000004}) == _canonical_params({"x": 0.3})


def test_compute_memo_key_deterministic():
	k1 = compute_memo_key("llm", {"temperature": 0.7, "model": "gpt-4"}, ["art1", "art2"])
	k2 = compute_memo_key("llm", {"model": "gpt-4", "temperature": 0.7}, ["art1", "art2"])
	assert k1 == k2


def test_compute_memo_key_changes_with_param():
	k1 = compute_memo_key("llm", {"temperature": 0.7}, ["art1"])
	k2 = compute_memo_key("llm", {"temperature": 0.8}, ["art1"])
	assert k1 != k2


def test_compute_memo_key_changes_with_input_artifact():
	k1 = compute_memo_key("transform", {}, ["art1"])
	k2 = compute_memo_key("transform", {}, ["art2"])
	assert k1 != k2


def test_compute_memo_key_input_order_invariant():
	k1 = compute_memo_key("join", {}, ["art1", "art2"])
	k2 = compute_memo_key("join", {}, ["art2", "art1"])
	assert k1 == k2


def test_compute_memo_key_returns_64_char_hex():
	key = compute_memo_key("source", {}, [])
	assert len(key) == 64
	assert all(c in "0123456789abcdef" for c in key)


def test_compute_memo_key_for_node_returns_none_when_not_memoizable():
	node = {"data": {"kind": "source", "params": {}, "meta": {"memoizable": False}}}
	assert compute_memo_key_for_node(node, []) is None


def test_compute_memo_key_for_node_returns_key_when_memoizable():
	node = {"data": {"kind": "llm", "params": {"temperature": 0}, "meta": {}}}
	key = compute_memo_key_for_node(node, ["art1"])
	assert isinstance(key, str)
	assert len(key) == 64

