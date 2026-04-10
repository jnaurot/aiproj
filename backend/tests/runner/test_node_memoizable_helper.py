from app.runner.run import _node_is_memoizable


def test_node_is_memoizable_defaults_true_when_meta_missing():
    node = {"data": {"kind": "source", "params": {}}}
    assert _node_is_memoizable(node) is True


def test_node_is_memoizable_true_when_explicitly_true():
    node = {"data": {"kind": "source", "params": {}, "meta": {"memoizable": True}}}
    assert _node_is_memoizable(node) is True


def test_node_is_memoizable_false_when_explicitly_false():
    node = {"data": {"kind": "source", "params": {}, "meta": {"memoizable": False}}}
    assert _node_is_memoizable(node) is False


def test_node_is_memoizable_true_when_null():
    node = {"data": {"kind": "source", "params": {}, "meta": {"memoizable": None}}}
    assert _node_is_memoizable(node) is True
