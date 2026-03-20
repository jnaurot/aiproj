from app.runner.run import _declared_in_port, _declared_out_port


def _transform_node(op: str, transform_kind: str | None = None) -> dict:
	return {
		"id": "n1",
		"data": {
			"kind": "transform",
			"transformKind": transform_kind or op,
			"params": {"op": op},
		},
	}


def test_declared_ports_text_to_table_are_adapter_aware() -> None:
	node = _transform_node("text_to_table")
	assert _declared_in_port("transform", node) == "text"
	assert _declared_out_port("transform", node) == "table"


def test_declared_ports_json_to_table_are_adapter_aware() -> None:
	node = _transform_node("json_to_table")
	assert _declared_in_port("transform", node) == "json"
	assert _declared_out_port("transform", node) == "table"


def test_declared_ports_table_to_json_are_adapter_aware() -> None:
	node = _transform_node("table_to_json")
	assert _declared_in_port("transform", node) == "table"
	assert _declared_out_port("transform", node) == "json"
