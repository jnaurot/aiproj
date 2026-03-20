from app.runner.run import _resolve_join_placeholder_node_ids


def test_resolve_join_placeholder_node_ids_maps_default_tokens() -> None:
	clauses = [
		{
			"leftNodeId": "upstream_left",
			"leftCol": "id",
			"rightNodeId": "upstream_right",
			"rightCol": "id",
			"how": "inner",
		}
	]
	resolved, mapping = _resolve_join_placeholder_node_ids(clauses, ["n_left", "n_right"])
	assert resolved[0]["leftNodeId"] == "n_left"
	assert resolved[0]["rightNodeId"] == "n_right"
	assert mapping == {"upstream_left": "n_left", "upstream_right": "n_right"}


def test_resolve_join_placeholder_node_ids_avoids_same_node_both_sides() -> None:
	clauses = [
		{
			"leftNodeId": "upstream_left",
			"leftCol": "id",
			"rightNodeId": "upstream_left",
			"rightCol": "id",
			"how": "inner",
		}
	]
	resolved, _ = _resolve_join_placeholder_node_ids(clauses, ["n_left", "n_right"])
	assert resolved[0]["leftNodeId"] == "n_left"
	assert resolved[0]["rightNodeId"] == "n_right"
