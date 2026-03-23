from app.runner.queues import next_nonempty_key


def test_next_nonempty_key_round_robins_candidates() -> None:
    keys = ["e1", "e2", "e3"]
    depths = {"e1": 0, "e2": 2, "e3": 1}
    selected, nxt = next_nonempty_key(
        keys,
        start_index=0,
        has_items=lambda key: int(depths.get(key, 0)) > 0,
    )
    assert selected == "e2"
    assert nxt == 2

    selected2, nxt2 = next_nonempty_key(
        keys,
        start_index=nxt,
        has_items=lambda key: int(depths.get(key, 0)) > 0,
    )
    assert selected2 == "e3"
    assert nxt2 == 0
