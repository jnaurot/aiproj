from __future__ import annotations

from app.runner.run import _edge_mode


def test_runtime_edge_mode_maps_legacy_config_to_param() -> None:
	assert _edge_mode({"data": {"mode": "config"}}) == "param"


def test_runtime_edge_mode_accepts_three_plane_model_only() -> None:
	assert _edge_mode({"data": {"mode": "work"}}) == "work"
	assert _edge_mode({"data": {"mode": "param"}}) == "param"
	assert _edge_mode({"data": {"mode": "control"}}) == "control"
	assert _edge_mode({"data": {"mode": "bogus"}}) == "work"
