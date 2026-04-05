from __future__ import annotations

from app.component_contracts import normalize_edge_mode


def test_normalize_edge_mode_migrates_legacy_config_to_param() -> None:
	assert normalize_edge_mode("config") == "param"
