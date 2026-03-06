from __future__ import annotations

from app.feature_flags import get_feature_flags


def test_graph_feature_flag_defaults_enabled(monkeypatch):
	for key in ("GRAPH_STORE_V2_READ", "GRAPH_STORE_V2_WRITE", "GRAPH_EXPORT_V2"):
		monkeypatch.delenv(key, raising=False)
	flags = get_feature_flags()
	assert flags["GRAPH_STORE_V2_READ"] is True
	assert flags["GRAPH_STORE_V2_WRITE"] is True
	assert flags["GRAPH_EXPORT_V2"] is True

