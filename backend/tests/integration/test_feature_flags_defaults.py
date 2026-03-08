from __future__ import annotations

from app.feature_flags import get_feature_flags


def test_graph_feature_flag_defaults_enabled(monkeypatch):
	for key in ("GRAPH_STORE_V2_READ", "GRAPH_STORE_V2_WRITE", "GRAPH_EXPORT_V2"):
		monkeypatch.delenv(key, raising=False)
	for key in ("STRICT_SCHEMA_EDGE_CHECKS", "STRICT_COERCION_POLICY"):
		monkeypatch.delenv(key, raising=False)
	flags = get_feature_flags()
	assert flags["GRAPH_STORE_V2_READ"] is True
	assert flags["GRAPH_STORE_V2_WRITE"] is True
	assert flags["GRAPH_EXPORT_V2"] is True
	assert flags["STRICT_SCHEMA_EDGE_CHECKS"] is True
	assert flags["STRICT_COERCION_POLICY"] is True


def test_strict_feature_flags_support_env_override(monkeypatch):
	monkeypatch.setenv("STRICT_SCHEMA_EDGE_CHECKS", "0")
	monkeypatch.setenv("STRICT_COERCION_POLICY", "false")
	flags = get_feature_flags()
	assert flags["STRICT_SCHEMA_EDGE_CHECKS"] is False
	assert flags["STRICT_COERCION_POLICY"] is False

