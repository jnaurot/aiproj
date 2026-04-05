from __future__ import annotations

from typing import Any, List

from fastapi.testclient import TestClient

from app.main import app


def _collect_enums(value: Any, out: List[list[str]]) -> None:
	if isinstance(value, dict):
		enum = value.get("enum")
		if isinstance(enum, list):
			out.append([str(item).strip().lower() for item in enum])
		for child in value.values():
			_collect_enums(child, out)
	elif isinstance(value, list):
		for child in value:
			_collect_enums(child, out)


def test_openapi_plane_contract_excludes_legacy_config_plane() -> None:
	with TestClient(app) as client:
		res = client.get("/openapi.json")
		assert res.status_code == 200, res.text
		body = res.json()
		enums: List[list[str]] = []
		_collect_enums(body, enums)
		for enum_vals in enums:
			as_set = set(enum_vals)
			if {"work", "param", "control"}.issubset(as_set):
				assert "config" not in as_set

