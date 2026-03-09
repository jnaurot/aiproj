from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app


def _graph_with_builtin_profile(profile_id: str) -> dict:
	return {
		"version": 1,
		"nodes": [
			{
				"id": "tool_python_1",
				"type": "tool",
				"position": {"x": 0, "y": 0},
				"data": {
					"kind": "tool",
					"label": "Tool Python",
					"ports": {"in": None, "out": "text"},
					"params": {
						"provider": "python",
						"builtin": {"profileId": profile_id, "customPackages": []},
						"python": {"code": "print('ok')", "args": {}, "capture_output": True},
					},
				},
			}
		],
		"edges": [],
	}


def test_e2e_profile_picker_save_load_roundtrip():
	graph_id = "graph_env_profile_roundtrip_gate"
	with TestClient(app) as client:
		created = client.post(
			"/graphs",
			json={
				"graphId": graph_id,
				"message": "seed env profile graph",
				"graph": _graph_with_builtin_profile("llm_finetune"),
			},
		)
		assert created.status_code == 200, created.text

		latest = client.get(f"/graphs/{graph_id}/latest")
		assert latest.status_code == 200, latest.text
		graph = (latest.json() or {}).get("graph") or {}
		nodes = graph.get("nodes") or []
		assert len(nodes) == 1
		builtin = (((nodes[0].get("data") or {}).get("params") or {}).get("builtin") or {})
		assert str(builtin.get("profileId") or "") == "llm_finetune"
