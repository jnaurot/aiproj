from __future__ import annotations

from types import MethodType
from uuid import uuid4

from fastapi.testclient import TestClient

from app.main import app


def _minimal_graph():
	return {
		"version": 1,
		"nodes": [
			{
				"id": "n1",
				"type": "source",
				"position": {"x": 0, "y": 0},
				"data": {
					"kind": "source",
					"label": "src",
					"sourceKind": "file",
					"status": "idle",
					"params": {"source_type": "text", "text": "hello", "output_mode": "rows"},
				},
			}
		],
		"edges": [],
	}


def test_create_run_forwards_adaptive_override_to_runtime():
	captured: dict[str, object] = {}
	with TestClient(app) as client:
		rt = client.app.state.runtime

		async def _fake_start_run(
			self,
			run_id,
			graph,
			run_from,
			run_mode=None,
			graph_id=None,
			adaptive_override=None,
		):
			captured["adaptive_override"] = adaptive_override
			h = self.get_run(run_id)
			if h:
				h.graph_id = str(graph_id or "")
				h.graph = graph
				h.status = "finished"

		rt.start_run = MethodType(_fake_start_run, rt)
		graph_id = f"graph_adaptive_override_{uuid4().hex[:8]}"
		res = client.post(
			"/runs",
			json={
				"graphId": graph_id,
				"runFrom": None,
				"graph": _minimal_graph(),
				"adaptive": {"mode": "enforce"},
			},
		)
		assert res.status_code == 200, res.text
		assert captured.get("adaptive_override") == {"mode": "enforce"}


def test_create_run_forwards_adaptive_override_off_and_observe():
	captured: list[object] = []
	with TestClient(app) as client:
		rt = client.app.state.runtime

		async def _fake_start_run(
			self,
			run_id,
			graph,
			run_from,
			run_mode=None,
			graph_id=None,
			adaptive_override=None,
		):
			captured.append(adaptive_override)
			h = self.get_run(run_id)
			if h:
				h.graph_id = str(graph_id or "")
				h.graph = graph
				h.status = "finished"

		rt.start_run = MethodType(_fake_start_run, rt)
		graph_id = f"graph_adaptive_override_modes_{uuid4().hex[:8]}"
		for mode in ("off", "observe"):
			res = client.post(
				"/runs",
				json={
					"graphId": graph_id,
					"runFrom": None,
					"graph": _minimal_graph(),
					"adaptive": {"mode": mode},
				},
			)
			assert res.status_code == 200, res.text

		assert captured == [{"mode": "off"}, {"mode": "observe"}]


def test_create_run_without_adaptive_override_forwards_none():
	captured: dict[str, object] = {}
	with TestClient(app) as client:
		rt = client.app.state.runtime

		async def _fake_start_run(
			self,
			run_id,
			graph,
			run_from,
			run_mode=None,
			graph_id=None,
			adaptive_override=None,
		):
			captured["adaptive_override"] = adaptive_override
			h = self.get_run(run_id)
			if h:
				h.graph_id = str(graph_id or "")
				h.graph = graph
				h.status = "finished"

		rt.start_run = MethodType(_fake_start_run, rt)
		graph_id = f"graph_adaptive_override_none_{uuid4().hex[:8]}"
		res = client.post(
			"/runs",
			json={
				"graphId": graph_id,
				"runFrom": None,
				"graph": _minimal_graph(),
			},
		)
		assert res.status_code == 200, res.text
		assert captured.get("adaptive_override") is None


def test_create_run_rejects_invalid_adaptive_override_mode():
	with TestClient(app) as client:
		graph_id = f"graph_adaptive_override_invalid_{uuid4().hex[:8]}"
		res = client.post(
			"/runs",
			json={
				"graphId": graph_id,
				"runFrom": None,
				"graph": _minimal_graph(),
				"adaptive": {"mode": "invalid_mode"},
			},
		)
		assert res.status_code == 422, res.text
