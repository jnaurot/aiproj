from __future__ import annotations

from types import MethodType

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
					"ports": {"in": None, "out": "table"},
					"params": {"source_type": "text", "text": "hello", "output_mode": "rows"},
				},
			}
		],
		"edges": [],
	}


def test_graph_revision_write_on_create_run(monkeypatch):
	monkeypatch.setenv("GRAPH_STORE_V2_WRITE", "1")
	monkeypatch.setenv("GRAPH_STORE_V2_READ", "0")

	with TestClient(app) as client:
		rt = client.app.state.runtime

		async def _fake_start_run(self, run_id, graph, run_from, run_mode=None, graph_id=None):
			h = self.get_run(run_id)
			if h:
				h.graph_id = str(graph_id or "")
				h.graph = graph
				h.status = "finished"

		rt.start_run = MethodType(_fake_start_run, rt)

		graph_id = "graph_phase2_dual_write_create"
		res = client.post(
			"/runs",
			json={"graphId": graph_id, "runFrom": None, "graph": _minimal_graph()},
		)
		assert res.status_code == 200, res.text

		rev = client.app.state.graph_revisions.get_latest(graph_id)
		assert rev is not None
		assert rev.graph_id == graph_id
		assert rev.message == "create_run"


def test_graph_revision_write_on_accept_params(monkeypatch):
	monkeypatch.setenv("GRAPH_STORE_V2_WRITE", "1")
	monkeypatch.setenv("GRAPH_STORE_V2_READ", "0")

	with TestClient(app) as client:
		rt = client.app.state.runtime

		async def _fake_start_run(self, run_id, graph, run_from, run_mode=None, graph_id=None):
			h = self.get_run(run_id)
			if h:
				h.graph_id = str(graph_id or "")
				h.graph = graph
				h.status = "finished"

		rt.start_run = MethodType(_fake_start_run, rt)
		graph_id = "graph_phase2_dual_write_accept"
		create_res = client.post(
			"/runs",
			json={"graphId": graph_id, "runFrom": None, "graph": _minimal_graph()},
		)
		assert create_res.status_code == 200, create_res.text
		run_id = create_res.json()["runId"]

		async def _fake_accept_node_params(self, run_id, graph, node_id, params):
			return {"runId": run_id, "nodeId": node_id, "affectedNodeIds": [node_id], "status": "accepted"}

		rt.accept_node_params = MethodType(_fake_accept_node_params, rt)

		res = client.post(
			f"/runs/{run_id}/nodes/n1/accept-params",
			json={"graph": _minimal_graph(), "params": {"source_type": "text", "text": "updated"}},
		)
		assert res.status_code == 200, res.text

		rev = client.app.state.graph_revisions.get_latest(graph_id)
		assert rev is not None
		assert rev.message == "accept_params:n1"


def test_graph_revision_write_ignores_legacy_env_disable(monkeypatch):
	monkeypatch.setenv("GRAPH_STORE_V2_WRITE", "0")
	monkeypatch.setenv("GRAPH_STORE_V2_READ", "0")

	with TestClient(app) as client:
		rt = client.app.state.runtime

		async def _fake_start_run(self, run_id, graph, run_from, run_mode=None, graph_id=None):
			h = self.get_run(run_id)
			if h:
				h.graph_id = str(graph_id or "")
				h.graph = graph
				h.status = "finished"

		rt.start_run = MethodType(_fake_start_run, rt)

		graph_id = "graph_phase2_dual_write_off"
		res = client.post(
			"/runs",
			json={"graphId": graph_id, "runFrom": None, "graph": _minimal_graph()},
		)
		assert res.status_code == 200, res.text
		rev = client.app.state.graph_revisions.get_latest(graph_id)
		assert rev is not None
