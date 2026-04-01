from __future__ import annotations

import importlib
import os
import time
from types import MethodType
from uuid import uuid4

from fastapi.testclient import TestClient

from app.main import app
from app.runner.metadata import NodeOutput


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


def _fanout_tool_graph():
	return {
		"version": 1,
		"nodes": [
			{
				"id": "tool_src",
				"type": "tool",
				"position": {"x": 0, "y": 0},
				"data": {
					"kind": "tool",
					"label": "Tool Source",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {}}},
				},
			},
			{
				"id": "tool_a",
				"type": "tool",
				"position": {"x": 150, "y": 0},
				"data": {"kind": "tool", "label": "Tool A", "params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {}}}},
			},
			{
				"id": "tool_b",
				"type": "tool",
				"position": {"x": 150, "y": 80},
				"data": {"kind": "tool", "label": "Tool B", "params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {}}}},
			},
		],
		"edges": [
			{"id": "e1", "source": "tool_src", "target": "tool_a"},
			{"id": "e2", "source": "tool_src", "target": "tool_b"},
		],
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


def test_run_and_adaptive_decisions_endpoint_surfaces_run_override_mode_source(monkeypatch):
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		await __import__("asyncio").sleep(0.12)
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	monkeypatch.setattr(run_mod, "get_env", lambda name, default=None: os.getenv(name, default))
	monkeypatch.setattr(
		run_mod,
		"apply_adaptive_policy",
		lambda **kwargs: {
			"nextCaps": {"global": 2, "source": 2, "transform": 2, "model": 1, "tool": 1},
			"changedCaps": {"global": {"from": 4, "to": 2}},
			"changed": True,
			"reasons": ["pressure"],
			"inputs": {"queueDepth": 2, "readyCount": 1, "avgLatencyMs": 5.0, "failureRate": 0.0, "leaseWaitMs": 0.0},
		},
	)
	monkeypatch.setenv("RUNNER_ADAPTIVE_MODE", "off")
	monkeypatch.setenv("RUNNER_ADAPTIVE_EVAL_INTERVAL_MS", "100")
	monkeypatch.setenv("RUNNER_ADAPTIVE_COOLDOWN_MS", "0")

	with TestClient(app) as client:
		graph_id = f"graph_adaptive_endpoint_{uuid4().hex[:8]}"
		create_res = client.post(
			"/runs",
			json={
				"graphId": graph_id,
				"runFrom": None,
				"graph": _fanout_tool_graph(),
				"adaptive": {"mode": "observe"},
			},
		)
		assert create_res.status_code == 200, create_res.text
		run_id = str((create_res.json() or {}).get("runId") or "").strip()
		assert run_id

		status = ""
		for _ in range(80):
			snap = client.get(f"/runs/{run_id}")
			assert snap.status_code == 200, snap.text
			status = str((snap.json() or {}).get("status") or "").strip().lower()
			if status in {"succeeded", "failed", "finished", "canceled"}:
				break
			time.sleep(0.05)
		assert status in {"succeeded", "finished"}, f"run did not finish cleanly: {status}"

		adaptive_res = client.get(
			"/experiments/adaptive/decisions",
			params={"graphId": graph_id, "mode": "observe", "sort": "created_desc", "limit": 20, "offset": 0},
		)
		assert adaptive_res.status_code == 200, adaptive_res.text
		rows = (adaptive_res.json() or {}).get("decisions") or []
		assert rows
		assert any(str(row.get("mode") or "") == "observe" for row in rows)
		assert all(str(row.get("modeSource") or "") == "run_override" for row in rows)
