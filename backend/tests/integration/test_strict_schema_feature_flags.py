from __future__ import annotations

import types
import sys
from types import SimpleNamespace

import pytest

if "duckdb" not in sys.modules:
	sys.modules["duckdb"] = types.SimpleNamespace()

from app.runner.artifacts import MemoryArtifactStore
from app.runner.cache import ExecutionCache
from app.runner.events import RunEventBus
from app.runner.run import run_graph


class _ComponentStoreStub:
	def __init__(self, definition):
		self._definition = definition

	def get_revision(self, component_id: str, revision_id: str):
		if component_id == "cmp_strict" and revision_id == "crev_1":
			return SimpleNamespace(definition=self._definition)
		return None


def _component_definition() -> dict:
	return {
		"graph": {
			"nodes": [
				{
					"id": "n1",
					"type": "source",
					"position": {"x": 0, "y": 0},
					"data": {
						"kind": "source",
						"label": "Source",
						"sourceKind": "file",
						"status": "idle",
						"ports": {"in": None, "out": "table"},
						"params": {"source_type": "text", "text": "x", "output_mode": "table"},
					},
				}
			],
			"edges": [],
		},
		"api": {
			"inputs": [],
			"outputs": [
				{
					"name": "out_data",
					"portType": "json",
					"required": True,
					"typedSchema": {"type": "json", "fields": []},
				}
			],
		},
	}


def _graph_component_node_with_mismatch() -> dict:
	return {
		"nodes": [
			{
				"id": "cmp1",
				"type": "component",
				"position": {"x": 0, "y": 0},
				"data": {
					"kind": "component",
					"label": "Component",
					"status": "idle",
					"ports": {"in": None, "out": "json"},
					"params": {
						"componentRef": {"componentId": "cmp_strict", "revisionId": "crev_1", "apiVersion": "v1"},
						"api": {
							"inputs": [],
							"outputs": [
								{
									"name": "out_data",
									"portType": "json",
									"required": True,
									"typedSchema": {"type": "json", "fields": []},
								}
							],
						},
						"bindings": {"inputs": {}, "outputs": {"out_data": {"nodeId": "n1", "artifact": "current"}}},
						"config": {},
					},
				},
			}
		],
		"edges": [],
	}


@pytest.mark.asyncio
async def test_strict_schema_edge_checks_on_fails_component_output_mismatch(monkeypatch):
	monkeypatch.setenv("STRICT_SCHEMA_EDGE_CHECKS", "1")
	monkeypatch.setenv("STRICT_COERCION_POLICY", "1")
	events = []
	await run_graph(
		run_id="run-strict-on",
		graph=_graph_component_node_with_mismatch(),
		run_from=None,
		bus=RunEventBus("run-strict-on", on_emit=lambda e: events.append(dict(e))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		runtime_ref=SimpleNamespace(component_revisions=_ComponentStoreStub(_component_definition())),
		graph_id="graph_strict_on",
	)
	assert any(
		e.get("type") == "node_finished"
		and e.get("nodeId") == "cmp1"
		and e.get("status") == "failed"
		and str(e.get("errorCode") or "") == "COMPONENT_OUTPUT_CONTRACT_MISMATCH"
		for e in events
	)
	assert any(e.get("type") == "run_finished" and e.get("status") == "failed" for e in events)


@pytest.mark.asyncio
async def test_strict_schema_edge_checks_off_allows_component_output_mismatch(monkeypatch):
	monkeypatch.setenv("STRICT_SCHEMA_EDGE_CHECKS", "0")
	monkeypatch.setenv("STRICT_COERCION_POLICY", "0")
	events = []
	await run_graph(
		run_id="run-strict-off",
		graph=_graph_component_node_with_mismatch(),
		run_from=None,
		bus=RunEventBus("run-strict-off", on_emit=lambda e: events.append(dict(e))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		runtime_ref=SimpleNamespace(component_revisions=_ComponentStoreStub(_component_definition())),
		graph_id="graph_strict_off",
	)
	assert any(
		e.get("type") == "node_finished" and e.get("nodeId") == "cmp1" and e.get("status") == "succeeded"
		for e in events
	)
	assert any(e.get("type") == "run_finished" and e.get("status") == "succeeded" for e in events)
