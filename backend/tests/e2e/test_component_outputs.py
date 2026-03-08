from __future__ import annotations

import importlib
import json
import sys
import types
from types import SimpleNamespace

import pytest

if "duckdb" not in sys.modules:
	sys.modules["duckdb"] = types.SimpleNamespace()

from app.runner.artifacts import MemoryArtifactStore
from app.runner.cache import ExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import NodeOutput


class _ComponentStoreStub:
	def __init__(self, revisions):
		self._revisions = revisions

	def get_revision(self, component_id: str, revision_id: str):
		return self._revisions.get((component_id, revision_id))


def _component_definition():
	return {
		"graph": {
			"nodes": [
				{
					"id": "inner_text",
					"data": {
						"kind": "tool",
						"label": "Inner Text",
						"params": {"provider": "builtin", "builtin": {"toolId": "emit_text", "args": {}}},
						"ports": {"in": None, "out": "text"},
					},
				},
				{
					"id": "inner_json",
					"data": {
						"kind": "tool",
						"label": "Inner Json",
						"params": {"provider": "builtin", "builtin": {"toolId": "emit_json", "args": {}}},
						"ports": {"in": None, "out": "json"},
					},
				},
			],
			"edges": [],
		},
		"api": {
			"inputs": [],
			"outputs": [
				{
					"name": "out_text",
					"portType": "text",
					"required": True,
					"typedSchema": {"type": "text", "fields": []},
				},
				{
					"name": "out_json",
					"portType": "json",
					"required": True,
					"typedSchema": {
						"type": "json",
						"fields": [{"name": "ok", "type": "text", "nullable": False}],
					},
				},
			],
		},
	}


def _graph(output_mode: str = "current"):
	return {
		"nodes": [
			{
				"id": "cmp_node",
				"data": {
					"kind": "component",
					"label": "Component",
					"params": {
						"componentRef": {"componentId": "cmp_multi", "revisionId": "crev_1"},
						"bindings": {
							"inputs": {},
							"outputs": {
								"out_text": {"nodeId": "inner_text", "artifact": output_mode},
								"out_json": {"nodeId": "inner_json", "artifact": output_mode},
							},
							"config": {},
						},
						"config": {},
					},
					"ports": {"in": None, "out": "json"},
				},
			}
		],
		"edges": [],
	}


def _wrapper_payload(raw: bytes | bytearray | str) -> dict:
	if isinstance(raw, (bytes, bytearray)):
		data = json.loads(raw.decode("utf-8"))
	else:
		data = json.loads(str(raw))
	if isinstance(data.get("component"), dict):
		return data
	payload = data.get("payload")
	return payload if isinstance(payload, dict) else {}


@pytest.mark.asyncio
async def test_component_named_outputs_are_present_and_typed_across_reruns():
	run_mod = importlib.import_module("app.runner.run")
	store = MemoryArtifactStore()
	cache = ExecutionCache()

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		if node["id"].endswith("inner_text"):
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "text", "payload": "hello text", "meta": {"status": "ok"}},
			)
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}, "meta": {"status": "ok"}},
		)

	original_exec_tool = run_mod.exec_tool
	run_mod.exec_tool = _fake_exec_tool
	try:
		runtime_ref = SimpleNamespace(
			component_revisions=_ComponentStoreStub(
				{("cmp_multi", "crev_1"): SimpleNamespace(definition=_component_definition())}
			)
		)

		events1 = []
		await run_mod.run_graph(
			run_id="run-e2e-component-out-1",
			graph=_graph("current"),
			run_from=None,
			bus=RunEventBus("run-e2e-component-out-1", on_emit=lambda e: events1.append(dict(e))),
			artifact_store=store,
			cache=cache,
			runtime_ref=runtime_ref,
			graph_id="graph-e2e-component-out",
		)
		parent_output_1 = next(e for e in events1 if e.get("type") == "node_output" and e.get("nodeId") == "cmp_node")
		parent_artifact_1 = await store.get(str(parent_output_1["artifactId"]))
		parent_raw_1 = await store.read(str(parent_artifact_1.artifact_id))
		payload_1 = _wrapper_payload(parent_raw_1)
		outputs_1 = payload_1.get("outputs") if isinstance(payload_1.get("outputs"), dict) else {}
		assert set(outputs_1.keys()) == {"out_text", "out_json"}
		assert outputs_1["out_text"]["port_type"] == "text"
		assert outputs_1["out_json"]["port_type"] == "json"
		assert str(outputs_1["out_text"]["artifact_id"]).strip()
		assert str(outputs_1["out_json"]["artifact_id"]).strip()

		# Re-run with same cache object: wrapper should remain correct and output refs stable.
		events2 = []
		await run_mod.run_graph(
			run_id="run-e2e-component-out-2",
			graph=_graph("current"),
			run_from=None,
			bus=RunEventBus("run-e2e-component-out-2", on_emit=lambda e: events2.append(dict(e))),
			artifact_store=store,
			cache=cache,
			runtime_ref=runtime_ref,
			graph_id="graph-e2e-component-out",
		)
		parent_output_2 = next(e for e in events2 if e.get("type") == "node_output" and e.get("nodeId") == "cmp_node")
		parent_artifact_2 = await store.get(str(parent_output_2["artifactId"]))
		parent_raw_2 = await store.read(str(parent_artifact_2.artifact_id))
		payload_2 = _wrapper_payload(parent_raw_2)
		outputs_2 = payload_2.get("outputs") if isinstance(payload_2.get("outputs"), dict) else {}
		assert set(outputs_2.keys()) == {"out_text", "out_json"}
		assert outputs_2["out_text"]["artifact_id"] == outputs_1["out_text"]["artifact_id"]
		assert outputs_2["out_json"]["artifact_id"] == outputs_1["out_json"]["artifact_id"]

		# Run with fresh cache object (force miss behavior): still must expose both named outputs.
		events3 = []
		await run_mod.run_graph(
			run_id="run-e2e-component-out-3",
			graph=_graph("current"),
			run_from=None,
			bus=RunEventBus("run-e2e-component-out-3", on_emit=lambda e: events3.append(dict(e))),
			artifact_store=store,
			cache=ExecutionCache(),
			runtime_ref=runtime_ref,
			graph_id="graph-e2e-component-out",
		)
		parent_output_3 = next(e for e in events3 if e.get("type") == "node_output" and e.get("nodeId") == "cmp_node")
		parent_artifact_3 = await store.get(str(parent_output_3["artifactId"]))
		parent_raw_3 = await store.read(str(parent_artifact_3.artifact_id))
		payload_3 = _wrapper_payload(parent_raw_3)
		outputs_3 = payload_3.get("outputs") if isinstance(payload_3.get("outputs"), dict) else {}
		assert set(outputs_3.keys()) == {"out_text", "out_json"}
		assert str(outputs_3["out_text"]["artifact_id"]).strip()
		assert str(outputs_3["out_json"]["artifact_id"]).strip()
	finally:
		run_mod.exec_tool = original_exec_tool
