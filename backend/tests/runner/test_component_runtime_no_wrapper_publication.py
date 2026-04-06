from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest

from app.runner.artifacts import MemoryArtifactStore
from app.runner.cache import ExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import NodeOutput


class _ComponentStore:
	def get_revision(self, component_id: str, revision_id: str):
		if str(component_id) != "CompWriter" or str(revision_id) != "r1":
			return None
		definition = {
			"graph": {
				"nodes": [
					{
						"id": "writer",
						"data": {
							"kind": "tool",
							"label": "writer",
							"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
						},
					}
				],
				"edges": [],
			},
			"api": {
				"inputs": [],
				"outputs": [{"name": "out_text", "required": True, "typedSchema": {"type": "text", "fields": []}}],
			},
		}
		return SimpleNamespace(definition=definition)


@pytest.mark.asyncio
async def test_component_runtime_publishes_native_handle_outputs_without_wrapper_artifact(monkeypatch) -> None:
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		if node_id.startswith("cmp:component_1:writer"):
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "text", "payload": "hello from internal writer", "meta": {}},
			)
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}, "meta": {}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

	graph = {
		"nodes": [
			{
				"id": "component_1",
				"data": {
					"kind": "component",
					"params": {
						"componentRef": {"componentId": "CompWriter", "revisionId": "r1", "apiVersion": "v1"},
						"api": {
							"inputs": [],
							"outputs": [{"name": "out_text", "required": True, "typedSchema": {"type": "text", "fields": []}}],
						},
						"exposureRegistry": [
							{
								"handle_id": "data_out::out_text",
								"alias": "out_text",
								"internal_source_path": "tool:writer",
								"kind": "data_output",
								"native_contract": {"type": "text", "fields": []},
								"exposed": True,
								"published": True,
								"debug_visible": False,
							}
						],
						"published_profile": [
							{
								"handle_id": "data_out::out_text",
								"alias": "out_text",
								"internal_source_path": "tool:writer",
								"kind": "data_output",
								"native_contract": {"type": "text", "fields": []},
								"exposed": True,
								"published": True,
								"debug_visible": False,
							}
						],
					},
				},
			}
		],
		"edges": [],
	}

	events: list[dict] = []
	runtime_ref = SimpleNamespace(component_revisions=_ComponentStore())
	await run_mod.run_graph(
		run_id="run-component-native-no-wrapper",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-component-native-no-wrapper", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-component-native-no-wrapper",
		runtime_ref=runtime_ref,
	)

	component_outputs = [
		e for e in events if e.get("type") == "node_output" and str(e.get("nodeId") or "") == "component_1"
	]
	assert component_outputs, "expected component node_output events"
	assert all(str(e.get("handle") or "").strip() for e in component_outputs), "all component outputs must be handle-scoped"
	assert all(str(e.get("handle") or "") == "out_text" for e in component_outputs)

