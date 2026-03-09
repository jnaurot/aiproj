from __future__ import annotations

import importlib
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


def _component_definition() -> dict:
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
				}
			],
			"edges": [],
		},
		"api": {
			"inputs": [],
			"outputs": [
				{
					"name": "out_data",
					"portType": "text",
					"required": True,
					"typedSchema": {"type": "text", "fields": []},
				}
			],
		},
	}


def _graph(component_revision_id: str) -> dict:
	return {
		"nodes": [
			{
				"id": "cmp_node",
				"data": {
					"kind": "component",
					"label": "Component",
					"params": {
						"componentRef": {"componentId": "cmp_edit", "revisionId": component_revision_id},
						"api": {
							"inputs": [],
							"outputs": [
								{
									"name": "out_data",
									"portType": "text",
									"required": True,
									"typedSchema": {"type": "text", "fields": []},
								}
							],
						},
						"bindings": {
							"inputs": {},
							"outputs": {"out_data": {"nodeId": "inner_text", "artifact": "current"}},
							"config": {},
						},
						"config": {},
					},
					"ports": {"in": None, "out": "text"},
				},
			},
			{
				"id": "llm1",
				"data": {
					"kind": "llm",
					"label": "LLM",
					"llmKind": "ollama",
					"params": {
						"baseUrl": "http://localhost:11434",
						"model": "x",
						"user_prompt": "summarize",
						"system_prompt": "",
						"output": {"mode": "text", "strict": True},
					},
					"ports": {"in": "text", "out": "text"},
				},
			},
		],
		"edges": [
			{
				"id": "e_cmp_llm",
				"source": "cmp_node",
				"sourceHandle": "out_data",
				"target": "llm1",
				"targetHandle": "in",
			}
		],
	}


@pytest.mark.asyncio
async def test_e2e_component_edit_workflow_gate_modify_save_return_rerun():
	run_mod = importlib.import_module("app.runner.run")
	store = MemoryArtifactStore()
	cache = ExecutionCache()

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "text", "payload": "component text"},
		)

	async def _fake_exec_llm(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data="downstream ok",
		)

	original_exec_tool = run_mod.exec_tool
	original_exec_llm = run_mod.exec_llm
	run_mod.exec_tool = _fake_exec_tool
	run_mod.exec_llm = _fake_exec_llm
	try:
		runtime_ref = SimpleNamespace(
			component_revisions=_ComponentStoreStub(
				{
					("cmp_edit", "crev_1"): SimpleNamespace(definition=_component_definition()),
					("cmp_edit", "crev_2"): SimpleNamespace(definition=_component_definition()),
				}
			)
		)

		events_run_1: list[dict] = []
		await run_mod.run_graph(
			run_id="run-component-edit-workflow-1",
			graph=_graph("crev_1"),
			run_from=None,
			bus=RunEventBus("run-component-edit-workflow-1", on_emit=lambda e: events_run_1.append(dict(e))),
			artifact_store=store,
			cache=cache,
			runtime_ref=runtime_ref,
			graph_id="graph-component-edit-workflow",
		)
		assert any(e.get("type") == "node_output" and e.get("nodeId") == "llm1" for e in events_run_1)
		assert not any("COMPONENT_OUTPUT_HANDLE_UNRESOLVED" in str(e.get("message") or "") for e in events_run_1)

		# Simulate: edit internals -> save same component name (new revision) -> return to graph -> rerun.
		events_run_2: list[dict] = []
		await run_mod.run_graph(
			run_id="run-component-edit-workflow-2",
			graph=_graph("crev_2"),
			run_from=None,
			bus=RunEventBus("run-component-edit-workflow-2", on_emit=lambda e: events_run_2.append(dict(e))),
			artifact_store=store,
			cache=ExecutionCache(),
			runtime_ref=runtime_ref,
			graph_id="graph-component-edit-workflow",
		)
		assert any(e.get("type") == "node_output" and e.get("nodeId") == "llm1" for e in events_run_2)
		assert not any("COMPONENT_OUTPUT_HANDLE_UNRESOLVED" in str(e.get("message") or "") for e in events_run_2)
	finally:
		run_mod.exec_tool = original_exec_tool
		run_mod.exec_llm = original_exec_llm
