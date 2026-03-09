from __future__ import annotations

import importlib
import sys
import types
from types import SimpleNamespace

import pytest

if "duckdb" not in sys.modules:
	sys.modules["duckdb"] = types.SimpleNamespace()

from app.graph_migrations import canonicalize_graph_payload
from app.runner.artifacts import MemoryArtifactStore
from app.runner.cache import ExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import NodeOutput


class _ComponentStoreStub:
	def __init__(self, revisions):
		self._revisions = revisions

	def get_revision(self, component_id: str, revision_id: str):
		return self._revisions.get((component_id, revision_id))


def _component_definition_multi_output() -> dict:
	return {
		"graph": {
			"nodes": [
				{
					"id": "inner_summary",
					"data": {
						"kind": "tool",
						"label": "Inner Summary",
						"params": {"provider": "builtin", "builtin": {"toolId": "echo", "args": {}}},
						"ports": {"in": "text", "out": "text"},
					},
				},
				{
					"id": "inner_source",
					"data": {
						"kind": "tool",
						"label": "Inner Source",
						"params": {"provider": "builtin", "builtin": {"toolId": "echo", "args": {}}},
						"ports": {"in": "text", "out": "text"},
					},
				},
			],
			"edges": [],
		},
		"api": {
			"inputs": [],
			"outputs": [
				{"name": "summary", "portType": "text", "required": True, "typedSchema": {"type": "text", "fields": []}},
				{"name": "source", "portType": "text", "required": True, "typedSchema": {"type": "text", "fields": []}},
			],
		},
	}


def _graph_component_to_llm(source_handle: str = "summary") -> dict:
	return {
		"nodes": [
			{
				"id": "cmp_node",
				"data": {
					"kind": "component",
					"label": "Component",
					"params": {
						"componentRef": {"componentId": "cmp_multi", "revisionId": "crev_1"},
						"api": {
							"inputs": [],
							"outputs": [
								{"name": "summary", "portType": "text", "required": True, "typedSchema": {"type": "text", "fields": []}},
								{"name": "source", "portType": "text", "required": True, "typedSchema": {"type": "text", "fields": []}},
							],
						},
						"bindings": {
							"inputs": {},
							"outputs": {
								"summary": {"nodeId": "inner_summary", "artifact": "current"},
								"source": {"nodeId": "inner_source", "artifact": "current"},
							},
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
			{"id": "e_cmp_llm", "source": "cmp_node", "sourceHandle": source_handle, "target": "llm1", "targetHandle": "in"},
		],
	}


@pytest.mark.asyncio
@pytest.mark.parametrize("cache_mode,expect_cache_hit", [("default_on", True), ("force_on", True), ("force_off", False)])
async def test_e2e_component_output_routing_matrix_by_cache_mode(cache_mode: str, expect_cache_hit: bool):
	run_mod = importlib.import_module("app.runner.run")
	store = MemoryArtifactStore()
	cache = ExecutionCache()

	async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data="source text")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		inner_id = str(node.get("id") or "")
		payload = "summary text" if inner_id.endswith("inner_summary") else "source text"
		return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data={"kind": "text", "payload": payload})

	async def _fake_exec_llm(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data="ok")

	original_exec_source = run_mod.exec_source
	original_exec_tool = run_mod.exec_tool
	original_exec_llm = run_mod.exec_llm
	run_mod.exec_source = _fake_exec_source
	run_mod.exec_tool = _fake_exec_tool
	run_mod.exec_llm = _fake_exec_llm
	try:
		runtime_ref = SimpleNamespace(
			component_revisions=_ComponentStoreStub({("cmp_multi", "crev_1"): SimpleNamespace(definition=_component_definition_multi_output())}),
			get_global_cache_mode=lambda: cache_mode,
		)

		events_run_1 = []
		await run_mod.run_graph(
			run_id=f"run-matrix-{cache_mode}-1",
			graph=_graph_component_to_llm(source_handle="summary"),
			run_from=None,
			bus=RunEventBus(f"run-matrix-{cache_mode}-1", on_emit=lambda e: events_run_1.append(dict(e))),
			artifact_store=store,
			cache=cache,
			runtime_ref=runtime_ref,
			graph_id=f"graph-matrix-{cache_mode}",
		)
		assert any(e.get("type") == "node_output" and e.get("nodeId") == "llm1" for e in events_run_1)
		assert not any("COMPONENT_OUTPUT_HANDLE_UNRESOLVED" in str(e.get("message") or "") for e in events_run_1)

		events_run_2 = []
		await run_mod.run_graph(
			run_id=f"run-matrix-{cache_mode}-2",
			graph=_graph_component_to_llm(source_handle="summary"),
			run_from=None,
			bus=RunEventBus(f"run-matrix-{cache_mode}-2", on_emit=lambda e: events_run_2.append(dict(e))),
			artifact_store=store,
			cache=cache,
			runtime_ref=runtime_ref,
			graph_id=f"graph-matrix-{cache_mode}",
		)
		cache_decisions = [e for e in events_run_2 if e.get("type") == "cache_decision"]
		hit_count = sum(1 for e in cache_decisions if str(e.get("decision") or "") == "cache_hit")
		if expect_cache_hit:
			assert hit_count > 0
		else:
			assert hit_count == 0
	finally:
		run_mod.exec_source = original_exec_source
		run_mod.exec_tool = original_exec_tool
		run_mod.exec_llm = original_exec_llm


@pytest.mark.asyncio
async def test_e2e_no_wrapper_fallback_for_undeclared_named_output_handle():
	run_mod = importlib.import_module("app.runner.run")
	store = MemoryArtifactStore()
	cache = ExecutionCache()

	async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data="source text")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data={"kind": "text", "payload": "summary text"})

	async def _fake_exec_llm(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data="ok")

	original_exec_source = run_mod.exec_source
	original_exec_tool = run_mod.exec_tool
	original_exec_llm = run_mod.exec_llm
	run_mod.exec_source = _fake_exec_source
	run_mod.exec_tool = _fake_exec_tool
	run_mod.exec_llm = _fake_exec_llm
	try:
		graph = _graph_component_to_llm(source_handle="missing_output")
		events = []
		await run_mod.run_graph(
			run_id="run-no-wrapper-fallback",
			graph=graph,
			run_from=None,
			bus=RunEventBus("run-no-wrapper-fallback", on_emit=lambda e: events.append(dict(e))),
			artifact_store=store,
			cache=cache,
			runtime_ref=SimpleNamespace(
				component_revisions=_ComponentStoreStub({("cmp_multi", "crev_1"): SimpleNamespace(definition=_component_definition_multi_output())})
			),
			graph_id="graph-no-wrapper-fallback",
		)
		assert any(
			e.get("type") == "log"
			and "COMPONENT_OUTPUT_HANDLE_UNRESOLVED" in str(e.get("message") or "")
			for e in events
		)
	finally:
		run_mod.exec_source = original_exec_source
		run_mod.exec_tool = original_exec_tool
		run_mod.exec_llm = original_exec_llm


def test_e2e_migration_contract_recompute_and_idempotency():
	legacy_graph = {
		"version": 1,
		"nodes": [
			{
				"id": "cmp1",
				"type": "component",
				"position": {"x": 0, "y": 0},
				"data": {
					"kind": "component",
					"ports": {"in": None, "out": "json"},
					"params": {
						"api": {
							"inputs": [],
							"outputs": [
								{"name": "summary", "portType": "text", "required": True, "typedSchema": {"type": "text", "fields": []}},
								{"name": "source", "portType": "json", "required": True, "typedSchema": {"type": "json", "fields": []}},
							],
						},
						"bindings": {
							"inputs": {},
							"config": {},
							"outputs": {
								"summary": {"nodeId": "n_summary", "artifact": "current"},
								"source": {"nodeId": "n_source", "artifact": "current"},
							},
						},
						"config": {},
					},
				},
			},
			{
				"id": "llm1",
				"type": "llm",
				"position": {"x": 300, "y": 0},
				"data": {"kind": "llm", "ports": {"in": "text", "out": "text"}, "params": {"model": "x"}},
			},
		],
		"edges": [
			{
				"id": "e1",
				"source": "cmp1",
				"sourceHandle": "out",
				"target": "llm1",
				"targetHandle": "in",
				"data": {
					"contract": {
						"out": "text",
						"in": "text",
						"payload": {"source": {"type": "json"}, "target": {"type": "string"}},
					}
				},
			}
		],
	}
	first, notes_1 = canonicalize_graph_payload(legacy_graph)
	second, notes_2 = canonicalize_graph_payload(first)
	edge = first["edges"][0]
	assert str(edge.get("sourceHandle") or "") == "summary"
	contract = ((edge.get("data") or {}).get("contract") or {})
	assert str(contract.get("out") or "") == "text"
	assert str((((contract.get("payload") or {}).get("source") or {}).get("type") or "")) == "string"
	assert first == second
	assert isinstance(notes_1, list)
	assert isinstance(notes_2, list)
