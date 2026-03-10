from __future__ import annotations

import importlib
import json
import sys
import types
from dataclasses import dataclass
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


def _child_text_component_definition() -> dict:
	return {
		"graph": {
			"nodes": [
				{
					"id": "inner_tool",
					"data": {
						"kind": "tool",
						"label": "Inner Text Tool",
						"params": {"provider": "builtin", "builtin": {"toolId": "echo", "args": {}}},
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
					"name": "text_out",
					"portType": "text",
					"required": True,
					"typedSchema": {"type": "text", "fields": []},
				}
			],
		},
	}


def _parent_wrapper_binding_definition() -> dict:
	return {
		"graph": {
			"nodes": [
				{
					"id": "inner_component",
					"data": {
						"kind": "component",
						"label": "Inner Component",
						"params": {
							"componentRef": {"componentId": "cmp_child_text", "revisionId": "crev_child_text"},
							"bindings": {
								"inputs": {},
								"outputs": {"text_out": {"nodeId": "inner_tool", "artifact": "current"}},
								"config": {},
							},
							"api": {
								"inputs": [],
								"outputs": [
									{
										"name": "text_out",
										"portType": "text",
										"required": True,
										"typedSchema": {"type": "text", "fields": []},
									}
								],
							},
						},
						"ports": {"in": None, "out": None},
					},
				}
			],
			"edges": [],
		},
		"api": {
			"inputs": [],
			"outputs": [
				{
					"name": "text_out",
					"portType": "text",
					"required": True,
					"typedSchema": {"type": "text", "fields": []},
				}
			],
		},
	}


def _top_graph() -> dict:
	return {
		"nodes": [
			{
				"id": "cmp_top",
				"data": {
					"kind": "component",
					"label": "Top Component",
					"params": {
						"componentRef": {"componentId": "cmp_parent_text", "revisionId": "crev_parent_text"},
						"api": {
							"inputs": [],
							"outputs": [
								{
									"name": "text_out",
									"portType": "text",
									"required": True,
									"typedSchema": {"type": "text", "fields": []},
								}
							],
						},
						"bindings": {
							"inputs": {},
							"outputs": {"text_out": {"nodeId": "inner_component", "artifact": "current"}},
							"config": {},
						},
						"config": {},
					},
					"ports": {"in": None, "out": None},
				},
			}
		],
		"edges": [],
	}


def _wrapper_outputs_from_artifact(raw: bytes | bytearray | str) -> dict:
	if isinstance(raw, (bytes, bytearray)):
		payload = json.loads(raw.decode("utf-8"))
	else:
		payload = json.loads(str(raw))
	root = payload if isinstance(payload, dict) else {}
	if isinstance(root.get("payload"), dict):
		root = root.get("payload")
	outputs = root.get("outputs")
	return outputs if isinstance(outputs, dict) else {}


@dataclass(frozen=True)
class _CacheScenario:
	mode: str
	expect_cache_hit: bool


@pytest.mark.asyncio
@pytest.mark.parametrize(
	"scenario",
	[
		_CacheScenario(mode="default_on", expect_cache_hit=True),
		_CacheScenario(mode="force_off", expect_cache_hit=False),
	],
	ids=["cache_default_on", "cache_force_off"],
)
async def test_component_nested_schema_golden_cache_and_wrapper_contracts(scenario: _CacheScenario, monkeypatch):
	run_mod = importlib.import_module("app.runner.run")
	store = MemoryArtifactStore()
	cache = ExecutionCache()

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "text", "payload": f"payload:{node['id']}", "meta": {"status": "ok"}},
		)

	original_exec_tool = run_mod.exec_tool
	run_mod.exec_tool = _fake_exec_tool
	try:
		runtime_ref = SimpleNamespace(
			component_revisions=_ComponentStoreStub(
				{
					("cmp_parent_text", "crev_parent_text"): SimpleNamespace(
						definition=_parent_wrapper_binding_definition()
					),
					("cmp_child_text", "crev_child_text"): SimpleNamespace(
						definition=_child_text_component_definition()
					),
				}
			),
			get_global_cache_mode=lambda: scenario.mode,
		)

		events_1: list[dict] = []
		await run_mod.run_graph(
			run_id=f"run-nested-schema-{scenario.mode}-1",
			graph=_top_graph(),
			run_from=None,
			bus=RunEventBus(
				f"run-nested-schema-{scenario.mode}-1", on_emit=lambda e: events_1.append(dict(e))
			),
			artifact_store=store,
			cache=cache,
			runtime_ref=runtime_ref,
			graph_id=f"graph-nested-schema-{scenario.mode}",
		)
		assert any(e.get("type") == "run_finished" and e.get("status") == "succeeded" for e in events_1)
		parent_out_1 = next(
			e for e in events_1 if e.get("type") == "node_output" and e.get("nodeId") == "cmp_top"
		)
		parent_art_1 = await store.get(str(parent_out_1.get("artifactId") or ""))
		parent_raw_1 = await store.read(str(parent_art_1.artifact_id))
		outputs_1 = _wrapper_outputs_from_artifact(parent_raw_1)
		text_out_1 = outputs_1.get("text_out") if isinstance(outputs_1.get("text_out"), dict) else {}
		contract_summary_1 = {
			"port_type": str(text_out_1.get("port_type") or ""),
			"typed_schema_expected": str((text_out_1.get("typed_schema_expected") or {}).get("type") or ""),
			"typed_schema_observed": str((text_out_1.get("typed_schema_observed") or {}).get("type") or ""),
		}
		assert contract_summary_1 == {
			"port_type": "text",
			"typed_schema_expected": "text",
			"typed_schema_observed": "text",
		}

		events_2: list[dict] = []
		await run_mod.run_graph(
			run_id=f"run-nested-schema-{scenario.mode}-2",
			graph=_top_graph(),
			run_from=None,
			bus=RunEventBus(
				f"run-nested-schema-{scenario.mode}-2", on_emit=lambda e: events_2.append(dict(e))
			),
			artifact_store=store,
			cache=cache,
			runtime_ref=runtime_ref,
			graph_id=f"graph-nested-schema-{scenario.mode}",
		)
		assert any(e.get("type") == "run_finished" and e.get("status") == "succeeded" for e in events_2)
		cache_hits = [
			e
			for e in events_2
			if e.get("type") == "cache_decision" and str(e.get("decision") or "") == "cache_hit"
		]
		if scenario.expect_cache_hit:
			assert cache_hits
		else:
			assert not cache_hits

		parent_out_2 = next(
			e for e in events_2 if e.get("type") == "node_output" and e.get("nodeId") == "cmp_top"
		)
		parent_art_2 = await store.get(str(parent_out_2.get("artifactId") or ""))
		parent_raw_2 = await store.read(str(parent_art_2.artifact_id))
		outputs_2 = _wrapper_outputs_from_artifact(parent_raw_2)
		text_out_2 = outputs_2.get("text_out") if isinstance(outputs_2.get("text_out"), dict) else {}
		contract_summary_2 = {
			"port_type": str(text_out_2.get("port_type") or ""),
			"typed_schema_expected": str((text_out_2.get("typed_schema_expected") or {}).get("type") or ""),
			"typed_schema_observed": str((text_out_2.get("typed_schema_observed") or {}).get("type") or ""),
		}
		assert contract_summary_2 == contract_summary_1
	finally:
		run_mod.exec_tool = original_exec_tool
