from __future__ import annotations

import importlib
import json
import sys
import types

import pytest

from app.runner.artifacts import MemoryArtifactStore
from app.runner.cache import ExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import NodeOutput


if "duckdb" not in sys.modules:
	sys.modules["duckdb"] = types.SimpleNamespace()


@pytest.mark.asyncio
async def test_jobflow_port_schema_golden_work_param_control(monkeypatch) -> None:
	run_mod = importlib.import_module("app.runner.run")
	select_seen: list[int] = []
	generate_seen: list[int] = []

	async def _extract_job_id(context, node: dict) -> int:
		work_item = ((node.get("data", {}).get("params", {}) or {}).get("_work_item") or {})
		if not isinstance(work_item, dict):
			return -1
		item_preview = work_item.get("itemPreview")
		if isinstance(item_preview, dict):
			job_id = item_preview.get("job_id")
			if isinstance(job_id, int):
				return job_id
		artifact_id = str(work_item.get("artifactId") or "").strip()
		if not artifact_id:
			return -1
		try:
			payload = await context.artifact_store.read(artifact_id)
			parsed = json.loads(payload.decode("utf-8"))
		except Exception:
			return -1
		if isinstance(parsed, dict):
			value = parsed.get("payload")
			if isinstance(value, dict):
				job_id = value.get("job_id")
				if isinstance(job_id, int):
					return job_id
		return -1

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node["id"])
		if node_id == "jobs_api":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": [{"job_id": 1}, {"job_id": 2}], "meta": {"ok": True}},
			)
		if node_id in {"resume_file", "projects_file", "preferences_file", "control_src"}:
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": {"node": node_id}, "meta": {"ok": True}},
			)
		job_id = await _extract_job_id(context, node)
		if node_id == "select_jobs":
			select_seen.append(job_id)
		if node_id == "generate_docs":
			generate_seen.append(job_id)
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"job_id": job_id}, "meta": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	graph = {
		"nodes": [
			{
				"id": "jobs_api",
				"data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}, "cache_enabled": False}},
			},
			{
				"id": "resume_file",
				"data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}, "cache_enabled": False}},
			},
			{
				"id": "projects_file",
				"data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}, "cache_enabled": False}},
			},
			{
				"id": "preferences_file",
				"data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}, "cache_enabled": False}},
			},
			{
				"id": "control_src",
				"data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}, "cache_enabled": False}},
			},
			{
				"id": "control_sink",
				"data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}, "cache_enabled": False}},
			},
			{
				"id": "select_jobs",
				"data": {
					"kind": "tool",
					"processingPolicy": {"consume_mode": "single_item", "batch_size": 1, "max_inflight": 1},
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}, "cache_enabled": False},
					"schema": {
						"expectedInputSchemas": {
							"in": {"typedSchema": {"type": "json", "fields": []}, "source": "declared", "state": "fresh"},
							"param_resume": {"typedSchema": {"type": "json", "fields": []}, "source": "declared", "state": "fresh"},
							"param_projects": {"typedSchema": {"type": "json", "fields": []}, "source": "declared", "state": "fresh"},
							"param_prefs": {"typedSchema": {"type": "json", "fields": []}, "source": "declared", "state": "fresh"},
						}
					},
				},
			},
			{
				"id": "generate_docs",
				"data": {
					"kind": "tool",
					"processingPolicy": {"consume_mode": "single_item", "batch_size": 1, "max_inflight": 1},
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}, "cache_enabled": False},
					"schema": {
						"expectedInputSchemas": {
							"in": {"typedSchema": {"type": "json", "fields": []}, "source": "declared", "state": "fresh"},
							"param_resume": {"typedSchema": {"type": "json", "fields": []}, "source": "declared", "state": "fresh"},
							"param_projects": {"typedSchema": {"type": "json", "fields": []}, "source": "declared", "state": "fresh"},
						}
					},
				},
			},
		],
		"edges": [
			{"id": "e_jobs_to_select", "source": "jobs_api", "target": "select_jobs", "targetHandle": "in", "data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 10}}},
			{"id": "e_resume_to_select", "source": "resume_file", "target": "select_jobs", "targetHandle": "param_resume", "data": {"mode": "param"}},
			{"id": "e_projects_to_select", "source": "projects_file", "target": "select_jobs", "targetHandle": "param_projects", "data": {"mode": "param"}},
			{"id": "e_prefs_to_select", "source": "preferences_file", "target": "select_jobs", "targetHandle": "param_prefs", "data": {"mode": "param"}},
			{"id": "e_control", "source": "control_src", "sourceHandle": "control_out", "target": "control_sink", "targetHandle": "control_gate", "data": {"mode": "control"}},
			{"id": "e_select_to_generate", "source": "select_jobs", "target": "generate_docs", "targetHandle": "in", "data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 10}}},
			{"id": "e_resume_to_generate", "source": "resume_file", "target": "generate_docs", "targetHandle": "param_resume", "data": {"mode": "param"}},
			{"id": "e_projects_to_generate", "source": "projects_file", "target": "generate_docs", "targetHandle": "param_projects", "data": {"mode": "param"}},
		],
	}
	events: list[dict] = []
	await run_mod.run_graph(
		run_id="run-jobflow-port-schema-golden",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-jobflow-port-schema-golden", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-jobflow-port-schema-golden",
	)

	assert sorted(select_seen) == [1, 2]
	# Runtime scheduling may materialize one or more downstream generate executions
	# depending on queue batching/consume strategy, but generated items must remain valid.
	assert len(generate_seen) >= 1
	assert all(isinstance(job_id, int) and job_id > 0 for job_id in generate_seen)
	assert any(evt.get("type") == "run_finished" and evt.get("status") == "succeeded" for evt in events)
	assert not any("TYPE_MISMATCH" in str(evt) for evt in events if evt.get("type") == "log")
