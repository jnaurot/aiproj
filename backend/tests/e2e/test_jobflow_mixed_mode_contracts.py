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
async def test_jobflow_mixed_mode_contracts_stream_work_and_snapshot_params(monkeypatch) -> None:
	run_mod = importlib.import_module("app.runner.run")
	select_seen: list[int] = []
	generate_seen: list[int] = []
	select_upstream_sets: list[tuple[str, ...]] = []
	call_counts: dict[str, int] = {
		"jobs_api": 0,
		"resume_file": 0,
		"projects_file": 0,
		"preferences_file": 0,
		"select_jobs": 0,
		"generate_docs": 0,
	}

	async def _extract_job_id(context, node: dict) -> int:
		work_item = ((node.get("data", {}).get("params", {}) or {}).get("_work_item") or {})
		if not isinstance(work_item, dict):
			return -1
		item_preview = work_item.get("itemPreview")
		if isinstance(item_preview, dict):
			if isinstance(item_preview.get("job_id"), int):
				return int(item_preview["job_id"])
			selected_job = item_preview.get("selected_job")
			if isinstance(selected_job, dict) and isinstance(selected_job.get("job_id"), int):
				return int(selected_job["job_id"])
		artifact_id = str(work_item.get("artifactId") or "").strip()
		if not artifact_id:
			return -1
		try:
			payload = await context.artifact_store.read(artifact_id)
			parsed = json.loads(payload.decode("utf-8"))
		except Exception:
			return -1
		if isinstance(parsed, dict):
			obj = parsed.get("payload")
			if isinstance(obj, dict):
				if isinstance(obj.get("job_id"), int):
					return int(obj["job_id"])
				selected = obj.get("selected_job")
				if isinstance(selected, dict) and isinstance(selected.get("job_id"), int):
					return int(selected["job_id"])
		return -1

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node["id"])
		call_counts[node_id] = int(call_counts.get(node_id, 0)) + 1
		if node_id == "jobs_api":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={
					"kind": "json",
					"payload": [
						{"job_id": 101, "title": "ML Engineer"},
						{"job_id": 102, "title": "Data Engineer"},
						{"job_id": 103, "title": "NLP Engineer"},
					],
					"meta": {"ok": True},
				},
			)
		if node_id == "resume_file":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": {"resume": "candidate profile"}, "meta": {"ok": True}},
			)
		if node_id == "projects_file":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": {"projects": ["p1", "p2"]}, "meta": {"ok": True}},
			)
		if node_id == "preferences_file":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": {"location": "remote"}, "meta": {"ok": True}},
			)
		work_item = ((node.get("data", {}).get("params", {}) or {}).get("_work_item") or {})
		item_preview = work_item.get("itemPreview")
		if node_id == "select_jobs":
			job_id = int((item_preview or {}).get("job_id") or -1)
			select_seen.append(job_id)
			select_upstream_sets.append(tuple(sorted(str(a) for a in (upstream_artifact_ids or []))))
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={
					"kind": "json",
					"payload": {"selected_job": item_preview},
					"meta": {"ok": True, "decision": "accept"},
				},
			)
		if node_id == "generate_docs":
			job_id = await _extract_job_id(context, node)
			generate_seen.append(job_id)
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={
					"kind": "json",
					"payload": {"application": {"job_id": job_id}},
					"meta": {"ok": True},
				},
			)
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}, "meta": {"ok": True}},
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
				"data": {
					"kind": "tool",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}, "cache_enabled": False},
				},
			},
			{
				"id": "preferences_file",
				"data": {
					"kind": "tool",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}, "cache_enabled": False},
				},
			},
			{
				"id": "select_jobs",
				"data": {
					"kind": "tool",
					"processingPolicy": {"consume_mode": "single_item", "batch_size": 1, "max_inflight": 1},
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}, "cache_enabled": False},
				},
			},
			{
				"id": "generate_docs",
				"data": {
					"kind": "tool",
					"processingPolicy": {"consume_mode": "single_item", "batch_size": 1, "max_inflight": 1},
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}, "cache_enabled": False},
				},
			},
		],
		"edges": [
			{
				"id": "e_jobs_to_select",
				"source": "jobs_api",
				"target": "select_jobs",
				"targetHandle": "in",
				"data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 50}},
			},
			{
				"id": "e_resume_to_select",
				"source": "resume_file",
				"target": "select_jobs",
				"targetHandle": "param_resume",
				"data": {"mode": "param"},
			},
			{
				"id": "e_projects_to_select",
				"source": "projects_file",
				"target": "select_jobs",
				"targetHandle": "param_projects",
				"data": {"mode": "param"},
			},
			{
				"id": "e_prefs_to_select",
				"source": "preferences_file",
				"target": "select_jobs",
				"targetHandle": "param_prefs",
				"data": {"mode": "param"},
			},
			{
				"id": "e_select_to_generate",
				"source": "select_jobs",
				"target": "generate_docs",
				"targetHandle": "in",
				"data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 50}},
			},
			{
				"id": "e_resume_to_generate",
				"source": "resume_file",
				"target": "generate_docs",
				"targetHandle": "param_resume",
				"data": {"mode": "param"},
			},
			{
				"id": "e_projects_to_generate",
				"source": "projects_file",
				"target": "generate_docs",
				"targetHandle": "param_projects",
				"data": {"mode": "param"},
			},
		],
	}
	events: list[dict] = []
	await run_mod.run_graph(
		run_id="run-jobflow-mixed-mode",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-jobflow-mixed-mode", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-jobflow-mixed-mode",
	)

	assert sorted(select_seen) == [101, 102, 103]
	assert len(generate_seen) == 3
	assert call_counts["jobs_api"] == 1
	assert call_counts["resume_file"] == 1
	assert call_counts["projects_file"] == 1
	assert call_counts["preferences_file"] == 1
	assert call_counts["select_jobs"] == 3
	assert call_counts["generate_docs"] == 3
	assert select_upstream_sets
	assert len(set(select_upstream_sets)) == 1
	assert any(evt.get("type") == "run_finished" and evt.get("status") == "succeeded" for evt in events)
