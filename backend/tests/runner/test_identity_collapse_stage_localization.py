from __future__ import annotations

import asyncio
import importlib
import json
import re
import sys
import types
from typing import Any, Dict, List, Optional, Tuple

import pytest

from app.runner.artifacts import MemoryArtifactStore
from app.runner.cache import ExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import NodeOutput
from app.runner.queues import QueueRegistry as BaseQueueRegistry


def _ensure_duckdb_stub() -> None:
	if "duckdb" not in sys.modules:
		sys.modules["duckdb"] = types.SimpleNamespace()


class _InstrumentedQueueRegistry(BaseQueueRegistry):
	def __init__(self, *args: Any, **kwargs: Any):
		super().__init__(*args, **kwargs)
		self.enq_records: List[Tuple[str, str, str]] = []
		self.deq_records: List[Tuple[str, str, str]] = []

	async def enqueue(
		self,
		edge_id: str,
		input_handle: str,
		item: Any,
		*,
		overflow: str = "block",
		timeout_sec: Optional[float] = None,
	) -> bool:
		artifact_id = ""
		if isinstance(item, dict):
			artifact_id = str(item.get("artifactId") or "")
		ok = await super().enqueue(
			edge_id,
			input_handle,
			item,
			overflow=overflow,
			timeout_sec=timeout_sec,
		)
		if ok:
			self.enq_records.append((str(edge_id), str(input_handle), artifact_id))
		return ok

	async def dequeue(self, edge_id: str, input_handle: str, *, timeout_sec: Optional[float] = None) -> Any | None:
		value = await super().dequeue(edge_id, input_handle, timeout_sec=timeout_sec)
		artifact_id = ""
		if isinstance(value, dict):
			artifact_id = str(value.get("artifactId") or "")
		if value is not None:
			self.deq_records.append((str(edge_id), str(input_handle), artifact_id))
		return value


@pytest.mark.asyncio
async def test_localize_identity_collapse_stage_after_downstream_soft_fail(monkeypatch) -> None:
	"""Diagnose where identity collapse begins:
	1) model outputs
	2) edge enqueue items
	3) edge dequeue items
	4) transform input resolution
	"""
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	monkeypatch.setenv("STRICT_SCHEMA_EDGE_CHECKS", "0")
	monkeypatch.setenv("STRICT_SCHEMA_EDGE_CHECKS_V2", "0")

	row_ids = [f"job_{i}" for i in range(6)]
	resume_call_count = {"n": 0}
	instrumented_queue_holder: Dict[str, _InstrumentedQueueRegistry] = {}
	job_desc_resolve_ids: List[str] = []

	# 1) Capture resolve_input_refs artifacts specifically for job_description.
	original_resolve_input_refs = run_mod.resolve_input_refs

	async def _resolve_input_refs_capture(*args: Any, **kwargs: Any):
		node_id = ""
		if len(args) >= 2:
			node_id = str(args[1] or "")
		refs = await original_resolve_input_refs(*args, **kwargs)
		if node_id == "job_description":
			for _handle, aid in refs:
				if str(aid or ""):
					job_desc_resolve_ids.append(str(aid))
		return refs

	monkeypatch.setattr(run_mod, "resolve_input_refs", _resolve_input_refs_capture)

	# 2) Instrument queue enqueue/dequeue item artifact IDs.
	def _queue_registry_factory(*args: Any, **kwargs: Any):
		reg = _InstrumentedQueueRegistry(*args, **kwargs)
		instrumented_queue_holder["registry"] = reg
		return reg

	monkeypatch.setattr(run_mod, "QueueRegistry", _queue_registry_factory)

	# 3) Controlled fake node executors (tool-based harness to avoid model param validation noise).
	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		if node_id == "source_jobs":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={
					"kind": "json",
					"payload": [{"id": rid, "title": f"title-{rid}", "score": 85, "pass": True} for rid in row_ids],
					"meta": {},
				},
			)
		if node_id == "model_score":
			params = ((node.get("data", {}) or {}).get("params", {}) or {})
			work_item = (params.get("_work_item") or {}) if isinstance(params, dict) else {}
			preview = work_item.get("itemPreview") if isinstance(work_item, dict) else {}
			row_id = str((preview or {}).get("id") or "")
			await asyncio.sleep(0.005)
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={
					"kind": "json",
					"payload": {
						"job": {"id": row_id, "title": f"title-{row_id}", "company_name": f"co-{row_id}"},
						"job_id": row_id,
						"pass": True,
						"score": 85,
						"reason": f"ok-{row_id}",
					},
					"meta": {},
				},
			)
		if node_id == "resume_builder":
			resume_call_count["n"] += 1
			if resume_call_count["n"] == 1:
				raise RuntimeError("intentional downstream failure")
			await asyncio.sleep(0.005)
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": {"ok": True}, "meta": {}},
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
			{"id": "source_jobs", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{
				"id": "model_score",
				"data": {
					"kind": "tool",
					"processingPolicy": {"consume_mode": "single_item", "batch_size": 1, "max_inflight": 1},
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
			{
				"id": "job_description",
				"data": {
					"kind": "transform",
					"processingPolicy": {"consume_mode": "single_item", "batch_size": 1, "max_inflight": 1},
					"params": {
						"op": "json_filter",
						"json_filter": {
							"mode": "rules",
							"rules": {
								"kind": "group",
								"op": "all",
								"conditions": [
									{"field": "pass", "op": "=", "value": True},
									{"field": "score", "op": ">=", "value": 70},
								],
							},
							"route_reject": True,
							"include_reject_meta": True,
						},
					},
				},
			},
			{
				"id": "resume_builder",
				"data": {
					"kind": "tool",
					"processingPolicy": {
						"consume_mode": "single_item",
						"batch_size": 1,
						"max_inflight": 1,
						"on_error": "skip_failed",
					},
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
		],
		"edges": [
			{
				"id": "e_source_model",
				"source": "source_jobs",
				"target": "model_score",
				"targetHandle": "in",
				"data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 16}},
			},
			{
				"id": "e_model_job",
				"source": "model_score",
				"target": "job_description",
				"targetHandle": "in",
				"data": {"mode": "work"},
			},
			{
				"id": "e_job_resume",
				"source": "job_description",
				"target": "resume_builder",
				"targetHandle": "in",
				"data": {"mode": "work"},
			},
		],
	}

	events: List[Dict[str, Any]] = []
	await run_mod.run_graph(
		run_id="run-stage-localize",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-stage-localize", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-stage-localize",
	)

	model_output_ids = [
		str(evt.get("artifactId") or "")
		for evt in events
		if evt.get("type") == "node_output" and str(evt.get("nodeId") or "") == "model_score"
	]
	if not model_output_ids:
		node_finished = [
			{
				"nodeId": str(evt.get("nodeId") or ""),
				"status": str(evt.get("status") or ""),
				"error": str(evt.get("error") or ""),
			}
			for evt in events
			if evt.get("type") == "node_finished"
		]
		logs = [
			str(evt.get("message") or "")
			for evt in events
			if evt.get("type") == "log"
		]
		raise AssertionError(
			f"model_score produced no outputs; node_finished={node_finished}; logs={logs[:30]}"
		)
	assert len(model_output_ids) == len(row_ids)
	assert len(set(model_output_ids)) == len(row_ids)

	reg = instrumented_queue_holder.get("registry")
	assert reg is not None
	model_job_enq_ids = [
		aid
		for edge_id, handle, aid in reg.enq_records
		if edge_id == "e_model_job" and handle == "in" and aid
	]
	model_job_deq_ids = [
		aid
		for edge_id, handle, aid in reg.deq_records
		if edge_id == "e_model_job" and handle == "in" and aid
	]
	assert len(model_job_enq_ids) == len(row_ids)
	assert len(model_job_deq_ids) == len(row_ids)

	# Parse transform input-schema artifact IDs as an additional visible signal.
	job_desc_input_schema_ids: List[str] = []
	pattern = re.compile(r'"artifact":"([0-9a-f]{64})"')
	for evt in events:
		if evt.get("type") != "log":
			continue
		if str(evt.get("nodeId") or "") != "job_description":
			continue
		msg = str(evt.get("message") or "")
		if "transform: input-schema" not in msg:
			continue
		m = pattern.search(msg)
		if m:
			job_desc_input_schema_ids.append(m.group(1))

	# Stage-by-stage localization checks.
	assert sorted(model_job_enq_ids) == sorted(model_output_ids), (
		"Collapse starts at enqueue stage (e_model_job queue received non-model identities)"
	)
	assert sorted(model_job_deq_ids) == sorted(model_job_enq_ids), (
		"Collapse starts at dequeue stage (queue dequeue identities differ from enqueued identities)"
	)
	assert sorted(job_desc_resolve_ids) == sorted(model_job_deq_ids), (
		"Collapse starts at input resolution stage (resolve_input_refs diverges from dequeued identities)"
	)
	assert sorted(job_desc_input_schema_ids) == sorted(job_desc_resolve_ids), (
		"Collapse appears between input resolution and transform execution logging"
	)
