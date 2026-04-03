import os
import re
from datetime import datetime, timezone

import pytest

from app.runner.artifacts import Artifact, DiskArtifactStore, MemoryArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import FileMetadata, NodeOutput


_ARTIFACT_RE = re.compile(r"\bartifactId=([^\s]+)")


def _mk_artifact(*, artifact_id: str, run_id: str, graph_id: str, node_id: str) -> Artifact:
	return Artifact(
		artifact_id=artifact_id,
		node_kind="transform",
		params_hash="params-hash",
		upstream_ids=[],
		created_at=datetime.now(timezone.utc),
		execution_version="v1",
		mime_type="application/json",
		payload_type="json",
		size_bytes=0,
		storage_uri="memory://placeholder",
		payload_schema={
			"type": "json",
			"artifactMetadataV1": {
				"metadataVersion": 1,
				"execKey": artifact_id,
				"nodeId": node_id,
				"nodeType": "transform",
				"nodeImplVersion": "v1",
				"paramsFingerprint": "params",
				"upstreamArtifactIds": [],
				"contractFingerprint": "contract",
				"schemaFingerprint": "schema",
				"mimeType": "application/json",
				"payloadType": "json",
				"createdAt": datetime.now(timezone.utc).isoformat(),
			},
		},
		run_id=run_id,
		graph_id=graph_id,
		node_id=node_id,
		exec_key=artifact_id,
	)


@pytest.mark.asyncio
async def test_no_write_time_prune_keeps_all_node_artifacts_in_run(monkeypatch):
	monkeypatch.setenv("ARTIFACT_RETENTION_MODE", "off")
	store = MemoryArtifactStore()
	run_id = "run-mid"
	graph_id = "graph-mid"
	node_id = "node-mid"
	await store.record_run(run_id, "running")

	artifact_ids = []
	for idx in range(12):
		artifact_id = f"aid-{idx}"
		artifact_ids.append(artifact_id)
		await store.write(
			_mk_artifact(artifact_id=artifact_id, run_id=run_id, graph_id=graph_id, node_id=node_id),
			f'{{"idx":{idx}}}'.encode("utf-8"),
		)

	for artifact_id in artifact_ids:
		assert await store.exists(artifact_id), f"artifact missing unexpectedly: {artifact_id}"


@pytest.mark.asyncio
async def test_run_scoped_retention_prunes_old_terminal_runs_only(monkeypatch, tmp_path):
	monkeypatch.setenv("ARTIFACT_RETENTION_MODE", "by_run")
	monkeypatch.setenv("ARTIFACT_KEEP_RECENT_RUNS", "2")
	monkeypatch.setenv("ARTIFACT_RETENTION_INCLUDE_FAILED", "1")
	monkeypatch.setenv("ARTIFACT_RETENTION_INCLUDE_CANCELED", "1")

	store = DiskArtifactStore(tmp_path / "artifacts-retention")

	# Active run should never be pruned.
	await store.record_run("run-active", "running")
	await store.write(
		_mk_artifact(artifact_id="aid-active", run_id="run-active", graph_id="graph-keep", node_id="node-a"),
		b'{"run":"active"}',
	)

	# Terminal runs.
	for idx in range(1, 5):
		run_id = f"run-{idx}"
		artifact_id = f"aid-{idx}"
		await store.record_run(run_id, "running")
		await store.write(
			_mk_artifact(artifact_id=artifact_id, run_id=run_id, graph_id="graph-keep", node_id="node-a"),
			f'{{"run":{idx}}}'.encode("utf-8"),
		)
		await store.update_run_status(run_id, "succeeded")

	# Keep recent 2 terminal runs => run-3/run-4 kept, run-1/run-2 pruned.
	assert not await store.exists("aid-1")
	assert not await store.exists("aid-2")
	assert await store.exists("aid-3")
	assert await store.exists("aid-4")

	# Active run remains untouched.
	assert await store.exists("aid-active")
	active = await store.get_run("run-active")
	assert isinstance(active, dict)
	assert str(active.get("status") or "") == "running"


@pytest.mark.asyncio
async def test_retention_runs_only_on_terminal_status(monkeypatch):
	monkeypatch.setenv("ARTIFACT_RETENTION_MODE", "by_run")
	monkeypatch.setenv("ARTIFACT_KEEP_RECENT_RUNS", "1")
	store = MemoryArtifactStore()

	for idx in range(2):
		run_id = f"run-nonterminal-{idx}"
		artifact_id = f"aid-nonterminal-{idx}"
		await store.record_run(run_id, "running")
		await store.write(
			_mk_artifact(artifact_id=artifact_id, run_id=run_id, graph_id="g", node_id="n"),
			f'{{"idx":{idx}}}'.encode("utf-8"),
		)
		await store.update_run_status(run_id, "paused")

	assert await store.exists("aid-nonterminal-0")
	assert await store.exists("aid-nonterminal-1")


async def _fake_exec_source_many_json(run_id, node, context, upstream_artifact_ids=None):
	return NodeOutput(
		status="succeeded",
		data=[{"idx": i, "value": f"v-{i}"} for i in range(8)],
		metadata=FileMetadata(
			file_path="memory://source.json",
			file_type="json",
			mime_type="application/json",
			size_bytes=256,
			data_schema={"type": "json"},
			content_hash="source-many-hash",
			node_id=node["id"],
			params_hash="source-many-params",
		),
		execution_time_ms=1.0,
	)


async def _fake_exec_tool_emit_item(run_id, node, context, upstream_artifact_ids=None):
	work_item = (((node.get("data", {}) or {}).get("params", {}) or {}).get("_work_item") or {})
	payload = work_item.get("itemPreview")
	idx = ""
	if isinstance(payload, dict):
		idx = str(payload.get("idx") or "")
	return NodeOutput(
		status="succeeded",
		data={"idx": idx, "echo": payload},
		metadata=FileMetadata(
			file_path=f"memory://tool-{idx or 'x'}.json",
			file_type="json",
			mime_type="application/json",
			size_bytes=128,
			data_schema={"type": "json"},
			content_hash=f"tool-many-{idx or 'x'}",
			node_id=node["id"],
			params_hash=f"tool-many-params-{idx or 'x'}",
		),
		execution_time_ms=1.0,
	)


@pytest.mark.asyncio
async def test_all_emitted_artifact_ids_stay_resolvable_within_run(monkeypatch, tmp_path):
	monkeypatch.setenv("ARTIFACT_RETENTION_MODE", "off")
	run_mod = __import__("app.runner.run", fromlist=["run_graph"])
	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source_many_json)
	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool_emit_item)

	graph = {
		"nodes": [
			{
				"id": "source_many",
				"data": {
					"kind": "source",
					"label": "SourceMany",
					"sourceKind": "file",
					"params": {"rel_path": ".", "filename": "input.json", "file_format": "json"},
				},
			},
			{
				"id": "tool_single",
				"data": {
					"kind": "tool",
					"label": "ToolSingle",
					"processingPolicy": {"consume_mode": "single_item", "batch_size": 1, "max_inflight": 1},
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
		],
		"edges": [
			{
				"id": "e_source_tool",
				"source": "source_many",
				"target": "tool_single",
				"data": {
					"mode": "work",
					"queue": {"max": 1000, "overflow": "block"},
					"work": {"item_mode": "json_items", "max_items": 256},
				},
			}
		],
	}

	events = []
	artifact_root = tmp_path / "artifacts-openable-run"
	artifact_store = DiskArtifactStore(artifact_root)
	await run_mod.run_graph(
		run_id="run-openable-artifacts",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-openable-artifacts", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=artifact_store,
		cache=SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite")),
		graph_id="graph-openable-artifacts",
	)

	output_logs = [
		evt
		for evt in events
		if str(evt.get("type") or "") == "log"
		and str(evt.get("nodeId") or "") == "tool_single"
		and "[output] artifactId=" in str(evt.get("message") or "")
	]
	artifact_ids = []
	for evt in output_logs:
		match = _ARTIFACT_RE.search(str(evt.get("message") or ""))
		if not match:
			continue
		artifact_ids.append(str(match.group(1)))
	assert len(artifact_ids) >= 8
	for artifact_id in artifact_ids:
		assert await artifact_store.exists(artifact_id), f"artifact missing after run completion: {artifact_id}"
