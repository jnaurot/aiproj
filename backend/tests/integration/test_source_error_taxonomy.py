import pytest

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.run import run_graph


@pytest.mark.asyncio
async def test_source_failure_event_contains_error_code_and_details(tmp_path):
	events: list[dict] = []
	artifact_root = tmp_path / "artifact-root-source-errors"
	source_dir = tmp_path / "source-errors"
	source_dir.mkdir(parents=True, exist_ok=True)
	bus = RunEventBus("run-source-errors-1", on_emit=lambda e: events.append(dict(e)))

	await run_graph(
		run_id="run-source-errors-1",
		graph={
			"nodes": [
				{
					"id": "source_missing",
					"data": {
						"kind": "source",
						"label": "Source",
						"sourceKind": "file",
						"params": {
							"rel_path": str(source_dir),
							"filename": "missing.txt",
							"file_format": "txt",
							"output": {"mode": "text"},
						},
					},
				}
			],
			"edges": [],
		},
		run_from=None,
		bus=bus,
		artifact_store=DiskArtifactStore(artifact_root),
		cache=SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite")),
		graph_id="graph-source-errors-1",
	)

	node_finished = [
		e for e in events if e.get("type") == "node_finished" and e.get("nodeId") == "source_missing"
	]
	assert node_finished
	last = node_finished[-1]
	assert str(last.get("status") or "") == "failed"
	assert str(last.get("errorCode") or "") == "SOURCE_NOT_FOUND"
	assert isinstance(last.get("errorDetails"), dict)
	assert str((last.get("errorDetails") or {}).get("sourceKind") or "") == "file"


@pytest.mark.asyncio
async def test_source_node_failure_uses_human_error_text_not_json_blob(tmp_path, monkeypatch):
	events: list[dict] = []
	artifact_root = tmp_path / "artifact-root-source-errors-2"
	bus = RunEventBus("run-source-errors-2", on_emit=lambda e: events.append(dict(e)))

	import app.executors.source as source_mod

	async def _boom(*_args, **_kwargs):
		raise ValueError("MISSING_SECRET: simulated missing secret")

	monkeypatch.setattr(source_mod, "_handle_api_source", _boom)

	await run_graph(
		run_id="run-source-errors-2",
		graph={
			"nodes": [
				{
					"id": "source_invalid",
					"data": {
						"kind": "source",
						"label": "Source",
						"sourceKind": "api",
						"params": {
							"source_type": "api",
							"url": "https://example.com",
							"method": "GET",
							"auth_type": "none",
							"output": {"mode": "json"},
						},
					},
				}
			],
			"edges": [],
		},
		run_from=None,
		bus=bus,
		artifact_store=DiskArtifactStore(artifact_root),
		cache=SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite")),
		graph_id="graph-source-errors-2",
	)

	node_finished = [
		e for e in events if e.get("type") == "node_finished" and e.get("nodeId") == "source_invalid"
	]
	assert node_finished
	last = node_finished[-1]
	assert str(last.get("status") or "") == "failed"
	assert str(last.get("errorCode") or "") == "SOURCE_CONFIG_INVALID"
	error_text = str(last.get("error") or "")
	assert error_text and error_text.strip().startswith("{")
	assert "missing_secret" in error_text.lower()
	assert isinstance(last.get("errorDetails"), dict)
	assert str((last.get("errorDetails") or {}).get("sourceKind") or "") == "api"


@pytest.mark.asyncio
async def test_source_node_output_failure_contains_structured_error(tmp_path):
	events: list[dict] = []
	artifact_root = tmp_path / "artifact-root-source-errors-3"
	source_dir = tmp_path / "source-errors-3"
	source_dir.mkdir(parents=True, exist_ok=True)
	bus = RunEventBus("run-source-errors-3", on_emit=lambda e: events.append(dict(e)))

	await run_graph(
		run_id="run-source-errors-3",
		graph={
			"nodes": [
				{
					"id": "source_missing_structured",
					"data": {
						"kind": "source",
						"label": "Source",
						"sourceKind": "file",
						"params": {
							"rel_path": str(source_dir),
							"filename": "missing.csv",
							"file_format": "csv",
							"output": {"mode": "table"},
						},
					},
				}
			],
			"edges": [],
		},
		run_from=None,
		bus=bus,
		artifact_store=DiskArtifactStore(artifact_root),
		cache=SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite")),
		graph_id="graph-source-errors-3",
	)

	node_finished = [
		e for e in events if e.get("type") == "node_finished" and e.get("nodeId") == "source_missing_structured"
	]
	assert node_finished
	last = node_finished[-1]
	assert str(last.get("status") or "") == "failed"
	assert str(last.get("errorCode") or "") == "SOURCE_NOT_FOUND"
	assert isinstance(last.get("errorDetails"), dict)
	assert str((last.get("errorDetails") or {}).get("sourceKind") or "") == "file"
	assert str((last.get("errorDetails") or {}).get("exceptionType") or "").strip() != ""
