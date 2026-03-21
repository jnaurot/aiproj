import importlib
import json
import json as jsonlib
from typing import Any, Dict

import pytest

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import FileMetadata, NodeOutput


class _GoldenStreamResponse:
	def __init__(self, lines):
		self._lines = lines

	async def __aenter__(self):
		return self

	async def __aexit__(self, exc_type, exc, tb):
		return False

	def raise_for_status(self):
		return None

	async def aiter_lines(self):
		for line in self._lines:
			yield line


class _GoldenResponse:
	def __init__(self, payload):
		self._payload = payload

	def raise_for_status(self):
		return None

	def json(self):
		return self._payload


class _GoldenClient:
	def __init__(self, state):
		self._state = state

	async def __aenter__(self):
		return self

	async def __aexit__(self, exc_type, exc, tb):
		return False

	def stream(self, method, url, json=None, headers=None):
		self._state["payloads"].append(json or {})
		self._state["urls"].append(url)
		mode = str((json or {}).get("response_format", {}).get("type") or "text")
		wants_json = mode == "json_object" or str((json or {}).get("taskKind") or "").strip().lower() in {"extract", "classify"}
		content = '{"ok":true,"entities":["a"]}' if wants_json else "workflow-ok"
		frame = {"choices": [{"delta": {"content": content}}]}
		return _GoldenStreamResponse([f"data: {jsonlib.dumps(frame)}", "data: [DONE]"])

	async def post(self, url, json=None, headers=None):
		self._state["payloads"].append(json or {})
		self._state["urls"].append(url)
		if url.endswith("/v1/embeddings"):
			return _GoldenResponse({"data": [{"embedding": [0.1, 0.2, 0.3]}]})
		return _GoldenResponse({"choices": [{"message": {"content": "workflow-ok"}}]})


async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
	params = ((node.get("data") or {}).get("params") or {}) if isinstance(node, dict) else {}
	file_format = str(params.get("file_format") or "txt").strip().lower()
	if file_format in {"png", "jpg", "jpeg", "webp"}:
		return NodeOutput(
			status="succeeded",
			data=b"\x89PNG\r\n",
			metadata=FileMetadata(
				file_path="memory://input.png",
				file_type="image",
				mime_type="image/png",
				size_bytes=6,
				data_schema={"type": "image"},
				content_hash="image-hash",
				node_id=node["id"],
				params_hash="paramhash",
			),
			execution_time_ms=1.0,
		)
	if file_format in {"wav", "mp3"}:
		return NodeOutput(
			status="succeeded",
			data=b"RIFF\x24\x00\x00\x00WAVEfmt ",
			metadata=FileMetadata(
				file_path="memory://input.wav",
				file_type="binary",
				mime_type="audio/wav",
				size_bytes=16,
				data_schema={"type": "audio"},
				content_hash="audio-hash",
				node_id=node["id"],
				params_hash="paramhash",
			),
			execution_time_ms=1.0,
		)
	return NodeOutput(
		status="succeeded",
		data="sample text input",
		metadata=FileMetadata(
			file_path="memory://input.txt",
			file_type="txt",
			mime_type="text/plain; charset=utf-8",
			size_bytes=17,
			data_schema={"type": "text"},
			content_hash="text-hash",
			node_id=node["id"],
			params_hash="paramhash",
		),
		execution_time_ms=1.0,
	)


def _graph(source_format: str, model_patch: Dict[str, Any]) -> Dict[str, Any]:
	model_params = {
		"base_url": "https://golden.local",
		"model": "gpt-golden",
		"user_prompt": "Process input",
		"output_mode": "text",
	}
	model_params.update(model_patch)
	return {
		"nodes": [
			{
				"id": "source_1",
				"data": {
					"kind": "source",
					"label": "Source",
					"sourceKind": "file",
					"params": {"rel_path": ".", "filename": f"in.{source_format}", "file_format": source_format},
				},
			},
			{
				"id": "model_1",
				"data": {
					"kind": "model",
					"label": "Model",
					"llmKind": "openai_compat",
					"modelKind": str(model_patch.get("modelKind") or "llm"),
					"taskKind": str(model_patch.get("taskKind") or "generate"),
					"params": model_params,
				},
			},
		],
		"edges": [{"id": "e1", "source": "source_1", "target": "model_1"}],
	}


@pytest.mark.asyncio
@pytest.mark.parametrize(
	"workflow_name,source_format,model_patch",
	[
		("text_generation", "txt", {"modelKind": "llm", "taskKind": "generate", "output_mode": "text"}),
		(
			"json_extraction",
			"txt",
			{
				"modelKind": "llm",
				"taskKind": "extract",
				"output_mode": "json",
				"output_schema": {"type": "object", "properties": {"ok": {"type": "boolean"}}},
			},
		),
		(
			"embeddings",
			"txt",
			{
				"modelKind": "embedding",
				"taskKind": "embed",
				"output_mode": "embeddings",
				"embedding_contract": {"dims": 3, "dtype": "float32", "layout": "1d"},
			},
		),
		("vision_caption", "png", {"modelKind": "vision", "taskKind": "caption", "output_mode": "text"}),
		("audio_transcribe", "wav", {"modelKind": "audio", "taskKind": "transcribe", "output_mode": "text"}),
		(
			"multimodal_extract",
			"txt",
			{
				"modelKind": "multimodal",
				"taskKind": "extract",
				"output_mode": "json",
				"input_envelope": [{"type": "image", "dataUrl": "data:image/png;base64,QUJD"}],
				"output_schema": {"type": "object"},
			},
		),
	],
)
async def test_model_golden_workflows_run_green(monkeypatch, tmp_path, workflow_name, source_format, model_patch):
	run_mod = importlib.import_module("app.runner.run")
	openai_mod = importlib.import_module("app.executors.llm_openai_compat")
	state = {"payloads": [], "urls": []}
	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	monkeypatch.setattr(openai_mod.httpx, "AsyncClient", lambda *args, **kwargs: _GoldenClient(state))

	graph = _graph(source_format, model_patch)
	events = []
	artifact_root = tmp_path / f"artifacts-golden-{workflow_name}"
	await run_mod.run_graph(
		run_id=f"run-golden-{workflow_name}",
		graph=graph,
		run_from=None,
		bus=RunEventBus(f"run-golden-{workflow_name}", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=DiskArtifactStore(artifact_root),
		cache=SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite")),
		graph_id="graph-golden-model-workflows",
	)

	output_events = [e for e in events if e.get("type") == "node_output" and e.get("nodeId") == "model_1"]
	assert output_events, f"{workflow_name}: missing model output"
	artifact_id = str(output_events[-1].get("artifactId") or "")
	assert artifact_id
	payload = await DiskArtifactStore(artifact_root).read(artifact_id)
	if model_patch.get("output_mode") == "json":
		json.loads(payload.decode("utf-8"))
	if model_patch.get("output_mode") == "embeddings":
		obj = json.loads(payload.decode("utf-8"))
		assert obj.get("mode") == "embeddings"
