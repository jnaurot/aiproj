import asyncio
import importlib
import json
import json as jsonlib
import time
import tracemalloc
from pathlib import Path
from typing import Any, Dict

import pytest

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import FileMetadata, NodeOutput


class _BenchStreamResponse:
	def __init__(self, lines, delay_s: float):
		self._lines = lines
		self._delay_s = delay_s

	async def __aenter__(self):
		return self

	async def __aexit__(self, exc_type, exc, tb):
		return False

	def raise_for_status(self):
		return None

	async def aiter_lines(self):
		for line in self._lines:
			if self._delay_s > 0:
				await asyncio.sleep(self._delay_s)
			yield line


class _BenchResponse:
	def __init__(self, payload):
		self._payload = payload

	def raise_for_status(self):
		return None

	def json(self):
		return self._payload


class _BenchClient:
	def __init__(self, delay_s: float):
		self._delay_s = delay_s

	async def __aenter__(self):
		return self

	async def __aexit__(self, exc_type, exc, tb):
		return False

	def stream(self, method, url, json=None, headers=None):
		frame = {"choices": [{"delta": {"content": "ok"}}]}
		return _BenchStreamResponse([f"data: {jsonlib.dumps(frame)}", "data: [DONE]"], self._delay_s)

	async def post(self, url, json=None, headers=None):
		if self._delay_s > 0:
			await asyncio.sleep(self._delay_s)
		if str(url).endswith("/v1/embeddings"):
			return _BenchResponse({"data": [{"embedding": [0.1, 0.2, 0.3]}]})
		return _BenchResponse({"choices": [{"message": {"content": "ok"}}]})


async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
	return NodeOutput(
		status="succeeded",
		data="benchmark text input",
		metadata=FileMetadata(
			file_path="memory://bench-input.txt",
			file_type="txt",
			mime_type="text/plain; charset=utf-8",
			size_bytes=20,
			data_schema={"type": "text"},
			content_hash="bench-hash",
			node_id=node["id"],
			params_hash="bench-params",
		),
		execution_time_ms=1.0,
	)


def _baseline() -> Dict[str, float]:
	path = Path(__file__).resolve().parents[1] / "fixtures" / "model_benchmark_baselines.v1.json"
	parsed = json.loads(path.read_text(encoding="utf-8"))
	return dict(parsed.get("metrics") or {})


def _graph() -> Dict[str, Any]:
	return {
		"nodes": [
			{
				"id": "source_1",
				"data": {
					"kind": "source",
					"label": "Source",
					"sourceKind": "file",
					"params": {"rel_path": ".", "filename": "bench.txt", "file_format": "txt"},
				},
			},
			{
				"id": "model_1",
				"data": {
					"kind": "model",
					"label": "Model",
					"llmKind": "openai_compat",
					"modelKind": "llm",
					"taskKind": "generate",
					"params": {
						"base_url": "https://bench.local",
						"model": "gpt-bench",
						"user_prompt": "Respond quickly",
						"output_mode": "text",
					},
				},
			},
		],
		"edges": [{"id": "e1", "source": "source_1", "target": "model_1"}],
	}


async def _run_once(run_mod, tmp_path: Path, run_id: str, *, cancel_event=None) -> float:
	artifact_root = tmp_path / run_id
	t0 = time.perf_counter()
	await run_mod.run_graph(
		run_id=run_id,
		graph=_graph(),
		run_from=None,
		bus=RunEventBus(run_id),
		artifact_store=DiskArtifactStore(artifact_root),
		cache=SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite")),
		graph_id="graph-model-bench",
		cancel_event=cancel_event,
	)
	return (time.perf_counter() - t0) * 1000.0


@pytest.mark.asyncio
async def test_model_benchmark_regression_gate(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")
	openai_mod = importlib.import_module("app.executors.llm_openai_compat")
	metrics = _baseline()

	# 1) single-run latency
	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	monkeypatch.setattr(openai_mod.httpx, "AsyncClient", lambda *args, **kwargs: _BenchClient(delay_s=0.02))
	latency_ms = await _run_once(run_mod, tmp_path, "bench-single")
	assert latency_ms <= float(metrics["single_run_latency_ms_max"])

	# 2) throughput under fanout
	start = time.perf_counter()
	await asyncio.gather(*[_run_once(run_mod, tmp_path, f"bench-throughput-{i}") for i in range(6)])
	throughput = 6.0 / max(0.001, (time.perf_counter() - start))
	assert throughput >= float(metrics["throughput_runs_per_sec_min"])

	# 3) memory footprint during repeated runs
	tracemalloc.start()
	for i in range(4):
		await _run_once(run_mod, tmp_path, f"bench-memory-{i}")
	_, peak_bytes = tracemalloc.get_traced_memory()
	tracemalloc.stop()
	peak_mb = peak_bytes / (1024.0 * 1024.0)
	assert peak_mb <= float(metrics["peak_memory_mb_max"])

	# 4) cancellation and recovery
	monkeypatch.setattr(openai_mod.httpx, "AsyncClient", lambda *args, **kwargs: _BenchClient(delay_s=0.25))
	cancel_event = asyncio.Event()
	cancel_task = asyncio.create_task(_run_once(run_mod, tmp_path, "bench-cancelled", cancel_event=cancel_event))
	await asyncio.sleep(0.05)
	cancel_t0 = time.perf_counter()
	cancel_event.set()
	cancel_ms = (time.perf_counter() - cancel_t0) * 1000.0
	await cancel_task
	monkeypatch.setattr(openai_mod.httpx, "AsyncClient", lambda *args, **kwargs: _BenchClient(delay_s=0.01))
	recovery_ms = await _run_once(run_mod, tmp_path, "bench-recovery")
	cancellation_recovery_ms = cancel_ms + recovery_ms
	assert cancellation_recovery_ms <= float(metrics["cancellation_recovery_ms_max"])

	# Report artifact for CI inspection/debugging.
	report = {
		"latency_ms": latency_ms,
		"throughput_runs_per_sec": throughput,
		"peak_memory_mb": peak_mb,
		"cancellation_recovery_ms": cancellation_recovery_ms,
	}
	report_path = tmp_path / "model_benchmark_report.json"
	report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
	assert report_path.exists()
