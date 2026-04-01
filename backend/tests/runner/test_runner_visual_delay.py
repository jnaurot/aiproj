import importlib
import types

import pytest

from app.runner.artifacts import MemoryArtifactStore
from app.runner.cache import ExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import NodeOutput


def _simple_source_graph() -> dict:
	return {
		"nodes": [
			{
				"id": "source_1",
				"data": {
					"kind": "source",
					"label": "Source",
					"sourceKind": "file",
					"params": {"file_path": "dummy.txt", "file_format": "txt"},
				},
			}
		],
		"edges": [],
	}


async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
	return NodeOutput(
		status="succeeded",
		metadata=None,
		execution_time_ms=1.0,
		data="ok",
	)


@pytest.mark.asyncio
async def test_runner_no_visual_delay_by_default(monkeypatch):
	run_mod = importlib.import_module("app.runner.run")
	monkeypatch.delenv("RUNNER_VISUAL_DELAY_MS", raising=False)
	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	assert run_mod._runner_visual_delay_seconds() == 0.0

	sleep_calls: list[float] = []
	orig_sleep = run_mod.asyncio.sleep

	async def _sleep_spy(delay):
		sleep_calls.append(float(delay))
		await orig_sleep(0)

	monkeypatch.setattr(run_mod.asyncio, "sleep", _sleep_spy)
	await run_mod.run_graph(
		run_id="run-visual-delay-default",
		graph=_simple_source_graph(),
		run_from=None,
		bus=RunEventBus("run-visual-delay-default", on_emit=lambda evt: None),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-visual-delay-default",
		runtime_ref=types.SimpleNamespace(get_global_cache_mode=lambda: "force_off"),
	)
	assert not any(delay >= 0.1 for delay in sleep_calls)


@pytest.mark.asyncio
async def test_runner_optional_visual_delay_applies_when_env_set(monkeypatch):
	run_mod = importlib.import_module("app.runner.run")
	monkeypatch.setenv("RUNNER_VISUAL_DELAY_MS", "120")
	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	assert run_mod._runner_visual_delay_seconds() == pytest.approx(0.12)

	sleep_calls: list[float] = []
	orig_sleep = run_mod.asyncio.sleep

	async def _sleep_spy(delay):
		sleep_calls.append(float(delay))
		await orig_sleep(0)

	monkeypatch.setattr(run_mod.asyncio, "sleep", _sleep_spy)
	await run_mod.run_graph(
		run_id="run-visual-delay-enabled",
		graph=_simple_source_graph(),
		run_from=None,
		bus=RunEventBus("run-visual-delay-enabled", on_emit=lambda evt: None),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-visual-delay-enabled",
		runtime_ref=types.SimpleNamespace(get_global_cache_mode=lambda: "force_off"),
	)
	assert any(abs(delay - 0.12) < 0.001 for delay in sleep_calls)


@pytest.mark.asyncio
async def test_transform_execution_order_unchanged_without_visual_delay(monkeypatch):
	run_mod = importlib.import_module("app.runner.run")
	monkeypatch.delenv("RUNNER_VISUAL_DELAY_MS", raising=False)
	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	events: list[dict] = []
	await run_mod.run_graph(
		run_id="run-visual-delay-order",
		graph=_simple_source_graph(),
		run_from=None,
		bus=RunEventBus("run-visual-delay-order", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-visual-delay-order",
		runtime_ref=types.SimpleNamespace(get_global_cache_mode=lambda: "force_off"),
	)
	start_idx = next(i for i, e in enumerate(events) if e.get("type") == "node_started")
	finish_idx = next(i for i, e in enumerate(events) if e.get("type") == "node_finished")
	assert start_idx < finish_idx


@pytest.mark.asyncio
async def test_transform_throughput_not_artificially_throttled_by_runner_delay(monkeypatch):
	run_mod = importlib.import_module("app.runner.run")
	monkeypatch.delenv("RUNNER_VISUAL_DELAY_MS", raising=False)
	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)

	sleep_calls: list[float] = []
	orig_sleep = run_mod.asyncio.sleep

	async def _sleep_spy(delay):
		sleep_calls.append(float(delay))
		await orig_sleep(0)

	monkeypatch.setattr(run_mod.asyncio, "sleep", _sleep_spy)
	await run_mod.run_graph(
		run_id="run-visual-delay-throughput",
		graph=_simple_source_graph(),
		run_from=None,
		bus=RunEventBus("run-visual-delay-throughput", on_emit=lambda evt: None),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-visual-delay-throughput",
		runtime_ref=types.SimpleNamespace(get_global_cache_mode=lambda: "force_off"),
	)
	assert not any(abs(delay - 0.5) < 0.001 for delay in sleep_calls)

