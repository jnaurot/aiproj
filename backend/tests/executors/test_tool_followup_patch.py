import pytest
import importlib
import sys
import types

from app.executors.tool import exec_tool
from app.runner.artifacts import MemoryArtifactStore, RunBindings
from app.runner.events import RunEventBus
from app.runner.metadata import ExecutionContext, NodeOutput


class _FakeResponse:
    def __init__(self, *, status: int, content_type: str, body: bytes):
        self.status = status
        self.headers = {"content-type": content_type}
        self._body = body

    async def read(self) -> bytes:
        return self._body


class _FakeResponseCtx:
    def __init__(self, response: _FakeResponse):
        self._response = response

    async def __aenter__(self):
        return self._response

    async def __aexit__(self, exc_type, exc, tb):
        return None


class _FakeSession:
    def __init__(self, response: _FakeResponse):
        self._response = response

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    def request(self, method, url, headers=None, json=None):
        return _FakeResponseCtx(self._response)


@pytest.mark.asyncio
async def test_tool_binary_mime_propagates_to_artifact(monkeypatch):
    if "duckdb" not in sys.modules:
        sys.modules["duckdb"] = types.SimpleNamespace()
    run_mod = importlib.import_module("app.runner.run")

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"kind": "binary", "payload": b"\x89PNG\r\n", "mime": "image/png"},
        )

    monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

    run_id = "run-tool-mime"
    events = []
    bus = RunEventBus(run_id, on_emit=lambda evt: events.append(dict(evt)))
    artifact_store = MemoryArtifactStore()

    graph = {
        "nodes": [
            {
                "id": "tool_1",
                "data": {
                    "kind": "tool",
                    "label": "Tool",
                    "params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {}}},
                    "ports": {"in": None, "out": "binary"},
                },
            }
        ],
        "edges": [],
    }

    await run_mod.run_graph(run_id=run_id, graph=graph, run_from=None, bus=bus, artifact_store=artifact_store)

    node_output_events = [e for e in events if e.get("type") == "node_output" and e.get("nodeId") == "tool_1"]
    assert node_output_events, "Expected node_output event for tool node"
    artifact_id = node_output_events[-1]["artifactId"]
    art = await artifact_store.get(artifact_id)
    assert art.mime_type == "image/png"


@pytest.mark.asyncio
async def test_http_json_invalid_body_is_contract_mismatch(monkeypatch):
    fake_response = _FakeResponse(
        status=200,
        content_type="application/json",
        body=b"not-json",
    )

    monkeypatch.setattr("aiohttp.ClientSession", lambda: _FakeSession(fake_response))

    run_id = "run-tool-invalid-json"
    context = ExecutionContext(
        run_id=run_id,
        bus=RunEventBus(run_id),
        artifact_store=MemoryArtifactStore(),
        bindings=RunBindings(run_id),
    )

    node = {
        "id": "tool_http",
        "data": {
            "params": {
                "provider": "http",
                "http": {"url": "https://example.test/data", "method": "GET"},
                "output": {"mode": "json"},
            }
        },
    }

    out = await exec_tool(run_id=run_id, node=node, context=context, upstream_artifact_ids=[])
    assert out.status == "failed"
    assert "contract mismatch" in (out.error or "").lower()
    if "duckdb" not in sys.modules:
        sys.modules["duckdb"] = types.SimpleNamespace()
    run_mod = importlib.import_module("app.runner.run")
    assert run_mod._is_contract_mismatch_error(out.error or "")
