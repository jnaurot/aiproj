import pytest
import importlib

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

    def request(self, method, url, headers=None, params=None, json=None):
        return _FakeResponseCtx(self._response)


@pytest.mark.asyncio
async def test_tool_binary_mime_propagates_to_artifact(monkeypatch):
    run_mod = importlib.import_module("app.runner.run")

    async def _fake_exec_tool(run_id, node, context, input_metadata=None, upstream_artifact_ids=None):
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

    await run_mod.run_graph(
        run_id=run_id,
        graph=graph,
        run_from=None,
        bus=bus,
        artifact_store=artifact_store,
        graph_id="graph_tool_mime",
    )

    assert isinstance(events, list)
    artifact_id = await artifact_store.get_latest_node_artifact(graph_id="graph_tool_mime", node_id="tool_1")
    if artifact_id is not None:
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
        graph_id="graph_tool_http_invalid_json",
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
    run_mod = importlib.import_module("app.runner.run")
    assert run_mod._is_contract_mismatch_error(out.error or "")


@pytest.mark.asyncio
async def test_http_query_is_forwarded_to_request(monkeypatch):
    fake_response = _FakeResponse(
        status=200,
        content_type="application/json",
        body=b'{"ok":true}',
    )
    captured_params = {}

    class _CapturingSession(_FakeSession):
        def request(self, method, url, headers=None, params=None, json=None):
            captured_params["method"] = method
            captured_params["url"] = url
            captured_params["params"] = params
            captured_params["json"] = json
            return _FakeResponseCtx(self._response)

    monkeypatch.setattr("aiohttp.ClientSession", lambda: _CapturingSession(fake_response))

    run_id = "run-tool-http-query"
    context = ExecutionContext(
        run_id=run_id,
        bus=RunEventBus(run_id),
        artifact_store=MemoryArtifactStore(),
        bindings=RunBindings(run_id),
        graph_id="graph_tool_http_query",
    )

    node = {
        "id": "tool_http",
        "data": {
            "params": {
                "provider": "http",
                "http": {
                    "url": "https://example.test/data",
                    "method": "GET",
                    "query": {"q": "alpha", "limit": 10, "exact": True},
                },
                "output": {"mode": "json"},
            }
        },
    }

    out = await exec_tool(run_id=run_id, node=node, context=context, upstream_artifact_ids=[])
    assert out.status == "succeeded"
    assert captured_params["method"] == "GET"
    assert captured_params["url"] == "https://example.test/data"
    assert captured_params["params"] == {"q": "alpha", "limit": 10, "exact": True}
