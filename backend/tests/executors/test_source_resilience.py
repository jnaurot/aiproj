import json
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

import httpx
import pytest

from app.executors.source import exec_source


def _ctx():
	async def _emit(*_args, **_kwargs):
		return None

	return SimpleNamespace(
		bus=SimpleNamespace(emit=_emit),
		artifact_store=SimpleNamespace(),
		graph_id="graph_test",
	)


def _error_payload(raw_error: str) -> dict:
	text = str(raw_error or "")
	return json.loads(text) if text.startswith("{") else {"message": text}


@pytest.mark.asyncio
async def test_source_api_retry_policy_applied(monkeypatch):
	attempts = {"count": 0}

	class _Resp:
		status_code = 200
		headers = {"content-type": "application/json"}

		def raise_for_status(self):
			return None

		def json(self):
			return {"ok": True}

		@property
		def text(self):
			return '{"ok":true}'

	class _Client:
		async def __aenter__(self):
			return self

		async def __aexit__(self, exc_type, exc, tb):
			return False

		async def request(self, **kwargs):
			attempts["count"] += 1
			if attempts["count"] == 1:
				raise httpx.ConnectError("transient", request=httpx.Request("GET", kwargs.get("url", "https://example.com")))
			return _Resp()

	monkeypatch.setattr("app.executors.source.httpx.AsyncClient", _Client)
	node = {
		"id": "src_retry",
		"data": {
			"sourceKind": "api",
			"params": {"url": "https://example.com", "method": "GET", "retry": {"max_attempts": 2, "backoff_seconds": 0, "jitter_seconds": 0}},
		},
	}
	result = await exec_source("run-source-api-retry-applied", node, _ctx())
	assert result.status == "succeeded"
	assert attempts["count"] == 2


@pytest.mark.asyncio
async def test_source_api_retry_stops_at_max_attempts(monkeypatch):
	attempts = {"count": 0}

	class _Client:
		async def __aenter__(self):
			return self

		async def __aexit__(self, exc_type, exc, tb):
			return False

		async def request(self, **kwargs):
			attempts["count"] += 1
			raise httpx.ConnectError("still failing", request=httpx.Request("GET", kwargs.get("url", "https://example.com")))

	monkeypatch.setattr("app.executors.source.httpx.AsyncClient", _Client)
	node = {
		"id": "src_retry_limit",
		"data": {
			"sourceKind": "api",
			"params": {"url": "https://example.com", "method": "GET", "retry": {"max_attempts": 3, "backoff_seconds": 0, "jitter_seconds": 0}},
		},
	}
	result = await exec_source("run-source-api-retry-limit", node, _ctx())
	assert result.status == "failed"
	assert attempts["count"] == 3
	payload = _error_payload(str(result.error or ""))
	assert payload.get("errorCode") == "SOURCE_CONNECTION_FAILED"


@pytest.mark.asyncio
async def test_source_partition_skip_failed_reports_failed_partitions(monkeypatch):
	class _Resp:
		status_code = 200
		headers = {"content-type": "application/json"}

		def raise_for_status(self):
			return None

		def json(self):
			return [{"id": 1}]

		@property
		def text(self):
			return '[{"id":1}]'

	class _Client:
		async def __aenter__(self):
			return self

		async def __aexit__(self, exc_type, exc, tb):
			return False

		async def request(self, **kwargs):
			query = parse_qs(urlparse(str(kwargs.get("url", ""))).query)
			part = str((query.get("partition") or [""])[0])
			if part == "2":
				raise httpx.ConnectError("partition down", request=httpx.Request("GET", kwargs.get("url", "https://example.com")))
			return _Resp()

	monkeypatch.setattr("app.executors.source.httpx.AsyncClient", _Client)
	node = {
		"id": "src_partition_skip",
		"data": {
			"sourceKind": "api",
			"params": {
				"url": "https://example.com/data",
				"method": "GET",
				"output": {"mode": "json"},
				"partition": {
					"enabled": True,
					"kind": "static_list",
					"static_values": [1, 2, 3],
					"on_error": "skip_failed",
					"bind_key": "partition",
					"parallelism_cap": 2,
				},
				"retry": {"max_attempts": 1},
			},
		},
	}
	result = await exec_source("run-source-partition-skip", node, _ctx())
	assert result.status == "succeeded"
	partitions = ((result.metadata.data_schema or {}).get("partitions") or {})
	failed = partitions.get("failed") if isinstance(partitions, dict) else []
	assert isinstance(failed, list)
	assert len(failed) == 1
	assert str((failed[0] or {}).get("partition_id") or "") == "2"


@pytest.mark.asyncio
async def test_source_partition_fail_fast_stops_on_first_failure(monkeypatch):
	attempts = {"count": 0}

	class _Resp:
		status_code = 200
		headers = {"content-type": "application/json"}

		def raise_for_status(self):
			return None

		def json(self):
			return [{"id": 1}]

		@property
		def text(self):
			return '[{"id":1}]'

	class _Client:
		async def __aenter__(self):
			return self

		async def __aexit__(self, exc_type, exc, tb):
			return False

		async def request(self, **kwargs):
			attempts["count"] += 1
			query = parse_qs(urlparse(str(kwargs.get("url", ""))).query)
			part = str((query.get("partition") or [""])[0])
			if part == "1":
				raise httpx.ConnectError("first partition failed", request=httpx.Request("GET", kwargs.get("url", "https://example.com")))
			return _Resp()

	monkeypatch.setattr("app.executors.source.httpx.AsyncClient", _Client)
	node = {
		"id": "src_partition_fail_fast",
		"data": {
			"sourceKind": "api",
			"params": {
				"url": "https://example.com/data",
				"method": "GET",
				"partition": {
					"enabled": True,
					"kind": "static_list",
					"static_values": [1, 2, 3],
					"on_error": "fail_fast",
					"bind_key": "partition",
					"parallelism_cap": 1,
				},
				"retry": {"max_attempts": 1},
			},
		},
	}
	result = await exec_source("run-source-partition-fail-fast", node, _ctx())
	assert result.status == "failed"
	assert attempts["count"] == 1
	assert "PARTITION_FAILED" in str(result.error or "")


@pytest.mark.asyncio
async def test_source_timeout_produces_source_timeout_error(monkeypatch):
	class _Client:
		async def __aenter__(self):
			return self

		async def __aexit__(self, exc_type, exc, tb):
			return False

		async def request(self, **kwargs):
			raise httpx.TimeoutException("slow source")

	monkeypatch.setattr("app.executors.source.httpx.AsyncClient", _Client)
	node = {
		"id": "src_timeout",
		"data": {"sourceKind": "api", "params": {"url": "https://example.com", "method": "GET", "retry": {"max_attempts": 1}}},
	}
	result = await exec_source("run-source-timeout", node, _ctx())
	assert result.status == "failed"
	payload = _error_payload(str(result.error or ""))
	assert payload.get("errorCode") == "SOURCE_TIMEOUT"
