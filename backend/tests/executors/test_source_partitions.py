import asyncio
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest

from app.executors.source import _merge_partition_results, _plan_partitions, exec_source


def _ctx():
	async def _emit(*_args, **_kwargs):
		return None

	return SimpleNamespace(
		bus=SimpleNamespace(emit=_emit),
		artifact_store=SimpleNamespace(),
		graph_id="graph_partition_test",
	)


def test_partition_planner_static_numeric_and_date():
	static_plan = _plan_partitions({"enabled": True, "kind": "static_list", "static_values": ["a", "b"]})
	assert [p["partition_id"] for p in static_plan] == ["a", "b"]

	numeric_plan = _plan_partitions(
		{"enabled": True, "kind": "numeric_shards", "numeric_start": 1, "numeric_end": 3, "numeric_step": 1}
	)
	assert [p["value"] for p in numeric_plan] == [1, 2, 3]

	date_plan = _plan_partitions(
		{
			"enabled": True,
			"kind": "date_range",
			"date_start": "2026-01-01",
			"date_end": "2026-01-03",
			"date_every_days": 1,
		}
	)
	assert [p["partition_id"] for p in date_plan] == ["2026-01-01", "2026-01-02", "2026-01-03"]


def test_partition_merge_is_deterministic_by_index():
	merged = _merge_partition_results(
		"table",
		[
			{"index": 2, "partition_id": "p2", "data": [{"value": "c"}]},
			{"index": 0, "partition_id": "p0", "data": [{"value": "a"}]},
			{"index": 1, "partition_id": "p1", "data": [{"value": "b"}]},
		],
	)
	assert [row.get("value") for row in merged] == ["a", "b", "c"]
	assert [row.get("__partition_id") for row in merged] == ["p0", "p1", "p2"]


@pytest.mark.asyncio
async def test_partition_api_exec_runs_concurrently(monkeypatch):
	concurrency = {"active": 0, "peak": 0}

	class _Resp:
		def __init__(self, value: str):
			self._value = value
			self.headers = {"content-type": "application/json"}

		def raise_for_status(self):
			return None

		def json(self):
			return [{"value": self._value}]

		@property
		def text(self):
			return f'{{"value":"{self._value}"}}'

	class _Client:
		async def __aenter__(self):
			return self

		async def __aexit__(self, exc_type, exc, tb):
			return False

		async def request(self, **kwargs):
			url = str(kwargs.get("url") or "")
			parsed = parse_qs(urlsplit(url).query)
			value = str((parsed.get("partition") or [""])[0])
			concurrency["active"] += 1
			concurrency["peak"] = max(concurrency["peak"], concurrency["active"])
			await asyncio.sleep(0.03)
			concurrency["active"] -= 1
			return _Resp(value)

	monkeypatch.setattr("app.executors.source.httpx.AsyncClient", _Client)
	node = {
		"id": "n_api_partitions",
		"data": {
			"sourceKind": "api",
			"params": {
				"url": "https://example.com/data",
				"method": "GET",
				"output": {"mode": "table"},
				"partition": {
					"enabled": True,
					"kind": "static_list",
					"bind_key": "partition",
					"on_error": "skip_failed",
					"parallelism_cap": 3,
					"static_values": [1, 2, 3],
				},
			},
		},
	}
	result = await exec_source("run_partition_api", node, _ctx())
	assert result.status == "succeeded"
	assert isinstance(result.data, list)
	assert len(result.data) == 3
	assert concurrency["peak"] > 1


@pytest.mark.asyncio
async def test_partition_api_failure_reports_partition_context(monkeypatch):
	class _Resp:
		def __init__(self, status_code: int, value: str):
			self.status_code = status_code
			self._value = value
			self.headers = {"content-type": "application/json"}
			self.request = SimpleNamespace(url=f"https://example.com?partition={value}")

		def raise_for_status(self):
			if self.status_code >= 400:
				import httpx

				raise httpx.HTTPStatusError("boom", request=httpx.Request("GET", "https://example.com"), response=self)

		def json(self):
			return [{"value": self._value}]

		@property
		def text(self):
			return f'{{"value":"{self._value}"}}'

	class _Client:
		async def __aenter__(self):
			return self

		async def __aexit__(self, exc_type, exc, tb):
			return False

		async def request(self, **kwargs):
			url = str(kwargs.get("url") or "")
			value = str((parse_qs(urlsplit(url).query).get("partition") or [""])[0])
			if value == "bad":
				return _Resp(503, value)
			return _Resp(200, value)

	monkeypatch.setattr("app.executors.source.httpx.AsyncClient", _Client)
	node = {
		"id": "n_api_partition_fail",
		"data": {
			"sourceKind": "api",
			"params": {
				"url": "https://example.com/data",
				"method": "GET",
				"partition": {
					"enabled": True,
					"kind": "static_list",
					"bind_key": "partition",
					"parallelism_cap": 2,
					"static_values": ["ok", "bad"],
				},
				"retry": {"max_attempts": 1, "backoff_seconds": 0, "jitter_seconds": 0},
			},
		},
	}
	result = await exec_source("run_partition_api_fail", node, _ctx())
	assert result.status == "failed"
	assert "PARTITION_FAILED" in str(result.error or "")
	assert "id=bad" in str(result.error or "")


@pytest.mark.asyncio
async def test_partition_api_skip_failed_policy_recovers_partial_results(monkeypatch):
	class _Resp:
		def __init__(self, status_code: int, value: str):
			self.status_code = status_code
			self._value = value
			self.headers = {"content-type": "application/json"}
			self.request = SimpleNamespace(url=f"https://example.com?partition={value}")

		def raise_for_status(self):
			if self.status_code >= 400:
				import httpx

				raise httpx.HTTPStatusError("boom", request=httpx.Request("GET", "https://example.com"), response=self)

		def json(self):
			return [{"value": self._value}]

		@property
		def text(self):
			return f'{{"value":"{self._value}"}}'

	class _Client:
		async def __aenter__(self):
			return self

		async def __aexit__(self, exc_type, exc, tb):
			return False

		async def request(self, **kwargs):
			url = str(kwargs.get("url") or "")
			value = str((parse_qs(urlsplit(url).query).get("partition") or [""])[0])
			if value == "bad":
				return _Resp(503, value)
			return _Resp(200, value)

	monkeypatch.setattr("app.executors.source.httpx.AsyncClient", _Client)
	node = {
		"id": "n_api_partition_skip_failed",
		"data": {
			"sourceKind": "api",
			"params": {
				"url": "https://example.com/data",
				"method": "GET",
				"output": {"mode": "table"},
				"partition": {
					"enabled": True,
					"kind": "static_list",
					"bind_key": "partition",
					"parallelism_cap": 2,
					"on_error": "skip_failed",
					"static_values": ["ok1", "bad", "ok2"],
				},
				"retry": {"max_attempts": 1, "backoff_seconds": 0, "jitter_seconds": 0},
			},
		},
	}
	result = await exec_source("run_partition_skip_failed", node, _ctx())
	assert result.status == "succeeded"
	assert isinstance(result.data, list)
	values = [str((row or {}).get("value") or "") for row in result.data if isinstance(row, dict)]
	assert "ok1" in values and "ok2" in values
	assert "bad" not in values


@pytest.mark.asyncio
async def test_partition_api_load_many_partitions(monkeypatch):
	class _Resp:
		def __init__(self, value: str):
			self._value = value
			self.headers = {"content-type": "application/json"}

		def raise_for_status(self):
			return None

		def json(self):
			return [{"value": self._value}]

		@property
		def text(self):
			return f'{{"value":"{self._value}"}}'

	class _Client:
		async def __aenter__(self):
			return self

		async def __aexit__(self, exc_type, exc, tb):
			return False

		async def request(self, **kwargs):
			url = str(kwargs.get("url") or "")
			value = str((parse_qs(urlsplit(url).query).get("partition") or [""])[0])
			await asyncio.sleep(0.001)
			return _Resp(value)

	monkeypatch.setattr("app.executors.source.httpx.AsyncClient", _Client)
	values = [f"p{i}" for i in range(60)]
	node = {
		"id": "n_api_partition_load",
		"data": {
			"sourceKind": "api",
			"params": {
				"url": "https://example.com/data",
				"method": "GET",
				"output": {"mode": "table"},
				"partition": {
					"enabled": True,
					"kind": "static_list",
					"bind_key": "partition",
					"parallelism_cap": 8,
					"static_values": values,
				},
			},
		},
	}
	result = await exec_source("run_partition_load", node, _ctx())
	assert result.status == "succeeded"
	assert isinstance(result.data, list)
	assert len(result.data) == len(values)


@pytest.mark.asyncio
async def test_partition_api_cancellation_under_fanout(monkeypatch):
	class _Resp:
		headers = {"content-type": "application/json"}

		def raise_for_status(self):
			return None

		def json(self):
			return [{"value": "ok"}]

		@property
		def text(self):
			return '{"value":"ok"}'

	class _Client:
		async def __aenter__(self):
			return self

		async def __aexit__(self, exc_type, exc, tb):
			return False

		async def request(self, **kwargs):
			await asyncio.sleep(1.0)
			return _Resp()

	monkeypatch.setattr("app.executors.source.httpx.AsyncClient", _Client)
	node = {
		"id": "n_api_partition_cancel",
		"data": {
			"sourceKind": "api",
			"params": {
				"url": "https://example.com/data",
				"method": "GET",
				"output": {"mode": "table"},
				"partition": {
					"enabled": True,
					"kind": "static_list",
					"bind_key": "partition",
					"parallelism_cap": 4,
					"static_values": ["a", "b", "c", "d", "e"],
				},
			},
		},
	}
	task = asyncio.create_task(exec_source("run_partition_cancel", node, _ctx()))
	await asyncio.sleep(0.05)
	task.cancel()
	with pytest.raises(asyncio.CancelledError):
		await task


@pytest.mark.asyncio
async def test_partition_api_timeout_surface(monkeypatch):
	import httpx

	class _Client:
		async def __aenter__(self):
			return self

		async def __aexit__(self, exc_type, exc, tb):
			return False

		async def request(self, **kwargs):
			raise httpx.ReadTimeout("timeout", request=httpx.Request("GET", kwargs.get("url", "https://example.com")))

	monkeypatch.setattr("app.executors.source.httpx.AsyncClient", _Client)
	node = {
		"id": "n_api_partition_timeout",
		"data": {
			"sourceKind": "api",
			"params": {
				"url": "https://example.com/data",
				"method": "GET",
				"partition": {
					"enabled": True,
					"kind": "static_list",
					"bind_key": "partition",
					"parallelism_cap": 2,
					"static_values": ["a", "b"],
				},
				"retry": {"max_attempts": 1, "backoff_seconds": 0, "jitter_seconds": 0},
			},
		},
	}
	result = await exec_source("run_partition_timeout", node, _ctx())
	assert result.status == "failed"
	assert "PARTITION_FAILED" in str(result.error or "")
