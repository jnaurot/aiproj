from types import SimpleNamespace

import pandas as pd
import pytest

from app.executors.source import exec_source
from app.runner.metadata import NodeOutput


def _ctx():
	async def _emit(*_args, **_kwargs):
		return None

	return SimpleNamespace(
		bus=SimpleNamespace(emit=_emit),
		artifact_store=SimpleNamespace(),
		graph_id="graph_test",
	)


@pytest.mark.asyncio
async def test_source_file_csv_success(tmp_path):
	file_path = tmp_path / "data.csv"
	pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}).to_csv(file_path, index=False)

	node = {
		"id": "n_source",
		"data": {
			"params": {
				"source_type": "file",
				"file_path": str(file_path),
				"file_format": "csv",
				"output_mode": "table",
			}
		},
	}
	result = await exec_source("run_1", node, _ctx())
	assert isinstance(result, NodeOutput)
	assert result.status == "succeeded"
	assert isinstance(result.data, list)
	assert result.metadata is not None
	assert result.metadata.row_count == 2


@pytest.mark.asyncio
async def test_source_file_not_found_returns_failed():
	node = {
		"id": "n_source",
		"data": {
			"params": {
				"source_type": "file",
				"file_path": "does-not-exist.csv",
				"file_format": "csv",
			}
		},
	}
	result = await exec_source("run_2", node, _ctx())
	assert result.status == "failed"
	assert "not found" in (result.error or "").lower()


@pytest.mark.asyncio
async def test_source_api_success(monkeypatch):
	class _Resp:
		headers = {"content-type": "application/json"}

		def raise_for_status(self):
			return None

		def json(self):
			return [{"ok": True}]

		@property
		def text(self):
			return '[{"ok":true}]'

	class _Client:
		async def __aenter__(self):
			return self

		async def __aexit__(self, exc_type, exc, tb):
			return False

		async def request(self, **kwargs):
			return _Resp()

	monkeypatch.setattr("app.executors.source.httpx.AsyncClient", _Client)
	node = {
		"id": "n_api",
		"data": {"params": {"source_type": "api", "url": "https://example.com", "method": "GET"}},
	}
	result = await exec_source("run_3", node, _ctx())
	assert result.status == "succeeded"


@pytest.mark.asyncio
async def test_source_invalid_type_returns_failed():
	node = {"id": "n_bad", "data": {"params": {"source_type": "invalid"}}}
	result = await exec_source("run_4", node, _ctx())
	assert result.status == "failed"
	assert "unknown source_type" in (result.error or "").lower()
