from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.executors.source import exec_source
from app.runner.events import RunEventBus


def _context_with_snapshot_bytes(payload: bytes):
	artifact_store = SimpleNamespace(
		exists=AsyncMock(return_value=True),
		read=AsyncMock(return_value=payload),
	)
	bus = MagicMock(spec=RunEventBus)
	bus.emit = AsyncMock()
	return SimpleNamespace(
		run_id="r1",
		graph_id="g1",
		bus=bus,
		artifact_store=artifact_store,
	)


def _file_node(params: dict):
	return {
		"id": "n_source",
		"data": {
			"kind": "source",
			"sourceKind": "file",
			"params": params,
		},
	}


@pytest.mark.asyncio
async def test_csv_table_sets_native_coercion_and_rows():
	ctx = _context_with_snapshot_bytes(b"a,b\n1,2\n")
	node = _file_node(
		{
			"snapshot_id": "a" * 64,
			"file_format": "csv",
			"encoding": "utf-8",
			"output_mode": "table",
			"source_type": "file",
		}
	)
	out = await exec_source("r1", node, ctx)
	assert out.status == "succeeded"
	assert isinstance(out.data, list)
	assert out.metadata is not None
	assert out.metadata.row_count == 1
	assert (out.metadata.data_schema or {}).get("table_coercion", {}).get("mode") == "native"
	cols = (out.metadata.data_schema or {}).get("table_columns") or []
	assert cols == [{"name": "a", "type": "int"}, {"name": "b", "type": "int"}]


@pytest.mark.asyncio
async def test_json_scalar_array_coerces_to_index_value_rows():
	ctx = _context_with_snapshot_bytes(b'[1,"x",{"k":1}]')
	node = _file_node(
		{
			"snapshot_id": "b" * 64,
			"file_format": "json",
			"encoding": "utf-8",
			"output_mode": "table",
			"source_type": "file",
		}
	)
	out = await exec_source("r1", node, ctx)
	assert out.status == "succeeded"
	assert isinstance(out.data, list)
	assert len(out.data) == 3
	assert out.data[0]["index"] == 0
	assert "value" in out.data[0]
	assert out.metadata is not None
	assert out.metadata.row_count == 3
	assert (out.metadata.data_schema or {}).get("table_coercion", {}).get("mode") == "json_scalar_array_rows"


@pytest.mark.asyncio
async def test_text_table_sets_text_1row_coercion():
	ctx = _context_with_snapshot_bytes("hello".encode("utf-8"))
	node = _file_node(
		{
			"snapshot_id": "c" * 64,
			"file_format": "txt",
			"encoding": "utf-8",
			"output_mode": "table",
			"source_type": "file",
		}
	)
	out = await exec_source("r1", node, ctx)
	assert out.status == "succeeded"
	assert out.data == [{"text": "hello"}]
	assert out.metadata is not None
	assert (out.metadata.data_schema or {}).get("table_coercion", {}).get("mode") == "text_1row"


@pytest.mark.asyncio
async def test_binary_table_sets_binary_hex_1row_coercion():
	ctx = _context_with_snapshot_bytes(b"\x89PNG\r\n")
	node = _file_node(
		{
			"snapshot_id": "d" * 64,
			"file_format": "png",
			"encoding": "utf-8",
			"output_mode": "table",
			"source_type": "file",
		}
	)
	out = await exec_source("r1", node, ctx)
	assert out.status == "succeeded"
	assert isinstance(out.data, list)
	assert "binary_hex" in out.data[0]
	assert out.metadata is not None
	assert (out.metadata.data_schema or {}).get("table_coercion", {}).get("mode") == "binary_hex_1row"


@pytest.mark.asyncio
async def test_legacy_inline_text_file_source_does_not_touch_filesystem():
	ctx = _context_with_snapshot_bytes(b"")
	node = _file_node(
		{
			"source_type": "text",
			"text": "hello from legacy text source",
			"output_mode": "table",
		}
	)
	out = await exec_source("r1", node, ctx)
	assert out.status == "succeeded"
	assert out.data == [{"text": "hello from legacy text source"}]
	assert out.metadata is not None
	assert (out.metadata.data_schema or {}).get("table_coercion", {}).get("mode") == "text_1row"
	ctx.artifact_store.exists.assert_not_called()
	ctx.artifact_store.read.assert_not_called()

