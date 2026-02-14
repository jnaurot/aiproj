"""Unit tests for Source executors - exec_source function"""
import pytest
import pandas as pd
from unittest.mock import AsyncMock, MagicMock, patch
from pathlib import Path

from app.executors.source import exec_source
from app.runner.metadata import ExecutionContext, FileMetadata, NodeOutput
from app.runner.events import RunEventBus


class TestExecSource:
    """Tests for exec_source function"""

    @pytest.mark.asyncio
    async def test_source_file_csv(self):
        """Test file source with CSV format"""
        # Mock context
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()
        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus

        node = {
            "id": "test_node",
            "data": {
                "params": {
                    "source_type": "file",
                    "file_path": "data/test.csv",
                    "file_format": "csv"
                }
            }
        }

        # Mock file reading
        with patch('pathlib.Path.exists', return_value=True), \
             patch('pathlib.Path.stat') as mock_stat, \
             patch('pandas.read_csv') as mock_read:

            mock_stat.return_value.st_size = 1024
            mock_read.return_value = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

            result = await exec_source(
                run_id="test_run",
                node=node,
                context=mock_context,
                input_metadata=None,
                upstream_artifact_ids=None
            )

            assert isinstance(result, NodeOutput)
            assert result.status == "succeeded"
            assert result.metadata is not None
            assert result.metadata.row_count == 3
            assert result.metadata.file_type == "csv"
            mock_bus.emit.assert_any_call(
                {"type": "log", "runId": "test_run", "at": pytest.approx(any), "level": "info", "message": pytest.approx(any), "nodeId": "test_node"}
            )

    @pytest.mark.asyncio
    async def test_source_file_parquet(self):
        """Test file source with Parquet format"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()
        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus

        node = {
            "id": "test_node",
            "data": {
                "params": {
                    "source_type": "file",
                    "file_path": "data/test.parquet",
                    "file_format": "parquet"
                }
            }
        }

        with patch('pathlib.Path.exists', return_value=True), \
             patch('pandas.read_parquet') as mock_read:

            mock_read.return_value = pd.DataFrame({"numbers": [10, 20, 30]})
            result = await exec_source("test_run", node, mock_context, None, None)

            assert result.status == "succeeded"
            assert result.metadata.file_type == "parquet"

    @pytest.mark.asyncio
    async def test_source_file_not_found(self):
        """Test file source when file doesn't exist"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()
        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus

        node = {
            "id": "test_node",
            "data": {
                "params": {
                    "source_type": "file",
                    "file_path": "data/nonexistent.csv"
                }
            }
        }

        with patch('pathlib.Path.exists', return_value=False):
            result = await exec_source("test_run", node, mock_context, None, None)

            assert result.status == "failed"
            assert "File not found" in result.error

    @pytest.mark.asyncio
    async def test_source_database(self):
        """Test database source"""
        with patch('pathlib.Path.exists', return_value=True), \
             patch('sqlalchemy.create_engine') as mock_engine:

            mock_engine_instance = MagicMock()
            mock_engine.return_value = mock_engine_instance
            mock_engine_instance.connect.return_value = MagicMock()
            mock_engine_instance.dispose.return_value = None

            mock_bus = MagicMock(spec=RunEventBus)
            mock_bus.emit = AsyncMock()
            mock_context = MagicMock(spec=ExecutionContext)
            mock_context.bus = mock_bus

            node = {
                "id": "test_node",
                "data": {
                    "params": {
                        "source_type": "database",
                        "connection_string": "postgresql://user:pass@localhost/db",
                        "table_name": "users"
                    }
                }
            }

            mock_context = MagicMock(spec=ExecutionContext)
            mock_context.bus = mock_bus
            mock_context.artifact_store = MagicMock()

            # Mock pandas read_sql
            with patch('pandas.read_sql_table') as mock_read:
                mock_read.return_value = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
                result = await exec_source("test_run", node, mock_context, None, None)

                assert result.status == "succeeded"
                assert result.metadata.row_count == 2

    @pytest.mark.asyncio
    async def test_source_api(self):
        """Test API source"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()
        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus

        node = {
            "id": "test_node",
            "data": {
                "params": {
                    "source_type": "api",
                    "url": "https://api.example.com/data",
                    "method": "GET"
                }
            }
        }

        with patch('pathlib.Path.exists', return_value=True), \
             patch('httpx.AsyncClient') as mock_client:

            mock_response = AsyncMock()
            mock_response.status_code = 200
            mock_response.json.return_value = [{"key": "value"}]
            mock_response.headers.get.return_value = "application/json"
            mock_client().__aenter__.return_value.request.return_value = mock_response

            result = await exec_source("test_run", node, mock_context, None, None)

            assert result.status == "succeeded"

    @pytest.mark.asyncio
    async def test_source_invalid_type(self):
        """Test source with invalid source_type"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()
        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus

        node = {
            "id": "test_node",
            "data": {
                "params": {
                    "source_type": "invalid"
                }
            }
        }

        with pytest.raises(ValueError, match="Unknown source_type"):
            await exec_source("test_run", node, mock_context, None, None)


class TestSourceFileFormats:
    """Test different file format handlers"""

    @pytest.mark.asyncio
    async def test_read_csv_format(self):
        """Test CSV format reading"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()
        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus

        node = {
            "id": "csv_node",
            "data": {
                "params": {
                    "source_type": "file",
                    "file_path": "data.csv",
                    "file_format": "csv",
                    "delimiter": ",",
                    "encoding": "utf-8"
                }
            }
        }

        with patch('pathlib.Path.exists', return_value=True), \
             patch('pandas.read_csv') as mock_read:

            mock_read.return_value = pd.DataFrame({
                "name": ["Alice", "Bob"],
                "age": [25, 30]
            })
            result = await exec_source("test_run", node, mock_context, None, None)

            assert result.status == "succeeded"
            assert result.metadata.file_type == "csv"
            assert result.metadata.row_count == 2

    @pytest.mark.asyncio
    async def test_read_json_format(self):
        """Test JSON format reading"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()
        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus

        node = {
            "id": "json_node",
            "data": {
                "params": {
                    "source_type": "file",
                    "file_path": "data.json",
                    "file_format": "json"
                }
            }
        }

        csv_file = "\n".join(['{}', '{"data":"test"}'])
        with patch('pathlib.Path.exists', return_value=True), \
             patch('pandas.read_json') as mock_read:
            
            mock_read.return_value = pd.DataFrame({"name": ["Test"], "value": [100]})
            result = await exec_source("test_run", node, mock_context, None, None)

            assert result.status == "succeeded"

    @pytest.mark.asyncio
    async def test_source_missing_params(self):
        """Test source with missing required parameters"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()
        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus

        node = {
            "id": "test_node",
            "data": {
                "params": {}
            }
        }

        with pytest.raises(ValueError, match="Unknown source_type"):
            await exec_source("test_run", node, mock_context, None, None)
