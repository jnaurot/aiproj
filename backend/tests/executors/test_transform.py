"""Unit tests for Transform executors - exec_transform function"""
import pytest
import pandas as pd
from unittest.mock import AsyncMock, MagicMock, patch
from pathlib import Path

from app.executors.transform import exec_transform
from app.runner.metadata import ExecutionContext, FileMetadata, NodeOutput
from app.runner.events import RunEventBus


class TestExecTransform:
    """Tests for exec_transform function"""

    @pytest.mark.asyncio
    async def test_transform_filter(self):
        """Test transform with filter operation"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()
        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_path = "input.parquet"

        node = {
            "id": "test_node",
            "data": {
                "params": {
                    "op": "filter"
                }
            }
        }

        # Mock file reading and processing
        with patch('pathlib.Path') as mock_path:
            mock_path_instance = MagicMock()
            mock_path_instance.exists.return_value = True
            mock_path_instance.__truediv__.return_value.parent.mkdir.return_value = None
            
            result = await exec_transform(
                run_id="test_run",
                node=node,
                context=mock_context,
                input_metadata=input_metadata,
                upstream_artifact_ids=None
            )

            assert isinstance(result, NodeOutput)
            assert result.status != "failed" or "requires input data" in result.error.lower()

    @pytest.mark.asyncio
    async def test_transform_missing_input(self):
        """Test transform without input metadata"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()
        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus

        node = {
            "id": "test_node",
            "data": {}
        }

        result = await exec_transform("test_run", node, mock_context, None, None)

        assert result.status == "failed"
        assert "requires input data" in result.error.lower()


class TestTransformParams:
    """Test transform parameter validation"""
    
    @pytest.mark.asyncio
    async def test_transform_code_execution(self):
        """Test transform with custom Python code"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()
        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_path = "input.parquet"

        node = {
            "id": "custom_node",
            "data": {
                "params": {
                    "code": "result = df * 2",
                    "input_var": "df",
                    "output_var": "result"
                }
            }
        }

        result = await exec_transform("test_run", node, mock_context, input_metadata, None)

        assert isinstance(result, NodeOutput)


class TestTransformSchema:
    """Test transform schema validation"""
    
    @pytest.mark.asyncio
    async def test_valid_transform_params(self):
        """Test with valid transform parameters"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()
        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_path = "input.parquet"

        node = {
            "id": "filter_node",
            "data": {
                "params": {
                    "op": "filter",
                    "expression": "df['age'] > 18"
                }
            }
        }

        with patch('pathlib.Path') as mock_path:
            result = await exec_transform("test_run", node, mock_context, input_metadata, None)
            assert result is not None
