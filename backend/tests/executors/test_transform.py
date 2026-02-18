"""Comprehensive pytest tests for Transform executor - exec_transform function"""
import pytest
import pandas as pd
import json
import hashlib
import tempfile
import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from app.executors.transform import exec_transform, iso_now
from app.runner.metadata import ExecutionContext, FileMetadata, NodeOutput
from app.runner.events import RunEventBus


class TestTransformHelperFunctions:
    """Test helper functions"""

    @pytest.mark.asyncio
    async def test_iso_now_format(self):
        """Test timestamp format is ISO 8601"""
        timestamp = iso_now()
        assert isinstance(timestamp, str)
        assert "T" in timestamp
        assert "Z" in timestamp or "+" in timestamp


class TestTransformBasicFunctionality:
    """Test basic exec_transform functionality"""

    @pytest.mark.asyncio
    async def test_exec_transform_with_valid_input(self):
        """Test exec_transform executes successfully with valid input"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_path = "input.parquet"
        input_metadata.storage_uri = "memory://artifact123"
        input_metadata.file_type = "parquet"
        input_metadata.size_bytes = 1024

        node = {
            "id": "test_filter_node",
            "data": {
                "params": {
                    "transform_type": "dataframe"
                }
            }
        }

        # Mock artifact retrieval
        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = Path(tmpdir) / "input.parquet"
            pd.DataFrame({"col1": [1, 2], "col2": [3, 4]}).to_parquet(input_path)
            mock_data = input_path.read_bytes()
            mock_context.artifact_store.read = AsyncMock(return_value=mock_data)
            mock_artifact = MagicMock()
            mock_artifact.mime_type = "application/vnd.apache.parquet"
            mock_context.artifact_store.get = AsyncMock(return_value=mock_artifact)

        with tempfile.TemporaryDirectory() as tmpdir:
            mock_path = Path(tmpdir) / "input.parquet"
            mock_path.write_text("col1,col2\n1,2\n3,4")

            with patch('pathlib.Path', lambda x: Path(x) if 'input' not in x else mock_path / 'some_output.parquet'):
                result = await exec_transform(
                    run_id="test_run",
                    node=node,
                    context=mock_context,
                    input_metadata=input_metadata,
                    upstream_artifact_ids=None
                )

                assert isinstance(result, NodeOutput)
                assert result.status in ["succeeded", "failed"]
                # Verify event was emitted
                mock_bus.emit.assert_any_call()

    @pytest.mark.asyncio
    async def test_exec_transform_missing_input_metadata(self):
        """Test exec_transform fails gracefully without input_metadata"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus

        node = {
            "id": "test_node",
            "data": {"params": {}}
        }

        result = await exec_transform(
            run_id="test_run",
            node=node,
            context=mock_context,
            input_metadata=None,
            upstream_artifact_ids=None
        )

        assert result.status == "failed"
        assert "requires input data" in result.error.lower()
        mock_bus.emit.assert_called()

    @pytest.mark.asyncio
    async def test_exec_transform_missing_context_bus(self):
        """Test exec_transform raises error when bus is missing"""
        mock_context = MagicMock(spec=ExecutionContext)
        # Don't set bus attribute

        node = {
            "id": "test_node",
            "data": {"params": {}}
        }

        with pytest.raises(AssertionError, match="context missing bus"):
            await exec_transform(
                run_id="test_run",
                node=node,
                context=mock_context,
                input_metadata=None,
                upstream_artifact_ids=None
            )

    @pytest.mark.asyncio
    async def test_exec_transform_missing_context_store(self):
        """Test exec_transform raises error when artifact_store is missing"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        # Don't set artifact_store attribute

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_type = "parquet"

        node = {
            "id": "test_node",
            "data": {"params": {}}
        }

        with pytest.raises(AssertionError, match="context missing artifact_store"):
            await exec_transform(
                run_id="test_run",
                node=node,
                context=mock_context,
                input_metadata=input_metadata,
                upstream_artifact_ids=None
            )


class TestTransformDataFrameOperations:
    """Test DataFrame operations in exec_transform"""

    @pytest.mark.asyncio
    async def test_exec_transform_filter_operation(self):
        """Test filter operation on DataFrame"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_path = "input.parquet"
        input_metadata.storage_uri = "memory://artifact123"
        input_metadata.file_type = "parquet"
        input_metadata.size_bytes = 1024

        node = {
            "id": "filter_node",
            "data": {
                "params": {
                    "transform_type": "dataframe",
                    "filter_expression": "df['value'] > 2"
                }
            }
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test input parquet file
            input_path = Path(tmpdir) / "input.parquet"
            df_input = pd.DataFrame({"value": [1, 2, 3, 4, 5]})
            df_input.to_parquet(input_path)

            mock_data = input_path.read_bytes()
            mock_context.artifact_store.read.return_value = mock_data
            mock_ctx_artifact = MagicMock()
            mock_ctx_artifact.mime_type = "application/vnd.apache.parquet"
            mock_context.artifact_store.get.return_value = mock_ctx_artifact

            with patch('pathlib.Path') as mock_path_class:
                # Mock output path creation
                output_path = Path(tmpdir) / "pipeline" / "test_filter_node" / "output.parquet"
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.write_text("test")

                mock_path_instance = MagicMock()
                mock_path_instance.stat.return_value.st_size = 100
                mock_path_instance.__str__ = lambda self: str(output_path)
                mock_path_instance.read_bytes.return_value = b"test output"

                mock_path_class.return_value = output_path

                result = await exec_transform(
                    run_id="test_run",
                    node=node,
                    context=mock_context,
                    input_metadata=input_metadata,
                    upstream_artifact_ids=None
                )

                assert result.status == "succeeded"
                assert result.metadata is not None
                mock_bus.emit.assert_called()

    @pytest.mark.asyncio
    async def test_exec_transform_map_operation(self):
        """Test map operation (apply function to DataFrame)"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_path = "input.parquet"
        input_metadata.file_type = "parquet"

        node = {
            "id": "map_node",
            "data": {
                "params": {
                    "transform_type": "dataframe",
                    "function": "value * 2",
                    "new_column_name": "doubled_value"
                }
            }
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = Path(tmpdir) / "input.parquet"
            df_input = pd.DataFrame({"value": [1, 2, 3]})
            df_input.to_parquet(input_path)

            mock_data = input_path.read_bytes()
            mock_context.artifact_store.read.return_value = mock_data
            mock_ctx_artifact = MagicMock()
            mock_ctx_artifact.mime_type = "application/vnd.apache.parquet"
            mock_context.artifact_store.get.return_value = mock_ctx_artifact

            with patch('pathlib.Path') as mock_path_class:
                with patch('app.executors.transform.params_hash', return_value="test_hash"):
                    output_path = Path(tmpdir) / "pipeline" / "map_node" / "output.parquet"
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    output_path.write_text("test")

                    mock_path_instance = MagicMock()
                    mock_path_instance.stat.return_value.st_size = 100
                    mock_path_instance.__str__ = lambda self: str(output_path)
                    mock_path_instance.read_bytes.return_value = b"test output"

                    mock_path_class.return_value = output_path

                    result = await exec_transform(
                        run_id="test_run",
                        node=node,
                        context=mock_context,
                        input_metadata=input_metadata,
                        upstream_artifact_ids=None
                    )

                    assert result.status == "succeeded"
                    mock_bus.emit.assert_called()

    @pytest.mark.asyncio
    async def test_exec_transform_aggregate_operation(self):
        """Test aggregate operation on DataFrame"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_path = "input.parquet"
        input_metadata.file_type = "parquet"

        node = {
            "id": "aggregate_node",
            "data": {
                "params": {
                    "transform_type": "dataframe",
                    "aggregations": {"price": "sum", "price": "count"}
                }
            }
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = Path(tmpdir) / "input.parquet"
            df_input = pd.DataFrame({
                "product": ["A", "B", "A"],
                "price": [10, 20, 30]
            })
            df_input.to_parquet(input_path)

            mock_data = input_path.read_bytes()
            mock_context.artifact_store.read.return_value = mock_data
            mock_ctx_artifact = MagicMock()
            mock_ctx_artifact.mime_type = "application/vnd.apache.parquet"
            mock_context.artifact_store.get.return_value = mock_ctx_artifact

            with patch('pathlib.Path') as mock_path_class:
                with patch('app.executors.transform.params_hash', return_value="test_hash"):
                    output_path = Path(tmpdir) / "pipeline" / "aggregate_node" / "output.parquet"
                    output_path.parent.mkdir(parents=True, exist_ok=True)

                    mock_path_instance = MagicMock()
                    mock_path_instance.stat.return_value.st_size = sum(p.size for p in input_path.parent.rglob('*'))
                    mock_path_instance.__str__ = lambda self: str(output_path.parent / "output.parquet")
                    mock_path_instance.read_bytes.return_value = b"test"

                    mock_path_class.side_effect = lambda x: output_path.parent if 'pipeline' not in x else output_path

                    result = await exec_transform(
                        run_id="test_run",
                        node=node,
                        context=mock_context,
                        input_metadata=input_metadata,
                        upstream_artifact_ids=None
                    )

                    assert result.status == "succeeded"
                    mock_bus.emit.assert_called()

    @pytest.mark.asyncio
    async def test_exec_transform_clean_operation(self):
        """Test clean operation (handle missing values, whitespace, etc.)"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_path = "input.parquet"
        input_metadata.file_type = "parquet"

        node = {
            "id": "clean_node",
            "data": {
                "params": {
                    "transform_type": "dataframe",
                    "clean": {
                        "drop_na": True,
                        "strip_whitespace": True
                    }
                }
            }
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = Path(tmpdir) / "input.parquet"
            df_input = pd.DataFrame({
                "name": [" Alice ", " Bob ", None],
                "value": [10, 20, 30]
            })
            df_input.to_parquet(input_path)

            mock_data = input_path.read_bytes()
            mock_context.artifact_store.read.return_value = mock_data
            mock_ctx_artifact = MagicMock()
            mock_ctx_artifact.mime_type = "application/vnd.apache.parquet"
            mock_context.artifact_store.get.return_value = mock_ctx_artifact

            with patch('pathlib.Path') as mock_path_class:
                with patch('app.executors.transform.params_hash', return_value="test_hash"):
                    output_path = Path(tmpdir) / "pipeline" / "clean_node" / "output.parquet"
                    output_path.parent.mkdir(parents=True, exist_ok=True)

                    mock_path_instance = MagicMock()
                    mock_path_instance.stat.return_value.st_size = 500
                    mock_path_instance.__str__ = lambda self: str(output_path)
                    mock_path_instance.read_bytes.return_value = b"test"

                    mock_path_class.side_effect = lambda x: output_path if 'output' in x else output_path.parent / "input.parquet"

                    result = await exec_transform(
                        run_id="test_run",
                        node=node,
                        context=mock_context,
                        input_metadata=input_metadata,
                        upstream_artifact_ids=None
                    )

                    assert result.status == "succeeded"
                    mock_bus.emit.assert_called()


class TestTransformConversions:
    """Test format conversion operations"""

    @pytest.mark.asyncio
    async def test_exec_transform_dataframe_to_json(self):
        """Test conversion from DataFrame to JSON"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_path = "data.parquet"
        input_metadata.storage_uri = "memory://artifact123"
        input_metadata.file_type = "parquet"
        input_metadata.size_bytes = 1024

        node = {
            "id": "to_json_node",
            "data": {
                "params": {
                    "transform_type": "dataframe-to-json"
                }
            }
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = Path(tmpdir) / "data.parquet"
            df_input = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
            df_input.to_parquet(input_path)

            mock_data = input_path.read_bytes()
            mock_context.artifact_store.read.return_value = mock_data
            mock_ctx_artifact = MagicMock()
            mock_ctx_artifact.mime_type = "application/vnd.apache.parquet"
            mock_context.artifact_store.get.return_value = mock_ctx_artifact

            with patch('pathlib.Path') as mock_path_class:
                with patch('app.executors.transform.params_hash', return_value="test_hash"):
                    output_dir = Path(tmpdir) / "pipeline" / "to_json_node"
                    output_dir.mkdir(parents=True, exist_ok=True)

                    mock_output_path = output_dir / "output.json"
                    mock_output_path.write_text(json.dumps(df_input.to_dict('records')))

                    mock_path_instance = MagicMock()
                    mock_path_instance.stat.return_value.st_size = len(mock_output_path.read_text())
                    mock_path_instance.__str__ = lambda self: str(mock_output_path)
                    mock_path_instance.read_bytes.return_value = mock_output_path.read_bytes()
                    mock_path_instance.read_text.return_value = mock_output_path.read_text()

                    mock_path_class.return_value = mock_output_path
                    mock_path_class.side_effect = lambda x: output_dir / ('output.json' if x.endswith('.json') else 'input.parquet')

                    result = await exec_transform(
                        run_id="test_run",
                        node=node,
                        context=mock_context,
                        input_metadata=input_metadata,
                        upstream_artifact_ids=None
                    )

                    assert result.status == "succeeded"
                    assert result.data is not None
                    assert isinstance(result.data, str)
                    # Verify JSON contains expected data
                    json_data = json.loads(result.data)
                    assert 'id' in json_data[0]
                    assert 'name' in json_data[0]

    @pytest.mark.asyncio
    async def test_exec_transform_json_to_dataframe(self):
        """Test conversion from JSON to DataFrame"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_path = "data.json"
        input_metadata.storage_uri = "memory://artifact123"
        input_metadata.file_type = "json"
        input_metadata.size_bytes = 1024

        node = {
            "id": "to_df_node",
            "data": {
                "params": {
                    "transform_type": "json-to-dataframe"
                }
            }
        }

        test_data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"}
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = Path(tmpdir) / "data.json"
            input_path.write_text(json.dumps(test_data))

            mock_context.artifact_store.read.return_value = input_path.read_bytes()
            mock_ctx_artifact = MagicMock()
            mock_ctx_artifact.mime_type = "application/json"
            mock_context.artifact_store.get.return_value = mock_ctx_artifact

            with patch('pathlib.Path') as mock_path_class:
                with patch('app.executors.transform.params_hash', return_value="test_hash"):
                    output_dir = Path(tmpdir) / "pipeline" / "to_df_node"
                    output_dir.mkdir(parents=True, exist_ok=True)

                    mock_output_path = output_dir / "output.parquet"
                    mock_output_path.write_bytes(b"parquet data")

                    mock_path_instance = MagicMock()
                    mock_path_instance.stat.return_value.st_size = 100
                    mock_path_instance.__str__ = lambda self: str(mock_output_path)
                    mock_path_instance.write_bytes.return_value = b"parquet"

                    mock_path_class.return_value = mock_output_path

                    result = await exec_transform(
                        run_id="test_run",
                        node=node,
                        context=mock_context,
                        input_metadata=input_metadata,
                        upstream_artifact_ids=None
                    )

                    assert result.status == "succeeded"
                    assert result.metadata is not None
                    assert result.file_type == "parquet"


class TestTransformCustomCode:
    """Test custom code execution in exec_transform"""

    @pytest.mark.asyncio
    async def test_exec_transform_custom_code_success(self):
        """Test custom transform with valid Python code"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_path = "input.parquet"
        input_metadata.file_type = "parquet"

        node = {
            "id": "custom_node",
            "data": {
                "params": {
                    "transform_type": "dataframe",
                    "code": "result_df = df[['name']].copy()\nresult_df['name_upper'] = df['name'].str.upper()",
                    "input_var": "df",
                    "output_var": "result_df"
                }
            }
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = Path(tmpdir) / "input.parquet"
            df_input = pd.DataFrame({"name": ["alice", "bob", "charlie"]})
            df_input.to_parquet(input_path)

            mock_data = input_path.read_bytes()
            mock_context.artifact_store.read.return_value = mock_data
            mock_ctx_artifact = MagicMock()
            mock_ctx_artifact.mime_type = "application/vnd.apache.parquet"
            mock_context.artifact_store.get.return_value = mock_ctx_artifact

            with patch('pathlib.Path') as mock_path_class:
                with patch('app.executors.transform.params_hash', return_value="test_hash"):
                    output_path = Path(tmpdir) / "pipeline" / "custom_node" / "output.parquet"
                    output_path.parent.mkdir(parents=True, exist_ok=True)

                    mock_path_instance = MagicMock()
                    mock_path_instance.stat.return_value.st_size = 500
                    mock_path_instance.write_bytes.return_value = b"data"
                    mock_path_instance.read_bytes.return_value = b"data"
                    mock_path_instance.__str__ = lambda self: str(output_path)

                    mock_path_class.side_effect = lambda x: output_path if x.endswith('.parquet') else input_path

                    result = await exec_transform(
                        run_id="test_run",
                        node=node,
                        context=mock_context,
                        input_metadata=input_metadata,
                        upstream_artifact_ids=None
                    )

                    assert result.status == "succeeded"

    @pytest.mark.asyncio
    async def test_exec_transform_custom_code_failure(self):
        """Test custom transform with invalid Python code"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_path = "input.parquet"
        input_metadata.file_type = "parquet"

        node = {
            "id": "bad_custom_node",
            "data": {
                "params": {
                    "transform_type": "dataframe",
                    "code": "this is invalid python code !!! KeyError"
                }
            }
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = Path(tmpdir) / "input.parquet"
            df_input = pd.DataFrame({"name": ["Alice"]})
            df_input.to_parquet(input_path)

            mock_data = input_path.read_bytes()
            mock_context.artifact_store.read.return_value = mock_data
            mock_ctx_artifact = MagicMock()
            mock_ctx_artifact.mime_type = "application/vnd.apache.parquet"
            mock_context.artifact_store.get.return_value = mock_ctx_artifact

            with patch('app.executors.transform.params_hash', return_value="test_hash"):
                with patch('pathlib.Path'):
                    result = await exec_transform(
                        run_id="test_run",
                        node=node,
                        context=mock_context,
                        input_metadata=input_metadata,
                        upstream_artifact_ids=None
                    )

                    assert result.status == "failed"
                    assert "failed" in result.error.lower()


class TestTransformErrorHandling:
    """Test error handling in exec_transform"""

    @pytest.mark.asyncio
    async def test_exec_transform_cannot_read_input(self):
        """Test error when cannot read input artifact"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_path = "input.parquet"

        node = {
            "id": "error_node",
            "data": {"params": {}}
        }

        mock_context.artifact_store.read.side_effect = FileNotFoundError("File not found")

        with pytest.raises(FileNotFoundError):
            await exec_transform(
                run_id="test_run",
                node=node,
                context=mock_context,
                input_metadata=input_metadata,
                upstream_artifact_ids=None
            )

    @pytest.mark.asyncio
    async def test_exec_transform_invalid_input_format(self):
        """Test error with unsupported input format"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_path = "input.xyz"
        input_metadata.file_type = "xyz"

        node = {
            "id": "format_node",
            "data": {"params": {"transform_type": "dataframe"}}
        }

        with pytest.raises((FileNotFoundError, KeyError, pd.errors.EmptyDataError)):
            await exec_transform(
                run_id="test_run",
                node=node,
                context=mock_context,
                input_metadata=input_metadata,
                upstream_artifact_ids=None
            )


class TestTransformEventEmission:
    """Test event emission during transformations"""

    @pytest.mark.asyncio
    async def test_exec_transform_emits_events_on_success(self):
        """Test that events are emitted on successful execution"""
        mock_bus = MagicMock(spec=RunEventBus)
        emit_mock = mock_bus.emit

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_path = "input.parquet"
        input_metadata.file_type = "parquet"

        node = {
            "id": "event_node",
            "data": {"params": {"transform_type": "dataframe"}}
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = Path(tmpdir) / "input.parquet"
            pd.DataFrame({"dummy": [1, 2, 3]}).to_parquet(input_path)

            mock_data = input_path.read_bytes()
            mock_context.artifact_store.read.return_value = mock_data
            mock_ctx_artifact = MagicMock()
            mock_ctx_artifact.mime_type = "application/vnd.apache.parquet"
            mock_context.artifact_store.get.return_value = mock_ctx_artifact

            with patch('pathlib.Path'):
                with patch('app.executors.transform.params_hash', return_value="hash"):
                    await exec_transform(
                        run_id="test_run",
                        node=node,
                        context=mock_context,
                        input_metadata=input_metadata,
                        upstream_artifact_ids=None
                    )

                    # Should emit at least one event
                    assert emit_mock.call_count > 0

                    # Check for specific event types
                    call_args_list = emit_mock.call_args_list
                    event_calls = [call for call in call_args_list if 'log' in call[1].get('type', '')]

                    assert len(event_calls) > 0


class TestTransformOutput:
    """Test the output structure and content"""

    @pytest.mark.asyncio
    async def test_exec_transform_output_has_metadata(self):
        """Test that successful output includes metadata"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_path = "input.parquet"
        input_metadata.file_type = "parquet"

        node = {
            "id": "metadata_node",
            "data": {"params": {"transform_type": "dataframe"}}
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = Path(tmpdir) / "input.parquet"
            pd.DataFrame({"id": [1, 2]}).to_parquet(input_path)

            mock_data = input_path.read_bytes()
            mock_context.artifact_store.read.return_value = mock_data
            mock_ctx_artifact = MagicMock()
            mock_ctx_artifact.mime_type = "application/vnd.apache.parquet"
            mock_context.artifact_store.get.return_value = mock_ctx_artifact

            output_path = Path(tmpdir) / "pipeline" / "metadata_node" / "output.parquet"
            output_path.parent.mkdir(parents=True, exist_ok=True)

            with patch('pathlib.Path') as mock_path_class:
                with patch('app.executors.transform.params_hash', return_value="test_hash"):
                    mock_path_instance = MagicMock()
                    mock_path_instance.stat.return_value.st_size = 100
                    mock_path_instance.write_bytes.return_value = b"data"
                    mock_path_instance.read_bytes.return_value = b"data"
                    mock_path_instance.__str__ = lambda self: str(output_path)

                    mock_path_class.side_effect = lambda x: output_path if x.endswith('.parquet') else input_path

                    result = await exec_transform(
                        run_id="test_run",
                        node=node,
                        context=mock_context,
                        input_metadata=input_metadata,
                        upstream_artifact_ids=None
                    )

                    assert isinstance(result, NodeOutput)
                    assert result.metadata is not None
                    assert isinstance(result.metadata, FileMetadata)

    @pytest.mark.asyncio
    async def test_exec_transform_output_status_field(self):
        """Test that output has status field"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_path = "input.parquet"

        node = {"id": "status_node", "data": {"params": {}}}

        with pytest.raises(Exception):
            await exec_transform(
                run_id="test_run",
                node=node,
                context=mock_context,
                input_metadata=input_metadata,
                upstream_artifact_ids=None
            )

        # For failed case
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        result = await exec_transform(
            run_id="test_run",
            node=node,
            context=mock_context,
            input_metadata=None,
            upstream_artifact_ids=None
        )

        assert hasattr(result, 'status')
        assert result.status == "failed"
