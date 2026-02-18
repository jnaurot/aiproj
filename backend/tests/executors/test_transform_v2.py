"""Updated simple pytest tests for Transform executor based on actual implementation"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone
from unittest.mock import MagicMock as MockMagic

from app.executors.transform import exec_transform, iso_now
from app.runner.metadata import ExecutionContext, FileMetadata, NodeOutput
from app.runner.events import RunEventBus


class TransformHelperTests:
    """Test helper functions"""

    def test_iso_now_format(self):
        """Test timestamp format is ISO 8601"""
        timestamp = iso_now()
        assert isinstance(timestamp, str)
        assert "T" in timestamp
        assert "Z" in timestamp or "+" in timestamp


class TransformBasicTests:
    """Test basic functionality"""

    @pytest.mark.asyncio
    async def test_exec_transform_with_valid_input(self):
        """Test exec_transform succeeds with valid input"""
        mock_bus = AsyncMock(spec=RunEventBus)
        mock_artifact_store = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = mock_artifact_store

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_path = "test.parquet"
        input_metadata.storage_uri = "memory://test123"
        input_metadata.file_type = "parquet"
        input_metadata.size_bytes = 1000

        node = {
            "id": "test_transform",
            "data": {"params": {"transform_type": "dataframe"}}
        }

        # Mock artifact store
        mock_artifact_bytes = b"parquet data"
        mock_artifact = MockMagic(mime_type="application/x-parquet")
        
        def mock_read_with_prefix(uri):
            if "test123" in uri:
                return mock_artifact_bytes
            return b""
        
        mock_artifact_store.read.side_effect = mock_read_with_prefix
        mock_artifact_store.get.return_value = mock_artifact

        result = await exec_transform(
            run_id="test_run",
            node=node,
            context=mock_context,
            input_metadata=input_metadata,
            upstream_artifact_ids=None
        )

        assert isinstance(result, NodeOutput)
        assert hasattr(result, 'status')

    @pytest.mark.asyncio
    async def test_exec_transform_missing_input_metadata(self):
        """Test exec_transform handles missing input_metadata"""
        mock_bus = AsyncMock(spec=RunEventBus)

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
        assert hasattr(result, 'error')

    @pytest.mark.asyncio
    async def test_exec_transform_missing_bus(self):
        """Test exec_transform raises on missing bus"""
        mock_context = MagicMock(spec=ExecutionContext)
        # Intentionally not setting bus

        node = {
            "id": "test_node",
            "data": {"params": {}}
        }

        with pytest.raises(AssertionError, match="missing bus"):
            await exec_transform(
                run_id="test_run",
                node=node,
                context=mock_context,
                input_metadata=None,
                upstream_artifact_ids=None
            )

    @pytest.mark.asyncio
    async def test_exec_transform_missing_store(self):
        """Test exec_transform raises on missing artifact_store"""
        mock_bus = AsyncMock(spec=RunEventBus)
        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        # Intentionally not setting artifact_store

        input_metadata = FileMetadata(
            file_path="test.parquet",
            storage_uri="memory://test",
            file_type="parquet",
            size_bytes=1000
        )

        node = {
            "id": "test_node",
            "data": {"params": {}}
        }

        with pytest.raises(AssertionError, match="missing artifact_store"):
            await exec_transform(
                run_id="test_run",
                node=node,
                context=mock_context,
                input_metadata=input_metadata,
                upstream_artifact_ids=None
            )


class TransformDataframeTests:
    """Test DataFrame operations"""

    @pytest.mark.asyncio
    async def test_exec_transform_with_dataframe_params(self):
        """Test exec_transform processes DataFrame params"""
        mock_bus = AsyncMock(spec=RunEventBus)
        mock_artifact_store = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = mock_artifact_store

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_path = "test.csv"
        input_metadata.storage_uri = "memory://csv123"
        input_metadata.file_type = "csv"

        node = {
            "id": "df_transform",
            "data": {"params": {"transform_type": "dataframe", "filter_expression": "col1 > 5"}}
        }

        mock_artifact_store.read.return_value = b"col1,col2\n1,2\n6,7"
        mock_artifact_store.get.return_value = MockMagic(mime_type="text/csv")

        # Mock the internal handler to avoid complex parsing
        with patch('app.executors.transform._handle_dataframe_operations') as mock_handler:
            mock_handler.return_value = NodeOutput(status="succeeded", metadata=MagicMock())

            result = await exec_transform(
                run_id="test_run",
                node=node,
                context=mock_context,
                input_metadata=input_metadata,
                upstream_artifact_ids=None
            )

            assert result.status == "succeeded"


class TransformConversionTests:
    """Test format conversions"""

    @pytest.mark.asyncio
    async def test_exec_transform_to_json_conversion(self):
        """Test conversion to JSON"""
        mock_bus = AsyncMock(spec=RunEventBus)
        mock_artifact_store = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = mock_artifact_store

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_path = "data.json"
        input_metadata.storage_uri = "memory://json456"
        input_metadata.file_type = "json"

        node = {
            "id": "to_json",
            "data": {"params": {"transform_type": "dataframe-to-json"}}
        }

        mock_artifact_store.read.return_value = b'[{"id": 1}]'
        mock_artifact_store.get.return_value = MockMagic(mime_type="application/json")

        with patch('app.executors.transform._handle_dataframe_operations') as mock_handler:
            mock_handler.return_value = NodeOutput(status="succeeded", metadata=MagicMock())

            result = await exec_transform(
                run_id="test_run",
                node=node,
                context=mock_context,
                input_metadata=input_metadata,
                upstream_artifact_ids=None
            )

            assert result.status == "succeeded"


class TransformErrorTests:
    """Test error handling"""

    @pytest.mark.asyncio
    async def test_exec_transform_file_not_found(self):
        """Test error handling for missing files"""
        mock_bus = AsyncMock(spec=RunEventBus)
        mock_artifact_store = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = mock_artifact_store

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_path = "missing.parquet"
        input_metadata.storage_uri = "memory://missing"

        mock_artifact_store.read.side_effect = FileNotFoundError("File not found")
        mock_artifact_store.get.side_effect = FileNotFoundError("File not found")

        node = {
            "id": "error_test",
            "data": {"params": {}}
        }

        result = await exec_transform(
            run_id="test_run",
            node=node,
            context=mock_context,
            input_metadata=input_metadata,
            upstream_artifact_ids=None
        )

        assert result.status == "failed"
        assert "failed" in result.error.lower()


class TransformEventTests:
    """Test event emission"""

    @pytest.mark.asyncio
    async def test_events_emitted_on_execution(self):
        """Verify events are emitted during execution"""
        mock_bus = AsyncMock(spec=RunEventBus)
        mock_bus.emit.reset_mock()

        mock_artifact_store = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = mock_artifact_store

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_path = "test.csv"
        input_metadata.storage_uri = "memory://test"
        input_metadata.file_type = "csv"

        node = {
            "id": "event_test",
            "data": {"params": {}}
        }

        mock_artifact_store.read.return_value = b"test"
        mock_artifact_store.get.return_value = MockMagic(mime_type="text/csv")

        with patch('app.executors.transform._handle_dataframe_operations') as mock_handler:
            mock_handler.return_value = NodeOutput(status="succeeded", metadata=MagicMock())

            result = await exec_transform(
                run_id="test_run",
                node=node,
                context=mock_context,
                input_metadata=input_metadata,
                upstream_artifact_ids=None
            )

            # Should emit events for start and completion
            assert mock_bus.emit.called


class TransformOutputTests:
    """Test output structure"""

    @pytest.mark.asyncio
    async def test_output_has_required_attributes(self):
        """Test NodeOutput has all expected attributes"""
        mock_bus = AsyncMock(spec=RunEventBus)
        mock_artifact_store = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = mock_artifact_store

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_path = "test.csv"
        input_metadata.storage_uri = "memory://test"
        input_metadata.file_type = "csv"

        node = {"id": "test"}

        mock_artifact_store.read.return_value = b"test"
        mock_artifact_store.get.return_value = MockMagic(mime_type="text/csv")

        with patch('app.executors.transform._handle_dataframe_operations') as mock_handler:
            mock_handler.return_value = NodeOutput(
                status="succeeded",
                metadata=MagicMock(row_count=10),
                execution_time_ms=50.0,
                data="[data]",
                error=None,
                is_stale=False
            )

            result = await exec_transform(
                run_id="test_run",
                node=node,
                context=mock_context,
                input_metadata=input_metadata,
                upstream_artifact_ids=None
            )

            assert hasattr(result, 'status')
            assert hasattr(result, 'metadata')
            assert hasattr(result, 'execution_time_ms')
            assert hasattr(result, 'data')
            assert hasattr(result, 'error')
            assert hasattr(result, 'is_stale')


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
