"""New test file for Transform executor"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone

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

        result = await exec_transform(
            run_id="test_run",
            node=node,
            context=mock_context,
            input_metadata=input_metadata,
            upstream_artifact_ids=None
        )

        assert isinstance(result, NodeOutput)
        assert hasattr(result, 'status')


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])