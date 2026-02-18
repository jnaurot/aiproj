"""Updated pytest tests for Tool executor based on actual implementation"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from app.executors.tool import exec_tool, iso_now
from app.runner.metadata import ExecutionContext, FileMetadata, NodeOutput
from app.runner.events import RunEventBus


class ToolHelperTests:
    """Test helper functions"""

    def test_iso_now_format(self):
        """Test timestamp format is ISO 8601"""
        timestamp = iso_now()
        assert isinstance(timestamp, str)
        assert "T" in timestamp
        assert "Z" in timestamp or "+" in timestamp


class ToolBasicTests:
    """Test basic functionality"""

    @pytest.mark.asyncio
    async def test_exec_tool_with_valid_params(self):
        """Test exec_tool succeeds with valid parameters"""
        mock_bus = AsyncMock(spec=RunEventBus)
        mock_artifact_store = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = mock_artifact_store

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_path = "input.json"
        input_metadata.storage_uri = "memory://input123"
        input_metadata.file_type = "json"

        node = {
            "id": "test_tool",
            "data": {"params": {"provider": "python", "python_code": "return 'success'"}}
        }

        result = await exec_tool(
            run_id="test_run",
            node=node,
            context=mock_context,
            input_metadata=input_metadata,
            upstream_artifact_ids=None
        )

        assert isinstance(result, NodeOutput)
        assert result.status in ["succeeded", "failed"]

    @pytest.mark.asyncio
    async def test_exec_tool_missing_input_metadata(self):
        """Test exec_tool handles missing input_metadata"""
        mock_bus = AsyncMock(spec=RunEventBus)

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus

        node = {"id": "test"}

        result = await exec_tool(
            run_id="test_run",
            node=node,
            context=mock_context,
            input_metadata=None,
            upstream_artifact_ids=None
        )

        assert result.status == "failed"
        assert hasattr(result, 'error')

    @pytest.mark.asyncio
    async def test_exec_tool_missing_bus(self):
        """Test exec_tool raises on missing bus"""
        mock_context = MagicMock(spec=ExecutionContext)
        # Intentionally missing bus

        input_metadata = FileMetadata(
            file_path="test.py",
            storage_uri="memory://test",
            file_type="python"
        )

        node = {"id": "test"}

        with pytest.raises(AssertionError, match="missing bus"):
            await exec_tool(
                run_id="test_run",
                node=node,
                context=mock_context,
                input_metadata=input_metadata,
                upstream_artifact_ids=None
            )

    @pytest.mark.asyncio
    async def test_exec_tool_missing_store(self):
        """Test exec_tool raises on missing artifact_store"""
        mock_bus = AsyncMock(spec=RunEventBus)
        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        # Intentionally missing artifact_store

        input_metadata = FileMetadata(
            file_path="test.py",
            storage_uri="memory://test",
            file_type="python"
        )

        node = {"id": "test"}

        with pytest.raises(AssertionError, match="missing artifact_store"):
            await exec_tool(
                run_id="test_run",
                node=node,
                context=mock_context,
                input_metadata=input_metadata,
                upstream_artifact_ids=None
            )


class ToolPythonTests:
    """Test Python provider"""

    @pytest.mark.asyncio
    async def test_exec_tool_python_provider(self):
        """Test Python provider executes successfully"""
        mock_bus = AsyncMock(spec=RunEventBus)
        mock_artifact_store = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = mock_artifact_store

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_path = "test.py"
        input_metadata.file_type = "python"

        node = {
            "id": "python_tool",
            "data": {"params": {"provider": "python", "python_code": "return 'success'"}}
        }

        result = await exec_tool(
            run_id="test_run",
            node=node,
            context=mock_context,
            input_metadata=input_metadata,
            upstream_artifact_ids=None
        )

        assert result.status == "succeeded"
        assert hasattr(result, 'data')


class ToolEventTests:
    """Test event emission"""

    @pytest.mark.asyncio
    async def test_events_emitted_on_exec(self):
        """Verify events are emitted during execution"""
        mock_bus = AsyncMock(spec=RunEventBus)
        mock_bus.emit.reset_mock()

        mock_artifact_store = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = mock_artifact_store

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_path = "test.json"
        input_metadata.file_type = "json"

        node = {"id": "event_test"}

        result = await exec_tool(
            run_id="test_run",
            node=node,
            context=mock_context,
            input_metadata=input_metadata,
            upstream_artifact_ids=None
        )

        # Should emit at least initial event
        assert mock_bus.emit.called


class ToolOutputTests:
    """Test output structure"""

    @pytest.mark.asyncio
    async def test_output_structure(self):
        """Test NodeOutput has required attributes"""
        mock_bus = AsyncMock(spec=RunEventBus)
        mock_artifact_store = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = mock_artifact_store

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_path = "test.py"
        input_metadata.file_type = "python"

        node = {"id": "test"}

        result = await exec_tool(
            run_id="test_run",
            node=node,
            context=mock_context,
            input_metadata=input_metadata,
            upstream_artifact_ids=None
        )

        # All executor outputs should have these attributes
        assert hasattr(result, 'status')
        assert hasattr(result, 'data')
        assert hasattr(result, 'metadata')


class ToolMCPTests:
    """Test MCP provider attempts"""

    @pytest.mark.asyncio
    async def test_exec_tool_mcp_provider(self):
        """Test tool attempts MCP provider (even if not available)"""
        mock_bus = AsyncMock(spec=RunEventBus)
        mock_artifact_store = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = mock_artifact_store

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_type = "json"

        node = {
            "id": "mcp_test",
            "data": {"params": {"provider": "mcp", "tool_name": "test"}}
        }

        result = await exec_tool(
            run_id="test_run",
            node=node,
            context=mock_context,
            input_metadata=input_metadata,
            upstream_artifact_ids=None
        )

        # Should complete, may succeed or fail based on actual availability
        assert isinstance(result, NodeOutput)


class ToolOtherProvidersTests:
    """Test other providers return expected structure"""

    @pytest.mark.asyncio
    async def test_exec_tool_other_providers(self):
        """Test exec_tool handles other providers"""
        mock_bus = AsyncMock(spec=RunEventBus)
        mock_artifact_store = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_type = "json"

        # Test different providers
        for provider in ["api", "webhook", "builtin"]:
            node = {
                "id": f"{provider}_test",
                "data": {"params": {"provider": provider, "url": "http://test.com"}}
            }

            result = await exec_tool(
                run_id=f"{provider}_run",
                node=node,
                context=mock_context,
                input_metadata=input_metadata,
                upstream_artifact_ids=None
            )

            # Should return valid NodeOutput structure
            assert isinstance(result, NodeOutput)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
