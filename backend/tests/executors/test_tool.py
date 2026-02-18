"""Comprehensive pytest tests for Tool executor - exec_tool function"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call
import aiohttp

from app.executors.tool import exec_tool, iso_now
from app.runner.metadata import ExecutionContext, FileMetadata, NodeOutput
from app.runner.events import RunEventBus


class TestToolHelperFunctions:
    """Test helper functions"""

    @pytest.mark.asyncio
    async def test_iso_now_format(self):
        """Test timestamp format is ISO 8601"""
        timestamp = iso_now()
        assert isinstance(timestamp, str)
        assert "T" in timestamp
        assert "Z" in timestamp or "+" in timestamp


class TestToolBasicFunctionality:
    """Test basic exec_tool functionality"""

    @pytest.mark.asyncio
    async def test_exec_tool_with_valid_params(self):
        """Test exec_tool executes successfully with valid parameters"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_path = "input.json"
        input_metadata.file_type = "json"
        input_metadata.size_bytes = 1024

        node = {
            "id": "test_tool_node",
            "data": {
                "params": {
                    "provider": "mcp",
                    "tool_name": "test_tool"
                }
            }
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
        mock_bus.emit.assert_any_call()

    @pytest.mark.asyncio
    async def test_exec_tool_missing_input_metadata(self):
        """Test exec_tool fails gracefully without input_metadata"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus

        node = {
            "id": "test_node",
            "data": {"params": {}}
        }

        result = await exec_tool(
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
    async def test_exec_tool_missing_params(self):
        """Test exec_tool fails when params are missing"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_type = "json"

        node = {
            "id": "no_params_node",
            "data": {"params": None}
        }

        result = await exec_tool(
            run_id="test_run",
            node=node,
            context=mock_context,
            input_metadata=input_metadata,
            upstream_artifact_ids=None
        )

        assert result.status == "failed"


class TestToolMCPProvider:
    """Test MCP provider in exec_tool"""

    @pytest.mark.asyncio
    async def test_exec_tool_mcp_provider_success(self):
        """Test MCP provider executes successfully"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_path = "input.json"
        input_metadata.file_type = "json"

        node = {
            "id": "mcp_node",
            "data": {
                "params": {
                    "provider": "mcp",
                    "mcp_server": "test-server",
                    "tool_name": "get_weather"
                }
            }
        }

        # Mock the invoke_mcp function
        async def mock_invoke_mcp(*args, **kwargs):
            return {"result": "success", "weather": "sunny"}

        with patch('app.executors.tool.invoke_mcp', side_effect=mock_invoke_mcp):
            result = await exec_tool(
                run_id="test_run",
                node=node,
                context=mock_context,
                input_metadata=input_metadata,
                upstream_artifact_ids=None
            )

            assert result.status == "succeeded"
            assert result.data is not None
            assert isinstance(result.data, str)
            mock_bus.emit.assert_called()

    @pytest.mark.asyncio
    async def test_exec_tool_mcp_provider_missing_import(self):
        """Test MCP provider when module is missing"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_type = "json"

        node = {
            "id": "missing_mcp_node",
            "data": {
                "params": {
                    "provider": "mcp",
                    "mcp_server": "test"
                }
            }
        }

        with patch('app.executors.tool.invoke_mcp') as mock_invoke:
            # Make invoke_mcp raise ImportError
            mock_invoke.side_effect = ImportError("MCP module not found")
            result = await exec_tool(
                run_id="test_run",
                node=node,
                context=mock_context,
                input_metadata=input_metadata,
                upstream_artifact_ids=None
            )

            assert result.status == "failed"
            assert "not available" in result.error.lower()

    @pytest.mark.asyncio
    async def test_exec_tool_mcp_provider_execution_error(self):
        """Test MCP provider when execution fails"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_type = "json"

        node = {
            "id": "mcp_error_node",
            "data": {
                "params": {
                    "provider": "mcp"
                }
            }
        }

        async def mock_invoke_mcp(*args, **kwargs):
            raise Exception("Tool execution failed")

        with patch('app.executors.tool.invoke_mcp', side_effect=mock_invoke_mcp):
            result = await exec_tool(
                run_id="test_run",
                node=node,
                context=mock_context,
                input_metadata=input_metadata,
                upstream_artifact_ids=None
            )

            assert result.status == "failed"
            assert "failed" in result.error.lower()
            mock_bus.emit.assert_called()


class TestToolPythonProvider:
    """Test Python provider in exec_tool"""

    @pytest.mark.asyncio
    async def test_exec_tool_python_provider_success(self):
        """Test Python provider executes successfully"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_type = "json"

        node = {
            "id": "python_node",
            "data": {
                "params": {
                    "provider": "python",
                    "python_code": "return 'Python execution successful'"
                }
            }
        }

        result = await exec_tool(
            run_id="test_run",
            node=node,
            context=mock_context,
            input_metadata=input_metadata,
            upstream_artifact_ids=None
        )

        assert result.status == "succeeded"
        assert result.data is not None

    @pytest.mark.asyncio
    async def test_exec_tool_python_provider_no_code(self):
        """Test Python provider when no code is provided"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_type = "json"

        node = {
            "id": "no_code_node",
            "data": {
                "params": {
                    "provider": "python",
                    "python_code": ""
                }
            }
        }

        result = await exec_tool(
            run_id="test_run",
            node=node,
            context=mock_context,
            input_metadata=input_metadata,
            upstream_artifact_ids=None
        )

        assert result.status == "failed"
        assert "not provided" in result.error.lower()

    @pytest.mark.asyncio
    async def test_exec_tool_python_provider_error(self):
        """Test Python provider when code execution fails"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_type = "json"

        node = {
            "id": "python_error_node",
            "data": {
                "params": {
                    "provider": "python",
                    "python_code": "raise ValueError('Test error')"
                }
            }
        }

        result = await exec_tool(
            run_id="test_run",
            node=node,
            context=mock_context,
            input_metadata=input_metadata,
            upstream_artifact_ids=None
        )

        assert result.status == "failed"
        assert "failed" in result.error.lower()


class TestToolAPIProvider:
    """Test API provider in exec_tool"""

    @pytest.mark.asyncio
    async def test_exec_tool_api_provider_success(self):
        """Test API provider with successful response"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_type = "json"

        node = {
            "id": "api_node",
            "data": {
                "params": {
                    "provider": "api",
                    "url": "https://api.example.com/data",
                    "method": "GET"
                }
            }
        }

        # Mock aiohttp.ClientSession response
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.text.return_value = json.dumps({"data": "success"})

        async with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = MagicMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)

            mock_request_context = MagicMock()
            mock_request_context.__aenter__ = AsyncMock(return_value=mock_response)
            mock_request_context.__aexit__ = AsyncMock(return_value=None)

            mock_session.request = MagicMock(return_value=mock_request_context)

            mock_session_class.return_value = mock_session

            result = await exec_tool(
                run_id="test_run",
                node=node,
                context=mock_context,
                input_metadata=input_metadata,
                upstream_artifact_ids=None
            )

            assert result.status == "succeeded"
            assert result.data is not None

    @pytest.mark.asyncio
    async def test_exec_tool_api_provider_error_response(self):
        """Test API provider with error status code"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_type = "json"

        node = {
            "id": "api_error_node",
            "data": {
                "params": {
                    "provider": "api",
                    "url": "https://api.example.com/error",
                    "method": "GET"
                }
            }
        }

        # Mock error response
        mock_response = MagicMock()
        mock_response.status = 404

        async with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = MagicMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)

            mock_request_context = MagicMock()
            mock_request_context.__aenter__ = AsyncMock(return_value=mock_response)
            mock_request_context.__aexit__ = AsyncMock(return_value=None)

            mock_session.request = MagicMock(return_value=mock_request_context)

            mock_session_class.return_value = mock_session

            result = await exec_tool(
                run_id="test_run",
                node=node,
                context=mock_context,
                input_metadata=input_metadata,
                upstream_artifact_ids=None
            )

            assert result.status == "failed"
            assert "404" in str(result.error).lower()

    @pytest.mark.asyncio
    async def test_exec_tool_api_provider_missing_url(self):
        """Test API provider when URL is missing"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_type = "json"

        node = {
            "id": "no_url_node",
            "data": {
                "params": {
                    "provider": "api",
                    "url": ""
                }
            }
        }

        result = await exec_tool(
            run_id="test_run",
            node=node,
            context=mock_context,
            input_metadata=input_metadata,
            upstream_artifact_ids=None
        )

        assert result.status == "failed"
        assert "not provided" in result.error.lower()

    @pytest.mark.asyncio
    async def test_exec_tool_api_provider_connection_error(self):
        """Test API provider with connection error"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_type = "json"

        node = {
            "id": "api_error_node",
            "data": {
                "params": {
                    "provider": "api",
                    "url": "https://nonexistent.example.com"
                }
            }
        }

        async with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = MagicMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)

            mock_session.request = AsyncMock(side_effect=aiohttp.ClientError("Connection failed"))

            mock_session_class.return_value = mock_session

            result = await exec_tool(
                run_id="test_run",
                node=node,
                context=mock_context,
                input_metadata=input_metadata,
                upstream_artifact_ids=None
            )

            assert result.status == "failed"
            assert "failed" in result.error.lower()


class TestToolWebhookProvider:
    """Test other provider types in exec_tool"""

    @pytest.mark.asyncio
    async def test_exec_tool_webhook_provider(self):
        """Test webhook provider (builtin type)"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_type = "json"

        node = {
            "id": "webhook_node",
            "data": {
                "params": {
                    "provider": "webhook",
                    "webhook_url": "https://example.com/webhook"
                }
            }
        }

        result = await exec_tool(
            run_id="test_run",
            node=node,
            context=mock_context,
            input_metadata=input_metadata,
            upstream_artifact_ids=None
        )

        assert result.status == "failed"  # Should fail as not implemented
        assert "not yet implemented" in result.error.lower()

    @pytest.mark.asyncio
    async def test_exec_tool_builtin_provider(self):
        """Test builtin provider (other types)"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_type = "json"

        node = {
            "id": "builtin_node",
            "data": {
                "params": {
                    "provider": "builtin",
                    "builtin_name": "email"
                }
            }
        }

        result = await exec_tool(
            run_id="test_run",
            node=node,
            context=mock_context,
            input_metadata=input_metadata,
            upstream_artifact_ids=None
        )

        assert result.status == "failed"
        assert "not yet implemented" in result.error.lower()

    @pytest.mark.asyncio
    async def test_exec_tool_script_provider(self):
        """Test script provider (other type)"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_type = "json"

        node = {
            "id": "script_node",
            "data": {
                "params": {
                    "provider": "script",
                    "script_path": "/path/to/script.py"
                }
            }
        }

        result = await exec_tool(
            run_id="test_run",
            node=node,
            context=mock_context,
            input_metadata=input_metadata,
            upstream_artifact_ids=None
        )

        assert result.status == "failed"
        assert "not yet implemented" in result.error.lower()


class TestToolInvalidProviders:
    """Test exec_tool with unsupported providers"""

    @pytest.mark.asyncio
    async def test_exec_tool_unsupported_provider(self):
        """Test with completely unsupported provider"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_type = "json"

        node = {
            "id": "unsupported_node",
            "data": {
                "params": {
                    "provider": "unsupported_provider_type"
                }
            }
        }

        result = await exec_tool(
            run_id="test_run",
            node=node,
            context=mock_context,
            input_metadata=input_metadata,
            upstream_artifact_ids=None
        )

        assert result.status == "failed"
        assert "Unsupported" in result.error or "not yet implemented" in result.error.lower()


class TestToolEventEmission:
    """Test that events are properly emitted"""

    @pytest.mark.asyncio
    async def test_exec_tool_emits_events_on_mcp_success(self):
        """Test event emission on MCP tool success"""
        mock_bus = MagicMock(spec=RunEventBus)
        emit_mock = mock_bus.emit

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_type = "json"

        node = {
            "id": "mcp_event_node",
            "data": {
                "params": {
                    "provider": "mcp",
                    "tool_name": "test"
                }
            }
        }

        with patch('app.executors.tool.invoke_mcp', return_value="success"):
            result = await exec_tool(
                run_id="test_run",
                node=node,
                context=mock_context,
                input_metadata=input_metadata,
                upstream_artifact_ids=None
            )

            assert emit_mock.call_count > 0

    @pytest.mark.asyncio
    async def test_exec_tool_emits_events_on_failure(self):
        """Test event emission on tool failure"""
        mock_bus = MagicMock(spec=RunEventBus)
        emit_mock = mock_bus.emit

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_type = "json"

        node = {
            "id": "fail_node",
            "data": {
                "params": {
                    "provider": "mcp"
                }
            }
        }

        with patch('app.executors.tool.invoke_mcp', side_effect=Exception("Failed")):
            result = await exec_tool(
                run_id="test_run",
                node=node,
                context=mock_context,
                input_metadata=input_metadata,
                upstream_artifact_ids=None
            )

            assert result.status == "failed"
            assert emit_mock.call_count > 0
            # Check that error event was emitted
            error_calls = [call for call in emit_mock.call_args_list if 'error' in str(call).lower()]
            assert len(error_calls) > 0


class TestToolOutputStructure:
    """Test the output structure of exec_tool"""

    @pytest.mark.asyncio
    async def test_exec_tool_output_is_node_output(self):
        """Test that output is NodeOutput instance"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_type = "json"

        node = {
            "id": "output_node",
            "data": {
                "params": {
                    "provider": "python",
                    "python_code": "return 'test output'"
                }
            }
        }

        result = await exec_tool(
            run_id="test_run",
            node=node,
            context=mock_context,
            input_metadata=input_metadata,
            upstream_artifact_ids=None
        )

        assert isinstance(result, NodeOutput)
        assert hasattr(result, 'status')
        assert hasattr(result, 'data')
        assert hasattr(result, 'metadata')

    @pytest.mark.asyncio
    async def test_exec_tool_failed_output_has_error(self):
        """Test that failed output includes error message"""
        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_type = "json"

        node = {
            "id": "error_node",
            "data": {"params": {}}
        }

        result = await exec_tool(
            run_id="test_run",
            node=node,
            context=mock_context,
            input_metadata=input_metadata,
            upstream_artifact_ids=None
        )

        assert result.status == "failed"
        assert hasattr(result, 'error')
        assert result.error is not None


class TestToolParallelExecution:
    """Test for parallel execution support"""

    @pytest.mark.asyncio
    async def test_exec_tool_multiple_calls_parallel(self):
        """Test that multiple tool executions can happen concurrently"""
        import asyncio

        mock_bus = MagicMock(spec=RunEventBus)
        mock_bus.emit = AsyncMock()

        async def mock_executor(run_id, node, context, input_metadata, artifact_ids):
            await asyncio.sleep(0.1)  # Simulate async work
            return NodeOutput(
                status="succeeded",
                data=f"Result from {node['id']}"
            )

        mock_context = MagicMock(spec=ExecutionContext)
        mock_context.bus = mock_bus
        mock_context.artifact_store = MagicMock()

        input_metadata = MagicMock(spec=FileMetadata)
        input_metadata.file_type = "json"

        # Create multiple parallel executions
        tasks = [
            exec_tool(
                run_id="parallel_run",
                node={"id": f"tool_{i}", "data": {"params": {"provider": "python", "python_code": f"{i}"}}},
                context=mock_context,
                input_metadata=input_metadata,
                upstream_artifact_ids=None
            )
            for i in range(3)
        ]

        results = await asyncio.gather(*tasks)

        assert len(results) == 3
        for result in results:
            assert isinstance(result, NodeOutput)
            assert result.status == "succeeded"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
