# """
# Unit tests for executors
# """
# import pytest
# from unittest.mock import Mock, patch, MagicMock
# import pandas as pd
# import json
# from io import StringIO
# # from app.runner.executor import (
# #     SourceExecutor,
# #     TransformExecutor,
# #     LLMExecutor,
# #     ToolExecutor
# # )
# from app.runner.schemas import NodeParamSchema


# class TestSourceExecutor:
#     """Test SourceExecutor"""
    
#     def test_source_executor_initialization(self):
#         """Test executor can be initialized"""
#         executor = SourceExecutor()
#         assert executor is not None
    
#     @patch('app.runner.executors.source.PandasSourceExecutor')
#     def test_execute_file_source(self, mock_source):
#         """Test executing file source node"""
#         # Setup mock
#         mock_source_instance = Mock()
#         mock_source_instance.execute.return_value = pd.DataFrame({"name": ["Alice", "Bob"]})
#         mock_source.return_value = mock_source_instance
        
#         params = {
#             "file_path": "/data/users.csv",
#             "file_format": "csv"
#         }
        
#         executor = SourceExecutor()
#         result = executor.execute(params, Mock())
        
#         assert isinstance(result, pd.DataFrame)
#         assert len(result) == 2
    
#     @patch('app.runner.executors.source.PandasSourceExecutor')
#     def test_execute_database_source(self, mock_source):
#         """Test executing database source node"""
#         from app.runner.schemas import SourceDatabaseParams
        
#         mock_source_instance = Mock()
#         mock_source_instance.execute.return_value = pd.DataFrame({"user_id": [1, 2]})
#         mock_source.return_value = mock_source_instance
        
#         params = SourceDatabaseParams.model_validate({
#             "connection_string": "postgresql://test",
#             "query": "SELECT * FROM users"
#         })
        
#         executor = SourceExecutor()
#         result = executor.execute(params, Mock())
        
#         assert isinstance(result, pd.DataFrame)
    
#     @patch('app.runner.executors.source.PandasSourceExecutor')
#     def test_execute_api_source(self, mock_source):
#         """Test executing API source node"""
#         from app.runner.schemas import SourceAPIParams
        
#         mock_response = Mock()
#         mock_response.json.return_value = [{"id": 1, "name": "Test"}]
#         mock_response.raise_for_status = Mock()
        
#         mock_source_instance = Mock()
#         mock_source_instance.execute.return_value = pd.DataFrame(mock_response.json.return_value)
#         mock_source.return_value = mock_source_instance
        
#         params = SourceAPIParams.model_validate({
#             "url": "https://api.example.com/data",
#             "method": "GET"
#         })
        
#         executor = SourceExecutor()
        
#         with patch('requests.get', return_value=mock_response):
#             result = executor.execute(params, Mock())
        
#         assert isinstance(result, pd.DataFrame)


# class TestTransformExecutor:
#     """Test TransformExecutor"""
    
#     def test_transform_executor_initialization(self):
#         """Test executor can be initialized"""
#         executor = TransformExecutor()
#         assert executor is not None
    
#     def test_execute_filter_transform(self):
#         """Test executing filter transform"""
#         df = pd.DataFrame({
#             "name": ["Alice", "Bob", "Charlie"],
#             "value": [1, 2, 3]
#         })
        
#         params = {
#             "filter_expression": "value > 1",
#             "transform_type": "filter"
#         }
        
#         executor = TransformExecutor()
#         result = executor.execute(params, df)
        
#         assert isinstance(result, pd.DataFrame)
#         assert len(result) == 2
#         assert result[result["value"] > 1].empty is False
    
#     def test_execute_map_transform(self):
#         """Test executing map transform"""
#         df = pd.DataFrame({
#             "name": ["Alice", "Bob"],
#             "age": [25, 30]
#         })
        
#         params = {
#             "function": "upper",
#             "target_columns": ["name"],
#             "transform_type": "map"
#         }
        
#         executor = TransformExecutor()
#         result = executor.execute(params, df)
        
#         assert "name" in result.columns
#         assert result.iloc[0]["name"] == "alice"  # Already uppercased
    
#     def test_execute_aggregate_transform(self):
#         """Test executing aggregate transform"""
#         df = pd.DataFrame({
#             "department": ["Tech", "Tech", "Sales"],
#             "salary": [50000, 60000, 70000]
#         })
        
#         params = {
#             "group_by": ["department"],
#             "aggregations": {"salary": "sum"},
#             "transform_type": "aggregate"
#         }
        
#         executor = TransformExecutor()
#         result = executor.execute(params, df)
        
#         assert isinstance(result, pd.DataFrame)
#         assert set(result["department"]) == {"Tech", "Sales"}
#         assert "salary_sum" in result.columns
    
#     def test_execute_custom_transform(self):
#         """Test executing custom transform"""
#         df = pd.DataFrame({
#             "x": [1, 2, 3],
#             "y": [4, 5, 6]
#         })
        
#         params = {
#             "code": "import pandas as pd\ndf['new_col'] = df['old_col'] * 2",
#             "input_var": "df",
#             "output_var": "result",
#             "transform_type": "custom"
#         }
        
#         executor = TransformExecutor()
#         result = executor.execute(params, df)
        
#         # Should return the result of the code execution
#         assert result is not None


# class TestLLMExecutor:
#     """Test LLMExecutor"""
    
#     def test_llm_executor_initialization(self):
#         """Test executor can be initialized"""
#         executor = LLMExecutor()
#         assert executor is not None
    
#     @patch('openai.ChatCompletion.create')
#     def test_execute_chat_completion(self, mock_openai):
#         """Test executing chat completion"""
#         mock_response = MagicMock()
#         mock_response.choices = [MagicMock(message=MagicMock(content="Hello"))]
#         mock_openai.return_value = mock_response
        
#         params = {
#             "model": "gpt-3.5-turbo",
#             "user_prompt": "Hello, {input_value}",
#             "base_url": "http://localhost:11434",
#             "temperature": 0.7
#         }
        
#         executor = LLMExecutor()
#         result = executor.execute(params, Mock(input_value="test"))
        
#         assert result is not None
#         assert len(mock_openai.call_args_list) == 1
    
#     @patch('app.runner.executors.llm.MockLLMExecutor')
#     def test_execute_ollama(self, mock_executor):
#         """Test executing with Ollama"""
#         from app.runner.schemas import LLMParams
        
#         mock_instance = Mock()
#         mock_instance.execute.return_value = "Response from Ollama"
#         mock_executor.return_value = mock_instance
        
#         params = LLMParams.model_validate({
#             "model": "llama2",
#             "user_prompt": "Test prompt",
#             "connection_ref": "ollama-server"
#         })
        
#         executor = LLMExecutor()
#         result = executor.execute(params, Mock(), llm_kind="ollama")
        
#         assert result == "Response from Ollama"
    
#     @patch('openai.ChatCompletion.create')
#     def test_execute_with_timeout(self, mock_openai):
#         """Test execution with timeout"""
#         import time
        
#         def slow_response(*args, **kwargs):
#             time.sleep(0.5)
#             return MagicMock(choices=[MagicMock(message=MagicMock(content="Done"))])
        
#         mock_openai.side_effect = slow_response
        
#         params = {
#             "model": "gpt-3.5-turbo",
#             "user_prompt": "Slow response needed",
#             "base_url": "http://localhost:11434",
#             "timeout_seconds": 1
#         }
        
#         executor = LLMExecutor()
#         result = executor.execute(params, Mock())
        
#         assert result is not None
    
#     @patch('openai.ChatCompletion.create')
#     def test_execute_retry_on_failure(self, mock_openai):
#         """Test execution retries on failure"""
#         mock_response = MagicMock()
#         mock_response.choices = [MagicMock(message=MagicMock(content="Response"))]
#         mock_response.raise_for_status = Mock()
        
#         mock_openai.side_effect = [Mock(), mock_response]  # First fails, second succeeds
        
#         params = {
#             "model": "gpt-3.5-turbo",
#             "user_prompt": "Test",
#             "base_url": "http://localhost:11434",
#             "max_retries": 1
#         }
        
#         executor = LLMExecutor()
#         result = executor.execute(params, Mock())
        
#         assert result is not None
#         assert mock_openai.call_count == 2


# class TestToolExecutor:
#     """Test ToolExecutor"""
    
#     def test_tool_executor_initialization(self):
#         """Test executor can be initialized"""
#         executor = ToolExecutor()
#         assert executor is not None
    
#     @patch('app.runner.executors.tool.MCPToolExecutor')
#     def test_execute_mcp_tool(self, mock_executor):
#         """Test executing MCP tool"""
#         from app.runner.schemas import MCPToolParams
        
#         mock_instance = Mock()
#         mock_instance.execute.return_value = {"result": "success"}
#         mock_executor.return_value = mock_instance
        
#         params = MCPToolParams.model_validate({
#             "mcp_server": "test-server",
#             "mcp_tool": "test_tool"
#         })
        
#         executor = ToolExecutor()
#         result = executor.execute(params, Mock())
        
#         assert result == {"result": "success"}
    
#     @patch('app.runner.executors.tool.MockToolExecutor')
#     def test_execute_python_tool(self, mock_executor):
#         """Test executing Python tool"""
#         from app.runner.schemas import PythonToolParams
        
#         mock_instance = Mock()
#         mock_instance.execute.return_value = "Python execution result"
#         mock_executor.return_value = mock_instance
        
#         params = PythonToolParams.model_validate({
#             "python_code": "return 'Hello World'"
#         })
        
#         executor = ToolExecutor()
#         result = executor.execute(params, Mock())
        
#         assert result == "Python execution result"
    
#     @patch('app.runner.executors.tool.MockToolExecutor')
#     def test_execute_builtin_tool(self, mock_executor):
#         """Test executing built-in tool"""
#         from app.runner.schemas import BuiltinToolParams
        
#         mock_instance = Mock()
#         mock_instance.execute.return_value = "Tool executed"
#         mock_executor.return_value = mock_instance
        
#         params = BuiltinToolParams.model_validate({
#             "builtin_name": "email",
#             "config": {"to": "test@example.com", "subject": "Test"}
#         })
        
#         executor = ToolExecutor()
#         result = executor.execute(params, Mock())
        
#         assert result == "Tool executed"


# class TestExecutorWithInvalidParams:
#     """Test executors with invalid parameters"""
    
#     def test_source_executor_invalid_params(self):
#         """Test source executor handles invalid params"""
#         executor = SourceExecutor()
        
#         with pytest.raises(Exception):
#             executor.execute("invalid", Mock())
    
#     def test_transform_executor_invalid_params(self):
#         """Test transform executor handles invalid params"""
#         executor = TransformExecutor()
        
#         with pytest.raises(Exception):
#             executor.execute("invalid", Mock())
    
#     def test_llm_executor_missing_required_params(self):
#         """Test LLM executor handles missing required params"""
#         executor = LLMExecutor()
        
#         with pytest.raises(Exception):
#             executor.execute({}, Mock())


# if __name__ == "__main__":
#     pytest.main([__file__, "-v"])
