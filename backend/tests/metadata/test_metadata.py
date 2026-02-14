"""
Unit tests for metadata models
"""
import pytest
from datetime import datetime, timezone
from typing import Dict, Any
from app.runner.metadata import FileMetadata, NodeOutput, ExecutionContext


class TestFileMetadata:
    """Test FileMetadata validation"""
    
    def test_success_with_required_fields(self):
        """Test successful creation with required fields"""
        file_path = "/path/to/file.csv"
        file_type = "csv"
        mime_type = "text/csv"
        size_bytes = 1024
        content_hash = "abc123"
        
        metadata = FileMetadata(
            file_path=file_path,
            file_type=file_type,
            mime_type=mime_type,
            size_bytes=size_bytes,
            content_hash=content_hash
        )
        
        assert metadata.file_path == file_path
        assert metadata.file_type == file_type
        assert metadata.mime_type == mime_type
        assert metadata.size_bytes == size_bytes
        assert metadata.content_hash == content_hash
    
    def test_optional_schema(self):
        """Test optional data_schema field"""
        schema = {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"}
            }
        }
        
        metadata = FileMetadata(
            file_path="/test.csv",
            file_type="csv",
            mime_type="text/csv",
            content_hash="abc",
            data_schema=schema
        )
        
        assert metadata.data_schema == schema
    
    def test_optional_row_count(self):
        """Test optional row_count field"""
        metadata = FileMetadata(
            file_path="/test.csv",
            file_type="csv",
            mime_type="text/csv",
            content_hash="abc",
            row_count=100
        )
        
        assert metadata.row_count == 100
    
    def test_all_optional_fields(self):
        """Test all optional fields can be set"""
        from unittest.mock import Mock
        
        metadata = FileMetadata(
            file_path="/test.parquet",
            file_type="parquet",
            mime_type="application/octet-stream",
            content_hash="hash123",
            data_schema={},
            row_count=50,
            access_method="s3",
            credentials_key="my-key",
            node_id="node-1",
            params_hash="params-hash",
            input_metadata_hash="input-hash",
            estimated_memory_mb=10.5,
            is_partitioned=True,
            partition_key="date"
        )
        
        assert metadata.data_schema == {}
        assert metadata.row_count == 50
        assert metadata.access_method == "s3"
        assert metadata.credentials_key == "my-key"
        assert metadata.node_id == "node-1"
        assert metadata.params_hash == "params-hash"
        assert metadata.input_metadata_hash == "input-hash"
        assert metadata.estimated_memory_mb == 10.5
        assert metadata.is_partitioned is True
        assert metadata.partition_key == "date"
    
    def test_created_at_auto_generated(self):
        """Test created_at is auto-generated"""
        metadata = FileMetadata(
            file_path="/test.csv",
            file_type="csv",
            mime_type="text/csv",
            size_bytes=1024,
            content_hash="abc"
        )
        
        assert isinstance(metadata.created_at, datetime)
        assert metadata.created_at.tzinfo is not None  # UTC timezone
    
    def test_different_file_types(self):
        """Test different file types are valid"""
        valid_types = ["csv", "parquet", "json", "txt", "binary", "image"]
        
        for file_type in valid_types:
            metadata = FileMetadata(
                file_path=f"/test.{file_type}",
                file_type=file_type,
                mime_type="application/octet-stream",
                content_hash="hash"
            )
            assert metadata.file_type == file_type
    
    def test_access_methods(self):
        """Test different access methods are valid"""
        methods = ["local", "s3", "postgres", "http"]
        
        for method in methods:
            metadata = FileMetadata(
                file_path="/test.csv",
                file_type="csv",
                mime_type="text/csv",
                content_hash="hash",
                access_method=method
            )
            assert metadata.access_method == method
    
    def test_lineage_fields(self):
        """Test lineage fields"""
        metadata = FileMetadata(
            file_path="/test.csv",
            file_type="csv",
            mime_type="text/csv",
            content_hash="hash",
            node_id="source-node-1",
            params_hash="params-456",
            input_metadata_hash="input-hash"
        )
        
        assert metadata.node_id == "source-node-1"
        assert metadata.params_hash == "params-456"
        assert metadata.input_metadata_hash == "input-hash"


class TestNodeOutput:
    """Test NodeOutput validation"""
    
    def test_success_succeeded(self):
        """Test successful node output"""
        output = NodeOutput(
            status="succeeded",
            metadata=None,
            data="output data",
            execution_time_ms=100.5
        )
        
        assert output.status == "succeeded"
        assert output.data == "output data"
        assert output.execution_time_ms == 100.5
    
    def test_success_failed(self):
        """Test failed node output"""
        output = NodeOutput(
            status="failed",
            metadata=None,
            execution_time_ms=50.0,
            error="Error message here"
        )
        
        assert output.status == "failed"
        assert output.error == "Error message here"
    
    def test_optional_metadata(self):
        """Test metadata is optional"""
        output = NodeOutput(
            status="succeeded",
            execution_time_ms=100.0
        )
        
        assert output.metadata is None
    
    def test_optional_data(self):
        """Test data is optional"""
        output = NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=100.0
        )
        
        assert output.data is None
    
    def test_stale_status(self):
        """Test stale node output"""
        output = NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=100.0,
            is_stale=True,
            stale_reason="Parameters changed"
        )
        
        assert output.is_stale is True
        assert output.stale_reason == "Parameters changed"
    
    def test_all_status_values(self):
        """Test all valid status values"""
        from unittest.mock import Mock
        
        for status in ["succeeded", "failed", "skipped"]:
            metadata = FileMetadata(
                file_path=f"/test.csv",
                file_type="csv",
                mime_type="text/csv",
                size_bytes=100,
                content_hash="hash"
            )
            
            output = NodeOutput(
                status=status,
                metadata=metadata,
                execution_time_ms=100.0
            )
            assert output.status == status


class TestExecutionContext:
    """Test ExecutionContext dataclass"""
    
    def test_required_fields(self):
        """Test required fields are set correctly"""
        from unittest.mock import Mock
        
        # Create mock objects
        mock_bus = Mock()
        mock_store = Mock()
        run_id = "run-123"
        
        context = ExecutionContext(
            run_id=run_id,
            bus=mock_bus,
            artifact_store=mock_store,
            bindings={}
        )
        
        assert context.run_id == run_id
        assert context.bus == mock_bus
        assert context.artifact_store == mock_store
        assert context.bindings == {}
    
    def test_default_optional_fields(self):
        """Test default values for optional fields"""
        from unittest.mock import Mock
        
        mock_bus = Mock()
        mock_store = Mock()
        
        context = ExecutionContext(
            run_id="run-123",
            bus=mock_bus,
            artifact_store=mock_store,
            bindings={}
        )
        
        assert context.outputs == {}
        assert context.metadata_cache == {}
        assert context.execution_version == "v1"
    
    def test_custom_optional_fields(self):
        """Test custom values for optional fields"""
        from unittest.mock import Mock
        
        mock_bus = Mock()
        mock_store = Mock()
        
        outputs = {"node-1": Mock()}
        metadata_cache = {"file-1": Mock()}
        
        context = ExecutionContext(
            run_id="run-123",
            bus=mock_bus,
            artifact_store=mock_store,
            bindings={},
            outputs=outputs,
            metadata_cache=metadata_cache,
            execution_version="v2"
        )
        
        assert context.outputs == outputs
        assert context.metadata_cache == metadata_cache
        assert context.execution_version == "v2"
    
    def test_required_fields_must_be_provided(self):
        """Test that required fields must be provided"""
        from unittest.mock import Mock
        
        with pytest.raises(TypeError):
            # Missing run_id
            ExecutionContext(
                bus=Mock(),
                artifact_store=Mock(),
                bindings={}
            )
        
        with pytest.raises(TypeError):
            # Missing bus
            ExecutionContext(
                run_id="run-123",
                artifact_store=Mock(),
                bindings={}
            )
        
        with pytest.raises(TypeError):
            # Missing artifact_store
            ExecutionContext(
                run_id="run-123",
                bus=Mock(),
                bindings={}
            )


class TestFileMetadataFileTypes:
    """Test file type combinations"""
    
    def test_csv_type_with_csv_file(self):
        """Test CSV file with CSV type"""
        metadata = FileMetadata(
            file_path="/data/users.csv",
            file_type="csv",
            mime_type="text/csv",
            content_hash="hash"
        )
        
        assert metadata.file_type == "csv"
        assert metadata.mime_type == "text/csv"
    
    def test_csv_type_with_other_formats(self):
        """Test CSV type can be used with various files"""
        metadata = FileMetadata(
            file_path="/data/data.csv",
            file_type="csv",
            mime_type="text/csv",
            content_hash="hash"
        )
        
        # Valid combination
        assert metadata.file_type == "csv"
    
    def test_parquet_type(self):
        """Test parquet file with parquet type"""
        metadata = FileMetadata(
            file_path="/data/data.parquet",
            file_type="parquet",
            mime_type="application/octet-stream",
            content_hash="hash"
        )
        
        assert metadata.file_type == "parquet"
    
    def test_json_type(self):
        """Test JSON file with json type"""
        metadata = FileMetadata(
            file_path="/data/data.json",
            file_type="json",
            mime_type="application/json",
            content_hash="hash"
        )
        
        assert metadata.file_type == "json"
        assert metadata.mime_type == "application/json"
    
    def test_txt_type(self):
        """Test text file with txt type"""
        metadata = FileMetadata(
            file_path="/data/data.txt",
            file_type="txt",
            mime_type="text/plain",
            content_hash="hash"
        )
        
        assert metadata.file_type == "txt"
        assert metadata.mime_type == "text/plain"
    
    def test_image_type(self):
        """Test image file with image type"""
        metadata = FileMetadata(
            file_path="/data/image.png",
            file_type="image",
            mime_type="image/png",
            content_hash="hash"
        )
        
        assert metadata.file_type == "image"
        assert metadata.mime_type == "image/png"


class TestNodeOutputExecutionTime:
    """Test execution time validation"""
    
    def test_non_negative_execution_time(self):
        """Test execution time should be non-negative"""
        # Valid
        output = NodeOutput(
            status="succeeded",
            execution_time_ms=0.0
        )
        assert output.execution_time_ms >= 0
    
    def test_zero_execution_time(self):
        """Test zero execution time is valid"""
        from unittest.mock import Mock
        
        metadata = FileMetadata(
            file_path="/test.csv",
            file_type="csv",
            mime_type="text/csv",
            size_bytes=100,
            content_hash="hash"
        )
        
        output = NodeOutput(
            status="succeeded",
            metadata=metadata,
            execution_time_ms=0.0
        )
        
        assert output.execution_time_ms == 0.0
    
    def test_large_execution_time(self):
        """Test larger execution times are valid"""
        output = NodeOutput(
            status="succeeded",
            execution_time_ms=1000000.5
        )
        
        assert output.execution_time_ms == 1000000.5
