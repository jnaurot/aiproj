"""
Unit tests for utility functions
"""
import pytest
from unittest.mock import Mock, patch
from app.runner.utils import (
    calculate_file_hash,
    validate_file_type,
    format_execution_time,
    sanitize_filename
)


class TestFileHash:
    """Test file hash calculation"""
    
    def test_calculate_file_hash_empty(self):
        """Test calculating hash of empty file"""
        import io
        
        data = io.BytesIO(b"")
        hash_result = calculate_file_hash(data)
        
        assert hash_result is not None
        assert isinstance(hash_result, str)
    
    def test_calculate_file_hash_content(self):
        """Test calculating hash with content"""
        import io
        
        data = io.BytesIO(b"test content")
        hash_result = calculate_file_hash(data)
        
        assert hash_result is not None
    
    def test_calculate_file_hash_different_content(self):
        """Test different content produces different hash"""
        import io
        
        data1 = io.BytesIO(b"content 1")
        data2 = io.BytesIO(b"content 2")
        
        hash1 = calculate_file_hash(data1)
        hash2 = calculate_file_hash(data2)
        
        assert hash1 != hash2
    
    def test_calculate_file_hash_large_file(self):
        """Test calculating hash of large file"""
        import io
        
        # Create large file content
        large_content = b"x" * 10 * 1024 * 1024  # 10MB
        data = io.BytesIO(large_content)
        
        hash_result = calculate_file_hash(data)
        
        assert hash_result is not None
        assert isinstance(hash_result, str)
    
    @patch('app.runner.utils.sha256')
    def test_calculate_file_hash_uses_mock(self, mock_sha256):
        """Test file hash uses sha256 function"""
        import io
        
        mock_sha256.return_value = "mock-hash"
        
        data = io.BytesIO(b"test")
        hash_result = calculate_file_hash(data)
        
        assert hash_result == "mock-hash"
        mock_sha256.assert_called_once()
    
    def test_calculate_file_hash_binary_mode(self):
        """Test calculating hash in binary mode"""
        import io
        
        data = io.BytesIO(b"\x00\x01\x02\xff")
        hash_result = calculate_file_hash(data)
        
        assert hash_result is not None


class TestFileValidation:
    """Test file validation functions"""
    
    def test_validate_file_type_csv(self):
        """Test CSV file validation"""
        filename = "data.csv"
        file_type = validate_file_type(filename)
        
        assert file_type == "csv"
    
    def test_validate_file_type_parquet(self):
        """Test parquet file validation"""
        filename = "data.parquet"
        file_type = validate_file_type(filename)
        
        assert file_type == "parquet"
    
    def test_validate_file_type_json(self):
        """Test JSON file validation"""
        filename = "data.json"
        file_type = validate_file_type(filename)
        
        assert file_type == "json"
    
    def test_validate_file_type_excel(self):
        """Test Excel file validation"""
        filename = "data.xlsx"
        file_type = validate_file_type(filename)
        
        assert file_type == "excel"
    
    def test_validate_file_type_txt(self):
        """Test text file validation"""
        filename = "data.txt"
        file_type = validate_file_type(filename)
        
        assert file_type == "txt"
    
    def test_validate_file_type_image(self):
        """Test image file validation"""
        filename = "data.png"
        file_type = validate_file_type(filename)
        
        assert file_type == "image"
    
    def test_validate_file_type_unknown(self):
        """Test unknown file type"""
        filename = "data.xyz"
        file_type = validate_file_type(filename)
        
        assert file_type == "unknown"
    
    def test_validate_file_type_case_sensitive(self):
        """Test file type validation respects case"""
        filename = "DATA.CSV"
        file_type = validate_file_type(filename)
        
        # Should match even with different case
        assert file_type == "csv"


class TestExecutionTimeFormatting:
    """Test execution time formatting"""
    
    def test_format_execution_time_short(self):
        """Test formatting short duration"""
        duration_ms = 125.5
        
        formatted = format_execution_time(duration_ms)
        
        assert isinstance(formatted, str)
        assert "125.5" in formatted or "125" in formatted
    
    def test_format_execution_time_medium(self):
        """Test formatting medium duration"""
        duration_ms = 5000.0
        
        formatted = format_execution_time(duration_ms)
        
        assert isinstance(formatted, str)
    
    def test_format_execution_time_long(self):
        """Test formatting long duration"""
        duration_ms = 100000.5
        
        formatted = format_execution_time(duration_ms)
        
        assert isinstance(formatted, str)
    
    def test_format_execution_time_zero(self):
        """Test formatting zero duration"""
        duration_ms = 0.0
        
        formatted = format_execution_time(duration_ms)
        
        assert "0" in formatted.lower()
    
    def test_format_execution_time_negative(self):
        """Test formatting negative duration"""
        duration_ms = -100.5
        
        formatted = format_execution_time(duration_ms)
        
        # Should handle gracefully
        assert isinstance(formatted, str)
    
    @patch('app.runner.utils.HUMANIZED_TIME_UNITS')
    def test_format_execution_time_with_units(self, mock_units):
        """Test formatting with humanized units"""
        mock_units.return_value = {"s": "seconds", "ms": "milliseconds"}
        
        duration_ms = 1000.0
        
        formatted = format_execution_time(duration_ms)
        
        assert isinstance(formatted, str)


class TestFilenameSanitization:
    """Test filename sanitization"""
    
    def test_sanitize_filename_basic(self):
        """Test basic filename sanitization"""
        filename = "test file.csv"
        sanitized = sanitize_filename(filename)
        
        assert " " in sanitized or "_" in sanitized
    
    def test_sanitize_filename_special_chars(self):
        """Test sanitizing filename with special characters"""
        filename = "test file@#$%.csv"
        sanitized = sanitize_filename(filename)
        
        assert "@" not in sanitized and "#" not in sanitized
    
    def test_sanitize_filename_windows_incompatible(self):
        """Test sanitizing Windows incompatible characters"""
        filename = "test:file.csv"
        sanitized = sanitize_filename(filename)
        
        assert ":" not in sanitized
    
    def test_sanitize_filename_empty(self):
        """Test sanitizing empty filename"""
        filename = ""
        sanitized = sanitize_filename(filename)
        
        assert sanitized == ""
    
    def test_sanitize_filename_only_extensions(self):
        """Test sanitizing filename with only extension"""
        filename = ".csv"
        sanitized = sanitize_filename(filename)
        
        assert sanitized == ".csv"
    
    def test_sanitize_filename_long(self):
        """Test sanitizing long filename"""
        filename = "a" * 1000 + ".csv"
        sanitized = sanitize_filename(filename)
        
        assert len(sanitized) <= 255  # Windows max length


class TestUtilityHelpers:
    """Test other utility helper functions"""
    
    def test_import_helper_success(self):
        """Test successful import helper"""
        from app.runner.utils import import_module
        
        module = import_module("os")
        
        assert module is not None
    
    def test_import_helper_failure(self):
        """Test import helper failure"""
        from app.runner.utils import import_module
        
        module = import_module("nonexistent.module")
        
        assert module is None
    
    def test_config_loading_helper(self):
        """Test config loading helper"""
        import tempfile
        import os
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            f.write('{"key": "value"}')
            temp_path = f.name
        
        try:
            from app.runner.utils import load_config
            
            config = load_config(temp_path)
            
            assert "key" in config
            assert config["key"] == "value"
        finally:
            os.unlink(temp_path)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
