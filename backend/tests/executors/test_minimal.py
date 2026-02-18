"""Minimal test file"""
import pytest
from unittest.mock import MagicMock

# Import the function we want to test
from app.executors.transform import iso_now


class TestIsoNow:
    """Test iso_now function"""

    def test_iso_now_format(self):
        """Test timestamp format is ISO 8601"""
        timestamp = iso_now()
        assert isinstance(timestamp, str)
        assert "T" in timestamp
        assert "Z" in timestamp or "+" in timestamp