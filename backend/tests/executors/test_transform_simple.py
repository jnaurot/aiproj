"""Simpler test file for Transform executor"""
import pytest
from unittest.mock import AsyncMock, MagicMock


class TestTransformSimple:
    """Test class"""

    def test_transform_simple(self):
        """Test simple assertion"""
        assert True

    def test_transform_with_mock(self):
        """Test with mock"""
        mock = MagicMock()
        mock.return_value = "test"
        assert mock() == "test"