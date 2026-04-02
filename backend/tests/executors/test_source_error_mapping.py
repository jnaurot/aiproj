import asyncio
import json

import httpx

from app.executors.source import _source_error_code_for_exception


def test_source_error_mapping_config_invalid():
	assert _source_error_code_for_exception(ValueError("missing_secret: API_KEY")) == "SOURCE_CONFIG_INVALID"
	assert _source_error_code_for_exception(ImportError("requires sqlalchemy")) == "SOURCE_CONFIG_INVALID"


def test_source_error_mapping_connection_failed():
	assert _source_error_code_for_exception(ConnectionError("dial tcp failed")) == "SOURCE_CONNECTION_FAILED"
	assert _source_error_code_for_exception(httpx.RequestError("network down")) == "SOURCE_CONNECTION_FAILED"


def test_source_error_mapping_auth_failed():
	assert _source_error_code_for_exception(PermissionError("forbidden")) == "SOURCE_AUTH_FAILED"
	assert _source_error_code_for_exception(httpx.HTTPStatusError("401", request=None, response=httpx.Response(401))) == "SOURCE_AUTH_FAILED"


def test_source_error_mapping_not_found():
	assert _source_error_code_for_exception(FileNotFoundError("missing file")) == "SOURCE_NOT_FOUND"
	assert _source_error_code_for_exception(httpx.HTTPStatusError("404", request=None, response=httpx.Response(404))) == "SOURCE_NOT_FOUND"


def test_source_error_mapping_parse_failed():
	assert _source_error_code_for_exception(json.JSONDecodeError("bad json", "x", 0)) == "SOURCE_PARSE_FAILED"
	assert _source_error_code_for_exception(UnicodeDecodeError("utf-8", b"\x80", 0, 1, "invalid")) == "SOURCE_PARSE_FAILED"


def test_source_error_mapping_timeout():
	assert _source_error_code_for_exception(asyncio.TimeoutError()) == "SOURCE_TIMEOUT"
	assert _source_error_code_for_exception(httpx.TimeoutException("timeout")) == "SOURCE_TIMEOUT"
