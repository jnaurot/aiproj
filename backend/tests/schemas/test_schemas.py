"""
Unit tests for schema validation and normalization
"""
import pytest
import sys
from types import SimpleNamespace
from typing import Dict, Any
from app.runner.schemas import (
    normalize_llm_params_frontend,
    normalize_source_params_frontend,
    LLMParams,
    SourceFileParams,
    SourceDatabaseParams,
    SourceAPIParams,
    TransformParamsCurrent,
    FilterTransformParams,
    MapTransformParams,
    AggregateTransformParams,
    CustomTransformParams,
    get_schema_for_node,
    SCHEMA_REGISTRY,
    validate_node_params,
    ComponentParams,
)


class TestLLMParams:
    """Test LLM parameter validation and normalization"""
    
    def test_normalize_camelcase_to_snake_case(self):
        """Test camelCase conversion to snake_case"""
        input_params = {
            "baseUrl": "http://api.example.com",
            "apiKeyRef": "sk-test123",
            "connectionRef": "conn-123"
        }
        
        result = normalize_llm_params_frontend(input_params)
        
        assert result["base_url"] == "http://api.example.com"
        assert result["api_key_ref"] == "sk-test123"
        assert result["connection_ref"] == "conn-123"
    
    def test_normalize_frontend_output_object(self):
        """Test frontend output object normalization keeps schema controls only."""
        input_params = {
            "model": "gpt-4",
            "user_prompt": "Hello",
            "output": {
                "mode": "json",
                "jsonSchema": {"type": "object"},
                "strict": True,
            }
        }
        
        result = normalize_llm_params_frontend(input_params)
        
        assert "output_mode" not in result
        assert result["output_schema"] == {"type": "object"}
        assert result["output_strict"] is True

    def test_normalize_frontend_output_mode_prefers_nested_mode_over_legacy(self):
        input_params = {
            "model": "gpt-4",
            "user_prompt": "Hello",
            "output_mode": "json",
            "output": {"mode": "text"}
        }

        result = normalize_llm_params_frontend(input_params)
        assert "output_mode" not in result
        assert isinstance(result.get("output"), dict)
        assert result["output"].get("mode") == "text"
    
    def test_normalize_stop_sequences(self):
        """Test stop_sequences normalization"""
        input_params = {
            "stopSequences": ["\n", "END"]
        }
        
        result = normalize_llm_params_frontend(input_params)
        
        assert result["stop_sequences"] == ["\n", "END"]

    def test_normalize_new_llm_frontend_keys(self):
        input_params = {
            "stop": ["A", "B"],
            "inputEncoding": "json_canonical",
            "inputEnvelope": [{"type": "text", "text": "hello"}],
            "requestPolicy": {"retries": 2, "timeout_seconds": 15},
            "promptRevisionId": "pr_001",
            "presencePenalty": 0.2,
            "frequencyPenalty": -0.1,
            "repeatPenalty": 1.1,
            "thinking": "on",
            "output": {"mode": "embeddings", "embedding": {"dims": 1536}},
        }
        result = normalize_llm_params_frontend(input_params)
        assert result["stop_sequences"] == ["A", "B"]
        assert result["input_encoding"] == "json_canonical"
        assert result["input_envelope"] == [{"type": "text", "text": "hello"}]
        assert result["request_policy"] == {"retries": 2, "timeout_seconds": 15}
        assert result["prompt_revision_id"] == "pr_001"
        assert result["presence_penalty"] == 0.2
        assert result["frequency_penalty"] == -0.1
        assert result["repeat_penalty"] == 1.1
        assert result["thinking"] == {"enabled": True, "mode": "visible"}
        assert "output_mode" not in result
        assert result["embedding_contract"] == {"dims": 1536}

    def test_normalize_output_validation_mode(self):
        result = normalize_llm_params_frontend(
            {
                "model": "gpt-4",
                "user_prompt": "hi",
                "baseUrl": "https://api.example.com",
                "evalGate": {"enabled": True, "min_output_chars": 5},
                "output": {"mode": "json", "jsonSchema": {"type": "object"}, "validationMode": "soft"},
            }
        )
        assert result["output_validation_mode"] == "soft"
        assert result["eval_gate"] == {"enabled": True, "min_output_chars": 5}
    
    def test_success_with_base_url(self):
        """Test successful validation with base_url"""
        params = {
            "model": "gpt-4",
            "user_prompt": "Test prompt",
            "base_url": "http://api.example.com"
        }
        
        llm_params = LLMParams.model_validate(params)
        assert llm_params.model == "gpt-4"
        assert llm_params.user_prompt == "Test prompt"
        assert llm_params.base_url == "http://api.example.com"
    
    def test_success_with_connection_ref(self):
        """Test successful validation with connection_ref"""
        params = {
            "model": "llama2",
            "user_prompt": "Test",
            "connection_ref": "my-ollama"
        }
        
        llm_params = LLMParams.model_validate(params)
        assert llm_params.model == "llama2"
        assert llm_params.connection_ref == "my-ollama"
    
    def test_requires_base_url_or_connection_ref(self):
        """Test validation error when neither base_url nor connection_ref provided"""
        with pytest.raises(ValueError, match="Either base_url or connection_ref"):
            LLMParams.model_validate({
                "model": "gpt-4",
                "user_prompt": "Test"
            })
    
    def test_requires_output_schema_for_json_mode(self):
        """Test validation error when output_mode=json but no output_schema"""
        with pytest.raises(ValueError, match="output_schema required when output_mode='json'"):
            LLMParams.model_validate({
                "model": "gpt-4",
                "user_prompt": "Test",
                "base_url": "http://api.example.com",
                "output_mode": "json"
            })

    def test_requires_embedding_contract_for_embeddings_mode(self):
        with pytest.raises(ValueError, match="embedding_contract required when output_mode='embeddings'"):
            LLMParams.model_validate(
                {
                    "model": "gpt-4",
                    "user_prompt": "Test",
                    "base_url": "http://api.example.com",
                    "output_mode": "embeddings",
                }
            )

    def test_embeddings_contract_defaults_and_validation(self):
        params = LLMParams.model_validate(
            {
                "model": "gpt-4",
                "user_prompt": "Test",
                "base_url": "http://api.example.com",
                "output_mode": "embeddings",
                "embedding_contract": {"dims": 1536},
            }
        )
        assert params.embedding_contract == {"dims": 1536, "dtype": "float32", "layout": "1d"}

    def test_output_mode_rejects_markdown(self):
        with pytest.raises(ValueError):
            LLMParams.model_validate(
                {
                    "model": "gpt-4",
                    "user_prompt": "Test",
                    "base_url": "http://api.example.com",
                    "output_mode": "markdown",
                }
            )
    
    def test_temperature_validation(self):
        """Test temperature is constrained to [0, 2]"""
        # Should succeed
        LLMParams.model_validate({
            "model": "test",
            "user_prompt": "Test",
            "base_url": "http://test.com",
            "temperature": 1.0
        })
        
        # Too low
        with pytest.raises(ValueError):
            LLMParams.model_validate({
                "model": "test",
                "user_prompt": "Test",
                "base_url": "http://test.com",
                "temperature": -0.1
            })
        
        # Too high
        with pytest.raises(ValueError):
            LLMParams.model_validate({
                "model": "test",
                "user_prompt": "Test",
                "base_url": "http://test.com",
                "temperature": 2.1
            })
    
    def test_max_tokens_validation(self):
        """Test max_tokens is constrained to [1, 100000]"""
        with pytest.raises(ValueError):
            LLMParams.model_validate({
                "model": "test",
                "user_prompt": "Test",
                "base_url": "http://test.com",
                "max_tokens": 0
            })
    
    def test_stop_sequences_default(self):
        """Test stop_sequences has default empty list"""
        llm_params = LLMParams.model_validate({
            "model": "test",
            "user_prompt": "Test",
            "base_url": "http://test.com"
        })
        
        assert llm_params.stop_sequences == []
    
    def test_input_mapping(self):
        """Test input_mapping is preserved"""
        params = {
            "model": "test",
            "user_prompt": "Test {input_value}",
            "base_url": "http://test.com",
            "input_mapping": {
                "input_value": "{{node_input.port_name}}"
            }
        }
        
        llm_params = LLMParams.model_validate(params)
        assert llm_params.input_mapping == {
            "input_value": "{{node_input.port_name}}"
        }

    def test_accepts_input_envelope(self):
        llm_params = LLMParams.model_validate(
            {
                "model": "test",
                "user_prompt": "Describe {input}",
                "base_url": "http://test.com",
                "input_envelope": [
                    {"type": "text", "text": "extra context"},
                    {"type": "image", "dataUrl": "data:image/png;base64,QUJD"},
                    {"type": "audio", "dataUrl": "data:audio/wav;base64,QUJD"},
                ],
            }
        )
        assert isinstance(llm_params.input_envelope, list)
        assert len(llm_params.input_envelope) == 3

    def test_rejects_invalid_input_envelope_part(self):
        with pytest.raises(ValueError, match="input_envelope\\[0\\]\\.type must be one of: text, image, audio"):
            LLMParams.model_validate(
                {
                    "model": "test",
                    "user_prompt": "Describe {input}",
                    "base_url": "http://test.com",
                    "input_envelope": [{"type": "video", "dataUrl": "data:video/mp4;base64,QUJD"}],
                }
            )


class TestSourceFileParams:
    """Test source file parameter validation"""
    
    def test_success_with_defaults(self):
        """Test successful validation with defaults"""
        params = {
            "rel_path": ".",
            "filename": "file.csv"
        }
        
        source_params = SourceFileParams.model_validate(params)
        assert source_params.rel_path == "."
        assert source_params.filename == "file.csv"
        assert source_params.file_format == "csv"
        assert source_params.encoding == "utf-8"
        assert source_params.cache_enabled is True
    
    def test_custom_file_format(self):
        """Test custom file format selection"""
        params = {
            "rel_path": ".",
            "filename": "data.parquet",
            "file_format": "parquet"
        }
        
        source_params = SourceFileParams.model_validate(params)
        assert source_params.file_format == "parquet"
    
    def test_custom_delimiter(self):
        """Test custom CSV delimiter"""
        params = {
            "rel_path": ".",
            "filename": "data.csv",
            "file_format": "csv",
            "delimiter": "|"
        }
        
        source_params = SourceFileParams.model_validate(params)
        assert source_params.delimiter == "|"

    def test_csv_has_header_flag(self):
        params = {
            "rel_path": ".",
            "filename": "data.csv",
            "file_format": "csv",
            "has_header": False,
        }
        source_params = SourceFileParams.model_validate(params)
        assert source_params.has_header is False

    def test_csv_dialect_and_locale_fields(self):
        params = {
            "rel_path": ".",
            "filename": "data.csv",
            "file_format": "csv",
            "quote_char": "\"",
            "escape_char": "\\",
            "malformed_row_policy": "warn",
            "decimal_separator": ",",
            "thousands_separator": ".",
            "date_columns": ["d"],
            "date_format": "%d.%m.%Y",
            "json_mode": "ndjson",
            "json_streaming_enabled": True,
            "json_stream_chunk_lines": 500,
            "json_stream_max_records": 2000,
            "json_flatten_strategy": "deep",
            "json_flatten_separator": "_",
            "excel_import_strategy": "stack",
            "excel_sheets": ["Sheet1", "Sheet2"],
            "excel_merged_cells_policy": "ffill",
            "excel_date_policy": "coerce",
            "excel_date_format": "%Y-%m-%d",
            "txt_record_mode": "fixed_chunk",
            "txt_chunk_size": 256,
            "pdf_extraction_mode": "hybrid",
            "pdf_page_mode": "range",
            "pdf_page_range": "1-2",
            "pdf_page_sample": 2,
            "image_extract_metadata": True,
            "image_svg_policy": "sanitize",
            "image_tiff_pages_mode": "all",
            "audio_extract_metadata": True,
            "audio_normalize": True,
            "audio_target_peak": 0.8,
            "audio_transcode_format": "wav",
            "video_extract_metadata": True,
            "video_frame_mode": "interval",
            "video_frame_interval_sec": 2.0,
            "video_max_frames": 6,
            "parquet_columns": ["id"],
            "parquet_row_groups": [0, 1],
            "parquet_max_rows": 50,
        }
        source_params = SourceFileParams.model_validate(params)
        assert source_params.quote_char == "\""
        assert source_params.escape_char == "\\"
        assert source_params.malformed_row_policy == "warn"
        assert source_params.decimal_separator == ","
        assert source_params.thousands_separator == "."
        assert source_params.date_columns == ["d"]
        assert source_params.date_format == "%d.%m.%Y"
        assert source_params.json_mode == "ndjson"
        assert source_params.json_streaming_enabled is True
        assert source_params.json_stream_chunk_lines == 500
        assert source_params.json_stream_max_records == 2000
        assert source_params.json_flatten_strategy == "deep"
        assert source_params.json_flatten_separator == "_"
        assert source_params.excel_import_strategy == "stack"
        assert source_params.excel_sheets == ["Sheet1", "Sheet2"]
        assert source_params.excel_merged_cells_policy == "ffill"
        assert source_params.excel_date_policy == "coerce"
        assert source_params.excel_date_format == "%Y-%m-%d"
        assert source_params.txt_record_mode == "fixed_chunk"
        assert source_params.txt_chunk_size == 256
        assert source_params.pdf_extraction_mode == "hybrid"
        assert source_params.pdf_page_mode == "range"
        assert source_params.pdf_page_range == "1-2"
        assert source_params.pdf_page_sample == 2
        assert source_params.image_extract_metadata is True
        assert source_params.image_svg_policy == "sanitize"
        assert source_params.image_tiff_pages_mode == "all"
        assert source_params.audio_extract_metadata is True
        assert source_params.audio_normalize is True
        assert source_params.audio_target_peak == 0.8
        assert source_params.audio_transcode_format == "wav"
        assert source_params.video_extract_metadata is True
        assert source_params.video_frame_mode == "interval"
        assert source_params.video_frame_interval_sec == 2.0
        assert source_params.video_max_frames == 6
        assert source_params.parquet_columns == ["id"]
        assert source_params.parquet_row_groups == [0, 1]
        assert source_params.parquet_max_rows == 50
    
    def test_valid_file_formats(self):
        """Test all valid file formats"""
        formats = [
            "csv",
            "parquet",
            "json",
            "excel",
            "txt",
            "pdf",
            "jpg",
            "jpeg",
            "png",
            "webp",
            "gif",
            "svg",
            "tif",
            "tiff",
            "mp3",
            "wav",
            "flac",
            "ogg",
            "m4a",
            "aac",
            "mp4",
            "mov",
            "webm",
        ]
        for fmt in formats:
            params = {
                "rel_path": ".",
                "filename": f"test.{fmt}",
                "file_format": fmt
            }
            SourceFileParams.model_validate(params)
            assert SourceFileParams.model_validate(params).file_format == fmt
    
    def test_missing_file_path(self):
        """Test validation_required flags missing snapshot/path"""
        params = SourceFileParams.model_validate({})
        errors = params.validate_required()
        assert "rel_path is required" in errors
        assert "filename is required" in errors

    def test_snapshot_id_satisfies_required(self):
        params = SourceFileParams.model_validate({"snapshot_id": "a" * 64})
        assert params.validate_required() == []
    
    def test_validate_required_method(self):
        """Test custom validation_required method"""
        params = {
            "rel_path": ".",
            "filename": "test.csv"
        }
        
        source_params = SourceFileParams.model_validate(params)
        errors = source_params.validate_required()
        
        assert errors == []  # No errors with valid input


class TestSourceDatabaseParams:
    """Test source database parameter validation"""
    
    def test_success_with_connection_string(self):
        """Test successful validation with connection_string"""
        params = {
            "connection_string": "postgres://user:pass@localhost/db",
            "query": "SELECT * FROM users"
        }
        
        db_params = SourceDatabaseParams.model_validate(params)
        assert db_params.connection_string == "postgres://user:pass@localhost/db"
        assert db_params.query == "SELECT * FROM users"
    
    def test_success_with_connection_ref(self):
        """Test successful validation with connection_ref"""
        params = {
            "connection_ref": "production-db",
            "table_name": "users"
        }
        
        db_params = SourceDatabaseParams.model_validate(params)
        assert db_params.connection_ref == "production-db"
        assert db_params.table_name == "users"
    
    def test_requires_connection_string_or_ref(self):
        """Test validation_required flags missing connection"""
        params = SourceDatabaseParams.model_validate({"query": "select 1"})
        errors = params.validate_required()
        assert "Either connection_string or connection_ref required" in errors
    
    def test_requires_query_or_table_name(self):
        """Test validation_required flags missing query/table"""
        params = SourceDatabaseParams.model_validate({"connection_string": "postgresql://test"})
        errors = params.validate_required()
        assert "Either query or table_name required" in errors
    
    def test_query_overrides_table_name(self):
        """Test query parameter works independently"""
        params = {
            "connection_string": "postgresql://test",
            "table_name": "users",
            "query": "SELECT * FROM active_users"
        }
        
        db_params = SourceDatabaseParams.model_validate(params)
        assert db_params.query == "SELECT * FROM active_users"
        assert db_params.table_name == "users"  # Still stored

    def test_normalize_source_database_preserves_string_query(self):
        raw = {
            "source_type": "database",
            "connection_string": "postgresql://user:pass@host/db",
            "query": "SELECT * FROM faculty;",
        }
        out = normalize_source_params_frontend(raw)
        assert out.get("query") == "SELECT * FROM faculty;"

    def test_normalize_source_database_rejects_non_string_query(self):
        raw = {
            "source_type": "database",
            "connection_string": "postgresql://user:pass@host/db",
            "query": {},
        }
        out = normalize_source_params_frontend(raw)
        assert out.get("query") is None


class TestSourceAPIParams:
    """Test source API parameter validation"""
    
    def test_success_with_minimum_fields(self):
        """Test successful validation with minimum required fields"""
        params = {
            "url": "https://api.example.com/data"
        }
        
        api_params = SourceAPIParams.model_validate(params)
        assert api_params.url == "https://api.example.com/data"
        assert api_params.method == "GET"
        assert api_params.headers == {}
        assert api_params.query == {}
        assert api_params.body_mode == "none"
    
    def test_custom_method(self):
        """Test custom HTTP method"""
        params = {
            "url": "https://api.example.com/data",
            "method": "HEAD"
        }
        
        api_params = SourceAPIParams.model_validate(params)
        assert api_params.method == "HEAD"
    
    def test_custom_headers(self):
        """Test custom headers"""
        params = {
            "url": "https://api.example.com/data",
            "headers": {
                "Authorization": "Bearer token123",
                "Content-Type": "application/json"
            }
        }
        
        api_params = SourceAPIParams.model_validate(params)
        assert api_params.headers["Authorization"] == "Bearer token123"
        assert api_params.headers["Content-Type"] == "application/json"
    
    def test_body_parameter_migrates_to_json_mode(self):
        """Test legacy request body migrates to json body mode"""
        params = {
            "url": "https://api.example.com/data",
            "method": "POST",
            "body_mode": "json",
            "body_json": {"key": "value"},
            "content_type": "application/json",
        }
        
        api_params = SourceAPIParams.model_validate(params)
        assert api_params.body_mode == "json"
        assert api_params.body_json == {"key": "value"}
        assert api_params.content_type == "application/json"

    def test_form_mode_defaults_content_type(self):
        params = {
            "url": "https://api.example.com/data",
            "method": "POST",
            "body_mode": "form",
            "body_form": {"a": "1"},
        }
        api_params = SourceAPIParams.model_validate(params)
        assert api_params.body_mode == "form"
        assert api_params.body_form == {"a": "1"}
        assert api_params.content_type == "application/x-www-form-urlencoded"
    
    def test_auth_types(self):
        """Test different authentication types"""
        # None
        api_params = SourceAPIParams.model_validate({
            "url": "https://api.example.com/data",
            "auth_type": "none"
        })
        assert api_params.auth_type == "none"
        
        # Bearer
        api_params = SourceAPIParams.model_validate({
            "url": "https://api.example.com/data",
            "auth_type": "bearer",
            "auth_token_ref": "token123"
        })
        assert api_params.auth_type == "bearer"
        assert api_params.auth_token_ref == "token123"
    
    def test_requires_url(self):
        """Test validation error when url is missing"""
        with pytest.raises(ValueError):
            SourceAPIParams.model_validate({})
    
    def test_requires_auth_token_for_bearer(self):
        """Test validation_required flags missing auth token for bearer"""
        params = SourceAPIParams.model_validate(
            {
                "url": "https://api.example.com/data",
                "auth_type": "bearer"
            }
        )
        errors = params.validate_required()
        assert "auth_token_ref required when using authentication" in errors
    
    def test_timeout_validation(self):
        """Test timeout_seconds is valid"""
        params = {
            "url": "https://api.example.com/data",
            "timeout_seconds": 10
        }
        
        api_params = SourceAPIParams.model_validate(params)
        assert api_params.timeout_seconds == 10

    def test_json_item_extraction_fields(self):
        params = {
            "url": "https://api.example.com/data",
            "json_item_path": "jobs",
            "json_item_strict": True,
        }
        api_params = SourceAPIParams.model_validate(params)
        assert api_params.json_item_path == "jobs"
        assert api_params.json_item_strict is True


class TestNormalizeSourceParamsFrontend:
    def test_database_connection_ref_camel_case_is_normalized(self):
        out = normalize_source_params_frontend(
            {
                "source_type": "database",
                "connectionRef": "DB_CONN",
                "table_name": "items",
            }
        )
        assert out["connection_ref"] == "DB_CONN"
        assert "connectionRef" not in out

    def test_legacy_output_mode_is_dropped_in_schema_first_mode(self):
        out = normalize_source_params_frontend({"output_mode": "rows"})
        assert "output_mode" not in out

        out_nested = normalize_source_params_frontend({"output": {"mode": "rows"}})
        assert "output_mode" not in out_nested
        assert "output" not in out_nested or "mode" not in out_nested.get("output", {})

    def test_api_camel_case_fields_are_normalized(self):
        raw = {
            "url": "https://api.example.com",
            "method": "POST",
            "contentType": "application/json",
            "bodyMode": "json",
            "bodyJson": {"q": "x"},
            "bodyForm": {"a": "1"},
            "bodyRaw": "hello",
            "jsonItemPath": "$.jobs[]",
            "jsonItemStrict": True,
            "__managedHeaders": {"contentType": True},
            "retryPolicy": {"maxAttempts": 3, "backoffSeconds": 0.2, "jitterSeconds": 0.1, "retryOnStatus": [429]},
            "rateLimit": {"rpsLimit": 4},
        }
        out = normalize_source_params_frontend(raw)
        assert out["content_type"] == "application/json"
        assert out["body_mode"] == "json"
        assert out["body_json"] == {"q": "x"}
        assert out["body_form"] == {"a": "1"}
        assert out["body_raw"] == "hello"
        assert out["managed_headers"] == {"contentType": True}
        assert out["json_item_path"] == "$.jobs[]"
        assert out["json_item_strict"] is True
        assert out["retry"]["max_attempts"] == 3
        assert out["retry"]["backoff_seconds"] == 0.2
        assert out["retry"]["jitter_seconds"] == 0.1
        assert out["retry"]["retry_on_status"] == [429]
        assert out["rate_limit"]["rps"] == 4

    def test_api_legacy_body_is_migrated(self):
        out_obj = normalize_source_params_frontend({"url": "https://api.example.com", "body": {"k": "v"}})
        assert out_obj["body_mode"] == "json"
        assert out_obj["body_json"] == {"k": "v"}
        assert "body" not in out_obj

        out_str = normalize_source_params_frontend({"url": "https://api.example.com", "body": "raw text"})
        assert out_str["body_mode"] == "raw"
        assert out_str["body_raw"] == "raw text"
        assert "body" not in out_str

    def test_incremental_camel_case_fields_are_normalized(self):
        out = normalize_source_params_frontend(
            {
                "source_type": "database",
                "connection_string": "sqlite:///memory",
                "query": "select 1",
                "incrementalConfig": {
                    "enabled": True,
                    "stateKey": "state-a",
                    "cursorColumn": "id",
                    "cursorType": "int",
                    "windowStart": "1",
                    "windowEnd": "9",
                },
            }
        )
        assert out["incremental"]["state_key"] == "state-a"
        assert out["incremental"]["cursor_column"] == "id"
        assert out["incremental"]["cursor_type"] == "int"
        assert out["incremental"]["window_start"] == "1"
        assert out["incremental"]["window_end"] == "9"

    def test_priming_camel_case_fields_are_normalized(self):
        out = normalize_source_params_frontend(
            {
                "source_type": "file",
                "filename": "data.txt",
                "primingConfig": {
                    "enabled": True,
                    "mode": "priming_only",
                    "driftPolicy": "strict",
                    "sampleRows": 25,
                    "sampleBytes": 1024,
                    "timeoutMs": 400,
                },
            }
        )
        assert out["priming"]["enabled"] is True
        assert out["priming"]["mode"] == "priming_only"
        assert out["priming"]["drift_policy"] == "strict"
        assert out["priming"]["sample_rows"] == 25
        assert out["priming"]["sample_bytes"] == 1024
        assert out["priming"]["timeout_ms"] == 400

    def test_file_has_header_camel_case_is_normalized(self):
        out = normalize_source_params_frontend(
            {
                "source_type": "file",
                "filename": "data.csv",
                "file_format": "csv",
                "hasHeader": True,
            }
        )
        assert out["has_header"] is True

    def test_file_csv_dialect_and_locale_camel_case_are_normalized(self):
        out = normalize_source_params_frontend(
            {
                "source_type": "file",
                "filename": "data.csv",
                "file_format": "csv",
                "quoteChar": "\"",
                "escapeChar": "\\",
                "malformedRowPolicy": "skip",
                "decimalSeparator": ",",
                "thousandsSeparator": ".",
                "dateColumns": ["created_at"],
                "dateFormat": "%d.%m.%Y",
                "jsonMode": "document",
                "jsonStreamingEnabled": True,
                "jsonStreamChunkLines": 200,
                "jsonStreamMaxRecords": 1000,
                "jsonFlattenStrategy": "shallow",
                "jsonFlattenSeparator": "_",
                "excelImportStrategy": "union",
                "excelSheets": ["Data"],
                "excelMergedCellsPolicy": "ffill",
                "excelDatePolicy": "string",
                "excelDateFormat": "%d/%m/%Y",
                "txtRecordMode": "paragraphs",
                "txtChunkSize": 512,
                "pdfExtractionMode": "tables",
                "pdfPageMode": "sample",
                "pdfPageRange": "2-4",
                "pdfPageSample": 3,
                "imageExtractMetadata": False,
                "imageSvgPolicy": "reject",
                "imageTiffPagesMode": "first",
                "audioExtractMetadata": False,
                "audioNormalize": True,
                "audioTargetPeak": 0.75,
                "audioTranscodeFormat": "mp3",
                "videoExtractMetadata": False,
                "videoFrameMode": "keyframes",
                "videoFrameIntervalSec": 1.5,
                "videoMaxFrames": 4,
                "parquetColumns": ["id"],
                "parquetRowGroups": [1],
                "parquetMaxRows": 25,
            }
        )
        assert out["quote_char"] == "\""
        assert out["escape_char"] == "\\"
        assert out["malformed_row_policy"] == "skip"
        assert out["decimal_separator"] == ","
        assert out["thousands_separator"] == "."
        assert out["date_columns"] == ["created_at"]
        assert out["date_format"] == "%d.%m.%Y"
        assert out["json_mode"] == "document"
        assert out["json_streaming_enabled"] is True
        assert out["json_stream_chunk_lines"] == 200
        assert out["json_stream_max_records"] == 1000
        assert out["json_flatten_strategy"] == "shallow"
        assert out["json_flatten_separator"] == "_"
        assert out["excel_import_strategy"] == "union"
        assert out["excel_sheets"] == ["Data"]
        assert out["excel_merged_cells_policy"] == "ffill"
        assert out["excel_date_policy"] == "string"
        assert out["excel_date_format"] == "%d/%m/%Y"
        assert out["txt_record_mode"] == "paragraphs"
        assert out["txt_chunk_size"] == 512
        assert out["pdf_extraction_mode"] == "tables"
        assert out["pdf_page_mode"] == "sample"
        assert out["pdf_page_range"] == "2-4"
        assert out["pdf_page_sample"] == 3
        assert out["image_extract_metadata"] is False
        assert out["image_svg_policy"] == "reject"
        assert out["image_tiff_pages_mode"] == "first"
        assert out["audio_extract_metadata"] is False
        assert out["audio_normalize"] is True
        assert out["audio_target_peak"] == 0.75
        assert out["audio_transcode_format"] == "mp3"
        assert out["video_extract_metadata"] is False
        assert out["video_frame_mode"] == "keyframes"
        assert out["video_frame_interval_sec"] == 1.5
        assert out["video_max_frames"] == 4
        assert out["parquet_columns"] == ["id"]
        assert out["parquet_row_groups"] == [1]
        assert out["parquet_max_rows"] == 25


class TestFilterTransformParams:
    """Test filter transform parameter validation"""
    
    def test_success(self):
        """Test successful validation"""
        params = {
            "filter_expression": "age > 18",
            "columns": ["age"]
        }
        
        filter_params = FilterTransformParams.model_validate(params)
        assert filter_params.filter_expression == "age > 18"
        assert filter_params.columns == ["age"]
    
    def test_missing_filter_expression(self):
        """Test validation error when filter_expression is missing"""
        with pytest.raises(ValueError):
            FilterTransformParams.model_validate({})
    
    def test_optional_columns(self):
        """Test columns are optional"""
        filter_params = FilterTransformParams.model_validate({
            "filter_expression": "age > 18"
        })
        assert filter_params.columns is None


class TestTransformPythonRemoval:
    """Transform op=python is no longer supported."""

    def test_transform_params_current_rejects_python_op(self):
        with pytest.raises(ValueError):
            TransformParamsCurrent.model_validate({"op": "python"})


class TestTransformSplitValidation:
    def test_split_requires_pattern_for_regex_mode(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "duckdb", SimpleNamespace(connect=lambda **_: None))
        node = {
            "data": {
                "kind": "transform",
                "params": {
                    "op": "split",
                    "split": {
                        "sourceColumn": "text",
                        "outColumn": "part",
                        "mode": "regex",
                        "flags": "im",
                        "trim": True,
                        "dropEmpty": True,
                        "emitIndex": True,
                        "emitSourceRow": True,
                        "maxParts": 5000,
                    },
                },
            }
        }
        errors = validate_node_params(node)
        assert "split.pattern is required when mode=regex" in errors

    def test_split_rejects_invalid_flags_and_bounds(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "duckdb", SimpleNamespace(connect=lambda **_: None))
        node = {
            "data": {
                "kind": "transform",
                "params": {
                    "op": "split",
                    "split": {
                        "sourceColumn": "text",
                        "outColumn": "part",
                        "mode": "delimiter",
                        "delimiter": ",",
                        "flags": "ix",
                        "trim": True,
                        "dropEmpty": True,
                        "emitIndex": True,
                        "emitSourceRow": True,
                        "maxParts": 0,
                    },
                },
            }
        }
        errors = validate_node_params(node)
        assert "split.flags allows only i, m, s" in errors
        assert "split.maxParts must be an integer between 1 and 100000" in errors

    def test_split_rejects_invalid_line_break_mode(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "duckdb", SimpleNamespace(connect=lambda **_: None))
        node = {
            "data": {
                "kind": "transform",
                "params": {
                    "op": "split",
                    "split": {
                        "sourceColumn": "text",
                        "outColumn": "part",
                        "mode": "lines",
                        "lineBreak": "windows",
                        "flags": "",
                        "trim": True,
                        "dropEmpty": True,
                        "emitIndex": True,
                        "emitSourceRow": True,
                        "maxParts": 5000,
                    },
                },
            }
        }
        errors = validate_node_params(node)
        assert "split.lineBreak must be one of: any, lf, crlf, cr" in errors


class TestTransformDedupeValidation:
    def test_dedupe_rejects_invalid_keep(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "duckdb", SimpleNamespace(connect=lambda **_: None))
        node = {
            "data": {
                "kind": "transform",
                "params": {
                    "op": "dedupe",
                    "dedupe": {
                        "by": ["text"],
                        "keep": "last",
                    },
                },
            }
        }
        errors = validate_node_params(node)
        assert "dedupe.keep must be 'first'" in errors


class TestMapTransformParams:
    """Test map transform parameter validation"""
    
    def test_success_with_defaults(self):
        """Test successful validation with defaults"""
        params = {
            "function": "upper",
            "target_columns": ["name"]
        }
        
        map_params = MapTransformParams.model_validate(params)
        assert map_params.function == "upper"
        assert map_params.target_columns == ["name"]
    
    def test_requires_function(self):
        """Test validation error when function is missing"""
        with pytest.raises(ValueError):
            MapTransformParams.model_validate({
                "target_columns": ["name"]
            })
    
    def test_requires_target_columns(self):
        """Test validation_required flags missing target_columns"""
        params = MapTransformParams.model_validate({"function": "upper"})
        errors = params.validate_required()
        assert "target_columns is required" in errors
    
    def test_function_types(self):
        """Test different function types"""
        # builtin
        MapTransformParams.model_validate({
            "function": "upper",
            "target_columns": ["name"]
        })
        
        # lambda
        MapTransformParams.model_validate({
            "function": "lambda x: x.strip()",
            "target_columns": ["name"]
        })
        
        # custom
        MapTransformParams.model_validate({
            "function": "my_custom_function",
            "target_columns": ["name"],
            "function_type": "custom"
        })


class TestAggregateTransformParams:
    """Test aggregate transform parameter validation"""
    
    def test_success(self):
        """Test successful validation"""
        params = {
            "group_by": ["department"],
            "aggregations": {
                "salary": "sum",
                "count": "avg"
            }
        }
        
        aggregate_params = AggregateTransformParams.model_validate(params)
        assert aggregate_params.group_by == ["department"]
        assert aggregate_params.aggregations == {
            "salary": "sum",
            "count": "avg"
        }
    
    def test_requires_aggregations(self):
        """Test validation_required flags missing aggregations"""
        params = AggregateTransformParams.model_validate({})
        errors = params.validate_required()
        assert "aggregations is required" in errors
    
    def test_optional_group_by(self):
        """Test group_by is optional"""
        aggregate_params = AggregateTransformParams.model_validate({
            "aggregations": {
                "total": "sum"
            }
        })
        assert aggregate_params.group_by == []


class TestCustomTransformParams:
    """Test custom transform parameter validation"""
    
    def test_success(self):
        """Test successful validation"""
        params = {
            "code": "import pandas as pd\ndf['new_col'] = df['old_col'] * 2",
            "input_var": "df",
            "output_var": "result"
        }
        
        custom_params = CustomTransformParams.model_validate(params)
        assert custom_params.code == "import pandas as pd\ndf['new_col'] = df['old_col'] * 2"
        assert custom_params.input_var == "df"
        assert custom_params.output_var == "result"
    
    def test_requires_code(self):
        """Test validation error when code is missing"""
        with pytest.raises(ValueError):
            CustomTransformParams.model_validate({})


class TestGetSchemaForNode:
    """Test schema retrieval for nodes"""
    
    def test_llm_node_schema(self):
        """Test schema retrieval for LLM node"""
        node = {
            "data": {
                "kind": "llm",
                "params": {}
            }
        }
        
        schema = get_schema_for_node(node)
        assert schema == LLMParams

    def test_model_node_schema(self):
        node = {
            "data": {
                "kind": "model",
                "params": {}
            }
        }
        schema = get_schema_for_node(node)
        assert schema == LLMParams
    
    def test_source_file_node_schema(self):
        """Test schema retrieval for file source node"""
        node = {
            "data": {
                "kind": "source",
                "sourceKind": "file",
                "params": {}
            }
        }
        
        schema = get_schema_for_node(node)
        assert schema == SourceFileParams
    
    def test_source_database_node_schema(self):
        """Test schema retrieval for database source node"""
        node = {
            "data": {
                "kind": "source",
                "sourceKind": "database",
                "params": {}
            }
        }
        
        schema = get_schema_for_node(node)
        assert schema == SourceDatabaseParams
    
    def test_source_api_node_schema(self):
        """Test schema retrieval for API source node"""
        node = {
            "data": {
                "kind": "source",
                "sourceKind": "api",
                "params": {}
            }
        }
        
        schema = get_schema_for_node(node)
        assert schema == SourceAPIParams

    def test_source_object_store_node_schema(self):
        node = {
            "data": {
                "kind": "source",
                "sourceKind": "object_store",
                "params": {}
            }
        }
        schema = get_schema_for_node(node)
        from app.runner.schemas import SourceObjectStoreParams

        assert schema == SourceObjectStoreParams

    def test_source_warehouse_node_schema(self):
        node = {
            "data": {
                "kind": "source",
                "sourceKind": "warehouse",
                "params": {}
            }
        }
        schema = get_schema_for_node(node)
        from app.runner.schemas import SourceWarehouseParams

        assert schema == SourceWarehouseParams
    
    def test_invalid_kind(self):
        """Test None returned for invalid node kind"""
        node = {
            "data": {
                "kind": "invalid",
                "params": {}
            }
        }
        
        schema = get_schema_for_node(node)
        assert schema is None
    
    def test_transform_no_schema(self):
        """Test transform schema is returned"""
        node = {
            "data": {
                "kind": "transform",
                "params": {}
            }
        }
        
        schema = get_schema_for_node(node)
        assert schema == TransformParamsCurrent

    def test_component_node_schema(self):
        node = {
            "data": {
                "kind": "component",
                "params": {}
            }
        }
        schema = get_schema_for_node(node)
        assert schema == ComponentParams


class TestSCHEMA_REGISTRY:
    """Test schema registry"""
    
    def test_registry_has_all_schemas(self):
        """Test all expected schemas are registered"""
        assert "source:file" in SCHEMA_REGISTRY
        assert "source:database" in SCHEMA_REGISTRY
        assert "source:api" in SCHEMA_REGISTRY
        assert "source:object_store" in SCHEMA_REGISTRY
        assert "source:warehouse" in SCHEMA_REGISTRY
        assert "llm" in SCHEMA_REGISTRY
        assert "transform" in SCHEMA_REGISTRY
        assert "tool:mcp" in SCHEMA_REGISTRY
        assert "tool" in SCHEMA_REGISTRY
        assert "component" in SCHEMA_REGISTRY
    
    def test_schema_types(self):
        """Test correct schema types in registry"""
        assert SCHEMA_REGISTRY["source:file"] == SourceFileParams
        assert SCHEMA_REGISTRY["llm"] == LLMParams
        assert SCHEMA_REGISTRY["tool:mcp"] is not None  # Tool params type
        assert SCHEMA_REGISTRY["component"] == ComponentParams


class TestComponentValidation:
    def test_component_requires_revision_id(self):
        node = {
            "data": {
                "kind": "component",
                "params": {
                    "componentRef": {"componentId": "cmp_inventory", "revisionId": ""}
                },
            }
        }
        errors = validate_node_params(node)
        assert any("MISSING_REVISION_ID" in err for err in errors)


class TestValidateNodeParamsSourceNormalization:
    def test_database_query_string_not_coerced_to_query_object(self):
        node = {
            "data": {
                "kind": "source",
                "sourceKind": "database",
                "params": {
                    "connection_ref": "conn:default",
                    "query": "select 1",
                },
            }
        }
        errors = validate_node_params(node)
        assert errors == []

    def test_partition_on_error_camel_case_normalizes(self):
        from app.runner.schemas import normalize_source_params_frontend

        normalized = normalize_source_params_frontend(
            {
                "source_type": "api",
                "url": "https://example.com",
                "method": "GET",
                "partitionConfig": {"enabled": True, "kind": "static_list", "onError": "skip_failed"},
            }
        )
        assert isinstance(normalized.get("partition"), dict)
        assert normalized.get("partition", {}).get("on_error") == "skip_failed"


class TestValidateNodeParamsModelKind:
    def test_model_kind_validation_rejects_unknown(self):
        node = {
            "data": {
                "kind": "model",
                "modelKind": "unknown_mode",
                "llmKind": "openai_compat",
                "params": {
                    "connectionRef": "OPENAI_API_KEY",
                    "model": "gpt-4o-mini",
                    "user_prompt": "hello",
                    "output": {"mode": "text"},
                },
            }
        }
        errors = validate_node_params(node)
        assert any("modelKind must be one of" in err for err in errors)

    def test_task_kind_validation_rejects_invalid_combo(self):
        node = {
            "data": {
                "kind": "model",
                "modelKind": "embedding",
                "taskKind": "generate",
                "llmKind": "openai_compat",
                "params": {
                    "connectionRef": "OPENAI_API_KEY",
                    "model": "text-embedding-3-small",
                    "user_prompt": "embed this",
                    "output": {"mode": "embeddings", "embedding": {"dims": 1536}},
                },
            }
        }
        errors = validate_node_params(node)
        assert any("not valid for modelKind" in err for err in errors)
