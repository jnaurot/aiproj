#C:\Users\Owner\Desktop\aiproj\backend\app\runner\schemas.py
# from platform import node
import json
from typing import Any, Dict, List, Literal, Optional, Union
from pydantic import BaseModel, Field, model_validator, validator
from enum import Enum


def normalize_llm_params_frontend(raw: Dict[str, Any]) -> Dict[str, Any]:
    p = dict(raw or {})

    # camelCase -> snake_case (what LLMParams expects)
    if "baseUrl" in p and "base_url" not in p:
        p["base_url"] = p.pop("baseUrl")

    if "connectionRef" in p and "connection_ref" not in p:
        p["connection_ref"] = p.pop("connectionRef")

    if "apiKeyRef" in p and "api_key_ref" not in p:
        p["api_key_ref"] = p.pop("apiKeyRef")
    if "promptRevisionId" in p and "prompt_revision_id" not in p:
        p["prompt_revision_id"] = p.pop("promptRevisionId")
    if "evalGate" in p and "eval_gate" not in p:
        p["eval_gate"] = p.pop("evalGate")

    if "system_prompt" in p and "system_prompt" not in p:
        # no-op; included just to show: FE already uses system_prompt
        pass

    if "user_prompt" in p and "user_prompt" not in p:
        pass

    # frontend output object -> backend output_schema/strict/embedding_contract
    out = p.get("output")
    if isinstance(out, dict):
        if "mode" in out:
            # Canonical schema-first mode; override stale legacy output_mode if present.
            p["output_mode"] = out.get("mode")
        if "jsonSchema" in out and "output_schema" not in p:
            p["output_schema"] = out.get("jsonSchema")
        if "strict" in out and "output_strict" not in p:
            p["output_strict"] = out.get("strict")
        if "validationMode" in out and "output_validation_mode" not in p:
            p["output_validation_mode"] = out.get("validationMode")
        if "embedding" in out and "embedding_contract" not in p:
            p["embedding_contract"] = out.get("embedding")

    # frontend may send stopSequences, inputMapping (if you add later)
    if "stopSequences" in p and "stop_sequences" not in p:
        p["stop_sequences"] = p.pop("stopSequences")
    if "stop" in p and "stop_sequences" not in p:
        p["stop_sequences"] = p.pop("stop")

    if "inputMapping" in p and "input_mapping" not in p:
        p["input_mapping"] = p.pop("inputMapping")
    if "inputEncoding" in p and "input_encoding" not in p:
        p["input_encoding"] = p.pop("inputEncoding")
    if "inputEnvelope" in p and "input_envelope" not in p:
        p["input_envelope"] = p.pop("inputEnvelope")
    if "requestPolicy" in p and "request_policy" not in p:
        p["request_policy"] = p.pop("requestPolicy")
    if "presencePenalty" in p and "presence_penalty" not in p:
        p["presence_penalty"] = p.pop("presencePenalty")
    if "frequencyPenalty" in p and "frequency_penalty" not in p:
        p["frequency_penalty"] = p.pop("frequencyPenalty")
    if "repeatPenalty" in p and "repeat_penalty" not in p:
        p["repeat_penalty"] = p.pop("repeatPenalty")
    if isinstance(p.get("thinking"), str):
        legacy = str(p.get("thinking"))
        mapping = {
            "off": {"enabled": False, "mode": "none"},
            "auto": {"enabled": True, "mode": "hidden"},
            "on": {"enabled": True, "mode": "visible"},
        }
        p["thinking"] = mapping.get(legacy, {"enabled": False, "mode": "none"})

    return p


def normalize_source_params_frontend(raw: Dict[str, Any]) -> Dict[str, Any]:
    p = dict(raw or {})
    if "connectionRef" in p and "connection_ref" not in p:
        p["connection_ref"] = p.pop("connectionRef")
    p.pop("sample_size", None)
    p.pop("sampleSize", None)
    if "snapshotId" in p and "snapshot_id" not in p:
        p["snapshot_id"] = p.pop("snapshotId")
    if "recentSnapshotIds" in p and "recent_snapshot_ids" not in p:
        p["recent_snapshot_ids"] = p.pop("recentSnapshotIds")
    if "snapshotMetadata" in p and "snapshot_metadata" not in p:
        p["snapshot_metadata"] = p.pop("snapshotMetadata")
    if "hasHeader" in p and "has_header" not in p:
        p["has_header"] = p.pop("hasHeader")
    if "quoteChar" in p and "quote_char" not in p:
        p["quote_char"] = p.pop("quoteChar")
    if "escapeChar" in p and "escape_char" not in p:
        p["escape_char"] = p.pop("escapeChar")
    if "malformedRowPolicy" in p and "malformed_row_policy" not in p:
        p["malformed_row_policy"] = p.pop("malformedRowPolicy")
    if "decimalSeparator" in p and "decimal_separator" not in p:
        p["decimal_separator"] = p.pop("decimalSeparator")
    if "thousandsSeparator" in p and "thousands_separator" not in p:
        p["thousands_separator"] = p.pop("thousandsSeparator")
    if "dateColumns" in p and "date_columns" not in p:
        p["date_columns"] = p.pop("dateColumns")
    if "dateFormat" in p and "date_format" not in p:
        p["date_format"] = p.pop("dateFormat")
    if "jsonMode" in p and "json_mode" not in p:
        p["json_mode"] = p.pop("jsonMode")
    if "jsonStreamingEnabled" in p and "json_streaming_enabled" not in p:
        p["json_streaming_enabled"] = p.pop("jsonStreamingEnabled")
    if "jsonStreamChunkLines" in p and "json_stream_chunk_lines" not in p:
        p["json_stream_chunk_lines"] = p.pop("jsonStreamChunkLines")
    if "jsonStreamMaxRecords" in p and "json_stream_max_records" not in p:
        p["json_stream_max_records"] = p.pop("jsonStreamMaxRecords")
    if "parquetColumns" in p and "parquet_columns" not in p:
        p["parquet_columns"] = p.pop("parquetColumns")
    if "parquetRowGroups" in p and "parquet_row_groups" not in p:
        p["parquet_row_groups"] = p.pop("parquetRowGroups")
    if "parquetMaxRows" in p and "parquet_max_rows" not in p:
        p["parquet_max_rows"] = p.pop("parquetMaxRows")
    if "rootId" in p and "rel_path" not in p:
        p["rel_path"] = p.pop("rootId")
    if "relPath" in p and "filename" not in p:
        p["filename"] = p.pop("relPath")
    if "root_id" in p and "rel_path" not in p:
        p["rel_path"] = p.pop("root_id")
    if "file_path" in p and ("rel_path" not in p or "filename" not in p):
        try:
            from pathlib import Path as _P

            _fp = _P(str(p.get("file_path")))
            p.setdefault("rel_path", str(_fp.parent) if str(_fp.parent) not in {"", "."} else ".")
            p.setdefault("filename", _fp.name or str(_fp))
        except Exception:
            p.setdefault("rel_path", ".")
            p.setdefault("filename", str(p.get("file_path")))
    out = p.get("output")
    if isinstance(out, dict):
        if "schema" in out and "output_schema" not in p:
            p["output_schema"] = out.get("schema")
        # Schema-first: ignore legacy output mode controls.
        out.pop("mode", None)
        if not out:
            p.pop("output", None)
        else:
            p["output"] = out
    # Schema-first: ignore legacy output mode controls.
    p.pop("output_mode", None)
    # Legacy alias support: older revisions used "rows" for table output.
    if isinstance(p.get("output"), dict):
        output_obj = p.get("output")
        output_mode = output_obj.get("mode") if isinstance(output_obj, dict) else None
        if str(output_mode or "").strip().lower() == "rows":
            output_obj.pop("mode", None)
    if "contentType" in p and "content_type" not in p:
        p["content_type"] = p.pop("contentType")
    if "bodyMode" in p and "body_mode" not in p:
        p["body_mode"] = p.pop("bodyMode")
    if "bodyJson" in p and "body_json" not in p:
        p["body_json"] = p.pop("bodyJson")
    if "bodyForm" in p and "body_form" not in p:
        p["body_form"] = p.pop("bodyForm")
    if "bodyRaw" in p and "body_raw" not in p:
        p["body_raw"] = p.pop("bodyRaw")
    if "incrementalConfig" in p and "incremental" not in p:
        p["incremental"] = p.pop("incrementalConfig")
    if "partitionConfig" in p and "partition" not in p:
        p["partition"] = p.pop("partitionConfig")
    if "primingConfig" in p and "priming" not in p:
        p["priming"] = p.pop("primingConfig")
    if "retryPolicy" in p and "retry" not in p:
        p["retry"] = p.pop("retryPolicy")
    if "rateLimit" in p and "rate_limit" not in p:
        p["rate_limit"] = p.pop("rateLimit")
    if "__managedHeaders" in p and "managed_headers" not in p:
        p["managed_headers"] = p.pop("__managedHeaders")
    if isinstance(p.get("body"), dict):
        p.setdefault("body_mode", "json")
        p.setdefault("body_json", p.get("body"))
    elif isinstance(p.get("body"), str):
        p.setdefault("body_mode", "raw")
        p.setdefault("body_raw", p.get("body"))
    p.pop("body", None)
    source_type = str(
        p.get("source_type")
        or p.get("sourceType")
        or p.get("kind")
        or ""
    ).strip().lower()
    # API source uses a query-map; database source uses SQL string query.
    if source_type == "api":
        if "query" in p and not isinstance(p.get("query"), dict):
            p["query"] = {}
    elif source_type in {"database", "warehouse"}:
        if "query" in p and not isinstance(p.get("query"), str):
            p["query"] = None
    elif source_type == "object_store":
        pass
    else:
        # Unknown/legacy shape: preserve historical API-safe behavior.
        if "query" in p and not isinstance(p.get("query"), dict):
            p["query"] = {}
    cache_policy = p.get("cache_policy")
    if isinstance(cache_policy, dict) and "ttlSeconds" in cache_policy and "ttl_seconds" not in cache_policy:
        cache_policy["ttl_seconds"] = cache_policy.pop("ttlSeconds")
    retry = p.get("retry")
    if isinstance(retry, dict):
        if "maxAttempts" in retry and "max_attempts" not in retry:
            retry["max_attempts"] = retry.pop("maxAttempts")
        if "backoffSeconds" in retry and "backoff_seconds" not in retry:
            retry["backoff_seconds"] = retry.pop("backoffSeconds")
        if "jitterSeconds" in retry and "jitter_seconds" not in retry:
            retry["jitter_seconds"] = retry.pop("jitterSeconds")
        if "retryOnStatus" in retry and "retry_on_status" not in retry:
            retry["retry_on_status"] = retry.pop("retryOnStatus")
    rate_limit = p.get("rate_limit")
    if isinstance(rate_limit, dict):
        if "rpsLimit" in rate_limit and "rps" not in rate_limit:
            rate_limit["rps"] = rate_limit.pop("rpsLimit")
    incremental = p.get("incremental")
    if isinstance(incremental, dict):
        if "stateKey" in incremental and "state_key" not in incremental:
            incremental["state_key"] = incremental.pop("stateKey")
        if "cursorColumn" in incremental and "cursor_column" not in incremental:
            incremental["cursor_column"] = incremental.pop("cursorColumn")
        if "cursorType" in incremental and "cursor_type" not in incremental:
            incremental["cursor_type"] = incremental.pop("cursorType")
        if "windowStart" in incremental and "window_start" not in incremental:
            incremental["window_start"] = incremental.pop("windowStart")
        if "windowEnd" in incremental and "window_end" not in incremental:
            incremental["window_end"] = incremental.pop("windowEnd")
    partition = p.get("partition")
    if isinstance(partition, dict):
        if "onError" in partition and "on_error" not in partition:
            partition["on_error"] = partition.pop("onError")
        if "staticValues" in partition and "static_values" not in partition:
            partition["static_values"] = partition.pop("staticValues")
        if "numericStart" in partition and "numeric_start" not in partition:
            partition["numeric_start"] = partition.pop("numericStart")
        if "numericEnd" in partition and "numeric_end" not in partition:
            partition["numeric_end"] = partition.pop("numericEnd")
        if "numericStep" in partition and "numeric_step" not in partition:
            partition["numeric_step"] = partition.pop("numericStep")
        if "dateStart" in partition and "date_start" not in partition:
            partition["date_start"] = partition.pop("dateStart")
        if "dateEnd" in partition and "date_end" not in partition:
            partition["date_end"] = partition.pop("dateEnd")
        if "dateEveryDays" in partition and "date_every_days" not in partition:
            partition["date_every_days"] = partition.pop("dateEveryDays")
        if "bindKey" in partition and "bind_key" not in partition:
            partition["bind_key"] = partition.pop("bindKey")
        if "parallelismCap" in partition and "parallelism_cap" not in partition:
            partition["parallelism_cap"] = partition.pop("parallelismCap")
    priming = p.get("priming")
    if isinstance(priming, dict):
        if "driftPolicy" in priming and "drift_policy" not in priming:
            priming["drift_policy"] = priming.pop("driftPolicy")
        if "sampleRows" in priming and "sample_rows" not in priming:
            priming["sample_rows"] = priming.pop("sampleRows")
        if "sampleBytes" in priming and "sample_bytes" not in priming:
            priming["sample_bytes"] = priming.pop("sampleBytes")
        if "timeoutMs" in priming and "timeout_ms" not in priming:
            priming["timeout_ms"] = priming.pop("timeoutMs")
    return p


def normalize_tool_params_frontend(raw: Dict[str, Any]) -> Dict[str, Any]:
    p = dict(raw or {})
    # canonical provider taxonomy
    provider = p.get("provider")
    legacy_tool_type = p.get("tool_type")
    if not provider and isinstance(legacy_tool_type, str):
        legacy_map = {
            "api": "http",
            "script": "shell",
            "builtin": "builtin",
            "python": "python",
            "mcp": "mcp",
        }
        p["provider"] = legacy_map.get(legacy_tool_type, legacy_tool_type)

    # common FE/BE key normalization
    if "connectionRef" in p and "connection_ref" not in p:
        p["connection_ref"] = p["connectionRef"]
    if "timeoutMs" in p and "timeout_ms" not in p:
        p["timeout_ms"] = p["timeoutMs"]
    return p


# ============================================================================
# BASE SCHEMA SYSTEM
# ============================================================================

class NodeParamSchema(BaseModel):
    """Base class for all node parameter schemas"""
    
    class Config:
        extra = "allow"  # Allow unknown fields for forward compatibility
        
    def validate_required(self) -> List[str]:
        """Override to implement custom validation logic"""
        return []

# ============================================================================
# SOURCE NODE SCHEMAS
# ============================================================================

class SourceKind(str, Enum):
    FILE = "file"
    DATABASE = "database"
    API = "api"
    OBJECT_STORE = "object_store"
    WAREHOUSE = "warehouse"


class SourcePrimingParams(NodeParamSchema):
    enabled: bool = False
    mode: Literal["advisory", "priming_only"] = "advisory"
    drift_policy: Literal["soft", "strict"] = "soft"
    sample_rows: int = Field(default=50, ge=1, le=10000)
    sample_bytes: int = Field(default=65536, ge=1, le=100_000_000)
    timeout_ms: int = Field(default=1500, ge=1, le=300000)

class SourceFileParams(NodeParamSchema):
    snapshot_id: Optional[str] = None
    rel_path: Optional[str] = Field(None, description="Directory path")
    filename: Optional[str] = Field(None, description="File name/path under rel_path")
    file_path: Optional[str] = None  # compatibility shim (legacy FE)
    recent_snapshot_ids: List[str] = Field(default_factory=list)
    snapshot_metadata: Optional[Dict[str, Any]] = None
    file_format: Literal[
        "csv",
        "tsv",
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
    ] = "csv"
    delimiter: Optional[str] = None  # for CSV
    has_header: Optional[bool] = None
    quote_char: Optional[str] = None
    escape_char: Optional[str] = None
    malformed_row_policy: Literal["fail", "skip", "warn"] = "fail"
    decimal_separator: Literal[".", ","] = "."
    thousands_separator: Optional[str] = None
    date_columns: List[str] = Field(default_factory=list)
    date_format: Optional[str] = None
    json_mode: Literal["document", "ndjson", "auto"] = "auto"
    json_streaming_enabled: bool = False
    json_stream_chunk_lines: int = Field(default=1000, ge=1)
    json_stream_max_records: Optional[int] = Field(default=None, ge=1)
    parquet_columns: List[str] = Field(default_factory=list)
    parquet_row_groups: List[int] = Field(default_factory=list)
    parquet_max_rows: Optional[int] = Field(default=None, ge=1)
    sheet_name: Optional[str] = None  # for Excel
    encoding: str = "utf-8"
    cache_enabled: bool = True
    priming: SourcePrimingParams = Field(default_factory=SourcePrimingParams)
    output_mode: Optional[Literal["table", "text", "json", "binary"]] = None
    output_schema: Optional[Dict[str, Any]] = None

    @model_validator(mode="after")
    def _derive_output_mode(self):
        if self.output_mode is not None:
            return self
        if self.file_format in {"csv", "tsv", "parquet", "excel"}:
            self.output_mode = "table"
        elif self.file_format == "json":
            self.output_mode = "json"
        elif self.file_format in {"txt", "pdf"}:
            self.output_mode = "text"
        elif self.file_format in {"jpg", "jpeg", "png", "webp", "gif", "svg", "tif", "tiff"}:
            self.output_mode = "binary"
        elif self.file_format in {"mp3", "wav", "flac", "ogg", "m4a", "aac"}:
            self.output_mode = "binary"
        elif self.file_format in {"mp4", "mov", "webm"}:
            self.output_mode = "binary"
        else:
            self.output_mode = "binary"
        return self
    
    def validate_required(self) -> List[str]:
        errors = []
        if not self.snapshot_id:
            if not self.rel_path:
                errors.append("rel_path is required")
            if not self.filename:
                errors.append("filename is required")
        if self.file_format == "csv" and self.delimiter is None:
            # Auto-detect or use default
            pass
        if self.quote_char is not None and len(str(self.quote_char)) != 1:
            errors.append("quote_char must be a single character")
        if self.escape_char is not None and len(str(self.escape_char)) != 1:
            errors.append("escape_char must be a single character")
        if self.thousands_separator is not None and len(str(self.thousands_separator)) > 1:
            errors.append("thousands_separator must be a single character")
        return errors

class SourceDatabaseParams(NodeParamSchema):
    #source_type: Literal[SourceKind.DATABASE] = SourceKind.DATABASE
    connection_string: Optional[str] = None
    connection_ref: Optional[str] = None  # reference to stored connection
    query: Optional[str] = None
    table_name: Optional[str] = None
    limit: Optional[int] = None
    incremental: Dict[str, Any] = Field(default_factory=lambda: {"enabled": False, "cursor_type": "auto"})
    partition: Dict[str, Any] = Field(
        default_factory=lambda: {"enabled": False, "kind": "static_list", "on_error": "fail_fast", "bind_key": "partition", "parallelism_cap": 2}
    )
    priming: SourcePrimingParams = Field(default_factory=SourcePrimingParams)
    output_mode: Literal["table", "text", "json", "binary"] = "table"
    output_schema: Optional[Dict[str, Any]] = None
    
    def validate_required(self) -> List[str]:
        errors = []
        if not self.connection_string and not self.connection_ref:
            errors.append("Either connection_string or connection_ref required")
        if not self.query and not self.table_name:
            errors.append("Either query or table_name required")
        return errors

class SourceAPIParams(NodeParamSchema):
    #source_type: Literal[SourceKind.API] = SourceKind.API
    url: str = Field(..., description="API endpoint URL")
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"] = "GET"
    headers: Dict[str, str] = Field(default_factory=dict)
    query: Dict[str, str] = Field(default_factory=dict)
    content_type: Optional[
        Literal[
            "application/json",
            "application/x-www-form-urlencoded",
            "multipart/form-data",
            "text/plain",
            "application/xml",
        ]
    ] = None
    body_mode: Literal["none", "json", "form", "raw", "multipart"] = "none"
    body_json: Optional[Dict[str, Any]] = None
    body_form: Optional[Dict[str, str]] = None
    body_raw: Optional[str] = None
    managed_headers: Optional[Dict[str, Any]] = None
    auth_type: Literal["none", "bearer", "basic", "api_key"] = "none"
    auth_token_ref: Optional[str] = None
    timeout_seconds: int = 30
    incremental: Dict[str, Any] = Field(default_factory=lambda: {"enabled": False, "cursor_type": "auto"})
    partition: Dict[str, Any] = Field(
        default_factory=lambda: {"enabled": False, "kind": "static_list", "on_error": "fail_fast", "bind_key": "partition", "parallelism_cap": 2}
    )
    retry: Dict[str, Any] = Field(
        default_factory=lambda: {
            "max_attempts": 1,
            "backoff_seconds": 0.25,
            "jitter_seconds": 0.05,
            "retry_on_status": [429, 500, 502, 503, 504],
        }
    )
    rate_limit: Dict[str, Any] = Field(default_factory=lambda: {"burst": 1})
    cache_policy: Dict[str, Any] = Field(default_factory=lambda: {"mode": "default"})
    priming: SourcePrimingParams = Field(default_factory=SourcePrimingParams)
    output_mode: Literal["table", "text", "json", "binary"] = "json"
    output_schema: Optional[Dict[str, Any]] = None

    @model_validator(mode="after")
    def _normalize_body_mode(self):
        if self.body_mode == "json":
            self.body_form = None
            self.body_raw = None
            if self.content_type is None:
                self.content_type = "application/json"
        elif self.body_mode in {"form", "multipart"}:
            self.body_json = None
            self.body_raw = None
            if self.body_form is None:
                self.body_form = {}
            if self.content_type is None:
                self.content_type = (
                    "application/x-www-form-urlencoded"
                    if self.body_mode == "form"
                    else "multipart/form-data"
                )
        elif self.body_mode == "raw":
            self.body_json = None
            self.body_form = None
            if self.body_raw is None:
                self.body_raw = ""
        else:
            self.body_json = None
            self.body_form = None
            self.body_raw = None
        return self
    
    def validate_required(self) -> List[str]:
        errors = []
        if not self.url:
            errors.append("url is required")
        if self.auth_type != "none" and not self.auth_token_ref:
            errors.append("auth_token_ref required when using authentication")
        return errors


class SourceObjectStoreParams(NodeParamSchema):
    provider: Literal["s3", "azure_blob", "gcs"] = "s3"
    connection_ref: Optional[str] = None
    bucket: Optional[str] = None
    key: Optional[str] = None
    file_format: Literal[
        "csv",
        "tsv",
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
    ] = "txt"
    encoding: str = "utf-8"
    priming: SourcePrimingParams = Field(default_factory=SourcePrimingParams)
    output_mode: Optional[Literal["table", "text", "json", "binary"]] = None
    output_schema: Optional[Dict[str, Any]] = None

    @model_validator(mode="after")
    def _derive_output_mode(self):
        if self.output_mode is not None:
            return self
        if self.file_format in {"csv", "tsv", "parquet", "excel"}:
            self.output_mode = "table"
        elif self.file_format == "json":
            self.output_mode = "json"
        elif self.file_format in {"txt", "pdf"}:
            self.output_mode = "text"
        else:
            self.output_mode = "binary"
        return self

    def validate_required(self) -> List[str]:
        errors = []
        if not self.bucket:
            errors.append("bucket is required")
        if not self.key:
            errors.append("key is required")
        return errors


class SourceWarehouseParams(NodeParamSchema):
    provider: Literal["snowflake", "bigquery", "databricks_sql"] = "snowflake"
    connection_string: Optional[str] = None
    connection_ref: Optional[str] = None
    query: Optional[str] = None
    limit: Optional[int] = None
    priming: SourcePrimingParams = Field(default_factory=SourcePrimingParams)
    output_mode: Literal["table", "text", "json", "binary"] = "table"
    output_schema: Optional[Dict[str, Any]] = None

    def validate_required(self) -> List[str]:
        errors = []
        if not self.connection_string and not self.connection_ref:
            errors.append("Either connection_string or connection_ref required")
        if not self.query:
            errors.append("query is required")
        return errors

# Union type for all source params
SourceParams = Union[
    SourceFileParams,
    SourceDatabaseParams,
    SourceAPIParams,
    SourceObjectStoreParams,
    SourceWarehouseParams,
]

# ============================================================================
# TRANSFORM NODE SCHEMAS
# ============================================================================

class TransformType(str, Enum):
    FILTER = "filter"
    MAP = "map"
    AGGREGATE = "aggregate"
    JOIN = "join"
    PIVOT = "pivot"
    CLEAN = "clean"
    CUSTOM = "custom"

class FilterTransformParams(NodeParamSchema):
    transform_type: Literal[TransformType.FILTER] = TransformType.FILTER
    filter_expression: str = Field(..., description="Python/SQL-like filter expression")
    columns: Optional[List[str]] = None  # filter specific columns
    
    def validate_required(self) -> List[str]:
        if not self.filter_expression:
            return ["filter_expression is required"]
        return []

class MapTransformParams(NodeParamSchema):
    transform_type: Literal[TransformType.MAP] = TransformType.MAP
    function: str = Field(..., description="Function to apply")
    target_columns: List[str] = Field(default_factory=list)
    new_column_name: Optional[str] = None
    function_type: Literal["builtin", "lambda", "custom"] = "builtin"
    
    def validate_required(self) -> List[str]:
        errors = []
        if not self.function:
            errors.append("function is required")
        if not self.target_columns:
            errors.append("target_columns is required")
        return errors

class AggregateTransformParams(NodeParamSchema):
    transform_type: Literal[TransformType.AGGREGATE] = TransformType.AGGREGATE
    group_by: List[str] = Field(default_factory=list)
    aggregations: Dict[str, str] = Field(
        default_factory=dict,
        description="Map of column to aggregation function"
    )
    
    def validate_required(self) -> List[str]:
        if not self.aggregations:
            return ["aggregations is required"]
        return []

class CleanTransformParams(NodeParamSchema):
    transform_type: Literal[TransformType.CLEAN] = TransformType.CLEAN
    drop_na: bool = False
    fill_na: Optional[Any] = None
    drop_duplicates: bool = False
    duplicate_subset: Optional[List[str]] = None
    strip_whitespace: bool = True
    
    def validate_required(self) -> List[str]:
        return []  # All optional

class CustomTransformParams(NodeParamSchema):
    transform_type: Literal[TransformType.CUSTOM] = TransformType.CUSTOM
    code: str = Field(..., description="Python code to execute")
    input_var: str = "df"  # variable name for input data
    output_var: str = "result"  # variable name for output
    
    def validate_required(self) -> List[str]:
        if not self.code:
            return ["code is required"]
        return []

TransformParams = Union[
    FilterTransformParams,
    MapTransformParams,
    AggregateTransformParams,
    CleanTransformParams,
    CustomTransformParams
]


class TransformParamsCurrent(NodeParamSchema):
    op: Literal[
        "filter",
        "select",
        "rename",
        "derive",
        "aggregate",
        "join",
        "sort",
        "limit",
        "dedupe",
        "null_policy",
        "outlier_policy",
        "text_clean",
        "nlp_normalize",
        "tokenize_chunk",
        "dataset_split",
        "class_imbalance",
        "categorical_encode",
        "numeric_scale",
        "embedding",
        "feature_selection",
        "leakage_detect",
        "quality_profile",
        "drift_compare",
        "determinism_profile",
        "fit_state_registry",
        "pii_guard",
        "inference_parity",
        "split",
        "quality_gate",
        "ml_contract",
        "sql",
        "json_to_table",
        "text_to_table",
        "table_to_json",
    ]
    enabled: bool = True
    notes: str = ""
    cache: Optional[Dict[str, Any]] = None
    filter: Optional[Dict[str, Any]] = None
    select: Optional[Dict[str, Any]] = None
    rename: Optional[Dict[str, Any]] = None
    derive: Optional[Dict[str, Any]] = None
    aggregate: Optional[Dict[str, Any]] = None
    join: Optional[Dict[str, Any]] = None
    sort: Optional[Dict[str, Any]] = None
    limit: Optional[Dict[str, Any]] = None
    dedupe: Optional[Dict[str, Any]] = None
    null_policy: Optional[Dict[str, Any]] = None
    outlier_policy: Optional[Dict[str, Any]] = None
    text_clean: Optional[Dict[str, Any]] = None
    nlp_normalize: Optional[Dict[str, Any]] = None
    tokenize_chunk: Optional[Dict[str, Any]] = None
    dataset_split: Optional[Dict[str, Any]] = None
    class_imbalance: Optional[Dict[str, Any]] = None
    categorical_encode: Optional[Dict[str, Any]] = None
    numeric_scale: Optional[Dict[str, Any]] = None
    embedding: Optional[Dict[str, Any]] = None
    feature_selection: Optional[Dict[str, Any]] = None
    leakage_detect: Optional[Dict[str, Any]] = None
    quality_profile: Optional[Dict[str, Any]] = None
    drift_compare: Optional[Dict[str, Any]] = None
    determinism_profile: Optional[Dict[str, Any]] = None
    fit_state_registry: Optional[Dict[str, Any]] = None
    pii_guard: Optional[Dict[str, Any]] = None
    inference_parity: Optional[Dict[str, Any]] = None
    split: Optional[Dict[str, Any]] = None
    quality_gate: Optional[Dict[str, Any]] = None
    ml_contract: Optional[Dict[str, Any]] = None
    sql: Optional[Dict[str, Any]] = None
    json_to_table: Optional[Dict[str, Any]] = None
    text_to_table: Optional[Dict[str, Any]] = None
    table_to_json: Optional[Dict[str, Any]] = None

    def validate_required(self) -> List[str]:
        op_to_payload = {
            "filter": "filter",
            "select": "select",
            "rename": "rename",
            "derive": "derive",
            "aggregate": "aggregate",
            "join": "join",
            "sort": "sort",
            "limit": "limit",
            "dedupe": "dedupe",
            "null_policy": "null_policy",
            "outlier_policy": "outlier_policy",
            "text_clean": "text_clean",
            "nlp_normalize": "nlp_normalize",
            "tokenize_chunk": "tokenize_chunk",
            "dataset_split": "dataset_split",
            "class_imbalance": "class_imbalance",
            "categorical_encode": "categorical_encode",
            "numeric_scale": "numeric_scale",
            "embedding": "embedding",
            "feature_selection": "feature_selection",
            "leakage_detect": "leakage_detect",
            "quality_profile": "quality_profile",
            "drift_compare": "drift_compare",
            "determinism_profile": "determinism_profile",
            "fit_state_registry": "fit_state_registry",
            "pii_guard": "pii_guard",
            "inference_parity": "inference_parity",
            "split": "split",
            "quality_gate": "quality_gate",
            "ml_contract": "ml_contract",
            "sql": "sql",
            "json_to_table": "json_to_table",
            "text_to_table": "text_to_table",
            "table_to_json": "table_to_json",
        }
        payload_key = op_to_payload.get(self.op)
        payload = getattr(self, payload_key, None) if payload_key else None
        if not isinstance(payload, dict):
            return [f"{payload_key} block is required for op='{self.op}'"]
        return []

# ============================================================================
# LLM NODE SCHEMAS
# ============================================================================
# from .schemas import LLMParams  # ❌ don't import itself
class LLMType(str, Enum):
    COMPLETION = "completion"
    EMBEDDINGS = "embeddings"
    CLASSIFICATION = "classification"

class LLMProvider(str, Enum):
    OPENAI = "openai_compat"
    OLLAMA = "ollama"
    
class LLMDialect(str, Enum):
    OPENAI_COMPAT = "openai_compat"
    OLLAMA = "ollama"


class LLMThinking(NodeParamSchema):
    enabled: bool = False
    mode: Literal["none", "hidden", "visible"] = "none"
    budget_tokens: Optional[int] = Field(None, ge=1)

class LLMParams(NodeParamSchema):
    # llm_type: LLMType = LLMType.COMPLETION
    
    base_url: Optional[str] = None
    connection_ref: Optional[str] = None  # later
    api_key_ref: Optional[str] = None     # only for openai_compat when needed
    
    model: str = Field(..., description="Model identifier")
    
    # Prompting
    system_prompt: Optional[str] = None
    user_prompt: str = Field(..., description="User prompt template")
    prompt_revision_id: Optional[str] = None
    eval_gate: Optional[Dict[str, Any]] = None
    
    # Generation params
    temperature: float = Field(1.0, ge=0.0, le=2.0)
    max_tokens: int = Field(1024, ge=1, le=100000)
    top_p: Optional[float] = Field(None, ge=0.0, le=1.0)
    seed: Optional[int] = None
    stop_sequences: List[str] = Field(default_factory=list)
    presence_penalty: Optional[float] = Field(None, ge=-2.0, le=2.0)
    frequency_penalty: Optional[float] = Field(None, ge=-2.0, le=2.0)
    repeat_penalty: Optional[float] = Field(None, ge=0.5, le=2.0)
    thinking: Optional[LLMThinking] = None
    input_encoding: Optional[Literal["text", "json_canonical", "table_canonical"]] = None
    
    # output
    output_mode: Literal["text", "json", "embeddings"] = "text"
    output_schema: Optional[Dict[str, Any]] = None
    output_strict: bool = True
    output_validation_mode: Literal["strict", "soft"] = "strict"
    embedding_contract: Optional[Dict[str, Any]] = None
    
    # Error handling
    retry_on_error: bool = True
    max_retries: int = Field(3, ge=0, le=10)
    timeout_seconds: int = Field(60, ge=1)
    
    input_mapping: Optional[Dict[str, str]] = None  # variables -> input keys/handles
    input_envelope: Optional[List[Dict[str, Any]]] = None
    request_policy: Optional[Dict[str, Any]] = None

    @model_validator(mode="after")
    def _validate_contract(self):
        if not self.base_url and not self.connection_ref:
            raise ValueError("Either base_url or connection_ref is required")
        if self.output_mode == "json" and not self.output_schema:
            raise ValueError("output_schema required when output_mode='json'")
        if self.output_mode == "embeddings":
            contract = self.embedding_contract
            if not isinstance(contract, dict):
                raise ValueError("embedding_contract required when output_mode='embeddings'")
            dims = contract.get("dims")
            if not isinstance(dims, int) or dims <= 0:
                raise ValueError("embedding_contract.dims must be a positive integer")
            dtype = contract.get("dtype")
            layout = contract.get("layout")
            if dtype is None:
                contract["dtype"] = "float32"
            elif dtype not in {"float32", "float16", "float64"}:
                raise ValueError("embedding_contract.dtype must be one of: float32, float16, float64")
            if layout is None:
                contract["layout"] = "1d"
            elif layout not in {"1d", "2d"}:
                raise ValueError("embedding_contract.layout must be one of: 1d, 2d")
        if self.input_envelope is not None:
            if not isinstance(self.input_envelope, list):
                raise ValueError("input_envelope must be an array")
            for i, part in enumerate(self.input_envelope):
                if not isinstance(part, dict):
                    raise ValueError(f"input_envelope[{i}] must be an object")
                part_type = str(part.get("type") or "").strip().lower()
                if part_type not in {"text", "image", "audio"}:
                    raise ValueError(f"input_envelope[{i}].type must be one of: text, image, audio")
                if part_type == "text":
                    if not isinstance(part.get("text"), str):
                        raise ValueError(f"input_envelope[{i}].text is required for type=text")
                else:
                    data_url = part.get("dataUrl")
                    if not isinstance(data_url, str) or not data_url.strip():
                        raise ValueError(f"input_envelope[{i}].dataUrl is required for type={part_type}")
        return self

# ============================================================================
# TOOL NODE SCHEMAS
# ============================================================================

class ToolType(str, Enum):
    MCP = "mcp"
    PYTHON = "python"
    API = "api"
    SCRIPT = "script"
    BUILTIN = "builtin"

class MCPToolParams(NodeParamSchema):
    tool_type: Literal[ToolType.MCP] = ToolType.MCP
    mcp_server: str = Field(..., description="MCP server identifier")
    mcp_tool: str = Field(..., description="Tool name within server")
    mcp_arguments: Dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: int = 30
    
    def validate_required(self) -> List[str]:
        errors = []
        if not self.mcp_server:
            errors.append("mcp_server is required")
        if not self.mcp_tool:
            errors.append("mcp_tool is required")
        return errors

class PythonToolParams(NodeParamSchema):
    tool_type: Literal[ToolType.PYTHON] = ToolType.PYTHON
    python_code: str = Field(..., description="Python code to execute")
    function_name: str = "main"
    environment: Literal["default", "isolated", "custom"] = "default"
    requirements: List[str] = Field(default_factory=list)
    timeout_seconds: int = 60
    
    def validate_required(self) -> List[str]:
        if not self.python_code:
            return ["python_code is required"]
        return []

class APIToolParams(NodeParamSchema):
    tool_type: Literal[ToolType.API] = ToolType.API
    url: str = Field(..., description="API endpoint")
    method: Literal["GET", "POST", "PUT", "DELETE", "PATCH"] = "POST"
    headers: Dict[str, str] = Field(default_factory=dict)
    body_template: Optional[str] = None  # JSON template with {variables}
    auth_type: Literal["none", "bearer", "basic", "api_key"] = "none"
    auth_ref: Optional[str] = None
    timeout_seconds: int = 30
    
    def validate_required(self) -> List[str]:
        errors = []
        if not self.url:
            errors.append("url is required")
        if self.auth_type != "none" and not self.auth_ref:
            errors.append("auth_ref required for authentication")
        return errors

class BuiltinToolParams(NodeParamSchema):
    tool_type: Literal[ToolType.BUILTIN] = ToolType.BUILTIN
    builtin_name: Literal["email", "slack", "webhook", "file_write", "database_insert"]
    config: Dict[str, Any] = Field(default_factory=dict)
    
    def validate_required(self) -> List[str]:
        # Validate based on builtin_name
        if self.builtin_name == "email":
            if "to" not in self.config:
                return ["config.to is required for email"]
        elif self.builtin_name == "slack":
            if "channel" not in self.config:
                return ["config.channel is required for slack"]
        return []


class ToolProviderParams(NodeParamSchema):
    provider: Literal["mcp", "http", "function", "python", "js", "shell", "db", "builtin"]
    side_effect_mode: Literal["pure", "idempotent", "effectful"] = "pure"
    cache_enabled: bool = True
    armed: bool = False
    output: Optional[Dict[str, Any]] = None
    mcp: Optional[Dict[str, Any]] = None
    http: Optional[Dict[str, Any]] = None
    function: Optional[Dict[str, Any]] = None
    python: Optional[Dict[str, Any]] = None
    js: Optional[Dict[str, Any]] = None
    shell: Optional[Dict[str, Any]] = None
    db: Optional[Dict[str, Any]] = None
    builtin: Optional[Dict[str, Any]] = None

    def validate_required(self) -> List[str]:
        errors: List[str] = []
        builtin_cfg = self.builtin if isinstance(self.builtin, dict) else None
        if builtin_cfg is not None:
            profile_id = str(builtin_cfg.get("profileId") or "core").strip()
            allowed_profile_ids = {"core", "data", "ml", "llm_finetune", "full", "custom"}
            if profile_id not in allowed_profile_ids:
                errors.append("builtin.profileId must be one of: core, data, ml, llm_finetune, full, custom")
            custom_packages = builtin_cfg.get("customPackages")
            if custom_packages is not None:
                if not isinstance(custom_packages, list):
                    errors.append("builtin.customPackages must be an array")
                else:
                    for idx, pkg in enumerate(custom_packages):
                        if not isinstance(pkg, str) or not pkg.strip():
                            errors.append(f"builtin.customPackages[{idx}] must be a non-empty string")
                if profile_id != "custom" and isinstance(custom_packages, list) and len(custom_packages) > 0:
                    errors.append("builtin.customPackages is only allowed when builtin.profileId='custom'")
            if profile_id == "custom":
                if not isinstance(custom_packages, list) or len(custom_packages) == 0:
                    errors.append("builtin.customPackages must include at least one package when builtin.profileId='custom'")
            locked_value = builtin_cfg.get("locked")
            if locked_value is not None and (not isinstance(locked_value, str) or not locked_value.strip()):
                errors.append("builtin.locked must be a non-empty string when provided")
        provider = self.provider
        if provider == "mcp":
            if not isinstance(self.mcp, dict):
                errors.append("mcp config is required")
            else:
                if not self.mcp.get("serverId"):
                    errors.append("mcp.serverId is required")
                if not self.mcp.get("toolName"):
                    errors.append("mcp.toolName is required")
        elif provider == "http":
            if not isinstance(self.http, dict):
                errors.append("http config is required")
            else:
                if not self.http.get("url"):
                    errors.append("http.url is required")
                if not self.http.get("method"):
                    errors.append("http.method is required")
        elif provider == "function":
            if not isinstance(self.function, dict):
                errors.append("function config is required")
            else:
                if not self.function.get("module"):
                    errors.append("function.module is required")
                if not self.function.get("export"):
                    errors.append("function.export is required")
        elif provider == "python":
            if not isinstance(self.python, dict) or not self.python.get("code"):
                errors.append("python.code is required")
        elif provider == "js":
            if not isinstance(self.js, dict) or not self.js.get("code"):
                errors.append("js.code is required")
        elif provider == "shell":
            if not isinstance(self.shell, dict) or not self.shell.get("command"):
                errors.append("shell.command is required")
        elif provider == "db":
            if not isinstance(self.db, dict):
                errors.append("db config is required")
            else:
                if not self.db.get("connectionRef"):
                    errors.append("db.connectionRef is required")
                if not self.db.get("sql"):
                    errors.append("db.sql is required")
        elif provider == "builtin":
            if not isinstance(self.builtin, dict) or not self.builtin.get("toolId"):
                errors.append("builtin.toolId is required")
        return errors

ToolParams = Union[MCPToolParams, PythonToolParams, APIToolParams, BuiltinToolParams]

# ============================================================================
# COMPONENT NODE SCHEMAS
# ============================================================================

class ComponentTypedField(NodeParamSchema):
    name: str
    type: Literal["table", "json", "text", "binary", "embeddings", "unknown"]
    nativeType: Optional[str] = None
    nullable: bool = False

    @model_validator(mode="before")
    @classmethod
    def _normalize_aliases(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        next_data = dict(data)
        raw_type = str(next_data.get("type") or "").strip().lower()
        if raw_type == "string":
            next_data["type"] = "text"
        return next_data


class ComponentTypedSchema(NodeParamSchema):
    type: Literal["table", "json", "text", "binary", "embeddings", "unknown"]
    fields: List[ComponentTypedField] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _normalize_aliases(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        next_data = dict(data)
        raw_type = str(next_data.get("type") or "").strip().lower()
        if raw_type == "string":
            next_data["type"] = "text"
        return next_data


class ComponentApiPort(NodeParamSchema):
    name: str
    required: bool = True
    typedSchema: ComponentTypedSchema


class ComponentApiContract(NodeParamSchema):
    inputs: List[ComponentApiPort] = Field(default_factory=list)
    outputs: List[ComponentApiPort] = Field(default_factory=list)


class ComponentRefParams(NodeParamSchema):
    componentId: str
    revisionId: str
    apiVersion: str = "v1"


class ComponentBindingsParams(NodeParamSchema):
    inputs: Dict[str, str] = Field(default_factory=dict)
    config: Dict[str, str] = Field(default_factory=dict)
    outputs: Dict[str, Dict[str, str]] = Field(default_factory=dict)


class ComponentParams(NodeParamSchema):
    componentRef: ComponentRefParams
    bindings: ComponentBindingsParams = Field(default_factory=ComponentBindingsParams)
    config: Dict[str, Any] = Field(default_factory=dict)
    api: Optional[ComponentApiContract] = None

    def validate_required(self) -> List[str]:
        errors: List[str] = []
        if not str(getattr(self.componentRef, "componentId", "") or "").strip():
            errors.append("componentRef.componentId is required")
        if not str(getattr(self.componentRef, "revisionId", "") or "").strip():
            errors.append("componentRef.revisionId is required")
        return errors

# ============================================================================
# SCHEMA REGISTRY
# ============================================================================

SCHEMA_REGISTRY: Dict[str, type[NodeParamSchema]] = {
    # Source schemas
    "source:file": SourceFileParams,
    "source:database": SourceDatabaseParams,
    "source:api": SourceAPIParams,
    "source:object_store": SourceObjectStoreParams,
    "source:warehouse": SourceWarehouseParams,
    
    # Transform schema (current op union contract)
    "transform": TransformParamsCurrent,
    
    # LLM schemas (single schema with type field)
    "llm": LLMParams,
    "model": LLMParams,
    
    # Tool schemas
    "tool:mcp": MCPToolParams,
    "tool:python": PythonToolParams,
    "tool:api": APIToolParams,
    "tool:builtin": BuiltinToolParams,
    "tool": ToolProviderParams,
    "component": ComponentParams,
}

def get_schema_for_node(node: Dict[str, Any]) -> Optional[type[NodeParamSchema]]:
    kind = node["data"].get("kind")

    if kind == "source":
        sk = node["data"].get("sourceKind", "file")
        return SCHEMA_REGISTRY.get(f"source:{sk}")

    elif kind == "transform":
        return SCHEMA_REGISTRY.get("transform")

    elif kind == "tool":
        return SCHEMA_REGISTRY.get("tool")

    elif kind in {"llm", "model"}:
        return SCHEMA_REGISTRY.get("llm")
    
    elif kind == "component":
        return SCHEMA_REGISTRY.get("component")

    return None


def _machine_error(code: str, param_path: str, message: str, **extra: Any) -> str:
    payload = {"errorCode": code, "paramPath": param_path, "message": message}
    if extra:
        payload.update(extra)
    return json.dumps(payload, sort_keys=True)


def validate_node_params(node: Dict[str, Any]) -> List[str]:
    """Validate node parameters against schema"""
    errors: list[str] = []

    kind = node.get("data", {}).get("kind")
    params = node.get("data", {}).get("params", {}) or {}

    try:
        if kind in {"llm", "model"}:
            norm = normalize_llm_params_frontend(params)

            llm_kind = node.get("data", {}).get("llmKind") or "ollama"
            model_kind = node.get("data", {}).get("modelKind") or "llm"
            task_kind = node.get("data", {}).get("taskKind") or "generate"
            if model_kind not in {"llm", "vision", "audio", "embedding", "reranker", "multimodal"}:
                errors.append(
                    _machine_error(
                        code="INVALID_VALUE",
                        param_path="data.modelKind",
                        message=(
                            "modelKind must be one of: llm, vision, audio, embedding, reranker, multimodal"
                        ),
                        value=model_kind,
                    )
                )
            task_allowed = {
                "llm": {"generate", "classify", "extract"},
                "vision": {"caption", "classify", "extract", "generate"},
                "audio": {"transcribe", "extract", "classify"},
                "embedding": {"embed"},
                "reranker": {"rerank"},
                "multimodal": {"generate", "classify", "extract", "caption", "transcribe"},
            }
            if task_kind not in {"generate", "classify", "extract", "embed", "rerank", "transcribe", "caption"}:
                errors.append(
                    _machine_error(
                        code="INVALID_VALUE",
                        param_path="data.taskKind",
                        message=(
                            "taskKind must be one of: generate, classify, extract, embed, rerank, transcribe, caption"
                        ),
                        value=task_kind,
                    )
                )
            elif task_kind not in task_allowed.get(model_kind, {"generate"}):
                errors.append(
                    _machine_error(
                        code="INVALID_VALUE",
                        param_path="data.taskKind",
                        message=f"taskKind '{task_kind}' is not valid for modelKind '{model_kind}'",
                        modelKind=model_kind,
                        taskKind=task_kind,
                    )
                )

            llm_params = LLMParams.model_validate(norm)
        elif kind == "source":
            source_kind = (
                node.get("data", {}).get("sourceKind")
                or (params.get("source_type") if isinstance(params, dict) else None)
                or "file"
            )
            norm_source = normalize_source_params_frontend(
                {
                    **(params if isinstance(params, dict) else {}),
                    "source_type": source_kind,
                }
            )
            norm_source["source_type"] = source_kind
            if source_kind == "file":
                model = SourceFileParams.model_validate(norm_source)
                errors.extend(model.validate_required())
            elif source_kind == "database":
                model = SourceDatabaseParams.model_validate(norm_source)
                errors.extend(model.validate_required())
            elif source_kind == "api":
                model = SourceAPIParams.model_validate(norm_source)
                errors.extend(model.validate_required())
            elif source_kind == "object_store":
                model = SourceObjectStoreParams.model_validate(norm_source)
                errors.extend(model.validate_required())
            elif source_kind == "warehouse":
                model = SourceWarehouseParams.model_validate(norm_source)
                errors.extend(model.validate_required())
            else:
                errors.append(f"Unsupported source kind: {source_kind}")
        elif kind == "transform":
            from .nodes.transform import normalize_transform_params

            transform_kind = (node.get("data", {}) or {}).get("transformKind")
            norm = normalize_transform_params(params, default_op=transform_kind)
            model = TransformParamsCurrent.model_validate(norm)
            errors.extend(model.validate_required())

            op = norm.get("op")
            payload_key = {
                "filter": "filter",
                "select": "select",
                "rename": "rename",
                "derive": "derive",
                "aggregate": "aggregate",
                "join": "join",
                "sort": "sort",
                "limit": "limit",
                "dedupe": "dedupe",
                "null_policy": "null_policy",
                "outlier_policy": "outlier_policy",
                "text_clean": "text_clean",
                "nlp_normalize": "nlp_normalize",
                "tokenize_chunk": "tokenize_chunk",
                "dataset_split": "dataset_split",
                "class_imbalance": "class_imbalance",
                "categorical_encode": "categorical_encode",
                "numeric_scale": "numeric_scale",
                "embedding": "embedding",
                "feature_selection": "feature_selection",
                "leakage_detect": "leakage_detect",
                "quality_profile": "quality_profile",
                "drift_compare": "drift_compare",
                "determinism_profile": "determinism_profile",
                "fit_state_registry": "fit_state_registry",
                "pii_guard": "pii_guard",
                "inference_parity": "inference_parity",
                "split": "split",
                "quality_gate": "quality_gate",
                "ml_contract": "ml_contract",
                "sql": "sql",
                "json_to_table": "json_to_table",
                "text_to_table": "text_to_table",
                "table_to_json": "table_to_json",
            }.get(op)
            payload = norm.get(payload_key) if payload_key else None
            if not isinstance(payload, dict):
                errors.append(f"{payload_key} block is required for op='{op}'")
            else:
                if op == "filter":
                    expr = payload.get("expr")
                    if expr is not None and not isinstance(expr, str):
                        errors.append("filter.expr must be a string")
                elif op == "select":
                    cols = payload.get("columns")
                    if not isinstance(cols, list):
                        errors.append("select.columns must be an array")
                    else:
                        seen: set[str] = set()
                        for i, col in enumerate(cols):
                            name = str(col or "").strip()
                            if not name:
                                errors.append(f"select.columns[{i}] cannot be empty")
                                continue
                            if name in seen:
                                errors.append(f"select.columns has duplicate column '{name}'")
                            else:
                                seen.add(name)
                    mode = str(payload.get("mode") or "include").strip().lower()
                    if mode not in {"include", "exclude"}:
                        errors.append("select.mode must be one of: include, exclude")
                    keep_order = str(payload.get("keepOrder") or "").strip().lower()
                    if keep_order and keep_order not in {"input", "custom"}:
                        errors.append("select.keepOrder must be one of: input, custom")
                    if "strict" in payload and not isinstance(payload.get("strict"), bool):
                        errors.append("select.strict must be boolean")
                elif op == "rename":
                    mp = payload.get("map")
                    if not isinstance(mp, dict) or len(mp) == 0:
                        errors.append("rename.map must be a non-empty object")
                elif op == "derive":
                    cols = payload.get("columns")
                    if not isinstance(cols, list) or len(cols) == 0:
                        errors.append("derive.columns must be a non-empty array")
                elif op == "aggregate":
                    group_by = payload.get("groupBy")
                    if group_by is not None and (
                        not isinstance(group_by, list)
                        or any(not str(c).strip() for c in group_by)
                    ):
                        errors.append("aggregate.groupBy must be an array of non-empty column names")
                    metrics = payload.get("metrics")
                    if not isinstance(metrics, list) or len(metrics) == 0:
                        errors.append("aggregate.metrics must be a non-empty array")
                    else:
                        seen_names: set[str] = set()
                        allowed_ops = {
                            "count_rows",
                            "count",
                            "count_distinct",
                            "min",
                            "max",
                            "sum",
                            "mean",
                            "avg_length",
                            "min_length",
                            "max_length",
                        }
                        needs_column = {
                            "count",
                            "count_distinct",
                            "min",
                            "max",
                            "sum",
                            "mean",
                            "avg_length",
                            "min_length",
                            "max_length",
                        }
                        for i, metric in enumerate(metrics):
                            if not isinstance(metric, dict):
                                errors.append(f"aggregate.metrics[{i}] must be an object")
                                continue
                            name = str(metric.get("name") or "").strip()
                            if not name:
                                errors.append(f"aggregate.metrics[{i}].name is required")
                            elif name in seen_names:
                                errors.append(f"aggregate.metrics[{i}].name must be unique")
                            else:
                                seen_names.add(name)
                            op_name = str(metric.get("op") or "").strip()
                            if op_name not in allowed_ops:
                                errors.append(
                                    f"aggregate.metrics[{i}].op must be one of: {', '.join(sorted(allowed_ops))}"
                                )
                            if op_name in needs_column:
                                column = str(metric.get("column") or "").strip()
                                if not column:
                                    errors.append(f"aggregate.metrics[{i}].column is required for op='{op_name}'")
                elif op == "join":
                    clauses = payload.get("clauses")
                    if not isinstance(clauses, list) or len(clauses) == 0:
                        errors.append("join.clauses must be a non-empty array")
                    else:
                        allowed_hows = {"inner", "left", "right", "full"}
                        for i, clause in enumerate(clauses):
                            if not isinstance(clause, dict):
                                errors.append(f"join.clauses[{i}] must be an object")
                                continue
                            if not str(clause.get("leftNodeId") or "").strip():
                                errors.append(f"join.clauses[{i}].leftNodeId is required")
                            if not str(clause.get("leftCol") or "").strip():
                                errors.append(f"join.clauses[{i}].leftCol is required")
                            if not str(clause.get("rightNodeId") or "").strip():
                                errors.append(f"join.clauses[{i}].rightNodeId is required")
                            if not str(clause.get("rightCol") or "").strip():
                                errors.append(f"join.clauses[{i}].rightCol is required")
                            how = str(clause.get("how") or "inner").strip().lower()
                            if how not in allowed_hows:
                                errors.append(f"join.clauses[{i}].how must be one of: inner, left, right, full")
                elif op == "sort":
                    by = payload.get("by")
                    if not isinstance(by, list) or len(by) == 0:
                        errors.append("sort.by must be a non-empty array")
                elif op == "limit":
                    n = payload.get("n")
                    if not isinstance(n, int) or n < 1:
                        errors.append("limit.n must be an integer >= 1")
                elif op == "dedupe":
                    by = payload.get("by")
                    all_columns = payload.get("allColumns")
                    keep = payload.get("keep")
                    if by is not None and not isinstance(by, list):
                        errors.append("dedupe.by must be an array of column names")
                    if isinstance(by, list) and any(not str(c).strip() for c in by):
                        errors.append("dedupe.by cannot contain empty column names")
                    if keep is not None and str(keep) != "first":
                        errors.append("dedupe.keep must be 'first'")
                elif op == "null_policy":
                    mode = str(payload.get("mode") or "report").strip().lower()
                    if mode not in {"report", "drop_rows", "fill_constant", "fill_stat"}:
                        errors.append("null_policy.mode must be one of: report, drop_rows, fill_constant, fill_stat")
                    cols = payload.get("columns")
                    if cols is not None and (not isinstance(cols, list) or any(not str(c).strip() for c in cols)):
                        errors.append("null_policy.columns must be an array of non-empty column names")
                    stat = str(payload.get("stat") or "mean").strip().lower()
                    if stat not in {"mean", "median", "mode"}:
                        errors.append("null_policy.stat must be one of: mean, median, mode")
                    rules = payload.get("rules")
                    if rules is not None:
                        if not isinstance(rules, list):
                            errors.append("null_policy.rules must be an array")
                        else:
                            for i, rule in enumerate(rules):
                                if not isinstance(rule, dict):
                                    errors.append(f"null_policy.rules[{i}] must be an object")
                                    continue
                                if not str(rule.get("column") or "").strip():
                                    errors.append(f"null_policy.rules[{i}].column is required")
                elif op == "outlier_policy":
                    mode = str(payload.get("mode") or "clip").strip().lower()
                    if mode not in {"clip", "winsorize", "drop"}:
                        errors.append("outlier_policy.mode must be one of: clip, winsorize, drop")
                    method = str(payload.get("method") or "iqr").strip().lower()
                    if method not in {"iqr", "zscore", "quantile"}:
                        errors.append("outlier_policy.method must be one of: iqr, zscore, quantile")
                    cols = payload.get("columns")
                    if cols is not None and (not isinstance(cols, list) or any(not str(c).strip() for c in cols)):
                        errors.append("outlier_policy.columns must be an array of non-empty column names")
                    lower_q = payload.get("lowerQuantile")
                    upper_q = payload.get("upperQuantile")
                    if lower_q is not None and upper_q is not None:
                        try:
                            if float(lower_q) >= float(upper_q):
                                errors.append("outlier_policy.upperQuantile must be greater than lowerQuantile")
                        except Exception:
                            errors.append("outlier_policy quantiles must be numeric")
                elif op == "text_clean":
                    cols = payload.get("columns")
                    if cols is not None and (not isinstance(cols, list) or any(not str(c).strip() for c in cols)):
                        errors.append("text_clean.columns must be an array of non-empty column names")
                    normalize_mode = str(payload.get("unicodeNormalize") or "nfkc").strip().lower()
                    if normalize_mode not in {"none", "nfc", "nfkc"}:
                        errors.append("text_clean.unicodeNormalize must be one of: none, nfc, nfkc")
                elif op == "nlp_normalize":
                    cols = payload.get("columns")
                    if cols is not None and (not isinstance(cols, list) or any(not str(c).strip() for c in cols)):
                        errors.append("nlp_normalize.columns must be an array of non-empty column names")
                    if not str(payload.get("language") or "").strip():
                        errors.append("nlp_normalize.language is required")
                    stemmer = str(payload.get("stemmer") or "none").strip().lower()
                    if stemmer not in {"none", "porter"}:
                        errors.append("nlp_normalize.stemmer must be one of: none, porter")
                    lemmatizer = str(payload.get("lemmatizer") or "none").strip().lower()
                    if lemmatizer not in {"none", "rule_based"}:
                        errors.append("nlp_normalize.lemmatizer must be one of: none, rule_based")
                    if not str(payload.get("tokenPattern") or "").strip():
                        errors.append("nlp_normalize.tokenPattern is required")
                elif op == "tokenize_chunk":
                    cols = payload.get("columns")
                    if cols is not None and (not isinstance(cols, list) or any(not str(c).strip() for c in cols)):
                        errors.append("tokenize_chunk.columns must be an array of non-empty column names")
                    tokenizer = str(payload.get("tokenizer") or "whitespace").strip().lower()
                    if tokenizer not in {"whitespace", "regex"}:
                        errors.append("tokenize_chunk.tokenizer must be one of: whitespace, regex")
                    max_tokens = payload.get("maxTokens")
                    overlap = payload.get("overlap")
                    if not isinstance(max_tokens, int) or max_tokens < 1:
                        errors.append("tokenize_chunk.maxTokens must be an integer >= 1")
                    if not isinstance(overlap, int) or overlap < 0:
                        errors.append("tokenize_chunk.overlap must be an integer >= 0")
                    if isinstance(max_tokens, int) and isinstance(overlap, int) and overlap >= max_tokens:
                        errors.append("tokenize_chunk.overlap must be less than maxTokens")
                    if not str(payload.get("outColumn") or "").strip():
                        errors.append("tokenize_chunk.outColumn is required")
                elif op == "dataset_split":
                    strategy = str(payload.get("strategy") or "random").strip().lower()
                    if strategy not in {"random", "stratified", "group", "time"}:
                        errors.append("dataset_split.strategy must be one of: random, stratified, group, time")
                    for key in ("trainRatio", "valRatio", "testRatio"):
                        try:
                            ratio = float(payload.get(key))
                            if ratio < 0 or ratio > 1:
                                errors.append(f"dataset_split.{key} must be between 0 and 1")
                        except Exception:
                            errors.append(f"dataset_split.{key} must be numeric")
                    if strategy == "stratified" and not str(payload.get("stratifyColumn") or "").strip():
                        errors.append("dataset_split.stratifyColumn is required when strategy=stratified")
                    if strategy == "group" and not str(payload.get("groupColumn") or "").strip():
                        errors.append("dataset_split.groupColumn is required when strategy=group")
                    if strategy == "time" and not str(payload.get("timeColumn") or "").strip():
                        errors.append("dataset_split.timeColumn is required when strategy=time")
                elif op == "class_imbalance":
                    strategy = str(payload.get("strategy") or "report").strip().lower()
                    if strategy not in {"report", "undersample", "oversample", "class_weight"}:
                        errors.append("class_imbalance.strategy must be one of: report, undersample, oversample, class_weight")
                    if not str(payload.get("labelColumn") or "").strip():
                        errors.append("class_imbalance.labelColumn is required")
                    try:
                        target_ratio = float(payload.get("targetRatio"))
                        if target_ratio < 0 or target_ratio > 1:
                            errors.append("class_imbalance.targetRatio must be between 0 and 1")
                    except Exception:
                        errors.append("class_imbalance.targetRatio must be numeric")
                elif op == "categorical_encode":
                    cols = payload.get("columns")
                    if cols is not None and (not isinstance(cols, list) or any(not str(c).strip() for c in cols)):
                        errors.append("categorical_encode.columns must be an array of non-empty column names")
                    encoding = str(payload.get("encoding") or "one_hot").strip().lower()
                    if encoding not in {"one_hot", "ordinal", "frequency"}:
                        errors.append("categorical_encode.encoding must be one of: one_hot, ordinal, frequency")
                    unknown_policy = str(payload.get("unknownPolicy") or "ignore").strip().lower()
                    if unknown_policy not in {"ignore", "error", "impute"}:
                        errors.append("categorical_encode.unknownPolicy must be one of: ignore, error, impute")
                elif op == "numeric_scale":
                    cols = payload.get("columns")
                    if cols is not None and (not isinstance(cols, list) or any(not str(c).strip() for c in cols)):
                        errors.append("numeric_scale.columns must be an array of non-empty column names")
                    method = str(payload.get("method") or "standard").strip().lower()
                    if method not in {"standard", "minmax", "robust"}:
                        errors.append("numeric_scale.method must be one of: standard, minmax, robust")
                elif op == "embedding":
                    cols = payload.get("columns")
                    if cols is not None and (not isinstance(cols, list) or any(not str(c).strip() for c in cols)):
                        errors.append("embedding.columns must be an array of non-empty column names")
                    provider = str(payload.get("provider") or "local_hash").strip().lower()
                    if provider not in {"local_hash", "openai", "ollama"}:
                        errors.append("embedding.provider must be one of: local_hash, openai, ollama")
                    dims = payload.get("dimensions")
                    if not isinstance(dims, int) or dims < 1 or dims > 4096:
                        errors.append("embedding.dimensions must be an integer between 1 and 4096")
                    if not str(payload.get("outputColumn") or "").strip():
                        errors.append("embedding.outputColumn is required")
                elif op == "feature_selection":
                    method = str(payload.get("method") or "variance").strip().lower()
                    if method not in {"variance", "mutual_info", "model_importance", "manual"}:
                        errors.append("feature_selection.method must be one of: variance, mutual_info, model_importance, manual")
                    cols = payload.get("columns")
                    if cols is not None and (not isinstance(cols, list) or any(not str(c).strip() for c in cols)):
                        errors.append("feature_selection.columns must be an array of non-empty column names")
                    selected = payload.get("selectedColumns")
                    if selected is not None and (not isinstance(selected, list) or any(not str(c).strip() for c in selected)):
                        errors.append("feature_selection.selectedColumns must be an array of non-empty column names")
                elif op == "leakage_detect":
                    if not str(payload.get("splitColumn") or "").strip():
                        errors.append("leakage_detect.splitColumn is required")
                    keys = payload.get("keyColumns")
                    if keys is not None and (not isinstance(keys, list) or any(not str(c).strip() for c in keys)):
                        errors.append("leakage_detect.keyColumns must be an array of non-empty column names")
                    try:
                        overlap = float(payload.get("maxAllowedOverlap"))
                        if overlap < 0 or overlap > 1:
                            errors.append("leakage_detect.maxAllowedOverlap must be between 0 and 1")
                    except Exception:
                        errors.append("leakage_detect.maxAllowedOverlap must be numeric")
                elif op == "quality_profile":
                    cols = payload.get("columns")
                    if cols is not None and (not isinstance(cols, list) or any(not str(c).strip() for c in cols)):
                        errors.append("quality_profile.columns must be an array of non-empty column names")
                elif op == "drift_compare":
                    cols = payload.get("compareColumns")
                    if cols is not None and (not isinstance(cols, list) or any(not str(c).strip() for c in cols)):
                        errors.append("drift_compare.compareColumns must be an array of non-empty column names")
                    metric = str(payload.get("metric") or "psi").strip().lower()
                    if metric not in {"psi", "jsd", "ks"}:
                        errors.append("drift_compare.metric must be one of: psi, jsd, ks")
                elif op == "determinism_profile":
                    if "strict" in payload and not isinstance(payload.get("strict"), bool):
                        errors.append("determinism_profile.strict must be boolean")
                    if "seed" in payload and not isinstance(payload.get("seed"), int):
                        errors.append("determinism_profile.seed must be integer")
                elif op == "fit_state_registry":
                    mode = str(payload.get("mode") or "fit").strip().lower()
                    if mode not in {"fit", "apply"}:
                        errors.append("fit_state_registry.mode must be one of: fit, apply")
                    if not str(payload.get("stateKey") or "").strip():
                        errors.append("fit_state_registry.stateKey is required")
                    cols = payload.get("includeColumns")
                    if cols is not None and (not isinstance(cols, list) or any(not str(c).strip() for c in cols)):
                        errors.append("fit_state_registry.includeColumns must be an array of non-empty column names")
                elif op == "pii_guard":
                    cols = payload.get("columns")
                    if cols is not None and (not isinstance(cols, list) or any(not str(c).strip() for c in cols)):
                        errors.append("pii_guard.columns must be an array of non-empty column names")
                    action = str(payload.get("action") or "report").strip().lower()
                    if action not in {"report", "mask", "drop_rows"}:
                        errors.append("pii_guard.action must be one of: report, mask, drop_rows")
                elif op == "inference_parity":
                    if "failOnMismatch" in payload and not isinstance(payload.get("failOnMismatch"), bool):
                        errors.append("inference_parity.failOnMismatch must be boolean")
                elif op == "sql" and not str(payload.get("query") or "").strip():
                    errors.append("sql.query is required")
                elif op == "json_to_table":
                    orient = str(payload.get("orient") or "records").strip().lower()
                    if orient not in {"records", "object"}:
                        errors.append("json_to_table.orient must be one of: records, object")
                    if not str(payload.get("rowsKey") or "rows").strip():
                        errors.append("json_to_table.rowsKey is required")
                elif op == "text_to_table":
                    mode = str(payload.get("mode") or "lines").strip().lower()
                    if mode not in {"lines", "csv", "tsv"}:
                        errors.append("text_to_table.mode must be one of: lines, csv, tsv")
                    if not str(payload.get("column") or "text").strip():
                        errors.append("text_to_table.column is required")
                    if mode == "csv" and not str(payload.get("delimiter") or ","):
                        errors.append("text_to_table.delimiter is required when mode=csv")
                elif op == "table_to_json":
                    orient = str(payload.get("orient") or "records").strip().lower()
                    if orient not in {"records", "split"}:
                        errors.append("table_to_json.orient must be one of: records, split")
                    if "pretty" in payload and not isinstance(payload.get("pretty"), bool):
                        errors.append("table_to_json.pretty must be boolean")
                elif op == "split":
                    source_col = str(payload.get("sourceColumn") or "").strip()
                    out_col = str(payload.get("outColumn") or "").strip()
                    mode = str(payload.get("mode") or "sentences").strip()
                    line_break = str(payload.get("lineBreak") or "any").strip().lower()
                    pattern = str(payload.get("pattern") or "")
                    delimiter = payload.get("delimiter")
                    flags = str(payload.get("flags") or "")
                    max_parts = payload.get("maxParts")
                    if not source_col:
                        errors.append("split.sourceColumn is required")
                    if not out_col:
                        errors.append("split.outColumn is required")
                    if mode not in {"sentences", "lines", "regex", "delimiter"}:
                        errors.append("split.mode must be one of: sentences, lines, regex, delimiter")
                    if mode == "regex" and not pattern.strip():
                        errors.append("split.pattern is required when mode=regex")
                    if mode == "delimiter" and (not isinstance(delimiter, str) or delimiter == ""):
                        errors.append("split.delimiter is required when mode=delimiter")
                    if line_break not in {"any", "lf", "crlf", "cr"}:
                        errors.append("split.lineBreak must be one of: any, lf, crlf, cr")
                    if any(ch not in {"i", "m", "s"} for ch in flags):
                        errors.append("split.flags allows only i, m, s")
                    if not isinstance(max_parts, int) or max_parts < 1 or max_parts > 100000:
                        errors.append("split.maxParts must be an integer between 1 and 100000")
                elif op == "quality_gate":
                    checks = payload.get("checks")
                    if not isinstance(checks, list):
                        errors.append("quality_gate.checks must be an array")
                    else:
                        for i, check in enumerate(checks):
                            if not isinstance(check, dict):
                                errors.append(f"quality_gate.checks[{i}] must be an object")
                                continue
                            kind = str(check.get("kind") or "").strip().lower()
                            severity = str(check.get("severity") or "fail").strip().lower()
                            if severity not in {"warn", "fail"}:
                                errors.append(f"quality_gate.checks[{i}].severity must be one of: warn, fail")
                            if kind not in {"null_pct", "range", "uniqueness", "class_balance", "leakage"}:
                                errors.append(
                                    f"quality_gate.checks[{i}].kind must be one of: null_pct, range, uniqueness, class_balance, leakage"
                                )
                                continue
                            if kind in {"null_pct", "range", "uniqueness", "class_balance"}:
                                if not str(check.get("column") or "").strip():
                                    errors.append(f"quality_gate.checks[{i}].column is required")
                            if kind == "range":
                                has_min = check.get("min") is not None and str(check.get("min")).strip() != ""
                                has_max = check.get("max") is not None and str(check.get("max")).strip() != ""
                                if not has_min and not has_max:
                                    errors.append(f"quality_gate.checks[{i}] range check requires min and/or max")
                            if kind == "leakage":
                                if not str(check.get("featureColumn") or "").strip():
                                    errors.append(f"quality_gate.checks[{i}].featureColumn is required")
                                if not str(check.get("targetColumn") or "").strip():
                                    errors.append(f"quality_gate.checks[{i}].targetColumn is required")
                elif op == "ml_contract":
                    task_type = str(payload.get("taskType") or "other").strip().lower()
                    if task_type not in {
                        "classification",
                        "regression",
                        "ranking",
                        "generation",
                        "embedding",
                        "pretraining",
                        "finetuning",
                        "other",
                    }:
                        errors.append(
                            "ml_contract.taskType must be one of: classification, regression, ranking, generation, embedding, pretraining, finetuning, other"
                        )
                    label = str(payload.get("labelColumn") or "").strip()
                    if not label:
                        errors.append("ml_contract.labelColumn is required")
                    features = payload.get("featureColumns")
                    if not isinstance(features, list) or len(features) == 0:
                        errors.append("ml_contract.featureColumns must be a non-empty array")
                    elif any(not str(c).strip() for c in features):
                        errors.append("ml_contract.featureColumns cannot contain empty values")
                    if "allowExtraFeatures" in payload and not isinstance(payload.get("allowExtraFeatures"), bool):
                        errors.append("ml_contract.allowExtraFeatures must be boolean")
                    if "requireNonNullLabel" in payload and not isinstance(payload.get("requireNonNullLabel"), bool):
                        errors.append("ml_contract.requireNonNullLabel must be boolean")
        elif kind == "tool":
            norm_tool = normalize_tool_params_frontend(params)
            tool_model = ToolProviderParams.model_validate(norm_tool)
            errors.extend(tool_model.validate_required())
        elif kind == "component":
            component_ref = params.get("componentRef")
            if not isinstance(component_ref, dict):
                errors.append(
                    _machine_error(
                        "MISSING_COMPONENT_REF",
                        "params.componentRef",
                        "componentRef is required",
                    )
                )
            else:
                if not str(component_ref.get("componentId") or "").strip():
                    errors.append(
                        _machine_error(
                            "MISSING_COMPONENT_ID",
                            "params.componentRef.componentId",
                            "componentRef.componentId is required",
                        )
                    )
                if not str(component_ref.get("revisionId") or "").strip():
                    errors.append(
                        _machine_error(
                            "MISSING_REVISION_ID",
                            "params.componentRef.revisionId",
                            "componentRef.revisionId is required",
                        )
                    )

            api = params.get("api")
            if api is not None:
                if not isinstance(api, dict):
                    errors.append(
                        _machine_error(
                            "INVALID_COMPONENT_API",
                            "params.api",
                            "api must be an object",
                        )
                    )
                else:
                    for section_name in ("inputs", "outputs"):
                        section = api.get(section_name)
                        if section is None:
                            continue
                        if not isinstance(section, list):
                            errors.append(
                                _machine_error(
                                    "INVALID_COMPONENT_API_SECTION",
                                    f"params.api.{section_name}",
                                    f"{section_name} must be an array",
                                )
                            )
                            continue
                        for idx, entry in enumerate(section):
                            if not isinstance(entry, dict):
                                errors.append(
                                    _machine_error(
                                        "INVALID_COMPONENT_API_ENTRY",
                                        f"params.api.{section_name}[{idx}]",
                                        "entry definition must be an object",
                                    )
                                )
                                continue
                            typed_schema = entry.get("typedSchema")
                            if not isinstance(typed_schema, dict):
                                errors.append(
                                    _machine_error(
                                        "MISSING_TYPED_SCHEMA",
                                        f"params.api.{section_name}[{idx}].typedSchema",
                                        "typedSchema is required",
                                    )
                                )
                                continue
                            typed = str(typed_schema.get("type") or "").strip()
                            if typed not in {"table", "json", "text", "binary", "embeddings", "unknown"}:
                                errors.append(
                                    _machine_error(
                                        "INVALID_TYPED_SCHEMA_TYPE",
                                        f"params.api.{section_name}[{idx}].typedSchema.type",
                                        "typedSchema.type must be one of: table, json, text, binary, embeddings, unknown",
                                    )
                                )

            component_model = ComponentParams.model_validate(params)
            errors.extend(component_model.validate_required())

        # ... other kinds ...

    except Exception as e:
        errors.append(f"Parameter validation failed: {e}")

    return errors
