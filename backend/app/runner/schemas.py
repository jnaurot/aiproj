#C:\Users\Owner\Desktop\aiproj\backend\app\runner\schemas.py
# from platform import node
from typing import Any, Dict, List, Literal, Optional, Union
from pydantic import BaseModel, Field, model_validator, validator
from enum import Enum

from pprint import pformat

def normalize_llm_params_frontend(raw: Dict[str, Any]) -> Dict[str, Any]:
    p = dict(raw or {})

    # camelCase -> snake_case (what LLMParams expects)
    if "baseUrl" in p and "base_url" not in p:
        p["base_url"] = p.pop("baseUrl")

    if "connectionRef" in p and "connection_ref" not in p:
        p["connection_ref"] = p.pop("connectionRef")

    if "apiKeyRef" in p and "api_key_ref" not in p:
        p["api_key_ref"] = p.pop("apiKeyRef")

    if "system_prompt" in p and "system_prompt" not in p:
        # no-op; included just to show: FE already uses system_prompt
        pass

    if "user_prompt" in p and "user_prompt" not in p:
        pass

    # frontend output object -> backend output_mode/output_schema
    out = p.get("output")
    if isinstance(out, dict):
        if "mode" in out and "output_mode" not in p:
            p["output_mode"] = out.get("mode")
        if "jsonSchema" in out and "output_schema" not in p:
            p["output_schema"] = out.get("jsonSchema")
        if "strict" in out and "output_strict" not in p:
            p["output_strict"] = out.get("strict")
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
        if "mode" in out and "output_mode" not in p:
            p["output_mode"] = out.get("mode")
        if "schema" in out and "output_schema" not in p:
            p["output_schema"] = out.get("schema")
    cache_policy = p.get("cache_policy")
    if isinstance(cache_policy, dict) and "ttlSeconds" in cache_policy and "ttl_seconds" not in cache_policy:
        cache_policy["ttl_seconds"] = cache_policy.pop("ttlSeconds")
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

class SourceFileParams(NodeParamSchema):
    rel_path: str = Field(..., description="Directory path")
    filename: str = Field(..., description="File name/path under rel_path")
    file_path: Optional[str] = None  # compatibility shim (legacy FE)
    file_format: Literal["csv", "tsv", "parquet", "json", "excel", "txt", "pdf"] = "csv"
    delimiter: Optional[str] = None  # for CSV
    sheet_name: Optional[str] = None  # for Excel
    sample_size: Optional[int] = None
    encoding: str = "utf-8"
    cache_enabled: bool = True
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
        if not self.rel_path:
            errors.append("rel_path is required")
        if not self.filename:
            errors.append("filename is required")
        if self.file_format == "csv" and self.delimiter is None:
            # Auto-detect or use default
            pass
        return errors

class SourceDatabaseParams(NodeParamSchema):
    #source_type: Literal[SourceKind.DATABASE] = SourceKind.DATABASE
    connection_string: Optional[str] = None
    connection_ref: Optional[str] = None  # reference to stored connection
    query: Optional[str] = None
    table_name: Optional[str] = None
    limit: Optional[int] = None
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
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"] = "GET"
    headers: Dict[str, str] = Field(default_factory=dict)
    body: Optional[Dict[str, Any]] = None
    auth_type: Literal["none", "bearer", "basic", "api_key"] = "none"
    auth_token_ref: Optional[str] = None
    timeout_seconds: int = 30
    cache_policy: Dict[str, Any] = Field(default_factory=lambda: {"mode": "default"})
    output_mode: Literal["table", "text", "json", "binary"] = "json"
    output_schema: Optional[Dict[str, Any]] = None
    
    def validate_required(self) -> List[str]:
        errors = []
        if not self.url:
            errors.append("url is required")
        if self.auth_type != "none" and not self.auth_token_ref:
            errors.append("auth_token_ref required when using authentication")
        return errors

# Union type for all source params
SourceParams = Union[SourceFileParams, SourceDatabaseParams, SourceAPIParams]

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
        "sql",
        "python",
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
    sql: Optional[Dict[str, Any]] = None
    code: Optional[Dict[str, Any]] = None

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
            "sql": "sql",
            "python": "code",
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
    embedding_contract: Optional[Dict[str, Any]] = None
    
    # Error handling
    retry_on_error: bool = True
    max_retries: int = Field(3, ge=0, le=10)
    timeout_seconds: int = Field(60, ge=1)
    
    input_mapping: Optional[Dict[str, str]] = None  # variables -> input keys/ports

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
# SCHEMA REGISTRY
# ============================================================================

SCHEMA_REGISTRY: Dict[str, type[NodeParamSchema]] = {
    # Source schemas
    "source:file": SourceFileParams,
    "source:database": SourceDatabaseParams,
    "source:api": SourceAPIParams,
    
    # Transform schema (current op union contract)
    "transform": TransformParamsCurrent,
    
    # LLM schemas (single schema with type field)
    "llm": LLMParams,
    
    # Tool schemas
    "tool:mcp": MCPToolParams,
    "tool:python": PythonToolParams,
    "tool:api": APIToolParams,
    "tool:builtin": BuiltinToolParams,
    "tool": ToolProviderParams,
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

    elif kind == "llm":
        return SCHEMA_REGISTRY.get("llm")

    return None


def validate_node_params(node: Dict[str, Any]) -> List[str]:
    """Validate node parameters against schema"""
    errors: list[str] = []

    kind = node.get("data", {}).get("kind")
    params = node.get("data", {}).get("params", {}) or {}

    try:
        if kind == "llm":
            print("\n[SCHEMAS] LLM params BEFORE normalize:")
            print(pformat(params)[:8000])

            norm = normalize_llm_params_frontend(params)

            print("[SCHEMAS] LLM params AFTER normalize:")
            print(pformat(norm)[:8000])

            llm_kind = node.get("data", {}).get("llmKind") or "ollama"

            llm_params = LLMParams.model_validate(norm)
        elif kind == "source":
            norm_source = normalize_source_params_frontend(params)
            source_kind = (node.get("data", {}).get("sourceKind") or norm_source.get("source_type") or "file")
            if source_kind == "file":
                model = SourceFileParams.model_validate(norm_source)
                errors.extend(model.validate_required())
            elif source_kind == "database":
                model = SourceDatabaseParams.model_validate(norm_source)
                errors.extend(model.validate_required())
            elif source_kind == "api":
                model = SourceAPIParams.model_validate(norm_source)
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
                "sql": "sql",
                "python": "code",
            }.get(op)
            payload = norm.get(payload_key) if payload_key else None
            if not isinstance(payload, dict):
                errors.append(f"{payload_key} block is required for op='{op}'")
            else:
                if op == "filter" and not str(payload.get("expr") or "").strip():
                    errors.append("filter.expr is required")
                elif op == "select":
                    cols = payload.get("columns")
                    if not isinstance(cols, list) or len(cols) == 0:
                        errors.append("select.columns must be a non-empty array")
                elif op == "rename":
                    mp = payload.get("map")
                    if not isinstance(mp, dict) or len(mp) == 0:
                        errors.append("rename.map must be a non-empty object")
                elif op == "derive":
                    cols = payload.get("columns")
                    if not isinstance(cols, list) or len(cols) == 0:
                        errors.append("derive.columns must be a non-empty array")
                elif op == "aggregate":
                    metrics = payload.get("metrics")
                    if not isinstance(metrics, list) or len(metrics) == 0:
                        errors.append("aggregate.metrics must be a non-empty array")
                elif op == "join":
                    if not str(payload.get("withNodeId") or "").strip():
                        errors.append("join.withNodeId is required")
                    ons = payload.get("on")
                    if not isinstance(ons, list) or len(ons) == 0:
                        errors.append("join.on must be a non-empty array")
                elif op == "sort":
                    by = payload.get("by")
                    if not isinstance(by, list) or len(by) == 0:
                        errors.append("sort.by must be a non-empty array")
                elif op == "limit":
                    n = payload.get("n")
                    if not isinstance(n, int) or n < 1:
                        errors.append("limit.n must be an integer >= 1")
                elif op == "sql" and not str(payload.get("query") or "").strip():
                    errors.append("sql.query is required")
                elif op == "python" and not str(payload.get("source") or "").strip():
                    errors.append("code.source is required")
        elif kind == "tool":
            norm_tool = normalize_tool_params_frontend(params)
            tool_model = ToolProviderParams.model_validate(norm_tool)
            errors.extend(tool_model.validate_required())

        # ... other kinds ...

    except Exception as e:
        errors.append(f"Parameter validation failed: {e}")

    return errors
