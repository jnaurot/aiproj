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

    # frontend may send stopSequences, inputMapping (if you add later)
    if "stopSequences" in p and "stop_sequences" not in p:
        p["stop_sequences"] = p.pop("stopSequences")

    if "inputMapping" in p and "input_mapping" not in p:
        p["input_mapping"] = p.pop("inputMapping")

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
    # source_type: Literal[SourceKind.FILE] = SourceKind.FILE
    file_path: str = Field(..., description="Path to file")
    file_format: Literal["csv", "parquet", "json", "excel", "txt"] = "csv"
    delimiter: Optional[str] = None  # for CSV
    sheet_name: Optional[str] = None  # for Excel
    sample_size: Optional[int] = None
    encoding: str = "utf-8"
    cache_enabled: bool = True
    
    def validate_required(self) -> List[str]:
        errors = []
        if not self.file_path:
            errors.append("file_path is required")
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
    method: Literal["GET", "POST", "PUT", "DELETE"] = "GET"
    headers: Dict[str, str] = Field(default_factory=dict)
    body: Optional[Dict[str, Any]] = None
    auth_type: Literal["none", "bearer", "basic", "api_key"] = "none"
    auth_token_ref: Optional[str] = None
    timeout_seconds: int = 30
    
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

# ============================================================================
# LLM NODE SCHEMAS
# ============================================================================
# from .schemas import LLMParams  # ❌ don't import itself
class LLMType(str, Enum):
    COMPLETION = "completion"
    CHAT = "chat"
    EMBEDDINGS = "embeddings"
    CLASSIFICATION = "classification"

class LLMProvider(str, Enum):
    OPENAI = "openai_compat"
    OLLAMA = "ollama"
    
class LLMDialect(str, Enum):
    OPENAI_COMPAT = "openai_compat"
    OLLAMA = "ollama"

class LLMParams(NodeParamSchema):
    # kind: LLMDialect
    # llm_type: LLMType = LLMType.CHAT
    
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
    
    # # Processing mode
    # batch_size: int = Field(1, ge=1, description="Rows to process at once")
    # apply_to_column: Optional[str] = None
    # output_column: str = "llm_output"
    
    # output
    output_mode: Literal["text", "markdown", "json"] = "text"
    output_schema: Optional[Dict[str, Any]] = None
    
    # Error handling
    retry_on_error: bool = True
    max_retries: int = Field(3, ge=0, le=10)
    timeout_seconds: int = Field(60, ge=1)
    
    input_mapping: Optional[Dict[str, str]] = None  # variables -> input keys/ports

    # @model_validator(mode="after")
    # def _validate_contract(self):
    #     llm_kind = node.get("data", {}).get("llmKind") or "ollama"
    #     llm_params = LLMParams.model_validate(norm)
    #     # optional extra checks by kind:
    #     if llm_kind == "openai_compat":
    #         # only required if you want to enforce auth at validation time
    #         if not llm_params.connection_ref and not llm_params.api_key_ref:
    #             raise ValueError("api_key_ref required for openai_compat when no connection_ref")
    #     return self
    @model_validator(mode="after")
    def _validate_contract(self):
        if not self.base_url and not self.connection_ref:
            raise ValueError("Either base_url or connection_ref is required")
        if self.output_mode == "json" and not self.output_schema:
            raise ValueError("output_schema required when output_mode='json'")
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

ToolParams = Union[MCPToolParams, PythonToolParams, APIToolParams, BuiltinToolParams]

# ============================================================================
# SCHEMA REGISTRY
# ============================================================================

SCHEMA_REGISTRY: Dict[str, type[NodeParamSchema]] = {
    # Source schemas
    "source:file": SourceFileParams,
    "source:database": SourceDatabaseParams,
    "source:api": SourceAPIParams,
    
    # Transform schemas
    "transform:filter": FilterTransformParams,
    "transform:map": MapTransformParams,
    "transform:aggregate": AggregateTransformParams,
    "transform:clean": CleanTransformParams,
    "transform:custom": CustomTransformParams,
    
    # LLM schemas (single schema with type field)
    "llm": LLMParams,
    
    # Tool schemas
    "tool:mcp": MCPToolParams,
    "tool:python": PythonToolParams,
    "tool:api": APIToolParams,
    "tool:builtin": BuiltinToolParams,
}

def get_schema_for_node(node: Dict[str, Any]) -> Optional[type[NodeParamSchema]]:
    kind = node["data"].get("kind")

    if kind == "source":
        sk = node["data"].get("sourceKind", "file")
        return SCHEMA_REGISTRY.get(f"source:{sk}")

    elif kind == "transform":
        return None

    elif kind == "tool":
        return None

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
            # Optional: kind-specific checks
            if llm_kind == "openai_compat":
                if not llm_params.connection_ref and not llm_params.api_key_ref:
                    raise ValueError(
                        "api_key_ref required for openai_compat when no connection_ref"
                    )
        elif kind == "source":
            # Keep whatever you already had here for sources.
            # If you validate sources elsewhere, leave this branch as-is.
            pass

        # ... other kinds ...

    except Exception as e:
        errors.append(f"Parameter validation failed: {e}")

    return errors