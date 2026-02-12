# backend/app/executors/source.py
import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional
import mimetypes

from ..runner.events import RunEventBus
from ..runner.metadata import ExecutionContext, NodeOutput, FileMetadata
from ..runner.schemas import SourceFileParams, SourceDatabaseParams, SourceAPIParams

# Import data reading libraries
import pandas as pd
import pyarrow.parquet as pq

# PDF libraries
try:
    import PyPDF2
    import pdfplumber
    HAS_PDF = True
except ImportError:
    HAS_PDF = False

# Database libraries
try:
    import sqlalchemy
    HAS_DATABASE = True
except ImportError:
    HAS_DATABASE = False

# HTTP library
import httpx

# print("[exec_source] has bus?", hasattr(context, "bus"), type(context.bus))

async def exec_source(
    run_id: str,
    node: Dict[str, Any],
    context: ExecutionContext,
    # bus: RunEventBus
    upstream_artifact_ids: Optional[list[str]] = None,
) -> NodeOutput:
    """
    Execute source node - load data from various sources
    
    Supports:
    - File sources: CSV, Parquet, JSON, Excel, TXT, PDF
    - Database sources: PostgreSQL, MySQL, SQLite
    - API sources: REST APIs with various auth methods
    """
    upstream_artifact_ids = upstream_artifact_ids or []
    assert context is not None, "context is None"
    assert hasattr(context, "bus"), "context missing bus"

    start_time = time.time()
    node_id = node["id"]
    params = node["data"].get("params", {})
    source_type = params.get("source_type", "file")
    
    try:
        # Route to appropriate handler based on source type
        if source_type == "file":
            output = await _handle_file_source(node_id, params, context.bus, run_id)
        elif source_type == "database":
            output = await _handle_database_source(node_id, params, context.bus, run_id)
        elif source_type == "api":
            output = await _handle_api_source(node_id, params, context.bus, run_id)
        else:
            raise ValueError(f"Unknown source_type: {source_type}")
        
        # Calculate execution time
        execution_time_ms = (time.time() - start_time) * 1000
        output.execution_time_ms = execution_time_ms
        
        await context.bus.emit({
            "type": "log",
            "runId": run_id,
            "at": _iso_now(),
            "level": "info",
            "message": f"Loaded {output.metadata.row_count if output.metadata else 'N/A'} rows from {source_type}",
            "nodeId": node_id
        })
        
        return output
        
    except Exception as e:
        execution_time_ms = (time.time() - start_time) * 1000
        await context.bus.emit({
            "type": "log",
            "runId": run_id,
            "at": _iso_now(),
            "level": "error",
            "message": f"Source execution failed: {str(e)}",
            "nodeId": node_id
        })
        
        return NodeOutput(
            status="failed",
            metadata=None,
            execution_time_ms=execution_time_ms,
            error=str(e)
        )


# ============================================================================
# FILE SOURCE HANDLERS
# ============================================================================

async def _handle_file_source(
    node_id: str,
    params: Dict[str, Any],
    bus: RunEventBus,
    run_id: str
) -> NodeOutput:
    """Handle file-based data sources"""
    
    # Validate params using schema
    schema = SourceFileParams(**params)
    
    file_path = Path(schema.file_path)
    
    # Check file exists
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    # Get file info
    file_size = file_path.stat().st_size
    file_format = schema.file_format
    
    # Determine MIME type
    mime_type, _ = mimetypes.guess_type(str(file_path))
    if not mime_type:
        mime_type = _get_mime_type_from_format(file_format)
    
    await bus.emit({
        "type": "log",
        "runId": run_id,
        "at": _iso_now(),
        "level": "info",
        "message": f"Reading {file_format.upper()} file: {file_path.name} ({_format_bytes(file_size)})",
        "nodeId": node_id
    })
    
    # Read file based on format
    if file_format == "csv":
        data, row_count, schema_info = await _read_csv(file_path, schema)
    elif file_format == "parquet":
        data, row_count, schema_info = await _read_parquet(file_path, schema)
    elif file_format == "json":
        data, row_count, schema_info = await _read_json(file_path, schema)
    elif file_format == "excel":
        data, row_count, schema_info = await _read_excel(file_path, schema)
    elif file_format == "txt":
        data, row_count, schema_info = await _read_text(file_path, schema)
    elif file_format == "pdf":
        data, row_count, schema_info = await _read_pdf(file_path, schema)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")
    
    # Write processed data to temporary output file
    output_path = _get_output_path(node_id, file_format)
    await _write_output(data, output_path, file_format)
    
    # Calculate content hash
    content_hash = _calculate_file_hash(output_path)
    
    # Calculate params hash
    params_hash = hashlib.sha256(
        json.dumps(params, sort_keys=True).encode()
    ).hexdigest()
    
    # Create metadata
    metadata = FileMetadata(
        file_path=str(output_path),
        file_type=file_format,
        mime_type=mime_type,
        size_bytes=output_path.stat().st_size,
        schema=schema_info,
        row_count=row_count,
        access_method="local",
        content_hash=content_hash,
        node_id=node_id,
        params_hash=params_hash,
        estimated_memory_mb=file_size / (1024 * 1024)
    )
    
    return NodeOutput(
        status="succeeded",
        metadata=metadata,
        execution_time_ms=0.0  # Will be set by caller
    )


# ============================================================================
# FILE READERS
# ============================================================================

async def _read_csv(
    file_path: Path,
    schema: SourceFileParams
) -> tuple[pd.DataFrame, int, Dict[str, Any]]:
    """Read CSV file"""
    
    df = pd.read_csv(
        file_path,
        delimiter=schema.delimiter or ",",
        encoding=schema.encoding,
        nrows=schema.sample_size
    )
    
    schema_info = {
        "columns": list(df.columns),
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
        "format": "table"
    }
    
    return df, len(df), schema_info


async def _read_parquet(
    file_path: Path,
    schema: SourceFileParams
) -> tuple[pd.DataFrame, int, Dict[str, Any]]:
    """Read Parquet file"""
    
    # Read with PyArrow for better performance
    table = pq.read_table(file_path)
    df = table.to_pandas()
    
    if schema.sample_size:
        df = df.head(schema.sample_size)
    
    schema_info = {
        "columns": list(df.columns),
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
        "format": "table",
        "parquet_schema": str(table.schema)
    }
    
    return df, len(df), schema_info


async def _read_json(
    file_path: Path,
    schema: SourceFileParams
) -> tuple[Any, int, Dict[str, Any]]:
    """Read JSON file"""
    
    with open(file_path, 'r', encoding=schema.encoding) as f:
        data = json.load(f)
    
    # Try to convert to DataFrame if it's a list of records
    if isinstance(data, list):
        df = pd.DataFrame(data)
        if schema.sample_size:
            df = df.head(schema.sample_size)
        
        schema_info = {
            "columns": list(df.columns) if not df.empty else [],
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()} if not df.empty else {},
            "format": "table",
            "json_type": "array"
        }
        return df, len(df), schema_info
    else:
        # Single JSON object - keep as-is
        schema_info = {
            "format": "json",
            "json_type": "object",
            "keys": list(data.keys()) if isinstance(data, dict) else []
        }
        return data, 1, schema_info


async def _read_excel(
    file_path: Path,
    schema: SourceFileParams
) -> tuple[pd.DataFrame, int, Dict[str, Any]]:
    """Read Excel file"""
    
    df = pd.read_excel(
        file_path,
        sheet_name=schema.sheet_name or 0,
        nrows=schema.sample_size
    )
    
    schema_info = {
        "columns": list(df.columns),
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
        "format": "table",
        "sheet_name": schema.sheet_name or "Sheet1"
    }
    
    return df, len(df), schema_info


async def _read_text(
    file_path: Path,
    schema: SourceFileParams
) -> tuple[str, int, Dict[str, Any]]:
    """Read plain text file"""
    
    with open(file_path, 'r', encoding=schema.encoding) as f:
        text = f.read()
    
    if schema.sample_size:
        lines = text.split('\n')
        text = '\n'.join(lines[:schema.sample_size])
    
    line_count = text.count('\n') + 1
    
    schema_info = {
        "format": "text",
        "encoding": schema.encoding,
        "line_count": line_count,
        "char_count": len(text)
    }
    
    return text, line_count, schema_info


async def _read_pdf(
    file_path: Path,
    schema: SourceFileParams
) -> tuple[Dict[str, Any], int, Dict[str, Any]]:
    """Read PDF file - extract text and metadata"""
    
    if not HAS_PDF:
        raise ImportError("PDF support requires PyPDF2 and pdfplumber. Install with: pip install PyPDF2 pdfplumber")
    
    pdf_data = {
        "text": [],
        "metadata": {},
        "pages": []
    }
    
    # Extract text using pdfplumber (better text extraction)
    with pdfplumber.open(file_path) as pdf:
        num_pages = len(pdf.pages)
        max_pages = schema.sample_size or num_pages
        
        for i, page in enumerate(pdf.pages[:max_pages]):
            page_text = page.extract_text() or ""
            pdf_data["text"].append(page_text)
            
            # Extract tables if any
            tables = page.extract_tables()
            
            pdf_data["pages"].append({
                "page_number": i + 1,
                "text": page_text,
                "has_tables": len(tables) > 0,
                "table_count": len(tables),
                "tables": tables if tables else []
            })
    
    # Extract metadata using PyPDF2
    with open(file_path, 'rb') as f:
        pdf_reader = PyPDF2.PdfReader(f)
        metadata = pdf_reader.metadata
        
        if metadata:
            pdf_data["metadata"] = {
                "title": metadata.get("/Title", ""),
                "author": metadata.get("/Author", ""),
                "subject": metadata.get("/Subject", ""),
                "creator": metadata.get("/Creator", ""),
                "producer": metadata.get("/Producer", ""),
                "creation_date": str(metadata.get("/CreationDate", "")),
            }
    
    # Combine all text
    full_text = "\n\n".join(pdf_data["text"])
    
    schema_info = {
        "format": "pdf",
        "page_count": len(pdf_data["pages"]),
        "total_pages": num_pages,
        "has_tables": any(p["has_tables"] for p in pdf_data["pages"]),
        "metadata": pdf_data["metadata"]
    }
    
    return pdf_data, len(pdf_data["pages"]), schema_info


# ============================================================================
# DATABASE SOURCE HANDLER
# ============================================================================

async def _handle_database_source(
    node_id: str,
    params: Dict[str, Any],
    bus: RunEventBus,
    run_id: str
) -> NodeOutput:
    """Handle database sources"""
    
    if not HAS_DATABASE:
        raise ImportError("Database support requires sqlalchemy. Install with: pip install sqlalchemy")
    
    schema = SourceDatabaseParams(**params)
    
    # Get connection string (from params or reference)
    conn_string = schema.connection_string
    if not conn_string and schema.connection_ref:
        # TODO: Implement connection reference lookup from secrets/config
        raise NotImplementedError("Connection references not yet implemented")
    
    await bus.emit({
        "type": "log",
        "runId": run_id,
        "at": _iso_now(),
        "level": "info",
        "message": f"Connecting to database...",
        "nodeId": node_id
    })
    
    # Create engine
    engine = sqlalchemy.create_engine(conn_string)
    
    try:
        # Execute query or read table
        if schema.query:
            query = schema.query
            if schema.limit:
                # Simple limit injection (be careful in production!)
                query = f"{query.rstrip(';')} LIMIT {schema.limit}"
            df = pd.read_sql(query, engine)
        elif schema.table_name:
            df = pd.read_sql_table(
                schema.table_name,
                engine,
                chunksize=schema.limit
            )
            if schema.limit:
                df = df.head(schema.limit)
        else:
            raise ValueError("Either query or table_name must be specified")
        
        # Write to output file
        output_path = _get_output_path(node_id, "parquet")
        df.to_parquet(output_path, index=False)
        
        # Calculate hashes
        content_hash = _calculate_file_hash(output_path)
        params_hash = hashlib.sha256(
            json.dumps(params, sort_keys=True).encode()
        ).hexdigest()
        
        schema_info = {
            "columns": list(df.columns),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "format": "table",
            "source": "database"
        }
        
        metadata = FileMetadata(
            file_path=str(output_path),
            file_type="parquet",
            mime_type="application/vnd.apache.parquet",
            size_bytes=output_path.stat().st_size,
            schema=schema_info,
            row_count=len(df),
            access_method="local",
            content_hash=content_hash,
            node_id=node_id,
            params_hash=params_hash,
            estimated_memory_mb=df.memory_usage(deep=True).sum() / (1024 * 1024)
        )
        
        return NodeOutput(
            status="succeeded",
            metadata=metadata,
            execution_time_ms=0.0
        )
        
    finally:
        engine.dispose()


# ============================================================================
# API SOURCE HANDLER
# ============================================================================

async def _handle_api_source(
    node_id: str,
    params: Dict[str, Any],
    bus: RunEventBus,
    run_id: str
) -> NodeOutput:
    """Handle API sources"""
    
    schema = SourceAPIParams(**params)
    
    await bus.emit({
        "type": "log",
        "runId": run_id,
        "at": _iso_now(),
        "level": "info",
        "message": f"Calling API: {schema.method} {schema.url}",
        "nodeId": node_id
    })
    
    # Prepare headers
    headers = dict(schema.headers)
    
    # Add authentication
    if schema.auth_type == "bearer" and schema.auth_token_ref:
        # TODO: Lookup token from secrets
        token = os.getenv(schema.auth_token_ref, "")
        headers["Authorization"] = f"Bearer {token}"
    elif schema.auth_type == "api_key" and schema.auth_token_ref:
        # TODO: Lookup API key from secrets
        api_key = os.getenv(schema.auth_token_ref, "")
        headers["X-API-Key"] = api_key
    
    # Make request
    async with httpx.AsyncClient() as client:
        response = await client.request(
            method=schema.method,
            url=schema.url,
            headers=headers,
            json=schema.body if schema.body else None,
            timeout=schema.timeout_seconds
        )
        response.raise_for_status()
        
        # Parse response
        content_type = response.headers.get("content-type", "")
        
        if "application/json" in content_type:
            data = response.json()
            file_format = "json"
        else:
            data = response.text
            file_format = "txt"
    
    # Write to output file
    output_path = _get_output_path(node_id, file_format)
    
    if file_format == "json":
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2)
        
        # Try to extract row count
        row_count = len(data) if isinstance(data, list) else 1
        schema_info = {
            "format": "json",
            "json_type": "array" if isinstance(data, list) else "object"
        }
    else:
        with open(output_path, 'w') as f:
            f.write(data)
        
        row_count = data.count('\n') + 1
        schema_info = {
            "format": "text",
            "source": "api"
        }
    
    # Calculate hashes
    content_hash = _calculate_file_hash(output_path)
    params_hash = hashlib.sha256(
        json.dumps(params, sort_keys=True).encode()
    ).hexdigest()
    
    metadata = FileMetadata(
        file_path=str(output_path),
        file_type=file_format,
        mime_type=content_type,
        size_bytes=output_path.stat().st_size,
        schema=schema_info,
        row_count=row_count,
        access_method="local",
        content_hash=content_hash,
        node_id=node_id,
        params_hash=params_hash
    )
    
    return NodeOutput(
        status="succeeded",
        metadata=metadata,
        execution_time_ms=0.0
    )


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def _get_output_path(node_id: str, file_format: str) -> Path:
    """Generate output file path for processed data"""
    output_dir = Path("/tmp/pipeline_data")  # TODO: Make configurable
    output_dir.mkdir(exist_ok=True, parents=True)
    
    return output_dir / f"{node_id}_output.{file_format}"


async def _write_output(data: Any, output_path: Path, file_format: str):
    """Write processed data to output file"""
    
    if file_format in ["csv", "parquet", "excel"]:
        # Data should be DataFrame
        if isinstance(data, pd.DataFrame):
            if file_format == "csv":
                data.to_csv(output_path, index=False)
            elif file_format == "parquet":
                data.to_parquet(output_path, index=False)
            elif file_format == "excel":
                data.to_excel(output_path, index=False)
    elif file_format == "json":
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2, default=str)
    elif file_format == "txt":
        with open(output_path, 'w') as f:
            f.write(str(data))
    elif file_format == "pdf":
        # For PDF, write the extracted data as JSON
        with open(output_path.with_suffix('.json'), 'w') as f:
            json.dump(data, f, indent=2, default=str)


def _calculate_file_hash(file_path: Path) -> str:
    """Calculate SHA256 hash of file"""
    sha256_hash = hashlib.sha256()
    
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    
    return sha256_hash.hexdigest()


def _get_mime_type_from_format(file_format: str) -> str:
    """Get MIME type from file format"""
    mime_types = {
        "csv": "text/csv",
        "parquet": "application/vnd.apache.parquet",
        "json": "application/json",
        "excel": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "txt": "text/plain",
        "pdf": "application/pdf"
    }
    return mime_types.get(file_format, "application/octet-stream")


def _format_bytes(bytes_size: int) -> str:
    """Format bytes to human-readable string"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.1f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.1f} TB"


def _iso_now() -> str:
    """Get current time in ISO format"""
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()