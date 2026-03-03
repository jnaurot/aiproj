import hashlib
import io
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx
import pandas as pd
import pyarrow.parquet as pq

from ..runner.events import RunEventBus
from ..runner.metadata import GraphContext, NodeOutput, FileMetadata
from ..runner.schemas import SourceAPIParams, SourceDatabaseParams, SourceFileParams, normalize_source_params_frontend

try:
    import PyPDF2
    import pdfplumber

    HAS_PDF = True
except ImportError:
    HAS_PDF = False

try:
    import sqlalchemy

    HAS_DATABASE = True
except ImportError:
    HAS_DATABASE = False


def _iso_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _canon_json_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str).encode("utf-8")


def _mode_to_file_type(mode: str) -> str:
    if mode == "json":
        return "json"
    if mode == "text":
        return "txt"
    if mode == "binary":
        return "binary"
    return "csv"


def _mode_to_mime(mode: str) -> str:
    if mode == "json":
        return "application/json"
    if mode == "text":
        return "text/plain; charset=utf-8"
    if mode == "binary":
        return "application/octet-stream"
    return "text/csv"


def _file_format_mime(file_format: str) -> str:
    ff = str(file_format or "").strip().lower()
    mapping = {
        "csv": "text/csv",
        "tsv": "text/tab-separated-values",
        "parquet": "application/vnd.apache.parquet",
        "json": "application/json",
        "excel": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "txt": "text/plain; charset=utf-8",
        "pdf": "application/pdf",
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "png": "image/png",
        "webp": "image/webp",
        "gif": "image/gif",
        "svg": "image/svg+xml",
        "tif": "image/tiff",
        "tiff": "image/tiff",
        "mp3": "audio/mpeg",
        "wav": "audio/wav",
        "flac": "audio/flac",
        "ogg": "audio/ogg",
        "m4a": "audio/mp4",
        "aac": "audio/aac",
        "mp4": "video/mp4",
        "mov": "video/quicktime",
        "webm": "video/webm",
    }
    return mapping.get(ff, "application/octet-stream")


def _canonical_table_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    cols = sorted({str(k) for row in rows for k in row.keys()})
    out: list[dict[str, Any]] = []
    for row in rows:
        out.append({col: row.get(col) for col in cols})
    return out


def _payload_bytes_for_mode(data: Any, mode: str) -> bytes:
    if mode == "binary":
        if isinstance(data, bytes):
            return data
        if isinstance(data, str):
            return data.encode("utf-8")
        return _canon_json_bytes(data)
    if mode == "text":
        return str(data if data is not None else "").encode("utf-8")
    if mode == "json":
        return _canon_json_bytes(data)
    rows = _canonical_table_rows(data if isinstance(data, list) else [])
    if not rows:
        return b""
    df = pd.DataFrame(rows)
    return df.to_csv(index=False, lineterminator="\n").encode("utf-8")


def _metadata_for_output(
    *,
    graph_id: str,
    node_id: str,
    source_kind: str,
    output_mode: str,
    data: Any,
    params: Dict[str, Any],
    mime_override: Optional[str] = None,
) -> FileMetadata:
    payload_bytes = _payload_bytes_for_mode(data, output_mode)
    return FileMetadata(
        file_path=f"artifact://{graph_id}/{node_id}/{source_kind}",
        file_type=_mode_to_file_type(output_mode),
        mime_type=str(mime_override or _mode_to_mime(output_mode)),
        size_bytes=len(payload_bytes),
        data_schema={"source_kind": source_kind, "output_mode": output_mode},
        row_count=(len(data) if isinstance(data, list) else None),
        access_method="local",
        content_hash=_sha256_bytes(payload_bytes),
        node_id=node_id,
        params_hash=hashlib.sha256(
            json.dumps(params, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
    )


def _get_output_mode(params: Dict[str, Any], default_mode: str) -> str:
    output_mode = params.get("output_mode")
    if not isinstance(output_mode, str):
        output = params.get("output")
        if isinstance(output, dict):
            output_mode = output.get("mode")
    mode = str(output_mode or default_mode)
    if mode not in {"table", "text", "json", "binary"}:
        return default_mode
    return mode


def _source_out_mode_from_node(node: Dict[str, Any]) -> Optional[str]:
    out = (((node or {}).get("data", {}) or {}).get("ports", {}) or {}).get("out")
    if isinstance(out, str) and out in {"table", "text", "json", "binary"}:
        return out
    return None


def _resolve_file_path(rel_path: str, filename: str) -> Path:
    base = Path(str(rel_path or ".")).expanduser()
    leaf = Path(str(filename or "")).expanduser()
    if leaf.is_absolute():
        return leaf.resolve()
    return (base / leaf).resolve()


def _sorted_string_map(value: Optional[Dict[str, Any]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in (value or {}).items():
        out[str(k)] = str(v if v is not None else "")
    return {k: out[k] for k in sorted(out.keys())}


def _merge_query_into_url(url: str, query: Optional[Dict[str, Any]]) -> str:
    if not url:
        return url
    split = urlsplit(url)
    url_query = {k: v for k, v in parse_qsl(split.query, keep_blank_values=True)}
    editor_query = _sorted_string_map(query)
    merged = {**url_query, **editor_query}
    ordered = [(k, merged[k]) for k in sorted(merged.keys())]
    return urlunsplit((split.scheme, split.netloc, split.path, urlencode(ordered, doseq=False), split.fragment))


async def exec_source(
    run_id: str,
    node: Dict[str, Any],
    context: GraphContext,
    upstream_artifact_ids: Optional[list[str]] = None,
) -> NodeOutput:
    upstream_artifact_ids = upstream_artifact_ids or []
    start_time = time.time()
    node_id = node["id"]
    raw_params = dict(node.get("data", {}).get("params", {}) or {})
    params = normalize_source_params_frontend(raw_params)
    source_type = (node.get("data", {}).get("sourceKind") or params.get("source_type") or "file")
    params["source_type"] = source_type

    try:
        if source_type == "file":
            output = await _handle_file_source(
                node_id,
                params,
                context.bus,
                run_id,
                context.graph_id,
                artifact_store=context.artifact_store,
                forced_output_mode=_source_out_mode_from_node(node),
            )
        elif source_type == "database":
            output = await _handle_database_source(node_id, params, context.bus, run_id, context.graph_id)
        elif source_type == "api":
            output = await _handle_api_source(node_id, params, context.bus, run_id, context.graph_id)
        else:
            raise ValueError(f"Unknown source_type: {source_type}")
        output.execution_time_ms = (time.time() - start_time) * 1000
        return output
    except Exception as exc:
        return NodeOutput(
            status="failed",
            metadata=None,
            execution_time_ms=(time.time() - start_time) * 1000,
            error=str(exc),
        )


async def _handle_file_source(
    node_id: str,
    params: Dict[str, Any],
    bus: RunEventBus,
    run_id: str,
    graph_id: str,
    artifact_store: Any,
    forced_output_mode: Optional[str] = None,
) -> NodeOutput:
    if isinstance(params.get("file_path"), str) and not params.get("filename"):
        legacy = Path(str(params.get("file_path")))
        params.setdefault("rel_path", str(legacy.parent) if str(legacy.parent) not in {"", "."} else ".")
        params.setdefault("filename", legacy.name or str(legacy))
    schema = SourceFileParams.model_validate(params)
    output_mode = forced_output_mode or _get_output_mode(params, _default_file_output_mode(schema.file_format))

    file_bytes: Optional[bytes] = None
    file_path: Optional[Path] = None
    if schema.snapshot_id:
        sid = str(schema.snapshot_id).strip().lower()
        if not sid:
            raise ValueError("snapshot_id is empty")
        if not await artifact_store.exists(sid):
            raise FileNotFoundError(f"Snapshot not found: {sid}")
        file_bytes = await artifact_store.read(sid)
        await bus.emit(
            {
                "type": "log",
                "runId": run_id,
                "at": _iso_now(),
                "level": "info",
                "message": f"Using snapshotId={sid}",
                "nodeId": node_id,
            }
        )
    else:
        file_path = _resolve_file_path(schema.rel_path or ".", schema.filename or "")
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        await bus.emit(
            {
                "type": "log",
                "runId": run_id,
                "at": _iso_now(),
                "level": "info",
                "message": f"Reading file {schema.filename}",
                "nodeId": node_id,
            }
        )

    rows: list[dict[str, Any]] | None = None
    text_data: str | None = None
    json_data: Any = None
    binary_data: bytes | None = None

    if schema.file_format in {"csv", "tsv"}:
        csv_input: Any = io.BytesIO(file_bytes) if file_bytes is not None else file_path
        df = pd.read_csv(
            csv_input,
            delimiter=schema.delimiter or ("\t" if schema.file_format == "tsv" else ","),
            encoding=schema.encoding,
        )
        rows = df.to_dict(orient="records")
    elif schema.file_format == "parquet":
        parquet_input: Any = io.BytesIO(file_bytes) if file_bytes is not None else file_path
        df = pq.read_table(parquet_input).to_pandas()
        rows = df.to_dict(orient="records")
    elif schema.file_format == "json":
        raw_json = (
            (file_bytes or b"").decode(schema.encoding, errors="replace")
            if file_bytes is not None
            else Path(file_path).read_text(encoding=schema.encoding)
        )
        json_data = json.loads(raw_json)
        if isinstance(json_data, list):
            rows = json_data
    elif schema.file_format == "excel":
        excel_input: Any = io.BytesIO(file_bytes) if file_bytes is not None else file_path
        df = pd.read_excel(excel_input, sheet_name=schema.sheet_name or 0)
        rows = df.to_dict(orient="records")
    elif schema.file_format == "txt":
        text_data = (
            (file_bytes or b"").decode(schema.encoding, errors="replace")
            if file_bytes is not None
            else Path(file_path).read_text(encoding=schema.encoding)
        )
    elif schema.file_format == "pdf":
        if not HAS_PDF:
            raise ImportError("PDF support requires PyPDF2 and pdfplumber")
        pdf_input: Any = io.BytesIO(file_bytes) if file_bytes is not None else file_path
        with pdfplumber.open(pdf_input) as pdf:
            text_data = "\n\n".join((page.extract_text() or "") for page in pdf.pages)
    else:
        binary_data = file_bytes if file_bytes is not None else Path(file_path).read_bytes()

    if output_mode == "table":
        if rows is not None:
            data = _canonical_table_rows(rows)
        elif isinstance(json_data, dict):
            data = [json_data]
        elif text_data is not None:
            data = [{"text": text_data}]
        elif binary_data is not None:
            data = [{"binary_hex": binary_data.hex()}]
        else:
            data = []
    elif output_mode == "json":
        if json_data is not None:
            data = json_data
        elif rows is not None:
            data = rows
        elif text_data is not None:
            data = {"text": text_data}
        else:
            data = {"binary_b64": (binary_data or b"").hex()}
    elif output_mode == "binary":
        if binary_data is not None:
            data = binary_data
        elif text_data is not None:
            data = text_data.encode("utf-8")
        elif json_data is not None:
            data = _canon_json_bytes(json_data)
        else:
            data = _payload_bytes_for_mode(rows or [], "table")
    else:
        if text_data is not None:
            data = text_data
        elif rows is not None:
            data = pd.DataFrame(rows).to_csv(index=False, lineterminator="\n")
        elif json_data is not None:
            data = json.dumps(json_data, sort_keys=True, separators=(",", ":"))
        else:
            data = (binary_data or b"").decode("utf-8", errors="replace")

    metadata = _metadata_for_output(
        graph_id=graph_id,
        node_id=node_id,
        source_kind="file",
        output_mode=output_mode,
        data=data,
        params=params,
        mime_override=_file_format_mime(schema.file_format),
    )
    return NodeOutput(status="succeeded", data=data, metadata=metadata, execution_time_ms=0.0)


async def _handle_database_source(
    node_id: str,
    params: Dict[str, Any],
    bus: RunEventBus,
    run_id: str,
    graph_id: str,
) -> NodeOutput:
    if not HAS_DATABASE:
        raise ImportError("Database support requires sqlalchemy")
    schema = SourceDatabaseParams.model_validate(params)
    output_mode = _get_output_mode(params, "table")

    conn_string = schema.connection_string
    if not conn_string and schema.connection_ref:
        raise NotImplementedError("Connection references not implemented")
    if not conn_string:
        raise ValueError("connection_string or connection_ref required")

    engine = sqlalchemy.create_engine(conn_string)
    try:
        if schema.query:
            query = schema.query
            if schema.limit:
                query = f"{query.rstrip(';')} LIMIT {schema.limit}"
            df = pd.read_sql(query, engine)
        elif schema.table_name:
            query = f"SELECT * FROM {schema.table_name}"
            if schema.limit:
                query += f" LIMIT {schema.limit}"
            df = pd.read_sql(query, engine)
        else:
            raise ValueError("Either query or table_name required")

        rows = df.to_dict(orient="records")
        if output_mode == "table":
            data: Any = _canonical_table_rows(rows)
        elif output_mode == "json":
            data = rows
        elif output_mode == "binary":
            data = df.to_csv(index=False, lineterminator="\n").encode("utf-8")
        else:
            data = df.to_csv(index=False, lineterminator="\n")

        metadata = _metadata_for_output(
            graph_id=graph_id,
            node_id=node_id,
            source_kind="database",
            output_mode=output_mode,
            data=data,
            params=params,
        )
        return NodeOutput(status="succeeded", data=data, metadata=metadata, execution_time_ms=0.0)
    finally:
        engine.dispose()


async def _handle_api_source(
    node_id: str,
    params: Dict[str, Any],
    bus: RunEventBus,
    run_id: str,
    graph_id: str,
) -> NodeOutput:
    schema = SourceAPIParams.model_validate(params)
    output_mode = _get_output_mode(params, "json")

    headers = {str(k): str(v) for k, v in dict(schema.headers).items()}
    headers = {k: v for k, v in headers.items() if k.lower() != "content-type"}
    if schema.content_type:
        headers["Content-Type"] = str(schema.content_type)
    if schema.auth_type == "bearer" and schema.auth_token_ref:
        headers["Authorization"] = f"Bearer {os.getenv(schema.auth_token_ref, '')}"
    elif schema.auth_type == "basic" and schema.auth_token_ref:
        raw = os.getenv(schema.auth_token_ref, "")
        import base64

        headers["Authorization"] = f"Basic {base64.b64encode(raw.encode('utf-8')).decode('ascii')}"
    elif schema.auth_type == "api_key" and schema.auth_token_ref:
        headers["X-API-Key"] = os.getenv(schema.auth_token_ref, "")

    final_url = _merge_query_into_url(schema.url, schema.query)
    request_kwargs: Dict[str, Any] = {
        "method": schema.method,
        "url": final_url,
        "headers": headers,
        "timeout": schema.timeout_seconds,
    }

    if schema.body_mode == "json":
        request_kwargs["json"] = schema.body_json or {}
    elif schema.body_mode == "form":
        request_kwargs["data"] = _sorted_string_map(schema.body_form)
    elif schema.body_mode == "multipart":
        form = _sorted_string_map(schema.body_form)
        request_kwargs["files"] = [(k, (None, v)) for k, v in form.items()]
    elif schema.body_mode == "raw":
        request_kwargs["content"] = (schema.body_raw or "").encode("utf-8")

    async with httpx.AsyncClient() as client:
        response = await client.request(**request_kwargs)
        response.raise_for_status()

    content_type = response.headers.get("content-type", "")
    is_json = "application/json" in content_type
    json_payload: Any = response.json() if is_json else None
    text_payload = response.text if not is_json else json.dumps(json_payload, sort_keys=True, separators=(",", ":"))

    if output_mode == "table":
        if isinstance(json_payload, list):
            data: Any = _canonical_table_rows(json_payload)
        elif isinstance(json_payload, dict):
            data = [json_payload]
        else:
            data = [{"text": line} for line in text_payload.splitlines() if line.strip()]
    elif output_mode == "json":
        data = json_payload if json_payload is not None else {"text": text_payload}
    elif output_mode == "binary":
        data = response.content
    else:
        data = text_payload

    metadata = _metadata_for_output(
        graph_id=graph_id,
        node_id=node_id,
        source_kind="api",
        output_mode=output_mode,
        data=data,
        params=params,
    )
    return NodeOutput(status="succeeded", data=data, metadata=metadata, execution_time_ms=0.0)


def _default_file_output_mode(file_format: str) -> str:
    if file_format in {"csv", "tsv", "parquet", "excel"}:
        return "table"
    if file_format == "json":
        return "json"
    if file_format in {"txt", "pdf"}:
        return "text"
    if file_format in {"jpg", "jpeg", "png", "webp", "gif", "svg", "tif", "tiff"}:
        return "binary"
    if file_format in {"mp3", "wav", "flac", "ogg", "m4a", "aac"}:
        return "binary"
    if file_format in {"mp4", "mov", "webm"}:
        return "binary"
    return "binary"
