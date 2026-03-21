import asyncio
import array
import csv
import hashlib
import io
import json
import logging
import os
import random
import re
import time
import wave
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pandas.api.types import (
    is_bool_dtype,
    is_datetime64_any_dtype,
    is_float_dtype,
    is_integer_dtype,
    is_object_dtype,
    is_string_dtype,
)

from ..runner.events import RunEventBus
from ..runner.metadata import GraphContext, NodeOutput, FileMetadata
from ..runner.schemas import (
    SourceAPIParams,
    SourceDatabaseParams,
    SourceFileParams,
    SourceObjectStoreParams,
    SourceWarehouseParams,
    normalize_source_params_frontend,
)
from ..runner.schema_infer import infer_typed_schema_from_sample_profile

logger = logging.getLogger(__name__)

_SQL_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

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

try:
    from PIL import Image, ExifTags

    HAS_PIL = True
except ImportError:
    HAS_PIL = False


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


def _csv_sample_text(
    file_bytes: Optional[bytes],
    file_path: Optional[Path],
    encoding: str,
    max_bytes: int = 8192,
) -> str:
    if isinstance(file_bytes, (bytes, bytearray)):
        return bytes(file_bytes[:max_bytes]).decode(encoding, errors="replace")
    if isinstance(file_path, Path) and file_path.exists():
        with file_path.open("rb") as fh:
            return fh.read(max_bytes).decode(encoding, errors="replace")
    return ""


def _csv_heuristic_has_header(sample_text: str, delimiter: str) -> Optional[bool]:
    lines = [ln for ln in sample_text.splitlines() if str(ln).strip()]
    if len(lines) < 2:
        return None
    try:
        reader = csv.reader(lines[:2], delimiter=delimiter)
        first = next(reader, [])
        second = next(reader, [])
    except Exception:
        return None
    if not first or not second or len(first) != len(second):
        return None

    def _looks_numeric(cell: str) -> bool:
        text = str(cell or "").strip()
        if text == "":
            return False
        try:
            float(text)
            return True
        except Exception:
            return False

    first_numeric = sum(1 for cell in first if _looks_numeric(cell))
    second_numeric = sum(1 for cell in second if _looks_numeric(cell))
    if first_numeric == 0 and second_numeric > 0:
        return True
    if first_numeric == len(first) and second_numeric == len(second):
        return False
    return None


def _detect_csv_has_header(
    file_bytes: Optional[bytes],
    file_path: Optional[Path],
    encoding: str,
    delimiter: str,
    explicit: Optional[bool],
) -> bool:
    if explicit is not None:
        return bool(explicit)
    sample_text = _csv_sample_text(file_bytes, file_path, encoding)
    if not sample_text.strip():
        return True
    try:
        sniff = bool(csv.Sniffer().has_header(sample_text))
    except Exception:
        sniff = False
    guess = _csv_heuristic_has_header(sample_text, delimiter)
    if guess is not None:
        return bool(guess)
    if sniff:
        return True
    return True


def _excel_heuristic_has_header(df: pd.DataFrame) -> Optional[bool]:
    try:
        if df.shape[0] < 2:
            return None
        first = list(df.iloc[0].values)
        second = list(df.iloc[1].values)
    except Exception:
        return None

    def _looks_number(value: Any) -> bool:
        if value is None:
            return False
        text = str(value).strip()
        if text == "":
            return False
        try:
            float(text)
            return True
        except Exception:
            return False

    first_numeric = sum(1 for item in first if _looks_number(item))
    second_numeric = sum(1 for item in second if _looks_number(item))
    if first_numeric == 0 and second_numeric > 0:
        return True
    if first_numeric == len(first) and second_numeric == len(second):
        return False
    return None


def _count_malformed_csv_rows(
    *,
    file_bytes: Optional[bytes],
    file_path: Optional[Path],
    encoding: str,
    delimiter: str,
    has_header: bool,
) -> Optional[int]:
    sample_text = _csv_sample_text(file_bytes, file_path, encoding, max_bytes=5 * 1024 * 1024)
    if not sample_text.strip():
        return 0
    try:
        rows = list(csv.reader(sample_text.splitlines(), delimiter=delimiter))
    except Exception:
        return None
    rows = [row for row in rows if row]
    if not rows:
        return 0
    start_idx = 1 if has_header and len(rows) > 1 else 0
    reference_idx = 0 if not has_header else 0
    expected_cols = len(rows[reference_idx]) if rows[reference_idx] else 0
    if expected_cols <= 0:
        return None
    malformed = 0
    for row in rows[start_idx:]:
        if len(row) != expected_cols:
            malformed += 1
    return malformed


def _canonical_table_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    seen: set[str] = set()
    cols: list[str] = []
    for row in rows:
        for k in row.keys():
            sk = str(k)
            if sk in seen:
                continue
            seen.add(sk)
            cols.append(sk)
    out: list[dict[str, Any]] = []
    for row in rows:
        out.append({col: row.get(col) for col in cols})
    return out


def _canonical_table_type_from_python(value: Any) -> str:
    if value is None:
        return "unknown"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int) and not isinstance(value, bool):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "string"
    if hasattr(value, "isoformat"):
        return "datetime"
    if isinstance(value, (dict, list)):
        return "json"
    return "unknown"


def _merge_table_types(existing: str, incoming: str) -> str:
    a = str(existing or "unknown").strip().lower()
    b = str(incoming or "unknown").strip().lower()
    if a == "unknown":
        return b
    if b == "unknown" or a == b:
        return a
    if {a, b} == {"int", "float"}:
        return "float"
    return "unknown"


def _table_type_from_series(series: pd.Series) -> str:
    dtype = series.dtype
    if is_bool_dtype(dtype):
        return "bool"
    if is_integer_dtype(dtype):
        return "int"
    if is_float_dtype(dtype):
        return "float"
    if is_datetime64_any_dtype(dtype):
        return "datetime"
    if is_string_dtype(dtype) and not is_object_dtype(dtype):
        return "string"
    non_null = series.dropna()
    if non_null.empty:
        return "unknown"
    inferred = "unknown"
    # Bound sampling cost for large columns while keeping deterministic order.
    for value in non_null.head(256):
        inferred = _merge_table_types(inferred, _canonical_table_type_from_python(value))
        if inferred == "unknown":
            break
    return inferred


def _infer_table_columns_from_dataframe(df: pd.DataFrame) -> list[dict[str, str]]:
    cols: list[dict[str, str]] = []
    for col in list(df.columns):
        name = str(col)
        try:
            col_type = _table_type_from_series(df[col])
        except Exception:
            col_type = "unknown"
        cols.append({"name": name, "type": col_type})
    return cols


def _arrow_type_label(dtype: Any) -> str:
    try:
        if pa.types.is_decimal(dtype):
            precision = int(getattr(dtype, "precision", 0) or 0)
            scale = int(getattr(dtype, "scale", 0) or 0)
            return f"decimal({precision},{scale})"
        if pa.types.is_timestamp(dtype):
            unit = str(getattr(dtype, "unit", "us") or "us")
            tz = getattr(dtype, "tz", None)
            return f"timestamp[{unit},{tz}]" if tz else f"timestamp[{unit}]"
        if pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
            value_type = _arrow_type_label(getattr(dtype, "value_type", None))
            return f"list<{value_type}>"
        if pa.types.is_struct(dtype):
            fields = []
            for field in list(getattr(dtype, "fields", []) or []):
                fields.append(f"{field.name}:{_arrow_type_label(field.type)}")
            return f"struct<{','.join(fields)}>"
    except Exception:
        pass
    text = str(dtype).strip().lower()
    return text or "unknown"


def _table_columns_from_arrow_schema(schema: Any) -> list[dict[str, str]]:
    cols: list[dict[str, str]] = []
    if schema is None:
        return cols
    try:
        fields = list(schema)
    except Exception:
        fields = []
    for field in fields:
        try:
            name = str(getattr(field, "name", "") or "").strip()
            if not name:
                continue
            dtype = getattr(field, "type", None)
            cols.append({"name": name, "type": _arrow_type_label(dtype)})
        except Exception:
            continue
    return cols


def _infer_table_columns_from_rows(rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    if not rows:
        return []
    seen: set[str] = set()
    ordered: list[str] = []
    inferred: Dict[str, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key, value in row.items():
            col = str(key)
            if col not in seen:
                seen.add(col)
                ordered.append(col)
            incoming = _canonical_table_type_from_python(value)
            prev = inferred.get(col, "unknown")
            inferred[col] = _merge_table_types(prev, incoming)
    return [{"name": col, "type": inferred.get(col, "unknown")} for col in ordered]


def _log_source_inference(node_id: str, source_kind: str, columns: Optional[list[dict[str, str]]]) -> None:
    if not isinstance(columns, list):
        return
    compact = [f"{str(c.get('name') or '').strip()}:{str(c.get('type') or 'unknown').strip() or 'unknown'}" for c in columns]
    print(f"[source-infer] nodeId={str(node_id)} sourceKind={str(source_kind)} columns={compact}")
    logger.info(
        "[source-infer] nodeId=%s sourceKind=%s columns=%s",
        str(node_id),
        str(source_kind),
        compact,
    )


def _table_rows_from_json_array(items: list[Any]) -> tuple[list[dict[str, Any]], str]:
    if all(isinstance(item, dict) for item in items):
        seen: set[str] = set()
        ordered_keys: list[str] = []
        for item in items:
            for key in item.keys():
                sk = str(key)
                if sk in seen:
                    continue
                seen.add(sk)
                ordered_keys.append(sk)
        rows: list[dict[str, Any]] = []
        for item in items:
            src = item if isinstance(item, dict) else {}
            rows.append({k: src.get(k) for k in ordered_keys})
        return rows, "json_rows"
    rows = [{"index": idx, "value": item} for idx, item in enumerate(items)]
    return rows, "json_scalar_array_rows"


def _parse_json_payload(raw_json: str, mode: str) -> tuple[Any, str]:
    text = str(raw_json or "")
    selected = str(mode or "auto").strip().lower() or "auto"
    if selected == "document":
        return json.loads(text), "document"
    if selected == "ndjson":
        rows: list[Any] = []
        for line in text.splitlines():
            ln = str(line).strip()
            if not ln:
                continue
            rows.append(json.loads(ln))
        return rows, "ndjson"
    # auto
    trimmed = text.lstrip()
    if trimmed.startswith("{") or trimmed.startswith("["):
        try:
            return json.loads(text), "document"
        except Exception:
            pass
    rows = []
    for line in text.splitlines():
        ln = str(line).strip()
        if not ln:
            continue
        rows.append(json.loads(ln))
    if rows:
        return rows, "ndjson"
    return json.loads(text), "document"


async def _parse_ndjson_stream(
    *,
    node_id: str,
    run_id: str,
    bus: RunEventBus,
    file_path: Optional[Path],
    file_bytes: Optional[bytes],
    encoding: str,
    chunk_lines: int,
    max_records: Optional[int],
) -> tuple[list[Any], Dict[str, Any]]:
    rows: list[Any] = []
    processed_lines = 0
    parse_errors = 0
    chunk = max(1, int(chunk_lines or 1000))
    limit = max_records if isinstance(max_records, int) and max_records > 0 else None

    def _iter_lines():
        if isinstance(file_path, Path):
            with file_path.open("r", encoding=encoding, errors="replace") as fh:
                for raw in fh:
                    yield raw
            return
        text = (file_bytes or b"").decode(encoding, errors="replace")
        for raw in text.splitlines():
            yield raw

    for raw in _iter_lines():
        line = str(raw).strip()
        if not line:
            continue
        processed_lines += 1
        try:
            rows.append(json.loads(line))
        except Exception:
            parse_errors += 1
        if processed_lines % chunk == 0:
            await bus.emit(
                {
                    "type": "log",
                    "runId": run_id,
                    "at": _iso_now(),
                    "level": "info",
                    "message": f"json-stream progress lines={processed_lines} rows={len(rows)}",
                    "nodeId": node_id,
                }
            )
        if limit is not None and len(rows) >= limit:
            break
    return rows, {
        "enabled": True,
        "lines_processed": processed_lines,
        "records_emitted": len(rows),
        "chunk_lines": chunk,
        "max_records": limit,
        "parse_errors": parse_errors,
    }


def _flatten_json_record(
    value: Any,
    *,
    strategy: str,
    separator: str,
    _prefix: str = "",
    _depth: int = 0,
) -> Dict[str, Any]:
    mode = str(strategy or "none").strip().lower()
    sep = str(separator or ".")
    if mode == "none":
        if isinstance(value, dict):
            return {str(k): v for k, v in value.items()}
        return {"value": value}
    if not isinstance(value, dict):
        return {"value": value}
    out: Dict[str, Any] = {}
    for key, item in value.items():
        k = str(key)
        path = f"{_prefix}{sep}{k}" if _prefix else k
        can_descend = isinstance(item, dict) and (mode == "deep" or (mode == "shallow" and _depth == 0))
        if can_descend:
            nested = _flatten_json_record(item, strategy=mode, separator=sep, _prefix=path, _depth=_depth + 1)
            out.update(nested)
        else:
            out[path] = item
    return out


def _apply_excel_policies(
    df: pd.DataFrame,
    *,
    merged_cells_policy: str,
    date_policy: str,
    date_format: Optional[str],
) -> pd.DataFrame:
    out = df.copy()
    if str(merged_cells_policy or "none").strip().lower() == "ffill":
        out = out.ffill(axis=0)
    policy = str(date_policy or "auto").strip().lower()
    if policy == "coerce":
        fmt = str(date_format or "").strip() or None
        for col in list(out.columns):
            series = out[col]
            if is_object_dtype(series.dtype) or is_string_dtype(series.dtype):
                try:
                    converted = pd.to_datetime(series, errors="coerce", format=fmt)
                    non_null = int(series.notna().sum())
                    parsed_non_null = int(converted.notna().sum())
                    # Only coerce if most non-null values parse as dates.
                    if non_null > 0 and (parsed_non_null / non_null) >= 0.8:
                        out[col] = converted
                except Exception:
                    continue
    elif policy == "string":
        for col in list(out.columns):
            series = out[col]
            if is_datetime64_any_dtype(series.dtype):
                out[col] = series.astype("string").fillna("")
    return out


def _text_to_records(text: str, mode: str, chunk_size: int) -> list[dict[str, Any]]:
    content = str(text or "")
    normalized_mode = str(mode or "raw").strip().lower()
    rows: list[dict[str, Any]] = []
    if normalized_mode == "lines":
        for idx, line in enumerate(content.splitlines()):
            rows.append({"row_index": idx, "text": line})
        return rows
    if normalized_mode == "paragraphs":
        parts = [p for p in re.split(r"\n\s*\n", content) if str(p).strip()]
        for idx, part in enumerate(parts):
            rows.append({"row_index": idx, "text": part})
        return rows
    if normalized_mode == "fixed_chunk":
        size = max(1, int(chunk_size or 1000))
        for idx, start in enumerate(range(0, len(content), size)):
            rows.append({"row_index": idx, "text": content[start : start + size]})
        return rows
    if content:
        return [{"row_index": 0, "text": content}]
    return []


def _select_pdf_page_indexes(total_pages: int, mode: str, page_range: Optional[str], sample_count: int) -> list[int]:
    if total_pages <= 0:
        return []
    normalized = str(mode or "all").strip().lower()
    if normalized == "sample":
        n = max(1, int(sample_count or 1))
        return list(range(min(total_pages, n)))
    if normalized == "range":
        selected: set[int] = set()
        for token in str(page_range or "").split(","):
            part = token.strip()
            if not part:
                continue
            if "-" in part:
                left, right = part.split("-", 1)
                try:
                    start = max(1, int(left))
                    end = min(total_pages, int(right))
                except Exception:
                    continue
                if end < start:
                    start, end = end, start
                for idx in range(start, end + 1):
                    selected.add(idx - 1)
            else:
                try:
                    idx = int(part)
                except Exception:
                    continue
                if 1 <= idx <= total_pages:
                    selected.add(idx - 1)
        if selected:
            return sorted(selected)
    return list(range(total_pages))


def _sanitize_svg_bytes(raw: bytes) -> bytes:
    text = raw.decode("utf-8", errors="replace")
    # Remove script blocks and inline event handlers.
    text = re.sub(r"<script[\s\S]*?</script>", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\son[a-zA-Z]+\s*=\s*\"[^\"]*\"", "", text)
    text = re.sub(r"\son[a-zA-Z]+\s*=\s*'[^']*'", "", text)
    return text.encode("utf-8")


def _extract_image_metadata(file_bytes: bytes, file_format: str, tiff_pages_mode: str) -> Dict[str, Any]:
    meta: Dict[str, Any] = {"format": file_format}
    if not HAS_PIL:
        return meta
    try:
        with Image.open(io.BytesIO(file_bytes)) as img:
            meta["width"] = int(getattr(img, "width", 0) or 0)
            meta["height"] = int(getattr(img, "height", 0) or 0)
            meta["color_mode"] = str(getattr(img, "mode", "") or "")
            meta["mime"] = str(Image.MIME.get(img.format, "") if getattr(img, "format", None) else "")
            n_frames = int(getattr(img, "n_frames", 1) or 1)
            if str(file_format).lower() in {"tif", "tiff"}:
                meta["tiff_pages"] = n_frames
                if str(tiff_pages_mode or "first").strip().lower() == "all":
                    meta["tiff_pages_mode"] = "all"
                else:
                    meta["tiff_pages_mode"] = "first"
            exif = None
            try:
                exif = img.getexif()
            except Exception:
                exif = None
            orientation = None
            if exif:
                for k, v in exif.items():
                    if str(ExifTags.TAGS.get(k, "")).lower() == "orientation":
                        orientation = int(v)
                        break
            if orientation is not None:
                meta["orientation"] = orientation
    except Exception:
        return meta
    return meta


def _extract_wav_metadata(file_bytes: bytes) -> Dict[str, Any]:
    meta: Dict[str, Any] = {}
    try:
        with wave.open(io.BytesIO(file_bytes), "rb") as wf:
            channels = int(wf.getnchannels())
            sample_rate = int(wf.getframerate())
            nframes = int(wf.getnframes())
            sampwidth = int(wf.getsampwidth())
            duration = float(nframes / sample_rate) if sample_rate > 0 else 0.0
            meta.update(
                {
                    "channels": channels,
                    "sample_rate": sample_rate,
                    "frame_count": nframes,
                    "sample_width_bytes": sampwidth,
                    "duration_sec": round(duration, 6),
                    "codec": "pcm",
                }
            )
    except Exception:
        return meta
    return meta


def _normalize_wav_bytes(file_bytes: bytes, target_peak: float) -> tuple[bytes, Dict[str, Any]]:
    target = max(0.01, min(1.0, float(target_peak)))
    try:
        with wave.open(io.BytesIO(file_bytes), "rb") as wf:
            params = wf.getparams()
            raw_frames = wf.readframes(wf.getnframes())
    except Exception:
        return file_bytes, {"applied": False, "reason": "wav_decode_failed"}
    if int(params.sampwidth) != 2:
        return file_bytes, {"applied": False, "reason": "unsupported_sample_width"}
    samples = array.array("h")
    samples.frombytes(raw_frames)
    if len(samples) == 0:
        return file_bytes, {"applied": False, "reason": "empty_audio"}
    peak = max(abs(int(s)) for s in samples)
    if peak <= 0:
        return file_bytes, {"applied": False, "reason": "silent_audio"}
    desired_peak = int(32767 * target)
    gain = float(desired_peak / peak)
    if gain <= 0:
        return file_bytes, {"applied": False, "reason": "invalid_gain"}
    normalized = array.array("h", [max(-32768, min(32767, int(round(int(s) * gain)))) for s in samples])
    out_buf = io.BytesIO()
    with wave.open(out_buf, "wb") as out_wf:
        out_wf.setparams(params)
        out_wf.writeframes(normalized.tobytes())
    return out_buf.getvalue(), {"applied": True, "gain": round(gain, 6), "target_peak": target}


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
    schema_extra: Optional[Dict[str, Any]] = None,
    source_observability: Optional[Dict[str, Any]] = None,
) -> FileMetadata:
    payload_bytes = _payload_bytes_for_mode(data, output_mode)
    data_schema: Dict[str, Any] = {"source_kind": source_kind, "output_mode": output_mode}
    if isinstance(schema_extra, dict):
        data_schema.update(schema_extra)
    if isinstance(source_observability, dict):
        data_schema["source_observability"] = source_observability
    return FileMetadata(
        file_path=f"artifact://{graph_id}/{node_id}/{source_kind}",
        file_type=_mode_to_file_type(output_mode),
        mime_type=str(mime_override or _mode_to_mime(output_mode)),
        size_bytes=len(payload_bytes),
        data_schema=data_schema,
        row_count=(len(data) if isinstance(data, list) else None),
        access_method="local",
        content_hash=_sha256_bytes(payload_bytes),
        node_id=node_id,
        params_hash=hashlib.sha256(
            json.dumps(params, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
    )


def _build_source_observability(
    *,
    source_kind: str,
    output_mode: str,
    input_bytes: Optional[int] = None,
    output_rows: Optional[int] = None,
    null_ratio: Optional[float] = None,
    type_drift: Optional[int] = None,
    retry_count: Optional[int] = None,
    partition_count: Optional[int] = None,
    execution_ms: Optional[float] = None,
    cost_estimate_usd: Optional[float] = None,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "source_kind": source_kind,
        "output_mode": output_mode,
        "input_bytes": int(input_bytes) if isinstance(input_bytes, (int, float)) and input_bytes >= 0 else None,
        "output_rows": int(output_rows) if isinstance(output_rows, (int, float)) and output_rows >= 0 else None,
        "null_ratio": float(null_ratio) if isinstance(null_ratio, (int, float)) else None,
        "type_drift": int(type_drift) if isinstance(type_drift, (int, float)) else None,
        "retry_count": int(retry_count) if isinstance(retry_count, (int, float)) and retry_count >= 0 else 0,
        "partition_count": int(partition_count) if isinstance(partition_count, (int, float)) and partition_count >= 0 else 1,
        "execution_ms": float(execution_ms) if isinstance(execution_ms, (int, float)) else None,
        "cost_estimate_usd": float(cost_estimate_usd) if isinstance(cost_estimate_usd, (int, float)) else 0.0,
    }
    return out


def _table_null_ratio(rows: list[dict[str, Any]]) -> float:
    if not rows:
        return 0.0
    total = 0
    nulls = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        for value in row.values():
            total += 1
            if value is None:
                nulls += 1
    return float(nulls / total) if total > 0 else 0.0


def _source_out_mode_from_node(node: Dict[str, Any]) -> Optional[str]:
    data = ((node or {}).get("data", {}) or {})
    schema = data.get("schema") if isinstance(data.get("schema"), dict) else {}
    expected = schema.get("expectedSchema") if isinstance(schema.get("expectedSchema"), dict) else {}
    typed = expected.get("typedSchema") if isinstance(expected.get("typedSchema"), dict) else {}
    expected_type = str(typed.get("type") or "").strip().lower()
    if expected_type in {"table", "text", "json", "binary"}:
        return expected_type
    params = data.get("params") if isinstance(data.get("params"), dict) else {}
    out = params.get("output") if isinstance(params.get("output"), dict) else {}
    out_mode = str(out.get("mode") or "").strip().lower()
    if out_mode in {"table", "text", "json", "binary"}:
        return out_mode
    legacy_source_type = str(params.get("source_type") or "").strip().lower()
    if legacy_source_type in {"text", "json", "table", "binary"}:
        return legacy_source_type
    source_kind = str(data.get("sourceKind") or params.get("source_type") or "file").strip().lower()
    # Legacy alias support for older graphs that used source_type as an output contract hint.
    if source_kind in {"text", "json", "table", "binary"}:
        return source_kind
    if source_kind == "api":
        return "json"
    if source_kind in {"database", "warehouse"}:
        return "table"
    if source_kind in {"file", "object_store"}:
        file_format = str(params.get("file_format") or "").strip().lower()
        return _default_file_output_mode(file_format)
    return None


def _source_priming_spec(params: Dict[str, Any]) -> Dict[str, Any]:
    raw = params.get("priming")
    if not isinstance(raw, dict):
        return {"enabled": False, "mode": "advisory"}
    enabled = bool(raw.get("enabled"))
    mode = str(raw.get("mode") or "advisory").strip().lower()
    drift_policy = str(raw.get("drift_policy") or "soft").strip().lower()
    if mode not in {"advisory", "priming_only"}:
        mode = "advisory"
    if drift_policy not in {"soft", "strict"}:
        drift_policy = "soft"
    sample_rows_raw = raw.get("sample_rows")
    sample_bytes_raw = raw.get("sample_bytes")
    timeout_raw = raw.get("timeout_ms")
    try:
        sample_rows = max(1, int(sample_rows_raw)) if sample_rows_raw is not None else 50
    except Exception:
        sample_rows = 50
    try:
        sample_bytes = max(1, int(sample_bytes_raw)) if sample_bytes_raw is not None else 65536
    except Exception:
        sample_bytes = 65536
    try:
        timeout_ms = max(1, int(timeout_raw)) if timeout_raw is not None else 1500
    except Exception:
        timeout_ms = 1500
    return {
        "enabled": enabled,
        "mode": mode,
        "drift_policy": drift_policy,
        "sample_rows": sample_rows,
        "sample_bytes": sample_bytes,
        "timeout_ms": timeout_ms,
    }


def _trim_rows_by_bytes(rows: list[dict[str, Any]], sample_bytes: int) -> tuple[list[dict[str, Any]], bool]:
    if not rows:
        return rows, False
    out: list[dict[str, Any]] = []
    trimmed = False
    used = 0
    for row in rows:
        row_bytes = len(_canon_json_bytes(row))
        if out and (used + row_bytes) > sample_bytes:
            trimmed = True
            break
        out.append(row)
        used += row_bytes
        if used >= sample_bytes:
            trimmed = True
            break
    if not out and rows:
        out = [rows[0]]
        trimmed = len(rows) > 1
    return out, trimmed


def _apply_priming_bounds(output_mode: str, data: Any, priming_spec: Dict[str, Any]) -> tuple[Any, Dict[str, Any]]:
    if not bool(priming_spec.get("enabled")):
        return data, {"applied": False}
    sample_rows = int(priming_spec.get("sample_rows") or 50)
    sample_bytes = int(priming_spec.get("sample_bytes") or 65536)
    mode = str(output_mode or "").strip().lower()
    truncated_rows = False
    truncated_bytes = False
    bounded = data
    if mode == "table" and isinstance(data, list):
        rows = [row for row in data if isinstance(row, dict)]
        if len(rows) > sample_rows:
            rows = rows[:sample_rows]
            truncated_rows = True
        rows, row_byte_trimmed = _trim_rows_by_bytes(rows, sample_bytes)
        truncated_bytes = truncated_bytes or row_byte_trimmed
        bounded = rows
    elif mode == "json":
        if isinstance(data, list):
            arr = list(data)
            if len(arr) > sample_rows:
                arr = arr[:sample_rows]
                truncated_rows = True
            payload = _canon_json_bytes(arr)
            if len(payload) > sample_bytes:
                arr, row_byte_trimmed = _trim_rows_by_bytes(
                    [{"value": item} if not isinstance(item, dict) else item for item in arr], sample_bytes
                )
                truncated_bytes = truncated_bytes or row_byte_trimmed
                if arr and all(isinstance(item, dict) and "value" in item and len(item) == 1 for item in arr):
                    bounded = [item.get("value") for item in arr]
                else:
                    bounded = arr
            else:
                bounded = arr
        elif isinstance(data, dict):
            payload = _canon_json_bytes(data)
            if len(payload) > sample_bytes:
                truncated_bytes = True
                bounded = {"_priming_note": "json object truncated for priming", "_bytes": sample_bytes}
    elif mode == "binary":
        raw = bytes(data) if isinstance(data, (bytes, bytearray)) else _payload_bytes_for_mode(data, "binary")
        if len(raw) > sample_bytes:
            bounded = raw[:sample_bytes]
            truncated_bytes = True
        else:
            bounded = raw
    else:
        text = str(data if data is not None else "")
        raw = text.encode("utf-8")
        if len(raw) > sample_bytes:
            bounded = raw[:sample_bytes].decode("utf-8", errors="replace")
            truncated_bytes = True
        else:
            bounded = text
    return bounded, {
        "applied": True,
        "sample_rows": sample_rows,
        "sample_bytes": sample_bytes,
        "truncated_rows": truncated_rows,
        "truncated_bytes": truncated_bytes,
    }


def _detect_payload_and_mime(
    *,
    data: Any,
    output_mode: str,
    current_mime: Optional[str] = None,
    file_format_hint: Optional[str] = None,
) -> Dict[str, Any]:
    mode = str(output_mode or "").strip().lower()
    ff = str(file_format_hint or "").strip().lower()
    payload_type = "binary"
    mime_type = str(current_mime or "").strip() or "application/octet-stream"
    confidence = 0.6
    detected_by = "mode"
    ambiguous = False

    if isinstance(data, (bytes, bytearray)):
        payload_type = "binary"
        mime_type = mime_type if mime_type else "application/octet-stream"
        confidence = 0.98
        detected_by = "python_type"
    elif isinstance(data, str):
        payload_type = "text"
        mime_type = "text/plain; charset=utf-8"
        confidence = 0.95
        detected_by = "python_type"
        raw = data.strip()
        if raw.startswith("{") or raw.startswith("["):
            try:
                json.loads(raw)
                payload_type = "json"
                mime_type = "application/json"
                confidence = 0.86
                detected_by = "content_sniff_json"
            except Exception:
                pass
    elif isinstance(data, dict):
        payload_type = "json"
        mime_type = "application/json"
        confidence = 0.97
        detected_by = "python_type"
    elif isinstance(data, list):
        if all(isinstance(item, dict) for item in data):
            payload_type = "table" if mode == "table" else "json"
            mime_type = "text/csv" if payload_type == "table" else "application/json"
            confidence = 0.9 if payload_type == "table" else 0.86
            detected_by = "list_of_objects"
        else:
            payload_type = "json"
            mime_type = "application/json"
            confidence = 0.82
            detected_by = "python_type"

    if mode == "binary" and payload_type != "binary":
        ambiguous = True
    if mode == "binary" and str(current_mime or "").lower().startswith(("text/", "application/json")):
        ambiguous = True
    if ff in {"jpg", "jpeg", "png", "webp", "gif", "svg", "tif", "tiff"}:
        payload_type = "image"
        mime_type = _file_format_mime(ff)
        confidence = 0.99
        detected_by = "file_format_hint"
    elif ff in {"mp3", "wav", "flac", "ogg", "m4a", "aac"}:
        payload_type = "audio"
        mime_type = _file_format_mime(ff)
        confidence = 0.99
        detected_by = "file_format_hint"
    elif ff in {"mp4", "mov", "webm"}:
        payload_type = "video"
        mime_type = _file_format_mime(ff)
        confidence = 0.99
        detected_by = "file_format_hint"
    elif ff == "pdf":
        mime_type = _file_format_mime(ff)
        if payload_type == "text":
            confidence = max(confidence, 0.75)
            detected_by = "file_format_hint"
    return {
        "payload_type": payload_type,
        "mime_type": mime_type,
        "confidence": round(float(confidence), 3),
        "detected_by": detected_by,
        "ambiguous": bool(ambiguous),
    }


def _schema_fingerprint_from_data(data: Any, payload_type: str) -> str:
    fields: list[dict[str, str]] = []
    if payload_type == "table" and isinstance(data, list):
        fields = _infer_table_columns_from_rows([row for row in data if isinstance(row, dict)])
    env = {"type": str(payload_type or "unknown"), "fields": fields}
    return hashlib.sha256(_canon_json_bytes(env)).hexdigest()


def _compute_priming_drift(
    *,
    expected_schema: Optional[Dict[str, Any]],
    inferred_schema: Optional[Dict[str, Any]],
    detected_mime: str,
    current_mime: str,
) -> Dict[str, Any]:
    expected = expected_schema if isinstance(expected_schema, dict) else {}
    inferred = inferred_schema if isinstance(inferred_schema, dict) else {}
    expected_type = str(expected.get("type") or "unknown").strip().lower()
    inferred_type = str(inferred.get("type") or "unknown").strip().lower()
    type_mismatch = expected_type not in {"", "unknown"} and inferred_type not in {"", "unknown"} and expected_type != inferred_type
    expected_fields = expected.get("fields") if isinstance(expected.get("fields"), list) else []
    inferred_fields = inferred.get("fields") if isinstance(inferred.get("fields"), list) else []
    expected_cols = {str(f.get("name") or "").strip().lower() for f in expected_fields if isinstance(f, dict)}
    inferred_cols = {str(f.get("name") or "").strip().lower() for f in inferred_fields if isinstance(f, dict)}
    expected_cols = {c for c in expected_cols if c}
    inferred_cols = {c for c in inferred_cols if c}
    missing_columns = sorted([c for c in expected_cols if c not in inferred_cols])
    new_columns = sorted([c for c in inferred_cols if c not in expected_cols])
    mime_mismatch = bool(expected.get("mime_type")) and str(expected.get("mime_type") or "").strip().lower() != str(detected_mime or current_mime).strip().lower()
    has_drift = bool(type_mismatch or missing_columns or new_columns or mime_mismatch)
    return {
        "has_drift": has_drift,
        "type_mismatch": type_mismatch,
        "missing_columns": missing_columns,
        "new_columns": new_columns,
        "mime_mismatch": mime_mismatch,
        "expected_type": expected_type,
        "observed_type": inferred_type,
    }


def _sample_preview(data: Any, payload_type: str) -> Any:
    if payload_type == "table" and isinstance(data, list):
        return [dict(row) for row in data[: min(len(data), 5)] if isinstance(row, dict)]
    if payload_type == "json":
        if isinstance(data, list):
            return data[: min(len(data), 5)]
        if isinstance(data, dict):
            return dict(list(data.items())[:10])
    if payload_type == "binary" and isinstance(data, (bytes, bytearray)):
        return {"hex_prefix": bytes(data[:16]).hex(), "size": len(data)}
    text = str(data if data is not None else "")
    return text[:200]


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


def _resolve_connection_ref(connection_ref: str) -> str:
    ref = str(connection_ref or "").strip()
    if not ref:
        raise ValueError("MISSING_SECRET: connection_ref is empty")
    env_name = ref[4:].strip() if ref.lower().startswith("env:") else ref
    value = str(os.getenv(env_name, "")).strip()
    if not value:
        raise ValueError(f"MISSING_SECRET: connection_ref '{ref}' is not set in environment")
    return value


def _resolve_required_env(ref: str, *, param_path: str) -> str:
    name = str(ref or "").strip()
    if not name:
        raise ValueError(f"MISSING_SECRET: {param_path} is required")
    value = str(os.getenv(name, "")).strip()
    if not value:
        raise ValueError(f"MISSING_SECRET: {param_path} '{name}' is not set in environment")
    return value


def _validate_table_identifier(table_name: str) -> tuple[Optional[str], str]:
    raw = str(table_name or "").strip()
    if not raw:
        raise ValueError("INVALID_IDENTIFIER: table_name is required")
    parts = raw.split(".")
    if len(parts) > 2:
        raise ValueError(f"INVALID_IDENTIFIER: unsupported table_name '{raw}'")
    for part in parts:
        if not _SQL_IDENTIFIER_RE.fullmatch(part):
            raise ValueError(f"INVALID_IDENTIFIER: unsafe table_name '{raw}'")
    if len(parts) == 1:
        return None, parts[0]
    return parts[0], parts[1]


def _incremental_state_path() -> Path:
    raw = str(os.getenv("SOURCE_INCREMENTAL_STATE_FILE") or "./data/source_incremental_state.json").strip()
    return Path(raw).expanduser().resolve()


def _load_incremental_state_map() -> Dict[str, Any]:
    path = _incremental_state_path()
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_incremental_state_map(state_map: Dict[str, Any]) -> None:
    path = _incremental_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state_map, sort_keys=True, ensure_ascii=False), encoding="utf-8")


def _cursor_sort_value(value: Any, cursor_type: str) -> Any:
    if value is None:
        return None
    t = str(cursor_type or "auto").strip().lower() or "auto"

    def _as_datetime(v: Any) -> Optional[float]:
        try:
            ts = pd.to_datetime(v, utc=True, errors="coerce")
            if pd.isna(ts):
                return None
            return float(ts.value)
        except Exception:
            return None

    if t == "int":
        try:
            return int(value)
        except Exception:
            return None
    if t == "float":
        try:
            return float(value)
        except Exception:
            return None
    if t == "datetime":
        return _as_datetime(value)
    if t == "string":
        return str(value)
    # auto
    for caster in (int, float):
        try:
            return caster(value)
        except Exception:
            continue
    dt = _as_datetime(value)
    if dt is not None:
        return dt
    return str(value)


def _compare_gt(left: Any, right: Any) -> bool:
    try:
        return left > right
    except Exception:
        return str(left) > str(right)


def _apply_incremental_rows(
    *,
    rows: list[dict[str, Any]],
    graph_id: str,
    node_id: str,
    incremental_spec: Optional[Dict[str, Any]],
) -> tuple[list[dict[str, Any]], Optional[Dict[str, Any]]]:
    spec = incremental_spec if isinstance(incremental_spec, dict) else {}
    if not bool(spec.get("enabled")):
        return rows, None
    cursor_column = str(spec.get("cursor_column") or "").strip()
    if not cursor_column:
        raise ValueError("INVALID_INCREMENTAL_CONFIG: incremental.cursor_column is required when enabled=true")
    cursor_type = str(spec.get("cursor_type") or "auto").strip().lower() or "auto"
    state_key = str(spec.get("state_key") or f"{graph_id}:{node_id}").strip()
    window_start_raw = spec.get("window_start")
    window_end_raw = spec.get("window_end")

    state_map = _load_incremental_state_map()
    prev_raw = state_map.get(state_key)
    prev_val = _cursor_sort_value(prev_raw, cursor_type)
    window_start = _cursor_sort_value(window_start_raw, cursor_type) if window_start_raw is not None else None
    window_end = _cursor_sort_value(window_end_raw, cursor_type) if window_end_raw is not None else None

    filtered: list[dict[str, Any]] = []
    max_raw: Any = prev_raw
    max_val = prev_val
    for row in rows:
        cur_raw = row.get(cursor_column) if isinstance(row, dict) else None
        cur_val = _cursor_sort_value(cur_raw, cursor_type)
        if cur_val is None:
            continue
        if prev_val is not None and not _compare_gt(cur_val, prev_val):
            continue
        if window_start is not None and _compare_gt(window_start, cur_val):
            continue
        if window_end is not None and _compare_gt(cur_val, window_end):
            continue
        filtered.append(row)
        if max_val is None or _compare_gt(cur_val, max_val):
            max_val = cur_val
            max_raw = cur_raw

    if max_raw is not None:
        state_map[state_key] = max_raw
        _save_incremental_state_map(state_map)

    meta = {
        "enabled": True,
        "state_key": state_key,
        "cursor_column": cursor_column,
        "cursor_type": cursor_type,
        "previous_cursor": prev_raw,
        "next_cursor": max_raw,
        "rows_before": len(rows),
        "rows_after": len(filtered),
    }
    return filtered, meta


def _plan_partitions(spec: Optional[Dict[str, Any]]) -> list[Dict[str, Any]]:
    cfg = spec if isinstance(spec, dict) else {}
    if not bool(cfg.get("enabled")):
        return []
    kind = str(cfg.get("kind") or "static_list").strip().lower()
    out: list[Dict[str, Any]] = []
    if kind == "static_list":
        values = cfg.get("static_values") if isinstance(cfg.get("static_values"), list) else []
        for idx, value in enumerate(values):
            out.append({"index": idx, "partition_id": str(value), "value": value})
        return out
    if kind == "numeric_shards":
        start = cfg.get("numeric_start")
        end = cfg.get("numeric_end")
        step = cfg.get("numeric_step")
        if start is None or end is None:
            return []
        try:
            s = float(start)
            e = float(end)
            st = float(step if step is not None else 1)
        except Exception:
            return []
        if st <= 0:
            return []
        idx = 0
        cur = s
        while cur <= e:
            value = int(cur) if float(cur).is_integer() else cur
            out.append({"index": idx, "partition_id": str(value), "value": value})
            cur += st
            idx += 1
        return out
    if kind == "date_range":
        start_raw = str(cfg.get("date_start") or "").strip()
        end_raw = str(cfg.get("date_end") or "").strip()
        if not start_raw or not end_raw:
            return []
        start_dt = pd.to_datetime(start_raw, utc=True, errors="coerce")
        end_dt = pd.to_datetime(end_raw, utc=True, errors="coerce")
        if pd.isna(start_dt) or pd.isna(end_dt):
            return []
        every_days = int(cfg.get("date_every_days") or 1)
        every_days = max(1, every_days)
        idx = 0
        cur = start_dt
        while cur <= end_dt:
            value = str(cur.date())
            out.append({"index": idx, "partition_id": value, "value": value})
            cur = cur + pd.Timedelta(days=every_days)
            idx += 1
        return out
    return []


def _merge_partition_results(output_mode: str, results: list[Dict[str, Any]]) -> Any:
    ordered = sorted(results, key=lambda item: int(item.get("index") or 0))
    payloads = [item.get("data") for item in ordered]
    if output_mode == "binary":
        chunks = [p for p in payloads if isinstance(p, (bytes, bytearray))]
        return b"".join([bytes(c) for c in chunks])
    if output_mode == "text":
        return "\n".join([str(p) for p in payloads if p is not None])
    if output_mode == "json":
        if all(isinstance(p, list) for p in payloads):
            merged: list[Any] = []
            for part in payloads:
                merged.extend(part)
            return merged
        if all(isinstance(p, dict) for p in payloads):
            return [{"partition_id": str(ordered[i].get("partition_id")), **dict(payloads[i])} for i in range(len(ordered))]
        return payloads
    # table/default
    merged_rows: list[dict[str, Any]] = []
    for i, payload in enumerate(payloads):
        part_id = str(ordered[i].get("partition_id") or "")
        if isinstance(payload, list):
            for row in payload:
                if isinstance(row, dict):
                    merged_rows.append({"__partition_id": part_id, **row})
    return _canonical_table_rows(merged_rows)


async def exec_source(
    run_id: str,
    node: Dict[str, Any],
    context: GraphContext,
    input_metadata: Optional[FileMetadata] = None,
    upstream_artifact_ids: Optional[list[str]] = None,
) -> NodeOutput:
    upstream_artifact_ids = upstream_artifact_ids or []
    start_time = time.time()
    start_mono = time.monotonic()
    node_id = node["id"]
    raw_params = dict(node.get("data", {}).get("params", {}) or {})
    source_type = (node.get("data", {}).get("sourceKind") or raw_params.get("source_type") or "file")
    raw_params["source_type"] = source_type
    params = normalize_source_params_frontend(raw_params)
    params["source_type"] = source_type
    priming_spec = _source_priming_spec(params)

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
            output = await _handle_database_source(
                node_id,
                params,
                context.bus,
                run_id,
                context.graph_id,
                forced_output_mode=_source_out_mode_from_node(node),
            )
        elif source_type == "api":
            output = await _handle_api_source(
                node_id,
                params,
                context.bus,
                run_id,
                context.graph_id,
                forced_output_mode=_source_out_mode_from_node(node),
            )
        elif source_type == "object_store":
            output = await _handle_object_store_source(
                node_id,
                params,
                context.bus,
                run_id,
                context.graph_id,
                forced_output_mode=_source_out_mode_from_node(node),
            )
        elif source_type == "warehouse":
            output = await _handle_warehouse_source(
                node_id,
                params,
                context.bus,
                run_id,
                context.graph_id,
                forced_output_mode=_source_out_mode_from_node(node),
            )
        else:
            raise ValueError(f"Unknown source_type: {source_type}")
        if output.status == "succeeded":
            output_mode = str(((output.metadata.data_schema or {}).get("output_mode") if output.metadata else "") or "").strip().lower()
            bounded_data, priming_meta = _apply_priming_bounds(output_mode, output.data, priming_spec)
            output.data = bounded_data
            if output.metadata is not None and isinstance(output.metadata.data_schema, dict) and priming_meta.get("applied"):
                schema_env = dict(output.metadata.data_schema)
                detection = _detect_payload_and_mime(
                    data=output.data,
                    output_mode=output_mode,
                    current_mime=output.metadata.mime_type,
                    file_format_hint=str((schema_env.get("file_format") or "")).strip().lower() or None,
                )
                schema_env["priming"] = {
                    "enabled": True,
                    "mode": str(priming_spec.get("mode") or "advisory"),
                    "drift_policy": str(priming_spec.get("drift_policy") or "soft"),
                    "priming_only": bool(str(priming_spec.get("mode") or "") == "priming_only"),
                    "sample_rows": int(priming_spec.get("sample_rows") or 50),
                    "sample_bytes": int(priming_spec.get("sample_bytes") or 65536),
                    "timeout_ms": int(priming_spec.get("timeout_ms") or 1500),
                    "timed_out": ((time.monotonic() - start_mono) * 1000.0) >= float(priming_spec.get("timeout_ms") or 1500),
                    "truncated_rows": bool(priming_meta.get("truncated_rows")),
                    "truncated_bytes": bool(priming_meta.get("truncated_bytes")),
                    "detection": detection,
                }
                output.metadata.data_schema = schema_env
                output.metadata.mime_type = str(detection.get("mime_type") or output.metadata.mime_type)
                payload_type = str(detection.get("payload_type") or output_mode or "unknown")
                expected_schema = params.get("output_schema")
                if not isinstance(expected_schema, dict):
                    out_obj = params.get("output")
                    expected_schema = out_obj.get("schema") if isinstance(out_obj, dict) and isinstance(out_obj.get("schema"), dict) else None
                inferred_schema = infer_typed_schema_from_sample_profile(output.data, payload_type)
                drift = _compute_priming_drift(
                    expected_schema=expected_schema,
                    inferred_schema=inferred_schema,
                    detected_mime=str(detection.get("mime_type") or ""),
                    current_mime=str(output.metadata.mime_type or ""),
                )
                output.metadata.priming_artifact = {
                    "version": 1,
                    "payload_type": payload_type,
                    "mime_type": str(detection.get("mime_type") or output.metadata.mime_type),
                    "schema_fingerprint": _schema_fingerprint_from_data(output.data, payload_type),
                    "inferred_schema": inferred_schema,
                    "sample_preview": _sample_preview(output.data, payload_type),
                    "stats": {
                        "sample_rows": int(priming_spec.get("sample_rows") or 50),
                        "sample_bytes": int(priming_spec.get("sample_bytes") or 65536),
                        "truncated_rows": bool(priming_meta.get("truncated_rows")),
                        "truncated_bytes": bool(priming_meta.get("truncated_bytes")),
                    },
                    "detection": detection,
                    "drift": drift,
                }
                schema_env["priming"]["drift"] = drift
                output.metadata.data_schema = schema_env
                if drift.get("has_drift"):
                    drift_msg = (
                        f"PRIMING_SCHEMA_DRIFT: type_mismatch={drift.get('type_mismatch')} "
                        f"missing={drift.get('missing_columns')} new={drift.get('new_columns')} "
                        f"mime_mismatch={drift.get('mime_mismatch')}"
                    )
                    if str(priming_spec.get("drift_policy") or "soft") == "strict":
                        return NodeOutput(
                            status="failed",
                            metadata=output.metadata,
                            execution_time_ms=(time.time() - start_time) * 1000,
                            error=drift_msg,
                        )
                    await context.bus.emit(
                        {
                            "type": "log",
                            "runId": run_id,
                            "at": _iso_now(),
                            "level": "warning",
                            "message": drift_msg,
                            "nodeId": node_id,
                        }
                    )
                if bool(detection.get("ambiguous")):
                    await context.bus.emit(
                        {
                            "type": "log",
                            "runId": run_id,
                            "at": _iso_now(),
                            "level": "warning",
                            "message": (
                                "PRIMING_TYPE_DETECTION_AMBIGUOUS: bounded sample produced ambiguous payload detection; "
                                f"payload={detection.get('payload_type')} mode={output_mode}"
                            ),
                            "nodeId": node_id,
                        }
                    )
                if output.metadata.row_count is not None and isinstance(output.data, list):
                    output.metadata.row_count = len(output.data)
                payload_bytes = _payload_bytes_for_mode(output.data, output_mode or "text")
                output.metadata.size_bytes = len(payload_bytes)
                output.metadata.content_hash = _sha256_bytes(payload_bytes)
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
    inline_text_input: Optional[str] = None
    if (
        not params.get("snapshot_id")
        and not params.get("snapshotId")
        and not params.get("file_path")
        and not params.get("filename")
        and isinstance(params.get("text"), str)
    ):
        # Backward compatibility for legacy source text nodes persisted as sourceKind=file.
        inline_text_input = str(params.get("text") or "")
        params = dict(params)
        params.setdefault("file_format", "txt")

    if isinstance(params.get("file_path"), str) and not params.get("filename"):
        legacy = Path(str(params.get("file_path")))
        params.setdefault("rel_path", str(legacy.parent) if str(legacy.parent) not in {"", "."} else ".")
        params.setdefault("filename", legacy.name or str(legacy))
    schema = SourceFileParams.model_validate(params)
    output_mode = forced_output_mode or _default_file_output_mode(schema.file_format)

    file_bytes: Optional[bytes] = None
    file_path: Optional[Path] = None
    if inline_text_input is not None:
        file_bytes = inline_text_input.encode(schema.encoding, errors="replace")
    elif schema.snapshot_id:
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
    table_coercion: Dict[str, Any] | None = None
    table_columns: list[dict[str, str]] | None = None
    csv_has_header: Optional[bool] = None
    excel_has_header: Optional[bool] = None
    format_specific_metadata: Dict[str, Any] = {}

    if schema.file_format in {"csv", "tsv"}:
        delimiter = schema.delimiter or ("\t" if schema.file_format == "tsv" else ",")
        malformed_policy = str(getattr(schema, "malformed_row_policy", "fail")).strip().lower()
        csv_has_header = _detect_csv_has_header(
            file_bytes=file_bytes,
            file_path=file_path,
            encoding=schema.encoding,
            delimiter=delimiter,
            explicit=getattr(schema, "has_header", None),
        )
        csv_input: Any = io.BytesIO(file_bytes) if file_bytes is not None else file_path
        on_bad_lines = "error"
        if malformed_policy == "skip":
            on_bad_lines = "skip"
        elif malformed_policy == "warn":
            on_bad_lines = "warn"
        quote_char = str(getattr(schema, "quote_char", "") or "").strip()
        escape_char = str(getattr(schema, "escape_char", "") or "").strip()
        thousands_separator = str(getattr(schema, "thousands_separator", "") or "").strip()
        date_columns = [str(c).strip() for c in (getattr(schema, "date_columns", []) or []) if str(c).strip()]
        df = pd.read_csv(
            csv_input,
            delimiter=delimiter,
            encoding=schema.encoding,
            header=0 if csv_has_header else None,
            quotechar=(quote_char if quote_char else "\""),
            escapechar=(escape_char if escape_char else None),
            on_bad_lines=on_bad_lines,
            decimal=str(getattr(schema, "decimal_separator", ".") or "."),
            thousands=(thousands_separator if thousands_separator else None),
            parse_dates=date_columns if len(date_columns) > 0 else False,
        )
        if date_columns and str(getattr(schema, "date_format", "") or "").strip():
            fmt = str(getattr(schema, "date_format") or "").strip()
            for col in date_columns:
                if col in df.columns:
                    try:
                        df[col] = pd.to_datetime(df[col], format=fmt, errors="coerce")
                    except Exception:
                        pass
        if not csv_has_header:
            df.columns = [f"column_{i+1}" for i in range(len(df.columns))]
        rows = df.to_dict(orient="records")
        table_columns = _infer_table_columns_from_dataframe(df)
        if malformed_policy == "warn":
            malformed_rows = _count_malformed_csv_rows(
                file_bytes=file_bytes,
                file_path=file_path,
                encoding=schema.encoding,
                delimiter=delimiter,
                has_header=bool(csv_has_header),
            )
            if isinstance(malformed_rows, int) and malformed_rows > 0:
                await bus.emit(
                    {
                        "type": "log",
                        "runId": run_id,
                        "at": _iso_now(),
                        "level": "warning",
                        "message": f"CSV parser skipped {malformed_rows} malformed row(s) due to malformed_row_policy=warn",
                        "nodeId": node_id,
                    }
                )
    elif schema.file_format == "parquet":
        parquet_input: Any = io.BytesIO(file_bytes) if file_bytes is not None else file_path
        projected_columns = [str(c).strip() for c in (getattr(schema, "parquet_columns", []) or []) if str(c).strip()]
        selected_row_groups = [int(g) for g in (getattr(schema, "parquet_row_groups", []) or []) if isinstance(g, int) and g >= 0]
        parquet_file = pq.ParquetFile(parquet_input)
        if selected_row_groups:
            parquet_table = parquet_file.read_row_groups(selected_row_groups, columns=projected_columns or None)
        else:
            parquet_table = parquet_file.read(columns=projected_columns or None)
        parquet_schema = parquet_table.schema
        max_rows = getattr(schema, "parquet_max_rows", None)
        if isinstance(max_rows, int) and max_rows > 0 and parquet_table.num_rows > max_rows:
            parquet_table = parquet_table.slice(0, max_rows)
        df = parquet_table.to_pandas()
        rows = df.to_dict(orient="records")
        table_columns = _table_columns_from_arrow_schema(parquet_schema)
        format_specific_metadata["parquet_logical_types"] = {
            str(col.get("name") or ""): str(col.get("type") or "unknown")
            for col in (table_columns or [])
            if isinstance(col, dict) and str(col.get("name") or "").strip()
        }
        try:
            if hasattr(parquet_input, "seek"):
                parquet_input.seek(0)
            parquet_pf = pq.ParquetFile(parquet_input)
            format_specific_metadata["parquet_stats"] = {
                "row_groups": int(parquet_pf.num_row_groups),
                "columns": int(len(parquet_pf.schema_arrow.names)),
                "selected_row_groups": selected_row_groups,
                "projected_columns": (projected_columns if projected_columns else list(parquet_table.column_names)),
                "output_rows": int(parquet_table.num_rows),
            }
        except Exception:
            pass
    elif schema.file_format == "json":
        json_mode = str(getattr(schema, "json_mode", "auto") or "auto").strip().lower() or "auto"
        stream_enabled = bool(getattr(schema, "json_streaming_enabled", False))
        resolved_json_mode = json_mode
        if stream_enabled and json_mode in {"ndjson", "auto"}:
            if json_mode == "auto":
                probe_text = _csv_sample_text(file_bytes, file_path, schema.encoding, max_bytes=4096).lstrip()
                if probe_text.startswith("{") or probe_text.startswith("["):
                    resolved_json_mode = "document"
                else:
                    resolved_json_mode = "ndjson"
            if resolved_json_mode == "ndjson":
                json_data, stream_meta = await _parse_ndjson_stream(
                    node_id=node_id,
                    run_id=run_id,
                    bus=bus,
                    file_path=file_path,
                    file_bytes=file_bytes,
                    encoding=schema.encoding,
                    chunk_lines=int(getattr(schema, "json_stream_chunk_lines", 1000) or 1000),
                    max_records=getattr(schema, "json_stream_max_records", None),
                )
                format_specific_metadata["json_streaming"] = stream_meta
            else:
                raw_json = (
                    (file_bytes or b"").decode(schema.encoding, errors="replace")
                    if file_bytes is not None
                    else Path(file_path).read_text(encoding=schema.encoding)
                )
                json_data, resolved_json_mode = _parse_json_payload(raw_json, "document")
        else:
            raw_json = (
                (file_bytes or b"").decode(schema.encoding, errors="replace")
                if file_bytes is not None
                else Path(file_path).read_text(encoding=schema.encoding)
            )
            json_data, resolved_json_mode = _parse_json_payload(raw_json, json_mode)
        format_specific_metadata["json_mode_resolved"] = resolved_json_mode
        if isinstance(json_data, list):
            rows = json_data
    elif schema.file_format == "excel":
        sheet = schema.sheet_name or 0
        strategy = str(getattr(schema, "excel_import_strategy", "single") or "single").strip().lower()
        selected_sheets = [str(s).strip() for s in (getattr(schema, "excel_sheets", []) or []) if str(s).strip()]
        explicit_header = getattr(schema, "has_header", None)

        def _read_excel(*, sheet_name: Any, header: Any) -> Any:
            read_input: Any = io.BytesIO(file_bytes) if file_bytes is not None else file_path
            return pd.read_excel(read_input, sheet_name=sheet_name, header=header)

        if strategy == "single":
            if explicit_header is None:
                probe = _read_excel(sheet_name=sheet, header=None)
                guess = _excel_heuristic_has_header(probe)
                excel_has_header = True if guess is None else bool(guess)
            else:
                excel_has_header = bool(explicit_header)
            df = _read_excel(sheet_name=sheet, header=0 if excel_has_header else None)
            if not excel_has_header:
                df.columns = [f"column_{i+1}" for i in range(len(df.columns))]
            df = _apply_excel_policies(
                df,
                merged_cells_policy=str(getattr(schema, "excel_merged_cells_policy", "none") or "none"),
                date_policy=str(getattr(schema, "excel_date_policy", "auto") or "auto"),
                date_format=getattr(schema, "excel_date_format", None),
            )
            rows = df.to_dict(orient="records")
            table_columns = _infer_table_columns_from_dataframe(df)
            format_specific_metadata["excel_provenance"] = {
                "strategy": "single",
                "sheets": [str(sheet)],
                "row_counts": {str(sheet): int(len(rows))},
            }
        else:
            # For multi-sheet runs, default to header row unless user explicitly disables it.
            excel_has_header = True if explicit_header is None else bool(explicit_header)
            workbook = _read_excel(sheet_name=None, header=0 if excel_has_header else None)
            if not isinstance(workbook, dict):
                workbook = {str(sheet): workbook}
            available = [str(name) for name in workbook.keys()]
            target_sheets = [name for name in available if not selected_sheets or name in set(selected_sheets)]
            frames: list[pd.DataFrame] = []
            row_counts: Dict[str, int] = {}
            for name in target_sheets:
                frame = workbook.get(name)
                if not isinstance(frame, pd.DataFrame):
                    continue
                local = frame.copy()
                if not excel_has_header:
                    local.columns = [f"column_{i+1}" for i in range(len(local.columns))]
                if strategy == "stack":
                    local["__sheet"] = name
                row_counts[name] = int(len(local))
                frames.append(local)
            df = pd.concat(frames, axis=0, ignore_index=True, sort=True) if frames else pd.DataFrame()
            df = _apply_excel_policies(
                df,
                merged_cells_policy=str(getattr(schema, "excel_merged_cells_policy", "none") or "none"),
                date_policy=str(getattr(schema, "excel_date_policy", "auto") or "auto"),
                date_format=getattr(schema, "excel_date_format", None),
            )
            rows = df.to_dict(orient="records")
            table_columns = _infer_table_columns_from_dataframe(df)
            format_specific_metadata["excel_provenance"] = {
                "strategy": strategy,
                "sheets": target_sheets,
                "row_counts": row_counts,
            }
        format_specific_metadata["excel_policy"] = {
            "merged_cells_policy": str(getattr(schema, "excel_merged_cells_policy", "none") or "none"),
            "date_policy": str(getattr(schema, "excel_date_policy", "auto") or "auto"),
            "date_format": (str(getattr(schema, "excel_date_format", "") or "").strip() or None),
        }
    elif schema.file_format == "txt":
        text_data = (
            (file_bytes or b"").decode(schema.encoding, errors="replace")
            if file_bytes is not None
            else Path(file_path).read_text(encoding=schema.encoding)
        )
        txt_mode = str(getattr(schema, "txt_record_mode", "raw") or "raw")
        if txt_mode in {"lines", "paragraphs", "fixed_chunk"}:
            rows = _text_to_records(text_data, txt_mode, int(getattr(schema, "txt_chunk_size", 1000) or 1000))
            table_columns = [{"name": "row_index", "type": "int"}, {"name": "text", "type": "string"}]
            format_specific_metadata["txt_recordization"] = {
                "mode": txt_mode,
                "chunk_size": int(getattr(schema, "txt_chunk_size", 1000) or 1000),
                "rows": len(rows),
            }
    elif schema.file_format == "pdf":
        if not HAS_PDF:
            raise ImportError("PDF support requires PyPDF2 and pdfplumber")
        requested_mode = str(getattr(schema, "pdf_extraction_mode", "text") or "text").strip().lower()
        resolved_mode = requested_mode
        pdf_input: Any = io.BytesIO(file_bytes) if file_bytes is not None else file_path
        with pdfplumber.open(pdf_input) as pdf:
            pages = list(pdf.pages)
            page_count = len(pages)
            page_mode = str(getattr(schema, "pdf_page_mode", "all") or "all")
            page_range = getattr(schema, "pdf_page_range", None)
            page_sample = int(getattr(schema, "pdf_page_sample", 1) or 1)
            selected_indexes = _select_pdf_page_indexes(page_count, page_mode, page_range, page_sample)
            selected_pages = [pages[i] for i in selected_indexes if 0 <= i < page_count]
            text_parts: list[str] = []
            table_rows: list[dict[str, Any]] = []
            per_page_meta: list[dict[str, Any]] = []
            if requested_mode in {"text", "hybrid", "ocr"}:
                # OCR currently degrades gracefully to native text when OCR deps are unavailable.
                if requested_mode == "ocr":
                    resolved_mode = "ocr_fallback_text"
                for page_idx, page in zip(selected_indexes, selected_pages):
                    page_text = page.extract_text() or ""
                    text_parts.append(page_text)
                    per_page_meta.append(
                        {
                            "page_index": int(page_idx),
                            "text_bytes": int(len(page_text.encode("utf-8"))),
                            "extracted_chars": int(len(page_text)),
                        }
                    )
            if requested_mode in {"tables", "hybrid"}:
                for page_idx, page in zip(selected_indexes, selected_pages):
                    try:
                        tables = page.extract_tables() or []
                    except Exception:
                        tables = []
                    if requested_mode == "tables":
                        per_page_meta.append(
                            {
                                "page_index": int(page_idx),
                                "text_bytes": 0,
                                "extracted_chars": 0,
                                "table_count": int(len(tables)),
                            }
                        )
                    for table_idx, table in enumerate(tables):
                        if not isinstance(table, list) or not table:
                            continue
                        header = [str(c or f"column_{i+1}") for i, c in enumerate(table[0] or [])]
                        for row in table[1:]:
                            values = row if isinstance(row, list) else []
                            record = {
                                str(header[i]): (values[i] if i < len(values) else None) for i in range(len(header))
                            }
                            record["__page"] = page_idx
                            record["__table"] = table_idx
                            table_rows.append(record)
            if requested_mode == "tables":
                rows = table_rows
            elif requested_mode == "hybrid":
                if output_mode == "table" and table_rows:
                    rows = table_rows
                text_data = "\n\n".join(text_parts)
            else:
                text_data = "\n\n".join(text_parts)
            format_specific_metadata["pdf_metadata"] = {
                "requested_mode": requested_mode,
                "resolved_mode": resolved_mode,
                "page_count": page_count,
                "selected_pages": [int(i) for i in selected_indexes],
                "page_mode": page_mode,
                "table_rows": len(table_rows),
                "confidence": 0.75 if resolved_mode.startswith("ocr") else 0.9,
                "pages": per_page_meta,
            }
    elif schema.file_format in {"jpg", "jpeg", "png", "webp", "gif", "svg", "tif", "tiff"}:
        raw = file_bytes if file_bytes is not None else Path(file_path).read_bytes()
        svg_policy = str(getattr(schema, "image_svg_policy", "sanitize") or "sanitize").strip().lower()
        if schema.file_format == "svg":
            lowered = raw.decode("utf-8", errors="ignore").lower()
            has_script = "<script" in lowered or "onload=" in lowered or "onerror=" in lowered
            if has_script and svg_policy == "reject":
                raise ValueError("SVG rejected by image_svg_policy=reject due to active content")
            if has_script and svg_policy == "sanitize":
                raw = _sanitize_svg_bytes(raw)
        binary_data = raw
        if bool(getattr(schema, "image_extract_metadata", True)):
            format_specific_metadata["image_metadata"] = _extract_image_metadata(
                binary_data,
                schema.file_format,
                str(getattr(schema, "image_tiff_pages_mode", "first") or "first"),
            )
            format_specific_metadata["image_metadata"]["svg_policy"] = svg_policy if schema.file_format == "svg" else "n/a"
    elif schema.file_format in {"mp3", "wav", "flac", "ogg", "m4a", "aac"}:
        raw = file_bytes if file_bytes is not None else Path(file_path).read_bytes()
        normalize_audio = bool(getattr(schema, "audio_normalize", False))
        target_peak = float(getattr(schema, "audio_target_peak", 0.9) or 0.9)
        transcode_target = str(getattr(schema, "audio_transcode_format", "") or "").strip().lower() or None
        normalize_meta: Dict[str, Any] = {"applied": False}
        transcode_meta: Dict[str, Any] = {"applied": False}
        if normalize_audio:
            if schema.file_format == "wav":
                raw, normalize_meta = _normalize_wav_bytes(raw, target_peak)
            else:
                normalize_meta = {"applied": False, "reason": "normalize_supported_for_wav_only"}
        if transcode_target and transcode_target != schema.file_format:
            transcode_meta = {"applied": False, "reason": "transcode_not_available"}
        binary_data = raw
        if bool(getattr(schema, "audio_extract_metadata", True)):
            audio_meta: Dict[str, Any] = {
                "format": schema.file_format,
                "codec": "unknown",
                "normalize": normalize_meta,
                "transcode": transcode_meta,
            }
            if schema.file_format == "wav":
                audio_meta.update(_extract_wav_metadata(binary_data))
            format_specific_metadata["audio_metadata"] = audio_meta
    elif schema.file_format in {"mp4", "mov", "webm"}:
        raw = file_bytes if file_bytes is not None else Path(file_path).read_bytes()
        binary_data = raw
        if bool(getattr(schema, "video_extract_metadata", True)):
            frame_mode = str(getattr(schema, "video_frame_mode", "none") or "none")
            frame_interval = float(getattr(schema, "video_frame_interval_sec", 1.0) or 1.0)
            max_frames = int(getattr(schema, "video_max_frames", 5) or 5)
            format_specific_metadata["video_metadata"] = {
                "format": schema.file_format,
                "codec": "unknown",
                "duration_sec": None,
                "resolution": None,
                "frame_extraction": {
                    "requested_mode": frame_mode,
                    "interval_sec": frame_interval,
                    "max_frames": max_frames,
                    "applied": False,
                    "reason": "video_decoder_not_available",
                    "artifacts": [],
                },
            }
    else:
        binary_data = file_bytes if file_bytes is not None else Path(file_path).read_bytes()

    if output_mode == "table":
        if rows is not None:
            if schema.file_format in {"csv", "tsv", "parquet", "excel"}:
                table_coercion = {"mode": "native", "lossy": False}
            elif schema.file_format == "json":
                rows, json_mode = _table_rows_from_json_array(rows)
                flatten_strategy = str(getattr(schema, "json_flatten_strategy", "none") or "none")
                flatten_separator = str(getattr(schema, "json_flatten_separator", ".") or ".")
                if flatten_strategy in {"shallow", "deep"}:
                    rows = [
                        _flatten_json_record(row, strategy=flatten_strategy, separator=flatten_separator)
                        if isinstance(row, dict)
                        else {"value": row}
                        for row in rows
                    ]
                    format_specific_metadata["json_flatten"] = {
                        "strategy": flatten_strategy,
                        "separator": flatten_separator,
                    }
                table_coercion = {"mode": json_mode, "lossy": False}
            data = _canonical_table_rows(rows)
        elif isinstance(json_data, dict):
            flatten_strategy = str(getattr(schema, "json_flatten_strategy", "none") or "none")
            flatten_separator = str(getattr(schema, "json_flatten_separator", ".") or ".")
            record = (
                _flatten_json_record(json_data, strategy=flatten_strategy, separator=flatten_separator)
                if flatten_strategy in {"shallow", "deep"}
                else json_data
            )
            data = [record]
            if flatten_strategy in {"shallow", "deep"}:
                format_specific_metadata["json_flatten"] = {
                    "strategy": flatten_strategy,
                    "separator": flatten_separator,
                }
            table_coercion = {"mode": "json_object_1row", "lossy": False}
        elif text_data is not None:
            data = [{"text": text_data}]
            lossy = bool(schema.file_format == "pdf")
            table_coercion = {
                "mode": "text_1row",
                "lossy": lossy,
                **({"notes": "PDF text extraction may be partial."} if lossy else {}),
            }
        elif binary_data is not None:
            data = [{"binary_hex": binary_data.hex()}]
            table_coercion = {"mode": "binary_hex_1row", "lossy": False}
        else:
            data = []
            table_coercion = {"mode": "native", "lossy": False}
        if table_columns is None:
            table_columns = _infer_table_columns_from_rows(data if isinstance(data, list) else [])
        _log_source_inference(node_id, "file", table_columns)
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
        schema_extra={
            "file_format": schema.file_format,
            **({"header_detected": csv_has_header} if schema.file_format in {"csv", "tsv"} else {}),
            **({"header_detected": excel_has_header} if schema.file_format == "excel" else {}),
            **(
                {
                    "csv_dialect": {
                        "delimiter": delimiter,
                        "quote_char": (quote_char if quote_char else "\""),
                        "escape_char": (escape_char if escape_char else None),
                        "malformed_row_policy": str(getattr(schema, "malformed_row_policy", "fail")),
                    }
                }
                if schema.file_format in {"csv", "tsv"}
                else {}
            ),
            **({"table_coercion": table_coercion} if output_mode == "table" and table_coercion else {}),
            **({"table_columns": table_columns} if output_mode == "table" and isinstance(table_columns, list) else {}),
            **format_specific_metadata,
        },
        source_observability=_build_source_observability(
            source_kind="file",
            output_mode=output_mode,
            input_bytes=(
                len(file_bytes)
                if isinstance(file_bytes, (bytes, bytearray))
                else (int(file_path.stat().st_size) if isinstance(file_path, Path) and file_path.exists() else None)
            ),
            output_rows=(len(data) if isinstance(data, list) else None),
            null_ratio=(
                _table_null_ratio(data)
                if output_mode == "table" and isinstance(data, list) and all(isinstance(r, dict) for r in data)
                else None
            ),
            partition_count=1,
        ),
    )
    return NodeOutput(status="succeeded", data=data, metadata=metadata, execution_time_ms=0.0)


async def _handle_database_source(
    node_id: str,
    params: Dict[str, Any],
    bus: RunEventBus,
    run_id: str,
    graph_id: str,
    forced_output_mode: Optional[str] = None,
) -> NodeOutput:
    if not HAS_DATABASE:
        raise ImportError("Database support requires sqlalchemy")
    schema = SourceDatabaseParams.model_validate(params)
    output_mode = forced_output_mode or "table"

    conn_string = schema.connection_string
    if not conn_string and schema.connection_ref:
        conn_string = _resolve_connection_ref(str(schema.connection_ref))
    if not conn_string:
        raise ValueError("connection_string or connection_ref required")

    table_schema_name: Optional[str] = None
    table_name_value: Optional[str] = None
    if schema.table_name:
        table_schema_name, table_name_value = _validate_table_identifier(str(schema.table_name))

    engine = sqlalchemy.create_engine(conn_string)
    try:
        if schema.query:
            query = schema.query
            if schema.limit:
                query = f"{query.rstrip(';')} LIMIT {schema.limit}"
            df = pd.read_sql(query, engine)
        elif table_name_value:
            md = sqlalchemy.MetaData()
            table = sqlalchemy.Table(table_name_value, md, schema=table_schema_name, autoload_with=engine)
            stmt = sqlalchemy.select(table)
            if schema.limit:
                stmt = stmt.limit(int(schema.limit))
            df = pd.read_sql(stmt, engine)
        else:
            raise ValueError("Either query or table_name required")

        rows = df.to_dict(orient="records")
        incremental_meta: Optional[Dict[str, Any]] = None
        if isinstance(schema.incremental, dict) and bool(schema.incremental.get("enabled")):
            rows, incremental_meta = _apply_incremental_rows(
                rows=rows,
                graph_id=graph_id,
                node_id=node_id,
                incremental_spec=schema.incremental,
            )
            df = pd.DataFrame(rows)
        table_columns = _infer_table_columns_from_dataframe(df)
        if output_mode == "table":
            data: Any = _canonical_table_rows(rows)
            _log_source_inference(node_id, "database", table_columns)
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
            schema_extra=(
                {
                    **({"table_columns": table_columns} if output_mode == "table" else {}),
                    **({"incremental": incremental_meta} if incremental_meta else {}),
                }
                if (output_mode == "table" or incremental_meta)
                else None
            ),
            source_observability=_build_source_observability(
                source_kind="database",
                output_mode=output_mode,
                output_rows=(len(data) if isinstance(data, list) else len(rows)),
                null_ratio=(
                    _table_null_ratio(data)
                    if output_mode == "table" and isinstance(data, list) and all(isinstance(r, dict) for r in data)
                    else None
                ),
                partition_count=1,
            ),
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
    forced_output_mode: Optional[str] = None,
) -> NodeOutput:
    schema = SourceAPIParams.model_validate(params)
    output_mode = forced_output_mode or "json"

    headers = {str(k): str(v) for k, v in dict(schema.headers).items()}
    headers = {k: v for k, v in headers.items() if k.lower() != "content-type"}
    if schema.content_type:
        headers["Content-Type"] = str(schema.content_type)
    if schema.auth_type != "none" and not schema.auth_token_ref:
        raise ValueError("MISSING_SECRET: params.auth_token_ref is required when authentication is enabled")
    if schema.auth_type == "bearer":
        token = _resolve_required_env(str(schema.auth_token_ref or ""), param_path="params.auth_token_ref")
        headers["Authorization"] = f"Bearer {token}"
    elif schema.auth_type == "basic":
        raw = _resolve_required_env(str(schema.auth_token_ref or ""), param_path="params.auth_token_ref")
        import base64

        headers["Authorization"] = f"Basic {base64.b64encode(raw.encode('utf-8')).decode('ascii')}"
    elif schema.auth_type == "api_key":
        headers["X-API-Key"] = _resolve_required_env(
            str(schema.auth_token_ref or ""), param_path="params.auth_token_ref"
        )

    base_url = _merge_query_into_url(schema.url, schema.query)
    base_request_kwargs: Dict[str, Any] = {
        "method": schema.method,
        "url": base_url,
        "headers": headers,
        "timeout": schema.timeout_seconds,
    }

    if schema.body_mode == "json":
        base_request_kwargs["json"] = schema.body_json or {}
    elif schema.body_mode == "form":
        base_request_kwargs["data"] = _sorted_string_map(schema.body_form)
    elif schema.body_mode == "multipart":
        form = _sorted_string_map(schema.body_form)
        base_request_kwargs["files"] = [(k, (None, v)) for k, v in form.items()]
    elif schema.body_mode == "raw":
        base_request_kwargs["content"] = (schema.body_raw or "").encode("utf-8")

    retry_cfg = schema.retry if isinstance(schema.retry, dict) else {}
    max_attempts = int(retry_cfg.get("max_attempts") or 1)
    max_attempts = max(1, max_attempts)
    backoff_seconds = float(retry_cfg.get("backoff_seconds") or 0.0)
    jitter_seconds = float(retry_cfg.get("jitter_seconds") or 0.0)
    retry_on_status_raw = retry_cfg.get("retry_on_status") if isinstance(retry_cfg.get("retry_on_status"), list) else []
    retry_on_status = {int(s) for s in retry_on_status_raw if isinstance(s, int)}
    if not retry_on_status:
        retry_on_status = {429, 500, 502, 503, 504}
    rate_cfg = schema.rate_limit if isinstance(schema.rate_limit, dict) else {}
    rps = float(rate_cfg.get("rps") or 0.0)
    min_interval = (1.0 / rps) if rps > 0 else 0.0
    last_request_monotonic = 0.0
    retry_attempts_total = 0

    partition_spec = schema.partition if isinstance(schema.partition, dict) else {}
    planned_partitions = _plan_partitions(partition_spec)
    bind_key = str(partition_spec.get("bind_key") or "partition").strip() or "partition"
    parallelism_cap = int(partition_spec.get("parallelism_cap") or 2)
    parallelism_cap = max(1, parallelism_cap)
    partition_on_error = str(partition_spec.get("on_error") or "fail_fast").strip().lower()
    if partition_on_error not in {"fail_fast", "skip_failed"}:
        partition_on_error = "fail_fast"

    async def _request_with_retry(client: httpx.AsyncClient, request_kwargs: Dict[str, Any]) -> httpx.Response:
        nonlocal last_request_monotonic, retry_attempts_total
        response = None
        for attempt in range(1, max_attempts + 1):
            retry_attempts_total += 1
            now_mono = time.monotonic()
            if min_interval > 0:
                wait_for = (last_request_monotonic + min_interval) - now_mono
                if wait_for > 0:
                    await asyncio.sleep(wait_for)
            last_request_monotonic = time.monotonic()
            try:
                response = await client.request(**request_kwargs)
                response.raise_for_status()
                return response
            except httpx.HTTPStatusError as exc:
                status = int(exc.response.status_code) if exc.response is not None else 0
                retryable = status in retry_on_status
                if (not retryable) or attempt >= max_attempts:
                    raise
            except httpx.RequestError:
                if attempt >= max_attempts:
                    raise
            delay = max(0.0, backoff_seconds * (2 ** (attempt - 1)))
            if jitter_seconds > 0:
                delay += random.uniform(0.0, jitter_seconds)
            await bus.emit(
                {
                    "type": "log",
                    "runId": run_id,
                    "at": _iso_now(),
                    "level": "warning",
                    "message": f"API retry attempt={attempt} delay_s={round(delay, 3)}",
                    "nodeId": node_id,
                }
            )
            if delay > 0:
                await asyncio.sleep(delay)
        if response is None:
            raise RuntimeError("API request produced no response")
        return response

    async def _decode_partition_response(idx: int, part_id: str, response: httpx.Response) -> Dict[str, Any]:
        content_type = response.headers.get("content-type", "")
        is_json = "application/json" in content_type
        json_payload: Any = response.json() if is_json else None
        text_payload = response.text if not is_json else json.dumps(json_payload, sort_keys=True, separators=(",", ":"))
        if output_mode == "table":
            if isinstance(json_payload, list):
                rows, _json_mode = _table_rows_from_json_array(json_payload)
                part_data = _canonical_table_rows(rows)
            elif isinstance(json_payload, dict):
                part_data = [json_payload]
            else:
                part_data = [{"text": line} for line in text_payload.splitlines() if line.strip()]
        elif output_mode == "json":
            part_data = json_payload if json_payload is not None else {"text": text_payload}
        elif output_mode == "binary":
            part_data = response.content
        else:
            part_data = text_payload
        return {"index": idx, "partition_id": part_id, "data": part_data}

    partition_results: list[Dict[str, Any]] = []
    if planned_partitions:
        sem = asyncio.Semaphore(parallelism_cap)

        async def _run_partition(part: Dict[str, Any], client: httpx.AsyncClient) -> Dict[str, Any]:
            idx = int(part.get("index") or 0)
            part_id = str(part.get("partition_id") or idx)
            value = part.get("value")
            req = dict(base_request_kwargs)
            req["url"] = _merge_query_into_url(str(base_request_kwargs.get("url") or ""), {bind_key: value})
            await bus.emit(
                {
                    "type": "log",
                    "runId": run_id,
                    "at": _iso_now(),
                    "level": "info",
                    "message": f"partition:start id={part_id} index={idx}",
                    "nodeId": node_id,
                }
            )
            try:
                async with sem:
                    resp = await _request_with_retry(client, req)
                    decoded = await _decode_partition_response(idx, part_id, resp)
            except Exception as exc:
                raise RuntimeError(
                    f"PARTITION_FAILED: id={part_id} index={idx} bind_key={bind_key} reason={str(exc)}"
                ) from exc
            await bus.emit(
                {
                    "type": "log",
                    "runId": run_id,
                    "at": _iso_now(),
                    "level": "info",
                    "message": f"partition:done id={part_id} index={idx}",
                    "nodeId": node_id,
                }
            )
            return decoded

        failed_partitions: list[Dict[str, Any]] = []
        async with httpx.AsyncClient() as client:
            gathered = await asyncio.gather(
                *[_run_partition(p, client) for p in planned_partitions],
                return_exceptions=(partition_on_error == "skip_failed"),
            )
        if partition_on_error == "skip_failed":
            for idx, item in enumerate(gathered):
                if isinstance(item, Exception):
                    spec = planned_partitions[idx] if idx < len(planned_partitions) else {}
                    failed_partitions.append(
                        {
                            "index": int(spec.get("index") or idx),
                            "partition_id": str(spec.get("partition_id") or idx),
                            "error": str(item),
                        }
                    )
                elif isinstance(item, dict):
                    partition_results.append(item)
            if failed_partitions:
                await bus.emit(
                    {
                        "type": "log",
                        "runId": run_id,
                        "at": _iso_now(),
                        "level": "warning",
                        "message": f"partition:skip_failed count={len(failed_partitions)}",
                        "nodeId": node_id,
                    }
                )
            if not partition_results:
                raise RuntimeError("PARTITION_FAILED: all partitions failed under skip_failed policy")
        else:
            partition_results = [item for item in gathered if isinstance(item, dict)]
        data = _merge_partition_results(output_mode, partition_results)
    else:
        async with httpx.AsyncClient() as client:
            response = await _request_with_retry(client, dict(base_request_kwargs))
        decoded = await _decode_partition_response(0, "default", response)
        data = decoded.get("data")
    table_coercion: Dict[str, Any] | None = None
    table_columns: list[dict[str, str]] | None = None
    incremental_meta: Dict[str, Any] | None = None
    incremental_spec = schema.incremental if isinstance(schema.incremental, dict) else {}

    if output_mode == "table":
        if isinstance(data, list):
            table_coercion = {"mode": "partition_merge" if planned_partitions else "native", "lossy": False}
        table_columns = _infer_table_columns_from_rows(data if isinstance(data, list) else [])
        _log_source_inference(node_id, "api", table_columns)
    elif output_mode == "json":
        if bool(incremental_spec.get("enabled")) and isinstance(data, list) and all(isinstance(r, dict) for r in data):
            filtered_rows, incremental_meta = _apply_incremental_rows(
                rows=[dict(r) for r in data if isinstance(r, dict)],
                graph_id=graph_id,
                node_id=node_id,
                incremental_spec=incremental_spec,
            )
            data = filtered_rows

    metadata = _metadata_for_output(
        graph_id=graph_id,
        node_id=node_id,
        source_kind="api",
        output_mode=output_mode,
        data=data,
        params=params,
        schema_extra={
            **({"table_coercion": table_coercion} if output_mode == "table" and table_coercion else {}),
            **({"table_columns": table_columns} if output_mode == "table" and isinstance(table_columns, list) else {}),
            **({"incremental": incremental_meta} if incremental_meta else {}),
            **(
                {
                    "partitions": {
                        "count": len(planned_partitions),
                        "on_error": partition_on_error,
                        "parallelism_cap": parallelism_cap,
                        "ids": [str(p.get("partition_id") or "") for p in sorted(partition_results, key=lambda x: int(x.get("index") or 0))],
                        "failed": (
                            [
                                {
                                    "index": int(f.get("index") or 0),
                                    "partition_id": str(f.get("partition_id") or ""),
                                    "error": str(f.get("error") or ""),
                                }
                                for f in failed_partitions
                            ]
                            if "failed_partitions" in locals() and failed_partitions
                            else []
                        ),
                    }
                }
                if planned_partitions
                else {}
            ),
        }
        if output_mode in {"table", "json"} and (table_coercion or table_columns or incremental_meta or planned_partitions)
        else None,
        source_observability=_build_source_observability(
            source_kind="api",
            output_mode=output_mode,
            output_rows=(len(data) if isinstance(data, list) else None),
            null_ratio=(
                _table_null_ratio(data)
                if output_mode == "table" and isinstance(data, list) and all(isinstance(r, dict) for r in data)
                else None
            ),
            retry_count=max(0, retry_attempts_total - (len(planned_partitions) if planned_partitions else 1)),
            partition_count=(len(planned_partitions) if planned_partitions else 1),
        ),
    )
    return NodeOutput(status="succeeded", data=data, metadata=metadata, execution_time_ms=0.0)


async def _handle_object_store_source(
    node_id: str,
    params: Dict[str, Any],
    bus: RunEventBus,
    run_id: str,
    graph_id: str,
    forced_output_mode: Optional[str] = None,
) -> NodeOutput:
    schema = SourceObjectStoreParams.model_validate(params)
    output_mode = forced_output_mode or _default_file_output_mode(str(schema.file_format))

    await bus.emit(
        {
            "type": "log",
            "runId": run_id,
            "at": _iso_now(),
            "level": "info",
            "message": f"object_store: provider={schema.provider} bucket={schema.bucket} key={schema.key}",
            "nodeId": node_id,
        }
    )

    data_bytes: Optional[bytes] = None
    if isinstance(params.get("mock_text"), str):
        data_bytes = str(params.get("mock_text") or "").encode(str(schema.encoding or "utf-8"), errors="replace")

    if data_bytes is None:
        root = str(os.getenv("OBJECT_STORE_MOCK_ROOT", "")).strip()
        key_path = str(schema.key or "").strip()
        candidate_paths: list[Path] = []
        if root:
            candidate_paths.append(Path(root) / str(schema.bucket or "") / key_path)
        candidate_paths.append(Path(key_path))
        for path in candidate_paths:
            try:
                if path.exists() and path.is_file():
                    data_bytes = path.read_bytes()
                    break
            except Exception:
                continue
    if data_bytes is None:
        raise FileNotFoundError(
            f"Object not found for bucket/key ({schema.bucket}/{schema.key}). Set OBJECT_STORE_MOCK_ROOT or provide mock_text."
        )

    rows: Optional[list[dict[str, Any]]] = None
    text_data: Optional[str] = None
    json_data: Any = None
    binary_data: Optional[bytes] = None
    table_columns: Optional[list[dict[str, str]]] = None
    ff = str(schema.file_format or "").strip().lower()

    if ff in {"csv", "tsv"}:
        df = pd.read_csv(
            io.BytesIO(data_bytes),
            delimiter=("\t" if ff == "tsv" else ","),
            encoding=str(schema.encoding or "utf-8"),
        )
        rows = df.to_dict(orient="records")
        table_columns = _infer_table_columns_from_dataframe(df)
    elif ff == "json":
        raw_json = data_bytes.decode(str(schema.encoding or "utf-8"), errors="replace")
        json_data = json.loads(raw_json)
        if isinstance(json_data, list):
            rows = json_data
    elif ff == "txt":
        text_data = data_bytes.decode(str(schema.encoding or "utf-8"), errors="replace")
    else:
        binary_data = data_bytes

    if output_mode == "table":
        if rows is not None:
            data = _canonical_table_rows(rows)
        elif isinstance(json_data, dict):
            data = [json_data]
        elif text_data is not None:
            data = [{"text": text_data}]
        else:
            data = [{"binary_hex": (binary_data or b"").hex()}]
        if table_columns is None:
            table_columns = _infer_table_columns_from_rows(data if isinstance(data, list) else [])
        _log_source_inference(node_id, "object_store", table_columns)
    elif output_mode == "json":
        if json_data is not None:
            data = json_data
        elif rows is not None:
            data = rows
        elif text_data is not None:
            data = {"text": text_data}
        else:
            data = {"binary_hex": (binary_data or b"").hex()}
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
        source_kind="object_store",
        output_mode=output_mode,
        data=data,
        params=params,
        schema_extra={
            "provider": schema.provider,
            "bucket": schema.bucket,
            "key": schema.key,
            **({"table_columns": table_columns} if output_mode == "table" and isinstance(table_columns, list) else {}),
        },
        source_observability=_build_source_observability(
            source_kind="object_store",
            output_mode=output_mode,
            input_bytes=len(data_bytes),
            output_rows=(len(data) if isinstance(data, list) else None),
            null_ratio=(
                _table_null_ratio(data)
                if output_mode == "table" and isinstance(data, list) and all(isinstance(r, dict) for r in data)
                else None
            ),
            partition_count=1,
        ),
    )
    return NodeOutput(status="succeeded", data=data, metadata=metadata, execution_time_ms=0.0)


async def _handle_warehouse_source(
    node_id: str,
    params: Dict[str, Any],
    bus: RunEventBus,
    run_id: str,
    graph_id: str,
    forced_output_mode: Optional[str] = None,
) -> NodeOutput:
    schema = SourceWarehouseParams.model_validate(params)
    output_mode = forced_output_mode or "table"

    rows_override = params.get("mock_rows")
    if isinstance(rows_override, list):
        rows = [dict(r) for r in rows_override if isinstance(r, dict)]
        df = pd.DataFrame(rows)
    else:
        if not HAS_DATABASE:
            raise ImportError("Warehouse support requires sqlalchemy (or provide mock_rows for local testing)")
        conn_string = schema.connection_string
        if not conn_string and schema.connection_ref:
            conn_string = _resolve_connection_ref(str(schema.connection_ref))
        if not conn_string:
            raise ValueError("connection_string or connection_ref required")
        engine = sqlalchemy.create_engine(conn_string)
        try:
            query = str(schema.query or "")
            if schema.limit:
                query = f"{query.rstrip(';')} LIMIT {schema.limit}"
            df = pd.read_sql(query, engine)
        finally:
            engine.dispose()
        rows = df.to_dict(orient="records")

    table_columns = _infer_table_columns_from_dataframe(df)
    if output_mode == "table":
        data: Any = _canonical_table_rows(rows)
        _log_source_inference(node_id, "warehouse", table_columns)
    elif output_mode == "json":
        data = rows
    elif output_mode == "binary":
        data = df.to_csv(index=False, lineterminator="\n").encode("utf-8")
    else:
        data = df.to_csv(index=False, lineterminator="\n")

    await bus.emit(
        {
            "type": "log",
            "runId": run_id,
            "at": _iso_now(),
            "level": "info",
            "message": f"warehouse: provider={schema.provider} rows={len(rows)}",
            "nodeId": node_id,
        }
    )

    metadata = _metadata_for_output(
        graph_id=graph_id,
        node_id=node_id,
        source_kind="warehouse",
        output_mode=output_mode,
        data=data,
        params=params,
        schema_extra={
            "provider": schema.provider,
            **({"table_columns": table_columns} if output_mode == "table" else {}),
        },
        source_observability=_build_source_observability(
            source_kind="warehouse",
            output_mode=output_mode,
            output_rows=(len(data) if isinstance(data, list) else len(rows)),
            null_ratio=(
                _table_null_ratio(data)
                if output_mode == "table" and isinstance(data, list) and all(isinstance(r, dict) for r in data)
                else None
            ),
            partition_count=1,
        ),
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
