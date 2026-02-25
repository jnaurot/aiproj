from __future__ import annotations

import csv
import io
import os
from typing import Any

from .metadata import GraphContext


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = str(os.getenv(name, "")).strip()
    if not raw:
        return default
    try:
        v = int(raw)
    except Exception:
        return default
    return max(minimum, v)


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, "")).strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _parse_charset(content_type: str) -> str:
    ctype = str(content_type or "")
    for part in ctype.split(";"):
        p = part.strip()
        if p.lower().startswith("charset="):
            return p.split("=", 1)[1].strip().strip('"') or "utf-8"
    return "utf-8"


def _decode_bytes(data: bytes, content_type: str) -> str:
    charset = _parse_charset(content_type)
    try:
        text = data.decode(charset, errors="replace")
    except Exception:
        text = data.decode("utf-8", errors="replace")
    return text.lstrip("\ufeff")


def _clip_chars(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return f"{text[:max_chars]}\n\n[TRUNCATED: max_chars={max_chars}]"


def _table_bytes_to_csv_text(
    data: bytes,
    content_type: str,
    *,
    max_rows: int,
    max_cols: int,
    max_chars: int,
    sort_rows: bool,
) -> str:
    raw = _decode_bytes(data, content_type)
    sep = "\t" if "tab-separated-values" in (content_type or "").lower() else ","
    reader = csv.reader(io.StringIO(raw), delimiter=sep)
    rows = list(reader)
    if not rows:
        return ""

    if sort_rows and len(rows) > 2:
        header = rows[0]
        body = rows[1:]
        body_sorted = sorted(body, key=lambda r: tuple(str(v) for v in r))
        rows = [header] + body_sorted

    clipped_rows = rows[: max_rows + 1]  # header + rows
    clipped_rows = [r[:max_cols] for r in clipped_rows]
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    for r in clipped_rows:
        writer.writerow(r)
    txt = out.getvalue()
    if len(rows) > len(clipped_rows):
        txt += f"[TRUNCATED: max_rows={max_rows}]\n"
    if any(len(r) > max_cols for r in rows):
        txt += f"[TRUNCATED: max_cols={max_cols}]\n"
    return _clip_chars(txt, max_chars)


async def materialize_text(context: GraphContext, artifact_id: str) -> str:
    art = await context.artifact_store.get(artifact_id)
    b = await context.artifact_store.read(artifact_id)
    mime = str(getattr(art, "mime_type", "") or "")
    payload_schema: Any = getattr(art, "payload_schema", None) or {}
    payload_type = str(payload_schema.get("type") or "").lower() if isinstance(payload_schema, dict) else ""
    port_type = str(getattr(art, "port_type", "") or "").lower()

    max_rows = _env_int("LLM_TABLE_MAX_ROWS", 200)
    max_cols = _env_int("LLM_TABLE_MAX_COLS", 50)
    max_chars = _env_int("LLM_PROMPT_MAX_CHARS", 20000)
    sort_rows = _env_bool("LLM_TABLE_SORT_ROWS", True)

    is_table = (
        port_type == "table"
        or payload_type == "table"
        or "csv" in mime.lower()
        or "tab-separated-values" in mime.lower()
    )
    if is_table:
        return _table_bytes_to_csv_text(
            b,
            mime,
            max_rows=max_rows,
            max_cols=max_cols,
            max_chars=max_chars,
            sort_rows=sort_rows,
        )
    return _clip_chars(_decode_bytes(b, mime), max_chars)
