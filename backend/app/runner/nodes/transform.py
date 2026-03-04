from __future__ import annotations

import io
import json
import hashlib
import re
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

try:
    import duckdb
except ModuleNotFoundError:  # pragma: no cover - exercised in lightweight test envs
    duckdb = None
import pandas as pd

logger = logging.getLogger(__name__)

OP_KEYS = {
    "filter": "filter",
    "select": "select",
    "rename": "rename",
    "derive": "derive",
    "aggregate": "aggregate",
    "join": "join",
    "sort": "sort",
    "limit": "limit",
    "dedupe": "dedupe",
    "split": "split",
    "sql": "sql",
}

# ---- helpers ----

def sha256_hex(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def quote_ident(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'

def canonical_json(obj: Any) -> str:
    # stable serialization for hashing / caching
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

def normalize_transform_params(params: Dict[str, Any], default_op: Optional[str] = None) -> Dict[str, Any]:
    p = dict(params)

    op = p.get("op") or default_op
    if op is None:
        inferred: List[str] = []
        for candidate_op, payload_key in OP_KEYS.items():
            if payload_key in p:
                inferred.append(candidate_op)
        inferred = sorted(set(inferred))
        if len(inferred) == 1:
            op = inferred[0]

    if op == "python":
        raise ValueError('Transform op "python" has been removed. Use Tool node provider="python".')

    if op not in OP_KEYS:
        raise ValueError(f"Transform params missing/invalid op: {op}")

    p["op"] = op

    # Legacy shape compatibility for LIMIT:
    # { op: "limit", n: 100 } -> { op: "limit", limit: { n: 100 } }
    if op == "limit" and "limit" not in p and "n" in p:
        p["limit"] = {"n": int(p.get("n") or 0)}

    p["enabled"] = bool(p.get("enabled", True))
    p.pop("notes", None)

    # normalize cache
    cache = p.get("cache") or {"enabled": False}
    if not cache.get("enabled", False):
        p["cache"] = {"enabled": False}
    else:
        # keep key only if present; drop UI junk
        p["cache"] = {"enabled": True, **({"key": cache["key"]} if cache.get("key") else {})}

    # keep only the active op payload
    keep_key = OP_KEYS[op]
    for k in ("filter","select","rename","derive","aggregate","join","sort","limit","dedupe","split","sql","code"):
        if k != keep_key:
            p.pop(k, None)

    if op == "dedupe":
        raw = p.get("dedupe") if isinstance(p.get("dedupe"), dict) else {}
        by_raw = raw.get("by")
        by: List[str] = []
        if isinstance(by_raw, list):
            seen: set[str] = set()
            for item in by_raw:
                col = str(item or "").strip()
                if not col or col in seen:
                    continue
                seen.add(col)
                by.append(col)
        all_columns = bool(raw.get("allColumns", len(by) == 0))
        if all_columns:
            by = []
        keep = str(raw.get("keep") or "first")
        stable_order_column = raw.get("stableOrderColumn")
        emit_dropped_count = bool(raw.get("emitDroppedCount", False))
        dedupe_payload: Dict[str, Any] = {"allColumns": all_columns, "by": by, "keep": keep}
        if isinstance(stable_order_column, str) and str(stable_order_column).strip():
            dedupe_payload["stableOrderColumn"] = str(stable_order_column).strip()
        if emit_dropped_count:
            dedupe_payload["emitDroppedCount"] = True
        p["dedupe"] = dedupe_payload

    return p

def inputs_fingerprint(inputs: List[Tuple[str, str]]) -> List[Dict[str, str]]:
    """
    inputs = [(port_name, upstream_artifact_id), ...]
    Return stable sorted list.
    """
    return [{"port": port, "artifact_id": aid} for port, aid in sorted(inputs, key=lambda x: x[0])]

# ---- data contracts ----

@dataclass(frozen=True)
class TransformResult:
    payload_bytes: bytes
    mime_type: str
    meta: Dict[str, Any]

# ---- table IO ----

def normalize_mime_type(raw: str) -> str:
    return (raw or "").split(";", 1)[0].strip().lower()


def _load_table_from_json_text(s: str) -> pd.DataFrame:
    # JSON array/object first, then JSONL fallback.
    try:
        obj = json.loads(s)
        if isinstance(obj, list):
            return pd.DataFrame(obj)
        if isinstance(obj, dict):
            if "rows" in obj and isinstance(obj["rows"], list):
                return pd.DataFrame(obj["rows"])
            return pd.DataFrame([obj])
    except Exception:
        pass

    rows = [json.loads(line) for line in s.splitlines() if line.strip()]
    return pd.DataFrame(rows)


def _load_table_from_plain_text(b: bytes) -> pd.DataFrame:
    s = b.decode("utf-8", errors="replace")
    # Deterministic text-table bridge: one non-empty line per row.
    lines = [ln for ln in s.splitlines() if ln.strip()]
    if not lines:
        return pd.DataFrame({"text": []})
    return pd.DataFrame({"text": lines})


def load_table_from_artifact_bytes(mime_type: str, b: bytes) -> pd.DataFrame:
    mt = normalize_mime_type(mime_type)

    # CSV / delimited text
    if mt in ("text/csv", "application/csv", "text/tab-separated-values"):
        sep = "\t" if mt == "text/tab-separated-values" else ","
        return pd.read_csv(io.BytesIO(b), sep=sep)
    if mt in ("text/plain", "application/plain"):
        return _load_table_from_plain_text(b)

    # JSON / JSONL
    if mt in ("application/json", "application/x-ndjson", "application/jsonl"):
        s = b.decode("utf-8", errors="replace")
        return _load_table_from_json_text(s)

    # Parquet
    if mt in ("application/vnd.apache.parquet", "application/x-parquet"):
        return pd.read_parquet(io.BytesIO(b))

    # Excel
    if mt in (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.ms-excel",
    ):
        return pd.read_excel(io.BytesIO(b))

    raise ValueError(
        "Unsupported input mime_type for Transform table operations: "
        f"{mime_type!r}. Supported: text/csv, text/tab-separated-values, "
        "application/json, application/x-ndjson, application/jsonl, "
        "application/vnd.apache.parquet, application/vnd.openxmlformats-officedocument.spreadsheetml.sheet."
    )

def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    # Determinism: stable column order as currently in df; stable line endings; no index.
    out = io.StringIO()
    df.to_csv(out, index=False, lineterminator="\n")
    return out.getvalue().encode("utf-8")


def _execute_split_op(primary_df: pd.DataFrame, spec: Dict[str, Any]) -> pd.DataFrame:
    source_col = str(spec.get("sourceColumn") or "text")
    out_col = str(spec.get("outColumn") or "part")
    mode = str(spec.get("mode") or "sentences")
    pattern = str(spec.get("pattern") or "")
    delimiter = str(spec.get("delimiter") or "")
    flags_raw = str(spec.get("flags") or "")
    trim = bool(spec.get("trim", True))
    drop_empty = bool(spec.get("dropEmpty", True))
    emit_index = bool(spec.get("emitIndex", True))
    emit_source_row = bool(spec.get("emitSourceRow", True))
    max_parts = int(spec.get("maxParts") or 5000)
    max_parts = max(1, min(100000, max_parts))
    max_chars = 2_000_000

    flag_value = 0
    if "i" in flags_raw:
        flag_value |= re.IGNORECASE
    if "m" in flags_raw:
        flag_value |= re.MULTILINE
    if "s" in flags_raw:
        flag_value |= re.DOTALL

    if mode == "regex":
        if not pattern.strip():
            raise ValueError("split.pattern is required when mode='regex'")
        splitter = re.compile(pattern, flags=flag_value)
    else:
        splitter = None

    if mode == "delimiter":
        if delimiter == "":
            raise ValueError("split.delimiter is required when mode='delimiter'")
        delimiter = delimiter.replace("\\r", "\r").replace("\\n", "\n").replace("\\t", "\t")

    def _split_text(text: str) -> List[str]:
        if mode == "lines":
            return re.split(r"\r\n|\n|\r", text)
        if mode == "sentences":
            normalized = text.replace("\r\n", "\n").replace("\r", "\n")
            normalized = re.sub(r"\s+", " ", normalized).strip()
            if not normalized:
                return []
            return re.split(r"(?<=[.!?])\s+", normalized)
        if mode == "regex":
            assert splitter is not None
            return splitter.split(text)
        if mode == "delimiter":
            return text.split(delimiter)
        return [text]

    rows_out: List[Dict[str, Any]] = []
    for src_idx, row in enumerate(primary_df.to_dict(orient="records")):
        value = row.get(source_col, "")
        text = "" if value is None else str(value)
        if len(text) > max_chars:
            raise ValueError(
                f"split source value exceeds max chars ({len(text)} > {max_chars}) for row={src_idx}"
            )
        parts = _split_text(text)
        emitted = 0
        for idx, part in enumerate(parts):
            token = part.strip() if trim else part
            if drop_empty and token == "":
                continue
            if emitted >= max_parts:
                logger.warning(
                    "Split capped: emitted=%s parts (maxParts=%s) for row=%s",
                    emitted,
                    max_parts,
                    src_idx,
                )
                break
            out_row: Dict[str, Any] = {out_col: token}
            if emit_index:
                out_row["index"] = idx
            if emit_source_row:
                out_row["source_row"] = src_idx
            rows_out.append(out_row)
            emitted += 1
    return pd.DataFrame(rows_out)


def _execute_dedupe_op(primary_df: pd.DataFrame, spec: Dict[str, Any]) -> pd.DataFrame:
    """
    Deduplicate is a logical TABLE_V1 transform.
    - by=[] means dedupe on the entire row.
    - keep='first' keeps the first row by stable row order (__rowid).
    """
    all_columns = bool(spec.get("allColumns", False))
    by_raw = spec.get("by")
    by: List[str] = []
    if isinstance(by_raw, list):
        seen: set[str] = set()
        for item in by_raw:
            col = str(item or "").strip()
            if not col or col in seen:
                continue
            seen.add(col)
            by.append(col)
    if all_columns:  # legacy compatibility
        by = []
    keep = str(spec.get("keep") or "first")
    if keep != "first":
        raise ValueError("dedupe.keep must be 'first'")
    if (not all_columns) and (len(by) == 0):
        raise ValueError("dedupe.by must include at least one column when allColumns=false")

    cols = [str(c) for c in list(primary_df.columns)]
    missing = [c for c in by if c not in cols]
    if missing:
        raise ValueError(f"dedupe.by columns missing from input: {', '.join(missing)}")

    working = primary_df.reset_index(drop=True).copy()
    working["__rowid"] = range(len(working))
    working = working.sort_values("__rowid", kind="stable")

    if by:
        deduped = working.drop_duplicates(subset=by, keep="first")
    else:
        deduped = working.drop_duplicates(subset=cols, keep="first") if cols else working.head(1)

    deduped = deduped.sort_values("__rowid", kind="stable").drop(columns=["__rowid"], errors="ignore")
    return deduped.reset_index(drop=True)

# ---- duckdb execution ----

def execute_transform_op(
    op: str,
    params: Dict[str, Any],
    inputs: Dict[str, pd.DataFrame],
    join_lookup: Optional[Dict[str, pd.DataFrame]] = None
) -> pd.DataFrame:
    """
    Execute via DuckDB to keep semantics consistent.
    Convention:
    - primary input table is inputs["in"] (or first input)
    - for join, params.join.withNodeId resolves via join_lookup[nodeId]
    """
    primary_df = inputs.get("in")
    if primary_df is None:
        if not inputs:
            raise ValueError("Transform has no input tables (expected at least one).")
        primary_df = next(iter(inputs.values()))

    if op == "split":
        spec = params["split"]
        return _execute_split_op(primary_df, spec)
    if op == "dedupe":
        spec = params["dedupe"]
        return _execute_dedupe_op(primary_df, spec)

    if duckdb is None:
        raise ModuleNotFoundError("duckdb is required for non-split transform operations")

    con = duckdb.connect(database=":memory:")
    try:
        # register input tables
        primary_name = "input"

        con.register(primary_name, primary_df)

        if op == "filter":
            expr = params["filter"]["expr"]
            return con.execute(f"select * from {primary_name} where {expr}").df()

        elif op == "select":
            cols = params["select"]["columns"]
            col_sql = ", ".join([quote_ident(c) for c in cols])
            return con.execute(f"select {col_sql} from {primary_name}").df()

        elif op == "rename":
            mp = params["rename"]["map"]
            # SELECT col AS new_name for mapped, else keep as-is
            cols = list(primary_df.columns)
            parts = []
            for c in cols:
                if c in mp:
                    parts.append(f"{quote_ident(c)} as {quote_ident(mp[c])}")
                else:
                    parts.append(f"{quote_ident(c)}")
            return con.execute(f"select {', '.join(parts)} from {primary_name}").df()

        elif op == "derive":
            cols = list(primary_df.columns)
            parts = [quote_ident(c) for c in cols]
            for d in params["derive"]["columns"]:
                parts.append(f"({d['expr']}) as {quote_ident(d['name'])}")
            return con.execute(f"select {', '.join(parts)} from {primary_name}").df()

        elif op == "aggregate":
            gb = params["aggregate"]["groupBy"]
            metrics = params["aggregate"]["metrics"]
            gb_sql = ", ".join([quote_ident(c) for c in gb]) if gb else ""
            metrics_sql = ", ".join([f"({m['expr']}) as {quote_ident(m['as'])}" for m in metrics])
            if gb_sql:
                q = f"select {gb_sql}, {metrics_sql} from {primary_name} group by {gb_sql}"
            else:
                q = f"select {metrics_sql} from {primary_name}"
            return con.execute(q).df()

        elif op == "join":
            if not join_lookup:
                raise ValueError("join_lookup missing for join op")
            spec = params["join"]
            with_node = spec["withNodeId"]
            other = join_lookup.get(with_node)
            if other is None:
                raise ValueError(f"join.withNodeId '{with_node}' not found in run context")
            con.register("other", other)

            how = spec["how"].lower()
            how_sql = {"inner":"inner", "left":"left", "right":"right", "full":"full outer"}[how]

            ons = spec["on"]
            on_sql = " AND ".join([
                f"{primary_name}.{quote_ident(x['left'])} = other.{quote_ident(x['right'])}"
                for x in ons
            ])
            return con.execute(f"select * from {primary_name} {how_sql} join other on {on_sql}").df()

        elif op == "sort":
            by = params["sort"]["by"]
            order_sql = ", ".join([f"{quote_ident(x['col'])} {x['dir'].upper()}" for x in by])
            return con.execute(f"select * from {primary_name} order by {order_sql}").df()

        elif op == "limit":
            n = int(params["limit"]["n"])
            return con.execute(f"select * from {primary_name} limit {n}").df()

        elif op == "dedupe":
            by = params["dedupe"].get("by")

            # columns to order by for deterministic “first row”
            all_cols = list(primary_df.columns)
            order_sql = ", ".join([quote_ident(c) for c in all_cols])

            if by:
                part_sql = ", ".join([quote_ident(c) for c in by])
                q = f"""
                select * exclude (rn)
                from (
                    select *,
                        row_number() over (
                            partition by {part_sql}
                            order by {order_sql}
                        ) as rn
                    from {primary_name}
                )
                where rn = 1
                """
                return con.execute(q).df()

            # no 'by' => deterministic unique set; DISTINCT doesn’t guarantee order, so sort it
            q = f"select distinct * from {primary_name} order by {order_sql}"
            return con.execute(q).df()

        elif op == "sql":
            q = params["sql"]["query"]
            # convention: user writes SQL referencing "input" (and optionally "other" if you add)
            return con.execute(q).df()

        raise ValueError(f"Unsupported transform op: {op}")
    finally:
        con.close()

def run_transform(
    *,
    params: Dict[str, Any],
    input_tables: Dict[str, pd.DataFrame],
    join_lookup: Optional[Dict[str, pd.DataFrame]],
) -> TransformResult:
    op = params["op"]
    
    out_df = execute_transform_op(op, params, input_tables, join_lookup=join_lookup)

    payload = df_to_csv_bytes(out_df)
    meta = {
        "row_count": int(len(out_df)),
        "columns": list(out_df.columns),
        "content_hash": sha256_hex(payload),
        "format": "csv",
    }
    return TransformResult(payload_bytes=payload, mime_type="text/csv; charset=utf-8", meta=meta)
