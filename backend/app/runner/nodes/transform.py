from __future__ import annotations

import io
import json
import hashlib
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import pandas as pd

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
    "sql": "sql",
    "python": "code",
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
    for k in ("filter","select","rename","derive","aggregate","join","sort","limit","dedupe","sql","code"):
        if k != keep_key:
            p.pop(k, None)

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
    con = duckdb.connect(database=":memory:")
    try:
        # register input tables
        primary_name = "input"
        
        primary_df = inputs.get("in")
        if primary_df is None:
            if not inputs:
                raise ValueError("Transform has no input tables (expected at least one).")
            primary_df = next(iter(inputs.values()))

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

        elif op == "python":
            # IMPORTANT: you can enable later behind an explicit "unsafe_allow_code" flag
            raise ValueError("python transform is disabled for determinism (enable explicitly later).")

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
