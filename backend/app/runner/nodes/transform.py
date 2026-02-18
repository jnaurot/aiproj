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
    "js": "code",
}

# ---- helpers ----

def sha256_hex(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def canonical_json(obj: Any) -> str:
    # stable serialization for hashing / caching
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

def normalize_transform_params(params: Dict[str, Any]) -> Dict[str, Any]:
    p = dict(params)

    op = p.get("op")
    if op not in OP_KEYS:
        raise ValueError(f"Transform params missing/invalid op: {op}")

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

def load_table_from_artifact_bytes(mime_type: str, b: bytes) -> pd.DataFrame:
    # Minimal: support CSV + JSON (records or JSONL) first.
    # Add parquet once your pipeline wants it.
    if mime_type.startswith("text/csv"):
        return pd.read_csv(io.BytesIO(b))
    if mime_type.startswith("application/json"):
        # allow jsonl or records
        s = b.decode("utf-8")
        if "\n" in s.strip().splitlines()[0]:
            # not reliable; prefer heuristic below
            pass
        try:
            obj = json.loads(s)
            if isinstance(obj, list):
                return pd.DataFrame(obj)
            if isinstance(obj, dict) and "rows" in obj:
                return pd.DataFrame(obj["rows"])
        except Exception:
            # JSONL fallback
            rows = [json.loads(line) for line in s.splitlines() if line.strip()]
            return pd.DataFrame(rows)
    raise ValueError(f"Unsupported input mime_type for Transform: {mime_type}")

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
            col_sql = ", ".join([duckdb.quote_identifier(c) for c in cols])
            return con.execute(f"select {col_sql} from {primary_name}").df()

        elif op == "rename":
            mp = params["rename"]["map"]
            # SELECT col AS new_name for mapped, else keep as-is
            cols = list(primary_df.columns)
            parts = []
            for c in cols:
                if c in mp:
                    parts.append(f"{duckdb.quote_identifier(c)} as {duckdb.quote_identifier(mp[c])}")
                else:
                    parts.append(f"{duckdb.quote_identifier(c)}")
            return con.execute(f"select {', '.join(parts)} from {primary_name}").df()

        elif op == "derive":
            cols = list(primary_df.columns)
            parts = [duckdb.quote_identifier(c) for c in cols]
            for d in params["derive"]["columns"]:
                parts.append(f"({d['expr']}) as {duckdb.quote_identifier(d['name'])}")
            return con.execute(f"select {', '.join(parts)} from {primary_name}").df()

        elif op == "aggregate":
            gb = params["aggregate"]["groupBy"]
            metrics = params["aggregate"]["metrics"]
            gb_sql = ", ".join([duckdb.quote_identifier(c) for c in gb]) if gb else ""
            metrics_sql = ", ".join([f"({m['expr']}) as {duckdb.quote_identifier(m['as'])}" for m in metrics])
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
                f"{primary_name}.{duckdb.quote_identifier(x['left'])} = other.{duckdb.quote_identifier(x['right'])}"
                for x in ons
            ])
            return con.execute(f"select * from {primary_name} {how_sql} join other on {on_sql}").df()

        elif op == "sort":
            by = params["sort"]["by"]
            order_sql = ", ".join([f"{duckdb.quote_identifier(x['col'])} {x['dir'].upper()}" for x in by])
            return con.execute(f"select * from {primary_name} order by {order_sql}").df()

        elif op == "limit":
            n = int(params["limit"]["n"])
            return con.execute(f"select * from {primary_name} limit {n}").df()

        elif op == "dedupe":
            by = params["dedupe"].get("by")

            # columns to order by for deterministic “first row”
            all_cols = list(primary_df.columns)
            order_sql = ", ".join([duckdb.quote_identifier(c) for c in all_cols])

            if by:
                part_sql = ", ".join([duckdb.quote_identifier(c) for c in by])
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

        elif op in ("python", "js"):
            # IMPORTANT: you can enable later behind an explicit "unsafe_allow_code" flag
            raise ValueError("python/js transform is disabled for determinism (enable explicitly later).")

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
