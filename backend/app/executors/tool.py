from __future__ import annotations

import asyncio
import base64
import importlib
import io
import json
import os
import re
import subprocess
import shutil
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from ..runner.metadata import FileMetadata, GraphContext, NodeOutput
from .builtin_profiles import resolve_builtin_environment


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


SENSITIVE_KEYS = {
    "authorization",
    "api_key",
    "apikey",
    "token",
    "password",
    "secret",
    "access_token",
    "refresh_token",
    "auth",
    "credentials",
}


def _is_sensitive_key(k: str) -> bool:
    kl = (k or "").lower()
    return any(s in kl for s in SENSITIVE_KEYS)


def _redact_value(v: Any) -> Any:
    if isinstance(v, dict):
        out = {}
        for k, vv in v.items():
            out[k] = "***REDACTED***" if _is_sensitive_key(str(k)) else _redact_value(vv)
        return out
    if isinstance(v, list):
        return [_redact_value(x) for x in v]
    return v


def _extract_tool_identity(params: Dict[str, Any]) -> Tuple[str, str]:
    provider = params.get("provider") or "unknown"
    name = params.get("name") or f"{provider}.invoke"
    version = params.get("toolVersion") or params.get("tool_version") or "v1"
    return str(name), str(version)


def _status_meta(status: str, base: Dict[str, Any], extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    m = {"status": status, **base}
    if extra:
        m.update(extra)
    return m


def _permissions(params: Dict[str, Any]) -> Dict[str, bool]:
    p = params.get("permissions") if isinstance(params.get("permissions"), dict) else {}
    return {
        "net": bool(p.get("net", False)),
        "fs": bool(p.get("fs", False)),
        "env": bool(p.get("env", False)),
        "subprocess": bool(p.get("subprocess", False)),
    }


def _requested_output_mode(node: Dict[str, Any], params: Dict[str, Any]) -> str:
    ports = (node.get("data", {}).get("ports", {}) or {}) if isinstance(node, dict) else {}
    out_port = str(ports.get("out") or "json").strip().lower()
    if out_port in ("json", "text", "binary"):
        return out_port
    # Tool out ports currently support json/text/binary only.
    # Fallback remains json for unknown/missing values.
    return "json"


def _jsonable(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    return str(value)


def _table_df_from_tool_artifact(artifact: Dict[str, Any]):
    try:
        import pandas as pd
    except Exception:
        return None

    mime_type = str(artifact.get("mime_type") or "").lower()
    port_type = str(artifact.get("port_type") or "").lower()
    text_value = artifact.get("text")
    bytes_b64 = artifact.get("bytes_b64")
    raw_bytes: Optional[bytes] = None
    if isinstance(bytes_b64, str) and bytes_b64:
        try:
            raw_bytes = base64.b64decode(bytes_b64.encode("ascii"), validate=False)
        except Exception:
            raw_bytes = None

    # Prefer the same loader used by Transform nodes so Tool DB accepts
    # the same TABLE_V1-compatible payloads/mime combinations.
    if raw_bytes is not None:
        try:
            from ..runner.nodes.transform import load_table_from_artifact_bytes

            return load_table_from_artifact_bytes(mime_type, raw_bytes)
        except Exception:
            pass

    # If the artifact is explicitly typed as table, try delimited parsing even when
    # mime type is generic text/plain or unset.
    if port_type == "table" and isinstance(text_value, str) and text_value.strip():
        try:
            return pd.read_csv(io.StringIO(text_value), sep=None, engine="python")
        except Exception:
            pass

    if "csv" in mime_type:
        if raw_bytes is not None:
            return pd.read_csv(io.BytesIO(raw_bytes))
        if isinstance(text_value, str):
            return pd.read_csv(io.StringIO(text_value))
    if "tab-separated-values" in mime_type:
        if raw_bytes is not None:
            return pd.read_csv(io.BytesIO(raw_bytes), sep="\t")
        if isinstance(text_value, str):
            return pd.read_csv(io.StringIO(text_value), sep="\t")
    if "json" in mime_type and isinstance(text_value, str):
        parsed = json.loads(text_value)
        if isinstance(parsed, list):
            return pd.DataFrame(parsed)
        if isinstance(parsed, dict):
            if parsed.get("kind") == "json" and isinstance(parsed.get("payload"), dict):
                payload = parsed.get("payload") or {}
                if isinstance(payload.get("rows"), list):
                    return pd.DataFrame(payload.get("rows") or [])
                result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
                if isinstance(result.get("rows"), list):
                    return pd.DataFrame(result.get("rows") or [])
            rows = parsed.get("rows")
            if isinstance(rows, list):
                return pd.DataFrame(rows)
            return pd.DataFrame([parsed])
    return None


def _extract_typed_columns_from_payload_schema(payload_schema: Any) -> list[Dict[str, str]]:
    if not isinstance(payload_schema, dict):
        return []
    schema_env = payload_schema.get("schema")
    cols = None
    if isinstance(schema_env, dict):
        table = schema_env.get("table")
        if isinstance(table, dict) and isinstance(table.get("columns"), list):
            cols = table.get("columns")
    if cols is None and isinstance(payload_schema.get("columns"), list):
        cols = payload_schema.get("columns")
    if not isinstance(cols, list):
        return []
    out: list[Dict[str, str]] = []
    seen: set[str] = set()
    for c in cols:
        if not isinstance(c, dict):
            continue
        name = str(c.get("name") or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        ctype = str(c.get("type") or "unknown").strip() or "unknown"
        out.append({"name": name, "type": ctype})
    return out


async def _materialize_tool_inputs(context: GraphContext, upstream_artifact_ids: list[str]) -> Dict[str, Any]:
    if not getattr(context, "graph_id", ""):
        raise ValueError("graphId is required for artifact lookup")
    artifacts: list[dict[str, Any]] = []
    seen_artifacts: set[str] = set()
    for aid in upstream_artifact_ids:
        aid_s = str(aid or "")
        if not aid_s or aid_s in seen_artifacts:
            continue
        seen_artifacts.add(aid_s)
        art = await context.artifact_store.get(aid)
        b = await context.artifact_store.read(aid)
        mt = (art.mime_type or "application/octet-stream").lower()
        text_value: Optional[str] = None
        json_value: Any = None
        if "json" in mt:
            try:
                json_value = json.loads(b.decode("utf-8", errors="replace"))
            except Exception:
                json_value = None
        if mt.startswith("text/") or "json" in mt or "csv" in mt:
            text_value = b.decode("utf-8", errors="replace")
        artifacts.append(
            {
                "artifact_id": aid,
                "mime_type": art.mime_type,
                "port_type": art.port_type,
                "payload_schema": art.payload_schema if isinstance(art.payload_schema, dict) else None,
                "typed_columns": _extract_typed_columns_from_payload_schema(art.payload_schema),
                "size_bytes": len(b),
                "text": text_value,
                "json": json_value,
                "bytes_b64": base64.b64encode(b).decode("ascii"),
            }
        )

    primary = artifacts[0] if artifacts else None
    return {
        "artifacts": artifacts,
        "primary": primary,
        "input_text": (primary or {}).get("text") if primary else None,
        "input_json": (primary or {}).get("json") if primary else None,
        "input_b64": (primary or {}).get("bytes_b64") if primary else None,
    }


def _format_output(result: Any, mode: str, meta: Dict[str, Any]) -> Dict[str, Any]:
    if mode == "binary":
        if isinstance(result, bytes):
            payload = result
        elif isinstance(result, str):
            payload = result.encode("utf-8")
        else:
            payload = json.dumps(_jsonable(result), ensure_ascii=False).encode("utf-8")
        return {"kind": "binary", "payload": payload, "meta": meta}
    if mode == "text":
        if isinstance(result, str):
            payload = result
        else:
            payload = json.dumps(_jsonable(result), ensure_ascii=False)
        return {"kind": "text", "payload": payload, "meta": meta}
    return {"kind": "json", "payload": _jsonable(result), "meta": meta}


def _contract_mismatch(message: str) -> str:
    return f"Tool output contract mismatch: {message}"


def _exception_text(exc: BaseException) -> str:
    text = str(exc).strip()
    if text:
        return text
    rep = repr(exc).strip()
    if rep:
        return rep
    return exc.__class__.__name__


def _builtin_field_type_from_name(type_name: str) -> Any:
    import typing

    normalized = str(type_name or "").strip().lower()
    optional = False
    if normalized.endswith("?"):
        optional = True
        normalized = normalized[:-1].strip()

    base_map: Dict[str, Any] = {
        "str": str,
        "string": str,
        "int": int,
        "integer": int,
        "float": float,
        "number": float,
        "bool": bool,
        "boolean": bool,
        "dict": dict,
        "object": dict,
        "list": list,
        "array": list,
        "any": Any,
    }

    if normalized.startswith("list[") and normalized.endswith("]"):
        inner_name = normalized[5:-1].strip()
        inner = _builtin_field_type_from_name(inner_name)
        annotation = list[inner]
    else:
        annotation = base_map.get(normalized)
        if annotation is None:
            raise ValueError(f"Unsupported schema field type: {type_name}")

    if optional:
        annotation = typing.Optional[annotation]
    return annotation


def _builtin_dynamic_model_from_args(schema_args: Dict[str, Any]) -> Any:
    from pydantic import create_model

    fields_raw = schema_args.get("fields")
    if not isinstance(fields_raw, dict) or not fields_raw:
        raise ValueError("core.json.validate_schema requires args.fields object")

    field_defs: Dict[str, tuple[Any, Any]] = {}
    for raw_name, raw_cfg in fields_raw.items():
        field_name = str(raw_name or "").strip()
        if not field_name:
            continue
        if isinstance(raw_cfg, str):
            annotation = _builtin_field_type_from_name(raw_cfg)
            field_defs[field_name] = (annotation, ...)
            continue
        if not isinstance(raw_cfg, dict):
            raise ValueError(f"Invalid schema definition for field '{field_name}'")
        annotation = _builtin_field_type_from_name(str(raw_cfg.get("type") or "any"))
        required = bool(raw_cfg.get("required", True))
        if required:
            default_value = ...
        elif "default" in raw_cfg:
            default_value = raw_cfg.get("default")
        else:
            default_value = None
        field_defs[field_name] = (annotation, default_value)

    if not field_defs:
        raise ValueError("core.json.validate_schema requires at least one field")
    return create_model("BuiltinDynamicSchemaModel", **field_defs)


def _coerce_numeric_array(values: Any) -> list[float]:
    if isinstance(values, (int, float)):
        return [float(values)]
    if not isinstance(values, list):
        raise ValueError("Expected args.values to be an array of numbers")
    out: list[float] = []
    for idx, v in enumerate(values):
        try:
            out.append(float(v))
        except Exception as exc:
            raise ValueError(f"values[{idx}] is not numeric") from exc
    return out


async def _exec_builtin_core_tool(
    tool_id: str,
    args: Dict[str, Any],
    input_value: Any,
    input_ctx: Dict[str, Any],
    permissions: Dict[str, bool],
) -> Dict[str, Any]:
    if tool_id in {"core.array.summary_stats", "core.array.normalize"}:
        import numpy as np

        raw_values = args.get("values", input_value)
        if isinstance(raw_values, dict) and isinstance(raw_values.get("values"), list):
            raw_values = raw_values.get("values")
        values = _coerce_numeric_array(raw_values)
        arr = np.asarray(values, dtype=float).reshape(-1)
        if arr.size == 0:
            raise ValueError("values cannot be empty")

        if tool_id == "core.array.summary_stats":
            return {
                "count": int(arr.size),
                "min": float(np.min(arr)),
                "max": float(np.max(arr)),
                "mean": float(np.mean(arr)),
                "std": float(np.std(arr)),
                "p50": float(np.percentile(arr, 50)),
                "p95": float(np.percentile(arr, 95)),
            }

        method = str(args.get("method") or "zscore").strip().lower()
        if method == "minmax":
            min_v = float(np.min(arr))
            max_v = float(np.max(arr))
            denom = max_v - min_v
            normalized = np.zeros_like(arr) if denom == 0 else (arr - min_v) / denom
            return {
                "method": "minmax",
                "min": min_v,
                "max": max_v,
                "values": [float(x) for x in normalized.tolist()],
            }
        if method == "zscore":
            mean_v = float(np.mean(arr))
            std_v = float(np.std(arr))
            normalized = np.zeros_like(arr) if std_v == 0 else (arr - mean_v) / std_v
            return {
                "method": "zscore",
                "mean": mean_v,
                "std": std_v,
                "values": [float(x) for x in normalized.tolist()],
            }
        raise ValueError("core.array.normalize supports method: zscore|minmax")

    if tool_id == "core.json.validate_schema":
        payload = args.get("payload", input_value)
        if payload is None and isinstance(input_ctx, dict):
            payload = input_ctx.get("input_json")
        model_cls = _builtin_dynamic_model_from_args(args)
        try:
            model = model_cls.model_validate(payload)
            return {"valid": True, "errors": [], "value": model.model_dump()}
        except Exception as exc:
            return {"valid": False, "errors": [str(exc)], "value": None}

    if tool_id in {"core.datetime.parse", "core.datetime.normalize_tz"}:
        from dateutil import parser, tz

        value = args.get("value", input_value)
        if value is None:
            raise ValueError("datetime value is required")
        dt = parser.parse(str(value))
        assume_tz = str(args.get("assume_tz") or "").strip()
        if dt.tzinfo is None and assume_tz:
            fallback_tz = tz.gettz(assume_tz)
            if fallback_tz is None:
                raise ValueError(f"Unknown timezone: {assume_tz}")
            dt = dt.replace(tzinfo=fallback_tz)
        if tool_id == "core.datetime.normalize_tz":
            target_tz_name = str(args.get("target_tz") or "UTC").strip()
            target_tz = tz.gettz(target_tz_name)
            if target_tz is None:
                raise ValueError(f"Unknown timezone: {target_tz_name}")
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=tz.UTC)
            dt = dt.astimezone(target_tz)
        return {
            "iso": dt.isoformat(),
            "date": dt.date().isoformat(),
            "time": dt.time().isoformat(),
            "tz": str(dt.tzinfo) if dt.tzinfo is not None else None,
            "unix_seconds": int(dt.timestamp()) if dt.tzinfo is not None else None,
        }

    if tool_id in {"core.http.request_json", "core.http.request_text"}:
        if not permissions.get("net", False):
            raise ValueError(f"{tool_id} requires permissions.net=true")

        import requests

        url = str(args.get("url") or "").strip()
        if not url:
            raise ValueError(f"{tool_id} requires args.url")
        method = str(args.get("method") or "GET").strip().upper()
        headers = args.get("headers") if isinstance(args.get("headers"), dict) else {}
        params = args.get("params") if isinstance(args.get("params"), dict) else None
        json_body = args.get("json") if "json" in args else None
        data_body = args.get("data") if "data" in args else None
        timeout_ms = int(args.get("timeout_ms") or 15000)
        timeout_s = max(0.001, float(timeout_ms) / 1000.0)

        def _request() -> Any:
            return requests.request(
                method,
                url,
                headers=headers,
                params=params,
                json=json_body,
                data=data_body,
                timeout=timeout_s,
            )

        response = await asyncio.to_thread(_request)
        base = {
            "status_code": int(response.status_code),
            "ok": bool(getattr(response, "ok", False)),
            "url": str(getattr(response, "url", url)),
            "reason": str(getattr(response, "reason", "")),
            "headers": dict(getattr(response, "headers", {}) or {}),
        }
        if tool_id == "core.http.request_json":
            try:
                payload = response.json()
            except Exception as exc:
                raise ValueError("Response is not valid JSON") from exc
            base["payload"] = payload
        else:
            base["payload"] = str(response.text or "")
        return base

    raise ValueError(f"Unsupported core builtin toolId: {tool_id}")


def _rows_from_value(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, list):
        rows: list[dict[str, Any]] = []
        for item in value:
            if isinstance(item, dict):
                rows.append(item)
            else:
                rows.append({"value": item})
        return rows
    if isinstance(value, dict):
        if isinstance(value.get("rows"), list):
            rows = value.get("rows") or []
            return [r for r in rows if isinstance(r, dict)]
        if isinstance(value.get("payload"), dict) and isinstance(value.get("payload", {}).get("rows"), list):
            rows = value.get("payload", {}).get("rows") or []
            return [r for r in rows if isinstance(r, dict)]
        return [value]
    return [{"value": value}]


def _rows_from_data_input(args: Dict[str, Any], input_value: Any) -> list[dict[str, Any]]:
    if "rows" in args:
        return _rows_from_value(args.get("rows"))
    return _rows_from_value(input_value)


async def _exec_builtin_data_tool(
    tool_id: str,
    args: Dict[str, Any],
    input_value: Any,
) -> Dict[str, Any]:
    rows = _rows_from_data_input(args, input_value)

    if tool_id in {"data.pandas.profile", "data.pandas.select_columns"}:
        import pandas as pd

        df = pd.DataFrame(rows)
        if tool_id == "data.pandas.profile":
            sample_size = int(args.get("sample_size") or 5)
            sample_size = max(0, sample_size)
            return {
                "row_count": int(len(df.index)),
                "columns": [str(c) for c in list(df.columns)],
                "dtypes": {str(k): str(v) for k, v in df.dtypes.astype(str).to_dict().items()},
                "null_count": {str(k): int(v) for k, v in df.isna().sum().to_dict().items()},
                "sample_rows": _jsonable(df.head(sample_size).to_dict(orient="records")),
            }
        raw_cols = args.get("columns")
        if not isinstance(raw_cols, list) or not raw_cols:
            raise ValueError("data.pandas.select_columns requires args.columns array")
        columns = [str(c) for c in raw_cols]
        missing = [c for c in columns if c not in df.columns]
        if missing:
            raise ValueError(f"Columns not found: {', '.join(missing)}")
        out_df = df[columns]
        return {
            "row_count": int(len(out_df.index)),
            "columns": columns,
            "rows": _jsonable(out_df.to_dict(orient="records")),
        }

    if tool_id == "data.polars.profile":
        import polars as pl

        pl_df = pl.DataFrame(rows)
        sample_size = int(args.get("sample_size") or 5)
        sample_size = max(0, sample_size)
        head_rows = pl_df.head(sample_size).to_dicts()
        null_counts = {}
        for name in pl_df.columns:
            null_counts[str(name)] = int(pl_df.select(pl.col(name).null_count()).item())
        return {
            "row_count": int(pl_df.height),
            "columns": [str(c) for c in pl_df.columns],
            "dtypes": {str(name): str(dtype) for name, dtype in zip(pl_df.columns, pl_df.dtypes)},
            "null_count": null_counts,
            "sample_rows": _jsonable(head_rows),
        }

    if tool_id == "data.pyarrow.schema":
        import pyarrow as pa

        table = pa.Table.from_pylist(rows)
        fields = [
            {"name": str(field.name), "type": str(field.type), "nullable": bool(field.nullable)}
            for field in table.schema
        ]
        return {
            "row_count": int(table.num_rows),
            "column_count": int(table.num_columns),
            "fields": fields,
        }

    raise ValueError(f"Unsupported data builtin toolId: {tool_id}")


async def exec_tool(
    run_id: str,
    node: Dict[str, Any],
    context: GraphContext,
    input_metadata: Optional[FileMetadata] = None,
    upstream_artifact_ids: Optional[list[str]] = None,
) -> NodeOutput:
    node_id = node.get("id", "<missing-node-id>")

    upstream_artifact_ids = upstream_artifact_ids or []
    assert context is not None, "context is None"
    assert hasattr(context, "bus"), "context missing bus"
    assert hasattr(context, "artifact_store"), "context missing artifact_store"

    params = node["data"].get("params", {}) or {}
    provider = params.get("provider")
    side_effect_mode = params.get("side_effect_mode", "pure")
    request_fingerprint = params.get("_request_fingerprint")
    idempotency_key = params.get("_idempotency_key")
    tool_name, tool_version = _extract_tool_identity(params)
    output_mode = _requested_output_mode(node, params)
    started = time.perf_counter()
    input_ctx = await _materialize_tool_inputs(context, upstream_artifact_ids)

    await context.bus.emit(
        {
            "type": "log",
            "runId": run_id,
            "at": iso_now(),
            "level": "info",
            "message": f"Tool provider: {provider}",
            "nodeId": node_id,
        }
    )
    await context.bus.emit(
        {
            "type": "tool_call_started",
            "runId": run_id,
            "at": iso_now(),
            "nodeId": node_id,
            "toolName": tool_name,
            "toolVersion": tool_version,
            "sideEffectMode": side_effect_mode,
        }
    )

    common_meta = {
        "tool_name": tool_name,
        "tool_version": tool_version,
        "request_fingerprint": request_fingerprint or "",
        "side_effect_mode": side_effect_mode,
        "permissions": _permissions(params),
    }

    if provider == "mcp":
        try:
            from ..tools.providers.mcp import invoke_mcp
        except ImportError:
            return NodeOutput(status="failed", metadata=None, execution_time_ms=0.0, error="MCP provider not available")

        try:
            mcp_cfg = params.get("mcp", {}) if isinstance(params.get("mcp"), dict) else {}
            mcp_payload = {
                "provider": "mcp",
                "server_id": mcp_cfg.get("serverId") or mcp_cfg.get("server_id"),
                "tool_name": mcp_cfg.get("toolName") or mcp_cfg.get("tool_name"),
                "args": mcp_cfg.get("args") or {},
                "input": input_ctx,
            }
            result = await invoke_mcp(run_id, mcp_payload, bus=context.bus)
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            redacted_result = _redact_value(result)
            return NodeOutput(
                status="succeeded",
                metadata=None,
                execution_time_ms=elapsed_ms,
                data=_format_output(
                    redacted_result,
                    output_mode,
                    _status_meta("ok", common_meta, {"timings": {"elapsed_ms": elapsed_ms}}),
                ),
            )
        except Exception as e:
            return NodeOutput(
                status="failed",
                metadata=None,
                execution_time_ms=(time.perf_counter() - started) * 1000.0,
                error=f"Tool execution failed: {str(e)}",
            )

    if provider == "python":
        try:
            py_cfg = params.get("python", {}) if isinstance(params.get("python"), dict) else {}
            py_args = py_cfg.get("args") if isinstance(py_cfg.get("args"), dict) else {}
            capture_output = bool(py_cfg.get("capture_output", True))
            py_code = str(py_cfg.get("code") or "")
            timeout_ms = params.get("timeoutMs")
            timeout_s = None
            if timeout_ms is not None:
                timeout_s = max(0.001, float(timeout_ms) / 1000.0)

            def _run_python() -> Any:
                scope: Dict[str, Any] = {
                    "input": input_ctx.get("input_json") if input_ctx.get("input_json") is not None else input_ctx.get("input_text"),
                    "inputs": input_ctx.get("artifacts"),
                    "input_text": input_ctx.get("input_text"),
                    "input_json": input_ctx.get("input_json"),
                    "input_b64": input_ctx.get("input_b64"),
                    "raw_input": input_ctx,
                    "args": py_args,
                    "result": None,
                }
                exec(py_code, scope)
                return scope.get("result")

            if timeout_s is None:
                result = await asyncio.to_thread(_run_python)
            else:
                result = await asyncio.wait_for(asyncio.to_thread(_run_python), timeout=timeout_s)
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            payload = (
                {
                    "ok": True,
                    "args": _redact_value(py_args),
                    "result": _redact_value(result),
                }
                if capture_output
                else _redact_value(result)
            )
            return NodeOutput(
                status="succeeded",
                metadata=None,
                execution_time_ms=elapsed_ms,
                data=_format_output(
                    payload,
                    output_mode,
                    _status_meta("ok", common_meta, {"timings": {"elapsed_ms": elapsed_ms}}),
                ),
            )
        except asyncio.TimeoutError:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            timeout_val = params.get("timeoutMs")
            timeout_text = int(float(timeout_val)) if timeout_val is not None else "unknown"
            py_cfg = params.get("python", {}) if isinstance(params.get("python"), dict) else {}
            error_payload = {
                "ok": False,
                "args": _redact_value(py_cfg.get("args") if isinstance(py_cfg.get("args"), dict) else {}),
                "error": f"Python tool timed out after {timeout_text}ms",
                "error_type": "TimeoutError",
            }
            return NodeOutput(
                status="failed",
                metadata=None,
                execution_time_ms=elapsed_ms,
                data=_format_output(
                    error_payload,
                    "json",
                    _status_meta("error", common_meta, {"timings": {"elapsed_ms": elapsed_ms}}),
                ),
                error=f"Python tool timed out after {timeout_text}ms",
            )
        except Exception as e:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            err_text = _exception_text(e)
            py_cfg = params.get("python", {}) if isinstance(params.get("python"), dict) else {}
            error_payload = {
                "ok": False,
                "args": _redact_value(py_cfg.get("args") if isinstance(py_cfg.get("args"), dict) else {}),
                "error": err_text,
                "error_type": e.__class__.__name__,
            }
            return NodeOutput(
                status="failed",
                metadata=None,
                execution_time_ms=elapsed_ms,
                data=_format_output(
                    error_payload,
                    "json",
                    _status_meta("error", common_meta, {"timings": {"elapsed_ms": elapsed_ms}}),
                ),
                error=f"Python tool execution failed: {err_text}",
            )

    if provider == "js":
        try:
            perms = _permissions(params)
            if not perms.get("subprocess", False):
                return NodeOutput(status="failed", metadata=None, execution_time_ms=0.0, error="JS tool requires permissions.subprocess=true")
            if side_effect_mode == "pure" and (perms.get("net") or perms.get("fs") or perms.get("env")):
                return NodeOutput(status="failed", metadata=None, execution_time_ms=0.0, error="Pure JS tool cannot request net/fs/env permissions")

            node_bin = shutil.which("node")
            if not node_bin:
                return NodeOutput(status="failed", metadata=None, execution_time_ms=0.0, error="Node.js runtime not found in PATH")

            js_cfg = params.get("js", {}) if isinstance(params.get("js"), dict) else {}
            js_code = str(js_cfg.get("code") or "").strip()
            if not js_code:
                return NodeOutput(status="failed", metadata=None, execution_time_ms=0.0, error="JS tool code is empty")
            js_args = js_cfg.get("args") if isinstance(js_cfg.get("args"), dict) else {}
            capture_output = bool(js_cfg.get("capture_output", True))
            vm_timeout_ms = int(params.get("timeoutMs") or 5000)
            vm_timeout_ms = max(1, vm_timeout_ms)

            runner = (
                "const vm=require('node:vm');"
                "const fs=require('node:fs');"
                "const inputJson=process.env.TOOL_INPUT_JSON||'null';"
                "const argsJson=process.env.TOOL_ARGS_JSON||'{}';"
                "const vmTimeoutMs=Math.max(1,Number(process.env.TOOL_VM_TIMEOUT_MS||'5000'));"
                "let input=null;try{input=JSON.parse(inputJson);}catch{input=null;}"
                "let args={};try{args=JSON.parse(argsJson);}catch{args={};}"
                "const code=fs.readFileSync(0,'utf8');"
                "(async()=>{"
                "const context={"
                "input:(input&&input.input_json!==undefined&&input.input_json!==null)?input.input_json:((input&&input.input_text!==undefined)?input.input_text:null),"
                "inputs:(input&&Array.isArray(input.artifacts))?input.artifacts:[],"
                "raw_input:input,"
                "args,"
                "result:null,"
                "console:{log:()=>{}}"
                "};"
                "vm.createContext(context);"
                "try{"
                "let ret=vm.runInContext(code,context,{timeout:vmTimeoutMs});"
                "if(ret&&typeof ret.then==='function'){ret=await ret;}"
                "const out=(context.result!==null&&context.result!==undefined)?context.result:ret;"
                "process.stdout.write(JSON.stringify({ok:true,result:(out===undefined?null:out)}));"
                "}catch(e){"
                "process.stdout.write(JSON.stringify({ok:false,error:String((e&&e.message)||e)}));"
                "process.exit(1);"
                "}"
                "})();"
            )

            child_env = dict(os.environ)
            child_env.update(
                {
                    "TOOL_INPUT_JSON": json.dumps(_jsonable(input_ctx), ensure_ascii=False),
                    "TOOL_ARGS_JSON": json.dumps(_jsonable(js_args), ensure_ascii=False),
                    "TOOL_VM_TIMEOUT_MS": str(vm_timeout_ms),
                }
            )

            stdout_s = ""
            stderr_s = ""
            try:
                proc = await asyncio.create_subprocess_exec(
                    node_bin,
                    "-e",
                    runner,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env=child_env,
                )
                stdout_b, stderr_b = await proc.communicate(input=js_code.encode("utf-8"))
                stdout_s = stdout_b.decode("utf-8", errors="replace").strip()
                stderr_s = stderr_b.decode("utf-8", errors="replace").strip()
            except NotImplementedError:
                completed = await asyncio.to_thread(
                    subprocess.run,
                    [node_bin, "-e", runner],
                    input=js_code,
                    text=True,
                    capture_output=True,
                    env=child_env,
                    check=False,
                )
                stdout_s = str(completed.stdout or "").strip()
                stderr_s = str(completed.stderr or "").strip()

            if not stdout_s:
                return NodeOutput(
                    status="failed",
                    metadata=None,
                    execution_time_ms=(time.perf_counter() - started) * 1000.0,
                    error=f"JS tool produced no output{': ' + stderr_s if stderr_s else ''}",
                )
            parsed = json.loads(stdout_s)
            if not isinstance(parsed, dict) or not parsed.get("ok", False):
                err = parsed.get("error") if isinstance(parsed, dict) else "Unknown JS execution error"
                return NodeOutput(
                    status="failed",
                    metadata=None,
                    execution_time_ms=(time.perf_counter() - started) * 1000.0,
                    error=f"JS tool execution failed: {err}",
                )
            result = parsed.get("result")
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            payload = (
                {
                    "ok": True,
                    "args": _redact_value(js_args),
                    "result": _redact_value(result),
                }
                if capture_output
                else _redact_value(result)
            )
            return NodeOutput(
                status="succeeded",
                metadata=None,
                execution_time_ms=elapsed_ms,
                data=_format_output(
                    payload,
                    output_mode,
                    _status_meta("ok", common_meta, {"timings": {"elapsed_ms": elapsed_ms}}),
                ),
            )
        except Exception as e:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            err_text = _exception_text(e)
            js_cfg = params.get("js", {}) if isinstance(params.get("js"), dict) else {}
            error_payload = {
                "ok": False,
                "args": _redact_value(js_cfg.get("args") if isinstance(js_cfg.get("args"), dict) else {}),
                "error": err_text,
                "error_type": e.__class__.__name__,
            }
            return NodeOutput(
                status="failed",
                metadata=None,
                execution_time_ms=elapsed_ms,
                data=_format_output(
                    error_payload,
                    "json",
                    _status_meta("error", common_meta, {"timings": {"elapsed_ms": elapsed_ms}}),
                ),
                error=f"JS tool execution failed: {err_text}",
            )

    if provider == "function":
        try:
            fn_cfg = params.get("function", {}) if isinstance(params.get("function"), dict) else {}
            module_name = str(fn_cfg.get("module") or "").strip()
            export_name = str(fn_cfg.get("export") or "").strip()
            if not module_name or not export_name:
                return NodeOutput(
                    status="failed",
                    metadata=None,
                    execution_time_ms=0.0,
                    error="function.module and function.export are required",
                )
            call_args = fn_cfg.get("args") if isinstance(fn_cfg.get("args"), dict) else {}
            capture_output = bool(fn_cfg.get("capture_output", True))
            mod = importlib.import_module(module_name)
            fn = getattr(mod, export_name)
            call_input = {
                "input": input_ctx.get("input_json") if input_ctx.get("input_json") is not None else input_ctx.get("input_text"),
                "inputs": input_ctx.get("artifacts"),
            }
            result = fn(call_input, call_args)
            if asyncio.iscoroutine(result):
                result = await result
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            payload = (
                {
                    "ok": True,
                    "module": module_name,
                    "export": export_name,
                    "args": call_args,
                    "result": _redact_value(result),
                }
                if capture_output
                else _redact_value(result)
            )
            return NodeOutput(
                status="succeeded",
                metadata=None,
                execution_time_ms=elapsed_ms,
                data=_format_output(
                    payload,
                    output_mode,
                    _status_meta("ok", common_meta, {"timings": {"elapsed_ms": elapsed_ms}}),
                ),
            )
        except Exception as e:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            err_text = _exception_text(e)
            error_payload = {
                "ok": False,
                "module": str((params.get("function") or {}).get("module") or ""),
                "export": str((params.get("function") or {}).get("export") or ""),
                "error": err_text,
                "error_type": e.__class__.__name__,
            }
            return NodeOutput(
                status="failed",
                metadata=None,
                execution_time_ms=elapsed_ms,
                data=_format_output(
                    error_payload,
                    "json",
                    _status_meta("error", common_meta, {"timings": {"elapsed_ms": elapsed_ms}}),
                ),
                error=f"Function tool execution failed: {err_text}",
            )

    if provider == "shell":
        try:
            perms = _permissions(params)
            if not perms.get("subprocess", False):
                return NodeOutput(status="failed", metadata=None, execution_time_ms=0.0, error="Shell tool requires permissions.subprocess=true")
            sh_cfg = params.get("shell", {}) if isinstance(params.get("shell"), dict) else {}
            command = str(sh_cfg.get("command") or "").strip()
            if not command:
                return NodeOutput(status="failed", metadata=None, execution_time_ms=0.0, error="Shell command is empty")
            cwd = str(sh_cfg.get("cwd") or "").strip() or None
            timeout_ms = sh_cfg.get("timeout_ms") if sh_cfg.get("timeout_ms") is not None else params.get("timeoutMs")
            timeout_s = None
            if timeout_ms is not None:
                timeout_s = max(0.001, float(timeout_ms) / 1000.0)
            env_cfg = sh_cfg.get("env") if isinstance(sh_cfg.get("env"), dict) else {}
            child_env = dict(os.environ)
            for k, v in env_cfg.items():
                child_env[str(k)] = str(v)
            fail_on_nonzero = bool(sh_cfg.get("fail_on_nonzero", True))

            timed_out = False
            exit_code: Optional[int]
            stdout_text = ""
            stderr_text = ""
            try:
                proc = await asyncio.create_subprocess_shell(
                    command,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    cwd=cwd,
                    env=child_env,
                )
                try:
                    if timeout_s is None:
                        stdout_b, stderr_b = await proc.communicate()
                    else:
                        stdout_b, stderr_b = await asyncio.wait_for(proc.communicate(), timeout=timeout_s)
                except asyncio.TimeoutError:
                    timed_out = True
                    proc.kill()
                    stdout_b, stderr_b = await proc.communicate()
                exit_code = int(proc.returncode)
                stdout_text = stdout_b.decode("utf-8", errors="replace")
                stderr_text = stderr_b.decode("utf-8", errors="replace")
            except NotImplementedError:
                # Windows fallback when the active asyncio loop does not implement subprocess transports.
                try:
                    completed = await asyncio.to_thread(
                        subprocess.run,
                        command,
                        shell=True,
                        capture_output=True,
                        text=True,
                        cwd=cwd,
                        env=child_env,
                        timeout=timeout_s,
                        check=False,
                    )
                    exit_code = int(completed.returncode)
                    stdout_text = str(completed.stdout or "")
                    stderr_text = str(completed.stderr or "")
                except subprocess.TimeoutExpired as tex:
                    timed_out = True
                    exit_code = None
                    stdout_text = str(tex.stdout or "")
                    stderr_text = str(tex.stderr or "")

            captured = {
                "ok": (int(exit_code or 0) == 0) and (not timed_out),
                "exit_code": exit_code,
                "stdout": stdout_text,
                "stderr": stderr_text,
                "stdout_lines": stdout_text.count("\n"),
                "stderr_lines": stderr_text.count("\n"),
                "timed_out": timed_out,
                "command": command,
            }
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            if timed_out:
                return NodeOutput(
                    status="failed",
                    metadata=None,
                    execution_time_ms=elapsed_ms,
                    data=_format_output(
                        _redact_value(captured),
                        "json",
                        _status_meta("error", common_meta, {"timings": {"elapsed_ms": elapsed_ms}}),
                    ),
                    error=f"Shell tool timed out after {int(float(timeout_ms))}ms",
                )
            if int(exit_code or 0) != 0 and fail_on_nonzero:
                return NodeOutput(
                    status="failed",
                    metadata=None,
                    execution_time_ms=elapsed_ms,
                    data=_format_output(
                        _redact_value(captured),
                        "json",
                        _status_meta("error", common_meta, {"timings": {"elapsed_ms": elapsed_ms}}),
                    ),
                    error=f"Shell tool failed with exit code {exit_code}",
                )
            return NodeOutput(
                status="succeeded",
                metadata=None,
                execution_time_ms=elapsed_ms,
                data=_format_output(
                    _redact_value(captured),
                    output_mode,
                    _status_meta("ok", common_meta, {"timings": {"elapsed_ms": elapsed_ms}}),
                ),
            )
        except Exception as e:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            err_text = _exception_text(e)
            sh_cfg = params.get("shell", {}) if isinstance(params.get("shell"), dict) else {}
            cmd_fallback = str(sh_cfg.get("command") or "")
            cwd_fallback = str(sh_cfg.get("cwd") or "") or None
            error_payload = {
                "ok": False,
                "exit_code": None,
                "stdout": "",
                "stderr": "",
                "stdout_lines": 0,
                "stderr_lines": 0,
                "timed_out": False,
                "command": cmd_fallback,
                "cwd": cwd_fallback,
                "error": err_text,
                "error_type": e.__class__.__name__,
            }
            return NodeOutput(
                status="failed",
                metadata=None,
                execution_time_ms=elapsed_ms,
                data=_format_output(
                    _redact_value(error_payload),
                    "json",
                    _status_meta("error", common_meta, {"timings": {"elapsed_ms": elapsed_ms}}),
                ),
                error=f"Shell tool execution failed: {err_text}",
            )

    if provider == "db":
        try:
            try:
                import duckdb
            except Exception:
                return NodeOutput(
                    status="failed",
                    metadata=None,
                    execution_time_ms=0.0,
                    error="duckdb is required for Tool DB",
                )

            db_cfg = params.get("db", {}) if isinstance(params.get("db"), dict) else {}
            conn_ref = str(db_cfg.get("connectionRef") or "").strip()
            sql = str(db_cfg.get("sql") or "").strip()
            sql_params = db_cfg.get("params") if isinstance(db_cfg.get("params"), dict) else {}
            capture_output = bool(db_cfg.get("capture_output", True))
            timeout_ms = params.get("timeoutMs")
            timeout_s = None
            if timeout_ms is not None:
                timeout_s = max(0.001, float(timeout_ms) / 1000.0)
            if not conn_ref or not sql:
                return NodeOutput(status="failed", metadata=None, execution_time_ms=0.0, error="db.connectionRef and db.sql are required")
            if conn_ref == ":memory:":
                db_path = ":memory:"
            elif conn_ref.startswith("duckdb:///"):
                db_path = conn_ref.replace("duckdb:///", "", 1)
            else:
                return NodeOutput(
                    status="failed",
                    metadata=None,
                    execution_time_ms=0.0,
                    error="Tool DB requires DuckDB connectionRef in format duckdb:///... or :memory:",
                )

            def _run_db() -> Dict[str, Any]:
                conn = duckdb.connect(database=db_path)
                try:
                    registered_inputs: list[Dict[str, Any]] = []
                    input_diagnostics: list[Dict[str, Any]] = []
                    artifacts = input_ctx.get("artifacts") if isinstance(input_ctx.get("artifacts"), list) else []
                    for idx, artifact in enumerate(artifacts):
                        if not isinstance(artifact, dict):
                            continue
                        diag = {
                            "artifact_id": str(artifact.get("artifact_id") or ""),
                            "mime_type": str(artifact.get("mime_type") or ""),
                            "size_bytes": int(artifact.get("size_bytes") or 0),
                            "table": f"input_{idx}",
                        }
                        try:
                            df = _table_df_from_tool_artifact(artifact)
                        except Exception as parse_exc:
                            df = None
                            diag["parse_error"] = _exception_text(parse_exc)
                        if df is None:
                            diag["parsed"] = False
                            input_diagnostics.append(diag)
                            continue
                        table_name = f"input_{idx}"
                        # Avoid transient replacement-scan lifetimes by materializing into
                        # a real temp table in DuckDB for the duration of this execution.
                        staging_name = f"__input_stage_{idx}"
                        conn.register(staging_name, df)
                        try:
                            conn.execute(f"create or replace temp table {table_name} as select * from {staging_name}")
                        finally:
                            try:
                                conn.unregister(staging_name)
                            except Exception:
                                pass
                        diag["parsed"] = True
                        diag["rows"] = int(len(df.index))
                        diag["columns"] = [str(c) for c in list(df.columns)]
                        typed_from_artifact = (
                            artifact.get("typed_columns")
                            if isinstance(artifact.get("typed_columns"), list)
                            else []
                        )
                        typed_map = {
                            str(c.get("name") or ""): str(c.get("type") or "unknown")
                            for c in typed_from_artifact
                            if isinstance(c, dict) and str(c.get("name") or "").strip()
                        }
                        typed_columns = []
                        for col in list(df.columns):
                            col_name = str(col)
                            try:
                                native = str(getattr(df[col].dtype, "name", "unknown"))
                            except Exception:
                                native = "unknown"
                            typed_columns.append(
                                {
                                    "name": col_name,
                                    "type": str(typed_map.get(col_name, "unknown")),
                                    "nativeType": native,
                                }
                            )
                        diag["typed_columns"] = typed_columns
                        input_diagnostics.append(diag)
                        registered_inputs.append(
                            {
                                "table": table_name,
                                "artifact_id": str(artifact.get("artifact_id") or ""),
                                "rows": int(len(df.index)),
                                "columns": [str(c) for c in list(df.columns)],
                                "typed_columns": typed_columns,
                            }
                        )
                    if (not registered_inputs) and re.search(r"\binput\b", sql, flags=re.IGNORECASE):
                        raise RuntimeError(
                            "No upstream table input was materialized for alias 'input'. "
                            f"diagnostics={json.dumps(input_diagnostics, ensure_ascii=False)}"
                        )

                    resolved_sql = sql
                    primary_input_count: Optional[int] = None
                    if registered_inputs:
                        primary_table = str(registered_inputs[0]["table"])
                        try:
                            probe = conn.execute(f"select count(*) as n from {primary_table}").fetchone()
                            primary_input_count = int((probe or [0])[0] or 0)
                        except Exception as probe_exc:
                            raise RuntimeError(
                                "Primary materialized input table is not queryable "
                                f"({primary_table}): {_exception_text(probe_exc)}"
                            ) from probe_exc
                        # Resolve logical alias to the verified primary input table.
                        resolved_sql = re.sub(
                            r"\binput\b",
                            primary_table,
                            resolved_sql,
                            flags=re.IGNORECASE,
                        )
                    try:
                        print(
                            "[tool-db-debug] execute",
                            json.dumps(
                                {
                                    "impl": "tool_db_v3_temp_input_view",
                                    "connectionRef": conn_ref,
                                    "sql": sql,
                                    "resolved_sql": resolved_sql,
                                    "primary_input_count": primary_input_count,
                                    "registered_inputs": registered_inputs,
                                    "input_artifacts": input_diagnostics,
                                },
                                ensure_ascii=False,
                            ),
                        )
                    except Exception:
                        pass
                    try:
                        res = conn.execute(resolved_sql, sql_params)
                    except Exception as db_exc:
                        dbg = {
                            "impl": "tool_db_v3_temp_input_view",
                            "connectionRef": conn_ref,
                            "sql": sql,
                            "resolved_sql": resolved_sql,
                            "primary_input_count": primary_input_count,
                            "registered_inputs": registered_inputs,
                            "input_artifacts": input_diagnostics,
                        }
                        raise RuntimeError(
                            f"{_exception_text(db_exc)} [tool-db-debug] {json.dumps(dbg, ensure_ascii=False)}"
                        ) from db_exc
                    cols = [d[0] for d in (res.description or [])]
                    is_query = len(cols) > 0
                    rows = [dict(zip(cols, row)) for row in res.fetchall()] if is_query else []
                    rowcount = getattr(res, "rowcount", -1)
                    affected_rows = None if is_query else max(0, int(rowcount if isinstance(rowcount, int) else -1))
                    conn.commit()
                    return {
                        "rows": rows,
                        "row_count": len(rows),
                        "columns": cols,
                        "affected_rows": affected_rows,
                        "is_query": is_query,
                        "resolved_sql": resolved_sql,
                        "input_tables": registered_inputs,
                        "input_artifacts": input_diagnostics,
                    }
                finally:
                    conn.close()

            if timeout_s is None:
                result = await asyncio.to_thread(_run_db)
            else:
                result = await asyncio.wait_for(asyncio.to_thread(_run_db), timeout=timeout_s)

            elapsed_ms = (time.perf_counter() - started) * 1000.0
            payload = (
                {
                    "ok": True,
                    "connectionRef": conn_ref,
                    "sql": sql,
                    "params": _redact_value(sql_params),
                    "result": _redact_value(result),
                }
                if capture_output
                else _redact_value(result)
            )
            return NodeOutput(
                status="succeeded",
                metadata=None,
                execution_time_ms=elapsed_ms,
                data=_format_output(
                    payload,
                    output_mode,
                    _status_meta("ok", common_meta, {"timings": {"elapsed_ms": elapsed_ms}}),
                ),
            )
        except asyncio.TimeoutError:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            timeout_val = params.get("timeoutMs")
            timeout_text = int(float(timeout_val)) if timeout_val is not None else "unknown"
            db_cfg = params.get("db", {}) if isinstance(params.get("db"), dict) else {}
            error_payload = {
                "ok": False,
                "connectionRef": str(db_cfg.get("connectionRef") or ""),
                "sql": str(db_cfg.get("sql") or ""),
                "params": _redact_value(db_cfg.get("params") if isinstance(db_cfg.get("params"), dict) else {}),
                "error": f"DB tool timed out after {timeout_text}ms",
                "error_type": "TimeoutError",
            }
            return NodeOutput(
                status="failed",
                metadata=None,
                execution_time_ms=elapsed_ms,
                data=_format_output(
                    error_payload,
                    "json",
                    _status_meta("error", common_meta, {"timings": {"elapsed_ms": elapsed_ms}}),
                ),
                error=f"DB tool timed out after {timeout_text}ms",
            )
        except Exception as e:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            err_text = _exception_text(e)
            db_cfg = params.get("db", {}) if isinstance(params.get("db"), dict) else {}
            error_payload = {
                "ok": False,
                "connectionRef": str(db_cfg.get("connectionRef") or ""),
                "sql": str(db_cfg.get("sql") or ""),
                "params": _redact_value(db_cfg.get("params") if isinstance(db_cfg.get("params"), dict) else {}),
                "error": err_text,
                "error_type": e.__class__.__name__,
            }
            return NodeOutput(
                status="failed",
                metadata=None,
                execution_time_ms=elapsed_ms,
                data=_format_output(
                    error_payload,
                    "json",
                    _status_meta("error", common_meta, {"timings": {"elapsed_ms": elapsed_ms}}),
                ),
                error=f"DB tool execution failed: {err_text}",
            )

    if provider == "builtin":
        try:
            bi_cfg = params.get("builtin", {}) if isinstance(params.get("builtin"), dict) else {}
            resolved_env = resolve_builtin_environment(bi_cfg)
            tool_id = str(bi_cfg.get("toolId") or "").strip()
            args = bi_cfg.get("args") if isinstance(bi_cfg.get("args"), dict) else {}
            input_value = input_ctx.get("input_json") if input_ctx.get("input_json") is not None else input_ctx.get("input_text")
            perms = _permissions(params)

            if tool_id == "noop":
                result = {"ok": True}
            elif tool_id == "echo":
                result = {"args": args, "input": input_value}
            elif tool_id == "validate_json":
                payload = args.get("payload", input_value)
                result = {"valid": isinstance(payload, (dict, list))}
            elif tool_id.startswith("core."):
                result = await _exec_builtin_core_tool(tool_id, args, input_value, input_ctx, perms)
            elif tool_id.startswith("data."):
                result = await _exec_builtin_data_tool(tool_id, args, input_value)
            else:
                return NodeOutput(
                    status="failed",
                    metadata=None,
                    execution_time_ms=0.0,
                    error=f"Unsupported builtin toolId: {tool_id}",
                )

            elapsed_ms = (time.perf_counter() - started) * 1000.0
            return NodeOutput(
                status="succeeded",
                metadata=None,
                execution_time_ms=elapsed_ms,
                data=_format_output(
                    _redact_value(result),
                    output_mode,
                    _status_meta(
                        "ok",
                        common_meta,
                        {
                            "timings": {"elapsed_ms": elapsed_ms},
                            "builtin_environment": resolved_env,
                        },
                    ),
                ),
            )
        except Exception as e:
            return NodeOutput(
                status="failed",
                metadata=None,
                execution_time_ms=(time.perf_counter() - started) * 1000.0,
                error=f"Builtin tool execution failed: {str(e)}",
            )

    if provider in ("api", "http"):
        try:
            import aiohttp

            http_cfg = params.get("http", {}) if isinstance(params.get("http"), dict) else {}
            url = http_cfg.get("url") or params.get("url") or ""
            method = (http_cfg.get("method") or params.get("method") or "GET").upper()
            headers = dict(http_cfg.get("headers") or params.get("headers") or {})
            query_raw = (
                http_cfg.get("query")
                if "query" in http_cfg
                else params.get("query")
            )
            body = http_cfg.get("body") if "body" in http_cfg else params.get("body")
            retry_cfg = params.get("retry") or params.get("retries") or {}
            max_attempts = max(1, int(retry_cfg.get("max_attempts") or retry_cfg.get("max") or 1))
            backoff_ms = max(0, int(retry_cfg.get("backoff_ms") or retry_cfg.get("backoffMs") or 0))

            if query_raw is None:
                query = None
            elif isinstance(query_raw, dict):
                query = dict(query_raw)
            else:
                return NodeOutput(
                    status="failed",
                    metadata=None,
                    execution_time_ms=0.0,
                    error="http.query must be an object",
                )

            if not url:
                return NodeOutput(status="failed", metadata=None, execution_time_ms=0.0, error="API URL not provided")
            if idempotency_key and method in {"POST", "PUT", "PATCH", "DELETE"}:
                headers.setdefault("Idempotency-Key", idempotency_key)

            attempt = 0
            last_status = 0
            last_body = b""
            last_ct = ""
            async with aiohttp.ClientSession() as session:
                while attempt < max_attempts:
                    attempt += 1
                    async with session.request(method, url, headers=headers, params=query, json=body) as response:
                        last_status = int(response.status)
                        last_ct = response.headers.get("content-type", "")
                        last_body = await response.read()
                        if last_status in {429, 500, 502, 503, 504} and attempt < max_attempts:
                            if backoff_ms > 0:
                                await asyncio.sleep(backoff_ms / 1000.0)
                            continue
                        break

            elapsed_ms = (time.perf_counter() - started) * 1000.0
            if last_status not in {200, 201, 202, 204}:
                return NodeOutput(
                    status="failed",
                    metadata=None,
                    execution_time_ms=elapsed_ms,
                    error=f"API returned status {last_status}",
                )

            ct = (last_ct or "").lower()
            mode = output_mode
            if mode == "json":
                try:
                    payload = json.loads(last_body.decode("utf-8", errors="replace"))
                except Exception:
                    return NodeOutput(
                        status="failed",
                        metadata=None,
                        execution_time_ms=elapsed_ms,
                        error=_contract_mismatch(
                            "outPort=json but response is not valid JSON"
                        ),
                    )
                envelope = {"kind": "json", "payload": _redact_value(payload)}
            elif mode == "binary":
                envelope = {"kind": "binary", "payload": last_body, "mime": (last_ct or "").strip() or None}
            else:
                text = last_body.decode("utf-8", errors="replace")
                envelope = {"kind": "text", "payload": _redact_value(text), "mime": (last_ct or "").strip() or None}

            envelope["meta"] = _status_meta(
                "ok",
                common_meta,
                {
                    "timings": {"elapsed_ms": elapsed_ms},
                    "http_status": last_status,
                    "content_type": ct,
                    "retry_count": max(0, attempt - 1),
                    "idempotency_key": idempotency_key if side_effect_mode == "idempotent" else None,
                },
            )
            return NodeOutput(
                status="succeeded",
                metadata=None,
                execution_time_ms=elapsed_ms,
                data=envelope,
            )
        except Exception as e:
            return NodeOutput(
                status="failed",
                metadata=None,
                execution_time_ms=(time.perf_counter() - started) * 1000.0,
                error=f"API tool execution failed: {str(e)}",
            )

    return NodeOutput(
        status="failed",
        metadata=None,
        execution_time_ms=(time.perf_counter() - started) * 1000.0,
        error=f"Unsupported tool provider: {provider}",
    )
