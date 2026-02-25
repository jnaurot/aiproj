from __future__ import annotations

import asyncio
import base64
import importlib
import json
import shutil
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from ..runner.metadata import GraphContext, NodeOutput


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


def _requested_output_mode(params: Dict[str, Any]) -> str:
    out = params.get("output") if isinstance(params.get("output"), dict) else {}
    mode = out.get("mode") if isinstance(out, dict) else None
    if mode in ("json", "text", "binary"):
        return mode
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


async def _materialize_tool_inputs(context: GraphContext, upstream_artifact_ids: list[str]) -> Dict[str, Any]:
    if not getattr(context, "graph_id", ""):
        raise ValueError("graphId is required for artifact lookup")
    artifacts: list[dict[str, Any]] = []
    for aid in upstream_artifact_ids:
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


async def exec_tool(
    run_id: str,
    node: Dict[str, Any],
    context: GraphContext,
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
    output_mode = _requested_output_mode(params)
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
            scope: Dict[str, Any] = {
                "input": input_ctx.get("input_json") if input_ctx.get("input_json") is not None else input_ctx.get("input_text"),
                "inputs": input_ctx.get("artifacts"),
                "input_text": input_ctx.get("input_text"),
                "input_json": input_ctx.get("input_json"),
                "input_b64": input_ctx.get("input_b64"),
                "result": None,
            }
            exec(str(py_cfg.get("code") or ""), scope)
            result = scope.get("result")
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            return NodeOutput(
                status="succeeded",
                metadata=None,
                execution_time_ms=elapsed_ms,
                data=_format_output(
                    _redact_value(result),
                    output_mode,
                    _status_meta("ok", common_meta, {"timings": {"elapsed_ms": elapsed_ms}}),
                ),
            )
        except Exception as e:
            return NodeOutput(
                status="failed",
                metadata=None,
                execution_time_ms=(time.perf_counter() - started) * 1000.0,
                error=f"Python tool execution failed: {str(e)}",
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

            runner = (
                "const vm=require('node:vm');"
                "const fs=require('node:fs');"
                "const inputJson=process.env.TOOL_INPUT_JSON||'null';"
                "let input=null;try{input=JSON.parse(inputJson);}catch{input=null;}"
                "const code=fs.readFileSync(0,'utf8');"
                "(async()=>{"
                "const context={input, result:null, console:{log:()=>{}}};"
                "vm.createContext(context);"
                "try{"
                "let ret=vm.runInContext(code,context,{timeout:5000});"
                "if(ret&&typeof ret.then==='function'){ret=await ret;}"
                "const out=(context.result!==null&&context.result!==undefined)?context.result:ret;"
                "process.stdout.write(JSON.stringify({ok:true,result:(out===undefined?null:out)}));"
                "}catch(e){"
                "process.stdout.write(JSON.stringify({ok:false,error:String((e&&e.message)||e)}));"
                "process.exit(1);"
                "}"
                "})();"
            )

            proc = await asyncio.create_subprocess_exec(
                node_bin,
                "-e",
                runner,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env={"TOOL_INPUT_JSON": json.dumps(_jsonable(input_ctx), ensure_ascii=False)},
            )
            stdout_b, stderr_b = await proc.communicate(input=js_code.encode("utf-8"))
            stdout_s = stdout_b.decode("utf-8", errors="replace").strip()
            stderr_s = stderr_b.decode("utf-8", errors="replace").strip()
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
            return NodeOutput(
                status="succeeded",
                metadata=None,
                execution_time_ms=elapsed_ms,
                data=_format_output(
                    _redact_value(result),
                    output_mode,
                    _status_meta("ok", common_meta, {"timings": {"elapsed_ms": elapsed_ms}}),
                ),
            )
        except Exception as e:
            return NodeOutput(
                status="failed",
                metadata=None,
                execution_time_ms=(time.perf_counter() - started) * 1000.0,
                error=f"JS tool execution failed: {str(e)}",
            )

    if provider == "function":
        try:
            fn_cfg = params.get("function", {}) if isinstance(params.get("function"), dict) else {}
            module_name = str(fn_cfg.get("module") or "").strip()
            export_name = str(fn_cfg.get("export") or "").strip()
            call_args = fn_cfg.get("args") if isinstance(fn_cfg.get("args"), dict) else {}
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
            return NodeOutput(
                status="succeeded",
                metadata=None,
                execution_time_ms=elapsed_ms,
                data=_format_output(
                    _redact_value(result),
                    output_mode,
                    _status_meta("ok", common_meta, {"timings": {"elapsed_ms": elapsed_ms}}),
                ),
            )
        except Exception as e:
            return NodeOutput(
                status="failed",
                metadata=None,
                execution_time_ms=(time.perf_counter() - started) * 1000.0,
                error=f"Function tool execution failed: {str(e)}",
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
            proc = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout_b, stderr_b = await proc.communicate()
            result = {
                "exit_code": int(proc.returncode),
                "stdout": stdout_b.decode("utf-8", errors="replace"),
                "stderr": stderr_b.decode("utf-8", errors="replace"),
            }
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            if proc.returncode != 0:
                return NodeOutput(
                    status="failed",
                    metadata=None,
                    execution_time_ms=elapsed_ms,
                    error=f"Shell tool failed with exit code {proc.returncode}",
                )
            return NodeOutput(
                status="succeeded",
                metadata=None,
                execution_time_ms=elapsed_ms,
                data=_format_output(
                    _redact_value(result),
                    output_mode,
                    _status_meta("ok", common_meta, {"timings": {"elapsed_ms": elapsed_ms}}),
                ),
            )
        except Exception as e:
            return NodeOutput(
                status="failed",
                metadata=None,
                execution_time_ms=(time.perf_counter() - started) * 1000.0,
                error=f"Shell tool execution failed: {str(e)}",
            )

    if provider == "db":
        try:
            db_cfg = params.get("db", {}) if isinstance(params.get("db"), dict) else {}
            conn_ref = str(db_cfg.get("connectionRef") or "").strip()
            sql = str(db_cfg.get("sql") or "").strip()
            sql_params = db_cfg.get("params") if isinstance(db_cfg.get("params"), dict) else {}
            if not conn_ref or not sql:
                return NodeOutput(status="failed", metadata=None, execution_time_ms=0.0, error="db.connectionRef and db.sql are required")
            if conn_ref.startswith("sqlite:///"):
                db_path = conn_ref.replace("sqlite:///", "", 1)
            elif conn_ref == ":memory:":
                db_path = ":memory:"
            else:
                return NodeOutput(
                    status="failed",
                    metadata=None,
                    execution_time_ms=0.0,
                    error="Only sqlite:///... or :memory: connectionRef is supported in this runtime",
                )
            conn = sqlite3.connect(db_path)
            try:
                cur = conn.cursor()
                cur.execute(sql, sql_params)
                cols = [d[0] for d in (cur.description or [])]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()] if cols else []
                conn.commit()
            finally:
                conn.close()
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            result = {"rows": rows, "row_count": len(rows)}
            return NodeOutput(
                status="succeeded",
                metadata=None,
                execution_time_ms=elapsed_ms,
                data=_format_output(
                    _redact_value(result),
                    output_mode,
                    _status_meta("ok", common_meta, {"timings": {"elapsed_ms": elapsed_ms}}),
                ),
            )
        except Exception as e:
            return NodeOutput(
                status="failed",
                metadata=None,
                execution_time_ms=(time.perf_counter() - started) * 1000.0,
                error=f"DB tool execution failed: {str(e)}",
            )

    if provider == "builtin":
        try:
            bi_cfg = params.get("builtin", {}) if isinstance(params.get("builtin"), dict) else {}
            tool_id = str(bi_cfg.get("toolId") or "").strip()
            args = bi_cfg.get("args") if isinstance(bi_cfg.get("args"), dict) else {}
            input_value = input_ctx.get("input_json") if input_ctx.get("input_json") is not None else input_ctx.get("input_text")

            if tool_id == "noop":
                result = {"ok": True}
            elif tool_id == "echo":
                result = {"args": args, "input": input_value}
            elif tool_id == "validate_json":
                payload = args.get("payload", input_value)
                result = {"valid": isinstance(payload, (dict, list))}
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
                    _status_meta("ok", common_meta, {"timings": {"elapsed_ms": elapsed_ms}}),
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
            body = http_cfg.get("body") if "body" in http_cfg else params.get("body")
            retry_cfg = params.get("retry") or params.get("retries") or {}
            max_attempts = max(1, int(retry_cfg.get("max_attempts") or retry_cfg.get("max") or 1))
            backoff_ms = max(0, int(retry_cfg.get("backoff_ms") or retry_cfg.get("backoffMs") or 0))

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
                    async with session.request(method, url, headers=headers, json=body) as response:
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
                            "output.mode=json but response is not valid JSON"
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
