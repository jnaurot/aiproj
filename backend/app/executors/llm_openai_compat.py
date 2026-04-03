from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
from jsonschema import ValidationError
from jsonschema import validate as jsonschema_validate

from app.runner.materialize import materialize_text
from .model_adapters import OpenAICompatAdapter
from .model_eval_gate import evaluate_model_output_gate
from .model_policy import (
    circuit_guard_allows,
    circuit_record_failure,
    circuit_record_success,
    normalize_request_policy,
    policy_backoff_seconds,
)
from .model_registry import resolve_model_connection
from ..runner.metadata import GraphContext, FileMetadata, NodeOutput
from ..runner.schemas import LLMParams


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _debug_flag(params: LLMParams, key: str) -> bool:
    debug_cfg = getattr(params, "debug", None)
    if debug_cfg is None:
        return False
    if isinstance(debug_cfg, dict):
        return bool(debug_cfg.get(key, False))
    return bool(getattr(debug_cfg, key, False))


def _debug_excerpt(value: Any, max_chars: int = 1200) -> str:
    try:
        text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, sort_keys=True)
    except Exception:
        text = str(value)
    text = str(text or "").replace("\r\n", "\n").replace("\r", "\n")
    return text[:max_chars]


def _sha256_text(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()


def _sha256_json(obj: object) -> str:
    b = json.dumps(obj, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(b).hexdigest()


def _token_estimate(text: str) -> int:
    return max(1, int(len(text or "") / 4))


def _extract_chat_content(obj: Dict[str, Any]) -> str:
    choices = obj.get("choices")
    if not isinstance(choices, list) or not choices:
        return ""
    choice0 = choices[0] if isinstance(choices[0], dict) else {}

    msg = choice0.get("message")
    if isinstance(msg, dict):
        content = msg.get("content")
        if isinstance(content, str):
            return content

    txt = choice0.get("text")
    if isinstance(txt, str):
        return txt

    return ""


async def exec_llm_openai_compat(
    run_id: str,
    node: Dict[str, Any],
    context: GraphContext,
    input_metadata: Optional[FileMetadata],
    params: LLMParams,
    input_text: Optional[str] = None,
    input_items: Optional[list[str]] = None,
    input_media: Optional[list[Dict[str, Any]]] = None,
    template_values: Optional[Dict[str, str]] = None,
    upstream_artifact_ids: Optional[list[str]] = None,
) -> NodeOutput:
    node_id = node.get("id", "<missing-node-id>")
    upstream_artifact_ids = upstream_artifact_ids or []
    t0 = asyncio.get_event_loop().time()

    assert context is not None, "context is None"
    assert hasattr(context, "bus"), "context missing bus"

    if not upstream_artifact_ids and not bool(getattr(params, "allow_prompt_only_model_execution", False)):
        await context.bus.emit(
            {
                "type": "log",
                "runId": run_id,
                "nodeId": node_id,
                "at": iso_now(),
                "level": "error",
                "message": "LLM node received no upstream artifacts (upstream_artifact_ids=[]).",
            }
        )
        return NodeOutput(
            status="failed",
            error="No upstream artifacts provided to LLM",
            metadata=None,
            execution_time_ms=(asyncio.get_event_loop().time() - t0) * 1000.0,
        )

    if upstream_artifact_ids:
        upstream_text = input_text if isinstance(input_text, str) else await materialize_text(context, upstream_artifact_ids[0])
    else:
        upstream_text = input_text if isinstance(input_text, str) else ""
    input_items = input_items or ([upstream_text] if upstream_text else [])
    adapter = OpenAICompatAdapter()
    try:
        resolved_conn = resolve_model_connection(params, provider="openai_compat")
    except Exception as e:
        return NodeOutput(
            status="failed",
            metadata=None,
            execution_time_ms=(asyncio.get_event_loop().time() - t0) * 1000.0,
            error=adapter.normalize_error(e),
        )
    effective_params = params.model_copy(update={"base_url": resolved_conn.base_url})
    request_policy = normalize_request_policy(params)
    try:
        prepared = adapter.prepare_request(
            effective_params,
            upstream_text,
            input_items=input_items,
            input_media=input_media,
            template_values=template_values,
        )
    except Exception as e:
        return NodeOutput(
            status="failed",
            metadata=None,
            execution_time_ms=(asyncio.get_event_loop().time() - t0) * 1000.0,
            error=adapter.normalize_error(e),
        )
    base_url = prepared.base_url
    output_mode = prepared.output_mode
    validation_mode = str(getattr(params, "output_validation_mode", "strict") or "strict").strip().lower()
    soft_mode = validation_mode == "soft" or (not bool(params.output_strict))
    payload = dict(prepared.payload)
    headers = dict(prepared.headers)
    api_key = resolved_conn.api_key
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    await context.bus.emit(
        {
            "type": "log",
            "runId": run_id,
            "nodeId": node_id,
            "at": iso_now(),
            "level": "info",
            "message": f"OpenAI-compatible chat: base_url={base_url} model={params.model} output_mode={output_mode}",
        }
    )

    url = prepared.url
    circuit_key = f"openai_compat::{base_url}::{params.model}"
    if not circuit_guard_allows(circuit_key, request_policy):
        return NodeOutput(
            status="failed",
            metadata=None,
            execution_time_ms=(asyncio.get_event_loop().time() - t0) * 1000.0,
            error="openai_compat request blocked by circuit breaker",
        )
    attempt = 0
    last_err: Optional[str] = None

    while True:
        try:
            timeout = httpx.Timeout(
                connect=10.0,
                read=float(request_policy.timeout_seconds),
                write=10.0,
                pool=10.0,
            )

            if output_mode == "embeddings":
                items: List[str] = input_items if input_items else [upstream_text]
                chunk_size = request_policy.batch_max_items if request_policy.batch_enabled else len(items)
                chunk_size = max(1, int(chunk_size or len(items)))
                all_rows: List[Dict[str, Any]] = []
                for batch_start in range(0, len(items), chunk_size):
                    batch = items[batch_start : batch_start + chunk_size]
                    embed_payload: Dict[str, Any] = {
                        "model": params.model,
                        "input": batch if len(batch) > 1 else batch[0],
                    }
                    if len(items) > chunk_size:
                        await context.bus.emit(
                            {
                                "type": "log",
                                "runId": run_id,
                                "nodeId": node_id,
                                "at": iso_now(),
                                "level": "info",
                                "message": f"embeddings: micro-batch start={batch_start} size={len(batch)}",
                            }
                        )
                    async with httpx.AsyncClient(timeout=timeout) as client:
                        resp = await client.post(url, json=embed_payload, headers=headers)
                        resp.raise_for_status()
                        obj = resp.json()
                    rows = obj.get("data")
                    if not isinstance(rows, list) or not rows:
                        return NodeOutput(
                            status="failed",
                            metadata=None,
                            execution_time_ms=(asyncio.get_event_loop().time() - t0) * 1000.0,
                            error="Embeddings response missing data[]",
                        )
                    all_rows.extend(rows)
                vectors: List[List[float]] = []
                for row in all_rows:
                    if not isinstance(row, dict) or not isinstance(row.get("embedding"), list):
                        return NodeOutput(
                            status="failed",
                            metadata=None,
                            execution_time_ms=(asyncio.get_event_loop().time() - t0) * 1000.0,
                            error="Embeddings response missing embedding vectors",
                        )
                    vec = row.get("embedding")
                    vectors.append([float(x) for x in vec])
                contract = params.embedding_contract or {}
                dims = int(contract.get("dims") or 0)
                layout = str(contract.get("layout") or "1d")
                embedding_warnings: List[str] = []
                for vec in vectors:
                    if len(vec) != dims:
                        msg = f"Embedding dims mismatch: expected {dims}, got {len(vec)}"
                        if soft_mode:
                            embedding_warnings.append(msg)
                        else:
                            return NodeOutput(
                                status="failed",
                                metadata=None,
                                execution_time_ms=(asyncio.get_event_loop().time() - t0) * 1000.0,
                                error=msg,
                            )
                if layout == "1d" and len(vectors) != 1:
                    msg = f"Embedding layout 1d requires exactly one vector, got {len(vectors)}"
                    if soft_mode:
                        embedding_warnings.append(msg)
                    else:
                        return NodeOutput(
                            status="failed",
                            metadata=None,
                            execution_time_ms=(asyncio.get_event_loop().time() - t0) * 1000.0,
                            error=msg,
                        )
                output_obj = {
                    "mode": "embeddings",
                    "dims": dims,
                    "dtype": str(contract.get("dtype") or "float32"),
                    "layout": layout,
                    "data": vectors[0] if layout == "1d" else vectors,
                }
                if embedding_warnings:
                    output_obj["_warnings"] = embedding_warnings
                data = json.dumps(output_obj, separators=(",", ":"), sort_keys=True)
            else:
                chunks: List[str] = []
                async with httpx.AsyncClient(timeout=timeout) as client:
                    async with client.stream("POST", url, json=payload, headers=headers) as resp:
                        resp.raise_for_status()

                        async for raw_line in resp.aiter_lines():
                            if not raw_line:
                                continue

                            line = raw_line.strip()
                            if line.startswith("data:"):
                                line = line[5:].strip()
                            if not line:
                                continue
                            if line == "[DONE]":
                                break

                            try:
                                obj = json.loads(line)
                            except json.JSONDecodeError:
                                await context.bus.emit(
                                    {
                                        "type": "log",
                                        "runId": run_id,
                                        "nodeId": node_id,
                                        "at": iso_now(),
                                        "level": "warn",
                                        "message": f"openai_compat stream: non-JSON line: {line[:200]}",
                                    }
                                )
                                continue

                            delta = ""
                            choices = obj.get("choices")
                            if isinstance(choices, list) and choices:
                                c0 = choices[0] if isinstance(choices[0], dict) else {}
                                d = c0.get("delta")
                                if isinstance(d, dict):
                                    delta = d.get("content") or ""

                            if delta:
                                chunks.append(delta)
                                await context.bus.emit(
                                    {
                                        "type": "llm_delta",
                                        "runId": run_id,
                                        "nodeId": node_id,
                                        "at": iso_now(),
                                        "delta": delta,
                                    }
                                )

                data = "".join(chunks).strip()
                if not data:
                    await context.bus.emit(
                        {
                            "type": "log",
                            "runId": run_id,
                            "nodeId": node_id,
                            "at": iso_now(),
                            "level": "warn",
                            "message": "openai_compat stream returned empty content; retrying once with stream=false",
                        }
                    )

                    payload_non_stream = dict(payload)
                    payload_non_stream["stream"] = False
                    async with httpx.AsyncClient(timeout=timeout) as client:
                        resp = await client.post(url, json=payload_non_stream, headers=headers)
                        resp.raise_for_status()
                        obj = resp.json()
                    data = _extract_chat_content(obj).strip()

            raw_output_for_debug = data
            if output_mode == "json":
                try:
                    json_data = json.loads(data) if data else None
                except json.JSONDecodeError as e:
                    if soft_mode:
                        await context.bus.emit(
                            {
                                "type": "log",
                                "runId": run_id,
                                "nodeId": node_id,
                                "at": iso_now(),
                                "level": "warn",
                                "message": f"JSON parse failed in soft mode: {str(e)}",
                            }
                        )
                        json_data = {"_warnings": ["json_parse_failed"], "_raw_text": data}
                    else:
                        await context.bus.emit(
                            {
                                "type": "log",
                                "runId": run_id,
                                "nodeId": node_id,
                                "at": iso_now(),
                                "level": "error",
                                "message": f"JSON parse failed in output_mode={output_mode}: {str(e)}",
                            }
                        )
                        if _debug_flag(params, "enabled") and _debug_flag(params, "log_raw_output"):
                            await context.bus.emit(
                                {
                                    "type": "log",
                                    "runId": run_id,
                                    "nodeId": node_id,
                                    "at": iso_now(),
                                    "level": "warn",
                                    "message": f"LLM debug raw output excerpt: {_debug_excerpt(data)}",
                                }
                            )
                        return NodeOutput(
                            status="failed",
                            metadata=None,
                            execution_time_ms=(asyncio.get_event_loop().time() - t0) * 1000.0,
                            error=f"LLM output_mode={output_mode} but response was not valid JSON",
                        )
                if params.output_strict and not soft_mode:
                    try:
                        jsonschema_validate(instance=json_data, schema=params.output_schema or {})
                    except ValidationError as e:
                        if _debug_flag(params, "enabled") and _debug_flag(params, "log_raw_output"):
                            await context.bus.emit(
                                {
                                    "type": "log",
                                    "runId": run_id,
                                    "nodeId": node_id,
                                    "at": iso_now(),
                                    "level": "warn",
                                    "message": f"LLM debug schema-fail output excerpt: {_debug_excerpt(json_data)}",
                                }
                            )
                        return NodeOutput(
                            status="failed",
                            metadata=None,
                            execution_time_ms=(asyncio.get_event_loop().time() - t0) * 1000.0,
                            error=f"LLM strict JSON schema validation failed: {e.message}",
                        )
                elif soft_mode:
                    try:
                        jsonschema_validate(instance=json_data, schema=params.output_schema or {})
                    except ValidationError as e:
                        if not isinstance(json_data, dict):
                            json_data = {"value": json_data}
                        warnings = list(json_data.get("_warnings") or []) if isinstance(json_data, dict) else []
                        warnings.append(f"json_schema_validation_failed:{e.message}")
                        json_data["_warnings"] = warnings

                data = json.dumps(json_data, separators=(",", ":"), sort_keys=True)
            parsed = adapter.parse_response(output_mode, data or "")
            data = parsed.data
            gate_ok, gate_reason = evaluate_model_output_gate(params, output_mode, data or "")
            if not gate_ok:
                return NodeOutput(
                    status="failed",
                    metadata=None,
                    execution_time_ms=(asyncio.get_event_loop().time() - t0) * 1000.0,
                    error=f"MODEL_EVAL_GATE_FAILED: {gate_reason}",
                )
            await context.bus.emit(
                {
                    "type": "log",
                    "runId": run_id,
                    "nodeId": node_id,
                    "at": iso_now(),
                    "level": "info",
                    "message": f"MODEL_EVAL_GATE: passed ({gate_reason})",
                }
            )
            mime_type = parsed.mime_type
            file_type = parsed.file_type
            file_path = f"memory://runs/{run_id}/nodes/{node_id}/llm_output.{parsed.file_suffix}"

            payload_bytes = (data or "").encode("utf-8")
            content_hash = _sha256_text(data or "")

            try:
                params_payload = params.model_dump()
            except Exception:
                params_payload = dict(params)
            params_hash = _sha256_json(params_payload)

            observability = {
                "provider": "openai_compat",
                "model": params.model,
                "prompt_revision_id": params.prompt_revision_id,
                "output_mode": output_mode,
                "retries": int(attempt),
                "latency_ms": max(0.0, (asyncio.get_event_loop().time() - t0) * 1000.0),
                "input_chars": int(len(upstream_text or "")),
                "output_chars": int(len(data or "")),
                "input_tokens_est": _token_estimate(upstream_text or ""),
                "output_tokens_est": _token_estimate(data or ""),
                "total_tokens_est": _token_estimate(upstream_text or "") + _token_estimate(data or ""),
                "cost_estimate_usd": round((_token_estimate(upstream_text or "") + _token_estimate(data or "")) * 0.000002, 8),
                "cache_decision": "executed",
            }
            if _debug_flag(params, "enabled") and _debug_flag(params, "log_raw_output"):
                observability["raw_output"] = raw_output_for_debug
            meta = FileMetadata(
                file_path=file_path,
                file_type=file_type,
                mime_type=mime_type,
                size_bytes=len(payload_bytes),
                content_hash=content_hash,
                node_id=node_id,
                params_hash=params_hash,
                observability=observability,
            )

            await context.bus.emit(
                {
                    "type": "model_observability",
                    "runId": run_id,
                    "nodeId": node_id,
                    "at": iso_now(),
                    "provider": "openai_compat",
                    "model": params.model,
                    "prompt_revision_id": params.prompt_revision_id,
                    "output_mode": output_mode,
                    "retries": int(attempt),
                    "latency_ms": meta.observability.get("latency_ms") if isinstance(meta.observability, dict) else None,
                    "total_tokens_est": meta.observability.get("total_tokens_est") if isinstance(meta.observability, dict) else None,
                    "cost_estimate_usd": meta.observability.get("cost_estimate_usd") if isinstance(meta.observability, dict) else None,
                    "cache_decision": "executed",
                }
            )
            circuit_record_success(circuit_key, request_policy)
            return NodeOutput(
                status="succeeded",
                data=data,
                metadata=meta,
                execution_time_ms=(asyncio.get_event_loop().time() - t0) * 1000.0,
                error=None,
            )

        except (httpx.HTTPError, httpx.TimeoutException) as e:
            last_err = str(e)
            attempt += 1
            circuit_record_failure(circuit_key, request_policy)

            await context.bus.emit(
                {
                    "type": "log",
                    "runId": run_id,
                    "nodeId": node_id,
                    "at": iso_now(),
                    "level": "warn",
                    "message": f"openai_compat request failed (attempt {attempt}/{request_policy.retries}): {last_err}",
                }
            )

            if not params.retry_on_error or attempt > request_policy.retries:
                return NodeOutput(
                    status="failed",
                    metadata=None,
                    execution_time_ms=(asyncio.get_event_loop().time() - t0) * 1000.0,
                    error=adapter.normalize_error(e),
                )

            backoff = policy_backoff_seconds(request_policy, attempt)
            await asyncio.sleep(backoff)
