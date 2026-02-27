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
from ..runner.metadata import GraphContext, FileMetadata, NodeOutput
from ..runner.schemas import LLMParams


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256_text(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()


def _sha256_json(obj: object) -> str:
    b = json.dumps(obj, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(b).hexdigest()


def _resolve_api_key(params: LLMParams) -> Optional[str]:
    """
    Resolve API key from api_key_ref.
    For now:
    - if api_key_ref is set and matches an env var name, use that env var value
    - otherwise treat api_key_ref as direct key
    """
    ref = (params.api_key_ref or "").strip()
    if not ref:
        return None

    try:
        import os

        env_val = os.getenv(ref)
        if env_val:
            return env_val
    except Exception:
        pass

    return ref


def _build_messages(params: LLMParams, upstream_text: str) -> List[Dict[str, str]]:
    user_prompt = params.user_prompt or "Summarize the input data."
    if "{input}" in user_prompt:
        user_content = user_prompt.replace("{input}", upstream_text)
    else:
        user_content = f"{user_prompt}\n\n--- INPUT DATA ---\n{upstream_text}"

    messages: List[Dict[str, str]] = []
    if params.system_prompt:
        messages.append({"role": "system", "content": params.system_prompt})
    messages.append({"role": "user", "content": user_content})
    return messages


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
    upstream_artifact_ids: Optional[list[str]] = None,
) -> NodeOutput:
    node_id = node.get("id", "<missing-node-id>")
    upstream_artifact_ids = upstream_artifact_ids or []
    t0 = asyncio.get_event_loop().time()

    assert context is not None, "context is None"
    assert hasattr(context, "bus"), "context missing bus"

    if not upstream_artifact_ids:
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

    upstream_text = input_text if isinstance(input_text, str) else await materialize_text(context, upstream_artifact_ids[0])
    input_items = input_items or ([upstream_text] if upstream_text else [])
    base_url = (params.base_url or "").rstrip("/")
    if not base_url:
        return NodeOutput(
            status="failed",
            metadata=None,
            execution_time_ms=(asyncio.get_event_loop().time() - t0) * 1000.0,
            error="OpenAI-compatible executor requires base_url",
        )

    api_key = _resolve_api_key(params)
    headers: Dict[str, str] = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    payload: Dict[str, Any] = {
        "model": params.model,
        "messages": _build_messages(params, upstream_text),
        "temperature": params.temperature,
        "max_tokens": params.max_tokens,
        "stream": True,
    }

    if params.top_p is not None:
        payload["top_p"] = params.top_p
    if params.seed is not None:
        payload["seed"] = params.seed
    if params.stop_sequences:
        payload["stop"] = params.stop_sequences
    if params.presence_penalty is not None:
        payload["presence_penalty"] = params.presence_penalty
    if params.frequency_penalty is not None:
        payload["frequency_penalty"] = params.frequency_penalty
    if params.output_mode == "json":
        payload["response_format"] = {"type": "json_object"}

    await context.bus.emit(
        {
            "type": "log",
            "runId": run_id,
            "nodeId": node_id,
            "at": iso_now(),
            "level": "info",
            "message": f"OpenAI-compatible chat: base_url={base_url} model={params.model} output_mode={params.output_mode}",
        }
    )

    url = f"{base_url}/v1/chat/completions"
    if params.output_mode == "embeddings":
        url = f"{base_url}/v1/embeddings"
    attempt = 0
    last_err: Optional[str] = None

    while True:
        try:
            timeout = httpx.Timeout(
                connect=10.0,
                read=float(params.timeout_seconds),
                write=10.0,
                pool=10.0,
            )

            if params.output_mode == "embeddings":
                embed_payload: Dict[str, Any] = {
                    "model": params.model,
                    "input": input_items if len(input_items) > 1 else (input_items[0] if input_items else upstream_text),
                }
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
                vectors: List[List[float]] = []
                for row in rows:
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
                for vec in vectors:
                    if len(vec) != dims:
                        return NodeOutput(
                            status="failed",
                            metadata=None,
                            execution_time_ms=(asyncio.get_event_loop().time() - t0) * 1000.0,
                            error=f"Embedding dims mismatch: expected {dims}, got {len(vec)}",
                        )
                if layout == "1d" and len(vectors) != 1:
                    return NodeOutput(
                        status="failed",
                        metadata=None,
                        execution_time_ms=(asyncio.get_event_loop().time() - t0) * 1000.0,
                        error=f"Embedding layout 1d requires exactly one vector, got {len(vectors)}",
                    )
                output_obj = {
                    "mode": "embeddings",
                    "dims": dims,
                    "dtype": str(contract.get("dtype") or "float32"),
                    "layout": layout,
                    "data": vectors[0] if layout == "1d" else vectors,
                }
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

            mime_type = "text/plain; charset=utf-8"
            file_type = "txt"
            file_path = f"memory://runs/{run_id}/nodes/{node_id}/llm_output.txt"

            if params.output_mode == "json":
                try:
                    json_data = json.loads(data) if data else None
                except json.JSONDecodeError as e:
                    await context.bus.emit(
                        {
                            "type": "log",
                            "runId": run_id,
                            "nodeId": node_id,
                            "at": iso_now(),
                            "level": "error",
                            "message": f"JSON parse failed in output_mode=json: {str(e)}",
                        }
                    )
                    return NodeOutput(
                        status="failed",
                        metadata=None,
                        execution_time_ms=(asyncio.get_event_loop().time() - t0) * 1000.0,
                        error="LLM output_mode=json but response was not valid JSON",
                    )
                if params.output_strict:
                    try:
                        jsonschema_validate(instance=json_data, schema=params.output_schema or {})
                    except ValidationError as e:
                        return NodeOutput(
                            status="failed",
                            metadata=None,
                            execution_time_ms=(asyncio.get_event_loop().time() - t0) * 1000.0,
                            error=f"LLM strict JSON schema validation failed: {e.message}",
                        )

                data = json.dumps(json_data, separators=(",", ":"), sort_keys=True)
                mime_type = "application/json"
                file_type = "json"
                file_path = f"memory://runs/{run_id}/nodes/{node_id}/llm_output.json"
            elif params.output_mode == "embeddings":
                mime_type = "application/json"
                file_type = "json"
                file_path = f"memory://runs/{run_id}/nodes/{node_id}/llm_output.embeddings.json"

            payload_bytes = (data or "").encode("utf-8")
            content_hash = _sha256_text(data or "")

            try:
                params_payload = params.model_dump()
            except Exception:
                params_payload = dict(params)
            params_hash = _sha256_json(params_payload)

            meta = FileMetadata(
                file_path=file_path,
                file_type=file_type,
                mime_type=mime_type,
                size_bytes=len(payload_bytes),
                content_hash=content_hash,
                node_id=node_id,
                params_hash=params_hash,
            )

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

            await context.bus.emit(
                {
                    "type": "log",
                    "runId": run_id,
                    "nodeId": node_id,
                    "at": iso_now(),
                    "level": "warn",
                    "message": f"openai_compat request failed (attempt {attempt}/{params.max_retries}): {last_err}",
                }
            )

            if not params.retry_on_error or attempt > params.max_retries:
                return NodeOutput(
                    status="failed",
                    metadata=None,
                    execution_time_ms=(asyncio.get_event_loop().time() - t0) * 1000.0,
                    error=f"openai_compat request failed: {last_err}",
                )

            backoff = min(2.0 ** (attempt - 1), 8.0)
            await asyncio.sleep(backoff)
