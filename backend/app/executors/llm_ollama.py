# executors/llm_ollama.py
from __future__ import annotations

from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
import asyncio
import json
import hashlib
import re

import httpx

from app.runner.materialize import materialize_text

# Adjust these imports to your actual paths/types
from ..runner.schemas import LLMParams
from ..runner.metadata import GraphContext, FileMetadata, NodeOutput
from ..runner.events import RunEventBus
from app.runner.emit import emit


def _sha256_text(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()

def _sha256_json(obj: object) -> str:
    b = json.dumps(obj, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(b).hexdigest()

def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_messages(params: LLMParams, user_content: str) -> List[Dict[str, str]]:
    msgs: List[Dict[str, str]] = []
    if params.system_prompt:
        msgs.append({"role": "system", "content": params.system_prompt})
    msgs.append({"role": "user", "content": user_content})
    return msgs

def _content_to_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts: List[str] = []
        for item in value:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                txt = item.get("text")
                if isinstance(txt, str):
                    parts.append(txt)
        return "".join(parts)
    return ""

def _extract_ollama_text(obj: Dict[str, Any]) -> str:
    if not isinstance(obj, dict):
        return ""

    msg = obj.get("message")
    if isinstance(msg, dict):
        content = _content_to_text(msg.get("content"))
        if content:
            return content

    response = _content_to_text(obj.get("response"))
    if response:
        return response

    output = _content_to_text(obj.get("output"))
    if output:
        return output

    return ""

def _extract_stream_delta(obj: Dict[str, Any]) -> str:
    """
    Accept multiple stream shapes:
    - Ollama chat: {"message":{"content":"..."}}
    - Ollama generate-style: {"response":"..."}
    - OpenAI-like proxies: {"choices":[{"delta":{"content":"..."}}]}
    NOTE: intentionally ignores any thinking/reasoning fields.
    """
    if not isinstance(obj, dict):
        return ""

    msg = obj.get("message")
    if isinstance(msg, dict):
        c = _content_to_text(msg.get("content"))
        if c:
            return c

    resp = _content_to_text(obj.get("response"))
    if resp:
        return resp

    choices = obj.get("choices")
    if isinstance(choices, list) and choices:
        c0 = choices[0] if isinstance(choices[0], dict) else {}
        delta = c0.get("delta")
        if isinstance(delta, dict):
            d = _content_to_text(delta.get("content"))
            if d:
                return d
            d_reason = _content_to_text(delta.get("reasoning_content"))
            if d_reason:
                return d_reason

        msg2 = c0.get("message")
        if isinstance(msg2, dict):
            m = _content_to_text(msg2.get("content"))
            if m:
                return m

        txt = _content_to_text(c0.get("text"))
        if txt:
            return txt

    return ""

def _extract_stream_thinking(obj: Dict[str, Any]) -> str:
    if not isinstance(obj, dict):
        return ""
    msg = obj.get("message")
    if isinstance(msg, dict):
        return _content_to_text(msg.get("thinking"))
    return _content_to_text(obj.get("thinking"))

def _extract_ollama_thinking(obj: Dict[str, Any]) -> str:
    if not isinstance(obj, dict):
        return ""
    msg = obj.get("message")
    if isinstance(msg, dict):
        t = _content_to_text(msg.get("thinking"))
        if t:
            return t
        t2 = _content_to_text(msg.get("reasoning"))
        if t2:
            return t2
        t3 = _content_to_text(msg.get("reasoning_content"))
        if t3:
            return t3
    top = _content_to_text(obj.get("thinking"))
    if top:
        return top
    top2 = _content_to_text(obj.get("reasoning"))
    if top2:
        return top2
    return ""

def _strip_think_tags(text: str) -> str:
    # Some models emit reasoning inline as <think>...</think> inside content.
    return re.sub(r"<think>[\s\S]*?</think>", "", text, flags=re.IGNORECASE).strip()


def _best_effort_input_text(input_metadata: Optional[FileMetadata]) -> str:
    """
    You currently pass only input_metadata to exec_llm(). Depending on how your
    FileMetadata is defined, this may NOT contain the full file contents.
    This tries a few common attribute names and falls back to "".
    """
    if not input_metadata:
        return ""

    for attr in ("text", "content", "preview_text", "preview", "snippet"):
        v = getattr(input_metadata, attr, None)
        if isinstance(v, str) and v.strip():
            return v

    return ""


def _render_user_prompt(params: LLMParams, input_metadata: Optional[FileMetadata]) -> str:
    """
    Minimal templating that won’t fight your future input_mapping work.
    - If prompt includes '{input}', substitute best-effort input text.
    - Otherwise return the prompt as-is.
    """
    prompt = params.user_prompt or ""
    if "{input}" in prompt:
        input_text = _best_effort_input_text(input_metadata)
        return prompt.replace("{input}", input_text)
    return prompt

def _compose_user_content(user_prompt: str, upstream_text: str) -> str:
    prompt = user_prompt or "Summarize the input data."
    if "{input}" in prompt:
        return prompt.replace("{input}", upstream_text)
    return f"{prompt}\n\n--- INPUT DATA ---\n{upstream_text}"


async def exec_llm_ollama(
    run_id: str,
    node: Dict[str, Any],
    context: GraphContext,
    # bus: RunEventBus,
    input_metadata: Optional[FileMetadata],
    params: LLMParams,
    upstream_artifact_ids: Optional[list[str]] = None
) -> NodeOutput:
    """
    Executes an LLM node using Ollama's native /api/chat endpoint.

    Called when node.data.llmKind == "ollama"
    Streams token/content deltas via the bus (log events), and returns final NodeOutput.

    NOTE: This module purposefully avoids assuming your internal dataflow format.
    Right now it can only safely template using '{input}' from input_metadata if present.
    """
    node_id = node.get("id", "<missing-node-id>")
    print("[ollama] node_id:", node_id)
    upstream_artifact_ids = upstream_artifact_ids or []

    assert context is not None, "context is None"
    assert hasattr(context, "bus"), "context missing bus"
    
    t0 = asyncio.get_event_loop().time()
        
    if not upstream_artifact_ids:
        await context.bus.emit({
            "type": "log",
            "runId": run_id,
            "nodeId": node_id,
            "at": iso_now(),
            "level": "error",
            "message": "LLM node received no upstream artifacts (upstream_artifact_ids=[]).",
        })
        # Decide: fail fast or continue with prompts only.
        # I recommend fail fast for now:
        return NodeOutput(
            status="failed", 
            error="No upstream artifacts provided to LLM", 
            execution_time_ms=(asyncio.get_event_loop().time() - t0) * 1000.0,
            metadata=None
            )
    
    text = await materialize_text(context, upstream_artifact_ids[0])
    print("TEXT: ",text)
    print("[llm] upstream_ids:", upstream_artifact_ids, "len:", len(text))

    base_url = (params.base_url or "").rstrip("/")
    if not base_url:
        return NodeOutput(
            status="failed",
            metadata=None,
            execution_time_ms=0.0,
            error="Ollama requires base_url (e.g., http://localhost:11434)",
        )

    # Build messages from system_prompt + rendered user_prompt
    # user_prompt = params.user_prompt or "Summarize the input data."
    # if "{input}" in user_prompt:
    #     user_content = user_prompt.replace("{input}", text)
    # else:
    #     user_content = f"{user_prompt}\n\n--- INPUT DATA ---\n{text}"
    # messages = _build_messages(params, user_content)
    user_prompt = params.user_prompt or "Summarize the input data."
    user_content = _compose_user_content(user_prompt, text)
    messages = _build_messages(params, user_content)



    # Ollama API payload
    payload: Dict[str, Any] = {
        "model": params.model,
        "messages": messages,
        "stream": True,
        "think": False,
        "options": {
            "temperature": params.temperature,
            "num_predict": params.max_tokens,
        },
    }
    if params.top_p is not None:
        payload["options"]["top_p"] = params.top_p
    if params.seed is not None:
        payload["options"]["seed"] = params.seed
    if params.stop_sequences:
        payload["options"]["stop"] = params.stop_sequences

    # Structured output (Ollama supports `format: "json"` for JSON mode)
    if params.output_mode == "json":
        payload["format"] = "json"

    await context.bus.emit(
        {
            "type": "log",
            "runId": run_id,
            "nodeId": node_id,
            "at": iso_now(),
            "level": "info",
            "message": f"Ollama chat: base_url={base_url} model={params.model} output_mode={params.output_mode}",
        }
    )

    url = f"{base_url}/api/chat"

    # Retry loop (simple exponential backoff)
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

            async with httpx.AsyncClient(timeout=timeout) as client:
                async with client.stream("POST", url, json=payload) as resp:
                    resp.raise_for_status()

                    chunks: List[str] = []
                    thinking_chunks: List[str] = []
                    sample_lines: List[str] = []
                    async for line in resp.aiter_lines():
                        if not line:
                            continue
                        if len(sample_lines) < 3:
                            sample_lines.append(line[:300])

                        # Ollama streams newline-delimited JSON objects
                        try:
                            obj = json.loads(line)
                        except json.JSONDecodeError:
                            # Don’t fail the whole run on a single bad line; log it.
                            await context.bus.emit(
                                {
                                    "type": "log",
                                    "runId": run_id,
                                    "nodeId": node_id,
                                    "at": iso_now(),
                                    "level": "warn",
                                    "message": f"Ollama stream: non-JSON line: {line[:200]}",
                                }
                            )
                            continue

                        delta = _extract_stream_delta(obj)
                        thinking_delta = _extract_stream_thinking(obj)

                        if delta:
                            chunks.append(delta)
                            # If you have a dedicated "delta" event, swap this.
                            await context.bus.emit({
                                "type": "llm_delta",
                                "runId": run_id,
                                "nodeId": node_id,
                                "at": iso_now(),
                                "delta": delta,
                            })
                        if thinking_delta:
                            thinking_chunks.append(thinking_delta)

                        if obj.get("done") is True:
                            break

            data = "".join(chunks).strip()
            if not data:
                # Some Ollama setups may finish a streamed chat without emitting deltas.
                # Fallback to a non-stream response to recover final text deterministically.
                await context.bus.emit(
                    {
                        "type": "log",
                        "runId": run_id,
                        "nodeId": node_id,
                        "at": iso_now(),
                        "level": "warn",
                        "message": "Ollama stream returned empty content; retrying once with stream=false",
                    }
                )

                payload_non_stream = dict(payload)
                payload_non_stream["stream"] = False
                async with httpx.AsyncClient(timeout=timeout) as client:
                    resp = await client.post(url, json=payload_non_stream)
                    resp.raise_for_status()
                    obj = resp.json()

                data = _extract_ollama_text(obj).strip()

            data = _strip_think_tags(data)

            if not data:
                # Last-resort fallback for models that emit only thinking tokens.
                thinking_text = "".join(thinking_chunks).strip()
                if not thinking_text:
                    thinking_text = _extract_ollama_thinking(obj).strip()
                thinking_text = _strip_think_tags(thinking_text)
                if thinking_text:
                    await context.bus.emit(
                        {
                            "type": "log",
                            "runId": run_id,
                            "nodeId": node_id,
                            "at": iso_now(),
                            "level": "warn",
                            "message": "Ollama returned no content; using thinking text as fallback output",
                        }
                    )
                    data = thinking_text

            if not data:
                await context.bus.emit(
                    {
                        "type": "log",
                        "runId": run_id,
                        "nodeId": node_id,
                        "at": iso_now(),
                        "level": "error",
                        "message": "Ollama returned empty output content after stream and fallback",
                    }
                )
                try:
                    # best-effort diagnostics to help identify provider payload shape
                    await context.bus.emit(
                        {
                            "type": "log",
                            "runId": run_id,
                            "nodeId": node_id,
                            "at": iso_now(),
                            "level": "warn",
                            "message": f"Ollama empty-output diagnostics: fallback keys={list(obj.keys()) if isinstance(obj, dict) else type(obj)}",
                        }
                    )
                    if sample_lines:
                        await context.bus.emit(
                            {
                                "type": "log",
                                "runId": run_id,
                                "nodeId": node_id,
                                "at": iso_now(),
                                "level": "warn",
                                "message": f"Ollama empty-output stream samples: {sample_lines}",
                            }
                        )
                except Exception:
                    pass
                return NodeOutput(
                    status="failed",
                    metadata=None,
                    execution_time_ms=(asyncio.get_event_loop().time() - t0) * 1000.0,
                    error="Ollama returned empty output content",
                )

            mime_type = "text/plain; charset=utf-8"
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
                data = json.dumps(json_data, separators=(",", ":"), sort_keys=True)
                file_path = f"memory://runs/{run_id}/nodes/{node_id}/llm_output.json"
                file_type = "json"
                mime_type = "application/json"
            elif params.output_mode == "markdown":
                file_path = f"memory://runs/{run_id}/nodes/{node_id}/llm_output.md"
                file_type = "txt"
                mime_type = "text/markdown; charset=utf-8"
            else: # text
                file_path = f"memory://runs/{run_id}/nodes/{node_id}/llm_output.txt"
                file_type = "txt"
                
            content_hash = _sha256_text(data or "")
            payload_bytes = (data or "").encode("utf-8")
            data_size = len(payload_bytes)    
            # pick a string path that your system accepts (it can be "memory://" or "inline://")
            # determine file_type consistent with your app's expectations
            # if your enum is something like "text" / "table" / "file", use the correct one.
            # hash output content
            # hash params (so cache/invalidation works)
            try:
                params_payload = params.model_dump()
            except Exception:
                params_payload = dict(params)
            params_hash = _sha256_json(params_payload)

            meta = FileMetadata(
                file_path=file_path,
                file_type=file_type,                 # must match your enum/literal
                mime_type=mime_type,
                size_bytes=data_size,

                content_hash=content_hash,
                node_id=node_id,
                params_hash=params_hash 
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
                    "message": f"Ollama request failed (attempt {attempt}/{params.max_retries}): {last_err}",
                }
            )

            if not params.retry_on_error or attempt > params.max_retries:
                return NodeOutput(
                    status="failed",
                    metadata=None,
                    execution_time_ms=(asyncio.get_event_loop().time() - t0) * 1000.0,
                    error=f"Ollama request failed: {last_err}",
                )

            # exponential backoff, capped
            backoff = min(2.0 ** (attempt - 1), 8.0)
            await asyncio.sleep(backoff)
