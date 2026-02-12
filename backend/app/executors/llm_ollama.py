


# executors/llm_ollama.py
from __future__ import annotations

from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
import asyncio
import json
import hashlib

import httpx

from app.runner.materialize import materialize_text

# Adjust these imports to your actual paths/types
from ..runner.schemas import LLMParams
from ..runner.metadata import ExecutionContext, FileMetadata, NodeOutput
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


async def exec_llm_ollama(
    run_id: str,
    node: Dict[str, Any],
    context: ExecutionContext,
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
    user_content = f"{user_prompt}\n\n--- INPUT DATA ---\n{text}"
    messages = _build_messages(params, user_content)



    # Ollama API payload
    payload: Dict[str, Any] = {
        "model": params.model,
        "messages": messages,
        "stream": True,
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
                    async for line in resp.aiter_lines():
                        if not line:
                            continue

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

                        # Typical shape: {"message":{"role":"assistant","content":"..."}, "done":false, ...}
                        msg = obj.get("message") or {}
                        delta = msg.get("content") or ""

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

                        if obj.get("done") is True:
                            break

            full_text = "".join(chunks).strip()

            # JSON mode: parse
            output_value: Any = full_text
            if params.output_mode == "json":
                try:
                    output_value = json.loads(full_text) if full_text else None
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

            # Store output in your execution context if you do that here (optional)
            # context.outputs[node_id] = output_value  # only if your context supports it

            full_text = "".join(chunks).strip()
            b = (full_text or "").encode("utf-8")


            # pick a string path that your system accepts (it can be "memory://" or "inline://")
            file_path = f"memory://runs/{run_id}/nodes/{node_id}/llm_output.txt"

            # determine file_type consistent with your app's expectations
            # if your enum is something like "text" / "table" / "file", use the correct one.
            file_type = "txt"

            # hash output content
            content_hash = _sha256_text(full_text)

            # hash params (so cache/invalidation works)
            try:
                params_payload = params.model_dump()
            except Exception:
                params_payload = dict(params)
            source_params_hash = _sha256_json(params_payload)

            meta = FileMetadata(
                file_path=file_path,
                file_type=file_type,                 # must match your enum/literal
                mime_type="text/plain",
                size_bytes=len(b),

                content_hash=content_hash,
                source_node_id=node_id,
                source_params_hash= source_params_hash 
                )
            
            return NodeOutput(
                status="succeeded",
                metadata=meta,
                execution_time_ms=(asyncio.get_event_loop().time() - t0) * 1000.0,
                value=full_text,
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
