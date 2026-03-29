import asyncio
import hashlib
import json
import base64
import os
import threading
import logging
from typing import Any, Dict, List, Optional

from app.runner.materialize import materialize_text
from app.runner.nodes.transform import load_table_from_artifact_bytes
from ..runner.metadata import GraphContext, NodeOutput
from datetime import datetime, timezone

from ..runner.schemas import LLMParams
from .llm_ollama import exec_llm_ollama           # new module (suggested)
from .llm_openai_compat import exec_llm_openai_compat
from .model_adapters import get_model_adapter
from .model_policy import normalize_request_policy

logger = logging.getLogger(__name__)

# print("[exec_llm] has bus?", hasattr(context, "bus"), type(context.bus))


_MODEL_PROVIDER_SEMAPHORES: Dict[str, asyncio.Semaphore] = {}
_MODEL_PROVIDER_WAITERS: Dict[str, set[str]] = {}
_MODEL_PROVIDER_HOLDERS: Dict[str, str] = {}
_MODEL_PROVIDER_LOCK = threading.Lock()


def _provider_cap_value(provider: str) -> int:
    key = str(provider or "").strip().upper().replace("-", "_")
    specific = os.getenv(f"RUNNER_MAX_MODEL_PROVIDER_{key}")
    default = os.getenv("RUNNER_MAX_MODEL_PROVIDER", "")
    raw = specific if specific not in {None, ""} else default
    if raw in {None, ""}:
        return 0
    try:
        n = int(str(raw).strip())
    except Exception:
        return 0
    return n if n > 0 else 0


def _provider_semaphore(provider: str) -> Optional[asyncio.Semaphore]:
    cap = _provider_cap_value(provider)
    if cap <= 0:
        return None
    key = str(provider or "").strip().lower()
    with _MODEL_PROVIDER_LOCK:
        sem = _MODEL_PROVIDER_SEMAPHORES.get(key)
        if sem is None:
            sem = asyncio.Semaphore(cap)
            _MODEL_PROVIDER_SEMAPHORES[key] = sem
        return sem


def _provider_key(provider: str) -> str:
    return str(provider or "").strip().lower()


def _provider_waiter_add(provider: str, node_id: str) -> int:
    key = _provider_key(provider)
    nid = str(node_id or "").strip()
    with _MODEL_PROVIDER_LOCK:
        waiters = _MODEL_PROVIDER_WAITERS.setdefault(key, set())
        if nid:
            waiters.add(nid)
        return len(waiters)


def _provider_waiter_remove(provider: str, node_id: str) -> int:
    key = _provider_key(provider)
    nid = str(node_id or "").strip()
    with _MODEL_PROVIDER_LOCK:
        waiters = _MODEL_PROVIDER_WAITERS.setdefault(key, set())
        if nid:
            waiters.discard(nid)
        if not waiters:
            _MODEL_PROVIDER_WAITERS.pop(key, None)
        return len(waiters)


def _provider_waiting_nodes(provider: str) -> List[str]:
    key = _provider_key(provider)
    with _MODEL_PROVIDER_LOCK:
        return sorted(str(item) for item in (_MODEL_PROVIDER_WAITERS.get(key) or set()) if str(item).strip())


def _provider_holder_set(provider: str, node_id: Optional[str]) -> Optional[str]:
    key = _provider_key(provider)
    holder = str(node_id or "").strip()
    with _MODEL_PROVIDER_LOCK:
        if holder:
            _MODEL_PROVIDER_HOLDERS[key] = holder
            return holder
        _MODEL_PROVIDER_HOLDERS.pop(key, None)
        return None


def _provider_holder_get(provider: str) -> Optional[str]:
    key = _provider_key(provider)
    with _MODEL_PROVIDER_LOCK:
        holder = str(_MODEL_PROVIDER_HOLDERS.get(key) or "").strip()
        return holder or None


def _provider_acquire_timeout_seconds(provider: str) -> float:
    key = str(provider or "").strip().upper().replace("-", "_")
    specific = os.getenv(f"RUNNER_MODEL_PROVIDER_ACQUIRE_TIMEOUT_{key}")
    default = os.getenv("RUNNER_MODEL_PROVIDER_ACQUIRE_TIMEOUT", "")
    raw = specific if specific not in {None, ""} else default
    if raw in {None, ""}:
        return 0.0
    try:
        value = float(str(raw).strip())
    except Exception:
        return 0.0
    return value if value > 0 else 0.0


#
def normalize_llm_params(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize frontend LLM params (camelCase + nested output) to backend shape."""
    p = dict(raw or {})

    # camelCase -> snake_case
    if "baseUrl" in p and "base_url" not in p:
        p["base_url"] = p.pop("baseUrl")

    if "connectionRef" in p and "connection_ref" not in p:
        p["connection_ref"] = p.pop("connectionRef")

    if "apiKeyRef" in p and "api_key_ref" not in p:
        p["api_key_ref"] = p.pop("apiKeyRef")
    if "promptRevisionId" in p and "prompt_revision_id" not in p:
        p["prompt_revision_id"] = p.pop("promptRevisionId")
    if "evalGate" in p and "eval_gate" not in p:
        p["eval_gate"] = p.pop("evalGate")

    # nested output -> flattened output schema controls
    out = p.get("output")
    if isinstance(out, dict):
        if "mode" in out:
            # Nested output.mode is the canonical control and must override stale legacy output_mode.
            p["output_mode"] = out.get("mode")
        if "jsonSchema" in out and "output_schema" not in p:
            p["output_schema"] = out.get("jsonSchema")
        if "strict" in out and "output_strict" not in p:
            p["output_strict"] = out.get("strict")
        if "validationMode" in out and "output_validation_mode" not in p:
            p["output_validation_mode"] = out.get("validationMode")
        if "embedding" in out and "embedding_contract" not in p:
            p["embedding_contract"] = out.get("embedding")

    if "stop" in p and "stop_sequences" not in p:
        p["stop_sequences"] = p.pop("stop")
    if "inputEncoding" in p and "input_encoding" not in p:
        p["input_encoding"] = p.pop("inputEncoding")
    if "inputEnvelope" in p and "input_envelope" not in p:
        p["input_envelope"] = p.pop("inputEnvelope")
    if "onError" in p and "on_error" not in p:
        p["on_error"] = p.pop("onError")
    if "requestPolicy" in p and "request_policy" not in p:
        p["request_policy"] = p.pop("requestPolicy")
    if isinstance(p.get("debug"), dict):
        dbg = dict(p.get("debug") or {})
        if "logInputPreview" in dbg and "log_input_preview" not in dbg:
            dbg["log_input_preview"] = dbg.pop("logInputPreview")
        if "logRawOutput" in dbg and "log_raw_output" not in dbg:
            dbg["log_raw_output"] = dbg.pop("logRawOutput")
        p["debug"] = dbg
    if "presencePenalty" in p and "presence_penalty" not in p:
        p["presence_penalty"] = p.pop("presencePenalty")
    if "frequencyPenalty" in p and "frequency_penalty" not in p:
        p["frequency_penalty"] = p.pop("frequencyPenalty")
    if "repeatPenalty" in p and "repeat_penalty" not in p:
        p["repeat_penalty"] = p.pop("repeatPenalty")
    if isinstance(p.get("thinking"), str):
        legacy = str(p.get("thinking"))
        mapping = {
            "off": {"enabled": False, "mode": "none"},
            "auto": {"enabled": True, "mode": "hidden"},
            "on": {"enabled": True, "mode": "visible"},
        }
        p["thinking"] = mapping.get(legacy, {"enabled": False, "mode": "none"})

    return p


def _llm_schema_declared_output_mode(node: Dict[str, Any]) -> str:
    data = (node.get("data", {}) if isinstance(node, dict) else {}) or {}
    schema_env = data.get("schema") if isinstance(data.get("schema"), dict) else {}
    if isinstance(schema_env, dict):
        for key in ("expectedSchema", "inferredSchema", "observedSchema"):
            obs = schema_env.get(key)
            if not isinstance(obs, dict):
                continue
            typed = obs.get("typedSchema")
            if not isinstance(typed, dict):
                continue
            t = str(typed.get("type") or "").strip().lower()
            if t == "string":
                t = "text"
            if t in {"json", "embeddings", "text"}:
                return t
    return "text"

#

def _resolve_llm_output_mode(node: Dict[str, Any], norm_params: Dict[str, Any]) -> str:
    explicit = str(norm_params.get("output_mode") or "").strip().lower()
    if explicit in {"text", "json", "embeddings"}:
        return explicit
    return _llm_schema_declared_output_mode(node)


def iso_now():
    return datetime.now(timezone.utc).isoformat()


def _canon_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _is_table_artifact(art: Any) -> bool:
    mime = str(getattr(art, "mime_type", "") or "").lower()
    payload_schema = getattr(art, "payload_schema", None) or {}
    schema_payload_type = str(payload_schema.get("type") or "").lower() if isinstance(payload_schema, dict) else ""
    artifact_payload_type = str(getattr(art, "payload_type", "") or "").lower()
    payload_type = artifact_payload_type or schema_payload_type
    return (
        payload_type == "table"
        or schema_payload_type == "table"
        or "csv" in mime
        or "tab-separated-values" in mime
        or "parquet" in mime
        or "spreadsheet" in mime
        or "excel" in mime
    )


def _is_json_artifact(art: Any) -> bool:
    mime = str(getattr(art, "mime_type", "") or "").lower()
    payload_schema = getattr(art, "payload_schema", None) or {}
    schema_payload_type = str(payload_schema.get("type") or "").lower() if isinstance(payload_schema, dict) else ""
    artifact_payload_type = str(getattr(art, "payload_type", "") or "").lower()
    payload_type = artifact_payload_type or schema_payload_type
    return payload_type == "json" or schema_payload_type == "json" or "application/json" in mime or "json" in mime


async def _serialize_artifact_input(context: GraphContext, artifact_id: str, input_encoding: str) -> str:
    if input_encoding == "text":
        return await materialize_text(context, artifact_id)

    art = await context.artifact_store.get(artifact_id)
    payload = await context.artifact_store.read(artifact_id)
    mime = str(getattr(art, "mime_type", "") or "")

    if input_encoding == "json_canonical":
        try:
            obj = json.loads(payload.decode("utf-8", errors="replace"))
        except Exception as e:
            if not _is_json_artifact(art):
                raise ValueError(f"inputEncoding=json_canonical requires JSON artifact input (artifact_id={artifact_id})") from e
            raise ValueError(f"Failed parsing JSON input artifact {artifact_id}: {e}") from e
        return _canon_json(obj)

    if input_encoding == "table_canonical":
        if not _is_table_artifact(art):
            raise ValueError(
                f"inputEncoding=table_canonical requires table artifact input (artifact_id={artifact_id})"
            )
        try:
            df = load_table_from_artifact_bytes(mime, payload)
        except Exception as e:
            raise ValueError(
                f"inputEncoding=table_canonical is not supported for mime_type={mime!r} (artifact_id={artifact_id})"
            ) from e
        cols = sorted(str(c) for c in list(df.columns))
        records = []
        for row in df.to_dict(orient="records"):
            records.append({k: row.get(k) for k in cols})
        return _canon_json({"format": "table_canonical_v1", "columns": cols, "rows": records})

    raise ValueError(f"Unsupported input_encoding: {input_encoding}")


def _serialize_runtime_work_item_input(work_item: Dict[str, Any], input_encoding: str) -> Optional[str]:
    preview = work_item.get("itemPreview")
    if preview is None:
        return None

    if input_encoding == "json_canonical":
        return _canon_json(preview)

    if input_encoding == "table_canonical":
        if isinstance(preview, dict):
            cols = sorted(str(k) for k in preview.keys())
            row = {k: preview.get(k) for k in cols}
            return _canon_json({"format": "table_canonical_v1", "columns": cols, "rows": [row]})
        return _canon_json({"format": "table_canonical_v1", "columns": ["value"], "rows": [{"value": preview}]})

    # text (default): keep scalar text as-is; object/list become compact canonical JSON.
    if isinstance(preview, str):
        return preview
    return _canon_json(preview)


def _runtime_work_item_preview(work_item: Dict[str, Any], max_chars: int = 220) -> str:
    preview = work_item.get("itemPreview")
    if isinstance(preview, dict):
        if isinstance(preview.get("title"), str) and preview.get("title", "").strip():
            return f"title={str(preview.get('title')).strip()[:max_chars]}"
        if isinstance(preview.get("id"), (str, int, float)):
            return f"id={str(preview.get('id'))[:80]}"
    if isinstance(preview, str):
        s = preview.strip()
        return s[:max_chars]
    try:
        return _canon_json(preview)[:max_chars]
    except Exception:
        return str(preview)[:max_chars]


def _llm_debug_flag(params: LLMParams, key: str) -> bool:
    debug_cfg = getattr(params, "debug", None)
    if debug_cfg is None:
        return False
    if isinstance(debug_cfg, dict):
        return bool(debug_cfg.get(key, False))
    return bool(getattr(debug_cfg, key, False))


def _llm_debug_excerpt(value: Any, max_chars: int = 800) -> str:
    try:
        text = value if isinstance(value, str) else _canon_json(value)
    except Exception:
        text = str(value)
    text = str(text or "")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    return text[:max_chars]


async def _serialize_image_media(context: GraphContext, artifact_id: str) -> Optional[Dict[str, Any]]:
    art = await context.artifact_store.get(artifact_id)
    mime = str(getattr(art, "mime_type", "") or "").strip().lower()
    payload_type = str(getattr(art, "payload_type", "") or "").strip().lower()
    if payload_type != "image" and not mime.startswith("image/"):
        return None
    payload = await context.artifact_store.read(artifact_id)
    if not isinstance(payload, (bytes, bytearray)):
        return None
    if not mime:
        mime = "image/png"
    b64 = base64.b64encode(bytes(payload)).decode("ascii")
    return {
        "type": "image",
        "mimeType": mime,
        "dataUrl": f"data:{mime};base64,{b64}",
    }


async def _serialize_audio_media(context: GraphContext, artifact_id: str) -> Optional[Dict[str, Any]]:
    art = await context.artifact_store.get(artifact_id)
    mime = str(getattr(art, "mime_type", "") or "").strip().lower()
    payload_type = str(getattr(art, "payload_type", "") or "").strip().lower()
    if payload_type != "audio" and not mime.startswith("audio/"):
        return None
    payload = await context.artifact_store.read(artifact_id)
    if not isinstance(payload, (bytes, bytearray)):
        return None
    if not mime:
        mime = "audio/wav"
    b64 = base64.b64encode(bytes(payload)).decode("ascii")
    return {
        "type": "audio",
        "mimeType": mime,
        "dataUrl": f"data:{mime};base64,{b64}",
    }


def _canonicalize_input_envelope(raw: Any) -> tuple[list[str], list[Dict[str, Any]]]:
    if raw is None:
        return [], []
    if not isinstance(raw, list):
        raise ValueError("input_envelope must be an array")
    text_parts: list[str] = []
    media_parts: list[Dict[str, Any]] = []
    for i, part in enumerate(raw):
        if not isinstance(part, dict):
            raise ValueError(f"input_envelope[{i}] must be an object")
        part_type = str(part.get("type") or "").strip().lower()
        if part_type == "text":
            text = part.get("text")
            if not isinstance(text, str):
                raise ValueError(f"input_envelope[{i}].text is required for type=text")
            text_parts.append(text)
            continue
        if part_type not in {"image", "audio"}:
            raise ValueError(f"input_envelope[{i}].type must be one of: text, image, audio")
        data_url = part.get("dataUrl")
        if not isinstance(data_url, str) or not data_url.strip():
            raise ValueError(f"input_envelope[{i}].dataUrl is required for type={part_type}")
        media: Dict[str, Any] = {"type": part_type, "dataUrl": data_url.strip()}
        mime = str(part.get("mimeType") or "").strip().lower()
        if mime:
            media["mimeType"] = mime
        media_parts.append(media)
    return text_parts, media_parts


def _model_error_payload(code: str, message: str, **extra: Any) -> str:
    payload: Dict[str, Any] = {"code": code, "errorCode": code, "message": str(message)}
    if extra:
        payload.update(extra)
    return json.dumps(payload, sort_keys=True, ensure_ascii=False)


def _normalize_model_error(error: Optional[str], *, default_code: str) -> str:
    raw = str(error or "").strip()
    if not raw:
        return _model_error_payload(default_code, "Model execution failed")
    try:
        parsed = json.loads(raw)
    except Exception:
        return _model_error_payload(default_code, raw)
    if isinstance(parsed, dict):
        code = str(parsed.get("errorCode") or parsed.get("code") or default_code).strip() or default_code
        message = str(parsed.get("message") or raw).strip() or raw
        parsed["code"] = code
        parsed["errorCode"] = code
        parsed["message"] = message
        return json.dumps(parsed, sort_keys=True, ensure_ascii=False)
    return _model_error_payload(default_code, raw)

async def exec_llm(
    run_id: str,
    node: Dict[str, Any],
    context: GraphContext,
    upstream_artifact_ids: Optional[list[str]] = None
) -> NodeOutput:
    """Execute LLM node"""
    
    node_id = node["id"]

    upstream_artifact_ids = upstream_artifact_ids or []

    assert context is not None, "context is None"
    assert hasattr(context, "bus"), "context missing bus"

    raw_params = node.get("data", {}).get("params", {}) or {}
    logger.debug("Model node raw params normalized", extra={"nodeId": node_id})

    norm_params = normalize_llm_params(raw_params)
    resolved_mode = _resolve_llm_output_mode(node, norm_params)
    norm_params["output_mode"] = resolved_mode
    if resolved_mode == "json" and not isinstance(norm_params.get("output_schema"), dict):
        # Schema-first: JSON mode is chosen by typed schema declaration, so allow empty schema by default.
        norm_params["output_schema"] = {}
    logger.debug("Model node params validated", extra={"nodeId": node_id, "llmKind": node.get("data", {}).get("llmKind")})

    # ✅ Validate normalized dict
    llm_params = LLMParams.model_validate(norm_params)

    llm_kind = node.get("data", {}).get("llmKind") or "ollama"
    adapter = get_model_adapter(llm_kind)
    model_kind = str(node.get("data", {}).get("modelKind") or "llm").strip().lower()

    input_encoding = llm_params.input_encoding or "text"
    runtime_work_item = raw_params.get("_work_item") if isinstance(raw_params.get("_work_item"), dict) else None
    serialized_inputs: List[str] = []
    serialized_media: List[Dict[str, Any]] = []
    runtime_item_text = (
        _serialize_runtime_work_item_input(runtime_work_item, input_encoding)
        if isinstance(runtime_work_item, dict)
        else None
    )
    if runtime_item_text is not None:
        text = runtime_item_text
        serialized_inputs = [text] if text else []
    elif not upstream_artifact_ids:
        text = ""
    elif len(upstream_artifact_ids) == 1:
        aid = upstream_artifact_ids[0]
        if model_kind in {"vision", "multimodal"}:
            media = await _serialize_image_media(context, aid)
            if media is not None:
                serialized_media.append(media)
        if model_kind in {"audio", "multimodal"}:
            media = await _serialize_audio_media(context, aid)
            if media is not None:
                serialized_media.append(media)
        text = await _serialize_artifact_input(context, aid, input_encoding)
        serialized_inputs = [text] if text else []
    else:
        text_parts: List[str] = []
        for idx, aid in enumerate(upstream_artifact_ids, start=1):
            if model_kind in {"vision", "multimodal"}:
                media = await _serialize_image_media(context, aid)
                if media is not None:
                    serialized_media.append(media)
            if model_kind in {"audio", "multimodal"}:
                media = await _serialize_audio_media(context, aid)
                if media is not None:
                    serialized_media.append(media)
            payload = await _serialize_artifact_input(context, aid, input_encoding)
            serialized_inputs.append(payload)
            text_parts.append(f"### INPUT {idx} artifact={aid}\n{payload}")
        text = "\n\n---\n\n".join(text_parts)
    envelope_text_parts, envelope_media_parts = _canonicalize_input_envelope(getattr(llm_params, "input_envelope", None))
    if envelope_text_parts:
        envelope_text = "\n".join(envelope_text_parts)
        text = f"{text}\n\n--- ENVELOPE ---\n{envelope_text}" if text else envelope_text
        serialized_inputs = [*serialized_inputs, envelope_text]
    if envelope_media_parts:
        serialized_media = [*serialized_media, *envelope_media_parts]
    logger.debug(
        "Model input prepared",
        extra={"nodeId": node_id, "upstreamCount": len(upstream_artifact_ids), "inputChars": len(text), "inputEncoding": input_encoding},
    )
    if isinstance(runtime_work_item, dict):
        await context.bus.emit(
            {
                "type": "log",
                "runId": run_id,
                "at": iso_now(),
                "level": "info",
                "message": (
                    "LLM work-item input: "
                    f"mode={str(runtime_work_item.get('itemMode') or 'artifact')} "
                    f"index={int(runtime_work_item.get('itemIndex') or 0)} "
                    f"artifact={str(runtime_work_item.get('artifactId') or '')[:12]} "
                    f"preview={_runtime_work_item_preview(runtime_work_item)}"
                ),
                "nodeId": node["id"],
            }
        )
    if _llm_debug_flag(llm_params, "enabled") and _llm_debug_flag(llm_params, "log_input_preview"):
        await context.bus.emit(
            {
                "type": "log",
                "runId": run_id,
                "at": iso_now(),
                "level": "info",
                "message": (
                    "LLM debug input excerpt: "
                    f"encoding={input_encoding} chars={len(text or '')} "
                    f"preview={_llm_debug_excerpt(text or '', 900)}"
                ),
                "nodeId": node["id"],
            }
        )


    await context.bus.emit(
        {
            "type": "log",
            "runId": run_id,
            "at": iso_now(),
            "level": "info",
            "message": f"LLM ({adapter.provider}) model: {llm_params.model}",
            "nodeId": node["id"],
        }
    )

    async def _dispatch(kind: str, params_override: LLMParams) -> NodeOutput:
        if kind == "ollama":
            return await exec_llm_ollama(
                run_id,
                node,
                context,
                None,
                params_override,
                input_text=text,
                input_items=serialized_inputs,
                input_media=serialized_media,
                upstream_artifact_ids=upstream_artifact_ids,
            )
        if kind == "openai_compat":
            return await exec_llm_openai_compat(
                run_id,
                node,
                context,
                None,
                params_override,
                input_text=text,
                input_items=serialized_inputs,
                input_media=serialized_media,
                upstream_artifact_ids=upstream_artifact_ids,
            )
        raise ValueError(f"Unsupported llmKind: {kind}")

    async def _emit_llm_lease(
        *,
        provider: str,
        state: str,
        node_id_for_event: Optional[str] = None,
        holder_node_id: Optional[str] = None,
    ) -> None:
        waiting_node_ids = _provider_waiting_nodes(provider)
        await context.bus.emit(
            {
                "type": "llm_lease",
                "schema_version": 1,
                "runId": run_id,
                "at": iso_now(),
                "state": str(state),
                "nodeId": str(node_id_for_event or node["id"]),
                "holderNodeId": holder_node_id,
                "waitQueueLength": len(waiting_node_ids),
                "waitingNodeIds": waiting_node_ids,
            }
        )

    request_policy = normalize_request_policy(llm_params)
    if request_policy.deterministic_enabled:
        if request_policy.deterministic_seed is not None and llm_params.seed is None:
            llm_params = llm_params.model_copy(update={"seed": int(request_policy.deterministic_seed)})
        if request_policy.deterministic_stable_order:
            serialized_media = sorted(
                list(serialized_media),
                key=lambda m: (
                    str(m.get("type") or ""),
                    str(m.get("mimeType") or ""),
                    str(m.get("dataUrl") or ""),
                ),
            )
        fingerprint_payload = {
            "llmKind": llm_kind,
            "model": llm_params.model,
            "seed": llm_params.seed,
            "inputItems": serialized_inputs,
            "inputMedia": serialized_media,
        }
        fingerprint = hashlib.sha256(
            json.dumps(fingerprint_payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
        ).hexdigest()
        await context.bus.emit(
            {
                "type": "log",
                "runId": run_id,
                "at": iso_now(),
                "level": "info",
                "message": f"MODEL_DETERMINISM: hash={fingerprint} seed={llm_params.seed}",
                "nodeId": node["id"],
            }
        )
    chain = [{"llm_kind": llm_kind, "params": llm_params}]
    for fallback in request_policy.fallback_chain:
        kind = str(fallback.get("llm_kind") or fallback.get("llmKind") or llm_kind).strip().lower()
        patch: Dict[str, Any] = {}
        for src_key, dst_key in (
            ("connection_ref", "connection_ref"),
            ("connectionRef", "connection_ref"),
            ("base_url", "base_url"),
            ("baseUrl", "base_url"),
            ("model", "model"),
            ("api_key_ref", "api_key_ref"),
            ("apiKeyRef", "api_key_ref"),
        ):
            val = fallback.get(src_key)
            if val is not None:
                patch[dst_key] = val
        chain.append({"llm_kind": kind, "params": llm_params.model_copy(update=patch)})

    last_output: Optional[NodeOutput] = None
    for idx, entry in enumerate(chain, start=1):
        kind = str(entry["llm_kind"])
        params_override = entry["params"]
        if idx > 1:
            await context.bus.emit(
                {
                    "type": "log",
                    "runId": run_id,
                    "at": iso_now(),
                    "level": "warn",
                    "message": f"MODEL_FALLBACK_ACTIVATED: trying fallback #{idx - 1} provider={kind} model={params_override.model}",
                    "nodeId": node["id"],
                }
            )
        sem = _provider_semaphore(kind)
        if sem is not None:
            acquire_timeout = _provider_acquire_timeout_seconds(kind)
            waiting_started_at = asyncio.get_running_loop().time()
            _provider_waiter_add(kind, str(node["id"]))
            await _emit_llm_lease(
                provider=kind,
                state="waiting",
                node_id_for_event=str(node["id"]),
                holder_node_id=_provider_holder_get(kind),
            )
            await context.bus.emit(
                {
                    "type": "log",
                    "runId": run_id,
                    "at": iso_now(),
                    "level": "info",
                    "message": f"MODEL_PROVIDER_QUEUE: waiting for provider slot provider={kind}",
                    "nodeId": node["id"],
                }
            )
            try:
                if acquire_timeout > 0:
                    await asyncio.wait_for(sem.acquire(), timeout=acquire_timeout)
                else:
                    await sem.acquire()
            except asyncio.TimeoutError:
                _provider_waiter_remove(kind, str(node["id"]))
                await _emit_llm_lease(
                    provider=kind,
                    state="waiting",
                    node_id_for_event=str(node["id"]),
                    holder_node_id=_provider_holder_get(kind),
                )
                return NodeOutput(
                    status="failed",
                    metadata=None,
                    execution_time_ms=0.0,
                    error=_model_error_payload(
                        "MODEL_PROVIDER_ACQUIRE_TIMEOUT",
                        f"Timed out waiting for provider slot ({kind}) after {acquire_timeout:.2f}s",
                        provider=kind,
                        acquireTimeoutSeconds=acquire_timeout,
                    ),
                )
            waited_ms = max(0, int((asyncio.get_running_loop().time() - waiting_started_at) * 1000))
            _provider_waiter_remove(kind, str(node["id"]))
            holder_node_id = _provider_holder_set(kind, str(node["id"]))
            await _emit_llm_lease(
                provider=kind,
                state="acquired",
                node_id_for_event=str(node["id"]),
                holder_node_id=holder_node_id,
            )
            await context.bus.emit(
                {
                    "type": "log",
                    "runId": run_id,
                    "at": iso_now(),
                    "level": "info",
                    "message": (
                        f"MODEL_PROVIDER_ACQUIRED: provider={kind} wait_ms={waited_ms} "
                        "request timeout starts now"
                    ),
                    "nodeId": node["id"],
                }
            )
            await context.bus.emit(
                {
                    "type": "control_signal",
                    "runId": run_id,
                    "at": iso_now(),
                    "signal": "llm_acquired",
                    "nodeId": node["id"],
                }
            )
            try:
                out = await _dispatch(kind, params_override)
            finally:
                current_holder = _provider_holder_get(kind)
                if current_holder == str(node["id"]):
                    _provider_holder_set(kind, None)
                await _emit_llm_lease(
                    provider=kind,
                    state="released",
                    node_id_for_event=str(node["id"]),
                    holder_node_id=_provider_holder_get(kind),
                )
                await context.bus.emit(
                    {
                        "type": "control_signal",
                        "runId": run_id,
                        "at": iso_now(),
                        "signal": "llm_released",
                        "nodeId": node["id"],
                    }
                )
                sem.release()
        else:
            _provider_holder_set(kind, str(node["id"]))
            await _emit_llm_lease(
                provider=kind,
                state="acquired",
                node_id_for_event=str(node["id"]),
                holder_node_id=str(node["id"]),
            )
            await context.bus.emit(
                {
                    "type": "control_signal",
                    "runId": run_id,
                    "at": iso_now(),
                    "signal": "llm_acquired",
                    "nodeId": node["id"],
                }
            )
            try:
                out = await _dispatch(kind, params_override)
            finally:
                _provider_holder_set(kind, None)
                await _emit_llm_lease(
                    provider=kind,
                    state="released",
                    node_id_for_event=str(node["id"]),
                    holder_node_id=None,
                )
                await context.bus.emit(
                    {
                        "type": "control_signal",
                        "runId": run_id,
                        "at": iso_now(),
                        "signal": "llm_released",
                        "nodeId": node["id"],
                    }
                )
        if out.status == "succeeded":
            return out
        last_output = out.model_copy(
            update={
                "error": _normalize_model_error(
                    out.error,
                    default_code="MODEL_EXECUTION_FAILED",
                )
            }
        )
    if last_output is not None:
        return last_output
    return NodeOutput(
        status="failed",
        metadata=None,
        execution_time_ms=0.0,
        error=_model_error_payload("MODEL_KIND_UNSUPPORTED", "Unsupported llmKind"),
    )


