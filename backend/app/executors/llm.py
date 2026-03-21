import json
import base64
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
from pprint import pformat

# print("[exec_llm] has bus?", hasattr(context, "bus"), type(context.bus))


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

    # nested output -> flattened output schema controls
    out = p.get("output")
    if isinstance(out, dict):
        if "jsonSchema" in out and "output_schema" not in p:
            p["output_schema"] = out.get("jsonSchema")
        if "strict" in out and "output_strict" not in p:
            p["output_strict"] = out.get("strict")
        if "embedding" in out and "embedding_contract" not in p:
            p["embedding_contract"] = out.get("embedding")

    if "stop" in p and "stop_sequences" not in p:
        p["stop_sequences"] = p.pop("stop")
    if "inputEncoding" in p and "input_encoding" not in p:
        p["input_encoding"] = p.pop("inputEncoding")
    if "inputEnvelope" in p and "input_envelope" not in p:
        p["input_envelope"] = p.pop("inputEnvelope")
    if "requestPolicy" in p and "request_policy" not in p:
        p["request_policy"] = p.pop("requestPolicy")
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

def iso_now():
    return datetime.now(timezone.utc).isoformat()


def _canon_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _is_table_artifact(art: Any) -> bool:
    mime = str(getattr(art, "mime_type", "") or "").lower()
    payload_schema = getattr(art, "payload_schema", None) or {}
    payload_type = str(payload_schema.get("type") or "").lower() if isinstance(payload_schema, dict) else ""
    payload_type = str(getattr(art, "payload_type", "") or "").lower()
    return (
        payload_type == "table"
        or payload_type == "table"
        or "csv" in mime
        or "tab-separated-values" in mime
        or "parquet" in mime
        or "spreadsheet" in mime
        or "excel" in mime
    )


def _is_json_artifact(art: Any) -> bool:
    mime = str(getattr(art, "mime_type", "") or "").lower()
    payload_schema = getattr(art, "payload_schema", None) or {}
    payload_type = str(payload_schema.get("type") or "").lower() if isinstance(payload_schema, dict) else ""
    payload_type = str(getattr(art, "payload_type", "") or "").lower()
    return payload_type == "json" or payload_type == "json" or "application/json" in mime or "json" in mime


async def _serialize_artifact_input(context: GraphContext, artifact_id: str, input_encoding: str) -> str:
    if input_encoding == "text":
        return await materialize_text(context, artifact_id)

    art = await context.artifact_store.get(artifact_id)
    payload = await context.artifact_store.read(artifact_id)
    mime = str(getattr(art, "mime_type", "") or "")

    if input_encoding == "json_canonical":
        if not _is_json_artifact(art):
            raise ValueError(f"inputEncoding=json_canonical requires JSON artifact input (artifact_id={artifact_id})")
        try:
            obj = json.loads(payload.decode("utf-8", errors="replace"))
        except Exception as e:
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
    print("LLM EXEC raw_params (before normalize):", pformat(raw_params)[:8000])

    norm_params = normalize_llm_params(raw_params)
    declared_mode = _llm_schema_declared_output_mode(node)
    norm_params["output_mode"] = declared_mode
    if declared_mode == "json" and not isinstance(norm_params.get("output_schema"), dict):
        # Schema-first: JSON mode is chosen by typed schema declaration, so allow empty schema by default.
        norm_params["output_schema"] = {}
    print("LLM EXEC norm_params (after normalize):", pformat(norm_params)[:8000])

    # ✅ Validate normalized dict
    llm_params = LLMParams.model_validate(norm_params)

    llm_kind = node.get("data", {}).get("llmKind") or "ollama"
    adapter = get_model_adapter(llm_kind)
    model_kind = str(node.get("data", {}).get("modelKind") or "llm").strip().lower()

    input_encoding = llm_params.input_encoding or "text"
    serialized_inputs: List[str] = []
    serialized_media: List[Dict[str, Any]] = []
    if not upstream_artifact_ids:
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
    print("[llm] upstream_ids:", upstream_artifact_ids, "len:", len(text), "input_encoding:", input_encoding)


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

    request_policy = normalize_request_policy(llm_params)
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
        out = await _dispatch(kind, params_override)
        if out.status == "succeeded":
            return out
        last_output = out
    return last_output if last_output is not None else NodeOutput(status="failed", metadata=None, execution_time_ms=0.0, error="Unsupported llmKind")


