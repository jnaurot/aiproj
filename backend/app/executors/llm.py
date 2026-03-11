import json
from typing import Any, Dict, List, Optional

from app.runner.materialize import materialize_text
from app.runner.nodes.transform import load_table_from_artifact_bytes
from ..runner.metadata import GraphContext, NodeOutput
from datetime import datetime, timezone

from ..runner.schemas import LLMParams
from .llm_ollama import exec_llm_ollama           # new module (suggested)
from .llm_openai_compat import exec_llm_openai_compat
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
    port_type = str(getattr(art, "port_type", "") or "").lower()
    return (
        port_type == "table"
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
    port_type = str(getattr(art, "port_type", "") or "").lower()
    return port_type == "json" or payload_type == "json" or "application/json" in mime or "json" in mime


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

    input_encoding = llm_params.input_encoding or "text"
    serialized_inputs: List[str] = []
    if not upstream_artifact_ids:
        text = ""
    elif len(upstream_artifact_ids) == 1:
        text = await _serialize_artifact_input(context, upstream_artifact_ids[0], input_encoding)
        serialized_inputs = [text]
    else:
        text_parts: List[str] = []
        for idx, aid in enumerate(upstream_artifact_ids, start=1):
            payload = await _serialize_artifact_input(context, aid, input_encoding)
            serialized_inputs.append(payload)
            text_parts.append(f"### INPUT {idx} artifact={aid}\n{payload}")
        text = "\n\n---\n\n".join(text_parts)
    print("[llm] upstream_ids:", upstream_artifact_ids, "len:", len(text), "input_encoding:", input_encoding)


    await context.bus.emit(
        {
            "type": "log",
            "runId": run_id,
            "at": iso_now(),
            "level": "info",
            "message": f"LLM ({llm_kind}) model: {llm_params.model}",
            "nodeId": node["id"],
        }
    )

    # ✅ Dispatch by node-level discriminator
    if llm_kind == "ollama":
        return await exec_llm_ollama(
            run_id,
            node,
            context,
            None,
            llm_params,
            input_text=text,
            input_items=serialized_inputs,
            upstream_artifact_ids=upstream_artifact_ids,
        )


    if llm_kind == "openai_compat":
        return await exec_llm_openai_compat(
            run_id,
            node,
            context,
            None,
            llm_params,
            input_text=text,
            input_items=serialized_inputs,
            upstream_artifact_ids=upstream_artifact_ids,
        )


    raise ValueError(f"Unsupported llmKind: {llm_kind}")

