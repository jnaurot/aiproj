from typing import Any, Dict, Optional

from app.runner.materialize import materialize_text
from ..runner.metadata import ExecutionContext, NodeOutput
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

    # nested output -> flattened output_mode/output_schema (if your backend uses those)
    out = p.get("output")
    if isinstance(out, dict):
        if "mode" in out and "output_mode" not in p:
            p["output_mode"] = out.get("mode")
        if "jsonSchema" in out and "output_schema" not in p:
            p["output_schema"] = out.get("jsonSchema")

    return p

#

def iso_now():
    return datetime.now(timezone.utc).isoformat()

async def exec_llm(
    run_id: str,
    node: Dict[str, Any],
    context: ExecutionContext,
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
    print("LLM EXEC norm_params (after normalize):", pformat(norm_params)[:8000])

    # ✅ Validate normalized dict
    llm_params = LLMParams.model_validate(norm_params)

    llm_kind = node.get("data", {}).get("llmKind") or "ollama"

    text_parts: list[str] = []
    if not upstream_artifact_ids:
        text = ""
    elif len(upstream_artifact_ids) == 1:
        text = await materialize_text(context, upstream_artifact_ids[0])
    else:
        # Deterministic multi-input combine: stable order from upstream_artifact_ids,
        # explicit provenance headers, and fixed separators.
        for idx, aid in enumerate(upstream_artifact_ids, start=1):
            payload = await materialize_text(context, aid)
            text_parts.append(f"### INPUT {idx} artifact={aid}\n{payload}")
        text = "\n\n---\n\n".join(text_parts)
    print("[llm] upstream_ids:", upstream_artifact_ids, "len:", len(text))


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
            upstream_artifact_ids=upstream_artifact_ids,
        )


    if llm_kind == "openai_compat":
        return await exec_llm_openai_compat(
            run_id,
            node,
            context,
            None,
            llm_params,
            upstream_artifact_ids=upstream_artifact_ids,
        )


    raise ValueError(f"Unsupported llmKind: {llm_kind}")

