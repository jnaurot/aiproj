from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Any, Dict, Optional
import httpx

from fastapi import APIRouter, Header, HTTPException, Query, Request
from pydantic import BaseModel, field_validator
from ..services.runtime_env import get_bool_env, get_env

router = APIRouter()


def _require_admin_for_promotion(x_model_admin: Optional[str]) -> None:
	require_admin = get_bool_env("MODEL_REGISTRY_REQUIRE_ADMIN", False)
	if not require_admin:
		return
	header_value = str(x_model_admin or "").strip().lower()
	if header_value not in {"1", "true", "yes", "on", "admin"}:
		raise HTTPException(status_code=403, detail="model promotion requires admin permission")


def _get_store(request: Request):
	store = getattr(request.app.state, "model_registry", None)
	if store is None:
		raise HTTPException(status_code=500, detail="model registry store unavailable")
	return store


async def _resolve_run_experiment(request: Request, run_id: Optional[str]) -> Optional[Dict[str, Any]]:
	rid = str(run_id or "").strip()
	if not rid:
		return None
	rt = request.app.state.runtime
	get_fn = getattr(rt.artifact_store, "get_run_experiment", None)
	if not callable(get_fn):
		raise HTTPException(status_code=404, detail="experiment tracking unavailable")
	row = await get_fn(rid)
	if not isinstance(row, dict):
		raise HTTPException(status_code=404, detail=f"run experiment not found: {rid}")
	return row


class ModelRegisterVersionRequest(BaseModel):
	modelId: Optional[str] = None
	modelName: Optional[str] = None
	versionId: Optional[str] = None
	stage: str = "candidate"
	runId: Optional[str] = None
	artifactId: Optional[str] = None
	metrics: Optional[Dict[str, Any]] = None
	params: Optional[Dict[str, Any]] = None
	environment: Optional[Dict[str, Any]] = None
	provenance: Optional[Dict[str, Any]] = None

	@field_validator("stage")
	@classmethod
	def validate_stage(cls, v):
		s = str(v or "").strip().lower()
		if s not in {"candidate", "baseline", "prod"}:
			raise ValueError("stage must be one of: candidate, baseline, prod")
		return s


class ModelPromoteRequest(BaseModel):
	toStage: str
	force: bool = False
	promotedBy: Optional[str] = None

	@field_validator("toStage")
	@classmethod
	def validate_stage(cls, v):
		s = str(v or "").strip().lower()
		if s not in {"candidate", "baseline", "prod"}:
			raise ValueError("toStage must be one of: candidate, baseline, prod")
		return s


class NodeDocExplainRequest(BaseModel):
	context: Dict[str, Any]
	signatureKey: str
	provider: Optional[str] = None
	model: Optional[str] = None
	temperature: Optional[float] = None
	topP: Optional[float] = None
	maxTokens: Optional[int] = None


class NodeDocExplainResponse(BaseModel):
	summary: str
	settings_explained: list[str]
	context_notes: list[str]
	generated_at: str
	signature_key: str
	provider_meta: Dict[str, str]


class NodeDocFeedbackRequest(BaseModel):
	context: Dict[str, Any]
	signatureKey: str
	generatedSummary: str
	verdict: str
	correctedSummary: Optional[str] = None

	@field_validator("verdict")
	@classmethod
	def validate_verdict(cls, v):
		s = str(v or "").strip().lower()
		if s not in {"good", "bad"}:
			raise ValueError("verdict must be one of: good, bad")
		return s


class NodeDocFeedbackResponse(BaseModel):
	ok: bool
	stored: bool
	entry_id: str
	kind: str
	subtype: str
	suggestion_file: str
	suggested_fields: list[str]
	notes: list[str]


_TOKEN_RE = re.compile(r"[a-z0-9_]+")


def _tokenize_text(value: str) -> set[str]:
	return {match.group(0) for match in _TOKEN_RE.finditer(str(value or "").strip().lower())}


def _suggest_fields_from_feedback(
	candidate_fields: Dict[str, Any],
	generated_summary: str,
	corrected_summary: str,
	limit: int = 8,
) -> list[str]:
	if not isinstance(candidate_fields, dict) or not candidate_fields:
		return []
	corrected_tokens = _tokenize_text(corrected_summary)
	generated_tokens = _tokenize_text(generated_summary)
	delta_tokens = corrected_tokens - generated_tokens
	scores: list[tuple[int, str]] = []
	for key, raw_value in candidate_fields.items():
		k = str(key or "").strip()
		if not k:
			continue
		value = str(raw_value or "").strip().lower()
		tokens = _tokenize_text(f"{k} {value}")
		if not tokens:
			continue
		score = len(tokens.intersection(delta_tokens))
		if score <= 0:
			continue
		scores.append((score, k))
	scores.sort(key=lambda item: (-item[0], item[1]))
	return [name for _, name in scores[: max(1, int(limit))]]


def _build_feedback_candidate_fields(context: Dict[str, Any], settings: Dict[str, Any]) -> Dict[str, Any]:
	fields: Dict[str, Any] = {}
	if isinstance(settings, dict):
		for key, value in settings.items():
			k = str(key or "").strip()
			if not k:
				continue
			fields[k] = value
	planes = context.get("planes") if isinstance(context.get("planes"), dict) else {}
	runtime = context.get("runtime") if isinstance(context.get("runtime"), dict) else {}
	node_label = str(context.get("node_label") or context.get("nodeLabel") or "").strip()
	node_kind = str(context.get("node_kind") or context.get("nodeKind") or "").strip()
	node_subtype = str(context.get("node_subtype") or context.get("nodeSubtype") or "").strip()
	if node_label:
		fields["node_label"] = node_label
	if node_kind:
		fields["node_kind"] = node_kind
	if node_subtype:
		fields["node_subtype"] = node_subtype
	data_inputs = planes.get("data_inputs") if isinstance(planes.get("data_inputs"), list) else []
	data_outputs = planes.get("data_outputs") if isinstance(planes.get("data_outputs"), list) else []
	data_input_sources = planes.get("data_input_sources") if isinstance(planes.get("data_input_sources"), list) else []
	param_inputs = planes.get("param_inputs") if isinstance(planes.get("param_inputs"), list) else []
	control_inputs = planes.get("control_inputs") if isinstance(planes.get("control_inputs"), list) else []
	fields["data_inputs"] = ", ".join(str(v) for v in data_inputs if str(v).strip())
	fields["data_outputs"] = ", ".join(str(v) for v in data_outputs if str(v).strip())
	fields["data_input_sources"] = ", ".join(str(v) for v in data_input_sources if str(v).strip())
	fields["param_inputs"] = ", ".join(str(v) for v in param_inputs if str(v).strip())
	fields["control_inputs"] = ", ".join(str(v) for v in control_inputs if str(v).strip())
	for key in ("pending_input_count", "inflight", "ready_work", "blocked_reason_code"):
		if key in runtime:
			fields[f"runtime_{key}"] = runtime.get(key)
	return fields


def _node_doc_feedback_llm_enabled() -> bool:
	return get_bool_env("NODE_DOC_FEEDBACK_LLM_ENABLED", True)


def _node_doc_feedback_timeout_seconds() -> float:
	raw = str(get_env("NODE_DOC_FEEDBACK_LLM_TIMEOUT_SECONDS", "4") or "").strip()
	try:
		return max(0.5, min(30.0, float(raw)))
	except Exception:
		return 4.0


def _node_doc_feedback_max_fields() -> int:
	raw = str(get_env("NODE_DOC_FEEDBACK_MAX_FIELDS", "8") or "").strip()
	try:
		return max(1, min(20, int(raw)))
	except Exception:
		return 8


def _node_doc_feedback_base_url() -> str:
	base = str(get_env("NODE_DOC_FEEDBACK_LLM_BASE_URL", "") or "").strip()
	if not base:
		base = str(get_env("OLLAMA_BASE_URL", "http://127.0.0.1:11434") or "").strip()
	return base.rstrip("/")


def _node_doc_feedback_model() -> str:
	return str(get_env("NODE_DOC_FEEDBACK_LLM_MODEL", "glm-4.7-flash:latest") or "").strip() or "glm-4.7-flash:latest"


def _node_doc_explain_llm_enabled() -> bool:
	return get_bool_env("NODE_DOC_EXPLAIN_LLM_ENABLED", True)


def _node_doc_explain_timeout_seconds() -> float:
	raw = str(get_env("NODE_DOC_EXPLAIN_LLM_TIMEOUT_SECONDS", "4") or "").strip()
	try:
		return max(0.5, min(30.0, float(raw)))
	except Exception:
		return 4.0


def _node_doc_explain_base_url() -> str:
	base = str(get_env("NODE_DOC_EXPLAIN_LLM_BASE_URL", "") or "").strip()
	if not base:
		base = str(get_env("OLLAMA_BASE_URL", "http://127.0.0.1:11434") or "").strip()
	return base.rstrip("/")


def _node_doc_explain_model() -> str:
	return str(get_env("NODE_DOC_EXPLAIN_LLM_MODEL", "glm-4.7-flash:latest") or "").strip() or "glm-4.7-flash:latest"


def _node_doc_explain_temperature() -> float:
	raw = str(get_env("NODE_DOC_EXPLAIN_LLM_TEMPERATURE", "0.2") or "").strip()
	try:
		return max(0.0, min(2.0, float(raw)))
	except Exception:
		return 0.2


def _node_doc_explain_top_p() -> float:
	raw = str(get_env("NODE_DOC_EXPLAIN_LLM_TOP_P", "1.0") or "").strip()
	try:
		return max(0.0, min(1.0, float(raw)))
	except Exception:
		return 1.0


def _node_doc_explain_max_tokens() -> int:
	raw = str(get_env("NODE_DOC_EXPLAIN_LLM_MAX_TOKENS", "512") or "").strip()
	try:
		return max(1, min(4096, int(raw)))
	except Exception:
		return 512


def _load_quick_fields_reference() -> str:
	candidates = [
		Path("docs/node_kind_quick_fields.md"),
		Path("../docs/node_kind_quick_fields.md"),
	]
	for path in candidates:
		try:
			if path.exists():
				return str(path.read_text(encoding="utf-8"))
		except Exception:
			continue
	return ""


def _extract_json_object_from_text(text: str) -> Dict[str, Any]:
	raw = str(text or "").strip()
	if not raw:
		return {}
	try:
		parsed = json.loads(raw)
		if isinstance(parsed, dict):
			return parsed
	except Exception:
		pass
	start = raw.find("{")
	end = raw.rfind("}")
	if start >= 0 and end > start:
		snippet = raw[start : end + 1]
		try:
			parsed = json.loads(snippet)
			if isinstance(parsed, dict):
				return parsed
		except Exception:
			return {}
	return {}


def _build_deterministic_node_doc_explanation(
	*,
	node_label: str,
	node_kind: str,
	node_subtype: str,
	settings: Dict[str, Any],
	runtime: Dict[str, Any],
	planes: Dict[str, Any],
) -> tuple[str, list[str], list[str]]:
	settings_explained = [
		f"{str(k).strip()}={str(v).strip()}"
		for k, v in settings.items()
		if str(k).strip() and str(v).strip()
	][:8]
	data_inputs = planes.get("data_inputs") if isinstance(planes.get("data_inputs"), list) else []
	data_outputs = planes.get("data_outputs") if isinstance(planes.get("data_outputs"), list) else []
	param_inputs = planes.get("param_inputs") if isinstance(planes.get("param_inputs"), list) else []
	control_inputs = planes.get("control_inputs") if isinstance(planes.get("control_inputs"), list) else []
	blocked = str(runtime.get("blocked_reason_code") or "").strip()
	pending = int(runtime.get("pending_input_count") or 0)
	inflight = int(runtime.get("inflight") or 0)
	ready = bool(runtime.get("ready_work") or False)

	subtype_phrase = f" ({node_subtype})" if node_subtype else ""
	prompt_excerpt = ""
	if node_kind in {"model", "llm"}:
		user_prompt = str(settings.get("user_prompt") or "").strip()
		if user_prompt:
			compact = " ".join(user_prompt.split())
			prompt_excerpt = compact[:140] + ("..." if len(compact) > 140 else "")
	summary = (
		f"{node_label} is a {node_kind}{subtype_phrase} node. "
		f"It reads {len(data_inputs)} data input handle(s), writes {len(data_outputs)} data output handle(s), "
		f"uses {len(param_inputs)} param handle(s), and consumes {len(control_inputs)} control handle(s)."
	)
	if prompt_excerpt:
		summary = f"{summary} User prompt intent: {prompt_excerpt}"
	context_notes = [
		f"pending_input_count={pending}",
		f"inflight={inflight}",
		f"ready_work={str(ready).lower()}",
	]
	if blocked:
		context_notes.append(f"blocked_reason={blocked}")
	if not settings_explained:
		context_notes.append("no_explicit_settings_detected")
	return summary, settings_explained, context_notes


async def _generate_node_doc_explanation_with_llm(
	*,
	context: Dict[str, Any],
	signature_key: str,
	provider: str,
	model: str,
	temperature: float | None = None,
	top_p: float | None = None,
	max_tokens: int | None = None,
) -> tuple[str | None, list[str], list[str], Dict[str, str], list[str]]:
	if not _node_doc_explain_llm_enabled():
		return None, [], [], {"provider": provider, "model": model}, ["llm_explain_disabled"]
	base_url = _node_doc_explain_base_url()
	quick_fields_ref = _load_quick_fields_reference()
	timeout_seconds = _node_doc_explain_timeout_seconds()
	model_name = model.strip() or _node_doc_explain_model()
	system_prompt = (
		"Generate concise node documentation from graph runtime context. "
		"Return strict JSON: {\"summary\": string, \"settings_explained\": string[], \"context_notes\": string[]}. "
		"Do not include markdown."
	)
	user_payload = {
		"context": context,
		"signature_key": signature_key,
		"quick_fields_reference_excerpt": quick_fields_ref[:4000],
	}
	body = {
		"model": model_name,
		"stream": False,
		"messages": [
			{"role": "system", "content": system_prompt},
			{"role": "user", "content": json.dumps(user_payload, ensure_ascii=True)},
		],
		"format": {
			"type": "object",
			"properties": {
				"summary": {"type": "string"},
				"settings_explained": {"type": "array", "items": {"type": "string"}},
				"context_notes": {"type": "array", "items": {"type": "string"}},
			},
			"required": ["summary"],
		},
	}
	options: Dict[str, Any] = {}
	if temperature is not None:
		options["temperature"] = float(temperature)
	if top_p is not None:
		options["top_p"] = float(top_p)
	if max_tokens is not None:
		options["num_predict"] = int(max_tokens)
	if options:
		body["options"] = options
	try:
		async with httpx.AsyncClient(timeout=timeout_seconds) as client:
			res = await client.post(f"{base_url}/api/chat", json=body)
			if int(res.status_code) < 200 or int(res.status_code) >= 300:
				return None, [], [], {"provider": provider, "model": model_name}, [f"llm_explain_http_{int(res.status_code)}"]
			payload = res.json() if hasattr(res, "json") else {}
			message = payload.get("message") if isinstance(payload, dict) else {}
			content = str(message.get("content") or "") if isinstance(message, dict) else ""
			obj = _extract_json_object_from_text(content)
			summary = str(obj.get("summary") or "").strip()
			if not summary:
				return None, [], [], {"provider": provider, "model": model_name}, ["llm_explain_missing_summary"]
			settings_explained = (
				[str(v).strip() for v in obj.get("settings_explained") if str(v).strip()]
				if isinstance(obj.get("settings_explained"), list)
				else []
			)[:8]
			context_notes = (
				[str(v).strip() for v in obj.get("context_notes") if str(v).strip()]
				if isinstance(obj.get("context_notes"), list)
				else []
			)[:8]
			return summary, settings_explained, context_notes, {"provider": provider, "model": model_name}, [
				f"llm_explain_base={base_url}"
			]
	except Exception as ex:
		return None, [], [], {"provider": provider, "model": model_name}, [f"llm_explain_error={type(ex).__name__}"]


async def _suggest_fields_with_llm(
	*,
	kind: str,
	subtype: str,
	candidate_fields: Dict[str, Any],
	generated_summary: str,
	corrected_summary: str,
) -> tuple[list[str], list[str]]:
	if not _node_doc_feedback_llm_enabled():
		return [], ["llm_suggester_disabled"]
	settings_keys = [str(k).strip() for k in candidate_fields.keys() if str(k).strip()]
	if not settings_keys:
		return [], ["llm_suggester_no_settings"]
	base_url = _node_doc_feedback_base_url()
	model = _node_doc_feedback_model()
	timeout_seconds = _node_doc_feedback_timeout_seconds()
	max_fields = _node_doc_feedback_max_fields()
	reference = _load_quick_fields_reference()
	system_prompt = (
		"You are selecting the most relevant setting keys for node-doc policy updates. "
		"Return strict JSON with: {\"suggested_fields\": [string], \"reason\": string}. "
		"Only choose keys from provided settings_keys."
	)
	user_payload = {
		"kind": kind,
		"subtype": subtype,
		"settings_keys": settings_keys,
		"settings_values": {k: str(v)[:240] for k, v in candidate_fields.items()},
		"generated_summary": generated_summary[:800],
		"corrected_summary": corrected_summary[:800],
		"quick_fields_reference_excerpt": reference[:4000],
		"max_fields": max_fields,
	}
	body = {
		"model": model,
		"stream": False,
		"messages": [
			{"role": "system", "content": system_prompt},
			{"role": "user", "content": json.dumps(user_payload, ensure_ascii=True)},
		],
		"format": {
			"type": "object",
			"properties": {
				"suggested_fields": {"type": "array", "items": {"type": "string"}},
				"reason": {"type": "string"},
			},
			"required": ["suggested_fields"],
		},
	}
	try:
		async with httpx.AsyncClient(timeout=timeout_seconds) as client:
			res = await client.post(f"{base_url}/api/chat", json=body)
			if int(res.status_code) < 200 or int(res.status_code) >= 300:
				return [], [f"llm_suggester_http_{int(res.status_code)}"]
			payload = res.json() if hasattr(res, "json") else {}
			message = payload.get("message") if isinstance(payload, dict) else {}
			content = ""
			if isinstance(message, dict):
				content = str(message.get("content") or "")
			obj = _extract_json_object_from_text(content)
			suggested = obj.get("suggested_fields") if isinstance(obj, dict) else []
			clean = []
			seen = set()
			for field in suggested if isinstance(suggested, list) else []:
				name = str(field or "").strip()
				if not name or name in seen:
					continue
				if name not in settings_keys:
					continue
				seen.add(name)
				clean.append(name)
				if len(clean) >= max_fields:
					break
			if not clean:
				return [], ["llm_suggester_no_candidates"]
			return clean, [f"llm_suggester_model={model}", f"llm_suggester_base={base_url}"]
	except Exception as ex:
		return [], [f"llm_suggester_error={type(ex).__name__}"]


def _node_doc_feedback_file_path() -> Path:
	path_raw = str(
		get_env("NODE_DOC_FEEDBACK_SUGGESTIONS_PATH", "docs/node_kind_quick_fields.suggestions.md") or ""
	).strip()
	if not path_raw:
		path_raw = "docs/node_kind_quick_fields.suggestions.md"
	return Path(path_raw)


def _node_doc_quick_fields_path() -> Path:
	path_raw = str(get_env("NODE_DOC_FEEDBACK_QUICK_FIELDS_PATH", "") or "").strip()
	if path_raw:
		return Path(path_raw)
	candidates = [
		Path("../docs/node_kind_quick_fields.md"),
		Path("docs/node_kind_quick_fields.md"),
	]
	for path in candidates:
		if path.exists():
			return path
	return candidates[0]


def _normalize_heading_token(value: str) -> str:
	return str(value or "").strip().lower().replace("_", " ")


def _find_section_bounds(lines: list[str], heading_prefix: str) -> list[tuple[int, int]]:
	idxs = [i for i, line in enumerate(lines) if line.startswith(heading_prefix)]
	bounds: list[tuple[int, int]] = []
	for pos, start in enumerate(idxs):
		end = idxs[pos + 1] if pos + 1 < len(idxs) else len(lines)
		bounds.append((start, end))
	return bounds


def _apply_suggested_fields_to_quick_fields(
	path: Path,
	kind: str,
	subtype: str,
	suggested_fields: list[str],
) -> bool:
	if not suggested_fields:
		return False
	if not path.exists():
		return False
	text = path.read_text(encoding="utf-8")
	lines = text.splitlines()
	if not lines:
		return False
	kind_token = _normalize_heading_token(kind)
	subtype_token = _normalize_heading_token(subtype)
	kind_bounds = _find_section_bounds(lines, "## ")
	kind_span: tuple[int, int] | None = None
	for start, end in kind_bounds:
		heading = _normalize_heading_token(lines[start][3:])
		if heading == kind_token:
			kind_span = (start, end)
			break
	if kind_span is None:
		return False
	kind_start, kind_end = kind_span
	sub_start = None
	sub_end = None
	for i in range(kind_start + 1, kind_end):
		if not lines[i].startswith("### "):
			continue
		label = _normalize_heading_token(lines[i][4:])
		base_label = label.split("(")[0].strip()
		if subtype_token and (subtype_token == base_label or subtype_token in label):
			sub_start = i
			break
	if sub_start is None:
		return False
	for j in range(sub_start + 1, kind_end):
		if lines[j].startswith("### "):
			sub_end = j
			break
	if sub_end is None:
		sub_end = kind_end
	formatted_fields = [f"`{str(v).strip()}`" for v in suggested_fields if str(v).strip()]
	if not formatted_fields:
		return False
	new_line = f"- suggested_fields: {', '.join(formatted_fields)}"
	replaced = False
	for i in range(sub_start + 1, sub_end):
		if lines[i].strip().lower().startswith("- suggested_fields:"):
			lines[i] = new_line
			replaced = True
			break
	if not replaced:
		insert_at = sub_start + 1
		for i in range(sub_start + 1, sub_end):
			if lines[i].strip().lower().startswith("- always examine:"):
				insert_at = i + 1
				break
		lines.insert(insert_at, new_line)
	updated = "\n".join(lines) + ("\n" if text.endswith("\n") else "")
	if updated == text:
		return False
	path.write_text(updated, encoding="utf-8")
	return True


def _append_feedback_suggestion(
	path: Path,
	entry: Dict[str, Any],
	suggested_fields: list[str],
	notes: list[str],
) -> None:
	path.parent.mkdir(parents=True, exist_ok=True)
	created_at = str(entry.get("created_at") or "")
	kind = str(entry.get("kind") or "")
	subtype = str(entry.get("subtype") or "")
	verdict = str(entry.get("verdict") or "")
	signature_key = str(entry.get("signature_key") or "")
	node_label = str(entry.get("node_label") or "")
	generated = str(entry.get("generated_summary") or "").strip()
	corrected = str(entry.get("corrected_summary") or "").strip()
	fields_text = ", ".join(suggested_fields) if suggested_fields else "(none)"
	notes_text = ", ".join(str(v).strip() for v in notes if str(v).strip()) if notes else "(none)"
	record_json = json.dumps(entry, ensure_ascii=True, separators=(",", ":"))
	lines = [
		"",
		f"## [{created_at}] {kind}:{subtype or '*'}",
		f"- node: `{node_label}`",
		f"- verdict: `{verdict}`",
		f"- signature: `{signature_key}`",
		f"- suggested_fields: {fields_text}",
		f"- notes: {notes_text}",
		"- generated_summary:",
		f"  - {generated}",
		"- corrected_summary:",
		f"  - {corrected if corrected else '(not provided)'}",
		"- raw:",
		f"```json\n{record_json}\n```",
	]
	if not path.exists():
		path.write_text("# Node Doc LLM Feedback Suggestions\n", encoding="utf-8")
	with path.open("a", encoding="utf-8") as handle:
		handle.write("\n".join(lines))


@router.post("/versions/register")
async def register_model_version(req: ModelRegisterVersionRequest, request: Request):
	store = _get_store(request)
	summary = await _resolve_run_experiment(request, req.runId)
	metrics = req.metrics if isinstance(req.metrics, dict) else {}
	params = req.params if isinstance(req.params, dict) else {}
	environment = req.environment if isinstance(req.environment, dict) else {}
	provenance = req.provenance if isinstance(req.provenance, dict) else {}
	graph_id = None
	if isinstance(summary, dict):
		graph_id = str(summary.get("graphId") or "").strip() or None
		summary_metrics = summary.get("metrics") if isinstance(summary.get("metrics"), dict) else {}
		summary_params = summary.get("params") if isinstance(summary.get("params"), dict) else {}
		summary_env = summary.get("environment") if isinstance(summary.get("environment"), dict) else {}
		summary_artifacts = summary.get("artifacts") if isinstance(summary.get("artifacts"), list) else []
		if not metrics and summary_metrics:
			metrics = summary_metrics
		if not params and summary_params:
			params = summary_params
		if not environment and summary_env:
			environment = summary_env
		provenance = {
			"source": "run_experiment",
			"runId": str(summary.get("runId") or req.runId or ""),
			"graphId": graph_id,
			"artifactIds": [
				str(a.get("artifactId"))
				for a in summary_artifacts
				if isinstance(a, dict) and str(a.get("artifactId") or "").strip()
			],
			**provenance,
		}

	try:
		version = store.register_version(
			model_id=req.modelId,
			model_name=req.modelName,
			version_id=req.versionId,
			stage=req.stage,
			run_id=req.runId,
			graph_id=graph_id,
			artifact_id=req.artifactId,
			metrics=metrics,
			params=params,
			environment=environment,
			provenance=provenance,
		)
	except ValueError as ex:
		raise HTTPException(status_code=400, detail=str(ex))

	return {
		"schemaVersion": 1,
		"modelId": version.model_id,
		"versionId": version.version_id,
		"versionNumber": version.version_number,
		"stage": version.stage,
		"createdAt": version.created_at,
		"runId": version.run_id,
		"graphId": version.graph_id,
		"artifactId": version.artifact_id,
		"provenance": version.provenance,
	}


@router.get("")
async def list_models(
	request: Request,
	limit: int = Query(default=50, ge=1, le=500),
	offset: int = Query(default=0, ge=0),
):
	store = _get_store(request)
	rows = store.list_models(limit=limit, offset=offset)
	return {"schemaVersion": 1, "models": rows}


@router.get("/{model_id}")
async def get_model(model_id: str, request: Request):
	store = _get_store(request)
	row = store.get_model(model_id)
	if not isinstance(row, dict):
		raise HTTPException(status_code=404, detail="model not found")
	return {"schemaVersion": 1, "model": row}


@router.get("/{model_id}/versions")
async def list_model_versions(
	model_id: str,
	request: Request,
	limit: int = Query(default=50, ge=1, le=500),
	offset: int = Query(default=0, ge=0),
):
	store = _get_store(request)
	rows = store.list_versions(model_id, limit=limit, offset=offset)
	return {"schemaVersion": 1, "modelId": model_id, "versions": rows}


@router.get("/{model_id}/versions/{version_id}")
async def get_model_version(model_id: str, version_id: str, request: Request):
	store = _get_store(request)
	row = store.get_version(model_id, version_id)
	if not isinstance(row, dict):
		raise HTTPException(status_code=404, detail="version not found")
	return {"schemaVersion": 1, "modelId": model_id, "version": row}


@router.post("/{model_id}/versions/{version_id}/promote")
async def promote_model_version(
	model_id: str,
	version_id: str,
	req: ModelPromoteRequest,
	request: Request,
	x_model_admin: Optional[str] = Header(default=None),
):
	_require_admin_for_promotion(x_model_admin)
	store = _get_store(request)
	try:
		result = store.promote_version(
			model_id=model_id,
			version_id=version_id,
			to_stage=req.toStage,
			force=bool(req.force),
			promoted_by=req.promotedBy,
		)
	except LookupError:
		raise HTTPException(status_code=404, detail="version not found")
	except RuntimeError as ex:
		message = str(ex)
		if message.startswith("stage_conflict:"):
			parts = message.split(":")
			stage = parts[1] if len(parts) > 1 else "unknown"
			conflict = parts[2] if len(parts) > 2 else ""
			raise HTTPException(
				status_code=409,
				detail={
					"code": "STAGE_CONFLICT",
					"stage": stage,
					"conflictVersionId": conflict or None,
					"message": f"stage '{stage}' already has a version",
				},
			)
		raise HTTPException(status_code=409, detail=message)
	except ValueError as ex:
		raise HTTPException(status_code=400, detail=str(ex))
	return {"schemaVersion": 1, "modelId": model_id, "versionId": version_id, **result}


@router.post("/node-doc-explain")
async def explain_node_doc(req: NodeDocExplainRequest):
	context = req.context if isinstance(req.context, dict) else {}
	node_label = str(context.get("node_label") or context.get("nodeLabel") or "Node").strip() or "Node"
	node_kind = str(context.get("node_kind") or context.get("nodeKind") or "node").strip().lower() or "node"
	node_subtype = str(context.get("node_subtype") or context.get("nodeSubtype") or "").strip().lower()
	settings = context.get("settings") if isinstance(context.get("settings"), dict) else {}
	candidate_fields = _build_feedback_candidate_fields(context, settings)
	runtime = context.get("runtime") if isinstance(context.get("runtime"), dict) else {}
	planes = context.get("planes") if isinstance(context.get("planes"), dict) else {}
	deterministic_summary, deterministic_settings, deterministic_notes = _build_deterministic_node_doc_explanation(
		node_label=node_label,
		node_kind=node_kind,
		node_subtype=node_subtype,
		settings=settings,
		runtime=runtime,
		planes=planes,
	)
	signature_key = str(req.signatureKey or "").strip() or "missing-signature"
	provider = str(req.provider or "").strip() or "ollama"
	model = str(req.model or "").strip() or _node_doc_explain_model()
	temperature = float(req.temperature) if isinstance(req.temperature, (int, float)) else _node_doc_explain_temperature()
	top_p = float(req.topP) if isinstance(req.topP, (int, float)) else _node_doc_explain_top_p()
	max_tokens = int(req.maxTokens) if isinstance(req.maxTokens, int) else _node_doc_explain_max_tokens()
	llm_summary, llm_settings, llm_notes, llm_provider_meta, llm_diag_notes = await _generate_node_doc_explanation_with_llm(
		context=context,
		signature_key=signature_key,
		provider=provider,
		model=model,
		temperature=temperature,
		top_p=top_p,
		max_tokens=max_tokens,
	)
	summary = llm_summary or deterministic_summary
	settings_explained = llm_settings or deterministic_settings
	context_notes = llm_notes or deterministic_notes
	if llm_summary:
		context_notes = (context_notes + ["llm_explain_used"])[:8]
		provider_meta = llm_provider_meta
	else:
		context_notes = (context_notes + llm_diag_notes + ["llm_explain_fallback=deterministic"])[:8]
		provider_meta = {"provider": "local", "model": "deterministic-node-docs-v1"}
	response = NodeDocExplainResponse(
		summary=summary,
		settings_explained=settings_explained,
		context_notes=context_notes,
		generated_at=datetime.now(timezone.utc).isoformat(),
		signature_key=signature_key,
		provider_meta=provider_meta,
	)
	return response.model_dump()


@router.post("/node-doc-feedback")
async def node_doc_feedback(req: NodeDocFeedbackRequest):
	context = req.context if isinstance(req.context, dict) else {}
	kind = str(context.get("node_kind") or context.get("nodeKind") or "node").strip().lower() or "node"
	subtype = str(context.get("node_subtype") or context.get("nodeSubtype") or "").strip().lower()
	node_label = str(context.get("node_label") or context.get("nodeLabel") or "Node").strip() or "Node"
	settings = context.get("settings") if isinstance(context.get("settings"), dict) else {}
	candidate_fields = _build_feedback_candidate_fields(context, settings)
	signature_key = str(req.signatureKey or "").strip() or "missing-signature"
	verdict = str(req.verdict or "").strip().lower()
	generated_summary = str(req.generatedSummary or "").strip()
	corrected_summary = str(req.correctedSummary or "").strip()
	entry_id = f"{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"
	entry = {
		"entry_id": entry_id,
		"created_at": datetime.now(timezone.utc).isoformat(),
		"kind": kind,
		"subtype": subtype,
		"node_label": node_label,
		"signature_key": signature_key,
		"verdict": verdict,
		"generated_summary": generated_summary,
		"corrected_summary": corrected_summary,
		"settings": settings,
		"candidate_fields": candidate_fields,
	}
	suggested_fields: list[str] = []
	notes = []
	if verdict == "good":
		notes.append("good_feedback_recorded")
	if verdict == "bad" and not corrected_summary:
		notes.append("missing_corrected_summary")
	if verdict == "bad" and corrected_summary:
		llm_fields, llm_notes = await _suggest_fields_with_llm(
			kind=kind,
			subtype=subtype,
			candidate_fields=candidate_fields,
			generated_summary=generated_summary,
			corrected_summary=corrected_summary,
		)
		notes.extend(llm_notes)
		suggested_fields = llm_fields
		if not suggested_fields:
			suggested_fields = _suggest_fields_from_feedback(
				candidate_fields,
				generated_summary,
				corrected_summary,
				limit=_node_doc_feedback_max_fields(),
			)
			if suggested_fields:
				notes.append("fallback_heuristic_used")
	if verdict == "bad" and corrected_summary and not suggested_fields:
		notes.append("no_field_candidates")

	suggestion_file = _node_doc_feedback_file_path()
	_append_feedback_suggestion(suggestion_file, entry, suggested_fields, notes)
	if verdict == "bad" and corrected_summary and suggested_fields:
		try:
			quick_fields_path = _node_doc_quick_fields_path()
			updated = _apply_suggested_fields_to_quick_fields(
				quick_fields_path,
				kind=kind,
				subtype=subtype,
				suggested_fields=suggested_fields,
			)
			if updated:
				notes.append(f"quick_fields_updated={quick_fields_path.as_posix()}")
			else:
				notes.append("quick_fields_no_change")
		except Exception as ex:
			notes.append(f"quick_fields_update_error={type(ex).__name__}")
	response = NodeDocFeedbackResponse(
		ok=True,
		stored=True,
		entry_id=entry_id,
		kind=kind,
		subtype=subtype,
		suggestion_file=str(suggestion_file.as_posix()),
		suggested_fields=suggested_fields,
		notes=notes,
	)
	return response.model_dump()
