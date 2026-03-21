from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol

from app.runner.schemas import LLMParams


@dataclass(frozen=True)
class AdapterPreparedRequest:
	provider: str
	base_url: str
	url: str
	output_mode: str
	headers: Dict[str, str]
	payload: Dict[str, Any]


@dataclass(frozen=True)
class AdapterParsedResponse:
	data: str
	mime_type: str
	file_type: str
	file_suffix: str


class ModelProviderAdapter(Protocol):
	def prepare_request(
		self,
		params: LLMParams,
		upstream_text: str,
		input_items: Optional[List[str]] = None,
		input_media: Optional[List[Dict[str, Any]]] = None,
	) -> AdapterPreparedRequest: ...

	def parse_response(self, output_mode: str, raw_data: str) -> AdapterParsedResponse: ...

	def normalize_error(self, error: Exception) -> str: ...


def resolve_output_mode(params: LLMParams) -> str:
	if isinstance(params.embedding_contract, dict) and params.embedding_contract:
		return "embeddings"
	if isinstance(params.output_schema, dict):
		return "json"
	return "text"


def build_messages(params: LLMParams, upstream_text: str) -> List[Dict[str, str]]:
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


def _canonicalize_input_media(input_media: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
	if not input_media:
		return []
	out: List[Dict[str, Any]] = []
	for i, media in enumerate(input_media):
		if not isinstance(media, dict):
			raise ValueError(f"input_media[{i}] must be an object")
		media_type = str(media.get("type") or "").strip().lower()
		if media_type not in {"image", "audio"}:
			raise ValueError(f"input_media[{i}].type must be one of: image, audio")
		data_url = str(media.get("dataUrl") or "").strip()
		if not data_url:
			raise ValueError(f"input_media[{i}].dataUrl is required")
		item: Dict[str, Any] = {
			"type": media_type,
			"dataUrl": data_url,
		}
		mime = str(media.get("mimeType") or "").strip().lower()
		if mime:
			item["mimeType"] = mime
		out.append(item)
	return out


class OpenAICompatAdapter:
	provider = "openai_compat"

	def prepare_request(
		self,
		params: LLMParams,
		upstream_text: str,
		input_items: Optional[List[str]] = None,
		input_media: Optional[List[Dict[str, Any]]] = None,
	) -> AdapterPreparedRequest:
		base_url = (params.base_url or "").rstrip("/")
		if not base_url:
			raise ValueError("OpenAI-compatible adapter requires base_url")
		output_mode = resolve_output_mode(params)
		headers: Dict[str, str] = {"Content-Type": "application/json"}
		payload: Dict[str, Any]
		url = f"{base_url}/v1/chat/completions"
		if output_mode == "embeddings":
			url = f"{base_url}/v1/embeddings"
			payload = {
				"model": params.model,
				"input": input_items if (input_items and len(input_items) > 1) else (input_items[0] if input_items else upstream_text),
			}
		else:
			messages: List[Dict[str, Any]] = build_messages(params, upstream_text)
			media_inputs = _canonicalize_input_media(input_media)
			if media_inputs:
				user_content = str(messages[-1].get("content") or "")
				media_parts: List[Dict[str, Any]] = []
				for media in media_inputs:
					media_type = str(media.get("type") or "").strip().lower()
					data_url = str(media.get("dataUrl") or "").strip()
					if not data_url:
						continue
					if media_type == "audio":
						# OpenAI-compatible multimodal payload for audio transcript/extract paths.
						audio_data = data_url
						audio_format = "wav"
						if data_url.startswith("data:"):
							header, _, b64 = data_url.partition(",")
							audio_data = b64.strip()
							mime = header[5:].split(";")[0].strip().lower() if ";" in header else ""
							if mime:
								audio_format = mime.split("/")[-1]
						media_parts.append(
							{
								"type": "input_audio",
								"input_audio": {
									"data": audio_data,
									"format": audio_format,
								},
							}
						)
					else:
						media_parts.append({"type": "image_url", "image_url": {"url": data_url}})
				if media_parts:
					messages[-1] = {
						"role": "user",
						"content": [{"type": "text", "text": user_content}, *media_parts],
					}
			payload = {
				"model": params.model,
				"messages": messages,
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
			if output_mode == "json":
				payload["response_format"] = {"type": "json_object"}
		return AdapterPreparedRequest(
			provider=self.provider,
			base_url=base_url,
			url=url,
			output_mode=output_mode,
			headers=headers,
			payload=payload,
		)

	def parse_response(self, output_mode: str, raw_data: str) -> AdapterParsedResponse:
		mode = str(output_mode or "text").strip().lower()
		if mode == "json":
			obj = json.loads(raw_data) if raw_data else None
			data = json.dumps(obj, separators=(",", ":"), sort_keys=True)
			return AdapterParsedResponse(data=data, mime_type="application/json", file_type="json", file_suffix="json")
		if mode == "embeddings":
			return AdapterParsedResponse(
				data=raw_data,
				mime_type="application/json",
				file_type="json",
				file_suffix="embeddings.json",
			)
		return AdapterParsedResponse(
			data=raw_data,
			mime_type="text/plain; charset=utf-8",
			file_type="txt",
			file_suffix="txt",
		)

	def normalize_error(self, error: Exception) -> str:
		return f"openai_compat request failed: {str(error)}"


class OllamaAdapter:
	provider = "ollama"

	def prepare_request(
		self,
		params: LLMParams,
		upstream_text: str,
		input_items: Optional[List[str]] = None,
		input_media: Optional[List[Dict[str, Any]]] = None,
	) -> AdapterPreparedRequest:
		base_url = (params.base_url or "").rstrip("/")
		if not base_url:
			raise ValueError("Ollama adapter requires base_url")
		output_mode = resolve_output_mode(params)
		if output_mode == "embeddings":
			raise ValueError("provider 'ollama' does not support output_mode='embeddings'")
		media_inputs = _canonicalize_input_media(input_media)
		if media_inputs and any(str(m.get("type") or "").strip().lower() == "audio" for m in media_inputs):
			raise ValueError("provider 'ollama' does not support audio input")
		thinking_mode = "none"
		if params.thinking and params.thinking.enabled:
			thinking_mode = params.thinking.mode
		payload: Dict[str, Any] = {
			"model": params.model,
			"messages": build_messages(params, upstream_text),
			"stream": True,
			"think": thinking_mode in {"hidden", "visible"},
			"options": {"temperature": params.temperature, "num_predict": params.max_tokens},
		}
		if params.top_p is not None:
			payload["options"]["top_p"] = params.top_p
		if params.seed is not None:
			payload["options"]["seed"] = params.seed
		if params.stop_sequences:
			payload["options"]["stop"] = params.stop_sequences
		if params.repeat_penalty is not None:
			payload["options"]["repeat_penalty"] = params.repeat_penalty
		if output_mode == "json":
			payload["format"] = "json"
		return AdapterPreparedRequest(
			provider=self.provider,
			base_url=base_url,
			url=f"{base_url}/api/chat",
			output_mode=output_mode,
			headers={},
			payload=payload,
		)

	def parse_response(self, output_mode: str, raw_data: str) -> AdapterParsedResponse:
		mode = str(output_mode or "text").strip().lower()
		if mode == "json":
			obj = json.loads(raw_data) if raw_data else None
			data = json.dumps(obj, separators=(",", ":"), sort_keys=True)
			return AdapterParsedResponse(data=data, mime_type="application/json", file_type="json", file_suffix="json")
		return AdapterParsedResponse(
			data=raw_data,
			mime_type="text/plain; charset=utf-8",
			file_type="txt",
			file_suffix="txt",
		)

	def normalize_error(self, error: Exception) -> str:
		msg = str(error)
		if "output_mode" in msg:
			return msg
		return f"ollama request failed: {msg}"


def get_model_adapter(llm_kind: str) -> ModelProviderAdapter:
	key = str(llm_kind or "").strip().lower()
	if key == "ollama":
		return OllamaAdapter()
	if key == "openai_compat":
		return OpenAICompatAdapter()
	raise ValueError(f"Unsupported llmKind: {llm_kind}")
