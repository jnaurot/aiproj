import json

import pytest

from app.executors.model_adapters import OpenAICompatAdapter, OllamaAdapter
from app.runner.schemas import LLMParams


def _params(output_mode: str = "text") -> LLMParams:
	raw = {
		"base_url": "http://localhost:11434",
		"model": "demo-model",
		"user_prompt": "hello {input}",
		"output_mode": output_mode,
	}
	if output_mode == "json":
		raw["output_schema"] = {"type": "object", "properties": {"ok": {"type": "boolean"}}}
	elif output_mode == "embeddings":
		raw["embedding_contract"] = {"dims": 8, "dtype": "float32", "layout": "1d"}
	return LLMParams.model_validate(raw)


def test_openai_adapter_conforms_contract():
	adapter = OpenAICompatAdapter()
	prepared = adapter.prepare_request(_params("json"), upstream_text="abc")
	assert prepared.provider == "openai_compat"
	assert prepared.output_mode == "json"
	assert prepared.url.endswith("/v1/chat/completions")
	assert "messages" in prepared.payload
	parsed = adapter.parse_response("json", json.dumps({"ok": True}))
	assert parsed.mime_type == "application/json"
	assert parsed.file_type == "json"
	assert parsed.file_suffix == "json"
	assert json.loads(parsed.data) == {"ok": True}
	assert "openai_compat" in adapter.normalize_error(ValueError("boom"))
	embed_prepared = adapter.prepare_request(_params("embeddings"), upstream_text="abc", input_items=["abc"])
	assert embed_prepared.url.endswith("/v1/embeddings")
	assert embed_prepared.output_mode == "embeddings"
	audio_prepared = adapter.prepare_request(
		_params("text"),
		upstream_text="abc",
		input_media=[{"type": "audio", "dataUrl": "data:audio/wav;base64,QUJD"}],
	)
	audio_messages = audio_prepared.payload.get("messages") or []
	assert isinstance(audio_messages, list) and len(audio_messages) > 0
	last_content = audio_messages[-1].get("content")
	assert isinstance(last_content, list)
	assert any(isinstance(part, dict) and part.get("type") == "input_audio" for part in last_content)
	first_payload = json.dumps(audio_prepared.payload, sort_keys=True)
	audio_prepared_2 = adapter.prepare_request(
		_params("text"),
		upstream_text="abc",
		input_media=[{"mimeType": "audio/wav", "dataUrl": "data:audio/wav;base64,QUJD", "type": "audio", "extra": "drop-me"}],
	)
	second_payload = json.dumps(audio_prepared_2.payload, sort_keys=True)
	assert first_payload == second_payload


def test_ollama_adapter_conforms_contract():
	adapter = OllamaAdapter()
	prepared = adapter.prepare_request(_params("text"), upstream_text="abc")
	assert prepared.provider == "ollama"
	assert prepared.output_mode == "text"
	assert prepared.url.endswith("/api/chat")
	assert prepared.payload.get("stream") is True
	parsed = adapter.parse_response("text", "hello")
	assert parsed.mime_type.startswith("text/plain")
	assert parsed.file_type == "txt"
	assert parsed.file_suffix == "txt"
	assert parsed.data == "hello"
	assert "ollama" in adapter.normalize_error(ValueError("boom"))


def test_ollama_adapter_rejects_embeddings_mode():
	adapter = OllamaAdapter()
	with pytest.raises(ValueError, match="does not support output_mode='embeddings'"):
		adapter.prepare_request(_params("embeddings"), upstream_text="abc", input_items=["abc"])


def test_ollama_adapter_rejects_audio_input():
	adapter = OllamaAdapter()
	with pytest.raises(ValueError, match="does not support audio input"):
		adapter.prepare_request(
			_params("text"),
			upstream_text="abc",
			input_media=[{"type": "audio", "dataUrl": "data:audio/wav;base64,QUJD"}],
		)


def test_openai_adapter_rejects_invalid_media_type():
	adapter = OpenAICompatAdapter()
	with pytest.raises(ValueError, match="input_media\\[0\\]\\.type must be one of: image, audio"):
		adapter.prepare_request(
			_params("text"),
			upstream_text="abc",
			input_media=[{"type": "video", "dataUrl": "data:video/mp4;base64,QUJD"}],
		)
