import json

import pytest

from app.executors.model_adapters import OpenAICompatAdapter, OllamaAdapter
from app.runner.schemas import LLMParams


def _params(output_mode: str = "text") -> LLMParams:
	raw = {
		"base_url": "http://localhost:11434",
		"model": "demo-model",
		"user_prompt": "hello {input}",
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
