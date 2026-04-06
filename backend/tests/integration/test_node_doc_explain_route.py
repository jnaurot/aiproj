from __future__ import annotations

import os

from fastapi.testclient import TestClient

from app.main import app
from app.routes import models as models_route


def _payload():
	return {
		"context": {
			"node_id": "n_1",
			"node_label": "Model_Spanish_Summary",
			"node_kind": "model",
			"node_subtype": "ollama",
			"settings": {"user_prompt": "translate to spanish", "model": "glm-4.7-flash:latest"},
			"runtime": {"pending_input_count": 0, "inflight": 0, "ready_work": True},
			"planes": {"data_inputs": ["summarize"], "data_outputs": [], "param_inputs": [], "control_inputs": []},
		},
		"signatureKey": "sig-explain-1",
		"provider": "ollama",
		"model": "glm-4.7-flash:latest",
	}


def test_node_doc_explain_fallback_deterministic_when_llm_disabled():
	os.environ["NODE_DOC_EXPLAIN_LLM_ENABLED"] = "0"
	with TestClient(app) as client:
		res = client.post("/models/node-doc-explain", json=_payload())
		assert res.status_code == 200
		body = res.json()
		assert "Model_Spanish_Summary is a model" in str(body.get("summary") or "")
		assert body.get("provider_meta", {}).get("provider") == "local"
		assert body.get("provider_meta", {}).get("model") == "deterministic-node-docs-v1"
		notes = body.get("context_notes") or []
		assert any("llm_explain_fallback=deterministic" in str(n) for n in notes)


def test_node_doc_explain_prefers_llm(monkeypatch):
	os.environ["NODE_DOC_EXPLAIN_LLM_ENABLED"] = "1"

	async def _fake_llm(**kwargs):
		return (
			"Model_Spanish reads summarize from Component and translate to Spanish.",
			["user_prompt=translate to spanish"],
			["llm_note=mock"],
			{"provider": "ollama", "model": "glm-4.7-flash:latest"},
			["llm_mock_called"],
		)

	monkeypatch.setattr(models_route, "_generate_node_doc_explanation_with_llm", _fake_llm)
	with TestClient(app) as client:
		res = client.post("/models/node-doc-explain", json=_payload())
		assert res.status_code == 200
		body = res.json()
		assert body.get("summary") == "Model_Spanish reads summarize from Component and translate to Spanish."
		assert body.get("provider_meta", {}).get("provider") == "ollama"
		assert body.get("provider_meta", {}).get("model") == "glm-4.7-flash:latest"
		notes = body.get("context_notes") or []
		assert "llm_explain_used" in notes

