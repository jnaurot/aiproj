from __future__ import annotations

import os

from fastapi.testclient import TestClient

from app.main import app
from app.routes import models as models_route


def test_node_doc_feedback_writes_suggestions_file(tmp_path):
	suggestions = tmp_path / "node_doc_feedback.md"
	os.environ["NODE_DOC_FEEDBACK_SUGGESTIONS_PATH"] = str(suggestions)
	os.environ["NODE_DOC_FEEDBACK_LLM_ENABLED"] = "0"
	payload = {
		"context": {
			"node_id": "n_1",
			"node_label": "Model_ScoreJob",
			"node_kind": "model",
			"node_subtype": "ollama",
			"settings": {
				"user_prompt": "Score fit based on location and salary",
				"temperature": "0",
				"model": "glm-4.7-flash:latest",
			},
		},
		"signatureKey": "sig-feedback-1",
		"generatedSummary": "This node runs a model.",
		"verdict": "bad",
		"correctedSummary": "Scores role fit from user_prompt and salary/location requirements.",
	}
	with TestClient(app) as client:
		res = client.post("/models/node-doc-feedback", json=payload)
		assert res.status_code == 200
		body = res.json()
		assert body.get("ok") is True
		assert body.get("stored") is True
		assert str(body.get("suggestion_file") or "").endswith("node_doc_feedback.md")
		assert isinstance(body.get("suggested_fields"), list)
	assert suggestions.exists()
	text = suggestions.read_text(encoding="utf-8")
	assert "Node Doc LLM Feedback Suggestions" in text
	assert "Model_ScoreJob" in text
	assert "sig-feedback-1" in text
	assert "corrected_summary" in text


def test_node_doc_feedback_prefers_llm_candidates(tmp_path, monkeypatch):
	suggestions = tmp_path / "node_doc_feedback_llm.md"
	os.environ["NODE_DOC_FEEDBACK_SUGGESTIONS_PATH"] = str(suggestions)
	os.environ["NODE_DOC_FEEDBACK_LLM_ENABLED"] = "1"

	async def _fake_llm(**kwargs):
		return ["user_prompt", "temperature"], ["llm_suggester_mock"]

	monkeypatch.setattr(models_route, "_suggest_fields_with_llm", _fake_llm)
	payload = {
		"context": {
			"node_id": "n_2",
			"node_label": "ResumeBuilder",
			"node_kind": "model",
			"node_subtype": "ollama",
			"settings": {"user_prompt": "Build resume", "temperature": "0", "model": "glm-4.7"},
		},
		"signatureKey": "sig-feedback-llm",
		"generatedSummary": "Builds a resume.",
		"verdict": "bad",
		"correctedSummary": "Focus on user_prompt and deterministic settings.",
	}
	with TestClient(app) as client:
		res = client.post("/models/node-doc-feedback", json=payload)
		assert res.status_code == 200
		body = res.json()
		assert body.get("ok") is True
		assert body.get("suggested_fields") == ["user_prompt", "temperature"]
		assert "llm_suggester_mock" in (body.get("notes") or [])
