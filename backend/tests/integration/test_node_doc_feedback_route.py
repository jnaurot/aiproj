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
			"planes": {
				"data_inputs": ["summarize"],
				"data_outputs": [],
				"data_input_sources": ["summarize<=Component.summarize [component]"],
				"param_inputs": [],
				"control_inputs": [],
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
	assert "- notes:" in text
	assert "candidate_fields" in text


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


def test_node_doc_feedback_fallback_can_select_semantic_fields(tmp_path):
	suggestions = tmp_path / "node_doc_feedback_semantic.md"
	os.environ["NODE_DOC_FEEDBACK_SUGGESTIONS_PATH"] = str(suggestions)
	os.environ["NODE_DOC_FEEDBACK_LLM_ENABLED"] = "0"
	payload = {
		"context": {
			"node_id": "n_3",
			"node_label": "Model_Spanish_Summary",
			"node_kind": "model",
			"node_subtype": "ollama",
			"settings": {"user_prompt": "translate to spanish", "temperature": "0.7"},
			"planes": {
				"data_inputs": ["summarize"],
				"data_outputs": [],
				"data_input_sources": ["in<=Component.summarize [component]"],
				"param_inputs": [],
				"control_inputs": [],
			},
		},
		"signatureKey": "sig-feedback-semantic",
		"generatedSummary": "Model_Spanish_Summary is a model node.",
		"verdict": "bad",
		"correctedSummary": "Model_Spanish reads summarize from Component and translate to Spanish.",
	}
	with TestClient(app) as client:
		res = client.post("/models/node-doc-feedback", json=payload)
		assert res.status_code == 200
		body = res.json()
		assert body.get("ok") is True
		fields = body.get("suggested_fields") or []
		assert any(str(v) in {"data_input_sources", "data_inputs", "user_prompt"} for v in fields)
		notes = body.get("notes") or []
		assert "llm_suggester_disabled" in notes


def test_node_doc_feedback_auto_updates_quick_fields_file(tmp_path, monkeypatch):
	suggestions = tmp_path / "node_doc_feedback_autoupdate.md"
	quick_fields = tmp_path / "node_kind_quick_fields.md"
	quick_fields.write_text(
		"\n".join(
			[
				"# Node Kind Quick Fields",
				"",
				"## Model",
				"### Ollama (Translation)",
				"- Always examine: `settings.user_prompt`",
				"- suggested_fields: `old_field`",
				"- Description pattern: \"Runs model\"",
				"",
			]
		),
		encoding="utf-8",
	)
	os.environ["NODE_DOC_FEEDBACK_SUGGESTIONS_PATH"] = str(suggestions)
	os.environ["NODE_DOC_FEEDBACK_QUICK_FIELDS_PATH"] = str(quick_fields)
	os.environ["NODE_DOC_FEEDBACK_LLM_ENABLED"] = "1"

	async def _fake_llm(**kwargs):
		return ["node_label", "data_input_sources", "user_prompt", "data_inputs"], ["llm_suggester_mock"]

	monkeypatch.setattr(models_route, "_suggest_fields_with_llm", _fake_llm)
	payload = {
		"context": {
			"node_id": "n_4",
			"node_label": "Model_Spanish_Summary",
			"node_kind": "model",
			"node_subtype": "ollama",
			"settings": {"user_prompt": "translate to spanish", "temperature": "0.7"},
			"planes": {
				"data_inputs": ["in"],
				"data_outputs": [],
				"data_input_sources": ["in<=Component.summary [component]"],
				"param_inputs": [],
				"control_inputs": [],
			},
		},
		"signatureKey": "sig-feedback-autoupdate",
		"generatedSummary": "Runs Ollama chat inference with optional JSON strict output.",
		"verdict": "bad",
		"correctedSummary": "Model_Spanish_Summary reads summarize from Component and translates to Spanish",
	}
	with TestClient(app) as client:
		res = client.post("/models/node-doc-feedback", json=payload)
		assert res.status_code == 200
		body = res.json()
		assert body.get("ok") is True
		notes = body.get("notes") or []
		assert any(str(v).startswith("quick_fields_updated=") for v in notes)

	updated = quick_fields.read_text(encoding="utf-8")
	assert "- suggested_fields: `old_field`" not in updated
	assert (
		"- suggested_fields: `node_label`, `data_input_sources`, `user_prompt`, `data_inputs`"
		in updated
	)
