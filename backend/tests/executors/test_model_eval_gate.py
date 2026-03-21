from app.executors.model_eval_gate import evaluate_model_output_gate
from app.runner.schemas import LLMParams


def _params(eval_gate=None) -> LLMParams:
	raw = {
		"base_url": "https://x.local",
		"model": "demo",
		"user_prompt": "hello",
	}
	if eval_gate is not None:
		raw["eval_gate"] = eval_gate
	return LLMParams.model_validate(raw)


def test_eval_gate_disabled_passes():
	ok, reason = evaluate_model_output_gate(_params(), "text", "hello")
	assert ok is True
	assert "disabled" in reason


def test_eval_gate_fails_on_short_output():
	ok, reason = evaluate_model_output_gate(
		_params({"enabled": True, "min_output_chars": 5}),
		"text",
		"hi",
	)
	assert ok is False
	assert "min_output_chars" in reason


def test_eval_gate_json_required_keys():
	ok, reason = evaluate_model_output_gate(
		_params({"enabled": True, "required_json_keys": ["ok", "score"]}),
		"json",
		'{"ok":true}',
	)
	assert ok is False
	assert "missing required_json_keys" in reason
