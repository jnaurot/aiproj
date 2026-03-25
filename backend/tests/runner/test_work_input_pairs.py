from app.runner.run import _is_non_work_input_handle, _work_input_pairs


def test_is_non_work_input_handle_recognizes_param_and_control_prefixes() -> None:
	assert _is_non_work_input_handle("param_context") is True
	assert _is_non_work_input_handle("control_in") is True
	assert _is_non_work_input_handle("ctl_stop") is True
	assert _is_non_work_input_handle("in") is False
	assert _is_non_work_input_handle("work_in") is False


def test_work_input_pairs_filters_non_work_handles_for_model_serialization() -> None:
	pairs = _work_input_pairs(
		[
			("in", "aid-work"),
			("param_context", "aid-param"),
			("control_in", "aid-control"),
		]
	)
	assert pairs == [("in", "aid-work")]

