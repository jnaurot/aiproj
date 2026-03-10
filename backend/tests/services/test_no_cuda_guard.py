from __future__ import annotations

from app.services.no_cuda_guard import _is_effective_installed_requirement, find_cuda_violations_in_specs


def test_find_cuda_violations_in_specs_detects_nvidia_and_cuda_markers():
	violations = find_cuda_violations_in_specs(
		[
			"numpy>=1.26",
			"nvidia-cublas-cu12>=12.4",
			"torch==2.4.0+cu124",
		],
		source="test",
	)
	items = [v.item for v in violations]
	assert "nvidia-cublas-cu12>=12.4" in items
	assert "torch==2.4.0+cu124" in items
	assert "numpy>=1.26" not in items


def test_find_cuda_violations_in_specs_ignores_rocm_specs():
	violations = find_cuda_violations_in_specs(
		[
			"torch==2.7.1+rocm7.1.1",
			"scikit-learn>=1.5",
		],
		source="test",
	)
	assert [v.item for v in violations] == []


def test_is_effective_installed_requirement_ignores_unselected_extra():
	assert _is_effective_installed_requirement('cudf-polars-cu12; extra == "gpu"') is False


def test_is_effective_installed_requirement_keeps_non_extra_marker():
	assert _is_effective_installed_requirement('nvidia-cublas-cu12; python_version >= "3.8"') is True
