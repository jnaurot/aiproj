import pytest

from app.executors.builtin_profiles import resolve_builtin_environment


def test_resolve_builtin_environment_defaults_to_core():
    resolved = resolve_builtin_environment({"toolId": "noop"})
    assert resolved["profileId"] == "core"
    assert "numpy" in resolved["packages"]
    assert resolved["source"] == "profile"


def test_resolve_builtin_environment_custom_requires_packages():
    with pytest.raises(ValueError):
        resolve_builtin_environment({"toolId": "noop", "profileId": "custom", "customPackages": []})


def test_resolve_builtin_environment_rejects_custom_packages_for_non_custom_profile():
    with pytest.raises(ValueError):
        resolve_builtin_environment(
            {
                "toolId": "noop",
                "profileId": "data",
                "customPackages": ["polars"],
            }
        )


def test_resolve_builtin_environment_includes_locked_when_present():
    resolved = resolve_builtin_environment({"toolId": "noop", "locked": "sha256:abc123"})
    assert resolved["profileId"] == "core"
    assert resolved["locked"] == "sha256:abc123"
