import pytest

from app.executors import builtin_profiles as mod
from app.executors.builtin_profiles import missing_packages_for_packages, resolve_builtin_environment


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


def test_package_module_name_uses_aliases_for_layer1_packages():
    assert mod.package_module_name("scikit-learn>=1.5") == "sklearn"
    assert mod.package_module_name("python-dateutil>=2.9") == "dateutil"


def test_missing_packages_for_packages_uses_sklearn_alias(monkeypatch):
    calls = []

    def _fake_find_spec(name: str):
        calls.append(name)
        return object() if name in {"sklearn", "numpy"} else None

    monkeypatch.setattr(mod.importlib.util, "find_spec", _fake_find_spec)
    missing = missing_packages_for_packages(["numpy>=1.26", "scikit-learn>=1.5"])
    assert missing == []
    assert "sklearn" in calls
