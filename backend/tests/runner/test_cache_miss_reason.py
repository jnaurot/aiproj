import types

import pytest

from app.runner.run import _cache_key_debug_payload, _classify_cache_miss_reason


class _Store:
    def __init__(self, meta: dict | None):
        self._meta = meta

    async def get_latest_node_artifact(self, *, graph_id: str, node_id: str, exclude_artifact_id: str):
        return "a_prev"

    async def get(self, artifact_id: str):
        payload_schema = {}
        if isinstance(self._meta, dict):
            payload_schema = {"artifactMetadataV1": self._meta}
        return types.SimpleNamespace(payload_schema=payload_schema)


@pytest.mark.asyncio
async def test_classify_cache_miss_reason_ignores_missing_params_fingerprint():
    context = types.SimpleNamespace(graph_id="g1", artifact_store=_Store(meta={"nodeImplVersion": "TOOL@1"}))
    reason = await _classify_cache_miss_reason(
        context=context,
        node_id="n1",
        exec_key="k_new",
        node_state_hash="h_new",
        upstream_ids=["u1"],
        determinism_env={},
        node_impl_version="TOOL@1",
    )
    assert reason == "CACHE_ENTRY_MISSING"


@pytest.mark.asyncio
async def test_classify_cache_miss_reason_reports_input_changed():
    context = types.SimpleNamespace(
        graph_id="g1",
        artifact_store=_Store(
            meta={
                "nodeImplVersion": "TOOL@1",
                "paramsFingerprint": "h_same",
                "upstreamArtifactIds": ["u_old"],
            }
        ),
    )
    reason = await _classify_cache_miss_reason(
        context=context,
        node_id="n1",
        exec_key="k_new",
        node_state_hash="h_same",
        upstream_ids=["u_new"],
        determinism_env={},
        node_impl_version="TOOL@1",
    )
    assert reason == "INPUT_CHANGED"


@pytest.mark.asyncio
async def test_classify_cache_miss_reason_reports_env_changed():
    context = types.SimpleNamespace(
        graph_id="g1",
        artifact_store=_Store(
            meta={
                "nodeImplVersion": "TOOL@1",
                "paramsFingerprint": "h_same",
                "upstreamArtifactIds": ["u1"],
                "determinismFingerprint": "det_old",
            }
        ),
    )
    reason = await _classify_cache_miss_reason(
        context=context,
        node_id="n1",
        exec_key="k_new",
        node_state_hash="h_same",
        upstream_ids=["u1"],
        determinism_env={"x": 1},
        node_impl_version="TOOL@1",
    )
    assert reason == "ENV_CHANGED"


@pytest.mark.asyncio
async def test_classify_cache_miss_reason_reports_build_changed_on_code_hash():
    context = types.SimpleNamespace(
        graph_id="g1",
        artifact_store=_Store(
            meta={
                "nodeImplVersion": "TOOL@1",
                "paramsFingerprint": "h_same",
                "codeHash": "a" * 64,
            }
        ),
    )
    reason = await _classify_cache_miss_reason(
        context=context,
        node_id="n1",
        exec_key="k_new",
        node_state_hash="h_same",
        upstream_ids=[],
        determinism_env={"executor_code_hash": "b" * 64},
        node_impl_version="TOOL@1",
    )
    assert reason == "BUILD_CHANGED"


def test_cache_key_debug_payload_redacts_raw_artifact_ids():
    payload = _cache_key_debug_payload(
        node_id="node_1",
        reason="INPUT_CHANGED",
        meta={
            "nodeImplVersion": "TOOL@1",
            "paramsFingerprint": "a" * 64,
            "upstreamArtifactIds": ["artifact-secret-1", "artifact-secret-2"],
            "determinismFingerprint": "d" * 64,
            "codeHash": "c" * 64,
            "profileLock": "profile:core:cpu",
        },
        node_state_hash="b" * 64,
        upstream_ids=["artifact-secret-3"],
        determinism_env={"executor_code_hash": "e" * 64},
        node_impl_version="TOOL@1",
    )
    text = str(payload)
    assert "artifact-secret-1" not in text
    assert "artifact-secret-2" not in text
    assert "artifact-secret-3" not in text
    assert payload["previous"]["upstream"]["count"] == 2
    assert payload["current"]["upstream"]["count"] == 1
    assert len(str(payload["previous"]["upstream"]["digest"])) == 12
    assert len(str(payload["current"]["upstream"]["digest"])) == 12
