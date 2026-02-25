import sys
import types
from datetime import datetime, timezone

import pytest

if "duckdb" not in sys.modules:
    sys.modules["duckdb"] = types.SimpleNamespace()

from app.runtime import RuntimeManager


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@pytest.mark.asyncio
async def test_recover_unfinished_run_emits_cache_summary_and_run_finished(monkeypatch, tmp_path):
    monkeypatch.setenv("ARTIFACT_STORE", "disk")
    monkeypatch.setenv("ARTIFACT_DIR", str(tmp_path / "artifact-root"))

    rt1 = RuntimeManager()
    run_id = "run-recovery-1"
    await rt1.artifact_store.record_run(run_id, "running")

    # Simulate a crashed run: started, made cache decisions, never finished.
    await rt1.event_store.append_event(
        {
            "type": "run_started",
            "runId": run_id,
            "at": _iso_now(),
            "runFrom": None,
        }
    )
    await rt1.event_store.append_event(
        {
            "type": "cache_decision",
            "schema_version": 1,
            "runId": run_id,
            "at": _iso_now(),
            "nodeId": "tool_1",
            "nodeKind": "tool",
            "decision": "cache_hit",
            "reason": "CACHE_HIT",
            "execKey": "k1",
        }
    )
    await rt1.event_store.append_event(
        {
            "type": "cache_decision",
            "schema_version": 1,
            "runId": run_id,
            "at": _iso_now(),
            "nodeId": "tool_2",
            "nodeKind": "tool",
            "decision": "cache_miss",
            "reason": "CACHE_ENTRY_MISSING",
            "execKey": "k2",
        }
    )

    rt2 = RuntimeManager()
    out = await rt2.recover_unfinished_runs()
    assert out["recovered"] == 1

    rec = await rt2.artifact_store.get_run(run_id)
    assert rec and rec.get("status") == "failed"

    replay = await rt2.list_run_events(run_id, after_id=0, limit=2000)
    payloads = [dict(r.get("payload") or {}) for r in replay]
    cache_summary = [p for p in payloads if p.get("type") == "cache_summary"]
    run_finished = [p for p in payloads if p.get("type") == "run_finished"]

    assert cache_summary, "Expected recovery to backfill cache_summary for unfinished run"
    assert cache_summary[-1].get("schema_version") == 1
    assert cache_summary[-1].get("cache_hit") == 1
    assert cache_summary[-1].get("cache_miss") == 1

    assert run_finished, "Expected recovery to terminally finish unfinished run"
    assert run_finished[-1].get("status") == "failed"
    assert run_finished[-1].get("error") == "RECOVERED_UNFINISHED_RUN_ON_STARTUP"
    assert run_finished[-1].get("recovered") is True
