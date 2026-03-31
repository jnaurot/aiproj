from __future__ import annotations

import asyncio

from fastapi.testclient import TestClient

from app.main import app


def test_state_machine_migration_endpoint_migrates_legacy_run_rows_and_events():
	with TestClient(app) as client:
		rt = app.state.runtime
		run_id = "run-state-machine-migration-1"
		asyncio.run(rt.artifact_store.record_run(run_id, "cancelled"))
		asyncio.run(
			rt.event_store.append_event(
				{
					"type": "run_cancelled",
					"runId": run_id,
					"status": "cancelled",
					"at": "2026-03-31T01:00:00Z",
				}
			)
		)
		asyncio.run(
			rt.event_store.append_event(
				{
					"type": "run_finished",
					"runId": run_id,
					"status": "done",
					"at": "2026-03-31T01:00:01Z",
				}
			)
		)

		res = client.post("/runs/migrations/state-machine", json={"runId": run_id, "dryRun": False})
		assert res.status_code == 200, res.text
		body = res.json()
		report = body.get("report") or {}
		summary = report.get("summary") or {}
		assert int(summary.get("runsFixed", 0)) >= 1
		assert int(summary.get("eventsFixed", 0)) >= 2

		run_rows = client.get("/runs").json().get("runs") or []
		row = next((r for r in run_rows if str(r.get("run_id") or "") == run_id), None)
		assert row is not None
		assert str(row.get("status") or "") == "canceled"

		replay = client.get(f"/runs/{run_id}/events")
		assert replay.status_code == 200, replay.text
		events = replay.json().get("events") or []
		assert events
		types = [str(e.get("type") or "") for e in events]
		assert "run_canceled" in types
		finished = [e for e in events if str(e.get("type") or "") == "run_finished"]
		assert finished
		assert str((finished[-1].get("payload") or {}).get("status") or "") == "succeeded"
