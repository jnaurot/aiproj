from __future__ import annotations

import asyncio

from fastapi.testclient import TestClient

from app.main import app


def test_run_transitions_route_returns_transition_events():
	with TestClient(app) as client:
		rt = app.state.runtime
		run_id = "run-transitions-route-1"
		asyncio.run(rt.artifact_store.record_run(run_id, "paused"))
		asyncio.run(
			rt.event_store.append_event(
				{
					"type": "state_transition",
					"runId": run_id,
					"entity": "run",
					"entityId": run_id,
					"source": "running",
					"target": "pausing",
					"reason": "test",
					"at": "2026-03-31T00:00:00Z",
				}
			)
		)
		asyncio.run(
			rt.event_store.append_event(
				{
					"type": "state_transition_violation",
					"runId": run_id,
					"entity": "run",
					"entityId": run_id,
					"source": "pending",
					"target": "paused",
					"reason": "test",
					"code": "illegal_transition",
					"at": "2026-03-31T00:00:01Z",
				}
			)
		)

		res = client.get(f"/runs/{run_id}/transitions")
		assert res.status_code == 200, res.text
		body = res.json()
		assert body["runId"] == run_id
		events = body.get("events") or []
		assert events
		types = {str(evt.get("type") or "") for evt in events}
		assert "state_transition" in types
		for evt in events:
			payload = evt.get("payload") if isinstance(evt.get("payload"), dict) else {}
			assert str(payload.get("entity") or "") in {"run", "node"}
