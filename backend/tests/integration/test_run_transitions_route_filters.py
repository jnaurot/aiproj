from __future__ import annotations

import asyncio
from uuid import uuid4

from fastapi.testclient import TestClient

from app.main import app


def test_run_transitions_route_filters_by_entity_and_violation_toggle():
	with TestClient(app) as client:
		rt = app.state.runtime
		run_id = f"run-transitions-route-filter-1-{uuid4().hex[:8]}"
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
					"type": "state_transition",
					"runId": run_id,
					"entity": "node",
					"entityId": "n_1",
					"source": "running",
					"target": "blocked",
					"reason": "WAITING_REQUIRED_INPUT",
					"at": "2026-03-31T00:00:01Z",
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
					"at": "2026-03-31T00:00:02Z",
				}
			)
		)

		res = client.get(f"/runs/{run_id}/transitions?entity=node&include_violations=false")
		assert res.status_code == 200, res.text
		body = res.json()
		assert body.get("entity") == "node"
		assert body.get("includeViolations") is False
		events = body.get("events") or []
		assert len(events) == 1
		event = events[0]
		assert str(event.get("type") or "") == "state_transition"
		payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
		assert str(payload.get("entity") or "") == "node"


def test_run_transitions_route_violations_only_and_cursor_progress():
	with TestClient(app) as client:
		rt = app.state.runtime
		run_id = f"run-transitions-route-filter-2-{uuid4().hex[:8]}"
		asyncio.run(rt.artifact_store.record_run(run_id, "paused"))
		asyncio.run(
			rt.event_store.append_event(
				{
					"type": "node_started",
					"runId": run_id,
					"nodeId": "n_ignore",
					"at": "2026-03-31T00:00:10Z",
				}
			)
		)
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
					"at": "2026-03-31T00:00:11Z",
				}
			)
		)
		asyncio.run(
			rt.event_store.append_event(
				{
					"type": "state_transition_violation",
					"runId": run_id,
					"entity": "node",
					"entityId": "n_viol",
					"source": "idle",
					"target": "succeeded",
					"code": "illegal_transition",
					"reason": "test",
					"at": "2026-03-31T00:00:12Z",
				}
			)
		)

		res = client.get(f"/runs/{run_id}/transitions?violations_only=true&include_violations=true")
		assert res.status_code == 200, res.text
		body = res.json()
		events = body.get("events") or []
		assert len(events) == 1
		assert str(events[0].get("type") or "") == "state_transition_violation"
		assert int(body.get("nextAfterId") or 0) >= int(events[0].get("id") or 0)
