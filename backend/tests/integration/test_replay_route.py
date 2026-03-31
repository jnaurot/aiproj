from __future__ import annotations

from fastapi.testclient import TestClient
import pytest

from app.main import app


def test_replay_route_success_passthrough(monkeypatch: pytest.MonkeyPatch):
	with TestClient(app) as client:
		rt = app.state.runtime
		async def _fake_request_replay(**kwargs):
			return {
				"sourceRunId": kwargs.get("source_run_id"),
				"runId": "run-replay-new-1",
				"found": True,
				"replayed": True,
				"status": "running",
			}
		monkeypatch.setattr(rt, "request_replay", _fake_request_replay)
		res = client.post("/runs/run-source-1/replay", json={})
		assert res.status_code == 200, res.text
		body = res.json()
		assert body["replayed"] is True
		assert body["runId"] == "run-replay-new-1"
		assert body["sourceRunId"] == "run-source-1"


def test_replay_route_returns_structured_conflict(monkeypatch: pytest.MonkeyPatch):
	with TestClient(app) as client:
		rt = app.state.runtime
		async def _fake_request_replay(**kwargs):
			return {
				"sourceRunId": kwargs.get("source_run_id"),
				"found": True,
				"replayed": False,
				"status": "unknown",
				"errorCode": "REPLAY_CONTRACT_VALIDATION_FAILED",
				"details": {"reasonCodes": ["node_state_changed"], "nodeIds": ["n2"], "mismatches": []},
			}
		monkeypatch.setattr(rt, "request_replay", _fake_request_replay)
		res = client.post("/runs/run-source-2/replay", json={})
		assert res.status_code == 409, res.text
		body = res.json().get("detail") or {}
		assert body["replayed"] is False
		assert body["errorCode"] == "REPLAY_CONTRACT_VALIDATION_FAILED"


def test_contract_diff_route_returns_payload(monkeypatch: pytest.MonkeyPatch):
	with TestClient(app) as client:
		rt = app.state.runtime
		async def _fake_diff_run_execution_contracts(**kwargs):
			return {
				"found": True,
				"runId": kwargs.get("run_id"),
				"againstRunId": kwargs.get("against_run_id"),
				"contractDiff": {"ok": False, "categories": ["node_params"], "reasonCodes": ["node_state_changed"]},
			}
		monkeypatch.setattr(rt, "diff_run_execution_contracts", _fake_diff_run_execution_contracts)
		res = client.get("/runs/run-a/contract-diff", params={"againstRunId": "run-b"})
		assert res.status_code == 200, res.text
		body = res.json()
		assert body["runId"] == "run-a"
		assert body["againstRunId"] == "run-b"
		assert isinstance(body.get("contractDiff"), dict)
