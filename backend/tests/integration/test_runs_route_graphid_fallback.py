import pytest
from fastapi.testclient import TestClient


@pytest.mark.asyncio
async def test_get_run_fallback_includes_graph_id_from_artifact_store(monkeypatch):
	monkeypatch.setenv("ARTIFACT_STORE", "memory")

	from app.main import app

	with TestClient(app) as client:
		rt = app.state.runtime
		assert rt is not None
		run_id = "run-fallback-graphid-1"
		await rt.artifact_store.record_run(run_id, "succeeded")
		get_graph_id_fn = getattr(rt.artifact_store, "get_run_graph_id", None)
		assert callable(get_graph_id_fn)
		original_get_run = rt.get_run
		try:
			rt.get_run = lambda _rid: None
			original_get_run_graph_id = rt.artifact_store.get_run_graph_id
			async def _fake_get_run_graph_id(_rid: str):
				return "graph-fallback-1"
			rt.artifact_store.get_run_graph_id = _fake_get_run_graph_id
			res = client.get(f"/runs/{run_id}")
			assert res.status_code == 200, res.text
			body = res.json()
			assert str(body.get("runId") or "") == run_id
			assert str(body.get("graphId") or "") == "graph-fallback-1"
		finally:
			rt.get_run = original_get_run
			rt.artifact_store.get_run_graph_id = original_get_run_graph_id
