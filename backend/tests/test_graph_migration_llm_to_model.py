import json
from pathlib import Path
import subprocess
import sys

from app.graph_migrations import migrate_llm_nodes_to_model


def _payload() -> dict:
	return {
		"graph": {
			"nodes": [
				{"id": "n1", "data": {"kind": "llm", "label": "Legacy"}},
				{"id": "n2", "data": {"kind": "model", "label": "Modern"}},
			],
			"edges": [],
		}
	}


def test_migrate_llm_nodes_to_model_converts_and_reports():
	migrated, report = migrate_llm_nodes_to_model(_payload())
	nodes = migrated["graph"]["nodes"]
	kinds = [str((n.get("data") or {}).get("kind")) for n in nodes]
	assert kinds == ["model", "model"]
	assert report["llmNodesFound"] == 1
	assert report["convertedNodeIds"] == ["n1"]
	assert report["idempotent"] is False


def test_migrate_llm_nodes_to_model_is_idempotent():
	first, _ = migrate_llm_nodes_to_model(_payload())
	second, report2 = migrate_llm_nodes_to_model(first)
	assert second == first
	assert report2["llmNodesFound"] == 0
	assert report2["idempotent"] is True


def test_migration_script_apply_creates_rollback_and_output(tmp_path):
	input_path = tmp_path / "graph.json"
	report_path = tmp_path / "report.json"
	input_path.write_text(json.dumps(_payload()), encoding="utf-8")
	rollback_dir = tmp_path / "rollbacks"
	cmd = [
		sys.executable,
		"scripts/migrate_llm_to_model.py",
		"--input",
		str(input_path),
		"--apply",
		"--report",
		str(report_path),
		"--rollback-dir",
		str(rollback_dir),
	]
	proc = subprocess.run(cmd, cwd=str(Path(__file__).resolve().parents[1]), capture_output=True, text=True, check=False)
	assert proc.returncode == 0, proc.stderr
	updated = json.loads(input_path.read_text(encoding="utf-8"))
	assert str(updated["graph"]["nodes"][0]["data"]["kind"]) == "model"
	report = json.loads(report_path.read_text(encoding="utf-8"))
	rollback_path = Path(str(report.get("rollbackArtifact") or ""))
	assert rollback_path.exists()
