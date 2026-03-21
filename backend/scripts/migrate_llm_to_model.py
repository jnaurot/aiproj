from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

_BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(_BACKEND_ROOT) not in sys.path:
	sys.path.insert(0, str(_BACKEND_ROOT))

from app.graph_migrations import migrate_llm_nodes_to_model


def _read_json(path: Path) -> Dict[str, Any]:
	return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
	path.parent.mkdir(parents=True, exist_ok=True)
	path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
	parser = argparse.ArgumentParser(description="Migrate graph node kinds from 'llm' to 'model'.")
	parser.add_argument("--input", required=True, help="Path to graph JSON file")
	parser.add_argument("--output", help="Output path (defaults to --input when --apply)")
	parser.add_argument("--report", help="Optional report JSON path")
	parser.add_argument("--apply", action="store_true", help="Write migrated graph")
	parser.add_argument("--rollback-dir", help="Directory for rollback artifact when --apply")
	args = parser.parse_args()

	input_path = Path(args.input).resolve()
	output_path = Path(args.output).resolve() if args.output else input_path
	raw = _read_json(input_path)
	migrated, report = migrate_llm_nodes_to_model(raw)
	report_payload = {
		"mode": "apply" if args.apply else "dry-run",
		"input": str(input_path),
		"output": str(output_path),
		**report,
	}

	if args.apply:
		rollback_root = Path(args.rollback_dir).resolve() if args.rollback_dir else (input_path.parent / ".migrations")
		ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
		rollback_name = f"{input_path.stem}.rollback.{ts}.json"
		rollback_path = rollback_root / rollback_name
		_write_json(rollback_path, raw)
		_write_json(output_path, migrated)
		report_payload["rollbackArtifact"] = str(rollback_path)

	if args.report:
		_write_json(Path(args.report).resolve(), report_payload)

	print(json.dumps(report_payload, ensure_ascii=False, indent=2, sort_keys=True))
	return 0


if __name__ == "__main__":
	raise SystemExit(main())
