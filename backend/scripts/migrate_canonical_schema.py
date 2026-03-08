from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Tuple

from app.component_contracts import (
	COMPONENT_SCHEMA_VERSION,
	canonicalize_component_definition,
	validate_component_definition,
)
from app.graph_migrations import canonicalize_graph_payload


def _stable_dump(value: Dict[str, Any]) -> str:
	return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256_json(value: Dict[str, Any]) -> str:
	return hashlib.sha256(_stable_dump(value).encode("utf-8")).hexdigest()


def migrate_graph_revisions(db_path: str, *, dry_run: bool = True) -> Dict[str, Any]:
	path = Path(db_path).resolve()
	report: Dict[str, Any] = {
		"dbPath": str(path),
		"dryRun": bool(dry_run),
		"scanned": 0,
		"changed": 0,
		"changedRevisionIds": [],
		"audit": [],
	}
	if not path.exists():
		report["error"] = "db_not_found"
		return report

	with sqlite3.connect(str(path)) as conn:
		conn.row_factory = sqlite3.Row
		rows = conn.execute("SELECT revision_id, graph_json, checksum FROM graph_revisions").fetchall()
		report["scanned"] = len(rows)
		for row in rows:
			revision_id = str(row["revision_id"])
			try:
				raw_graph = json.loads(str(row["graph_json"] or "{}"))
			except Exception:
				raw_graph = {}
			normalized_graph, notes = canonicalize_graph_payload(raw_graph)
			next_json = _stable_dump(normalized_graph)
			next_checksum = _sha256_json(normalized_graph)
			changed = next_json != str(row["graph_json"] or "") or next_checksum != str(row["checksum"] or "")
			if not changed:
				continue
			report["changed"] += 1
			report["changedRevisionIds"].append(revision_id)
			report["audit"].append(
				{
					"revisionId": revision_id,
					"notes": notes,
					"checksumBefore": str(row["checksum"] or ""),
					"checksumAfter": next_checksum,
				}
			)
			if not dry_run:
				conn.execute(
					"""
					UPDATE graph_revisions
					SET graph_json = ?, checksum = ?
					WHERE revision_id = ?
					""",
					(next_json, next_checksum, revision_id),
				)
		if not dry_run:
			conn.commit()
	return report


def migrate_component_revisions(db_path: str, *, dry_run: bool = True) -> Dict[str, Any]:
	path = Path(db_path).resolve()
	report: Dict[str, Any] = {
		"dbPath": str(path),
		"dryRun": bool(dry_run),
		"scanned": 0,
		"changed": 0,
		"changedRevisionIds": [],
		"audit": [],
	}
	if not path.exists():
		report["error"] = "db_not_found"
		return report

	with sqlite3.connect(str(path)) as conn:
		conn.row_factory = sqlite3.Row
		rows = conn.execute(
			"SELECT revision_id, definition_json, checksum, schema_version FROM component_revisions"
		).fetchall()
		report["scanned"] = len(rows)
		for row in rows:
			revision_id = str(row["revision_id"])
			try:
				raw_definition = json.loads(str(row["definition_json"] or "{}"))
			except Exception:
				raw_definition = {}
			from_schema = int(row["schema_version"] or COMPONENT_SCHEMA_VERSION)
			normalized_definition, migration_notes = canonicalize_component_definition(
				raw_definition, from_schema
			)
			diagnostics = [d.as_dict() for d in validate_component_definition(normalized_definition)]
			next_json = _stable_dump(normalized_definition)
			next_checksum = _sha256_json(normalized_definition)
			changed = next_json != str(row["definition_json"] or "") or next_checksum != str(row["checksum"] or "")
			if not changed:
				continue
			report["changed"] += 1
			report["changedRevisionIds"].append(revision_id)
			report["audit"].append(
				{
					"revisionId": revision_id,
					"migrationNotes": migration_notes,
					"diagnostics": diagnostics,
					"checksumBefore": str(row["checksum"] or ""),
					"checksumAfter": next_checksum,
				}
			)
			if not dry_run:
				conn.execute(
					"""
					UPDATE component_revisions
					SET definition_json = ?, checksum = ?, schema_version = ?
					WHERE revision_id = ?
					""",
					(next_json, next_checksum, int(COMPONENT_SCHEMA_VERSION), revision_id),
				)
		if not dry_run:
			conn.commit()
	return report


def migrate_all(
	*,
	graph_db_path: str,
	component_db_path: str,
	dry_run: bool = True,
) -> Dict[str, Any]:
	return {
		"schemaVersion": 1,
		"dryRun": bool(dry_run),
		"graphs": migrate_graph_revisions(graph_db_path, dry_run=dry_run),
		"components": migrate_component_revisions(component_db_path, dry_run=dry_run),
	}


def main() -> int:
	parser = argparse.ArgumentParser(description="Migrate stored graph/component revisions to canonical schema.")
	parser.add_argument("--graph-db", default="./data/graphs/graphs.sqlite", help="Path to graph revision sqlite db.")
	parser.add_argument(
		"--component-db",
		default="./data/components/components.sqlite",
		help="Path to component revision sqlite db.",
	)
	parser.add_argument(
		"--apply",
		action="store_true",
		help="Apply updates in-place. Without this flag, script runs in dry-run mode.",
	)
	args = parser.parse_args()

	report = migrate_all(
		graph_db_path=str(args.graph_db),
		component_db_path=str(args.component_db),
		dry_run=not bool(args.apply),
	)
	print(json.dumps(report, indent=2, ensure_ascii=False))
	return 0


if __name__ == "__main__":
	raise SystemExit(main())
