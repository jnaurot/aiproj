from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from app.component_revisions import ComponentRevisionStore
from app.graph_revisions import GraphRevisionStore
from scripts.migrate_canonical_schema import (
	migrate_component_revisions,
	migrate_graph_revisions,
)


def _seed_graph_db(db_path: Path) -> str:
	store = GraphRevisionStore(str(db_path))
	created = store.create_revision(
		graph_id="graph_migrate",
		graph={
			"version": 1,
			"nodes": [{"id": "n1", "data": {"kind": "source", "ports": {"in": None, "out": "table"}}}],
			"edges": [],
		},
	)
	revision_id = created.revision_id
	with sqlite3.connect(str(db_path)) as conn:
		raw = {
			"version": 1,
			"nodes": [{"id": "n1", "data": {"kind": "SOURCE", "ports": {"in": "", "out": "TABLE"}}}],
		}
		conn.execute(
			"UPDATE graph_revisions SET graph_json = ?, checksum = ? WHERE revision_id = ?",
			(json.dumps(raw), "stale-checksum", revision_id),
		)
		conn.commit()
	return revision_id


def _seed_component_db(db_path: Path) -> str:
	store = ComponentRevisionStore(str(db_path))
	created = store.create_revision(
		component_id="cmp_migrate",
		definition={
			"graph": {"nodes": [], "edges": []},
			"api": {
				"inputs": [],
				"outputs": [{"name": "out_data", "portType": "json", "typedSchema": {"type": "json", "fields": []}}],
			},
			"configSchema": {},
		},
	)
	revision_id = created.revision_id
	with sqlite3.connect(str(db_path)) as conn:
		raw = {
			"graph": {"additional": True},
			"api": {"inputs": {}, "outputs": {}},
			"configSchema": [],
		}
		conn.execute(
			"UPDATE component_revisions SET definition_json = ?, checksum = ?, schema_version = ? WHERE revision_id = ?",
			(json.dumps(raw), "stale-checksum", 999, revision_id),
		)
		conn.commit()
	return revision_id


def test_graph_migration_dry_run_and_apply(tmp_path: Path):
	db_path = tmp_path / "graphs.sqlite"
	revision_id = _seed_graph_db(db_path)

	dry = migrate_graph_revisions(str(db_path), dry_run=True)
	assert dry["scanned"] == 1
	assert dry["changed"] == 1
	assert dry["changedRevisionIds"] == [revision_id]

	with sqlite3.connect(str(db_path)) as conn:
		row = conn.execute(
			"SELECT graph_json, checksum FROM graph_revisions WHERE revision_id = ?",
			(revision_id,),
		).fetchone()
		assert row is not None
		assert str(row[1]) == "stale-checksum"

	live = migrate_graph_revisions(str(db_path), dry_run=False)
	assert live["changed"] == 1
	with sqlite3.connect(str(db_path)) as conn:
		row = conn.execute(
			"SELECT graph_json, checksum FROM graph_revisions WHERE revision_id = ?",
			(revision_id,),
		).fetchone()
		assert row is not None
		graph = json.loads(str(row[0]))
		assert isinstance(graph.get("nodes"), list)
		assert isinstance(graph.get("edges"), list)
		assert graph["nodes"][0]["data"]["kind"] == "source"
		assert graph["nodes"][0]["data"]["ports"]["out"] == "table"
		assert str(row[1]) != "stale-checksum"

	second_live = migrate_graph_revisions(str(db_path), dry_run=False)
	assert second_live["changed"] == 0
	assert second_live["changedRevisionIds"] == []


def test_component_migration_dry_run_and_apply(tmp_path: Path):
	db_path = tmp_path / "components.sqlite"
	revision_id = _seed_component_db(db_path)

	dry = migrate_component_revisions(str(db_path), dry_run=True)
	assert dry["scanned"] == 1
	assert dry["changed"] == 1
	assert dry["changedRevisionIds"] == [revision_id]

	with sqlite3.connect(str(db_path)) as conn:
		row = conn.execute(
			"SELECT definition_json, checksum, schema_version FROM component_revisions WHERE revision_id = ?",
			(revision_id,),
		).fetchone()
		assert row is not None
		assert str(row[1]) == "stale-checksum"
		assert int(row[2]) == 999

	live = migrate_component_revisions(str(db_path), dry_run=False)
	assert live["changed"] == 1
	with sqlite3.connect(str(db_path)) as conn:
		row = conn.execute(
			"SELECT definition_json, checksum, schema_version FROM component_revisions WHERE revision_id = ?",
			(revision_id,),
		).fetchone()
		assert row is not None
		definition = json.loads(str(row[0]))
		assert isinstance(definition.get("graph", {}).get("nodes"), list)
		assert isinstance(definition.get("graph", {}).get("edges"), list)
		assert isinstance(definition.get("api", {}).get("inputs"), list)
		assert isinstance(definition.get("api", {}).get("outputs"), list)
		assert int(row[2]) == 1
		assert str(row[1]) != "stale-checksum"

	second_live = migrate_component_revisions(str(db_path), dry_run=False)
	assert second_live["changed"] == 0
	assert second_live["changedRevisionIds"] == []
