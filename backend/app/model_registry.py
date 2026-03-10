from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4


_ALLOWED_STAGES = {"candidate", "baseline", "prod"}
_PROMOTION_TRANSITIONS = {
	"candidate": {"baseline"},
	"baseline": {"prod", "candidate"},
	"prod": {"baseline"},
}


def _iso_now() -> str:
	return datetime.now(timezone.utc).isoformat()


def _to_json(value: Optional[Dict[str, Any]]) -> str:
	return json.dumps(value or {}, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _from_json(value: Optional[str]) -> Dict[str, Any]:
	if not value:
		return {}
	try:
		parsed = json.loads(value)
		return parsed if isinstance(parsed, dict) else {}
	except Exception:
		return {}


def _normalize_stage(stage: Optional[str], *, default: str = "candidate") -> str:
	s = str(stage or "").strip().lower() or default
	if s not in _ALLOWED_STAGES:
		raise ValueError(f"invalid stage: {stage}")
	return s


@dataclass
class ModelVersion:
	model_id: str
	version_id: str
	version_number: int
	stage: str
	created_at: str
	run_id: Optional[str]
	graph_id: Optional[str]
	artifact_id: Optional[str]
	metrics: Dict[str, Any]
	params: Dict[str, Any]
	environment: Dict[str, Any]
	provenance: Dict[str, Any]
	promoted_at: Optional[str]
	promoted_by: Optional[str]


class ModelRegistryStore:
	def __init__(self, db_path: str):
		self._db_path = str(db_path)
		db_parent = Path(self._db_path).resolve().parent
		db_parent.mkdir(parents=True, exist_ok=True)
		self._lock = threading.RLock()
		self._init_db()

	def _connect(self) -> sqlite3.Connection:
		conn = sqlite3.connect(self._db_path)
		conn.row_factory = sqlite3.Row
		return conn

	def _init_db(self) -> None:
		with self._lock:
			with self._connect() as conn:
				cur = conn.cursor()
				cur.execute(
					"""
					CREATE TABLE IF NOT EXISTS models (
						model_id TEXT PRIMARY KEY,
						model_name TEXT,
						model_name_norm TEXT,
						created_at TEXT NOT NULL,
						updated_at TEXT NOT NULL,
						latest_version_id TEXT
					)
					"""
				)
				cur.execute(
					"""
					CREATE TABLE IF NOT EXISTS model_versions (
						version_id TEXT PRIMARY KEY,
						model_id TEXT NOT NULL,
						version_number INTEGER NOT NULL,
						stage TEXT NOT NULL,
						created_at TEXT NOT NULL,
						run_id TEXT,
						graph_id TEXT,
						artifact_id TEXT,
						metrics_json TEXT NOT NULL,
						params_json TEXT NOT NULL,
						environment_json TEXT NOT NULL,
						provenance_json TEXT NOT NULL,
						promoted_at TEXT,
						promoted_by TEXT,
						FOREIGN KEY(model_id) REFERENCES models(model_id)
					)
					"""
				)
				cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_models_name_norm ON models(model_name_norm)")
				cur.execute(
					"""
					CREATE UNIQUE INDEX IF NOT EXISTS idx_model_versions_model_number
					ON model_versions(model_id, version_number)
					"""
				)
				cur.execute(
					"""
					CREATE INDEX IF NOT EXISTS idx_model_versions_model_created
					ON model_versions(model_id, created_at DESC)
					"""
				)
				cur.execute(
					"""
					CREATE INDEX IF NOT EXISTS idx_model_versions_model_stage
					ON model_versions(model_id, stage)
					"""
				)
				conn.commit()

	def clear_all(self) -> None:
		with self._lock:
			with self._connect() as conn:
				cur = conn.cursor()
				cur.execute("DELETE FROM model_versions")
				cur.execute("DELETE FROM models")
				conn.commit()

	def _ensure_model(
		self,
		conn: sqlite3.Connection,
		*,
		model_id: Optional[str],
		model_name: Optional[str],
	) -> tuple[str, Optional[str]]:
		mid = str(model_id or "").strip()
		mname = str(model_name or "").strip() or None
		mname_norm = mname.lower() if mname else None
		if mid:
			row = conn.execute(
				"SELECT model_id, model_name FROM models WHERE model_id=?",
				(mid,),
			).fetchone()
			if row:
				if mname_norm and str(row["model_name"] or "").strip().lower() != mname_norm:
					raise ValueError(f"model_id already exists with different name: {mid}")
				return (mid, str(row["model_name"]) if row["model_name"] else mname)
		elif mname_norm:
			row = conn.execute(
				"SELECT model_id, model_name FROM models WHERE model_name_norm=?",
				(mname_norm,),
			).fetchone()
			if row:
				return (str(row["model_id"]), str(row["model_name"]) if row["model_name"] else mname)

		if not mid:
			mid = f"model_{uuid4()}"
		now = _iso_now()
		conn.execute(
			"""
			INSERT INTO models (model_id, model_name, model_name_norm, created_at, updated_at, latest_version_id)
			VALUES (?, ?, ?, ?, ?, NULL)
			""",
			(mid, mname, mname_norm, now, now),
		)
		return (mid, mname)

	def register_version(
		self,
		*,
		model_id: Optional[str],
		model_name: Optional[str],
		stage: str = "candidate",
		run_id: Optional[str] = None,
		graph_id: Optional[str] = None,
		artifact_id: Optional[str] = None,
		metrics: Optional[Dict[str, Any]] = None,
		params: Optional[Dict[str, Any]] = None,
		environment: Optional[Dict[str, Any]] = None,
		provenance: Optional[Dict[str, Any]] = None,
		version_id: Optional[str] = None,
	) -> ModelVersion:
		stage_norm = _normalize_stage(stage, default="candidate")
		rid = str(run_id or "").strip() or None
		gid = str(graph_id or "").strip() or None
		aid = str(artifact_id or "").strip() or None
		ver_id = str(version_id or "").strip() or f"mver_{uuid4()}"
		created_at = _iso_now()
		metrics_obj = metrics if isinstance(metrics, dict) else {}
		params_obj = params if isinstance(params, dict) else {}
		env_obj = environment if isinstance(environment, dict) else {}
		prov_obj = provenance if isinstance(provenance, dict) else {}
		with self._lock:
			with self._connect() as conn:
				mid, normalized_name = self._ensure_model(conn, model_id=model_id, model_name=model_name)
				row = conn.execute(
					"SELECT COALESCE(MAX(version_number), 0) AS m FROM model_versions WHERE model_id=?",
					(mid,),
				).fetchone()
				next_num = int((row["m"] if row else 0) or 0) + 1
				conn.execute(
					"""
					INSERT INTO model_versions (
						version_id, model_id, version_number, stage, created_at,
						run_id, graph_id, artifact_id,
						metrics_json, params_json, environment_json, provenance_json,
						promoted_at, promoted_by
					) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
					""",
					(
						ver_id,
						mid,
						next_num,
						stage_norm,
						created_at,
						rid,
						gid,
						aid,
						_to_json(metrics_obj),
						_to_json(params_obj),
						_to_json(env_obj),
						_to_json(prov_obj),
						created_at if stage_norm != "candidate" else None,
						"system" if stage_norm != "candidate" else None,
					),
				)
				conn.execute(
					"""
					UPDATE models
					SET model_name = COALESCE(?, model_name),
						model_name_norm = COALESCE(?, model_name_norm),
						updated_at = ?, latest_version_id = ?
					WHERE model_id = ?
					""",
					(
						normalized_name,
						normalized_name.lower() if normalized_name else None,
						created_at,
						ver_id,
						mid,
					),
				)
				conn.commit()

		return ModelVersion(
			model_id=mid,
			version_id=ver_id,
			version_number=next_num,
			stage=stage_norm,
			created_at=created_at,
			run_id=rid,
			graph_id=gid,
			artifact_id=aid,
			metrics=metrics_obj,
			params=params_obj,
			environment=env_obj,
			provenance=prov_obj,
			promoted_at=created_at if stage_norm != "candidate" else None,
			promoted_by="system" if stage_norm != "candidate" else None,
		)

	def list_models(self, *, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
		lim = max(1, min(int(limit), 500))
		off = max(0, int(offset))
		with self._lock:
			with self._connect() as conn:
				rows = conn.execute(
					"""
					SELECT m.model_id, m.model_name, m.created_at, m.updated_at, m.latest_version_id,
						(SELECT COUNT(1) FROM model_versions v WHERE v.model_id = m.model_id) AS version_count,
						(SELECT v.version_id FROM model_versions v WHERE v.model_id = m.model_id AND v.stage='baseline' ORDER BY v.created_at DESC LIMIT 1) AS baseline_version_id,
						(SELECT v.version_id FROM model_versions v WHERE v.model_id = m.model_id AND v.stage='prod' ORDER BY v.created_at DESC LIMIT 1) AS prod_version_id
					FROM models m
					ORDER BY m.updated_at DESC
					LIMIT ? OFFSET ?
					""",
					(lim, off),
				).fetchall()
				return [
					{
						"modelId": str(r["model_id"]),
						"modelName": str(r["model_name"]) if r["model_name"] else None,
						"createdAt": str(r["created_at"]),
						"updatedAt": str(r["updated_at"]),
						"latestVersionId": str(r["latest_version_id"]) if r["latest_version_id"] else None,
						"versionCount": int(r["version_count"] or 0),
						"baselineVersionId": str(r["baseline_version_id"]) if r["baseline_version_id"] else None,
						"prodVersionId": str(r["prod_version_id"]) if r["prod_version_id"] else None,
					}
					for r in rows
				]

	def get_model(self, model_id: str) -> Optional[Dict[str, Any]]:
		mid = str(model_id or "").strip()
		if not mid:
			return None
		with self._lock:
			with self._connect() as conn:
				row = conn.execute(
					"""
					SELECT m.model_id, m.model_name, m.created_at, m.updated_at, m.latest_version_id,
						(SELECT COUNT(1) FROM model_versions v WHERE v.model_id = m.model_id) AS version_count,
						(SELECT v.version_id FROM model_versions v WHERE v.model_id = m.model_id AND v.stage='baseline' ORDER BY v.created_at DESC LIMIT 1) AS baseline_version_id,
						(SELECT v.version_id FROM model_versions v WHERE v.model_id = m.model_id AND v.stage='prod' ORDER BY v.created_at DESC LIMIT 1) AS prod_version_id
					FROM models m
					WHERE m.model_id = ?
					""",
					(mid,),
				).fetchone()
				if not row:
					return None
				return {
					"modelId": str(row["model_id"]),
					"modelName": str(row["model_name"]) if row["model_name"] else None,
					"createdAt": str(row["created_at"]),
					"updatedAt": str(row["updated_at"]),
					"latestVersionId": str(row["latest_version_id"]) if row["latest_version_id"] else None,
					"versionCount": int(row["version_count"] or 0),
					"baselineVersionId": str(row["baseline_version_id"]) if row["baseline_version_id"] else None,
					"prodVersionId": str(row["prod_version_id"]) if row["prod_version_id"] else None,
				}

	def list_versions(self, model_id: str, *, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
		mid = str(model_id or "").strip()
		if not mid:
			return []
		lim = max(1, min(int(limit), 500))
		off = max(0, int(offset))
		with self._lock:
			with self._connect() as conn:
				rows = conn.execute(
					"""
					SELECT version_id, model_id, version_number, stage, created_at,
						run_id, graph_id, artifact_id,
						metrics_json, params_json, environment_json, provenance_json,
						promoted_at, promoted_by
					FROM model_versions
					WHERE model_id = ?
					ORDER BY version_number DESC
					LIMIT ? OFFSET ?
					""",
					(mid, lim, off),
				).fetchall()
				return [self._row_to_version_dict(r) for r in rows]

	def get_version(self, model_id: str, version_id: str) -> Optional[Dict[str, Any]]:
		mid = str(model_id or "").strip()
		vid = str(version_id or "").strip()
		if not mid or not vid:
			return None
		with self._lock:
			with self._connect() as conn:
				row = conn.execute(
					"""
					SELECT version_id, model_id, version_number, stage, created_at,
						run_id, graph_id, artifact_id,
						metrics_json, params_json, environment_json, provenance_json,
						promoted_at, promoted_by
					FROM model_versions
					WHERE model_id = ? AND version_id = ?
					""",
					(mid, vid),
				).fetchone()
				return self._row_to_version_dict(row) if row else None

	def promote_version(
		self,
		*,
		model_id: str,
		version_id: str,
		to_stage: str,
		force: bool = False,
		promoted_by: Optional[str] = None,
	) -> Dict[str, Any]:
		mid = str(model_id or "").strip()
		vid = str(version_id or "").strip()
		target = _normalize_stage(to_stage)
		if not mid or not vid:
			raise ValueError("model_id and version_id are required")
		now = _iso_now()
		actor = str(promoted_by or "").strip() or "system"

		with self._lock:
			with self._connect() as conn:
				row = conn.execute(
					"""
					SELECT version_id, stage
					FROM model_versions
					WHERE model_id = ? AND version_id = ?
					""",
					(mid, vid),
				).fetchone()
				if not row:
					raise LookupError("version_not_found")
				current = str(row["stage"] or "candidate").lower()
				if current == target:
					return {"ok": True, "changed": False, "fromStage": current, "toStage": target, "demotedVersionId": None}
				allowed = _PROMOTION_TRANSITIONS.get(current, set())
				if target not in allowed:
					raise ValueError(f"invalid transition: {current}->{target}")

				conflict = None
				if target in {"baseline", "prod"}:
					conflict_row = conn.execute(
						"""
						SELECT version_id
						FROM model_versions
						WHERE model_id = ? AND stage = ? AND version_id <> ?
						ORDER BY created_at DESC
						LIMIT 1
						""",
						(mid, target, vid),
					).fetchone()
					conflict = str(conflict_row["version_id"]) if conflict_row and conflict_row["version_id"] else None
				demoted_version_id = None
				if conflict:
					if not force:
						raise RuntimeError(f"stage_conflict:{target}:{conflict}")
					conn.execute(
						"""
						UPDATE model_versions
						SET stage='candidate', promoted_at=?, promoted_by=?
						WHERE model_id = ? AND version_id = ?
						""",
						(now, actor, mid, conflict),
					)
					demoted_version_id = conflict

				conn.execute(
					"""
					UPDATE model_versions
					SET stage=?, promoted_at=?, promoted_by=?
					WHERE model_id = ? AND version_id = ?
					""",
					(target, now, actor, mid, vid),
				)
				conn.execute(
					"UPDATE models SET updated_at=? WHERE model_id=?",
					(now, mid),
				)
				conn.commit()
				return {
					"ok": True,
					"changed": True,
					"fromStage": current,
					"toStage": target,
					"demotedVersionId": demoted_version_id,
				}

	def _row_to_version_dict(self, row: sqlite3.Row) -> Dict[str, Any]:
		return {
			"versionId": str(row["version_id"]),
			"modelId": str(row["model_id"]),
			"versionNumber": int(row["version_number"]),
			"stage": str(row["stage"]),
			"createdAt": str(row["created_at"]),
			"runId": str(row["run_id"]) if row["run_id"] else None,
			"graphId": str(row["graph_id"]) if row["graph_id"] else None,
			"artifactId": str(row["artifact_id"]) if row["artifact_id"] else None,
			"metrics": _from_json(row["metrics_json"]),
			"params": _from_json(row["params_json"]),
			"environment": _from_json(row["environment_json"]),
			"provenance": _from_json(row["provenance_json"]),
			"promotedAt": str(row["promoted_at"]) if row["promoted_at"] else None,
			"promotedBy": str(row["promoted_by"]) if row["promoted_by"] else None,
		}
