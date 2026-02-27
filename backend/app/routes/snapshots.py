from __future__ import annotations

import hashlib
import mimetypes
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, File, HTTPException, Request, UploadFile

router = APIRouter()


def _iso_now() -> str:
	return datetime.now(timezone.utc).isoformat()


@router.post("")
async def upload_snapshot(request: Request, file: UploadFile = File(...)):
	rt = request.app.state.runtime
	store = rt.artifact_store
	if not file or not file.filename:
		raise HTTPException(400, "file upload is required")

	tmp_root = Path(getattr(store, "_root", Path("./data/artifacts"))).resolve() / "tmp_uploads"
	tmp_root.mkdir(parents=True, exist_ok=True)

	fd, tmp_name = tempfile.mkstemp(prefix="snapshot_", suffix=".upload", dir=str(tmp_root))
	os.close(fd)
	tmp_path = Path(tmp_name)
	size = 0
	sha = hashlib.sha256()
	try:
		with open(tmp_path, "wb") as out:
			while True:
				chunk = await file.read(1024 * 1024)
				if not chunk:
					break
				out.write(chunk)
				sha.update(chunk)
				size += len(chunk)
		snapshot_id = sha.hexdigest().lower()
		mime = file.content_type or (mimetypes.guess_type(file.filename or "")[0] or "application/octet-stream")
		metadata = {
			"snapshotId": snapshot_id,
			"originalFilename": str(file.filename or ""),
			"byteSize": int(size),
			"mimeType": str(mime),
			"importedAt": _iso_now(),
			"graphId": "__snapshots__",
		}
		if not hasattr(store, "write_snapshot_from_file"):
			raise HTTPException(500, "artifact store does not support snapshot uploads")
		await store.write_snapshot_from_file(
			snapshot_id=snapshot_id,
			file_path=tmp_path,
			metadata=metadata,
			mime_type=mime,
		)
		return {"snapshotId": snapshot_id, "metadata": metadata}
	finally:
		try:
			if tmp_path.exists():
				tmp_path.unlink()
		except Exception:
			pass


@router.get("/{snapshot_id}")
async def get_snapshot(snapshot_id: str, request: Request):
	return await get_snapshot_meta(snapshot_id, request)


@router.get("/{snapshot_id}/meta")
async def get_snapshot_meta(snapshot_id: str, request: Request):
	rt = request.app.state.runtime
	store = rt.artifact_store
	sid = str(snapshot_id or "").strip().lower()
	if not sid:
		raise HTTPException(400, "snapshot_id is required")
	if not await store.exists(sid):
		raise HTTPException(404, "Snapshot not found")
	meta = None
	if hasattr(store, "get_snapshot_metadata"):
		meta = await store.get_snapshot_metadata(sid)
	return {"snapshotId": sid, "metadata": meta or {"snapshotId": sid}}
