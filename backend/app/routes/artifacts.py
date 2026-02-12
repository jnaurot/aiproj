# app/routes/artifacts.py
from fastapi import APIRouter, Response, HTTPException
from ..runner.artifacts import get_artifact_store  # however you access it

router = APIRouter()

@router.get("/artifacts/{artifact_id}")
async def get_artifact(artifact_id: str):
    store = get_artifact_store()
    if not await store.exists(artifact_id):
        raise HTTPException(404, "artifact not found")

    art = await store.get(artifact_id)
    data = await store.read(artifact_id)
    return Response(content=data, media_type=art.mime_type)
