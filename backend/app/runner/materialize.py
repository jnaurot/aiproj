async def materialize_text(context: ExecutionContext, artifact_id: str) -> str:
    b = await context.artifact_store.read(artifact_id)
    return b.decode("utf-8", errors="replace")
