from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routes.maintenance import router as maintenance_router
from .routes.runs import router as runs_router
from .runtime import RuntimeManager

app = FastAPI(title="Flow Runner")

@app.on_event("startup")
async def startup():
    app.state.runtime = RuntimeManager()
    
# If you proxy through SvelteKit, you can keep this strict.
# If you hit FastAPI directly in dev, allow your dev origin.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:4173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(runs_router, prefix="/runs")
app.include_router(maintenance_router, prefix="/maintenance")
