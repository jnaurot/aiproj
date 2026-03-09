from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .component_revisions import ComponentRevisionStore
from .feature_flags import get_feature_flags
from .graph_revisions import GraphRevisionStore
from .runner.capabilities import capabilities_response, capability_signature
from .routes.components import router as components_router
from .routes.env_profiles import router as env_profiles_router
from .routes.graphs import router as graphs_router
from .routes.maintenance import router as maintenance_router
from .routes.runs import router as runs_router
from .routes.snapshots import router as snapshots_router
from .runtime import RuntimeManager
from .services.no_cuda_guard import ensure_no_cuda_or_raise

app = FastAPI(title="Flow Runner")

@app.on_event("startup")
async def startup():
    ensure_no_cuda_or_raise(check_installed=True)
    app.state.runtime = RuntimeManager()
    app.state.graph_revisions = GraphRevisionStore("./data/graphs/graphs.sqlite")
    app.state.component_revisions = ComponentRevisionStore("./data/components/components.sqlite")
    # Runner expansion reads component revisions from runtime_ref (RuntimeManager).
    # Keep app.state and runtime_ref aligned.
    app.state.runtime.component_revisions = app.state.component_revisions
    print("[feature-flags]", get_feature_flags())
    await app.state.runtime.recover_unfinished_runs()
    
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
app.include_router(graphs_router, prefix="/graphs")
app.include_router(components_router, prefix="/components")
app.include_router(env_profiles_router, prefix="/env")
app.include_router(snapshots_router, prefix="/snapshots")
app.include_router(maintenance_router, prefix="/maintenance")


@app.get("/capabilities")
async def get_capabilities():
    caps = capabilities_response()
    flags = get_feature_flags()
    return {
        "schemaVersion": 1,
        "signature": capability_signature(),
        "capabilities": caps,
        "featureFlags": {
            "STRICT_SCHEMA_EDGE_CHECKS": bool(flags.get("STRICT_SCHEMA_EDGE_CHECKS", True)),
            "STRICT_COERCION_POLICY": bool(flags.get("STRICT_COERCION_POLICY", True)),
        },
    }
