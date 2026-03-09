# Backend Requirement Profiles

This repo now uses layered requirement profiles so installs can stay small by default and expand only when needed.

## `uv` baseline (TKT-073)

Dependency source-of-truth is now `backend/pyproject.toml` for `uv` workflows.

- `base`: API/runtime baseline.
- `cpu_dev`: default development profile for Win11 container and local CI.
- `rocm_train`: Linux ROCm training/finetuning profile (CUDA excluded).

Expected Python policy:
- `3.11.x` only (`>=3.11,<3.12`)
- Target runtime patch: `3.11.14`

Install examples:

```powershell
# default group (cpu_dev)
uv sync

# explicit group
uv sync --group cpu_dev
uv sync --group rocm_train
```

Note:
- ROCm-specific `torch` wheel/source wiring is intentionally deferred to container/runtime tickets.
- `rocm_train` baseline excludes CUDA packages by design.

## Lock strategy (TKT-074)

Lock artifacts:
- `backend/uv.lock` (canonical resolver lock)
- `backend/requirements/locks/cpu-dev.lock.txt` (exported reproducible install set)
- `backend/requirements/locks/rocm-train.lock.txt` (exported reproducible install set, CUDA-free baseline)

Regenerate lock artifacts:

```powershell
cd backend
python scripts/lock_profiles.py generate --python 3.11.14
```

Check lock artifacts are up to date:

```powershell
cd backend
python scripts/lock_profiles.py check --python 3.11.14
```

Repro guidance:
- Always use `--locked` installs in CI/container builds.
- Keep lock generation pinned to Python `3.11.14`.

## Docker CPU dev profile (TKT-075)

This repo ships a CPU-only backend dev container profile for Win11 Docker Desktop.

Artifacts:
- `backend/Dockerfile` target: `cpu-dev`
- `compose.yaml` profile: `backend-cpu-dev`

Run backend with live reload:

```powershell
docker compose --profile backend-cpu-dev up --build backend
```

Run backend test subset inside container:

```powershell
docker compose --profile backend-cpu-dev run --rm backend `
	uv run python -m pytest `
	tests/integration/test_env_profile_preflight.py `
	tests/e2e/test_env_profile_regression_gate.py
```

Startup smoke check:

```powershell
curl http://localhost:8000/capabilities
```

## Default install

From `backend/`:

```powershell
python -m pip install -r requirements.txt
```

Default includes:
- `core.txt` (API/runtime + core data stack)
- `compat.txt` (pandas compatibility)
- `testing.txt` (pytest stack)

## Optional install layers

Install only what your workload needs:

```powershell
python -m pip install -r requirements/llm.txt
python -m pip install -r requirements/dl.txt
python -m pip install -r requirements/finetune.txt
python -m pip install -r requirements/observability.txt
```

## Profiles

- `core.txt`: FastAPI runtime, `polars`, `numpy`, `scipy`, `scikit-learn`, `pyarrow`, core clients.
- `compat.txt`: `pandas` for existing pandas-centric executors and legacy compatibility.
- `llm.txt`: tokenization/fuzzy matching/sentence-transformers helpers.
- `dl.txt`: `torch` runtime.
- `finetune.txt`: `transformers`, `datasets`, `accelerate`, `peft`, `trl`, optional `bitsandbytes`.
- `observability.txt`: `structlog`.
- `testing.txt`: pytest and test utilities.

## Notes

- `bitsandbytes` can be platform-sensitive (especially on Windows). Keep it optional.
- The repository currently uses pandas in runtime codepaths, so `compat.txt` remains in default install.
