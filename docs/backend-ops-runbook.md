# Backend Ops Runbook (Win11 Dev -> Ubuntu ROCm Cutover)

## Scope
- Backend runtime only.
- Frontend remains local/thin client.
- Containerized backend on:
  - Win11 dev machine (CPU profile)
  - Ubuntu 24.04 target machine (ROCm 7.1.1 profile)
- No CUDA dependencies are allowed.

## Prerequisites

### Shared
- Repo checked out.
- Docker + Compose available.
- Backend endpoints:
  - `http://localhost:8000` for `backend-cpu-dev`
  - `http://localhost:8001` for `backend-rocm`

### Win11 dev
- Docker Desktop running.

### Ubuntu 24.04 target
- ROCm host stack `7.1.1` installed and working.
- Device nodes present:
  - `/dev/kfd`
  - `/dev/dri`
- Docker engine can map ROCm devices.

## Bring-up: Win11 CPU Dev Profile
From repo root:

```powershell
docker compose --profile backend-cpu-dev up --build backend
```

Health check:

```powershell
curl http://localhost:8000/capabilities
```

Regression subset in container:

```powershell
docker compose --profile backend-cpu-dev run --rm backend `
	uv run python -m pytest `
	tests/e2e/test_env_profile_regression_gate.py `
	tests/integration/test_env_profile_preflight.py
```

No-CUDA guard:

```powershell
docker compose --profile backend-cpu-dev run --rm backend `
	python scripts/no_cuda_guard.py --check-pyproject --check-lockfiles --check-installed
```

## Bring-up: Ubuntu ROCm Runtime Profile
From repo root:

```bash
docker compose --profile backend-rocm up --build -d backend-rocm
```

Health check:

```bash
curl http://localhost:8001/capabilities
```

ROCm device check:

```bash
docker compose --profile backend-rocm exec backend-rocm \
	python scripts/rocm_device_check.py
```

Expected: JSON with `"ok": true`.

No-CUDA guard:

```bash
docker compose --profile backend-rocm exec backend-rocm \
	python scripts/no_cuda_guard.py --check-pyproject --check-lockfiles --check-installed
```

## Cutover Procedure (Win11 -> Ubuntu)
1. Confirm lockfiles are current on main branch.
2. Run Win11 CPU profile regression subset.
3. Build and boot Ubuntu ROCm profile.
4. Verify `/capabilities` and ROCm device check.
5. Run backend env-profile regression subset on Ubuntu:
```bash
docker compose --profile backend-rocm exec backend-rocm \
	uv run python -m pytest \
	tests/e2e/test_env_profile_regression_gate.py \
	tests/integration/test_env_profile_preflight.py
```
6. Switch operational traffic to Ubuntu backend endpoint.
7. Observe first production runs; keep Win11 profile available as fallback.

## Rollback
If Ubuntu cutover fails:
1. Stop Ubuntu backend service:
```bash
docker compose --profile backend-rocm stop backend-rocm
```
2. Route traffic back to prior backend endpoint.
3. Re-run CPU profile on Win11 if needed:
```powershell
docker compose --profile backend-cpu-dev up --build backend
```
4. Capture failing ROCm logs before teardown:
```bash
docker compose --profile backend-rocm logs --tail=300 backend-rocm
```

Rollback principle: immediate endpoint fallback first, diagnosis second.

## Lock Refresh Workflow
From `backend/`:

```powershell
python scripts/lock_profiles.py generate --python 3.11.14
python scripts/lock_profiles.py check --python 3.11.14
```

Artifacts expected:
- `backend/uv.lock`
- `backend/requirements/locks/cpu-dev.lock.txt`
- `backend/requirements/locks/rocm-train.lock.txt`

After refresh:
1. Run no-CUDA guard.
2. Run env-profile regression subset.
3. Rebuild both container profiles.

## Troubleshooting

### ROCm device check fails (`"ok": false`)
- Verify host has `/dev/kfd` and `/dev/dri`.
- Verify compose profile `backend-rocm` is used.
- Check container device mapping:
```bash
docker compose --profile backend-rocm config
```

### Backend starts but import/profile checks fail
- Run:
```bash
python scripts/no_cuda_guard.py --check-pyproject --check-lockfiles --check-installed
```
- If violation exists, remove CUDA-linked specs and refresh locks.

### Lock drift or inconsistent environments
- Run lock check:
```powershell
cd backend
python scripts/lock_profiles.py check --python 3.11.14
```
- Re-generate locks and rebuild containers.

### Slow or stale behavior due to cached layers
- Rebuild without cache for diagnosis:
```bash
docker compose --profile backend-rocm build --no-cache backend-rocm
docker compose --profile backend-cpu-dev build --no-cache backend
```

## Operational Notes
- Keep backend logic in server; frontend should not own env resolution.
- Prefer profile-based installs over ad-hoc pip installs.
- Keep ROCm profile CUDA-free by policy and CI guard.
